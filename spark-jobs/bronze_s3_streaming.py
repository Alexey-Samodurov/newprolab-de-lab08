import argparse
from functools import partial

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)

from hudi_utils import hudi_opts, write_hudi
from watermark_utils import (
    bootstrap_watermark_table,
    extract_source_partitions_from_column,
    write_watermark as _write_watermark_rows,
)


TX_SCHEMA = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("promo_code_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
])

CANCEL_SCHEMA = StructType([
    StructField("cancellation_id", LongType(), True),
    StructField("original_transaction_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("cancelled_at", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
])

RATES_SCHEMA = StructType([
    StructField("update_id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("rate_tgrk_punk", DoubleType(), True),
    StructField("rate_tgrk_rub", DoubleType(), True),
])


def handle_transactions(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of transaction events and write to Hudi.

    Enriches each row with a composite primary key, event_day partition, and
    ingested_at timestamp, then upserts into the bronze transactions Hudi table
    and records a watermark entry.

    Args:
        spark: Active SparkSession.
        batch_df: DataFrame containing raw transaction records for this batch.
        batch_id: Unique micro-batch identifier assigned by Structured Streaming.
    """
    if batch_df.rdd.isEmpty():
        print(f"[tx batch={batch_id}] empty")
        return
    df = (batch_df
          .withColumn("created_ts", F.to_timestamp(F.from_unixtime("created_at")))
          .withColumn("event_day", F.date_format("created_ts", "yyyy-MM-dd"))
          .withColumn("composite_pk",
                      F.concat_ws("|",
                                  F.col("transaction_id").cast("string"),
                                  F.coalesce(F.col("created_at").cast("string"), F.lit("0")),
                                  F.coalesce(F.col("user_id").cast("string"), F.lit("0"))))
          .withColumn("ingested_at", F.current_timestamp()))
    df.persist()
    rows = df.count()
    write_hudi(df, hudi_opts(
        "transactions", "bronze",
        pk="composite_pk",
        partition_field="event_day",
        precombine="ingested_at",
        column_stats_cols="event_day,status,transaction_type,ingested_at",
    ))
    _write_watermark_rows(
        spark, "transactions",
        extract_source_partitions_from_column(df, "event_day"),
        rows, batch_id,
    )
    df.unpersist()
    print(f"[tx batch={batch_id}] rows={rows}")


def handle_cancellations(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of cancellation events and write to Hudi.

    Parses the cancelled_at timestamp, derives event_day, deduplicates by
    cancellation_id, and upserts into the bronze cancellations Hudi table using a
    global index. Records a watermark entry after the write.

    Args:
        spark: Active SparkSession.
        batch_df: DataFrame containing raw cancellation records for this batch.
        batch_id: Unique micro-batch identifier assigned by Structured Streaming.
    """
    if batch_df.rdd.isEmpty():
        print(f"[cancel batch={batch_id}] empty")
        return
    df = (batch_df
          .withColumn("cancelled_ts", F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm"))
          .withColumn("event_day", F.date_format("cancelled_ts", "yyyy-MM-dd"))
          .withColumn("ingested_at", F.current_timestamp())
          .dropDuplicates(["cancellation_id"]))
    df.persist()
    rows = df.count()
    write_hudi(df, hudi_opts(
        "cancellations", "bronze",
        pk="cancellation_id",
        partition_field="event_day",
        precombine="ingested_at",
        column_stats_cols="event_day,reason,ingested_at",
        global_index=True,
    ))
    _write_watermark_rows(
        spark, "cancellations",
        extract_source_partitions_from_column(df, "event_day"),
        rows, batch_id,
    )
    df.unpersist()
    print(f"[cancel batch={batch_id}] rows={rows}")


def handle_rates(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of exchange rate events and write to Hudi.

    Adds an ingested_at timestamp and upserts into the bronze exchange_rates
    non-partitioned Hudi table, then records a watermark entry.

    Args:
        spark: Active SparkSession.
        batch_df: DataFrame containing raw exchange rate records for this batch.
        batch_id: Unique micro-batch identifier assigned by Structured Streaming.
    """
    if batch_df.rdd.isEmpty():
        print(f"[rates batch={batch_id}] empty")
        return
    df = batch_df.withColumn("ingested_at", F.current_timestamp())
    df.persist()
    rows = df.count()
    write_hudi(df, hudi_opts(
        "exchange_rates", "bronze",
        pk="update_id",
        partition_field="",
        precombine="timestamp",
        column_stats_cols="timestamp",
        enable_record_index=False,
    ))
    _write_watermark_rows(
        spark, "exchange_rates", ["__nonpartitioned__"], rows, batch_id,
    )
    df.unpersist()
    print(f"[rates batch={batch_id}] rows={rows}")


def start_file_stream(
    spark: SparkSession,
    *,
    name: str,
    path: str,
    schema: StructType | None,
    glob: str,
    handler,
    checkpoint: str,
    trigger_seconds: int = 30,
    max_files_per_trigger: int = 20,
    max_file_age: str = "7d",
):
    """Start a JSON file-based Structured Streaming query.

    Configures a readStream with glob filtering and file age limits, then launches
    a writeStream with the given foreachBatch handler and checkpoint location.

    Args:
        spark: Active SparkSession.
        name: Query name used in Spark UI and logs.
        path: S3a source path to read JSON files from.
        schema: Optional StructType to enforce on the JSON source.
        glob: Glob pattern passed to pathGlobFilter (e.g. ``"transactions.jsonl"``).
        handler: Callable ``(batch_df, batch_id)`` invoked for each micro-batch.
        checkpoint: S3a path for streaming checkpoint storage.
        trigger_seconds: Processing trigger interval in seconds.
        max_files_per_trigger: Maximum number of files to process per trigger.
        max_file_age: Maximum age of files to include (e.g. ``"7d"``).

    Returns:
        StreamingQuery: The running streaming query object.
    """
    reader = (spark.readStream
              .format("json")
              .option("pathGlobFilter", glob)
              .option("maxFilesPerTrigger", str(max_files_per_trigger))
              .option("latestFirst", "false")
              .option("maxFileAge", max_file_age)
              .option("recursiveFileLookup", "true"))
    if schema is not None:
        reader = reader.schema(schema)

    df = reader.load(path)

    query = (df.writeStream
             .queryName(name)
             .foreachBatch(handler)
             .option("checkpointLocation", checkpoint)
             .trigger(processingTime=f"{trigger_seconds} seconds")
             .start())
    print(f"[stream:{name}] started; src={path} ckpt={checkpoint}")
    return query


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for S3 source paths and checkpoint root.

    Each source (transactions, cancellations, rates) is read directly from
    the public YC bucket ``npl-de18-lab8-data``. Reference data is handled
    separately in ``bronze_reference_batch.py``.

    Returns:
        argparse.Namespace: Parsed arguments with source paths and checkpoint root.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--transactions-path", default="s3a://npl-de18-lab8-data/")
    p.add_argument("--cancellations-path", default="s3a://npl-de18-lab8-data/cancellations/")
    p.add_argument("--rates-path", default="s3a://npl-de18-lab8-data/exchange_rates/")
    p.add_argument("--ckpt-root", default="s3a://checkpoints/bronze-s3-stream")
    return p.parse_args()


def main() -> None:
    """Bootstrap the watermark table and start all three S3 file streams.

    Parses CLI arguments, creates a SparkSession, ensures the watermark table
    exists, and launches streaming queries for transactions, cancellations, and
    exchange rates. Blocks until any query terminates.
    """
    args = parse_args()

    spark = (SparkSession.builder
             .appName("bronze-s3-streaming")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    bootstrap_watermark_table(spark)

    queries = [
        start_file_stream(
            spark, name="transactions",
            path=args.transactions_path,
            schema=TX_SCHEMA,
            glob="transactions.jsonl",
            handler=partial(handle_transactions, spark),
            checkpoint=f"{args.ckpt_root}/transactions",
            trigger_seconds=60,
            max_files_per_trigger=50,
        ),
        start_file_stream(
            spark, name="cancellations",
            path=args.cancellations_path,
            schema=CANCEL_SCHEMA,
            glob="cancellations.jsonl",
            handler=partial(handle_cancellations, spark),
            checkpoint=f"{args.ckpt_root}/cancellations",
            trigger_seconds=30,
            max_files_per_trigger=40,
        ),
        start_file_stream(
            spark, name="rates",
            path=args.rates_path,
            schema=RATES_SCHEMA,
            glob="rates.jsonl",
            handler=partial(handle_rates, spark),
            checkpoint=f"{args.ckpt_root}/rates",
            trigger_seconds=30,
            max_files_per_trigger=30,
        ),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
