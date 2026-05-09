import argparse
from functools import partial

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)

from hudi_utils import hudi_opts, write_hudi


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

from watermark_utils import (
    bootstrap_watermark_table,
    extract_source_partitions_from_column,
    write_watermark as _write_watermark_rows,
)


def handle_transactions(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
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
    """CLI: каждый источник — свой путь.

    transactions / cancellations / rates читаются напрямую из публичного
    YC бакета `npl-de18-lab8-data`. Reference вынесен в bronze_reference_batch.py.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--transactions-path", default="s3a://npl-de18-lab8-data/")
    p.add_argument("--cancellations-path", default="s3a://npl-de18-lab8-data/cancellations/")
    p.add_argument("--rates-path", default="s3a://npl-de18-lab8-data/exchange_rates/")
    p.add_argument("--ckpt-root", default="s3a://checkpoints/bronze-s3-stream")
    return p.parse_args()


def main() -> None:
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
