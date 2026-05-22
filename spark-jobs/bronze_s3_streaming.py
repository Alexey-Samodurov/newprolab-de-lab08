import argparse
from functools import partial

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)

from hudi_utils import hudi_opts, write_hudi
from hudi_commit_meta import read_latest_commit, normalize_partitions
from log_utils import get_logger
from watermark_utils import bootstrap_watermark_table, write_watermark


log = get_logger(__name__)
_HUDI_BASE = "s3a://lake/bronze"
_LAST_INSTANT: dict[str, str | None] = {}


def _init_last_instant(spark: SparkSession, table: str) -> None:
    """Подтянуть текущий latest commit при старте.

    Без этого первый микро-батч после рестарта повторно отправит watermark
    для уже зафиксированного коммита.
    """
    instant, _, _ = read_latest_commit(spark, f"{_HUDI_BASE}/{table}", None)
    _LAST_INSTANT[table] = instant


def _emit_transactions_watermark(spark: SparkSession, batch_id: int) -> None:
    """Single consumer of watermark table — Airflow sensor for transactions.

    Метрики берём из Hudi commit-метаданных (Hadoop FS, без Spark job-а),
    никаких persist/count/distinct по батчу.
    """
    table = "transactions"
    path = f"{_HUDI_BASE}/{table}"
    prev = _LAST_INSTANT.get(table)
    instant, raw_parts, rows = read_latest_commit(spark, path, prev)
    if instant is None or instant == prev or rows == 0:
        return
    _LAST_INSTANT[table] = instant
    parts = normalize_partitions(raw_parts)
    write_watermark(spark, table, parts, rows, batch_id)
    log.info("table=%s batch=%s instant=%s rows=%s parts=%s",
             table, batch_id, instant, rows, len(parts))


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
    """Process a micro-batch of transactions and upsert into bronze Hudi.

    Все преобразования собраны в один ``select`` (вместо цепочки
    ``withColumn``), а event_day достаётся напрямую через ``from_unixtime``
    с форматом, без промежуточного timestamp-каста.
    """
    if "day" in batch_df.columns:
        event_day_expr = F.coalesce(
            F.col("day"),
            F.from_unixtime("created_at", "yyyy-MM-dd"),
        )
    else:
        event_day_expr = F.from_unixtime("created_at", "yyyy-MM-dd")

    df = batch_df.select(
        "*",
        event_day_expr.alias("event_day"),
        F.concat_ws(
            "|",
            F.col("transaction_id").cast("string"),
            F.coalesce(F.col("created_at").cast("string"), F.lit("0")),
            F.coalesce(F.col("user_id").cast("string"), F.lit("0")),
        ).alias("composite_pk"),
        F.current_timestamp().alias("ingested_at"),
    )
    write_hudi(df, hudi_opts(
        "transactions", "bronze",
        pk="composite_pk",
        partition_field="event_day",
        precombine="ingested_at",
    ))
    _emit_transactions_watermark(spark, batch_id)


def handle_cancellations(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of cancellations and upsert into bronze Hudi.

    Hudi сам дедуплицирует по recordkey + precombine, поэтому ранее
    использованный ``dropDuplicates`` (полный shuffle на каждый батч)
    убран. Watermark не пишем — downstream его не читает.
    """
    df = batch_df.select(
        "*",
        F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm").alias("cancelled_ts"),
    ).select(
        "*",
        F.date_format("cancelled_ts", "yyyy-MM-dd").alias("event_day"),
        F.current_timestamp().alias("ingested_at"),
    )
    write_hudi(df, hudi_opts(
        "cancellations", "bronze",
        pk="cancellation_id",
        partition_field="event_day",
        precombine="ingested_at",
        global_index=True,
    ))


def handle_rates(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of exchange rates and upsert into bronze Hudi.

    PK сделан композитным (``update_id|timestamp``), чтобы даже при
    повторной отправке апстримом одного и того же ``update_id`` с другим
    ``timestamp`` мы сохраняли историю котировок, а не затирали её через
    precombine. Производное поле ``rate_day`` выводится в silver
    (``exchange_rates_daily``) — в bronze оставляем только сырые поля.
    """
    df = batch_df.select(
        "*",
        F.concat_ws(
            "|",
            F.col("update_id").cast("string"),
            F.coalesce(F.col("timestamp").cast("string"), F.lit("0")),
        ).alias("rate_pk"),
        F.current_timestamp().alias("ingested_at"),
    )
    write_hudi(df, hudi_opts(
        "exchange_rates", "bronze",
        pk="rate_pk",
        partition_field="",
        precombine="ingested_at",
        enable_record_index=False,
    ))


def start_file_stream(
    spark: SparkSession,
    *,
    name: str,
    path: str,
    schema: StructType | None,
    handler,
    checkpoint: str,
    trigger_seconds: int = 30,
    max_files_per_trigger: int = 20,
    max_file_age: str = "2d",
    glob: str | None = None,
    recursive: bool = False,
):
    """Start a JSON file-based Structured Streaming query."""
    reader = (spark.readStream
              .format("json")
              .option("maxFilesPerTrigger", str(max_files_per_trigger))
              .option("maxFileAge", max_file_age)
              .option("recursiveFileLookup", "true" if recursive else "false"))
    if glob is not None:
        reader = reader.option("pathGlobFilter", glob)
    if schema is not None:
        reader = reader.schema(schema)

    df = reader.load(path)

    query = (df.writeStream
             .queryName(name)
             .foreachBatch(handler)
             .option("checkpointLocation", checkpoint)
             .trigger(processingTime=f"{trigger_seconds} seconds")
             .start())
    log.info("stream=%s started src=%s ckpt=%s", name, path, checkpoint)
    return query


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--transactions-path",
        default="s3a://npl-de18-lab8-data/day=*/slot=*/",
    )
    p.add_argument(
        "--cancellations-path",
        default="s3a://npl-de18-lab8-data/cancellations/",
    )
    p.add_argument(
        "--rates-path",
        default="s3a://npl-de18-lab8-data/exchange_rates/",
    )
    p.add_argument("--ckpt-root", default="s3a://checkpoints/bronze-s3-stream")
    return p.parse_args()


def main() -> None:
    """Bootstrap watermark table and start S3 file streams."""
    args = parse_args()

    spark = (SparkSession.builder
             .appName("bronze-s3-streaming")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .config("spark.scheduler.mode", "FAIR")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "default")

    bootstrap_watermark_table(spark)
    _init_last_instant(spark, "transactions")

    queries = [
        start_file_stream(
            spark, name="transactions",
            path=args.transactions_path,
            schema=TX_SCHEMA,
            glob="transactions.jsonl",
            handler=partial(handle_transactions, spark),
            checkpoint=f"{args.ckpt_root}/transactions",
            trigger_seconds=60,
            max_files_per_trigger=25,
        ),
        start_file_stream(
            spark, name="cancellations",
            path=args.cancellations_path,
            schema=CANCEL_SCHEMA,
            glob="cancellations.jsonl",
            handler=partial(handle_cancellations, spark),
            checkpoint=f"{args.ckpt_root}/cancellations",
            trigger_seconds=600,
            max_files_per_trigger=30,
        ),
        start_file_stream(
            spark, name="rates",
            path=args.rates_path,
            schema=RATES_SCHEMA,
            glob="rates.jsonl",
            handler=partial(handle_rates, spark),
            checkpoint=f"{args.ckpt_root}/rates",
            trigger_seconds=600,
            max_files_per_trigger=30,
        ),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
