import sys
import os
from functools import partial
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from hudi_utils import hudi_opts, write_hudi
from hudi_commit_meta import read_latest_commit, normalize_partitions
from log_utils import get_logger
from watermark_utils import bootstrap_watermark_table, write_watermark


log = get_logger(__name__)
_HUDI_BASE = "s3a://lake/bronze"
_LAST_INSTANT: dict[str, str | None] = {}


EVENT_SCHEMA = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("promo_code_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("cancellation_id", LongType(), True),
    StructField("original_transaction_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("cancelled_at", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("update_id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("rate_tgrk_punk", DoubleType(), True),
    StructField("rate_tgrk_rub", DoubleType(), True),
    StructField("_source", StringType(), True),
])


def _emit_transactions_watermark(spark, batch_id: int) -> None:
    """Watermark per Hudi-коммит для transactions (единственный consumer — Airflow).

    Данные берём из ``.hoodie/*.commit`` (Hadoop FS, без Spark job-а),
    никаких persist/count по микро-батчу.
    """
    table = "transactions_kafka"
    path = f"{_HUDI_BASE}/{table}"
    prev = _LAST_INSTANT.get(table)
    instant, raw_parts, rows = read_latest_commit(spark, path, prev)
    if instant is None or instant == prev or rows == 0:
        return
    _LAST_INSTANT[table] = instant
    parts = normalize_partitions(raw_parts)
    write_watermark(spark, "transactions", parts, rows, batch_id)


def process_batch(spark, batch_df, batch_id):
    """Route Kafka events to bronze Hudi tables without per-source persist/count.

    Ранее каждый sub-DataFrame (tx/cancel/rates) шёл через
    ``persist()`` + ``count()`` (полный скан) + повторный скан в write.
    Сейчас: один проход чтения, ``write_hudi`` сам через ``take(1)``
    отфильтровывает пустые батчи (один task вместо полного job-а),
    Hudi дедуплицирует по recordkey+precombine.
    """
    tx = batch_df.filter(F.col("_source") == "transaction").select(
        "*",
        F.from_unixtime("created_at", "yyyy-MM-dd").alias("event_day"),
        F.concat_ws(
            "|",
            F.col("transaction_id").cast("string"),
            F.coalesce(F.col("created_at").cast("string"), F.lit("0")),
            F.coalesce(F.col("user_id").cast("string"), F.lit("0")),
        ).alias("composite_pk"),
        F.current_timestamp().alias("ingested_at"),
    )
    write_hudi(tx, hudi_opts(
        "transactions", "bronze",
        pk="composite_pk", partition_field="event_day",
        precombine="ingested_at", table_suffix="_kafka",
    ))
    _emit_transactions_watermark(spark, batch_id)

    cancel = batch_df.filter(F.col("_source") == "cancellation").select(
        "*",
        F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm").alias("cancelled_ts"),
    ).select(
        "*",
        F.date_format("cancelled_ts", "yyyy-MM-dd").alias("event_day"),
        F.current_timestamp().alias("ingested_at"),
    )
    write_hudi(cancel, hudi_opts(
        "cancellations", "bronze",
        pk="cancellation_id", partition_field="event_day",
        precombine="ingested_at", table_suffix="_kafka",
        global_index=True,
    ))

    rates = batch_df.filter(F.col("_source") == "exchange_rate").select(
        "*",
        F.concat_ws(
            "|",
            F.col("update_id").cast("string"),
            F.coalesce(F.col("timestamp").cast("string"), F.lit("0")),
        ).alias("rate_pk"),
        F.current_timestamp().alias("ingested_at"),
    )
    write_hudi(rates, hudi_opts(
        "exchange_rates", "bronze",
        pk="rate_pk", partition_field="",
        precombine="ingested_at", table_suffix="_kafka",
        enable_record_index=False,
    ))

    log.info("batch=%s processed", batch_id)


def main():
    bootstrap = (sys.argv[1] if len(sys.argv) > 1
                 else os.environ.get("KAFKA_BOOTSTRAP_SERVERS"))
    topic = (sys.argv[2] if len(sys.argv) > 2
             else os.environ.get("KAFKA_TOPIC", "lab08_transactions"))
    starting = sys.argv[3] if len(sys.argv) > 3 else "earliest"

    if not bootstrap:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS не задан (ни через argv, ни через env). "
            "Проверь Secret lab08-credentials и envSecretKeyRefs в SparkApplication.")

    spark = (SparkSession.builder
             .appName("bronze-kafka-ingest")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    bootstrap_watermark_table(spark)
    instant, _, _ = read_latest_commit(spark, f"{_HUDI_BASE}/transactions_kafka", None)
    _LAST_INSTANT["transactions_kafka"] = instant

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", starting)
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts", "offset AS kafka_offset")
              .select(F.from_json("json", EVENT_SCHEMA).alias("e"), "kafka_ts", "kafka_offset")
              .select("e.*", "kafka_ts", "kafka_offset"))

    query = (parsed.writeStream
             .foreachBatch(partial(process_batch, spark))
             .option("checkpointLocation", "s3a://checkpoints/bronze-kafka")
             .trigger(processingTime="60 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
