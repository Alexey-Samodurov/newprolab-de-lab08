"""Long-running Kafka stream ingest into bronze under partition-ownership.

The job owns the *open* partition of each bronze table:

* ``bronze.transactions``  — ``event_day``
* ``bronze.cancellations`` — ``ingestion_day``
* ``bronze.exchange_rates`` — unpartitioned (idempotent upsert by ``rate_pk``)

For every micro-batch it reads the latest ``producer='s3'`` watermark
(``read_s3_high_watermark``) and routes:

* records with ``partition_value > s3_high_watermark`` into bronze;
* records with ``partition_value <= s3_high_watermark`` into
  ``bronze_dlq.late_events`` (these belong to a partition the S3 writer
  has already finalised, so writing them into bronze would create a
  silent divergence with the canonical batch result).

``exchange_rates`` is not subject to the contract because both writers
emit byte-identical rows keyed by ``rate_pk`` (snapshot semantics, no
late-arriving) — Hudi upsert is idempotent.

The job also publishes a tiny ``producer='kafka'`` watermark per micro
batch carrying the last committed Kafka offset, used for human-level
recovery visibility.
"""

from __future__ import annotations

import os
import sys
from functools import partial

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

from utils.bronze_transforms import (
    prepare_cancellations,
    prepare_rates,
    prepare_transactions,
)
from utils.hudi import hudi_opts, write_hudi
from utils.log import get_logger
from utils.watermark import (
    PRODUCER_KAFKA,
    bootstrap_watermark_table,
    read_s3_high_watermark,
    write_watermark,
)


log = get_logger(__name__)


KAFKA_TRIGGER_SECONDS = 15
KAFKA_MAX_OFFSETS_PER_TRIGGER = 10000
DLQ_REASON_LATE = "event_day_le_s3_watermark"

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

_TX_RAW_COLS = [
    "transaction_id", "user_id", "user_uuid", "amount", "currency",
    "transaction_type", "promo_code_id", "status", "created_at",
]
_CANCEL_RAW_COLS = [
    "cancellation_id", "original_transaction_id", "reason",
    "cancelled_at", "refund_amount",
]
_RATES_RAW_COLS = [
    "update_id", "timestamp", "rate_tgrk_punk", "rate_tgrk_rub",
]


def _tx_opts() -> dict:
    return hudi_opts(
        "transactions", "bronze",
        pk="composite_pk",
        partition_field="event_day",
        precombine="ingested_at",
        index_type="SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


def _cancel_opts() -> dict:
    return hudi_opts(
        "cancellations", "bronze",
        pk="cancellation_pk",
        partition_field="ingestion_day",
        precombine="ingested_at",
        index_type="GLOBAL_SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


def _rates_opts() -> dict:
    return hudi_opts(
        "exchange_rates", "bronze",
        pk="rate_pk",
        partition_field="",
        precombine="ingested_at",
        enable_record_index=False,
        index_type="GLOBAL_SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


def _dlq_opts() -> dict:
    return hudi_opts(
        "late_events", "bronze_dlq",
        pk="dlq_pk",
        partition_field="ingestion_day",
        precombine="ingested_at",
        index_type="SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


def _route_by_watermark(
    df: DataFrame,
    *,
    partition_col: str,
    s3_high_watermark,
) -> tuple[DataFrame, DataFrame]:
    """Split a prepared bronze frame into (on_time, late) by S3 watermark.

    The boundary is *strict*: ``partition_value > watermark`` is on-time
    (Kafka owns it); ``<= watermark`` is late (S3 owns it, route to DLQ).
    A null watermark (no S3 run yet) keeps everything on-time.
    """
    if s3_high_watermark is None:
        empty = df.where(F.lit(False))
        return df, empty
    wm = F.to_date(F.lit(s3_high_watermark.isoformat()))
    on_time = df.where(F.to_date(F.col(partition_col)) > wm)
    late = df.where(F.to_date(F.col(partition_col)) <= wm)
    return on_time, late


def _write_dlq(
    df: DataFrame,
    *,
    source_table: str,
    pk_col: str,
    partition_col: str,
) -> int:
    """Persist late records into ``bronze_dlq.late_events``.

    Returns the row count for logging. The full original record is
    preserved as JSON in ``payload_json`` so a future replay can
    reconstruct it bit-for-bit.
    """
    if not df.take(1):
        return 0
    payload_cols = [c for c in df.columns if not c.startswith("_hoodie_")]
    dlq = (
        df.select(*payload_cols)
          .withColumn("payload_json", F.to_json(F.struct(*payload_cols)))
          .withColumn("dlq_pk",
                      F.concat_ws("|", F.lit(source_table), F.col(pk_col)))
          .withColumn("source_table", F.lit(source_table))
          .withColumn("reason", F.lit(DLQ_REASON_LATE))
          .withColumn("event_day_observed",
                      F.date_format(F.col(partition_col), "yyyy-MM-dd"))
          .withColumn("ingestion_day", F.to_date(F.current_timestamp()))
          .withColumn("ingested_at", F.current_timestamp())
          .select(
              "dlq_pk", "source_table", "reason", "payload_json",
              "event_day_observed", "ingestion_day", "ingested_at",
          )
    )
    write_hudi(dlq, _dlq_opts())
    n = dlq.count()
    log.warning("dlq: %s rows routed (source=%s reason=%s)",
                n, source_table, DLQ_REASON_LATE)
    return n


def _process_transactions(spark: SparkSession, batch_df: DataFrame) -> None:
    raw = batch_df.filter(F.col("_source") == "transaction").select(
        *_TX_RAW_COLS, "kafka_ts", "kafka_offset",
    )
    if not raw.take(1):
        return
    prepared = prepare_transactions(raw, F.current_timestamp())
    s3_wm = read_s3_high_watermark(spark, "transactions")
    on_time, late = _route_by_watermark(
        prepared, partition_col="event_day", s3_high_watermark=s3_wm,
    )
    write_hudi(on_time.drop("kafka_ts", "kafka_offset"), _tx_opts())
    _write_dlq(
        late, source_table="transactions",
        pk_col="composite_pk", partition_col="event_day",
    )


def _process_cancellations(spark: SparkSession, batch_df: DataFrame) -> None:
    raw = batch_df.filter(F.col("_source") == "cancellation").select(
        *_CANCEL_RAW_COLS, "kafka_ts", "kafka_offset",
    )
    if not raw.take(1):
        return
    prepared = prepare_cancellations(
        raw,
        ingested_at=F.current_timestamp(),
        ingestion_day=F.to_date(F.col("kafka_ts")),
    )
    s3_wm = read_s3_high_watermark(spark, "cancellations")
    on_time, late = _route_by_watermark(
        prepared, partition_col="ingestion_day", s3_high_watermark=s3_wm,
    )
    write_hudi(on_time.drop("kafka_ts", "kafka_offset"), _cancel_opts())
    _write_dlq(
        late, source_table="cancellations",
        pk_col="cancellation_pk", partition_col="ingestion_day",
    )


def _process_rates(spark: SparkSession, batch_df: DataFrame) -> None:
    raw = batch_df.filter(F.col("_source") == "exchange_rate").select(
        *_RATES_RAW_COLS, "kafka_ts", "kafka_offset",
    )
    if not raw.take(1):
        return
    prepared = prepare_rates(raw, F.current_timestamp())
    write_hudi(prepared.drop("kafka_ts", "kafka_offset"), _rates_opts())


def _emit_kafka_watermark(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """Publish a tiny ``producer='kafka'`` watermark with last offset.

    Informational only — used by humans for recovery visibility, not by
    any gate. One row per table per batch.
    """
    if not batch_df.take(1):
        return
    offsets = batch_df.agg(F.max("kafka_offset").alias("max_off")).collect()
    if not offsets or offsets[0]["max_off"] is None:
        return
    max_off = int(offsets[0]["max_off"])
    partition = f"offset={max_off}"
    write_watermark(
        spark,
        table_name="__kafka_batch__",
        partitions=[partition],
        rows_in_batch=0,
        batch_id=batch_id,
        producer=PRODUCER_KAFKA,
    )


def process_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch entry: route by ``_source`` and per-source contract."""
    batch_df.cache()
    try:
        _process_transactions(spark, batch_df)
        _process_cancellations(spark, batch_df)
        _process_rates(spark, batch_df)
        _emit_kafka_watermark(spark, batch_df, batch_id)
        log.info("batch=%s processed", batch_id)
    finally:
        batch_df.unpersist()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-kafka-ingest")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.hive.metastore.client.socket.timeout", "600s")
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> None:
    """Run the Kafka streaming ingest job.

    Raises:
        RuntimeError: If ``KAFKA_BOOTSTRAP_SERVERS`` is not configured.
    """
    bootstrap = (sys.argv[1] if len(sys.argv) > 1
                 else os.environ.get("KAFKA_BOOTSTRAP_SERVERS"))
    topic = (sys.argv[2] if len(sys.argv) > 2
             else os.environ.get("KAFKA_TOPIC", "lab08_transactions"))
    starting = sys.argv[3] if len(sys.argv) > 3 else "earliest"

    if not bootstrap:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS is not configured (neither argv nor env). "
            "Check Secret lab08-credentials and envSecretKeyRefs in SparkApplication."
        )

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    bootstrap_watermark_table(spark)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", starting)
           .option("maxOffsetsPerTrigger", str(KAFKA_MAX_OFFSETS_PER_TRIGGER))
           .option("failOnDataLoss", "false")
           .load())

    parsed = (
        raw
        .selectExpr(
            "CAST(value AS STRING) AS json",
            "timestamp AS kafka_ts",
            "offset AS kafka_offset",
        )
        .select(F.from_json("json", EVENT_SCHEMA).alias("e"),
                "kafka_ts", "kafka_offset")
        .select("e.*", "kafka_ts", "kafka_offset")
    )

    query = (
        parsed.writeStream
        .foreachBatch(partial(process_batch, spark))
        .option("checkpointLocation", "s3a://checkpoints/bronze-kafka")
        .trigger(processingTime=f"{KAFKA_TRIGGER_SECONDS} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
