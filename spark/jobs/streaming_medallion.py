"""Long-running NRT medallion driven by Hudi Incremental Source from bronze.

Subscribes to ``bronze.transactions`` and ``bronze.cancellations`` via
``readStream.format("hudi")`` (Hudi CDC) and writes the same gold tables
that the nightly dbt run materialises. The two writers never collide
on the same partition because the streaming job restricts itself to
``partition > s3_high_watermark`` (i.e. partitions still owned by the
Kafka writer per ADR-004) and dbt restricts itself to the closed days.

The gold tables share their Hudi primary keys with dbt models, so
Hudi's native upsert performs the MERGE — no explicit ``MERGE INTO``
SQL is required.

Aggregations implemented here (matching dbt models verbatim by schema):

* ``gold.transactions_by_hour_live``
* ``gold.purchases_by_hour_live``
* ``gold.cancellations_summary_live``
* ``gold.exchange_rates_latest`` — live FX snapshot (one row per pair,
  Hudi precombine на ``rate_ts``).

``gold.revenue_daily`` is left to the nightly dbt run because its
``forward+backward fill`` rate-pick logic is non-trivial and its
freshness target (daily) is met by batch alone.
"""

from __future__ import annotations

import os
import sys
from functools import partial

from pyspark.sql import DataFrame, SparkSession, functions as F

from utils.hudi import hudi_opts, write_hudi
from utils.log import get_logger
from utils.watermark import bootstrap_watermark_table, read_s3_high_watermark


log = get_logger(__name__)


STREAMING_TRIGGER_SECONDS = 30
_BRONZE_BASE = "s3a://lake/bronze"


def _broadcast_refs(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Load tiny reference tables as broadcast snapshots.

    Reads are batch (not streaming) and re-evaluated per micro-batch by
    callers, so reference updates land in the live aggregates within one
    trigger interval.
    """
    users = spark.read.table("bronze.users").select(
        "user_id",
        F.coalesce(F.col("is_test_user"), F.lit(False)).alias("u_is_test_user"),
    )
    test_users = spark.read.table("bronze.test_users").select(
        F.col("test_user_uuid"),
    )
    promo = spark.read.table("bronze.promo_codes").select(
        "promo_code_id", "expiry_date",
    )
    return users, test_users, promo


def _enrich_transactions(
    tx: DataFrame,
    users: DataFrame,
    test_users: DataFrame,
    promo: DataFrame,
) -> DataFrame:
    """Reproduce the silver/transactions_clean DQ flags for streaming.

    Only the columns consumed by the live gold aggregates are kept;
    full silver materialisation stays in dbt.
    """
    return (
        tx.alias("d")
        .join(F.broadcast(users.alias("u")), F.col("d.user_id") == F.col("u.user_id"), "left")
        .join(
            F.broadcast(test_users.alias("tu")),
            F.col("d.user_uuid") == F.col("tu.test_user_uuid"), "left",
        )
        .join(
            F.broadcast(promo.alias("pc")),
            F.col("d.promo_code_id") == F.col("pc.promo_code_id"), "left",
        )
        .select(
            F.col("d.composite_pk").alias("composite_pk"),
            F.col("d.transaction_id").alias("transaction_id"),
            F.col("d.amount").alias("amount"),
            F.col("d.currency").alias("currency"),
            F.col("d.transaction_type").alias("transaction_type"),
            F.col("d.status").alias("status"),
            F.col("d.created_at").alias("created_at"),
            F.to_timestamp(F.from_unixtime("d.created_at")).alias("created_ts"),
            F.col("d.event_day").alias("event_day"),
            F.col("d.ingested_at").alias("ingested_at"),
            (
                F.col("tu.test_user_uuid").isNotNull() | F.coalesce(F.col("u.u_is_test_user"), F.lit(False))
            ).alias("is_test_user"),
            (
                (F.col("d.status") == "completed") & (F.col("d.transaction_type") == "purchase")
            ).alias("is_revenue_eligible"),
            (
                F.col("d.amount").isNull() | (F.col("d.amount") <= 0)
            ).alias("is_amount_invalid"),
        )
    )


def _agg_transactions_by_hour(enriched: DataFrame) -> DataFrame:
    return (
        enriched.groupBy("event_day", F.hour("created_ts").alias("hour_of_day"), "is_test_user")
        .agg(
            F.count(F.lit(1)).alias("tx_cnt"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_cnt"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_cnt"),
            F.max("ingested_at").alias("updated_at"),
        )
        .withColumn(
            "pk",
            F.concat_ws(
                "_", F.col("event_day"),
                F.col("hour_of_day").cast("string"),
                F.col("is_test_user").cast("string"),
            ),
        )
        .select("pk", "event_day", "hour_of_day", "is_test_user",
                "tx_cnt", "completed_cnt", "failed_cnt", "updated_at")
    )


def _agg_purchases_by_hour(enriched: DataFrame) -> DataFrame:
    return (
        enriched
        .where(F.col("is_revenue_eligible") & ~F.col("is_test_user"))
        .groupBy("event_day", F.hour("created_ts").alias("hour_of_day"))
        .agg(
            F.count(F.lit(1)).alias("purchase_cnt"),
            F.sum("amount").alias("gross_amount_native"),
            F.max("ingested_at").alias("updated_at"),
        )
        .withColumn(
            "pk",
            F.concat_ws("_", F.col("event_day"), F.col("hour_of_day").cast("string")),
        )
        .select("pk", "event_day", "hour_of_day", "purchase_cnt",
                "gross_amount_native", "updated_at")
    )


def _process_transactions_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    _batch_id: int,
) -> None:
    """foreachBatch handler for the bronze.transactions incremental stream."""
    if not batch_df.take(1):
        return
    s3_wm = read_s3_high_watermark(spark, "transactions")
    open_df = (
        batch_df.where(F.to_date(F.col("event_day")) > F.to_date(F.lit(s3_wm.isoformat())))
        if s3_wm is not None else batch_df
    )
    if not open_df.take(1):
        return

    users, test_users, promo = _broadcast_refs(spark)
    enriched = _enrich_transactions(open_df, users, test_users, promo).cache()
    try:
        tx_hour = _agg_transactions_by_hour(enriched)
        write_hudi(
            tx_hour,
            hudi_opts(
                "transactions_by_hour_live", "gold",
                pk="pk", partition_field="",
                precombine="updated_at",
                enable_record_index=False,
                index_type="GLOBAL_SIMPLE",
                enable_metadata=False,
                multi_writer=False,
                enable_hive_sync=True,
            ),
        )

        purchases = _agg_purchases_by_hour(enriched)
        write_hudi(
            purchases,
            hudi_opts(
                "purchases_by_hour_live", "gold",
                pk="pk", partition_field="",
                precombine="updated_at",
                enable_record_index=False,
                index_type="GLOBAL_SIMPLE",
                enable_metadata=False,
                multi_writer=False,
                enable_hive_sync=True,
            ),
        )
        log.info("streaming.tx: tx_hour=%s purchases=%s",
                 tx_hour.count(), purchases.count())
    finally:
        enriched.unpersist()


def _process_rates_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    _batch_id: int,
) -> None:
    """Maintain ``gold.exchange_rates_latest`` — current FX snapshot.

    ``bronze.exchange_rates`` is unpartitioned (see ADR-004): both S3
    and Kafka writers upsert by ``rate_pk`` with idempotent semantics,
    so no watermark filter is needed here. Hudi's ``precombine`` on
    ``rate_ts`` keeps the newest rate per logical pair.
    """
    if not batch_df.take(1):
        return

    latest = batch_df.select(
        F.lit("tgrk").alias("pair"),
        F.col("timestamp").alias("rate_ts"),
        F.col("rate_tgrk_punk").alias("rate_tgrk_punk"),
        F.col("rate_tgrk_rub").alias("rate_tgrk_rub"),
        F.col("ingested_at").alias("updated_at"),
    )
    write_hudi(
        latest,
        hudi_opts(
            "exchange_rates_latest", "gold",
            pk="pair", partition_field="",
            precombine="rate_ts",
            enable_record_index=False,
            index_type="GLOBAL_SIMPLE",
            hoodie_table_name="exchange_rates_latest",
            enable_metadata=False,
            multi_writer=True,
        ),
    )
    log.info("streaming.rates: rows=%s", latest.count())


def _process_cancellations_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    _batch_id: int,
) -> None:
    """Lightweight live ``gold.cancellations_summary_live`` from streaming bronze.

    Skips the orphan-attribution metrics (which require a full join to
    ``silver.transactions_clean`` and would defeat the streaming
    latency goal). The nightly dbt model rewrites the same rows with
    the full attribution set; live readers see a strictly-smaller
    column set with zero placeholders that get filled in at T+1.
    """
    if not batch_df.take(1):
        return
    s3_wm = read_s3_high_watermark(spark, "cancellations")
    open_df = (
        batch_df.where(
            F.to_date(F.col("ingestion_day")) > F.to_date(F.lit(s3_wm.isoformat()))
        )
        if s3_wm is not None else batch_df
    )
    if not open_df.take(1):
        return

    agg = (
        open_df
        .withColumn("reason_norm", F.coalesce(F.col("reason"), F.lit("unknown")))
        .groupBy(F.col("event_day").alias("cancel_day"), "reason_norm")
        .agg(
            F.count(F.lit(1)).alias("cancellations_cnt"),
            F.sum(
                F.when(F.col("refund_amount").isNull() | (F.col("refund_amount") < 0), 1)
                .otherwise(0)
            ).alias("invalid_refund_cnt"),
            F.lit(0).cast("long").alias("orphan_cnt"),
            F.lit(0).cast("long").alias("ambiguous_attribution_cnt"),
            F.lit(None).cast("double").alias("avg_seconds_to_cancel"),
            F.lit(None).cast("double").alias("min_seconds_to_cancel"),
            F.lit(None).cast("double").alias("max_seconds_to_cancel"),
            F.sum(F.coalesce("refund_amount", F.lit(0.0))).alias("total_refund_amount"),
            F.max("ingested_at").alias("updated_at"),
        )
        .withColumnRenamed("reason_norm", "reason")
        .withColumn("pk", F.concat_ws("_", F.col("cancel_day"), F.col("reason")))
        .select(
            "pk", "cancel_day", "reason", "cancellations_cnt",
            "invalid_refund_cnt", "orphan_cnt", "ambiguous_attribution_cnt",
            "avg_seconds_to_cancel", "min_seconds_to_cancel",
            "max_seconds_to_cancel", "total_refund_amount", "updated_at",
        )
    )
    write_hudi(
        agg,
        hudi_opts(
            "cancellations_summary_live", "gold",
            pk="pk", partition_field="",
            precombine="updated_at",
            enable_record_index=False,
            index_type="GLOBAL_SIMPLE",
            enable_metadata=False,
            multi_writer=False,
            enable_hive_sync=True,
        ),
    )
    log.info("streaming.cancel: summary_rows=%s", agg.count())


def _latest_hudi_instant(spark: SparkSession, table_path: str) -> str:
    """Return the latest completed Hudi commit instant under ``table_path``.

    Used to start streaming sources at "now" instead of from history:
    batch DAGs (catchup=True) own everything that already exists in
    bronze; the stream takes over for what arrives next. Falls back to
    ``"earliest"`` if the table has no commits yet, so the first ever
    ``make up`` is still safe.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    base = jvm.org.apache.hadoop.fs.Path(table_path.rstrip("/") + "/.hoodie")
    fs = base.getFileSystem(hconf)
    if not fs.exists(base):
        return "earliest"
    latest = None
    for s in fs.listStatus(base):
        name = s.getPath().getName()
        for suffix in (".commit", ".deltacommit", ".replacecommit"):
            if name.endswith(suffix):
                ts = name[: -len(suffix)]
                if latest is None or ts > latest:
                    latest = ts
                break
    return latest or "earliest"


def _hudi_stream(spark: SparkSession, table: str) -> DataFrame:
    """Open a Hudi Incremental Source stream against a bronze table.

    Starts from the latest existing commit so the stream never replays
    history that batch jobs already own (ADR-004 partition-ownership).
    On checkpoint restart this option is ignored by Spark.
    """
    path = f"{_BRONZE_BASE}/{table}"
    start = _latest_hudi_instant(spark, path)
    log.info("hudi_stream %s start.offset=%s", table, start)
    return (
        spark.readStream.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.streaming.start.offset", start)
        .load(path)
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("streaming-medallion")
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
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    bootstrap_watermark_table(spark)

    checkpoint_root = os.environ.get(
        "STREAMING_MEDALLION_CHECKPOINTS", "s3a://checkpoints/streaming-medallion"
    )

    tx_query = (
        _hudi_stream(spark, "transactions")
        .writeStream
        .foreachBatch(partial(_process_transactions_batch, spark))
        .option("checkpointLocation", f"{checkpoint_root}/transactions")
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .start()
    )

    cancel_query = (
        _hudi_stream(spark, "cancellations")
        .writeStream
        .foreachBatch(partial(_process_cancellations_batch, spark))
        .option("checkpointLocation", f"{checkpoint_root}/cancellations")
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .start()
    )

    rates_query = (
        _hudi_stream(spark, "exchange_rates")
        .writeStream
        .foreachBatch(partial(_process_rates_batch, spark))
        .option("checkpointLocation", f"{checkpoint_root}/exchange_rates")
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .start()
    )

    log.info("streaming-medallion started (tx=%s cancel=%s rates=%s)",
             tx_query.id, cancel_query.id, rates_query.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    sys.exit(main() or 0)
