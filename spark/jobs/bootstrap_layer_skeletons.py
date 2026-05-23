"""One-shot bootstrap of empty Hudi tables for bronze + DLQ.

Run before any writer job and before dbt the first time. Idempotent —
existing tables are left untouched. Creates exactly the physical
objects the ADR-004 partition-ownership contract requires:

* ``bronze.transactions``  (partitioned by ``event_day``)
* ``bronze.cancellations`` (partitioned by ``ingestion_day``)
* ``bronze.exchange_rates`` (unpartitioned)
* ``bronze_dlq.late_events`` (partitioned by ``ingestion_day``)
* ``bronze.ingest_watermarks_{transactions,cancellations,exchange_rates,kafka}`` (via ``bootstrap_watermark_table``)
* ``gold.{transactions_by_hour,purchases_by_hour,cancellations_summary}_live`` —
  пустые Hudi-placeholders, чтобы UNION ALL в Superset virtual datasets
  (ADR-004 §L/N) работал даже до первого commit-а стрима.

silver / settled-gold tables (``gold.transactions_by_hour`` etc.) are
created by dbt on its first run, so no
``CREATE TABLE IF NOT EXISTS`` is performed for them.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, LongType, StringType, StructField,
    StructType, TimestampType,
)

from utils.hudi import hudi_opts
from utils.log import get_logger
from utils.watermark import bootstrap_watermark_table


log = get_logger(__name__)


_TX_SCHEMA = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("promo_code_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("event_day", StringType(), False),
    StructField("composite_pk", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

_CANCEL_SCHEMA = StructType([
    StructField("cancellation_id", LongType(), True),
    StructField("original_transaction_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("cancelled_at", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("cancelled_ts", TimestampType(), True),
    StructField("ingestion_day", DateType(), False),
    StructField("event_day", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
    StructField("cancellation_pk", StringType(), False),
])

_RATES_SCHEMA = StructType([
    StructField("update_id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("rate_tgrk_punk", DoubleType(), True),
    StructField("rate_tgrk_rub", DoubleType(), True),
    StructField("rate_pk", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

_DLQ_SCHEMA = StructType([
    StructField("dlq_pk", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("reason", StringType(), False),
    StructField("payload_json", StringType(), True),
    StructField("event_day_observed", StringType(), True),
    StructField("ingestion_day", DateType(), False),
    StructField("ingested_at", TimestampType(), False),
])

_TX_BY_HOUR_LIVE_SCHEMA = StructType([
    StructField("pk", StringType(), False),
    StructField("event_day", StringType(), True),
    StructField("hour_of_day", LongType(), True),
    StructField("is_test_user", BooleanType(), True),
    StructField("tx_cnt", LongType(), True),
    StructField("completed_cnt", LongType(), True),
    StructField("failed_cnt", LongType(), True),
    StructField("updated_at", TimestampType(), True),
])

_PURCHASES_BY_HOUR_LIVE_SCHEMA = StructType([
    StructField("pk", StringType(), False),
    StructField("event_day", StringType(), True),
    StructField("hour_of_day", LongType(), True),
    StructField("purchase_cnt", LongType(), True),
    StructField("gross_amount_native", DoubleType(), True),
    StructField("updated_at", TimestampType(), True),
])

_CANCELLATIONS_SUMMARY_LIVE_SCHEMA = StructType([
    StructField("pk", StringType(), False),
    StructField("cancel_day", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("cancellations_cnt", LongType(), True),
    StructField("invalid_refund_cnt", LongType(), True),
    StructField("orphan_cnt", LongType(), True),
    StructField("ambiguous_attribution_cnt", LongType(), True),
    StructField("avg_seconds_to_cancel", DoubleType(), True),
    StructField("min_seconds_to_cancel", DoubleType(), True),
    StructField("max_seconds_to_cancel", DoubleType(), True),
    StructField("total_refund_amount", DoubleType(), True),
    StructField("updated_at", TimestampType(), True),
])


@dataclass(frozen=True)
class TableSpec:
    db: str
    table: str
    schema: StructType
    pk: str
    partition_field: str
    precombine: str
    index_type: str


_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        db="bronze", table="transactions", schema=_TX_SCHEMA,
        pk="composite_pk", partition_field="event_day",
        precombine="ingested_at", index_type="SIMPLE",
    ),
    TableSpec(
        db="bronze", table="cancellations", schema=_CANCEL_SCHEMA,
        pk="cancellation_pk", partition_field="ingestion_day",
        precombine="ingested_at", index_type="GLOBAL_SIMPLE",
    ),
    TableSpec(
        db="bronze", table="exchange_rates", schema=_RATES_SCHEMA,
        pk="rate_pk", partition_field="",
        precombine="ingested_at", index_type="GLOBAL_SIMPLE",
    ),
    TableSpec(
        db="bronze_dlq", table="late_events", schema=_DLQ_SCHEMA,
        pk="dlq_pk", partition_field="ingestion_day",
        precombine="ingested_at", index_type="SIMPLE",
    ),
    TableSpec(
        db="gold", table="transactions_by_hour_live",
        schema=_TX_BY_HOUR_LIVE_SCHEMA,
        pk="pk", partition_field="",
        precombine="updated_at", index_type="GLOBAL_SIMPLE",
    ),
    TableSpec(
        db="gold", table="purchases_by_hour_live",
        schema=_PURCHASES_BY_HOUR_LIVE_SCHEMA,
        pk="pk", partition_field="",
        precombine="updated_at", index_type="GLOBAL_SIMPLE",
    ),
    TableSpec(
        db="gold", table="cancellations_summary_live",
        schema=_CANCELLATIONS_SUMMARY_LIVE_SCHEMA,
        pk="pk", partition_field="",
        precombine="updated_at", index_type="GLOBAL_SIMPLE",
    ),
)


def _create_if_missing(spark: SparkSession, spec: TableSpec) -> None:
    """Create one empty Hudi table when it is not yet registered in HMS."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {spec.db}")
    full = f"{spec.db}.{spec.table}"
    if spark.catalog.tableExists(full):
        log.info("bootstrap: %s already exists, skip", full)
        return

    empty = spark.createDataFrame([], spec.schema)
    opts = hudi_opts(
        spec.table, spec.db,
        pk=spec.pk,
        partition_field=spec.partition_field,
        precombine=spec.precombine,
        enable_record_index=False,
        index_type=spec.index_type,
        shuffle_parallelism=1,
    )
    opts["hoodie.datasource.write.operation"] = "bulk_insert"
    writer = empty.write.format("hudi")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.mode("append").save()
    log.info(
        "bootstrap: %s created (index=%s partition=%r)",
        full, spec.index_type, spec.partition_field,
    )


def build_spark(app_name: str) -> SparkSession:
    """Create a SparkSession with sane batch defaults and Hive sync."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "2")
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> int:
    spark = build_spark("bootstrap-layer-skeletons")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bootstrap_watermark_table(spark)
        for spec in _SPECS:
            _create_if_missing(spark, spec)
        log.info("bootstrap: complete")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
