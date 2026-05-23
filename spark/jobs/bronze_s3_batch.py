"""Daily batch ingest for S3 bronze tables (transactions / cancellations / exchange_rates).

The job is the **owner of closed-day partitions** under the ADR-004
partition-ownership contract. For each source it:

1. Reads a single ``day=<ds>`` slice from the public upstream bucket.
2. Reads the existing Kafka-residual rows in the same target partition
   (rows the streaming writer landed before the ownership boundary
   moved).
3. Unions both inputs and de-duplicates by primary key, with S3 winning
   on collisions (``source_priority=0`` vs ``1``).
4. Writes the merged result via Hudi ``insert_overwrite`` of just that
   partition — atomic ``replace_commit`` in the Hudi timeline.
5. Publishes a watermark row with ``producer='s3'``; the Kafka writer
   reads it to know which partitions have moved into S3 ownership and
   should route incoming events to DLQ instead.

Run as one source per Airflow task::

    python bronze_s3_batch.py --source transactions --ds 2026-05-01
    python bronze_s3_batch.py --source cancellations --ds 2026-05-01
    python bronze_s3_batch.py --source exchange_rates --ds 2026-05-01
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)
from pyspark.sql.utils import AnalysisException

from utils.bronze_transforms import (
    prepare_cancellations,
    prepare_rates,
    prepare_transactions,
)
from utils.hudi import hudi_opts, write_hudi
from utils.log import get_logger
from utils.watermark import PRODUCER_S3, bootstrap_watermark_table, write_watermark


log = get_logger(__name__)

_DEFAULT_SRC_ROOT = "s3a://npl-de18-lab8-data"
_SOURCE_TRANSACTIONS = "transactions"
_SOURCE_CANCELLATIONS = "cancellations"
_SOURCE_RATES = "exchange_rates"
_HUDI_BASE = "s3a://lake/bronze"

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
])

_CANCEL_SCHEMA = StructType([
    StructField("cancellation_id", LongType(), True),
    StructField("original_transaction_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("cancelled_at", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
])

_RATES_SCHEMA = StructType([
    StructField("update_id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("rate_tgrk_punk", DoubleType(), True),
    StructField("rate_tgrk_rub", DoubleType(), True),
])


@dataclass(frozen=True)
class SourceSpec:
    """Declarative spec for a bronze source.

    Attributes:
        name: CLI source identifier.
        table: Target Hudi table name under ``bronze``.
        schema: Schema enforced on the JSON reader.
        path_template: Format string with ``{src_root}`` and ``{ds}``
            placeholders that yields the absolute S3a glob to read.
        prepare: Transformation that adds bronze-specific derived
            columns (composite keys, partition column, ``ingested_at``).
        hudi_options: Builder of Hudi writer options for the target.
        pk: Primary key column name on the prepared DataFrame.
        partition_col: Hudi partition column on the prepared DataFrame
            (``""`` for non-partitioned sources).
    """

    name: str
    table: str
    schema: StructType
    path_template: str
    prepare: Callable[[DataFrame, str], DataFrame]
    hudi_options: Callable[[], dict]
    pk: str
    partition_col: str
    read_options: dict = field(default_factory=dict)


def _ingested_at(ds: str):
    """Return a literal timestamp column representing the ingestion day.

    The bronze layer stamps every batch row with the logical date of
    the Airflow run that produced it. That makes backfills deterministic
    and keeps downstream ``to_date(ingested_at) = run_date()`` filters
    correct regardless of wall-clock skew.
    """
    return F.to_timestamp(F.lit(ds))


def _prepare_transactions(df: DataFrame, ds: str) -> DataFrame:
    return prepare_transactions(df, _ingested_at(ds))


def _prepare_cancellations(df: DataFrame, ds: str) -> DataFrame:
    return prepare_cancellations(
        df,
        ingested_at=_ingested_at(ds),
        ingestion_day=F.to_date(F.lit(ds)),
    )


def _prepare_rates(df: DataFrame, ds: str) -> DataFrame:
    return prepare_rates(df, _ingested_at(ds))


def _tx_opts() -> dict:
    return hudi_opts(
        _SOURCE_TRANSACTIONS, "bronze",
        pk="composite_pk",
        partition_field="event_day",
        precombine="ingested_at",
        index_type="SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


def _cancel_opts() -> dict:
    return hudi_opts(
        _SOURCE_CANCELLATIONS, "bronze",
        pk="cancellation_pk",
        partition_field="ingestion_day",
        precombine="ingested_at",
        index_type="GLOBAL_SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


def _rates_opts() -> dict:
    return hudi_opts(
        _SOURCE_RATES, "bronze",
        pk="rate_pk",
        partition_field="",
        precombine="ingested_at",
        enable_record_index=False,
        index_type="GLOBAL_SIMPLE",
        enable_metadata=False,
        multi_writer=True,
    )


SOURCE_SPECS: dict[str, SourceSpec] = {
    _SOURCE_TRANSACTIONS: SourceSpec(
        name=_SOURCE_TRANSACTIONS,
        table=_SOURCE_TRANSACTIONS,
        schema=_TX_SCHEMA,
        path_template="{src_root}/day={ds}",
        prepare=_prepare_transactions,
        hudi_options=_tx_opts,
        pk="composite_pk",
        partition_col="event_day",
        read_options={
            "recursiveFileLookup": "true",
            "pathGlobFilter": "transactions.jsonl",
        },
    ),
    _SOURCE_CANCELLATIONS: SourceSpec(
        name=_SOURCE_CANCELLATIONS,
        table=_SOURCE_CANCELLATIONS,
        schema=_CANCEL_SCHEMA,
        path_template="{src_root}/cancellations/day={ds}/cancellations.jsonl",
        prepare=_prepare_cancellations,
        hudi_options=_cancel_opts,
        pk="cancellation_pk",
        partition_col="ingestion_day",
    ),
    _SOURCE_RATES: SourceSpec(
        name=_SOURCE_RATES,
        table=_SOURCE_RATES,
        schema=_RATES_SCHEMA,
        path_template="{src_root}/exchange_rates/day={ds}/rates.jsonl",
        prepare=_prepare_rates,
        hudi_options=_rates_opts,
        pk="rate_pk",
        partition_col="",
    ),
}


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Probe S3 via Hadoop FS without triggering a Spark job."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if any(ch in path for ch in "*?[{"):
        return bool(fs.globStatus(hpath))
    return bool(fs.exists(hpath))


def read_day(spark: SparkSession, spec: SourceSpec, path: str) -> DataFrame:
    """Read one day of a bronze source as a typed DataFrame.

    An empty DataFrame is returned when the path does not yet exist
    (legal upstream gap).
    """
    if not _path_exists(spark, path):
        log.warning("source=%s path=%s does not exist, treating as empty", spec.name, path)
        return spark.createDataFrame([], spec.schema)
    reader = spark.read.schema(spec.schema)
    for key, value in spec.read_options.items():
        reader = reader.option(key, value)
    return reader.json(path)


def _read_kafka_residual(
    spark: SparkSession,
    table: str,
    partition_col: str,
    partition_val: str,
    target_schema: list[str],
) -> DataFrame:
    """Read existing Kafka-written rows in the target partition.

    Returns an empty frame when the table or partition does not exist
    yet. Output columns are aligned to ``target_schema`` so
    ``unionByName`` is safe.
    """
    full = f"bronze.{table}"
    empty = spark.createDataFrame(
        [], spark.read.table(full).schema if spark.catalog.tableExists(full)
        else StructType([])
    )
    if not spark.catalog.tableExists(full):
        return empty
    try:
        df = spark.read.table(full)
        if partition_col:
            df = df.where(f"{partition_col} = '{partition_val}'")
        for c in df.columns:
            if c.startswith("_hoodie_"):
                df = df.drop(c)
        missing = [c for c in target_schema if c not in df.columns]
        for m in missing:
            df = df.withColumn(m, F.lit(None))
        return df.select(*target_schema)
    except AnalysisException as exc:
        log.warning("residual read failed for %s: %s", full, exc)
        return empty


def _cutover_merge(
    spark: SparkSession,
    spec: SourceSpec,
    prepared: DataFrame,
    partition_val: str,
) -> tuple[DataFrame, int, int]:
    """Union prepared S3 input with Kafka-residual rows and dedup by PK.

    S3 wins on collisions (``source_priority=0``); Kafka-only records
    survive (covers the case where the S3 generator dropped a slot but
    Kafka delivered the events).

    Args:
        spark: Active SparkSession.
        spec: Source spec.
        prepared: S3 input already enriched by ``spec.prepare``.
        partition_val: Partition value to merge (``YYYY-MM-DD``).

    Returns:
        Tuple ``(merged_df, n_s3, n_kafka_only)`` for logging.
    """
    target_cols = prepared.columns
    residual = _read_kafka_residual(
        spark, spec.table, spec.partition_col, partition_val, target_cols,
    )
    s3_df = prepared.withColumn("__src_pri", F.lit(0))
    kafka_df = residual.withColumn("__src_pri", F.lit(1))
    union = s3_df.unionByName(kafka_df, allowMissingColumns=False)
    w = Window.partitionBy(spec.pk).orderBy(
        F.col("__src_pri").asc(),
        F.col("ingested_at").desc(),
    )
    merged = (
        union.withColumn("__rn", F.row_number().over(w))
             .where("__rn = 1")
             .drop("__rn", "__src_pri")
    )
    return merged, s3_df.count(), kafka_df.count()


def ingest(spark: SparkSession, spec: SourceSpec, ds: str, src_root: str) -> int:
    """Run the full read → prepare → cutover-merge → upsert → watermark pipeline.

    Args:
        spark: Active SparkSession.
        spec: Source spec to process.
        ds: Logical date ``YYYY-MM-DD``.
        src_root: S3a prefix of the upstream bucket.

    Returns:
        Number of source rows processed (informational).
    """
    path = spec.path_template.format(src_root=src_root.rstrip("/"), ds=ds)
    log.info("source=%s ds=%s path=%s", spec.name, ds, path)

    raw = read_day(spark, spec, path)
    s3_rows = raw.count()
    prepared = spec.prepare(raw, ds)

    if spec.partition_col:
        merged, n_s3, n_kafka = _cutover_merge(spark, spec, prepared, ds)
        opts = spec.hudi_options()
        opts["hoodie.datasource.write.operation"] = "insert_overwrite"
        write_hudi(merged, opts)
        log.info(
            "source=%s ds=%s s3_rows=%s kafka_residual=%s merged_written=%s",
            spec.name, ds, n_s3, n_kafka, "insert_overwrite",
        )
    else:
        write_hudi(prepared, spec.hudi_options())
        log.info("source=%s ds=%s s3_rows=%s upserted (non-partitioned)", spec.name, ds, s3_rows)

    _emit_watermark(spark, spec, ds, rows=s3_rows)
    return s3_rows


def _emit_watermark(spark: SparkSession, spec: SourceSpec, ds: str, rows: int) -> None:
    """Publish the S3-producer watermark consumed by the Kafka writer
    and the downstream Airflow medallion gate."""
    partition = f"day={ds}"
    write_watermark(
        spark,
        table_name=spec.table,
        partitions=[partition],
        rows_in_batch=rows,
        batch_id=int(ds.replace("-", "")),
        producer=PRODUCER_S3,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", required=True, choices=sorted(SOURCE_SPECS.keys()),
        help="Bronze source to ingest.",
    )
    parser.add_argument(
        "--ds", required=True,
        help="Logical date YYYY-MM-DD (Airflow ds).",
    )
    parser.add_argument(
        "--src-root", default=_DEFAULT_SRC_ROOT,
        help="S3a root of the upstream bucket.",
    )
    return parser.parse_args(argv)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "256")
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")
        .config("spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads", "32")
        .config("spark.hadoop.fs.s3a.paging.maximum", "5000")
        .config("spark.hadoop.fs.s3a.threads.max", "64")
        .config("spark.hadoop.fs.s3a.connection.maximum", "128")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .config("spark.hadoop.hive.metastore.client.socket.timeout", "600s")
        .enableHiveSupport()
        .getOrCreate()
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec = SOURCE_SPECS[args.source]

    spark = build_spark(f"bronze-s3-batch-{spec.name}-{args.ds}")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bootstrap_watermark_table(spark)
        ingest(spark, spec, args.ds, args.src_root)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
