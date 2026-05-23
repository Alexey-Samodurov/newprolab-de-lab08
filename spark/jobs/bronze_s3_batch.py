"""Daily batch ingest for S3 bronze tables (transactions / cancellations / exchange_rates).

The job loads a single source partition ``day=<ds>`` from the public
upstream bucket, upserts it into the corresponding Hudi bronze table and
(for transactions only) publishes a watermark row that downstream dbt
gates on.

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

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

from utils.hudi import hudi_opts, write_hudi
from utils.log import get_logger
from utils.watermark import bootstrap_watermark_table, write_watermark


log = get_logger(__name__)

_DEFAULT_SRC_ROOT = "s3a://npl-de18-lab8-data"
_SOURCE_TRANSACTIONS = "transactions"
_SOURCE_CANCELLATIONS = "cancellations"
_SOURCE_RATES = "exchange_rates"

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
        path_template: Format string with a ``{src_root}`` and ``{ds}``
            placeholders that yields the absolute S3a glob to read.
        prepare: Transformation that adds bronze-specific derived
            columns (composite keys, ``event_day``, ``ingested_at``).
        hudi_options: Builder of Hudi writer options for the target.
        emits_watermark: Whether the job publishes a row into
            ``bronze.ingest_watermarks`` for this source.
    """

    name: str
    table: str
    schema: StructType
    path_template: str
    prepare: Callable[[DataFrame, str], DataFrame]
    hudi_options: Callable[[], dict]
    emits_watermark: bool
    read_options: dict = field(default_factory=dict)


def _ingested_at(ds: str):
    """Return a literal timestamp column representing the ingestion day.

    The bronze layer stamps every row with the logical date of the Airflow
    run that produced it. That makes backfills deterministic and keeps the
    downstream ``to_date(ingested_at) = run_date()`` filter (used for
    late-arriving cancellations) correct regardless of wall-clock skew.

    Args:
        ds: Airflow logical date in ``YYYY-MM-DD`` form.

    Returns:
        Spark column expression of type ``timestamp``.
    """
    return F.to_timestamp(F.lit(ds))


def _prepare_transactions(df: DataFrame, ds: str) -> DataFrame:
    """Add ``event_day``, ``composite_pk`` and ``ingested_at`` to transactions."""
    return df.select(
        "*",
        F.from_unixtime("created_at", "yyyy-MM-dd").alias("event_day"),
        F.concat_ws(
            "|",
            F.col("transaction_id").cast("string"),
            F.coalesce(F.col("created_at").cast("string"), F.lit("0")),
            F.coalesce(F.col("user_id").cast("string"), F.lit("0")),
        ).alias("composite_pk"),
        _ingested_at(ds).alias("ingested_at"),
    )


def _prepare_cancellations(df: DataFrame, ds: str) -> DataFrame:
    """Parse ``cancelled_at``, derive ``event_day`` / ``ingested_at`` and
    build a globally-unique ``cancellation_pk``.

    Upstream ``cancellation_id`` is only unique within a single daily file
    (it restarts from ``1`` every day), so it cannot be used as a Hudi
    record key. ``cancellation_pk`` is composed of the raw ``cancelled_at``
    string and the source file day (``src_day = ds``) plus the id; that
    keeps the key unique even when:

    * ``cancelled_at`` cannot be parsed (``cancelled_ts is NULL``) — the
      raw string still participates in the key;
    * the same numeric ``cancellation_id`` lands in two different daily
      files because per-file numbering restarts — ``src_day`` (file day)
      disambiguates them while ``event_day`` (derived from
      ``cancelled_ts``) is the partition that keeps late-arriving
      records grouped by their actual cancel day.

    ``event_day`` is the parsed cancel date, falling back to the file
    day when the timestamp cannot be parsed; this ensures partition
    keys are never ``NULL``.
    """
    parsed = df.select(
        "*",
        F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm").alias("cancelled_ts"),
    )
    return (
        parsed
        .withColumn(
            "event_day",
            F.coalesce(
                F.date_format("cancelled_ts", "yyyy-MM-dd"),
                F.lit(ds),
            ),
        )
        .withColumn("ingested_at", _ingested_at(ds))
        .withColumn("src_day", F.lit(ds))
        .withColumn(
            "cancellation_pk",
            F.concat_ws(
                "|",
                F.lit(ds),
                F.coalesce(F.col("cancelled_at"), F.lit("__nullts__")),
                F.coalesce(F.col("cancellation_id").cast("string"), F.lit("__nullid__")),
            ),
        )
    )


def _prepare_rates(df: DataFrame, ds: str) -> DataFrame:
    """Build composite ``rate_pk`` so re-sent updates keep history."""
    return df.select(
        "*",
        F.concat_ws(
            "|",
            F.col("update_id").cast("string"),
            F.coalesce(F.col("timestamp").cast("string"), F.lit("0")),
        ).alias("rate_pk"),
        _ingested_at(ds).alias("ingested_at"),
    )


def _tx_opts() -> dict:
    return hudi_opts(
        _SOURCE_TRANSACTIONS, "bronze",
        pk="composite_pk",
        partition_field="event_day",
        precombine="ingested_at",
    )


def _cancel_opts() -> dict:
    return hudi_opts(
        _SOURCE_CANCELLATIONS, "bronze",
        pk="cancellation_pk",
        partition_field="event_day",
        precombine="ingested_at",
    )


def _rates_opts() -> dict:
    return hudi_opts(
        _SOURCE_RATES, "bronze",
        pk="rate_pk",
        partition_field="",
        precombine="ingested_at",
        enable_record_index=False,
    )


SOURCE_SPECS: dict[str, SourceSpec] = {
    _SOURCE_TRANSACTIONS: SourceSpec(
        name=_SOURCE_TRANSACTIONS,
        table=_SOURCE_TRANSACTIONS,
        schema=_TX_SCHEMA,
        path_template="{src_root}/day={ds}",
        prepare=_prepare_transactions,
        hudi_options=_tx_opts,
        emits_watermark=True,
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
        emits_watermark=False,
    ),
    _SOURCE_RATES: SourceSpec(
        name=_SOURCE_RATES,
        table=_SOURCE_RATES,
        schema=_RATES_SCHEMA,
        path_template="{src_root}/exchange_rates/day={ds}/rates.jsonl",
        prepare=_prepare_rates,
        hudi_options=_rates_opts,
        emits_watermark=False,
    ),
}


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Probe S3 via Hadoop FS without triggering a Spark job.

    Uses cheap ``fs.exists`` for non-glob prefixes (single HEAD/LIST)
    and falls back to ``globStatus`` only when the path actually contains
    wildcard characters.
    """
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if any(ch in path for ch in "*?[{"):
        return bool(fs.globStatus(hpath))
    return bool(fs.exists(hpath))


def read_day(spark: SparkSession, spec: SourceSpec, path: str) -> DataFrame:
    """Read one day of a bronze source as a typed DataFrame.

    Args:
        spark: Active SparkSession.
        spec: Source spec describing schema and target.
        path: Absolute S3a path (prefix or glob) pointing to the day's files.

    Returns:
        DataFrame with the source schema. An empty DataFrame is returned
        when the path does not yet exist (legal upstream gap).
    """
    if not _path_exists(spark, path):
        log.warning("source=%s path=%s does not exist, treating as empty", spec.name, path)
        return spark.createDataFrame([], spec.schema)
    reader = spark.read.schema(spec.schema)
    for key, value in spec.read_options.items():
        reader = reader.option(key, value)
    return reader.json(path)


def ingest(spark: SparkSession, spec: SourceSpec, ds: str, src_root: str) -> int:
    """Run the full read → prepare → upsert → watermark pipeline.

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
    rows = raw.count()
    if rows == 0:
        log.info("source=%s ds=%s rows=0 — skip upsert", spec.name, ds)
        if spec.emits_watermark:
            _emit_watermark(spark, spec, ds, rows=0)
        return 0

    prepared = spec.prepare(raw, ds)
    write_hudi(prepared, spec.hudi_options())
    log.info("source=%s ds=%s rows=%s upserted", spec.name, ds, rows)

    if spec.emits_watermark:
        _emit_watermark(spark, spec, ds, rows=rows)
    return rows


def _emit_watermark(spark: SparkSession, spec: SourceSpec, ds: str, rows: int) -> None:
    """Publish a watermark row consumed by the downstream Airflow gate."""
    partition = f"day={ds}"
    write_watermark(
        spark,
        table_name=spec.table,
        partitions=[partition],
        rows_in_batch=rows,
        batch_id=int(ds.replace("-", "")),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments.

    Args:
        argv: Optional argument vector, defaults to ``sys.argv[1:]``.

    Returns:
        Parsed namespace with ``source``, ``ds`` and ``src_root``.
    """
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
    """Create a SparkSession with sane batch defaults and Hive sync."""
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
    """Entry point.

    Returns:
        ``0`` on success (including legal empty-day skip), ``1`` on
        unexpected errors that should fail the Airflow task.
    """
    args = parse_args(argv)
    spec = SOURCE_SPECS[args.source]

    spark = build_spark(f"bronze-s3-batch-{spec.name}-{args.ds}")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if spec.emits_watermark:
            bootstrap_watermark_table(spark)
        ingest(spark, spec, args.ds, args.src_root)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
