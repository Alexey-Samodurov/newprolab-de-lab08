from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Iterable, Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from utils.hudi import hudi_opts, write_hudi
from utils.log import get_logger


log = get_logger(__name__)
WATERMARK_SCHEMA = StructType([
    StructField("watermark_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("source_partition", StringType(), False),
    StructField("rows_in_batch", LongType(), True),
    StructField("committed_at", StringType(), False),
])

_WATERMARK_DB = "bronze"
_WATERMARK_TABLE_PREFIX = "ingest_watermarks"
_KAFKA_TABLE = f"{_WATERMARK_TABLE_PREFIX}_kafka"
_S3_SOURCES: tuple[str, ...] = ("transactions", "cancellations", "exchange_rates")
_BOOTSTRAPPED: set[str] = set()

PRODUCER_S3 = "s3"
PRODUCER_KAFKA = "kafka"
_VALID_PRODUCERS = frozenset({PRODUCER_S3, PRODUCER_KAFKA})


def _s3_table_for(table_name: str) -> str:
    """Return the physical Hudi table that holds S3 watermarks for ``table_name``."""
    return f"{_WATERMARK_TABLE_PREFIX}_{table_name}"


def _physical_table(producer: str, table_name: str) -> str:
    """Resolve a (producer, source) pair to its physical watermark Hudi table.

    Watermarks are sharded one Hudi table per writer to make every table
    a single-writer surface: Airflow's three bronze tasks own one table
    each (S3 producer), and the streaming job owns one shared Kafka table.
    This removes timeline-level races between independent SparkApplications
    that previously led to lost commits in the unified ``ingest_watermarks``
    table.
    """
    if producer == PRODUCER_KAFKA:
        return _KAFKA_TABLE
    if producer == PRODUCER_S3:
        if table_name not in _S3_SOURCES:
            raise ValueError(
                f"unknown S3 watermark source {table_name!r}, "
                f"expected one of {_S3_SOURCES}"
            )
        return _s3_table_for(table_name)
    raise ValueError(
        f"producer must be one of {sorted(_VALID_PRODUCERS)}, got {producer!r}"
    )


def _watermark_hudi_opts(table: str) -> dict:
    """Build minimal Hudi options for a per-writer watermark table.

    Each shard carries a tiny marker row per partition for downstream
    gating. Expensive features are disabled (metadata table, column
    stats, clustering, multi-shuffle); Hive sync stays on so HMS sees
    each shard as a regular Hudi table. The table is intentionally
    *non-partitioned*: a single writer plus tiny row volume make file
    listing cheap, while keeping the Hudi timeline trivially flat.

    Returns:
        Hudi writer options dict.
    """
    opts = hudi_opts(
        table, _WATERMARK_DB,
        pk="watermark_id",
        partition_field="",
        precombine="committed_at",
        column_stats_cols="committed_at",
        enable_record_index=False,
        shuffle_parallelism=1,
        enable_column_stats=False,
        enable_hive_sync=True,
    )
    opts.update({
        "hoodie.metadata.enable": "false",
        "hoodie.metadata.index.column.stats.enable": "false",
        "hoodie.clustering.inline": "false",
        "hoodie.clustering.async.enabled": "false",
        "hoodie.write.concurrency.mode": "single_writer",
        "hoodie.cleaner.policy.failed.writes": "LAZY",
    })
    return opts


def write_watermark(
    spark: SparkSession,
    table_name: str,
    partitions: Iterable[str],
    rows_in_batch: int,
    batch_id: int,
    *,
    producer: str = PRODUCER_S3,
) -> None:
    """Emit one watermark row per partition for downstream gating.

    ``rows_in_batch`` is informational only; the source-of-truth metric
    comes from Hudi commit metadata, so no extra count is triggered.

    Args:
        spark: Active SparkSession.
        table_name: Logical table the watermark belongs to.
        partitions: Partition keys that received data in this batch.
        rows_in_batch: Best-effort row count reported by the caller.
        batch_id: Micro-batch identifier, logged for traceability.
        producer: Writer identity (``"s3"`` or ``"kafka"``). Encoded
            inside ``watermark_id`` to keep the on-disk schema stable
            while allowing downstream consumers to filter by writer.
    """
    if producer not in _VALID_PRODUCERS:
        raise ValueError(
            f"producer must be one of {sorted(_VALID_PRODUCERS)}, got {producer!r}"
        )
    parts = list(partitions)
    if not parts:
        return
    physical = _physical_table(producer, table_name)
    committed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    rows = [
        Row(
            watermark_id=f"{table_name}|{producer}|{p}",
            table_name=table_name,
            source_partition=p,
            rows_in_batch=int(rows_in_batch),
            committed_at=committed_at,
        )
        for p in parts
    ]
    df = spark.createDataFrame(rows, schema=WATERMARK_SCHEMA)
    write_hudi(df, _watermark_hudi_opts(physical))
    log.info(
        "watermark table=%s producer=%s shard=%s batch=%s partitions=%s",
        table_name, producer, physical, batch_id, parts,
    )


def read_s3_high_watermark(
    spark: SparkSession,
    table_name: str,
) -> Optional[date]:
    """Return latest ``day=<YYYY-MM-DD>`` committed by the S3 producer.

    Reads the per-source shard (``ingest_watermarks_<table_name>``) so
    the scan is naturally pruned to a single writer's data. Returns
    ``None`` when no S3 watermark has been published yet (cold-start,
    before first daily run).

    Args:
        spark: Active SparkSession.
        table_name: Logical table name (``transactions``,
            ``cancellations``, ``exchange_rates``).

    Returns:
        Latest closed day as :class:`datetime.date` or ``None``.
    """
    physical = _physical_table(PRODUCER_S3, table_name)
    full = f"{_WATERMARK_DB}.{physical}"
    if not spark.catalog.tableExists(full):
        return None
    rows = (
        spark.read.format("hudi")
             .load(f"s3a://lake/{_WATERMARK_DB}/{physical}")
             .where(f"watermark_id LIKE '{table_name}|{PRODUCER_S3}|day=%'")
             .selectExpr(
                 "regexp_extract(source_partition, '^day=(\\\\d{4}-\\\\d{2}-\\\\d{2})$', 1) AS d"
             )
             .where("d <> ''")
             .agg({"d": "max"})
             .collect()
    )
    if not rows or rows[0][0] is None:
        return None
    return datetime.strptime(rows[0][0], "%Y-%m-%d").date()


def bootstrap_watermark_table(spark: SparkSession) -> None:
    """Create every watermark shard once per process if missing.

    Iterates over the four physical shards (three S3-producer shards,
    one Kafka-producer shard) and writes a sentinel bootstrap row to
    any that does not yet exist in HMS. Subsequent calls in the same
    process are no-ops.

    Args:
        spark: Active SparkSession.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_WATERMARK_DB}")
    shards = [_KAFKA_TABLE] + [_s3_table_for(s) for s in _S3_SOURCES]
    for shard in shards:
        if shard in _BOOTSTRAPPED:
            continue
        full_name = f"{_WATERMARK_DB}.{shard}"
        try:
            if spark.catalog.tableExists(full_name):
                _BOOTSTRAPPED.add(shard)
                log.info("bootstrap: %s already exists, skip", full_name)
                continue
        except Exception:
            pass

        sentinel = spark.createDataFrame(
            [Row(
                watermark_id=f"__bootstrap__|__init__|{shard}",
                table_name="__bootstrap__",
                source_partition="__init__",
                rows_in_batch=0,
                committed_at="1970-01-01T00:00:00",
            )],
            schema=WATERMARK_SCHEMA,
        )
        write_hudi(sentinel, _watermark_hudi_opts(shard))
        _BOOTSTRAPPED.add(shard)
        log.info("bootstrap: %s created", full_name)
