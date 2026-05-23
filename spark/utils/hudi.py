"""Hudi writer options, an append-mode upsert and ``.hoodie`` commit metadata readers."""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession


def hudi_opts(
    table: str,
    db: str,
    pk: str,
    partition_field: str,
    precombine: str,
    *,
    table_suffix: str = "",
    column_stats_cols: str | None = None,
    cluster_sort_cols: str | None = None,
    enable_record_index: bool = True,
    global_index: bool = False,
    shuffle_parallelism: int = 4,
    enable_column_stats: bool = False,
    enable_hive_sync: bool = True,
) -> dict:
    """Build Hudi writer options for a CoW table with HMS sync.

    Args:
        table: Logical table name (e.g. ``"transactions"``).
        db: Target database (e.g. ``"bronze"``).
        pk: Recordkey field.
        partition_field: Partition column, or ``""`` for non-partitioned.
        precombine: Precombine field (must monotonically increase per record).
        table_suffix: Suffix appended to the physical table name to avoid
            timeline clashes between concurrent writers.
        column_stats_cols: Comma-separated columns indexed in column-stats.
            Defaults to ``partition_field`` + ``precombine``.
        cluster_sort_cols: Comma-separated columns to sort by during
            clustering. Defaults to ``partition_field`` or ``pk``.
        enable_record_index: Enable Hudi RECORD_INDEX for fast upserts.
        global_index: Use GLOBAL_BLOOM index across all partitions. Needed
            when the recordkey can move across partitions between batches.
            Mutually exclusive with ``enable_record_index``.
        shuffle_parallelism: Parallelism for upsert/insert/bulk/delete shuffles.
        enable_column_stats: Toggle column-stats metadata index.
        enable_hive_sync: Toggle Hive Metastore sync.

    Returns:
        Hudi writer options ready for ``df.write.format("hudi")``.
    """
    full_table = f"{table}{table_suffix}"

    if column_stats_cols is None:
        cs_cols = [c for c in (partition_field, precombine) if c]
        column_stats_cols = ",".join(cs_cols) if cs_cols else pk
    if cluster_sort_cols is None:
        cluster_sort_cols = partition_field or pk

    opts: dict[str, str] = {
        "hoodie.table.name": f"{db}_{full_table}",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": pk,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partition_field or "",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.upsert.shuffle.parallelism": str(shuffle_parallelism),
        "hoodie.insert.shuffle.parallelism": str(shuffle_parallelism),
        "hoodie.bulkinsert.shuffle.parallelism": str(shuffle_parallelism),
        "hoodie.delete.shuffle.parallelism": str(shuffle_parallelism),
        "hoodie.datasource.hive_sync.enable": "true" if enable_hive_sync else "false",
        "hoodie.datasource.meta_sync.condition.sync": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.database": db,
        "hoodie.datasource.hive_sync.table": full_table,
        "hoodie.datasource.hive_sync.partition_fields": partition_field or "",
        "hoodie.datasource.hive_sync.partition_extractor_class": (
            "org.apache.hudi.hive.MultiPartKeysValueExtractor"
            if partition_field
            else "org.apache.hudi.hive.NonPartitionedExtractor"
        ),
        "path": f"s3a://lake/{db}/{full_table}",
        "hoodie.parquet.compression.codec": "zstd",
        "hoodie.parquet.max.file.size": str(128 * 1024 * 1024),
        "hoodie.parquet.small.file.limit": str(100 * 1024 * 1024),
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async.enabled": "true",
        "hoodie.clean.policy": "KEEP_LATEST_COMMITS",
        "hoodie.clean.commits.retained": "10",
        "hoodie.archive.automatic": "false",
        "hoodie.keep.min.commits": "30",
        "hoodie.keep.max.commits": "40",
        "hoodie.clustering.inline": "false",
        "hoodie.clustering.async.enabled": "false",
        "hoodie.clustering.async.max.commits": "4",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        "hoodie.cleaner.policy.failed.writes": "LAZY",
        "hoodie.clustering.plan.strategy.target.file.max.bytes": str(128 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.small.file.limit": str(100 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.sort.columns": cluster_sort_cols,
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.compact.max.delta.commits": "20",
        "hoodie.metadata.index.column.stats.enable": "true" if enable_column_stats else "false",
    }
    if enable_column_stats:
        opts["hoodie.metadata.index.column.stats.column.list"] = column_stats_cols

    if global_index:
        opts["hoodie.index.type"] = "GLOBAL_BLOOM"
        opts["hoodie.bloom.index.update.partition.path"] = "true"
    elif enable_record_index and not partition_field:
        opts["hoodie.index.type"] = "RECORD_INDEX"
        opts["hoodie.metadata.record.level.index.enable"] = "true"
    else:
        opts["hoodie.index.type"] = "BLOOM"

    return opts


def write_hudi(df: DataFrame, opts: dict) -> None:
    """Append-mode upsert that skips empty batches.

    Uses ``df.take(1)`` (single-task probe) instead of ``df.rdd.isEmpty()``
    (full RDD job) so empty micro-batches cost almost nothing.

    Args:
        df: DataFrame to write.
        opts: Hudi writer options.
    """
    if not df.take(1):
        return
    w = df.write.format("hudi")
    for k, v in opts.items():
        w = w.option(k, v)
    w.mode("append").save()


def reference_hudi_opts(table: str, db: str, pk: str) -> dict:
    """Build Hudi options for a non-partitioned reference snapshot table.

    Uses ``insert_overwrite_table`` for atomic full-snapshot rewrites and
    keeps the metadata footprint minimal (no RECORD_INDEX, tiny shuffle).

    Args:
        table: Logical Hudi table name.
        db: Target database.
        pk: Recordkey column.

    Returns:
        Hudi writer options dict.
    """
    opts = hudi_opts(
        table, db,
        pk=pk,
        partition_field="",
        precombine="ingested_at",
        column_stats_cols="ingested_at",
        enable_record_index=False,
        shuffle_parallelism=2,
    )
    opts["hoodie.datasource.write.operation"] = "insert_overwrite_table"
    return opts


def read_latest_commit(
    spark: SparkSession,
    hudi_path: str,
    prev_instant: str | None,
) -> tuple[str | None, list[str], int]:
    """Read the latest completed Hudi commit metadata from S3.

    Reads directly via Hadoop FS (no Spark job), so batch metrics are
    obtained without scanning data.

    Args:
        spark: Active SparkSession.
        hudi_path: Base path of the Hudi table.
        prev_instant: Previously observed instant; if it matches the
            latest, no new metadata is returned.

    Returns:
        Tuple ``(instant, partitions, rows_written)`` where ``partitions``
        contains raw ``partitionToWriteStats`` keys (e.g.
        ``"event_day=2024-01-01"`` or ``""``) and ``rows_written`` is the
        sum of ``numWrites + numUpdateWrites``. Returns
        ``(latest_or_None, [], 0)`` when nothing new is available.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    base = jvm.org.apache.hadoop.fs.Path(hudi_path.rstrip("/") + "/.hoodie")
    fs = base.getFileSystem(hconf)
    if not fs.exists(base):
        return None, [], 0

    statuses = fs.listStatus(base)
    latest_ts: str | None = None
    latest_path = None
    for s in statuses:
        name = s.getPath().getName()
        if name.endswith(".commit"):
            ts = name[: -len(".commit")]
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
                latest_path = s.getPath()

    if latest_ts is None or latest_ts == prev_instant:
        return latest_ts, [], 0

    in_ = fs.open(latest_path)
    try:
        baos = jvm.java.io.ByteArrayOutputStream()
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(in_, baos, 4096, False)
        content = baos.toString("UTF-8")
    finally:
        in_.close()

    md = json.loads(content)
    p2s = md.get("partitionToWriteStats") or {}
    parts = list(p2s.keys())
    rows = 0
    for stats in p2s.values():
        for st in stats or []:
            rows += int(st.get("numWrites") or 0)
            rows += int(st.get("numUpdateWrites") or 0)
    return latest_ts, parts, rows


def normalize_partitions(
    raw_parts: list[str],
    *,
    nonpartitioned_marker: str = "__nonpartitioned__",
    out_prefix: str = "day=",
) -> list[str]:
    """Normalize raw Hudi partition keys to the watermark's ``day=<value>`` format.

    Hudi with ``hive_style_partitioning=true`` emits keys like
    ``event_day=2024-01-01``; the watermark table historically stores them
    as ``day=<value>``. This strips the column name and prepends a fixed
    prefix to preserve backward compatibility.

    Args:
        raw_parts: Raw partition keys from ``partitionToWriteStats``.
        nonpartitioned_marker: Marker used when a key is empty.
        out_prefix: Prefix prepended to extracted partition values.

    Returns:
        Sorted list of normalized partition keys.
    """
    out: set[str] = set()
    for p in raw_parts:
        if not p:
            out.add(nonpartitioned_marker)
            continue
        value = p.split("=", 1)[1] if "=" in p else p
        out.add(f"{out_prefix}{value}")
    return sorted(out)
