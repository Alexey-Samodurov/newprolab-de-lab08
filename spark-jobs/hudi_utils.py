from __future__ import annotations

from pyspark.sql import DataFrame


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
        table: logical table name (e.g. "transactions").
        db: target database (e.g. "bronze").
        pk: recordkey field.
        partition_field: partition column (or "" for non-partitioned).
        precombine: precombine field (must monotonically increase per record).
        table_suffix: appended to physical table name (e.g. "_kafka") to keep
            multiple writers from clashing on the same Hudi timeline.
        column_stats_cols: comma-separated list of columns to index in
            metadata column-stats. Defaults to partition_field + precombine
            if not given. Smaller list = compact index = faster query plan.
        cluster_sort_cols: comma-separated list of columns to sort by when
            clustering. Defaults to partition_field (if set) else pk —
            sorted files have tighter min/max → better predicate pushdown.
        enable_record_index: turn on Hudi RECORD_INDEX (HFile per recordkey
            in metadata table). Speeds up upsert when number of files
            grows. Disable for tiny / append-only tables.
        global_index: use a GLOBAL_BLOOM index that scans ALL partitions for
            the recordkey instead of only the incoming row's partition. Required
            when partition path is NOT a deterministic function of the recordkey
            (e.g. cancellations partitioned by event_day derived from
            cancelled_ts: the same cancellation_id can land in different
            event_day partitions across micro-batches → partition-scoped BLOOM
            misses the existing record and produces cross-partition duplicates).
            Pairs with `hoodie.bloom.index.update.partition.path=true` so Hudi
            relocates the record into the new partition instead of inserting
            a second copy. Mutually exclusive with RECORD_INDEX.

    Returns:
        dict: Hudi writer options ready to be passed to ``df.write.format("hudi")``.
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
        "hoodie.keep.min.commits": "20",
        "hoodie.keep.max.commits": "30",
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
    """Append-mode upsert; no-op for empty batches (avoid empty-commit churn).

    ``df.take(1)`` дешевле, чем ``df.rdd.isEmpty()``: запускает один task
    на одной партиции и сразу останавливается, тогда как ``isEmpty`` через
    RDD триггерит полноценный Spark job на каждый микро-батч.
    """
    if not df.take(1):
        return
    w = df.write.format("hudi")
    for k, v in opts.items():
        w = w.option(k, v)
    w.mode("append").save()


def reference_hudi_opts(table: str, db: str, pk: str) -> dict:
    """
    Generates Hudi configuration options for referencing specific Hudi tables.

    This function constructs a dictionary of options required to configure and
    interact with a Hudi table. The provided arguments specify key attributes
    of the target Hudi table, while additional settings are predefined for
    insert overwrite operations.

    Args:
        table (str): The name of the Hudi table.
        db (str): The name of the database containing the Hudi table.
        pk (str): The primary key column of the Hudi table.

    Returns:
        dict: A dictionary containing the Hudi table configuration options.
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
