"""
Общие хелперы для записи в Hudi из Spark Structured Streaming.

Используются в bronze_s3_streaming.py и bronze_kafka_ingest.py.
Параметризуются через `table_suffix` чтобы Kafka-стрим писал в `*_kafka`
таблицы рядом с S3-стримом и не конкурировал по recordkey.

Конфигурация оптимизирована под dashboard read pattern (Trino + Superset
с time-grain=P1D и фильтрами по event_day / is_test_user) и streaming
write pattern (низкий TPS, COW, частые micro-batches):

  - `zstd` вместо gzip                  : −25–35% размер при том же CPU;
  - inline clustering каждые 4 коммита  : объединяет мелкие parquet'ы
                                          streaming-batch'ей в файлы 128MB;
  - агрессивный cleaner (retained=2)    : time-travel не используется,
                                          старые версии = мусор;
  - агрессивный archive (4/5 commits)   : `.hoodie/` timeline компактный →
                                          быстрее листинг;
  - column-stats index (metadata table) : Trino прунит файлы по min/max
                                          без открытия parquet (важно для
                                          фильтров `event_day >= ...`);
  - record-level index (для bronze)     : upsert находит файл по PK через
                                          HFile-индекс — быстрее чем BLOOM
                                          когда файлов много.
"""
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
    """
    full_table = f"{table}{table_suffix}"

    # Defaults derived from caller's intent (без явных значений — берём логичный fallback).
    if column_stats_cols is None:
        cs_cols = [c for c in (partition_field, precombine) if c]
        column_stats_cols = ",".join(cs_cols) if cs_cols else pk
    if cluster_sort_cols is None:
        cluster_sort_cols = partition_field or pk

    opts: dict[str, str] = {
        # --- identity / sync ---------------------------------------------
        "hoodie.table.name": f"{db}_{full_table}",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": pk,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partition_field or "",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.upsert.shuffle.parallelism": "4",
        "hoodie.insert.shuffle.parallelism": "4",
        "hoodie.datasource.hive_sync.enable": "true",
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

        # --- storage: меньше байт на S3 ----------------------------------
        # zstd обычно даёт на 25–35% меньший parquet чем gzip при том же CPU.
        # Trino/Spark читают zstd нативно.
        "hoodie.parquet.compression.codec": "zstd",
        "hoodie.parquet.max.file.size": str(128 * 1024 * 1024),
        "hoodie.parquet.small.file.limit": str(100 * 1024 * 1024),

        # --- cleaner: меньше «мусорных» версий ---------------------------
        # Time-travel не используется ни Trino, ни Superset — оставляем
        # ровно столько, сколько нужно для concurrent reader'ов.
        # NB: в Hudi 1.x ключи переименованы из hoodie.cleaner.* в hoodie.clean.*
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async.enabled": "true",
        "hoodie.clean.policy": "KEEP_LATEST_COMMITS",
        "hoodie.clean.commits.retained": "2",

        # --- archive: компактный .hoodie/ timeline -----------------------
        "hoodie.archive.automatic": "true",
        "hoodie.keep.min.commits": "4",
        "hoodie.keep.max.commits": "5",

        # --- inline clustering: меньше мелких файлов ---------------------
        # Streaming пишет много маленьких parquet'ов; раз в 4 коммита
        # схлопываем их в файлы 128MB, отсортированные по `cluster_sort_cols`.
        # Сортировка → tighter min/max → агрессивный file-skipping в Trino.
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.inline.max.commits": "4",
        "hoodie.clustering.plan.strategy.target.file.max.bytes": str(128 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.small.file.limit": str(100 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.sort.columns": cluster_sort_cols,

        # --- metadata table + column-stats index -------------------------
        # Column stats живут в metadata table → Trino читает их одним
        # parquet-ом и прунит файлы данных без листинга S3.
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.column.stats.column.list": column_stats_cols,

        # --- observability: Hudi metrics OFF ----------------------------
        # KNOWN ISSUE (Hudi 1.1.1): нативный PROMETHEUS reporter падает с
        # NoSuchMethodError в io.prometheus.client.dropwizard.DropwizardExports
        # при первом commit, потому что Hudi шейдит Dropwizard MetricRegistry
        # в org.apache.hudi.com.codahale.metrics.*, а стандартный
        # simpleclient_dropwizard с Maven Central линкуется на unshaded
        # com.codahale.metrics. Совместимый shaded jar Hudi на Maven Central
        # не публикует. PUSHGATEWAY требует разворачивания push-gw, JMX тоже
        # тянет JmxSink + javaagent. Поэтому Hudi metrics выключены, а
        # наблюдаемость build-ится на Spark DAGScheduler (PrometheusServlet
        # порт 4040) — см. lab08/OBSERVABILITY_PLAN.md → Known issues.
        "hoodie.metrics.on": "false",
    }

    # Record-level index: HFile в metadata, маппинг recordkey → fileId.
    # Для bronze с миллионами PK ускоряет upsert (vs default BLOOM).
    # NB: Hudi 1.1.1 имеет известный баг "File pruning with partitioned rli has not yet
    # been implemented" — RLI несовместим с partitioned tables на read path. Для
    # партиционированных таблиц форсим BLOOM (старый, но рабочий путь).
    if enable_record_index and not partition_field:
        opts["hoodie.index.type"] = "RECORD_INDEX"
        opts["hoodie.metadata.record.level.index.enable"] = "true"
    else:
        # BLOOM index — file-skipping через bloom-фильтры в footers; работает с
        # partitioned tables. Для bronze упорядочен по event_day → bloom попадание высокое.
        opts["hoodie.index.type"] = "BLOOM"

    return opts


def write_hudi(df: DataFrame, opts: dict) -> None:
    """Append-mode upsert; no-op for empty batches (avoid empty-commit churn)."""
    if df.rdd.isEmpty():
        return
    w = df.write.format("hudi")
    for k, v in opts.items():
        w = w.option(k, v)
    w.mode("append").save()
