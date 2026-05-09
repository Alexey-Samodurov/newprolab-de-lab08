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
    global_index: bool = False,
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
        "hoodie.upsert.shuffle.parallelism": "16",
        "hoodie.insert.shuffle.parallelism": "16",
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
        # ВАЖНО: archival ВЫКЛЮЧЕН глобально из-за известного бага
        # trinodb/trino#26458 + apache/hudi#13994. В Hudi table version 8
        # (default в 1.x) архивный timeline пишется как parquet в
        # `.hoodie/timeline/history/`, а Trino-Hudi connector не умеет
        # читать parquet timeline (TrinoHudiFileReaderFactory кидает
        # `UnsupportedOperationException: ... does not support Parquet`).
        # Симптом: любая bronze-таблица перестаёт читаться из Trino как
        # только у неё накопится >keep.max коммитов и сработает archive.
        # Workaround: archive выключен → timeline растёт линейно по числу
        # коммитов, но cleaner всё равно физически удаляет старые data-
        # файлы (см. clean.commits.retained=2 выше), так что место на S3
        # не утекает. `.hoodie/` слегка раздут, но листинг при column-stats
        # index всё равно не нужен. Включить обратно после релиза Hudi
        # с фиксом + апгрейда trino-hudi.
        "hoodie.archive.automatic": "false",
        "hoodie.keep.min.commits": "4",
        "hoodie.keep.max.commits": "5",

        # --- async clustering: меньше мелких файлов БЕЗ блокировки commit -
        # Раньше было `clustering.inline=true` — clustering выполнялся
        # синхронно внутри commit'а раз в 4 коммита и для крупных tx batch'ей
        # (370k+ rows) добавлял 60–120с к каждому 4-му commit'у → trigger
        # уезжал на минуты, executors OOM-ились, micro-batch очередь росла.
        # Теперь: inline=false + async.enabled=true → clustering планируется
        # автоматически (replacecommit instant) и выполняется фоновым
        # thread'ом писателя; основной upsert commit'ится за обычное время.
        # Семантика та же: schedule раз в 4 commits, target 128MB, sort
        # по cluster_sort_cols (event_day) → тот же file-skipping в Trino.
        "hoodie.clustering.inline": "false",
        "hoodie.clustering.async.enabled": "true",
        "hoodie.clustering.async.max.commits": "4",
        "hoodie.clustering.plan.strategy.target.file.max.bytes": str(128 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.small.file.limit": str(100 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.sort.columns": cluster_sort_cols,

        # --- metadata table + column-stats index -------------------------
        # Column stats живут в metadata table → Trino читает их одним
        # parquet-ом и прунит файлы данных без листинга S3.
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.column.stats.column.list": column_stats_cols,
    }

    # Record-level index: HFile в metadata, маппинг recordkey → fileId.
    # Для bronze с миллионами PK ускоряет upsert (vs default BLOOM).
    # NB: Hudi 1.1.1 имеет известный баг "File pruning with partitioned rli has not yet
    # been implemented" — RLI несовместим с partitioned tables на read path. Для
    # партиционированных таблиц форсим BLOOM (старый, но рабочий путь).
    if global_index:
        # GLOBAL_BLOOM: bloom-фильтры лежат в metadata table и индексируются
        # по recordkey глобально, без partition-scope. Нужен когда partition
        # path НЕ детерминированно выводится из PK (см. cancellations:
        # event_day = date(cancelled_ts), а cancelled_ts может варьироваться
        # для одного и того же cancellation_id между регенерациями source-файлов).
        # update.partition.path=true → при нахождении PK в другой партиции Hudi
        # ПЕРЕМЕЩАЕТ запись (delete старую + insert новую), а не дублирует.
        opts["hoodie.index.type"] = "GLOBAL_BLOOM"
        opts["hoodie.bloom.index.update.partition.path"] = "true"
    elif enable_record_index and not partition_field:
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


def reference_hudi_opts(table: str, db: str, pk: str) -> dict:
    """Hudi-options для reference-таблиц: non-partitioned full snapshot.

    Bronze reference == текущий snapshot upstream (см. ADR-002, FIX_PLAN P1-5):
    `insert_overwrite_table` атомарно заменяет содержимое таблицы — записи,
    выбывшие из upstream-snapshot, исчезают из bronze (zombie-устранение).

    Используется из:
      * `spark-jobs/bronze_reference_batch.py` — основной writer (one-shot job).

    Параметры зафиксированы для случая «маленькая non-partitioned таблица
    со snapshot-семантикой»:
      * partition_field = ""              (reference не партиционируется);
      * precombine = "ingested_at"        (DataFrame должен содержать колонку);
      * column_stats = "ingested_at"      (минимальный индекс — справочник мал);
      * record-index выключен             (для < 100k строк HFile-overhead не оправдан);
      * operation = insert_overwrite_table.
    """
    opts = hudi_opts(
        table, db,
        pk=pk,
        partition_field="",
        precombine="ingested_at",
        column_stats_cols="ingested_at",
        enable_record_index=False,
    )
    opts["hoodie.datasource.write.operation"] = "insert_overwrite_table"
    return opts
