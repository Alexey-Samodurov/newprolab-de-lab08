from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from hudi_utils import hudi_opts, write_hudi
from log_utils import get_logger


log = get_logger(__name__)
WATERMARK_SCHEMA = StructType([
    StructField("watermark_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("source_partition", StringType(), False),
    StructField("rows_in_batch", LongType(), True),
    StructField("committed_at", StringType(), False),
])

_WATERMARK_TABLE = "ingest_watermarks"
_WATERMARK_DB = "bronze"
_BOOTSTRAPPED: set[str] = set()


def _watermark_hudi_opts() -> dict:
    """Cheap Hudi opts for the watermark table.

    Watermark — это per-partition маркер для downstream (Airflow sensor).
    Дорогие фичи отключены:

    * ``hoodie.metadata.enable=false`` — никаких MDT-коммитов на каждую запись.
    * ``shuffle.parallelism=1`` — одна-две строки на коммит.
    * ``clustering=false`` — крошечные файлы, кластеризация бессмысленна.

    ``hive_sync`` остаётся включённым: таблица партиционируется по
    ``table_name``, и при появлении новой партиции (например, первого
    батча ``transactions`` после bootstrap) её нужно зарегистрировать в
    HMS, иначе Trino/Airflow не увидят строки. ``meta_sync.condition.sync``
    в ``hudi_opts`` гарантирует, что HMS дёргается только при реальных
    изменениях схемы/партиций, а не на каждый коммит.
    """
    opts = hudi_opts(
        _WATERMARK_TABLE, _WATERMARK_DB,
        pk="watermark_id",
        partition_field="table_name",
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
) -> None:
    """Emit one watermark row per partition for downstream (Airflow) gating.

    Использует Hudi с минимальной обвязкой (см. ``_watermark_hudi_opts``).
    Поле ``rows_in_batch`` нужно только как «> 0 → есть данные»; точное
    значение приходит из Hudi commit-метаданных (``read_latest_commit``) и
    дополнительный count по DataFrame не запускается.
    """
    parts = list(partitions)
    if not parts:
        return
    committed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    rows = [
        Row(
            watermark_id=f"{table_name}|{p}",
            table_name=table_name,
            source_partition=p,
            rows_in_batch=int(rows_in_batch),
            committed_at=committed_at,
        )
        for p in parts
    ]
    df = spark.createDataFrame(rows, schema=WATERMARK_SCHEMA)
    write_hudi(df, _watermark_hudi_opts())
    log.info("watermark table=%s batch=%s partitions=%s", table_name, batch_id, parts)


def bootstrap_watermark_table(spark: SparkSession) -> None:
    """Создать таблицу bronze.ingest_watermarks один раз за жизнь процесса.

    Раньше функция писала sentinel-строку при каждом старте → Hudi commit +
    Hive sync. Теперь: проверяем существование через HMS, и только если
    таблицы нет — создаём её одним bootstrap-коммитом с включённым
    hive_sync. Повторные вызовы внутри одного процесса — no-op.
    """
    if _WATERMARK_TABLE in _BOOTSTRAPPED:
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_WATERMARK_DB}")
    full_name = f"{_WATERMARK_DB}.{_WATERMARK_TABLE}"
    try:
        if spark.catalog.tableExists(full_name):
            _BOOTSTRAPPED.add(_WATERMARK_TABLE)
            log.info("bootstrap: %s already exists, skip", full_name)
            return
    except Exception:
        pass

    sentinel = spark.createDataFrame(
        [Row(
            watermark_id="__bootstrap__|__init__",
            table_name="__bootstrap__",
            source_partition="__init__",
            rows_in_batch=0,
            committed_at="1970-01-01T00:00:00",
        )],
        schema=WATERMARK_SCHEMA,
    )
    write_hudi(sentinel, _watermark_hudi_opts())
    _BOOTSTRAPPED.add(_WATERMARK_TABLE)
    log.info("bootstrap: %s created", full_name)
