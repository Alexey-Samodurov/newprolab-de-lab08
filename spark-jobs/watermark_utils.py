from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import DataFrame, Row, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from hudi_utils import hudi_opts, write_hudi


WATERMARK_SCHEMA = StructType([
    StructField("watermark_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("source_partition", StringType(), False),
    StructField("rows_in_batch", LongType(), True),
    StructField("committed_at", StringType(), False),
])

_WATERMARK_TABLE = "ingest_watermarks"
_WATERMARK_DB = "bronze"


def _watermark_hudi_opts() -> dict:
    opts = hudi_opts(
        _WATERMARK_TABLE, _WATERMARK_DB,
        pk="watermark_id",
        partition_field="",
        precombine="committed_at",
        column_stats_cols="table_name,source_partition,committed_at",
        enable_record_index=False,
    )
    opts.update({
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        "hoodie.cleaner.policy.failed.writes": "LAZY",
        "hoodie.metadata.enable": "false",
        "hoodie.metadata.index.column.stats.enable": "false",
        "hoodie.clustering.inline": "false",
    })
    return opts


def extract_source_partitions_from_column(batch_df: DataFrame, column: str) -> list[str]:
    """Достаём distinct event-day из колонки batch'а.

    Ожидается, что колонка содержит ISO-дату `YYYY-MM-DD`. Префикс `day=`
    добавляется здесь для совместимости с историческими source-day ключами.
    Если колонки нет (не-day-partitioned поток вроде rates) — возвращаем
    sentinel `__nonpartitioned__`.
    """
    if column not in batch_df.columns:
        return ["__nonpartitioned__"]
    rows = (
        batch_df.select(F.col(column).alias("d"))
        .where(F.col("d").isNotNull())
        .distinct()
        .collect()
    )
    parts = {f"day={r.d}" for r in rows if r.d}
    return sorted(parts) if parts else ["__nonpartitioned__"]


def write_watermark(
    spark: SparkSession,
    table_name: str,
    partitions: Iterable[str],
    rows_in_batch: int,
    batch_id: int,
) -> None:
    """Записать watermark-строки. Контракт см. в doc-string модуля.

    Вызывается ПОСЛЕ успешного `write_hudi(target, ...)`. Если основная
    запись упадёт — watermark не появится, sensor продолжит ждать.
    Семантика upsert по `watermark_id` гарантирует idempotent-ность.
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
    print(f"[watermark:{table_name} batch={batch_id}] partitions={parts}")


def bootstrap_watermark_table(spark: SparkSession) -> None:
    """Создать `bronze.ingest_watermarks`, если её ещё нет (idempotent).

    Гарантирует, что:
      1. Hive schema `bronze` существует — иначе Trino падает с
         `SCHEMA_NOT_FOUND` ещё до того, как первая Hudi-таблица создаст
         её лениво.
      2. Сама таблица `ingest_watermarks` существует — sentinel-write
         через Hudi регистрирует её в HMS.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_WATERMARK_DB}")
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
    print("[bootstrap] bronze.ingest_watermarks ensured")
