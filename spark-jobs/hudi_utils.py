"""
Общие хелперы для записи в Hudi из Spark Structured Streaming.

Используются в bronze_s3_streaming.py и bronze_kafka_ingest.py.
Параметризуются через `table_suffix` чтобы Kafka-стрим писал в `*_kafka`
таблицы рядом с S3-стримом и не конкурировал по recordkey.
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
    """
    full_table = f"{table}{table_suffix}"
    return {
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
        "hoodie.metadata.enable": "true",
        "path": f"s3a://lake/{db}/{full_table}",
    }


def write_hudi(df: DataFrame, opts: dict) -> None:
    """Append-mode upsert; no-op for empty batches (avoid empty-commit churn)."""
    if df.rdd.isEmpty():
        return
    w = df.write.format("hudi")
    for k, v in opts.items():
        w = w.option(k, v)
    w.mode("append").save()
