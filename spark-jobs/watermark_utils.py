from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import DataFrame, Row, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from hudi_utils import hudi_opts, write_hudi

# Serializes all writes to the single bronze.ingest_watermarks Hudi table.
# Multiple Structured Streaming queries run their foreachBatch callbacks in
# parallel driver threads; concurrent commits to the same (non-partitioned)
# file group trigger HoodieWriteConflictException because Hudi's OCC +
# SimpleConcurrentFileWritesConflictResolutionStrategy rejects any overlap.
_WATERMARK_WRITE_LOCK = threading.Lock()


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
    """
    Generates and returns options for configuring Hudi with watermark-specific settings.

    This function constructs a dictionary of Hudi options tailored for the watermark
    table, including settings for primary key, precombine field, and column statistics.
    Additional options specific to concurrency, locking, and cleaning policies are
    also included.

    Returns:
        dict: A dictionary containing the Hudi configuration options.
    """
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
    """
    Extract unique partition values from a specified column in a DataFrame.

    This function processes a given DataFrame column, identifies unique non-null
    values, and formats them as partition strings in the form of `day=value`.
    If the column does not exist or contains only null values, it defaults
    to returning a single-element list with `__nonpartitioned__`.

    Parameters:
    column : str
        The name of the column to extract partition values from.

    batch_df : DataFrame
        The input DataFrame to process for partition extraction.

    Returns:
    list[str]
        A sorted list of partition strings derived from the specified column.
        If the column is not present in the DataFrame or no partitions are
        found, a default value of `["__nonpartitioned__"]` is returned.
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
    """
    Writes watermark information for processed partitions into a target table using Apache Hudi.

    This function constructs watermark records for the given table and list of partitions,
    then writes them to the corresponding destination using a standardized schema. Each
    watermark record contains details such as the table name, source partition, number
    of rows in the batch, and the committed timestamp.

    Parameters:
        spark (SparkSession): Spark session used to create DataFrames and perform write operations.
        table_name (str): Name of the target table for which the watermark is being written.
        partitions (Iterable[str]): List of source partition identifiers to include in the watermark data.
        rows_in_batch (int): Number of rows processed in the current batch.
        batch_id (int): Unique identifier of the current batch being processed.

    Returns:
        None
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
    with _WATERMARK_WRITE_LOCK:
        write_hudi(df, _watermark_hudi_opts())
    print(f"[watermark:{table_name} batch={batch_id}] partitions={parts}")


def bootstrap_watermark_table(spark: SparkSession) -> None:
    """
    Ensures the initialization of the watermark table.

    This function creates the schema for the watermark table if it does not
    already exist, and inserts an initial sentinel record to bootstrap the
    watermark tracking system. It guarantees that the bronze.ingest_watermarks
    table is in place for further usage.

    Arguments:
        spark: A SparkSession instance used to execute SQL commands and create
        the initial DataFrame.

    Returns:
        None
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
