"""Pure DataFrame transforms shared by S3 batch and Kafka stream writers.

Used to guarantee byte-identical derived columns (``event_day``,
``ingestion_day``, primary keys, ``ingested_at``) across both writers
so the partition-ownership contract from ADR-004 holds without ad-hoc
divergence between code paths.

All functions are side-effect free and return a new DataFrame.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F


CANCELLED_AT_PATTERN = "yyyy MMM dd HH:mm"
PK_NULL_SENTINEL_TS = "__nullts__"
PK_NULL_SENTINEL_ID = "__nullid__"
PK_NULL_SENTINEL_USER = "0"
PK_NULL_SENTINEL_CREATED = "0"
PK_NULL_SENTINEL_TIMESTAMP = "0"


def prepare_transactions(df: DataFrame, ingested_at: Column) -> DataFrame:
    """Add ``event_day``, ``composite_pk`` and ``ingested_at`` to transactions.

    ``event_day = date(from_unixtime(created_at, UTC))`` and is used as
    the Hudi partition column. ``composite_pk`` joins
    ``transaction_id|created_at|user_id`` (the upstream ``transaction_id``
    is not unique — see ADR-001).

    Args:
        df: Raw transactions DataFrame with the source columns.
        ingested_at: Spark column expression of type ``timestamp`` set
            on every record. The caller chooses semantics (logical
            airflow ``ds`` for batch / wall-clock for streaming).

    Returns:
        DataFrame with the derived columns appended via ``select("*", ...)``.
    """
    return df.select(
        "*",
        F.from_unixtime("created_at", "yyyy-MM-dd").alias("event_day"),
        F.concat_ws(
            "|",
            F.col("transaction_id").cast("string"),
            F.coalesce(F.col("created_at").cast("string"),
                       F.lit(PK_NULL_SENTINEL_CREATED)),
            F.coalesce(F.col("user_id").cast("string"),
                       F.lit(PK_NULL_SENTINEL_USER)),
        ).alias("composite_pk"),
        ingested_at.alias("ingested_at"),
    )


def prepare_cancellations(
    df: DataFrame,
    ingested_at: Column,
    ingestion_day: Column,
) -> DataFrame:
    """Parse ``cancelled_at`` and add partition / pk / ingestion columns.

    Critically, partition column is ``ingestion_day`` (the day the
    record was received), **not** ``event_day``. This keeps the
    partition-ownership contract well-defined for late-arriving
    cancellations (a cancellation for ``cancelled_at = T-3`` received
    today lands in the Kafka-owned partition ``ingestion_day = today``
    rather than colliding with the S3-owned partition ``T-3``). The
    business-day column ``event_day`` is preserved as a regular value
    and downstream silver/gold continue to group by ``event_day``.

    Args:
        df: Raw cancellations DataFrame with the source columns.
        ingested_at: Spark column expression of type ``timestamp``.
        ingestion_day: Spark column expression of type ``date`` used
            as the Hudi partition (e.g. ``date(kafka_ts)`` for stream,
            ``to_date(lit(ds))`` for batch).

    Returns:
        DataFrame with derived columns appended.
    """
    parsed = df.select(
        "*",
        F.to_timestamp("cancelled_at", CANCELLED_AT_PATTERN).alias("cancelled_ts"),
    )
    return (
        parsed
        .withColumn("ingestion_day", ingestion_day)
        .withColumn(
            "event_day",
            F.coalesce(
                F.date_format("cancelled_ts", "yyyy-MM-dd"),
                F.date_format(F.col("ingestion_day"), "yyyy-MM-dd"),
            ),
        )
        .withColumn("ingested_at", ingested_at)
        .withColumn(
            "cancellation_pk",
            F.concat_ws(
                "|",
                F.coalesce(F.col("cancelled_at"), F.lit(PK_NULL_SENTINEL_TS)),
                F.coalesce(F.col("cancellation_id").cast("string"),
                           F.lit(PK_NULL_SENTINEL_ID)),
                F.coalesce(F.col("original_transaction_id").cast("string"),
                           F.lit(PK_NULL_SENTINEL_ID)),
            ),
        )
    )


def prepare_rates(df: DataFrame, ingested_at: Column) -> DataFrame:
    """Build composite ``rate_pk`` so re-sent updates keep history."""
    return df.select(
        "*",
        F.concat_ws(
            "|",
            F.col("update_id").cast("string"),
            F.coalesce(F.col("timestamp").cast("string"),
                       F.lit(PK_NULL_SENTINEL_TIMESTAMP)),
        ).alias("rate_pk"),
        ingested_at.alias("ingested_at"),
    )
