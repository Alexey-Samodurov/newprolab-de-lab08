"""DAG: daily medallion build (silver → gold → test) on top of bronze.

Gated on the ``bronze.ingest_watermarks_<source>`` shards via a ``PythonSensor`` in
``reschedule`` mode. The sensor holds the DAGRun in ``running`` state
until the watermark for ``transactions|day=<ds>`` appears with non-zero
rows. On timeout the DAGRun fails visibly. No retries, no silent skips.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.sensors.python import PythonSensor

from _common import (
    BRONZE_SOURCES,
    JOBS_DIR,
    WATERMARK_PRODUCER,
    build_spark_application_spec,
    make_spark_task,
)


log = logging.getLogger(__name__)

DBT_VARS = "{run_date: '{{ data_interval_start | ds }}'}"


def _dbt_task_spec(step: str, dbt_args: list[str]) -> dict:
    """Spark template_spec for one dbt step."""
    return build_spark_application_spec(
        name=f"dbt-{step}",
        main_file=f"{JOBS_DIR}/run_dbt.py",
        arguments=dbt_args,
        py_files=(),
        app_label=f"dbt-{step}",
    )


def _trino_query_tolerant(sql: str):
    """Run a Trino query, swallowing not-yet-created schema/table errors."""
    from airflow.providers.trino.hooks.trino import TrinoHook

    ignore = {"SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND", "DATABASE_NOT_FOUND"}
    try:
        return TrinoHook(trino_conn_id="trino_default").get_records(sql)
    except Exception as exc:
        name = getattr(exc, "error_name", None) or ""
        text = f"{type(exc).__name__}: {exc!r}"
        if name in ignore or any(token in text for token in ignore):
            log.info("tolerant: swallowing not-yet-created target (%s): %s",
                     name or "by-text", text[:300])
            return []
        raise


def _bronze_ready(**context) -> bool:
    """Return True when all bronze sources are ready for the given day.

    Transactions readiness is gated on the watermark table (authoritative
    row count). Cancellations and exchange_rates have no watermark emitter,
    so their readiness is checked by querying at least one row from their
    bronze table for the ``day=<ds>`` partition.
    """
    ds = context["ds"]

    rows = _trino_query_tolerant(
        f"SELECT rows_in_batch FROM hudi.bronze.ingest_watermarks_{WATERMARK_PRODUCER} "
        f"WHERE watermark_id LIKE '{WATERMARK_PRODUCER}|s3|day=%' "
        f"  AND source_partition='day={ds}' "
        "ORDER BY committed_at DESC LIMIT 1"
    )
    if not (rows and rows[0][0] and int(rows[0][0]) > 0):
        log.info("bronze not ready ds=%s: transactions watermark missing or zero — poke again", ds)
        return False
    log.info("bronze transactions ready ds=%s rows_in_batch=%s", ds, rows[0][0])

    for source in [s for s in BRONZE_SOURCES if s != WATERMARK_PRODUCER]:
        wm = _trino_query_tolerant(
            f"SELECT rows_in_batch FROM hudi.bronze.ingest_watermarks_{source} "
            f"WHERE watermark_id LIKE '{source}|s3|%' "
            f"  AND source_partition IN ('day={ds}', 'snapshot') "
            "ORDER BY committed_at DESC LIMIT 1"
        )
        if not wm:
            log.info("bronze not ready ds=%s: %s s3-watermark missing — poke again", ds, source)
            return False
        log.info("bronze %s ready ds=%s rows_in_batch=%s", source, ds, wm[0][0])

    return True


with DAG(
    dag_id="transactions_medallion",
    description="daily 02:30 UTC: dbt silver → gold → tests on top of bronze",
    start_date=datetime(2026, 4, 24),
    schedule="30 2 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "lab08"},
    tags=["lab08", "dbt", "medallion"],
) as dag:
    bronze_ready = PythonSensor(
        task_id="bronze_ready",
        python_callable=_bronze_ready,
        mode="reschedule",
        poke_interval=60,
        timeout=350,
        soft_fail=False,
    )

    dbt_silver = make_spark_task(
        task_id="dbt_silver",
        template_spec=_dbt_task_spec("silver", ["run", "--select", "silver", "--vars", DBT_VARS]),
    )
    dbt_gold = make_spark_task(
        task_id="dbt_gold",
        template_spec=_dbt_task_spec("gold", ["run", "--select", "gold", "--vars", DBT_VARS]),
    )
    dbt_test = make_spark_task(
        task_id="dbt_test",
        template_spec=_dbt_task_spec("test", ["test", "--vars", DBT_VARS]),
    )

    bronze_ready >> dbt_silver >> dbt_gold >> dbt_test
