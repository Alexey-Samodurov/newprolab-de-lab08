"""DAG: daily ingest of S3 bronze tables (transactions / cancellations / exchange_rates).

Reads ``day=<ds>`` (T-1) from the public Yandex Cloud bucket. Three Spark
tasks run in parallel, one per source. Only the ``transactions`` task
publishes a watermark row that the downstream medallion DAG gates on.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator

from _common import (
    BRONZE_RESOURCE_PROFILES,
    BRONZE_SOURCES,
    JOBS_DIR,
    SOURCE_BUCKET,
    SOURCE_ENDPOINT,
    SOURCE_ROOT,
    UPSTREAM_BUCKET_CONF,
    build_spark_application_spec,
    make_spark_task,
)


log = logging.getLogger(__name__)


def _bronze_task_spec(source: str) -> dict:
    """Spark template_spec for one bronze source."""
    return build_spark_application_spec(
        name=f"bronze-{source}",
        main_file=f"{JOBS_DIR}/bronze_s3_batch.py",
        arguments=[
            "--source", source,
            "--ds", "{{ ds }}",
            "--src-root", SOURCE_ROOT,
        ],
        py_files=(),
        app_label=f"bronze-{source}",
        extra_conf=UPSTREAM_BUCKET_CONF,
        resource_profile=BRONZE_RESOURCE_PROFILES[source],
    )


def _source_day_available(**context) -> bool:
    """Probe upstream for at least one ``day=<ds>/`` object.

    Anonymous boto3 against the public Yandex Cloud endpoint. Returns
    ``True`` if at least one key is visible; ``False`` short-circuits the
    DAG run so the slot is skipped without retries.
    """
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config

    ds = context["ds"]
    client = boto3.client(
        "s3",
        endpoint_url=SOURCE_ENDPOINT,
        config=Config(signature_version=UNSIGNED),
    )
    prefix = f"day={ds}/"
    response = client.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=prefix, MaxKeys=1)
    available = response.get("KeyCount", 0) > 0
    log.info("source ds=%s prefix=%s available=%s", ds, prefix, available)
    return available


with DAG(
    dag_id="bronze_s3_ingest",
    description="daily 02:00 UTC: ingest 3 bronze sources (T-1) from public S3",
    start_date=datetime(2026, 4, 24),
    schedule="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "lab08"},
    tags=["lab08", "bronze", "hudi"],
) as dag:
    check_source = ShortCircuitOperator(
        task_id="check_source_day",
        python_callable=_source_day_available,
    )

    bronze_tasks = [
        make_spark_task(task_id=f"bronze_{source}", template_spec=_bronze_task_spec(source))
        for source in BRONZE_SOURCES
    ]

    check_source >> bronze_tasks
