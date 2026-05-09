from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.python import PythonSensor

NAMESPACE = "spark-jobs"
SPARK_IMAGE = "lab08/spark:3.5.8-hudi-1.1.1"

SPARK_CONF: dict[str, str] = {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "s3a://lake/warehouse/",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
    "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "spark.kubernetes.namespace": NAMESPACE,
    "spark.kubernetes.executor.deleteOnTermination": "true",
    # 16 партиций для shuffle (dedup-window cancellations_clean, joins в gold).
    # default=200 → overhead на task scheduling и россыпь мелких parquet-ов;
    # совпадает с hudi.upsert/insert.shuffle.parallelism в hudi_utils.
    "spark.sql.shuffle.partitions": "16",
    # AQE + coalesce — динамически схлопывает skew-партиции и убирает
    # пустые после фильтров → меньше пиковая память на executor'е.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Promo_codes / users — крошечные справочники, broadcast вместо
    # shuffle-hash join. 32MB порог покрывает наши reference-таблицы.
    "spark.sql.autoBroadcastJoinThreshold": str(32 * 1024 * 1024),
    "spark.metrics.namespace": "spark",
    "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
    "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus/",
    "spark.metrics.conf.driver.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
    "spark.metrics.conf.executor.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "3",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.dynamicAllocation.shuffleTracking.timeout": "120s",
}

_AWS_ENV_REFS = {
    "AWS_ACCESS_KEY_ID": {"name": "lab08-credentials", "key": "AWS_ACCESS_KEY_ID"},
    "AWS_SECRET_ACCESS_KEY": {"name": "lab08-credentials", "key": "AWS_SECRET_ACCESS_KEY"},
}

DRIVER_BASE: dict = {
    "cores": 1,
    "coreLimit": "1200m",
    "memory": "1g",
    "memoryOverhead": "256m",
    "serviceAccount": "spark",
    "envSecretKeyRefs": _AWS_ENV_REFS,
    "volumeMounts": [
        {"name": "jobs-code", "mountPath": "/opt/spark/jobs"},
        {"name": "dbt-cm", "mountPath": "/tmp/cm"},
    ],
}

EXECUTOR_BASE: dict = {
    "cores": 1,
    "coreLimit": "1200m",
    "instances": 1,
    "memory": "2g",
    "memoryOverhead": "512m",
    "envSecretKeyRefs": _AWS_ENV_REFS,
}

VOLUMES: list[dict] = [
    {"name": "jobs-code", "configMap": {"name": "spark-jobs-code"}},
    {"name": "dbt-cm", "configMap": {"name": "dbt-project"}},
]


def _build_dbt_spec(step: str, dbt_args: list[str]) -> dict:
    """SparkApplication-манифест для конкретного dbt-шага.

    `step` ∈ {silver, gold, test}. Имя CR-а суффиксуется Jinja-шаблоном
    `ts_nodash` — даёт уникальность per task instance.
    """
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"dbt-{step}-" + "{{ ts_nodash | lower }}",
            "namespace": NAMESPACE,
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/jobs/run_dbt.py",
            "arguments": dbt_args,
            "sparkVersion": "3.5.8",
            "restartPolicy": {"type": "Never"},
            "sparkConf": SPARK_CONF,
            "driver": {**DRIVER_BASE, "labels": {"app": f"dbt-{step}"}},
            "executor": {**EXECUTOR_BASE, "labels": {"app": f"dbt-{step}"}},
            "volumes": VOLUMES,
        },
    }


def make_dbt_spark_task(task_id: str, dbt_args: list[str]) -> SparkKubernetesOperator:
    step = task_id.replace("dbt_", "")
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=NAMESPACE,
        template_spec=_build_dbt_spec(step, dbt_args),
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=True,
        do_xcom_push=False,
        startup_timeout_seconds=600,
        reattach_on_restart=True,
        success_run_history_limit=1,
    )


def _trino_query_tolerant(sql: str):
    """Выполнить Trino-запрос, проглатывая ошибки «схемы/таблицы ещё нет».

    Стрим лениво создаёт `bronze.ingest_watermarks` при первом батче. До
    этого момента любой `SELECT FROM hudi.bronze.ingest_watermarks` падает
    с SCHEMA_NOT_FOUND или TABLE_NOT_FOUND. С точки зрения Airflow это
    просто «данных пока нет», поэтому возвращаем пустой результат вместо
    исключения. Любые другие ошибки пробрасываются — это настоящие
    инциденты (Trino упал, права, синтаксис).
    """
    from airflow.providers.trino.hooks.trino import TrinoHook

    _IGNORE = {"SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND", "DATABASE_NOT_FOUND"}
    try:
        return TrinoHook(trino_conn_id="trino_default").get_records(sql)
    except Exception as exc:
        # У TrinoUserError из trino-python-client есть атрибут `error_name`.
        # Если его нет — фолбэк на подстроку в repr/str.
        name = getattr(exc, "error_name", None) or ""
        text = f"{type(exc).__name__}: {exc!r}"
        if name in _IGNORE or any(t in text for t in _IGNORE):
            print(f"[tolerant] swallow not-yet-created target ({name or 'by-text'}): {text[:300]}")
            return []
        raise


def _bronze_watermark_ready(**context) -> bool:
    """Sensor: появилась ли watermark-строка `transactions|day=<ds>`.

    Контракт см. в lab08/ADR-001-bronze-watermark-table.md (раздел
    «Sensor semantics»). Сравнение строго `=`, не `>=`.
    """
    ds = context["ds"]
    rows = _trino_query_tolerant(
        "SELECT 1 FROM hudi.bronze.ingest_watermarks "
        "WHERE table_name='transactions' "
        f"  AND source_partition='day={ds}' LIMIT 1"
    )
    return bool(rows)


def _partition_has_data(**context) -> bool:
    """ShortCircuit: rows_in_batch>0 в watermark для day=<ds>.

    Защита от edge-case: стрим обработал файл нулевого размера → watermark
    есть, но в bronze.transactions ничего полезного нет → dbt бессмысленен.
    """
    ds = context["ds"]
    rows = _trino_query_tolerant(
        "SELECT rows_in_batch FROM hudi.bronze.ingest_watermarks "
        "WHERE table_name='transactions' "
        f"  AND source_partition='day={ds}' "
        "ORDER BY committed_at DESC LIMIT 1"
    )
    if rows and rows[0][0] and int(rows[0][0]) > 0:
        print(f"watermark for day={ds}: rows_in_batch={rows[0][0]} → run dbt")
        return True
    print(f"watermark for day={ds} empty/zero → SKIP dbt")
    return False


with DAG(
    dag_id="transactions_pipeline",
    description="daily 02:00 UTC: check source → wait bronze → dbt silver → gold → tests",
    start_date=datetime(2026, 4, 24),
    schedule="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "lab08"},
    tags=["lab08", "hudi", "dbt"],
) as dag:
    from airflow.operators.python import ShortCircuitOperator

    wait_bronze = PythonSensor(
        task_id="wait_bronze_ready",
        python_callable=_bronze_watermark_ready,
        # Контракт см. в lab08/ADR-001-bronze-watermark-table.md.
        # SCHEMA_NOT_FOUND/DATABASE_NOT_FOUND внутри callback трактуются
        # как «строки ещё нет» — стрим bronze ещё не успел создать таблицу.
        poke_interval=30,
        # 10 мин: покрывает штатный cold-start стрима + первый foreachBatch.
        # Worker не блокируется (mode=reschedule). Пропуски источника
        # (25/26 апр) уйдут в skipped через 10 мин — разовая плата.
        timeout=60 * 10,
        mode="reschedule",
        soft_fail=True,
    )

    check_partition = ShortCircuitOperator(
        task_id="check_partition_has_data",
        python_callable=_partition_has_data,
    )

    DBT_VARS = "{run_date: '{{ data_interval_start | ds }}'}"

    dbt_silver = make_dbt_spark_task("dbt_silver", ["run", "--select", "silver", "--vars", DBT_VARS])
    dbt_gold = make_dbt_spark_task("dbt_gold", ["run", "--select", "gold", "--vars", DBT_VARS])
    dbt_test = make_dbt_spark_task("dbt_test", ["test", "--vars", DBT_VARS])

    wait_bronze >> check_partition >> dbt_silver >> dbt_gold >> dbt_test
