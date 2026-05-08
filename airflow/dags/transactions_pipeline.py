"""
Lab08 — DAG: ждём bronze → dbt silver → gold → tests.

Bronze.* наполняется long-running streaming SparkApplication'ами
(`bronze-s3-streaming`, `bronze-kafka-ingest`), которые применяются
декларативно через `kubectl apply` в `make up` и не управляются Airflow'ом
(они сами рестартятся через `restartPolicy: Always` и держат checkpoint
на S3 — рестарт DAG'ом им не нужен).

DAG крутится по cron `*/30`. На первом запуске (после `make up`) Spark может
ещё не успеть наполнить bronze — для этого первая таска `wait_bronze_ready`
поллит Trino через `TrinoHook` пока в `hudi.bronze.transactions` не появятся
строки. Только после этого dbt силвер/голд гоняется. На последующих запусках
sensor проходит мгновенно.

dbt-таски запускаются через `SparkKubernetesOperator` — каждая создаёт
SparkApplication CR (`apiVersion: sparkoperator.k8s.io/v1beta2`), driver
которого выполняет `spark-submit run_dbt.py <args>`. dbt-spark с
`method: session` подцепляет уже сконфигурированный SparkSession (HMS,
S3, Hudi extensions передаются через `sparkConf`). Thrift Server больше
не нужен — см. lab08/MIGRATION_THRIFT_TO_OPERATOR.md.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.python import PythonSensor

NAMESPACE = "spark-jobs"
SPARK_IMAGE = "lab08/spark:3.5.8-hudi-1.1.1"
SKIP_IF_RECENT_SUCCESS_MINUTES = 40


def _bronze_has_rows() -> bool:
    """Проверяем, что streaming уже создал bronze.transactions и записал rows.

    Любая ошибка (таблица ещё не создана HMS sync'ом, Trino ещё стартует и т.п.)
    трактуется как not-ready → sensor продолжает poke. Это закрывает race
    между ингестом и dbt без ручного оркестрирования.
    """
    try:
        from airflow.providers.trino.hooks.trino import TrinoHook
        hook = TrinoHook(trino_conn_id="trino_default")
        row = hook.get_first("SELECT count(*) FROM hudi.bronze.transactions")
        ready = bool(row) and row[0] > 0
        print(f"bronze.transactions count={row[0] if row else 'N/A'}, ready={ready}")
        return ready
    except Exception as exc:
        print(f"bronze.transactions ещё не доступна: {exc}")
        return False


SPARK_CONF: dict[str, str] = {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "s3a://warehouse/",
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
    "spark.metrics.namespace": "spark",
    "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
    "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus/",
    "spark.metrics.conf.driver.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
    "spark.metrics.conf.executor.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
}

_AWS_ENV_REFS = {
    "AWS_ACCESS_KEY_ID": {"name": "lab08-credentials", "key": "AWS_ACCESS_KEY_ID"},
    "AWS_SECRET_ACCESS_KEY": {"name": "lab08-credentials", "key": "AWS_SECRET_ACCESS_KEY"},
}

DRIVER_BASE: dict = {
    "cores": 1,
    "coreLimit": "2000m",
    "memory": "1500m",
    "serviceAccount": "spark",
    "envSecretKeyRefs": _AWS_ENV_REFS,
    "volumeMounts": [
        {"name": "jobs-code", "mountPath": "/opt/spark/jobs"},
        {"name": "dbt-cm", "mountPath": "/tmp/cm"},
    ],
}

EXECUTOR_BASE: dict = {
    "cores": 1,
    "instances": 1,
    "memory": "1500m",
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
            # dbt должен либо пройти, либо упасть; перезапуск повторно прогонит модели.
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
        # cold-start image-pull может занять время на свежем ноде.
        startup_timeout_seconds=600,
        # default; на restart scheduler-а — найти уже созданный SparkApplication.
        reattach_on_restart=True,
        # не копим завершённые SparkApplication CR.
        success_run_history_limit=1,
    )


def _skip_if_recent_success(**context) -> bool:
    """ShortCircuit: True → продолжать, False → skip downstream.

    Manual runs (run_id начинается с 'manual__') всегда проходят — пользователь
    запросил явно. Для scheduled runs смотрим на самый свежий success этого DAG'а:
    если < SKIP_IF_RECENT_SUCCESS_MINUTES назад — пропускаем (значит предыдущий
    long-running cycle ещё не успел "остыть", lookback в dbt всё равно покрыл
    эти данные).
    """
    dag_run = context["dag_run"]
    if dag_run.run_id.startswith("manual__"):
        print(f"manual run ({dag_run.run_id}) — пропуск проверки, продолжаем")
        return True

    from airflow.models import DagRun
    from airflow.utils.state import DagRunState

    last_success = (
        DagRun.find(dag_id=dag_run.dag_id, state=DagRunState.SUCCESS)
        or []
    )
    last_success.sort(key=lambda r: r.end_date or r.start_date, reverse=True)
    if not last_success:
        print("предыдущих success run'ов нет — продолжаем")
        return True

    prev = last_success[0]
    end = prev.end_date or prev.start_date
    age = datetime.now(timezone.utc) - end
    threshold = timedelta(minutes=SKIP_IF_RECENT_SUCCESS_MINUTES)
    print(f"последний success: {prev.run_id} закончился {end}, age={age}, threshold={threshold}")
    if age < threshold:
        print(f"SKIP: предыдущий success моложе {SKIP_IF_RECENT_SUCCESS_MINUTES} мин")
        return False
    return True


with DAG(
    dag_id="transactions_pipeline",
    description="wait bronze (streaming) → dbt silver → gold → tests (через SparkApplication CRD)",
    start_date=datetime(2025, 10, 6),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lab08"},
    tags=["lab08", "hudi", "dbt"],
) as dag:
    skip_if_recent = ShortCircuitOperator(
        task_id="skip_if_recent_success",
        python_callable=_skip_if_recent_success,
        ignore_downstream_trigger_rules=False,
    )

    wait_bronze = PythonSensor(
        task_id="wait_bronze_ready",
        python_callable=_bronze_has_rows,
        poke_interval=20,
        timeout=60 * 30,
        mode="reschedule",
    )

    dbt_silver = make_dbt_spark_task("dbt_silver", ["run", "--select", "silver"])
    dbt_gold = make_dbt_spark_task("dbt_gold", ["run", "--select", "gold"])
    dbt_test = make_dbt_spark_task("dbt_test", ["test"])

    skip_if_recent >> wait_bronze >> dbt_silver >> dbt_gold >> dbt_test
