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
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sensors.python import PythonSensor
from kubernetes.client import models as k8s

NAMESPACE = "spark-jobs"
# Если предыдущий success моложе этого порога — текущий scheduled run пропускаем.
# Закрывает дублирующий пересчёт когда long-running run (cold-start Spark + sensor +
# dbt ≈ 10 мин) пересекается со следующим cron-тиком */30.
SKIP_IF_RECENT_SUCCESS_MINUTES = 25


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


DBT_INIT_SCRIPT_PATH = "/tmp/cm/dbt-init.sh"
"""Путь к общему init-скрипту внутри pod (см. dbt/dbt-init.sh).

Раскладывает flat-configmap dbt-project в dbt-структуру.
"""

dbt_volumes = [
    k8s.V1Volume(name="cm", config_map=k8s.V1ConfigMapVolumeSource(name="dbt-project")),
    k8s.V1Volume(name="workdir", empty_dir=k8s.V1EmptyDirVolumeSource()),
]
dbt_volume_mounts = [
    k8s.V1VolumeMount(name="cm", mount_path="/tmp/cm"),
    k8s.V1VolumeMount(name="workdir", mount_path="/tmp/dbt-project"),
]
dbt_env = [k8s.V1EnvVar(name="DBT_PROFILES_DIR", value="/tmp/dbt-project")]
dbt_image = "lab08/spark:3.5.8-hudi-1.1.1"


def make_dbt_task(task_id: str, dbt_args: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        namespace=NAMESPACE,
        image=dbt_image,
        image_pull_policy="IfNotPresent",
        service_account_name="spark",
        cmds=["sh", "-c"],
        arguments=[f"sh {DBT_INIT_SCRIPT_PATH} && cd /tmp/dbt-project && dbt {dbt_args} --no-version-check"],
        volumes=dbt_volumes,
        volume_mounts=dbt_volume_mounts,
        env_vars=dbt_env,
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=180,
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
    description="wait bronze (streaming) → dbt silver → gold → tests",
    start_date=datetime(2025, 10, 6),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lab08"},
    tags=["lab08", "hudi", "dbt"],
) as dag:
    # ShortCircuit: пропускаем scheduled run, если предыдущий success был
    # < SKIP_IF_RECENT_SUCCESS_MINUTES минут назад. Cron */30 + cold-start
    # ~10 мин раньше давал 2 пересчёта подряд (15:00 и 15:30 на одних и тех
    # же данных). Manual run от bootstrap'а всегда проходит.
    skip_if_recent = ShortCircuitOperator(
        task_id="skip_if_recent_success",
        python_callable=_skip_if_recent_success,
        # ignore_downstream_trigger_rules=False даёт downstream корректный 'skipped',
        # а не 'upstream_failed' — DAGRun помечается success.
        ignore_downstream_trigger_rules=False,
    )

    # Sensor с mode='reschedule' освобождает worker-slot между poke,
    # чтобы первый запуск (cold-start Spark может занять до 5-10 мин)
    # не держал ресурсы. timeout=30 мин — fail-fast если streaming не поднялся.
    wait_bronze = PythonSensor(
        task_id="wait_bronze_ready",
        python_callable=_bronze_has_rows,
        poke_interval=20,
        timeout=60 * 30,
        mode="reschedule",
    )

    dbt_silver = make_dbt_task("dbt_silver", "run --select silver")
    dbt_gold = make_dbt_task("dbt_gold", "run --select gold")
    dbt_test = make_dbt_task("dbt_test", "test")

    skip_if_recent >> wait_bronze >> dbt_silver >> dbt_gold >> dbt_test

