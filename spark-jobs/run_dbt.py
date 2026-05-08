"""Shim для запуска dbt внутри SparkApplication driver pod.

spark-submit поднимает Python-процесс в driver pod-е (mode: cluster, type: Python).
В Spark image уже установлен dbt-core / dbt-spark[session] и есть PySpark
(`/opt/spark/python`). dbt-spark с `method: session` создаст SparkSession
через `SparkSession.builder.getOrCreate()` — поднимется тот же session, что
сконфигурён через `sparkConf` SparkApplication (HMS uri, S3 endpoint,
HoodieSparkSessionExtension и т.п.).

Шаги:
  1. Раскладываем flat-ConfigMap `dbt-project` (примонтирован в /tmp/cm)
     в нормальную dbt-структуру через общий `dbt-init.sh` (тот же скрипт,
     что использовался KubernetesPodOperator-ом раньше).
  2. Передаём argv после имени скрипта в dbt CLI без изменений:
       spark-submit run_dbt.py run --select silver
     эквивалентно
       dbt run --select silver
  3. Возвращаем proper exit code, чтобы Spark Operator увидел статус
     COMPLETED / FAILED, а Airflow — success / failed таски.
"""
from __future__ import annotations

import os
import subprocess
import sys

INIT_SCRIPT = "/tmp/cm/dbt-init.sh"
PROJECT_DIR = "/tmp/dbt-project"


def main() -> int:
    subprocess.run(["sh", INIT_SCRIPT], check=True)

    os.environ.setdefault("DBT_PROFILES_DIR", PROJECT_DIR)
    os.chdir(PROJECT_DIR)

    # Импортируем dbt после chdir / env, чтобы корректно разрешились пути.
    from dbt.cli.main import dbtRunner

    args = sys.argv[1:] or ["debug"]
    print(f"[run_dbt] invoking dbt with args: {args}", flush=True)

    res = dbtRunner().invoke(args)
    if res.exception is not None:
        print(f"[run_dbt] dbt raised exception: {res.exception}", flush=True)
        return 2
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
