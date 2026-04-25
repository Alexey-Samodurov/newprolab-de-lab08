FROM apache/airflow:2.10.4

# Pre-install providers + cosmos чтобы init-контейнеры не делали pip backtracking на каждом старте.
USER airflow
RUN pip install --no-cache-dir \
        "astronomer-cosmos==1.7.1" \
        "apache-airflow-providers-cncf-kubernetes==9.0.0" \
        "apache-airflow-providers-trino==5.8.0" \
        "dbt-core==1.8.7" \
        "dbt-spark[PyHive]==1.8.0"
