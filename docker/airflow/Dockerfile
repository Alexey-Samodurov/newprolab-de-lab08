FROM apache/airflow:2.10.4

# Pre-install только то, что реально используется DAG-ами:
#  - kubernetes provider (KubernetesPodOperator для dbt в transactions_pipeline)
#  - trino provider (TrinoHook для PythonSensor wait_bronze_ready)
# dbt сам не нужен в airflow image — он запускается в spark image внутри pod (см. ADR #10).
USER airflow
RUN pip install --no-cache-dir \
        "apache-airflow-providers-cncf-kubernetes==9.0.0" \
        "apache-airflow-providers-trino==5.8.0"
