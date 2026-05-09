FROM apache/airflow:2.10.4

USER airflow
RUN pip install --no-cache-dir \
        "apache-airflow-providers-cncf-kubernetes==9.0.0" \
        "apache-airflow-providers-trino==5.8.0"
