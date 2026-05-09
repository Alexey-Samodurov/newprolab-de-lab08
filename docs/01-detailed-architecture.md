# Detailed Architecture: Lab08 — Транзакционная Аналитика

---

## 1. Введение

`lab08` — локально разворачиваемый Lakehouse end-to-end:

1. **Ingest:** YC S3 (`npl-de18-lab8-data`, JSONL, batch-файлы) и внешний Kafka.
2. **Хранение:** Apache Hudi (CoW) поверх MinIO в medallion-слоях `bronze/silver/gold`.
3. **Трансформации:** dbt-spark (`method: session`, incremental + Hudi `merge`).
4. **Каталог:** Hive Metastore (Postgres backend).
5. **Query/BI:** Trino 470 (read-only), Apache Superset 4.x.
6. **Оркестрация:** Airflow 2.10.4 (`KubernetesExecutor`); стриминг — долгоживущие `SparkApplication` CRD'ы.
7. **Observability:** kube-prometheus-stack, Spark Prometheus servlet, statsd-exporter для Airflow.

Всё разворачивается в Kubernetes (kind / Docker Desktop) декларативно через `helmfile.yaml` + raw-манифесты `k8s/`.

---

## 2. System Context (C4 L1)

```mermaid
C4Context
title System Context — Lab08 Lakehouse

Person(de, "Data Engineer", "Разворачивает и эксплуатирует платформу")

System_Boundary(lab08, "Lab08 Lakehouse Platform") {
  System(platform, "Lab08 Platform", "K8s + Spark + Hudi + dbt + Trino + Superset + Airflow")
}

System_Ext(ycs3, "Yandex Cloud S3", "npl-de18-lab8-data: transactions, cancellations, exchange_rates, reference")
System_Ext(kafka, "External Kafka", "Поток событий (transaction / cancellation / rate)")

Rel(ycs3, platform, "JSONL", "S3A HTTPS, anonymous")
Rel(kafka, platform, "Event stream", "Kafka protocol")
Rel(de, platform, "make up / kubectl / browser → *.lab08.local")
```

---

## 3. Containers (C4 L2)

```mermaid
C4Container
title Container Diagram — Lab08

System_Ext(ycs3, "YC S3", "npl-de18-lab8-data")
System_Ext(kafka, "External Kafka")

System_Boundary(k8s, "Kubernetes Cluster") {
  Container_Boundary(ns_storage, "ns: storage") {
    ContainerDb(minio, "MinIO Tenant", "MinIO Operator 6.0.4", "buckets lake/, checkpoints/")
  }
  Container_Boundary(ns_dp, "ns: data-platform") {
    ContainerDb(hms_pg, "HMS Postgres", "Bitnami PG 15", "Backend HMS")
    Container(hms, "Hive Metastore", "Hive 3.0.0 Thrift", "thrift://...:9083")
    Container(spark_op, "Spark Operator", "kubeflow 1.4.6", "Управляет SparkApplication CRD")
    Container(airflow, "Airflow", "2.10.4 K8sExecutor", "DAG transactions_pipeline (0 2 * * *)")
    Container(trino, "Trino", "v470 (chart 0.34.0)", "Read-only, каталог hudi → HMS")
    Container(superset, "Superset", "4.x (chart 0.13.2)", "Дашборды поверх Trino")
  }
  Container_Boundary(ns_spark, "ns: spark-jobs") {
    Container(s3stream, "bronze-s3-streaming", "SparkApp, restart=Always", "S3 file source → bronze")
    Container(kstream, "bronze-kafka-ingest", "SparkApp, restart=Always", "Kafka → bronze.events_kafka")
    Container(dbt, "dbt SparkApp", "ephemeral, restart=Never", "run_dbt.py: silver / gold / test")
  }
  Container_Boundary(ns_mon, "ns: monitoring") {
    Container(prom, "kube-prometheus-stack", "65.5.1", "Prom + Grafana + AM")
    Container(statsd, "statsd-exporter", "0.13.0", "Airflow StatsD → Prom")
  }
  Container(ingress, "ingress-nginx", "4.11.0", "*.lab08.local")
}

Rel(ycs3, s3stream, "Read JSONL", "S3A HTTPS anon")
Rel(kafka, kstream, "Read events", "Kafka")
Rel(s3stream, minio, "Hudi upsert + checkpoint", "S3A")
Rel(kstream, minio, "Hudi upsert + checkpoint", "S3A")
Rel(s3stream, hms, "Sync table", "Thrift")
Rel(kstream, hms, "Sync table", "Thrift")

Rel(airflow, spark_op, "Apply SparkApplication CR", "K8s API")
Rel(spark_op, dbt, "Spawn driver+executor", "K8s API")
Rel(dbt, hms, "Hudi catalog ops", "Thrift")
Rel(dbt, minio, "Read bronze / write silver, gold", "S3A")
Rel(airflow, trino, "wait_bronze_ready (sensor)", "HTTP")

Rel(trino, hms, "Lookup tables", "Thrift")
Rel(trino, minio, "Read Parquet", "S3A")
Rel(superset, trino, "SQL", "HTTP")
Rel(hms, hms_pg, "JDBC", "PG")

Rel(prom, s3stream, "Scrape /metrics/prometheus", "HTTP")
Rel(prom, kstream, "Scrape /metrics/prometheus", "HTTP")
Rel(airflow, statsd, "StatsD", "UDP 9125")
```

### 3.1 Стек

| Container | Версия | Namespace |
|---|---|---|
| ingress-nginx | 4.11.0 | `ingress-nginx` |
| MinIO Operator + Tenant | 6.0.4 | `minio-operator`, `storage` |
| HMS Postgres (Bitnami) | 15.5.38 | `data-platform` |
| Hive Metastore | 3.0.0 (raw manifest) | `data-platform` |
| Spark Operator | 1.4.6 | `data-platform` |
| Airflow | chart 1.15.0 (Airflow 2.10.4) | `data-platform` |
| Trino | chart 0.34.0 (v470) | `data-platform` |
| Superset | chart 0.13.2 (4.x) | `data-platform` |
| kube-prometheus-stack | 65.5.1 | `monitoring` |
| statsd-exporter | 0.13.0 | `monitoring` |

**Кастомные образы:** `lab08/spark:3.5.8-hudi-1.1.1` (Spark + Hudi + Hadoop S3A + Kafka + dbt-spark 1.8.0 + jobs); `lab08/airflow:2.10.4` (cncf-kubernetes, trino, statsd providers).

### 3.2 Протоколы

| От → К | Протокол / формат | Auth |
|---|---|---|
| Spark / Trino → MinIO | S3A HTTP / Parquet | secret `lab08-credentials` |
| Spark → YC S3 | S3A HTTPS / JSONL | Anonymous provider |
| Spark / Trino / dbt → HMS | Thrift | none (in-cluster) |
| HMS → Postgres | JDBC | `hive/hive` |
| Airflow → Spark Operator | K8s API (`SparkApplication` v1beta2) | SA `airflow` (RBAC `k8s/airflow-rbac.yaml`) |
| Airflow → Trino | HTTP REST | `trino_default` |
| Superset → Trino | HTTP REST | configured DB conn |
| Spark → Kafka | Kafka | env `KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC` |
| Prometheus → Spark | HTTP `/metrics/prometheus/` | none |
| Airflow → StatsD | UDP 9125 | none |

---

## 4. Components (C4 L3)

### 4.1 Spark streaming ingest (`spark-jobs/`)

```mermaid
C4Component
title Component — bronze-s3-streaming + bronze-kafka-ingest

Container_Ext(ycs3, "YC S3")
Container_Ext(kafka, "External Kafka")
ContainerDb(minio, "MinIO lake/, checkpoints/")
Container_Ext(hms, "HMS Thrift")

Container_Boundary(s3app, "SparkApp: bronze-s3-streaming") {
  Component(s3main, "bronze_s3_streaming.py", "PySpark", "4 параллельных file-source стрима в одной JVM")
  Component(handlers, "handle_transactions/cancellations/rates + reference loaders", "foreachBatch", "enrich → composite_pk, event_day, ingested_at → upsert")
  Component(hudi_utils, "hudi_utils.py", "lib", "hudi_opts(), write_hudi(): bulk_insert / upsert")
  Component(wmark, "watermark_utils.py", "lib", "bronze.ingest_watermarks (commit log)")
}

Container_Boundary(kapp, "SparkApp: bronze-kafka-ingest") {
  Component(kmain, "bronze_kafka_ingest.py", "PySpark", "readStream(kafka) → process_batch")
  Component(kproc, "process_batch", "foreachBatch", "split by _source → tx / cancel / rates → upsert")
}

Rel(ycs3, s3main, "readStream(json)", "S3A")
Rel(s3main, handlers, "writeStream.foreachBatch")
Rel(handlers, hudi_utils, "write_hudi(...)")
Rel(handlers, wmark, "write_watermark(...)")
Rel(kafka, kmain, "readStream(kafka)")
Rel(kmain, kproc, "writeStream.foreachBatch")
Rel(kproc, hudi_utils, "write_hudi(...)")
Rel(hudi_utils, minio, "Parquet + .hoodie/", "S3A")
Rel(hudi_utils, hms, "saveAsTable / sync", "Thrift")
Rel(s3main, minio, "checkpoint s3a://checkpoints/bronze-s3-stream-yc/*")
Rel(kmain, minio, "checkpoint s3a://checkpoints/bronze-kafka")
```

**Свойства:** `restartPolicy: Always`, `onFailureRetries: 10`; чекпойнты на S3 → resume after pod kill; exactly-once на уровне файлов через checkpoint + Hudi upsert по `composite_pk`/PK; dynamic allocation `min=1, max=3`.

### 4.2 Airflow DAG `transactions_pipeline`

```mermaid
C4Component
title Component — DAG transactions_pipeline (cron 0 2 * * *)

Container_Ext(trino, "Trino")
Container_Ext(spark_op, "Spark Operator")

Container_Boundary(dag, "DAG") {
  Component(wait, "wait_bronze_ready", "PythonSensor mode=reschedule", "SELECT 1 FROM hudi.bronze.ingest_watermarks WHERE table_name='transactions' AND source_partition='day={{ ds }}', soft_fail=true")
  Component(check, "check_partition_has_data", "ShortCircuitOperator", "rows_in_batch > 0?")
  Component(silver, "dbt_silver", "SparkKubernetesOperator", "dbt run --select silver --vars run_date")
  Component(gold, "dbt_gold", "SparkKubernetesOperator", "dbt run --select gold")
  Component(test, "dbt_test", "SparkKubernetesOperator", "dbt test")
  Component(spec, "_build_dbt_spec", "helper", "Шаблон SparkApplication v1beta2: image lab08/spark:3.5.8-hudi-1.1.1, dbt-project ConfigMap, sparkConf+volumes")
}

Rel(wait, trino, "tolerant SELECT (ignore TABLE_NOT_FOUND)")
Rel(check, trino, "tolerant SELECT")
Rel(silver, spec, "uses")
Rel(gold, spec, "uses")
Rel(test, spec, "uses")
Rel(silver, spark_op, "Apply CR")
Rel(gold, spark_op, "Apply CR")
Rel(test, spark_op, "Apply CR")
Rel(wait, check, "▶")
Rel(check, silver, "▶")
Rel(silver, gold, "▶")
Rel(gold, test, "▶")
```

`start_date=2026-04-24`, `catchup=True`, `max_active_runs=1`. Каждая dbt-таска создаёт ephemeral `SparkApplication` (`restartPolicy.type: Never`); `mainApplicationFile=local:///opt/spark/jobs/run_dbt.py`; dbt-проект из ConfigMap `dbt-project` (mount `/tmp/cm`), код из `spark-jobs-code` (mount `/opt/spark/jobs`); AWS keys через `envSecretKeyRefs` (secret `lab08-credentials`).

### 4.3 dbt-проект (`dbt/`)

```mermaid
flowchart LR
  subgraph "bronze (Hudi sources)"
    BTX[bronze.transactions]
    BCAN[bronze.cancellations]
    BER[bronze.exchange_rates]
    BUSR[bronze.users]
    BTU[bronze.test_users]
    BPC[bronze.promo_codes]
  end
  subgraph "silver (incremental, hudi merge)"
    STX["transactions_clean<br/>flags: is_user_missing, is_user_unknown,<br/>is_test_user, is_amount_invalid,<br/>is_revenue_eligible, is_promo_expired_at_use"]
    SCAN[cancellations_clean]
    SER[exchange_rates_daily]
    SERL[exchange_rates_long]
  end
  subgraph "gold (incremental, hudi merge)"
    GTBH[transactions_by_hour]
    GPBH[purchases_by_hour]
    GREV[revenue_daily]
    GREF[refunds_daily]
    GPROMO[promo_codes_analysis]
    GPEXP[promo_expired_usage_daily]
    GCAN[cancellations_summary]
    GCOH[user_cohorts]
    GDQ[dq_summary_daily]
  end
  BTX --> STX
  BUSR --> STX
  BTU --> STX
  BPC --> STX
  BCAN --> SCAN
  BER --> SER
  BER --> SERL
  STX --> GTBH
  STX --> GPBH
  STX --> GREV
  SERL --> GREV
  STX --> GPROMO
  BPC --> GPROMO
  STX --> GPEXP
  SCAN --> GREF
  SCAN --> GCAN
  STX --> GCOH
  STX --> GDQ
```

**Конфигурация:** `profile=lab08`, `dbt-spark` 1.8.0 `method: session`, `+materialized=incremental`, `+file_format=hudi`, `+incremental_strategy=merge`, `+location_root=s3a://lake/{silver,gold}`, `vars.base_currency=TGRK`. Hudi: `compression=zstd`, `clean.policy=KEEP_LATEST_COMMITS`, `commits.retained=2`, inline clustering на silver, column-stats index по `event_day, hour_of_day, is_test_user`.

**DQ:** ~30 generic dbt-тестов (`unique`, `not_null`, `accepted_values`) в `_silver.yml`, `_gold.yml`, `sources.yml` + 2 singular (recon bronze vs silver, orphan-rate cancellations).

---

## 5. Key Scenarios

### 5.1 Streaming ingest (S3 → bronze)

```mermaid
sequenceDiagram
    autonumber
    participant YC as YC S3
    participant Drv as bronze-s3-streaming Driver
    participant Exec as Spark Executor
    participant MinIO as MinIO
    participant HMS
    participant W as bronze.ingest_watermarks

    loop micro-batch
        Drv->>YC: list & read JSONL (S3A anon)
        YC-->>Exec: новые файлы
        Exec->>Exec: enrich (composite_pk, event_day, ingested_at)
        Exec->>MinIO: write_hudi(upsert)
        Exec->>HMS: register/alter
        Exec->>W: write_watermark(table, partition, rows_in_batch)
        Drv->>MinIO: commit checkpoint
    end
    Note over Drv,MinIO: pod restart → resume offset → idempotent upsert
```

### 5.2 Daily transformation (Airflow + dbt)

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Scheduler (0 2 * * *)
    participant S as wait_bronze_ready
    participant T as Trino
    participant SC as check_partition_has_data
    participant SO as Spark Operator
    participant DBT as dbt SparkApp

    Cron->>S: trigger (ds)
    loop poke=30s, timeout=10m, reschedule
        S->>T: SELECT 1 FROM hudi.bronze.ingest_watermarks ...
        T-->>S: rows? (ignore TABLE_NOT_FOUND)
    end
    S->>SC: ✓ ready
    SC->>T: SELECT rows_in_batch ...
    alt rows_in_batch > 0
        SC->>SO: spawn dbt-silver-{ts}
        DBT->>DBT: read bronze / write silver
        SO->>SO: spawn dbt-gold-{ts}
        SO->>SO: spawn dbt-test-{ts}
    else
        SC-->>Cron: skip
    end
```

### 5.3 Read path (Superset → Trino → Hudi)

```mermaid
sequenceDiagram
    participant SS as Superset
    participant T as Trino coordinator
    participant TW as Trino worker
    participant HMS
    participant M as MinIO

    SS->>T: SQL (catalog hudi)
    T->>HMS: getTable(gold.revenue_daily)
    HMS-->>T: location + schema
    T->>TW: distribute split
    TW->>M: GET parquet
    TW-->>T: rows
    T-->>SS: result
```

---

