# Детальная архитектура

## Назначение

Lab08 — локально разворачиваемый лейкхаус для финтех-данных. Поднимается одной командой и закрывает полный цикл: ingest, хранение, трансформации, BI.

- **Источники данных:** JSONL-файлы из Yandex Cloud S3 (бакет `npl-de18-lab8-data`) и поток событий из внешней Kafka.
- **Хранилище:** Apache Hudi (Copy-on-Write) поверх MinIO, разложено по слоям `bronze / silver / gold`.
- **Трансформации:** dbt-spark, режим `session`, incremental-модели через Hudi `merge`.
- **Каталог:** Hive Metastore с Postgres-бэкендом.
- **Чтение:** Trino 470 (read-only), Apache Superset 4.x для дашбордов.
- **Оркестрация:** Airflow 2.10.4 на `KubernetesExecutor`. Стриминг — долгоживущие `SparkApplication` CRD.
- **Метрики:** kube-prometheus-stack, Spark Prometheus servlet, statsd-exporter для Airflow.

Среда — Kubernetes (kind или Docker Desktop). Конфигурация декларативная: `helmfile.yaml` плюс raw-манифесты в `k8s/`.

---

## Контекст

```mermaid
C4Context
title Lab08 — System Context

Person(de, "Data Engineer", "Разворачивает и эксплуатирует платформу")
System(platform, "Lab08 Platform", "Lakehouse в Kubernetes")
System_Ext(ycs3, "Yandex Cloud S3", "JSONL: transactions, cancellations, rates, reference")
System_Ext(kafka, "External Kafka", "Поток событий")

Rel(ycs3, platform, "JSONL", "S3A HTTPS")
Rel(kafka, platform, "События", "Kafka")
Rel(de, platform, "make up / kubectl / browser")
```

На границе системы — два внешних источника данных и один пользователь, дата-инженер.

---

## Контейнеры

Платформа сгруппирована по логическим слоям. Полная карта компонентов с версиями приведена ниже, в таблице.

```mermaid
flowchart LR
    subgraph Ext["Внешние источники"]
        YCS3[YC S3]
        KFK[Kafka]
    end

    subgraph Ingest["Ingest (ns: spark-jobs)"]
        S3S[bronze-s3-streaming]
        KS[bronze-kafka-ingest]
    end

    subgraph Storage["Storage (ns: storage / data-platform)"]
        MinIO[(MinIO Tenant)]
        HMS[Hive Metastore]
    end

    subgraph Transform["Transform (ns: spark-jobs)"]
        DBT[dbt SparkApp]
    end

    subgraph Serve["Serve (ns: data-platform)"]
        Trino[Trino]
        SS[Superset]
    end

    subgraph Orchestration["Orchestration (ns: data-platform)"]
        AF[Airflow]
        SO[Spark Operator]
    end

    YCS3 --> S3S
    KFK --> KS
    S3S --> MinIO
    KS --> MinIO
    S3S --> HMS
    KS --> HMS

    AF --> SO --> DBT
    DBT --> MinIO
    DBT --> HMS

    Trino --> HMS
    Trino --> MinIO
    SS --> Trino
```

В диаграмме опущены вспомогательные связи (HMS → Postgres, Prometheus scraping, ingress) — они описаны в таблице протоколов.

### Состав

| Компонент | Версия | Namespace |
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

**Собственные образы:** `lab08/spark:3.5.8-hudi-1.1.1` (Spark + Hudi + Hadoop S3A + Kafka + dbt-spark 1.8.0 + код задач) и `lab08/airflow:2.10.4` (с провайдерами cncf-kubernetes, trino, statsd).

### Протоколы взаимодействия

| Источник → Приёмник | Протокол / формат | Аутентификация |
|---|---|---|
| Spark / Trino → MinIO | S3A HTTP / Parquet | secret `lab08-credentials` |
| Spark → YC S3 | S3A HTTPS / JSONL | Anonymous provider |
| Spark / Trino / dbt → HMS | Thrift | внутри кластера |
| HMS → Postgres | JDBC | `hive/hive` |
| Airflow → Spark Operator | K8s API (`SparkApplication` v1beta2) | SA `airflow` (RBAC в `k8s/airflow-rbac.yaml`) |
| Airflow → Trino | HTTP REST | connection `trino_default` |
| Superset → Trino | HTTP REST | DB connection |
| Spark → Kafka | Kafka | env `KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC` |
| Prometheus → Spark | HTTP `/metrics/prometheus/` | — |
| Airflow → StatsD | UDP 9125 | — |

---

## Компоненты

### Стриминговый ingest

```mermaid
flowchart LR
    YCS3[YC S3] --> S3main[bronze_s3_streaming.py]
    KFK[Kafka] --> Kmain[bronze_kafka_ingest.py]

    S3main --> Handlers[handlers: transactions / cancellations / rates / reference]
    Kmain --> KProc[process_batch: split by _source]

    Handlers --> HU[hudi_utils.write_hudi]
    KProc --> HU
    Handlers --> WM[watermark_utils → bronze.ingest_watermarks]

    HU --> MinIO[(MinIO: lake/, checkpoints/)]
    HU --> HMS[HMS]
```

Оба `SparkApplication` запускаются с `restartPolicy: Always`, `onFailureRetries: 10`. Чекпойнты лежат на S3 — при рестарте пода обработка продолжается с того же места. Exactly-once на уровне файлов обеспечивается связкой checkpoint + Hudi upsert по `composite_pk` (или обычному PK). Dynamic allocation: 1–3 executor.

### DAG `transactions_pipeline`

```mermaid
flowchart LR
    Wait[wait_bronze_ready<br/>PythonSensor] --> Check[check_partition_has_data<br/>ShortCircuit]
    Check -->|rows > 0| Silver[dbt_silver]
    Check -->|rows = 0| Skip([skip])
    Silver --> Gold[dbt_gold]
    Gold --> Test[dbt_test]
```

Расписание: `0 2 * * *`. `start_date=2026-04-24`, `catchup=True`, `max_active_runs=1`.

`wait_bronze_ready` опрашивает `hudi.bronze.ingest_watermarks` через Trino (poke 30s, timeout 10m, mode=reschedule). `check_partition_has_data` пропускает downstream-таски, если в партиции нет строк.

Каждая dbt-таска поднимает одноразовый `SparkApplication` (`restartPolicy: Never`). `mainApplicationFile=local:///opt/spark/jobs/run_dbt.py`. dbt-проект подкладывается из ConfigMap `dbt-project` (mount `/tmp/cm`), код задач — из `spark-jobs-code` (mount `/opt/spark/jobs`). AWS-ключи прокидываются через `envSecretKeyRefs` из секрета `lab08-credentials`.

### dbt-проект

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
    STX[transactions_clean]
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

Профиль `lab08`, адаптер `dbt-spark` 1.8.0 в режиме `method: session`. Дефолты: `materialized=incremental`, `file_format=hudi`, `incremental_strategy=merge`, `location_root=s3a://lake/{silver,gold}`. Базовая валюта (`vars.base_currency`) — `TGRK`.

Hudi-настройки: `compression=zstd`, `clean.policy=KEEP_LATEST_COMMITS`, `commits.retained=2`, inline clustering на silver, column-stats индекс по `event_day, hour_of_day, is_test_user`.

В `transactions_clean` выставляются флаги качества: `is_user_missing`, `is_user_unknown`, `is_test_user`, `is_amount_invalid`, `is_revenue_eligible`, `is_promo_expired_at_use`.

**Проверки качества:** около 30 generic dbt-тестов (`unique`, `not_null`, `accepted_values`) в `_silver.yml`, `_gold.yml`, `sources.yml`, плюс 2 singular — recon bronze vs silver и orphan-rate cancellations.

---

## Сценарии работы

### Стриминговый ingest S3 → bronze

```mermaid
sequenceDiagram
    autonumber
    participant YC as YC S3
    participant Drv as Driver
    participant Exec as Executor
    participant MinIO
    participant HMS
    participant W as ingest_watermarks

    loop micro-batch
        Drv->>YC: list & read JSONL
        YC-->>Exec: новые файлы
        Exec->>Exec: enrich (composite_pk, event_day, ingested_at)
        Exec->>MinIO: write_hudi(upsert)
        Exec->>HMS: register / alter
        Exec->>W: write_watermark
        Drv->>MinIO: commit checkpoint
    end
    Note over Drv,MinIO: рестарт пода → resume offset → идемпотентный upsert
```

### Ежедневные трансформации

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Scheduler
    participant S as wait_bronze_ready
    participant T as Trino
    participant SC as check_partition
    participant SO as Spark Operator

    Cron->>S: trigger (ds)
    loop poke 30s, timeout 10m
        S->>T: SELECT 1 FROM ingest_watermarks
    end
    S->>SC: ready
    SC->>T: SELECT rows_in_batch
    alt rows > 0
        SC->>SO: spawn dbt-silver → dbt-gold → dbt-test
    else
        SC-->>Cron: skip
    end
```

### Чтение Superset → Trino → Hudi

```mermaid
sequenceDiagram
    participant SS as Superset
    participant T as Trino
    participant HMS
    participant M as MinIO

    SS->>T: SQL (catalog hudi)
    T->>HMS: getTable
    HMS-->>T: location + schema
    T->>M: GET parquet
    M-->>T: rows
    T-->>SS: result
```
