# Детальная архитектура

## Назначение

Lab08 — локально разворачиваемый лейкхаус для финтех-данных. Поднимается одной командой и закрывает полный цикл: ingest, хранение, трансформации, BI.

- **Источники данных:** JSONL-файлы из Yandex Cloud S3 (бакет `npl-de18-lab8-data`) и поток событий из внешней Kafka.
- **Хранилище:** Apache Hudi (Copy-on-Write) поверх MinIO, разложено по слоям `bronze / silver / gold`.
- **Трансформации:** dbt-spark (`method: session`, Hudi `merge`); + streaming-medallion — долгоживущий Spark-стрим, который держит `gold.*_live` витрины (NRT-слой Lambda).
- **Каталог:** Hive Metastore поверх общего Postgres (raw-манифест `k8s/postgres.yaml`, одна БД на HMS / Airflow / Superset).
- **Чтение:** Trino 470 (read-only), Apache Superset 4.x. Lambda-витрины собираются как Superset virtual datasets — UNION ALL settled (`gold.<x>`) + live (`gold.<x>_live`) с cutover по `max(event_day)`.
- **Оркестрация:** Airflow 2.10.4 на `KubernetesExecutor`. Bronze S3 и медальон — посуточные DAG'и; Kafka-ingest и streaming-medallion — долгоживущие `SparkApplication`.
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
        S3B[bronze-s3-batch<br/>daily, per source]
        REF[bronze-reference-batch<br/>one-shot]
        KS[bronze-kafka-ingest<br/>streaming, optional]
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

    YCS3 --> S3B
    YCS3 --> REF
    KFK --> KS
    S3B --> MinIO
    REF --> MinIO
    KS --> MinIO
    S3B --> HMS
    REF --> HMS
    KS --> HMS

    AF --> SO --> DBT
    AF --> S3B
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
| Postgres (shared: HMS / Airflow / Superset) | 16 (raw manifest) | `data-platform` |
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

### Bronze ingest

Lambda-разделение: batch — источник истины, Kafka — speed-слой.

```mermaid
flowchart LR
    YCS3[YC S3] --> S3B[bronze_s3_batch.py<br/>--source --ds]
    YCS3 --> REF[bronze_reference_batch.py]
    KFK[Kafka] --> Kmain[bronze_kafka_ingest.py]
    BOOT[bootstrap_layer_skeletons.py<br/>one-shot]

    S3B --> HU[utils.hudi.write_hudi]
    REF --> HU
    Kmain --> HU
    BOOT --> HU
    S3B --> WM["utils.watermark →<br/>bronze.ingest_watermarks_&lt;source&gt;"]
    Kmain --> WMK[bronze.ingest_watermarks_kafka]
    Kmain --> DLQ[bronze_dlq.late_events]

    HU --> MinIO[(MinIO: lake/, checkpoints/)]
    HU --> HMS[HMS]
```

**S3 batch (`bronze_s3_batch.py`)** — один источник за одни сутки. Идемпотентный upsert по `composite_pk` / `cancellation_pk` / `rate_pk`. Derived-колонки (`event_day`, `ingestion_day`, PK, `ingested_at`) считаются единым модулем `spark/utils/bronze_transforms.py` — общим с Kafka-writer'ом, чтобы партиционирование и ключи были байт-в-байт одинаковыми. Для transactions/cancellations выполняется cutover-merge с Kafka-резидуалом и `insert_overwrite` на партицию (S3 — источник истины для своих партиций). Cancellations партиционируются по `ingestion_day`, а не по `event_day` — late-arriving отмена не конкурирует с уже закрытой батч-партицией (см. ADR-004). Watermark пишет каждый источник в свой шард `bronze.ingest_watermarks_<source>` — single-writer per shard, без race-condition.

**Reference (`bronze_reference_batch.py`)** — one-shot загрузка `users / test_users / promo_codes`.

**Bootstrap (`bootstrap_layer_skeletons.py`)** — one-shot: создаёт пустые Hudi-таблицы под bronze, DLQ и `gold.*_live`. Последние нужны, чтобы Superset UNION ALL не падал до первого commit стрима.

**Kafka (`bronze_kafka_ingest.py`)** — долгоживущий `SparkApplication`. Чекпоинт на S3, watermark консолидирован в `ingest_watermarks_kafka`. Запускается из `make up`.

**Streaming medallion (`streaming_medallion.py`)** — второй долгоживущий стрим. Читает bronze как Hudi-stream, агрегирует и пишет в `gold.{transactions_by_hour,purchases_by_hour,cancellations_summary}_live` и `gold.exchange_rates_latest`. Routing по watermark: `event_day ≤ s3_high_watermark` отбрасывается (это уже у dbt). Single-writer на каждый output — нет конкурентов dbt.

### DAG `bronze_s3_ingest`

```mermaid
flowchart LR
    Check[check_source_day<br/>ShortCircuit] -->|day exists| Tx[bronze_transactions]
    Check --> Can[bronze_cancellations]
    Check --> Er[bronze_exchange_rates]
    Check -->|day missing| Skip([skip])
```

Расписание `0 2 * * *`, `start_date=2026-04-24`, `catchup=True`, `max_active_runs=1`, T-1 (грузит `ds = вчера`). `check_source_day` — ShortCircuit-гейт на наличие S3-партиции: если директории нет, слот сразу skipped без ретраев. Три bronze-таски запускаются параллельно одноразовыми `SparkApplication` через `SparkKubernetesOperator`. Каждая после успешного upsert пишет строку в свой шард `bronze.ingest_watermarks_<source>`.

Ресурсные профили per source (`_common.BRONZE_RESOURCE_PROFILES`):

| Источник | Driver | Executor | Instances |
|---|---|---|---|
| `transactions` | 1g + 256m | 1g + 512m | 2 (dyn 2–3) |
| `cancellations` | 712m + 128m | 1g + 128m | 1 |
| `exchange_rates` | 712m + 128m | 1g + 128m | 1 |

### DAG `transactions_medallion`

```mermaid
flowchart LR
    Wait[bronze_ready<br/>PythonSensor reschedule] --> Silver[dbt_silver]
    Silver --> Gold[dbt_gold]
    Gold --> Test[dbt_test]
```

Расписание `30 2 * * *` (сдвиг даёт bronze ~30 мин на завершение). `bronze_ready` — `PythonSensor(mode=reschedule, poke=60s, timeout≈6m)` опрашивает все три шарда `bronze.ingest_watermarks_{transactions,cancellations,exchange_rates}` на нужный `ds`. При таймауте DAGRun падает — видно в UI (без silent-skip).

Каждая dbt-таска поднимает одноразовый `SparkApplication` (`restartPolicy: Never`, `mainApplicationFile=local:///opt/spark/jobs/run_dbt.py`). dbt-проект — из ConfigMap `dbt-project` (`/tmp/cm`), код задач — из `spark-jobs-code` (`/opt/spark/jobs`). AWS-ключи прокидываются через `envSecretKeyRefs` из `lab08-credentials`.

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
  subgraph "gold.*_live (streaming, single-writer)"
    GTBHL[transactions_by_hour_live]
    GPBHL[purchases_by_hour_live]
    GCANL[cancellations_summary_live]
    GERL2[exchange_rates_latest]
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

Профиль `lab08`, адаптер `dbt-spark` 1.8.0 в режиме `method: session`. Дефолты: `materialized=incremental`, `file_format=hudi`, `incremental_strategy=merge`, `location_root=s3a://lake/{silver,gold}`. Базовая валюта (`vars.base_currency`) — `TGRK`. Витрины по cancellations (`cancellations_summary`, `refunds_daily`) — `materialized='table'` (десятки строк, безопаснее пересобирать целиком).

Hudi-настройки: CoW + HMS sync, `compression=zstd`, `clean.policy=KEEP_LATEST_COMMITS`, `commits.retained=10`, async clean, archive off с `keep.min/max.commits = 30/40`. Индексы: `SIMPLE` для partition-bound PK (transactions), `GLOBAL_SIMPLE` там, где record-key может «переезжать» между партициями (cancellations, exchange_rates, gold-витрины). Concurrency — single-writer per shard + `InProcessLockProvider`.

В `transactions_clean` выставляются флаги качества: `is_user_missing`, `is_user_unknown`, `is_test_user`, `is_amount_invalid`, `is_revenue_eligible`, `is_promo_expired_at_use`.

**Проверки качества:** около 30 generic dbt-тестов (`unique`, `not_null`, `accepted_values`) в `_silver.yml`, `_gold.yml`, `sources.yml`, плюс 2 singular — recon bronze vs silver и orphan-rate cancellations.

### Lambda-unified BI-слой

dbt держит settled-витрины за закрытые дни, streaming-medallion — `*_live` за свежие. Сшивает Superset через virtual dataset (`gold.*_unified`): UNION ALL settled и live, фильтр по `event_day > max(settled)`. Cutover автоматический. SQL в `superset/init_dashboards/datasets.py`. Не dbt view, потому что Trino-Hudi коннектор Hive views не читает.

---

## Сценарии работы

### Bronze ingest S3 → Hudi (daily batch)

```mermaid
sequenceDiagram
    autonumber
    participant AF as Airflow
    participant SO as Spark Operator
    participant Drv as bronze_s3_batch.py
    participant YC as YC S3
    participant MinIO
    participant HMS
    participant W as ingest_watermarks

    AF->>AF: check_source_day (ShortCircuit)
    AF->>SO: spawn bronze_{source} (×3 parallel)
    SO->>Drv: --source --ds
    Drv->>YC: read day=<ds>/**/*.jsonl
    Drv->>Drv: enrich (composite_pk, event_day, ingested_at)
    Drv->>MinIO: write_hudi(upsert)
    Drv->>HMS: register / alter
    Drv->>W: write_watermark(ds) per source shard
    Note over Drv: retry / clear DAGRun → идемпотентный upsert
```

### Ежедневные трансформации

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Scheduler
    participant S as bronze_ready (PythonSensor)
    participant T as Trino
    participant SO as Spark Operator

    Cron->>S: trigger (ds, 02:30 UTC)
    loop poke 60s, timeout ~6m
        S->>T: SELECT 1 FROM ingest_watermarks WHERE ds = ...
    end
    alt watermark present
        S->>SO: spawn dbt-silver → dbt-gold → dbt-test
    else timeout
        S-->>Cron: fail (visible in UI)
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
