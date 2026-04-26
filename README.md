# Lab08 — Транзакционная аналитика

Аналитический пайплайн для финтех-данных: батч-загрузка из S3, стриминг из Kafka,
medallion-хранилище на Hudi/MinIO, трансформации через dbt, визуализация в Superset.

---

## Архитектура

```
S3 / MinIO (lake/raw/)     ──► PySpark Structured Streaming (long-running SparkApp)
                                              │  • file source, foreachBatch → Hudi upsert
                                              │  • exactly-once на уровне файлов через checkpoint
Kafka (внешний)            ──► PySpark Structured Streaming (long-running SparkApp)
                                              │
                                             ▼
                            Hudi CoW tables (bronze/silver/gold) on MinIO
                                              │
                           ┌──────────────────┼──────────────────────────┐
                           ▼                  ▼                          ▼
                  Spark Thrift Server      Trino 470+               Trino → Superset
                  (client mode, HMS)       (read-only Hudi)         (dashboards)
                  ▲ JDBC :10000
                  │
            Long-running streaming SparkApplications (k8s, applied via `make up`):
              bronze-s3-streaming    (file source → bronze.* upsert; restartPolicy: Always)
              bronze-kafka-ingest    (Kafka source → bronze.events_kafka)

            Airflow DAGs (KubernetesExecutor):
              transactions_pipeline  (PythonSensor ждёт bronze rows → dbt silver → gold → test, */30 min)
```

**Стек:**

| Компонент | Версия | Роль |
|---|---|---|
| Apache Spark | 3.5.8 | structured streaming (S3 + Kafka), thrift server |
| Apache Hudi | 1.1.1 (CoW) | формат таблиц bronze/silver/gold |
| MinIO | 6.x | локальное S3-совместимое хранилище |
| Hive Metastore | 3.0.0 (postgres) | каталог таблиц |
| Trino | 470 | read-only query engine для BI |
| dbt-spark | 1.8.0 (thrift) | трансформации silver/gold |
| Apache Airflow | 2.10.4 (KubernetesExecutor) | оркестрация |
| Apache Superset | 4.x | дашборды |
| Kubernetes | kind / docker-desktop | runtime |

---

## Быстрый старт

### Предварительные требования

- Docker Desktop с включённым Kubernetes (`docker-desktop` context)
- `kubectl`, `helm`, `helmfile`, `helm-diff` plugin
- 16 GB RAM выделено для Docker Desktop
- Python 3.9+ (для `make superset-init`)

```bash
# Проверить готовность окружения
make check
```

### 1. Собрать образы

```bash
make images
# Builds:
#   lab08/spark:3.5.8-hudi-1.1.1   (Spark + Hudi + dbt-spark)
#   lab08/airflow:2.10.4     (Airflow + providers)
```

### 2. Поднять инфраструктуру

```bash
# Создать k8s/secrets.yaml из примера, вписать пароли
cp k8s/secrets.example.yaml k8s/secrets.yaml
# Отредактировать: s3.secret.key, kafka.bootstrap и т.д.

make secrets
make up
# Выполняет: namespaces → helmfile sync → HMS → spark-code configmap → dbt configmap
```

> **Первый запуск занимает ~10 минут** — MinIO Tenant, HMS (arm64 через QEMU), Airflow.

### 3. Загрузить DAG-файлы в Airflow

```bash
make airflow-dags
```

### 4. Настроить доступ к UI через браузер

```bash
# Добавить *.lab08.local в /etc/hosts (один раз, требует sudo)
make hosts

# Применить Ingress ресурсы (уже вызывается в make up)
make ingress
```

Открывать в браузере:

| Сервис | URL | Credentials |
|---|---|---|
| Airflow | http://airflow.lab08.local | admin / admin |
| Superset | http://superset.lab08.local | admin / admin |
| Trino | http://trino.lab08.local | — |
| MinIO | http://minio.lab08.local | minioadmin / ... |

> Если `make hosts` не сработал, альтернатива: `make port-forward` (старый подход с ручным port-forward).

### 5. Запустить пайплайн

```bash
# `make up` уже всё делает:
#   - kubectl apply на bronze-s3-streaming + bronze-kafka-ingest (long-running SparkApps,
#     стартуют сразу, держат checkpoint, рестартятся через restartPolicy: Always);
#   - bootstrap-pipeline ждёт пока gold наполнится.
#
# Streaming-SparkApps НЕ управляются Airflow'ом и не требуют ручного триггера —
# они стартуют декларативно вместе с инфрой и работают непрерывно.
#
# DAG `transactions_pipeline` (cron */30) сам через PythonSensor ждёт пока в
# bronze.transactions появятся строки и только потом запускает dbt silver/gold/test.
# Никаких ручных триггеров для запуска ingest не нужно.

# Точечно перезапустить streaming (например после обновления PySpark-скрипта):
make streaming-apps

# Точечно прогнать dbt-пайплайн:
make airflow-trigger-pipeline
```

### 6. Создать дашборды в Superset

```bash
make superset-init
# Создаёт: database connection, 6 datasets, 6 charts, 1 dashboard
# Open: http://localhost:8088/dashboard/list
```

### 7. Kafka streaming

`bronze-kafka-ingest` поднимается автоматически через `make streaming-apps`
(вызывается из `make up`). Долгоживущий SparkApplication, перезапускается через
`restartPolicy: Always`. Ручного триггера не требуется.

```bash
# Перезапустить (например после правок bronze_kafka_ingest.py):
kubectl -n spark-jobs delete sparkapplication bronze-kafka-ingest --ignore-not-found
make streaming-apps
```

---

## Слои данных (Medallion)

### Bronze — сырые данные + технические поля

| Таблица | Источник | Темп прихода | PK |
|---|---|---|---|
| `bronze.transactions` | S3 streaming (file source, JSONL) | ~10 мин слот | `composite_pk` = `transaction_id\|created_at\|user_id` |
| `bronze.cancellations` | S3 streaming | 1 раз в день | `cancellation_id` |
| `bronze.exchange_rates` | S3 streaming | 2-3 раза в день | `update_id` |
| `bronze.users` | S3 streaming (reference) | snapshot | `user_id` |
| `bronze.test_users` | S3 streaming (reference) | snapshot | `test_user_uuid` |
| `bronze.promo_codes` | S3 streaming (reference) | snapshot | `promo_code_id` |
| `bronze.events_kafka` | Kafka streaming | real-time | `composite_pk` |

Все bronze-таблицы наполняются **через Hudi upsert**, поэтому корректно отрабатывают
дубли, поздние перезаливки файлов и late-arriving данные. Поле `_ingested_at` —
precombine для Hudi.

### Silver — типизировано, дедуплицировано, обогащено

| Модель | Что делает |
|---|---|
| `silver.transactions_clean` | Дедуп по `composite_pk` (latest по `_ingested_at`), флаги качества |
| `silver.cancellations_clean` | Парсинг `cancelled_at` → timestamp, флаг невалидного refund |
| `silver.exchange_rates_daily` | Latest rate per day, PK `rate_day` |

### Gold — витрины под бизнес-вопросы

| Модель | Описание |
|---|---|
| `gold.transactions_by_hour` | Распределение по часам с разбивкой is_test_user |
| `gold.purchases_by_hour` | Только `purchase + completed`, по часам |
| `gold.revenue_daily` | Выручка в TGRK, конвертация через forward-fill курсов |
| `gold.promo_codes_analysis` | Использование vs лимиты, просроченные |
| `gold.cancellations_summary` | % отмен по дням/причинам, среднее время до отмены |
| `gold.user_cohorts` | Новые vs возвращающиеся пользователи по дням |

---

## Решения по обработке грязных данных

| Проблема | Решение |
|---|---|
| Дублирующиеся `transaction_id` (~5%) | `composite_pk = concat(transaction_id, created_at, user_id)` — уникальный ключ Hudi; silver берёт latest по `_ingested_at` |
| Пустой `user_id` (~5%) | Не дропаем, флаг `is_user_missing = true`; витрины считают с/без unmatched |
| Несуществующий `user_id` (~3%) | Флаг `is_user_unknown = true` через LEFT JOIN к `bronze.users` |
| Тестовые пользователи (~20% в будни) | Флаг `is_test_user = true` через JOIN к `bronze.test_users`; все золотые витрины фильтруют `is_test_user = false` |
| Нулевые/отрицательные суммы (~11%) | Флаг `is_amount_invalid = true`; в выручку не включаются |
| Просроченные промокоды (~2%) | Флаг `used_after_expiry` в `gold.promo_codes_analysis` |
| Late-arriving cancellations | Hudi upsert + silver incremental с lookback 7 дней |
| Разные форматы дат | `created_at` (unix) → `created_ts`; `cancelled_at` (строка "2025 Oct 06 14:30") → `cancelled_ts` в silver |
| Отсутствие курсов в отдельные дни | Forward-fill: `max(rate_day) <= event_day` через JOIN |

---

## Оркестрация

**Streaming ingest (вне Airflow).** Bronze-слой наполняется долгоживущими
SparkApplication'ами `bronze-s3-streaming` и `bronze-kafka-ingest`, которые
применяются декларативно через `kubectl apply` в `make up` (target
`make streaming-apps`). Они держат checkpoint на S3 и рестартятся через
`restartPolicy: Always`, поэтому не требуют ни Airflow-триггера, ни ручного
запуска. Логика стримов не меняется со временем — деплой = создание ресурса в
кластере.

**Airflow DAG.**

| DAG | Расписание | Шаги |
|---|---|---|
| `transactions_pipeline` | `*/30 * * * *` | wait_bronze_ready (PythonSensor → Trino: ждёт rows в `hudi.bronze.transactions`) → dbt_silver → dbt_gold → dbt_test |

Сенсор закрывает race между ingest и трансформациями: на холодном старте Spark
может ещё не успеть наполнить bronze — DAG автоматически ждёт появления данных,
ничего вручную триггерить не нужно. На последующих запусках sensor проходит
мгновенно. `mode='reschedule'` освобождает worker-slot между poke-итерациями.

**Bronze ingest detail.** Long-running Spark Structured Streaming (4 параллельных
file-source стрима в одной JVM: transactions / cancellations / exchange_rates /
reference). Чекпойнты на `s3a://hudi/.checkpoints/bronze-s3-stream/*` гарантируют,
что рестарт SparkApplication продолжает с offset'а. Дедупликация и поздние
перезаливки файлов закрываются Hudi upsert'ом по `composite_pk`/`record_key`.

**dbt-таски** запускаются через `KubernetesPodOperator` с образом `lab08/spark:3.5.8-hudi-1.1.1`,
который содержит dbt-core + dbt-spark. Проект dbt разворачивается из ConfigMap `dbt-project`.

---

## DQ-тесты

- **30 generic dbt tests** — unique, not_null, accepted_values на всех моделях
- `tests/recon_silver_transactions_count.sql` — сверка count bronze vs silver (drift > 5% = WARN)
- `tests/recon_cancellations_orphan_rate.sql` — % отмен без транзакции (> 10% = WARN на sample, строже на проде)

---

## Структура репозитория

```
lab08/
├── README.md                    # этот файл
├── PLAN.md                      # архитектурные решения (ADR) и lessons learned
├── Makefile                     # make up / make down / make superset-init / ...
├── helmfile.yaml                # декларативная установка всего стека
├── helm-values/                 # values для каждого helm chart
├── docker/
│   ├── spark/Dockerfile         # Spark 3.5.8 + Hudi 1.1.1 + dbt-spark
│   └── airflow/Dockerfile       # Airflow 2.10.4 + providers
├── k8s/
│   ├── namespaces.yaml
│   ├── secrets.example.yaml
│   ├── hive-metastore.yaml
│   ├── spark-rbac.yaml
│   ├── airflow-rbac.yaml
│   └── spark-applications/
│       ├── bronze-s3-streaming.yaml
│       ├── bronze-kafka-ingest.yaml
│       └── thrift-server.yaml
├── spark-jobs/                  # PySpark скрипты (единственный источник правды)
│   ├── bronze_s3_streaming.py
│   └── bronze_kafka_ingest.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/              # 3 модели
│   │   └── gold/                # 6 моделей
│   └── tests/                   # 2 singular DQ-теста
├── airflow/
│   └── dags/
│       └── transactions_pipeline.py   # единственный DAG: sensor + dbt silver/gold/test
├── superset/
│   └── init_dashboards.py       # bootstrap script: создаёт charts/dashboard через API
└── sample/                      # sample данные от организаторов
```

---

## Полезные команды

```bash
# Состояние всех подов
make status

# Пересобрать ConfigMap с кодом (после изменений в spark-jobs/)
make spark-code

# Пересобрать ConfigMap с dbt проектом (после изменений в dbt/)
make dbt-configmap

# Удалить всё
make down
```