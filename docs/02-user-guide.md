# Руководство пользователя

Lab08 — учебный стенд транзакционной аналитики, разворачиваемый локально командой `make up`. Данные поступают из YC S3 и Kafka, сохраняются в Hudi-таблицы поверх MinIO, проходят через dbt и публикуются в дашбордах Superset.

Документ описывает повседневную работу со стендом. Устройство платформы — см. `01-detailed-architecture.md`.

Управление выполняется через `make`-таргеты в корне репозитория и веб-интерфейсы на `*.lab08.local`. Ручных операций в кластере не требуется.

---

## Первый запуск

### Предусловия

| Требование | Назначение |
|---|---|
| Docker Desktop с включённым Kubernetes, контекст `docker-desktop`, ≥ 16 GB RAM | основной runtime |
| `kubectl`, `helm`, `helmfile`, плагин `helm-diff` | управление кластером |
| Python 3.9+ | требуется для `make superset-init` |
| Креды S3 и Kafka в `k8s/secrets.yaml` (создаётся из `secrets.example.yaml`) | доступ к внешним источникам |
| `sudo` | требуется для `make hosts` (модифицирует `/etc/hosts`) |

### Установка

1. **Проверка окружения:** `make check`. Ожидается `kubectl OK / helm OK / helmfile OK / helm-diff OK` и список нод.
2. **Секреты:** `cp k8s/secrets.example.yaml k8s/secrets.yaml`, заполнить AWS- и Kafka-креды.
3. **Развёртывание:** `make up`. Таргет идемпотентный: сборка образов → helmfile sync → HMS → ConfigMap'ы → DAG'и → unpause. Холодный старт занимает около 10 минут (MinIO Tenant, HMS на arm64 через QEMU, Airflow стартуют долго).
4. **DNS:** `make hosts` — добавляет `*.lab08.local` в `/etc/hosts`.
5. **Проверка состояния:** `make status`. Все pod'ы должны быть в статусе `Running` или `Completed`.
6. **Дашборды:** `make superset-init` — создаёт 6 чартов и один дашборд в Superset.

Если `make up` падает на этапе HMS, следует подождать 5 минут и повторить запуск: Postgres и HMS на arm64 поднимаются медленно.

### Точки входа

| Сервис | URL | Логин |
|---|---|---|
| Airflow | http://airflow.lab08.local | admin / admin |
| Superset | http://superset.lab08.local | admin / admin |
| Trino | http://trino.lab08.local | — |
| MinIO | http://minio.lab08.local | minioadmin / … |
| Grafana | http://grafana.lab08.local | admin / admin |
| Prometheus | http://prometheus.lab08.local | — |

- **Airflow** — мониторинг DAG `transactions_pipeline`, ручной триггер, логи dbt-таcк.
- **Superset** — дашборд `Lab08 Transactions Overview` и SQL Lab.
- **Trino** — состояние запросов и worker'ов.
- **MinIO Console** — содержимое бакетов `lake/` и `checkpoints/`.
- **Grafana** — дашборд `lab08-overview` с метриками Spark и Airflow.
- **Prometheus** — сырые метрики и алерты.

---

## Роли и операции

В системе одна роль — **Data Engineer**. Multi-tenant ACL не предусмотрен (учебный стенд).

```mermaid
flowchart LR
  DE(["Data Engineer"])
  UC1[/"Развернуть / снести стенд"/]
  UC2[/"Запустить / триггернуть DAG"/]
  UC3[/"Обновить spark-jobs или dbt-проект"/]
  UC4[/"Диагностировать сбой"/]
  UC5[/"Создать дашборды Superset"/]
  UC6[/"Запросить Trino ad-hoc"/]

  DE --> UC1
  DE --> UC2
  DE --> UC3
  DE --> UC4
  DE --> UC5
  DE --> UC6
```

| Задача | Команда |
|---|---|
| Развернуть / снести | `make up` / `make down` |
| Триггернуть DAG | `make airflow-trigger-pipeline` или Airflow UI |
| Обновить PySpark-код | правка `spark-jobs/*.py` → `make spark-code` → `make streaming-apps` |
| Обновить dbt | правка `dbt/` → `make dbt-configmap` |
| Создать дашборды | `make superset-init` |
| Ad-hoc SQL | Trino UI или Superset SQL Lab по `hudi.bronze.*`, `hudi.silver.*`, `hudi.gold.*` |

---

## Сценарии

### A. Развёртывание с нуля

Шаги совпадают с разделом «Установка» выше. При сбоях — см. раздел «Диагностика».

### B. Ручной запуск пайплайна

Цель — прогнать `bronze → silver → gold → tests` для текущего `data_interval_start`.

Предусловия: стенд развёрнут, стримы `bronze-s3-streaming` и `bronze-kafka-ingest` в статусе `RUNNING`, в `bronze.ingest_watermarks` есть записи.

1. Убедиться, что стримы работают: `kubectl -n spark-jobs get sparkapplication`.
2. Опционально — проверить наличие данных в bronze: `make verify-trino`.
3. Триггернуть DAG: `make airflow-trigger-pipeline` либо кнопка «Trigger DAG» в Airflow UI.
4. В Graph view убедиться, что цепочка `wait_bronze_ready → check_partition_has_data → dbt_silver → dbt_gold → dbt_test` завершилась успешно. Логи dbt доступны в task-логах (driver pod).

Сенсор сам ожидает прихода данных; отдельно триггерить ingest не нужно.

### C. Обновление кода

**PySpark / streaming:**

1. Внести правки в `spark-jobs/*.py`.
2. `make spark-code` — пересоздаёт ConfigMap `spark-jobs-code`.
3. Перезапустить нужный SparkApplication:
   ```bash
   kubectl -n spark-jobs delete sparkapplication bronze-s3-streaming --ignore-not-found
   make streaming-apps
   ```
4. Проверить запуск нового driver: `kubectl -n spark-jobs get pods -w`.

**dbt:**

1. Внести правки в `dbt/models/` или `dbt/tests/`.
2. `make dbt-configmap` — пересоздаёт ConfigMap `dbt-project`.
3. Триггернуть DAG (`make airflow-trigger-pipeline`). Следующий ephemeral `SparkApplication` подхватит обновлённый ConfigMap.

### D. Просмотр данных

Через Trino UI / CLI, каталог `hudi`:

```sql
SELECT * FROM hudi.gold.revenue_daily ORDER BY event_day DESC LIMIT 30;
SELECT count(*) FROM hudi.bronze.transactions;
SELECT * FROM hudi.bronze.ingest_watermarks ORDER BY committed_at DESC LIMIT 20;
```

В Superset — готовый дашборд `Lab08 Transactions Overview` или SQL Lab на том же каталоге.

### E. Снос стенда

`make down` — выполняет `helmfile destroy` и удаляет HMS. Для удаления данных MinIO — дополнительно `docker volume prune`.

При сносе PVC MinIO удаляется, все Hudi-таблицы пропадают. На следующем `make up` всё пересоздаётся, но значения `_ingested_at` будут новыми.

---

## Модель данных

### Слои (medallion)

- **bronze.*** — сырые данные плюс технические поля (`composite_pk`, `event_day`, `ingested_at`). Hudi upsert, exactly-once на уровне файлов через checkpoint.
- **silver.*** — типизация, дедуп, флаги качества: `is_user_missing`, `is_user_unknown`, `is_test_user`, `is_amount_invalid`, `is_revenue_eligible`, `is_promo_expired_at_use`.
- **gold.*** — витрины: `transactions_by_hour`, `purchases_by_hour`, `revenue_daily`, `refunds_daily`, `cancellations_summary`, `promo_codes_analysis`, `promo_expired_usage_daily`, `user_cohorts`, `dq_summary_daily`.

### Бизнес-правила

| Правило | Реализация |
|---|---|
| Дубли `transaction_id` (~5%) разрешаются через `composite_pk = concat_ws('|', transaction_id, created_at, user_id)` | bronze ingest, Hudi record_key |
| `is_test_user=true` исключаются из gold-витрин | silver и gold-фильтры |
| `is_revenue_eligible=false` (нулевые / отрицательные суммы, тестовые пользователи) не попадают в `revenue_daily` | silver |
| `is_promo_expired_at_use` — промокод применён после `expiry_date` | silver + `promo_expired_usage_daily` |
| Forward-fill курсов: `max(rate_day) <= event_day` | `gold.revenue_daily` |
| Late-arriving cancellations | lookback 7 дней в silver |
| Базовая валюта — `TGRK` | dbt `vars.base_currency` |

### Проверки качества

- ~30 generic dbt-тестов (`unique`, `not_null`, `accepted_values`).
- 2 singular: recon bronze vs silver count (drift > 5% → WARN) и `cancellations orphan_rate` (> 10% → WARN).
- Запускаются шагом `dbt_test` в DAG. Ручной запуск — `dbt test --vars '{run_date: ...}'` в driver pod.

---

## Диагностика

| Симптом | Решение |
|---|---|
| `make check` сообщает «Wrong context» | `kubectl config use-context docker-desktop` |
| Pod'ы HMS / Airflow в `Pending` или `CrashLoopBackOff` после `make up` | Подождать 5–10 минут (на arm64 через QEMU инициализация занимает много времени). Если не помогло — `kubectl -n data-platform logs <pod>` |
| `make up` падает на secrets | Не создан `k8s/secrets.yaml`. Создать из `secrets.example.yaml` |
| DAG `transactions_pipeline` на паузе | `make airflow-unpause` или кнопка в UI |
| `wait_bronze_ready` всегда тайм-аутит | Стрим `bronze-s3-streaming` не пишет в `bronze.ingest_watermarks`. Проверить `kubectl -n spark-jobs logs bronze-s3-streaming-driver`, креды S3, доступ к YC bucket |
| `dbt_silver` падает с «Hudi timeline corruption» | `make reset-watermarks`; для cancellations — `make reset-cancellations` |
| Дубли в `bronze.cancellations` | `make reset-cancellations` — удаляет таблицы и очищает checkpoint, следующий запуск пересоберёт |
| Trino возвращает `TABLE_NOT_FOUND` | Сенсор игнорирует это; для ручных запросов — дождаться первого успешного ingest или выполнить `SHOW TABLES IN hudi.bronze` |
| `make superset-init` падает с auth error | Superset ещё инициализируется. Повторить через минуту; проверить `kubectl -n data-platform get pod -l app=superset` |
| `*.lab08.local` не открывается | Не выполнен `make hosts`. Альтернатива — `make port-forward` |
| Образы не собрались | Запустить поштучно: `make spark-image` / `make airflow-image` / `make hms-image`, проанализировать лог |
| Полный сброс | `make down` → `docker volume prune` → `make up` |

Полезные команды:

```bash
make status                   # pod'ы по всем namespaces
make diff                     # helmfile diff
make verify-trino             # smoke-тест Trino и bronze.transactions
kubectl -n spark-jobs get sparkapplication
kubectl -n spark-jobs logs <driver-pod>
```

---

## Глоссарий

| Термин | Определение |
|---|---|
| **Lakehouse** | Архитектура, в которой DWH-возможности (ACID, схема, time-travel) реализованы поверх объектного хранилища |
| **Hudi (CoW)** | Apache Hudi в режиме Copy-on-Write — каждая запись пересоздаёт parquet-файл |
| **Medallion** | Подход bronze (raw) → silver (cleaned) → gold (business marts) |
| **HMS** | Hive Metastore — каталог таблиц (schema, location) |
| **MinIO** | S3-совместимое хранилище. В Lab08 — бакеты `lake/` и `checkpoints/` |
| **Trino** | Distributed SQL engine, в Lab08 в режиме read-only с каталогом `hudi` |
| **Superset** | BI-инструмент, дашборды поверх Trino |
| **SparkApplication** | CRD `sparkoperator.k8s.io/v1beta2` от Kubeflow Spark Operator |
| **Streaming SparkApp** | Долгоживущий `SparkApplication` (`restartPolicy: Always`) — `bronze-s3-streaming`, `bronze-kafka-ingest` |
| **dbt-spark (session)** | Адаптер dbt, использующий уже сконфигурированный SparkSession в driver pod |
| **composite_pk** | `concat_ws('|', transaction_id, created_at, user_id)` — record key Hudi для транзакций, компенсирует дубли |
| **`_ingested_at`** | Технический timestamp, precombine-поле Hudi для dedup latest-wins |
| **bronze.ingest_watermarks** | Commit-log стримов `(table_name, source_partition, rows_in_batch, committed_at)`. На нём завязан Airflow-сенсор |
| **TGRK** | Внутренний код базовой валюты конверсии, задан через `vars.base_currency` |
| **PythonSensor (reschedule)** | Airflow-сенсор, освобождающий worker-slot между poke-итерациями |
| **ShortCircuitOperator** | Пропускает downstream-таски, если callable вернул `False` |
