# Руководство пользователя

Lab08 — учебный стенд транзакционной аналитики, разворачиваемый локально командой `make up`. Данные поступают из YC S3 (посуточный batch, источник истины) и Kafka (опциональный speed-слой), сохраняются в Hudi-таблицы поверх MinIO, проходят через dbt и публикуются в дашбордах Superset.

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
3. **Развёртывание:** `make up`. Таргет идемпотентный: сборка образов → helmfile sync → HMS → ConfigMap'ы → DAG'и → reference-batch → unpause → `bootstrap-pipeline`. Холодный старт занимает около 10 минут (MinIO Tenant, HMS на arm64 через QEMU, Airflow стартуют долго). `bootstrap-pipeline` ждёт первого наполнения gold-таблиц через Trino.
4. **DNS:** `make hosts` — добавляет `*.lab08.local` в `/etc/hosts`.
5. **Проверка состояния:** `make status`. Все pod'ы должны быть в статусе `Running` или `Completed`.
6. **Дашборды:** `make superset-init` — создаёт чарты и дашборд в Superset.

Kafka-стрим и streaming-medallion поднимаются `make up` автоматически. Если хочешь без NRT-слоя — пропусти `make kafka-streaming-app` и `make streaming-medallion-app`; batch-контур (bronze S3 + dbt + Superset settled) работает независимо.

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

- **Airflow** — мониторинг DAG'ов `bronze_s3_ingest` и `transactions_medallion`, ручной триггер, логи задач.
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
| Дождаться готовности данных (после `make up`) | `make bootstrap-pipeline` |
| Триггернуть bronze-ingest | `make airflow-trigger-bronze` или Airflow UI |
| Триггернуть медальон | `make airflow-trigger-medallion` или Airflow UI |
| Перезапустить reference-batch | `make reference-batch` |
| Включить Kafka speed-слой | `make kafka-streaming-app` |
| Включить streaming-medallion (NRT gold) | `make streaming-medallion-app` |
| Обновить PySpark-код | правка `spark/*.py` → `make spark-code` (перезапуск стрима — вручную, см. ниже) |
| Обновить dbt | правка `dbt/` → `make dbt-configmap` |
| Создать дашборды | `make superset-init` |
| Ad-hoc SQL | Trino UI или Superset SQL Lab по `hudi.bronze.*`, `hudi.silver.*`, `hudi.gold.*` |

---

## Сценарии

### A. Развёртывание с нуля

Шаги совпадают с разделом «Установка» выше. При сбоях — см. раздел «Диагностика».

### B. Ручной запуск пайплайна

Цель — прогнать `bronze → silver → gold → tests` для конкретного `ds` (T-1).

1. Триггернуть bronze: `make airflow-trigger-bronze` либо «Trigger DAG» у `bronze_s3_ingest`. Внутри — `check_source_day` (ShortCircuit на наличие `day=<ds>/` в S3) → 3 параллельные таски `bronze_transactions / bronze_cancellations / bronze_exchange_rates`. Каждая после upsert пишет в свой шард `bronze.ingest_watermarks_<source>`.
2. Триггернуть медальон: `make airflow-trigger-medallion` либо «Trigger DAG» у `transactions_medallion`. Сенсор `bronze_ready` (`reschedule`, poke 60s) ждёт все три шарда watermark и запускает `dbt_silver → dbt_gold → dbt_test`.
3. По расписанию оба DAG'а запускаются автоматически: bronze в `0 2 * * *`, медальон в `30 2 * * *` (T-1 от текущего дня).

Если за `ds` нет данных в S3 — `bronze_s3_ingest` skipped (это легальный исход), медальон в этом случае упадёт по таймауту сенсора.

### C. Обновление кода

**PySpark (bronze batch jobs):**

1. Внести правки в `spark/jobs/*.py` или `spark/utils/*.py`.
2. `make spark-code` — пересоздаёт ConfigMap `spark-jobs-code`. Следующий DAGRun bronze поднимет новый driver с обновлённым кодом.

**PySpark (Kafka streaming, если запущен):**

1. `make spark-code`.
2. Перезапустить:
   ```bash
   kubectl -n spark-jobs delete sparkapplication bronze-kafka-ingest --ignore-not-found
   make kafka-streaming-app
   ```

**dbt:**

1. Внести правки в `dbt/models/` или `dbt/tests/`.
2. `make dbt-configmap` — пересоздаёт ConfigMap `dbt-project`.
3. Триггернуть медальон (`make airflow-trigger-medallion`). Следующий ephemeral `SparkApplication` подхватит обновлённый ConfigMap.

### D. Просмотр данных

Через Trino UI / CLI, каталог `hudi`:

```sql
SELECT * FROM hudi.gold.revenue_daily ORDER BY event_day DESC LIMIT 30;
SELECT count(*) FROM hudi.bronze.transactions;
SELECT * FROM hudi.bronze.ingest_watermarks_transactions ORDER BY committed_at DESC LIMIT 20;
```

В Superset — готовый дашборд `Lab08 Transactions Overview` или SQL Lab на том же каталоге.

### E. Снос стенда

`make down` — выполняет `helmfile destroy` и удаляет HMS. Для удаления данных MinIO — дополнительно `docker volume prune`.

При сносе PVC MinIO удаляется, все Hudi-таблицы пропадают. На следующем `make up` всё пересоздаётся, но значения `_ingested_at` будут новыми.

---

## Модель данных

Бизнес-логика, бизнес-определения, обработка грязных кейсов, layout
silver/gold и live-витрины — `04-data-modeling.md`.

С эксплуатационной точки зрения важно знать:

- **Изменение reference-данных задним числом** (юзера пометили как
  тестового, у промокода поменялся `expiry_date`) silver/gold не
  пересчитают автоматически — модели инкрементальные по `event_day`.
  Нужен полный пересчёт: `dbt run --full-refresh` (или хотя бы пересчёт
  затронутых дней через `--vars '{run_date: ...}'`).
- **Бэкфил истории**: `kubectl -n data-platform exec deploy/airflow-scheduler -- airflow dags backfill bronze_s3_ingest -s YYYY-MM-DD -e YYYY-MM-DD`,
  затем то же для `transactions_medallion`. Hudi upsert идемпотентен —
  повторные дни не задвоятся.
- **Перезапуск streaming-job** (`bronze-kafka-ingest`, `streaming-medallion`)
  безопасен: checkpoint на S3 + upsert в Hudi по PK → возобновление с того
  же offset, без дублей.

---

## Глоссарий

Эксплуатационные термины. Data-термины (`composite_pk`, `cancellation_pk`,
`gold.*_unified`, базовая валюта и т.д.) — в `04-data-modeling.md`.
Архитектурные (HMS, Hudi, medallion) — в `01-detailed-architecture.md`.

| Термин | Определение |
|---|---|
| **SparkApplication** | CRD `sparkoperator.k8s.io/v1beta2` от Kubeflow Spark Operator |
| **Streaming SparkApp** | Долгоживущий `SparkApplication` (`restartPolicy: Always`): `bronze-kafka-ingest`, `streaming-medallion` |
| **Batch SparkApp** | Эфемерный `SparkApplication` (`restartPolicy: Never`), создаётся Airflow на каждую задачу: bronze S3 batch, reference, dbt |
| **bronze.ingest_watermarks_\<source\>** | Per-source Hudi commit-log, по одному writer на shard. Сенсор `bronze_ready` в `transactions_medallion` ждёт строки по `ds` во всех трёх шардах |
| **PythonSensor (reschedule)** | Airflow-сенсор, освобождающий worker-slot между poke-итерациями |
| **ShortCircuitOperator** | Пропускает downstream-таски, если callable вернул `False` (используется в `check_source_day`) |
