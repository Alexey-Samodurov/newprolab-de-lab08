# Моделирование данных и обработка грязных кейсов

Документ объясняет, как мы посчитали витрины в Lab08 и что делаем с каждой
проблемой в данных. Архитектура и эксплуатация — в `01-detailed-architecture.md`
и `02-user-guide.md`, здесь только бизнес-логика.

---

## TL;DR: какой график откуда

| График | Источник (settled) | Live-копия |
|---|---|---|
| Транзакции по часам (нагрузка) | `transactions_by_hour` | `transactions_by_hour_live` |
| Покупки по часам | `purchases_by_hour` | `purchases_by_hour_live` |
| Выручка в TGRK по дням | `revenue_daily` | — (см. ниже) |
| Возвраты по дням | `refunds_daily` | — |
| Промокоды: статус, лимиты, просрочка | `promo_codes_analysis` + `promo_expired_usage_daily` | — |
| Отмены: %, причины, время до отмены | `cancellations_summary` | `cancellations_summary_live` |
| Реальные vs тестовые пользователи | срез `is_test_user` в `transactions_by_hour` | то же в `_live` |
| Разбивка по валютам | `tx_tgrk` / `tx_punk` / `tx_rub` в `revenue_daily` | `exchange_rates_latest` для live-конвертации |
| Когорты (new vs returning) | `user_cohorts` | — |
| Качество данных | `dq_summary_daily` | — |

«Live» = NRT-витрина из Kafka-стрима, отдельная таблица. Settled и live
сшиваются на уровне Superset (см. `01-detailed-architecture.md`).

---

## Слои данных

Классический medallion поверх Hudi (Copy-on-Write), партиции по `event_day`.

| Слой | Назначение | Что лежит |
|---|---|---|
| **Bronze** | Сырой ingest из S3 и Kafka. Только парсинг JSONL и составные ключи. | `transactions`, `cancellations`, `exchange_rates`, `users`, `test_users`, `promo_codes`, `bronze_dlq.late_events` |
| **Silver** | Дедуп, нормализация типов, флаги качества. | `transactions_clean`, `cancellations_clean`, `exchange_rates_daily`, `exchange_rates_long` |
| **Gold** | Витрины под графики и DQ-метрики. Инкрементальные, идемпотентные. | см. таблицу выше |

Главный принцип: **на silver мы ничего не выкидываем**, только помечаем
флагами. Решение «учитывать или нет» принимается явно в gold-моделях через
`WHERE`. Это позволяет в любой момент собрать альтернативную витрину без
перегрузки bronze.

### Справочники (`users`, `test_users`, `promo_codes`)

Статические по своей природе, но в bronze живут в таких же Hudi-таблицах,
как и события. Перезагрузка справочника — обычный upsert по PK
(`user_id`, `promo_code_id`). На silver/gold подтягиваются обычным join-ом,
а в streaming-витринах ещё и broadcast-ом, чтобы каждый микробатч видел
актуальную версию.

---

## Бизнес-определения

- **Транзакция** = любая запись из `bronze.transactions`. У неё есть тип
  (`transaction_type ∈ {purchase, transfer, refund}`) и статус
  (`status ∈ {completed, pending, failed, cancelled}`). Тип `refund` —
  это не отмена покупки (отмены лежат отдельно в `cancellations`), а тип
  движения денег.
- **Покупка** = `transaction_type='purchase'` И `status='completed'`. Только
  такая запись приносит выручку. Зашито во флаг `is_revenue_eligible`.
  `transfer` и `refund`-транзакции в `revenue_daily` и `purchases_by_hour`
  не идут — попадают только в `transactions_by_hour` как «нагрузка».
- **`status='cancelled'` на самой транзакции** — это не отмена в смысле
  таблицы `cancellations` (это отдельная сущность). Такая транзакция в
  выручку не идёт (фильтр по `status='completed'`), но в нагрузочный
  график попадает. Если рядом нет записи в `cancellations` — нечего
  пересчитывать в `refunds_daily`.
- **GROSS-выручка** в `revenue_daily` — без вычета refund-ов. Возвраты живут
  отдельной витриной `refunds_daily` по дню отмены. У бизнеса есть честный
  исторический GROSS, который не мутирует задним числом, и отдельная линия
  возвратов.
- **Процент отмен** — считается на BI-слое как
  `cancellations_summary.cancellations_cnt / purchases_by_hour.purchase_cnt`
  на общем `day`. В gold не материализуем — пропорция зависит от выбранного
  знаменателя (все покупки / только реальные юзеры / по валюте), и нет
  смысла фиксировать одну версию.
- **Базовая валюта** — TGRK.
- **Когорта** = день первой транзакции юзера. В `user_cohorts` каждый день
  бьётся на `new` (cohort_day = event_day) и `returning`.

---

## Параллельный батч и стрим: что это меняет для данных

Как именно устроена параллельная обработка (watermark, DLQ, UNION в Superset,
streaming-medallion) — описано в `01-detailed-architecture.md`. Здесь — только
последствия для бизнес-данных:

- **Дубликаты между источниками** не страшны: оба пишут в одну bronze-таблицу
  по составному ключу, повторная запись == upsert. Силверная дедупликация
  оставит самую свежую версию по `ingested_at`.
- **Поздно прилетевшее в Kafka событие за уже закрытый день** не портит
  settled-цифру: уходит в DLQ. Источник истины для прошлых дней — S3.
- **Падение стрима** не теряет данные: батч всё равно догонит ночью.
- **Падение батча** не убивает дашборд: live-витрина продолжает работать
  по Kafka.
- **Late-arriving отмены** работают одинаково в обоих контурах — `event_day`
  отмены сохраняется из payload, и dbt пересчитывает только затронутые дни
  (детали ниже).

Флаги качества и бизнес-фильтры ставятся **уже после слияния батча и стрима**
в bronze, поэтому всё, что описано ниже, применяется одинаково независимо
от того, через какой источник прилетела запись.

### Live-витрины (NRT)

Те же бизнес-определения, что и в settled, считаются параллельно
streaming-job-ом и пишутся в отдельные таблицы с суффиксом `_live`. Это
нужно, чтобы сегодняшний день был виден в дашборде до прихода ночного
батча.

**Что есть:** `transactions_by_hour_live`, `purchases_by_hour_live`,
`cancellations_summary_live`, `exchange_rates_latest` (одна строка на пару
валют, Hudi `precombine` по `timestamp` — для live-конвертации в TGRK).

**Чего сознательно нет:**

- `revenue_daily_live` — логика выбора курса (forward + backward fill)
  нетривиальна, а свежесть «раз в сутки» полностью покрывается батчем.
  Дублировать сложную логику в двух местах — гарантированно набрать
  расхождения.
- `user_cohorts_live`, `promo_codes_analysis_live`, `dq_summary_daily_live` —
  осмысленны на горизонте дней, а не минут.

Бизнес-семантика и DQ-флаги в live и settled **идентичны** — стримовый job
повторяет те же выражения для `is_revenue_eligible`, `is_test_user` и др.,
читая те же reference-таблицы броадкастом. Поэтому когда settled-день
догоняется батчем и подменяет live-данные на дашборде, цифры не «прыгают».

---

## Как решаются грязные кейсы

### Дубли `transaction_id`

`transaction_id` сам по себе не уникален — апстрим выдаёт разные транзакции
с одним id.

- В bronze ключом Hudi работает `composite_pk = transaction_id|created_at|user_id`.
  Спасает и от внутри-апстримных коллизий, и от того, что одна и та же
  транзакция придёт и через S3, и через Kafka — upsert схлопнет в одну
  строку.
- В silver дедуп оставляет самую свежую версию по `ingested_at`
  (`row_number() OVER ... ORDER BY ingested_at DESC`).
- Флаг `is_transaction_id_duplicated` отдельно показывает, что внутри дня
  есть коллизии по «человеческому» id — попадает в DQ-витрину.
- В `cancellations_summary` есть колонка `ambiguous_attribution_cnt` —
  отмена ссылается на `transaction_id`, у которого нашлось несколько
  физических матчей.

### Пустые и битые `user_id`

- `is_user_missing` — `user_id IS NULL`.
- `is_user_unknown` — `user_id` есть, но в `bronze.users` его нет.

Витрины выручки, промокодов и когорт фильтруют такие строки явно.
В `dq_summary_daily` они видны как `tx_user_missing` / `tx_user_unknown` —
если процент скакнул, понятно куда смотреть.

### Отрицательные и нулевые суммы

`is_amount_invalid = (amount IS NULL OR amount <= 0)`. Из выручки выкидываем,
в почасовой нагрузке `transactions_by_hour` оставляем — попытка платежа
случилась, платформу нагрузила. Доля видна в `dq_summary_daily.tx_amount_invalid`.

### Просроченные промокоды

В silver джойнимся с `promo_codes` и ставим `is_promo_expired_at_use`, если
`created_at` транзакции наступил строго после `expiry_date + 1 day`
(граничный день считаем валидным — у expiry нет таймзоны, не наказываем
за пограничный случай).

Две gold-витрины:

- `promo_codes_analysis` — состояние «здесь и сейчас» по каждому промокоду:
  `uses_total`, `uses_completed`, `used_after_expiry`, `over_limit` (когда
  фактических использований больше `max_uses`).
- `promo_expired_usage_daily` — дневной счётчик, чтобы алертить, если бэк
  внезапно перестал валидировать expiry.

### Тестовые пользователи

Маркируем, не режем — каждому виджету бизнес решает индивидуально.

- `is_test_user = true`, если `user_uuid` есть в `test_users` ИЛИ
  `users.is_test_user = true`.
- `is_test_user_inconsistent` — два источника правды разошлись, ловим в DQ.
- `revenue_daily`, `purchases_by_hour`, `promo_*`, `cancellations_summary`,
  `user_cohorts` — только реальные юзеры.
- `transactions_by_hour` режется по `is_test_user` отдельными сериями —
  на графике видна и реальная, и тестовая нагрузка.

### Отмены с опозданием (приходят на следующий день)

`cancellation_id` уникален только внутри дневного файла, поэтому первичный
ключ — `cancellation_pk = event_day|cancellation_id`.

В `cancellations_clean` инкремент идёт по дню загрузки (`ingested_at`), но
исходный `event_day` сохраняется. Дальше `cancellations_summary` и
`refunds_daily` инкрементально берут набор `affected_days` — это дни,
в которые сегодня прилетели новые записи, — и **полностью пересчитывают свой
исторический день**. Так вчерашняя цифра отмен корректно обновляется, если
сегодня прилетела поздняя отмена на вчера. `revenue_daily` при этом не
трогается — GROSS не мутирует.

Дополнительно считаем `seconds_to_cancel = cancelled_ts - created_ts` —
для метрики «среднее время до отмены». `orphan_cnt` — отмены без матча в
транзакциях: либо `original_transaction_id IS NULL`, либо такого
`transaction_id` нет (битая ссылка или последствие дубля). Их видно
в дашборде отдельной серией.

### Дыры в курсах валют

Котировок может быть 2–3 в день, а может не быть совсем. Плюс может прилететь
транзакция в валюте, для которой курсов вообще никогда не было.

1. `exchange_rates_daily` — последняя котировка дня.
2. `exchange_rates_long` — long-формат `(rate_day, currency, rate_to_tgrk)`.
   Добавление новой валюты не требует правок gold-моделей.
3. `revenue_daily` для каждой пары `(event_day, currency)` ищет курс так:
   - **forward-fill**: последний известный курс с `rate_day <= event_day`;
   - если нет — **backward-fill**: первый курс `rate_day > event_day`;
   - если и этого нет (например, валюта вообще не котировалась) —
     `rate_source = 'missing'`, строка в выручку **не попадает**, но
     `tx_rate_missing` инкрементируется как DQ-метрика.
   - `tx_rate_backfilled` отдельно показывает, сколько строк пересчитано
     по «будущему» курсу — сигнал, что апстрим запаздывает.

### Разные форматы дат

`created_at` (unix timestamp) → `created_ts` через `from_unixtime(...)` в
silver. `cancelled_at` (строка вида `"2025 Oct 06 14:30"`) парсится ещё на
bronze-ingest в Spark — выше по пайплайну работает только типизированный
timestamp.

### DQ-сводка как первоклассная витрина

`dq_summary_daily` агрегирует все silver-флаги по дню:
`tx_user_missing`, `tx_user_unknown`, `tx_test_user`, `tx_amount_invalid`,
`tx_id_duplicated`, `tx_promo_expired`, `tx_test_user_inconsistent`. Это
позволяет:

- видеть базлайны грязности и ловить отклонения;
- объяснять расхождение между `tx_cnt` в `transactions_by_hour` и
  `purchase_cnt` в `purchases_by_hour` — куда делись транзакции;
- быстро отвечать на вопрос «а вы это вообще обработали?».

---

## Идемпотентность и пересчёт

Все silver/gold-модели — `materialized='incremental'` со стратегией `merge`
поверх Hudi по бизнес-ключу (`composite_pk`, `cancellation_pk`, `event_day`,
синтетические `pk`). Повторный запуск за тот же `run_date` не дублирует
данные. Стриминг тоже идемпотентен: чекпойнт на S3 + upsert в Hudi по PK,
перезапуск streaming-job-а возобновляет работу с того же offset без дублей.

**Полный пересчёт** — `dbt run --full-refresh`, время порядка одного
backfill-пробега по всей истории bronze.

**Бэкфил дня (или диапазона)** — оба DAG-а в Airflow поддерживают
`catchup=True`. Ручной backfill:

```bash
airflow dags backfill bronze_s3_ingest      -s YYYY-MM-DD -e YYYY-MM-DD
airflow dags backfill transactions_medallion -s YYYY-MM-DD -e YYYY-MM-DD
```

**Изменение reference-данных задним числом** (юзер стал тестовым,
у промокода переехал `expiry_date`, появилась новая запись в `users`) —
incremental-модели прошлые дни **не пересчитывают**. Чтобы исторические
витрины подхватили новую правду, нужен `--full-refresh` либо точечный
пересчёт затронутых дней через `dbt run --vars '{run_date: ...}'`.
