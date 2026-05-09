{#
  Gold: Дневная GROSS-выручка в базовой валюте TGRK.

  ADR (FIX_PLAN P1-1):
   * `gross_revenue_tgrk` = sum(amount / rate_to_tgrk) по completed-purchase
     транзакциям реальных пользователей. Невалидные суммы (amount<=0 или NULL)
     ИСКЛЮЧЕНЫ через `is_amount_invalid = false`.
   * Это GROSS — refunds НЕ вычитаются из дня транзакции. Refund-ы относятся
     к дню отмены и считаются отдельно в `refunds_daily` (см. P1-2).
     Причина — daily-схема без пересчёта истории (см. P0-1): отмена за tx_day=N
     приходит в day=N+K и не должна задним числом править revenue_daily[N].
   * Конвертация: rate_to_tgrk = «сколько единиц <currency> за 1 TGRK»,
     `amount_tgrk = amount / rate_to_tgrk`. TGRK→TGRK = 1.0.
   * Курс ищется как-of `event_day` через forward+backward fill из
     `exchange_rates_long`. `rate_source` показывает, использовался ли
     forward (последний известный до даты) или backward (первый после).
     Если ни одного нет — конвертация = NULL, и транзакция не попадает в gross.
   * Список валют не хардкодится — джойн по `currency` через long-таблицу
     (см. P1-9).
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge', unique_key='event_day',
   options={'primaryKey': 'event_day', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH tx AS (
    SELECT * FROM {{ ref('transactions_clean') }}
    WHERE is_revenue_eligible = true
      AND is_test_user = false
      AND is_amount_invalid = false
    {% if is_incremental() %}
      AND event_day = {{ run_date() }}
    {% endif %}
),
rates AS (
    SELECT rate_day, currency, rate_to_tgrk
    FROM {{ ref('exchange_rates_long') }}
),
-- Все нужные пары (event_day, currency) на которые надо найти курс.
needed AS (
    SELECT DISTINCT event_day, currency FROM tx
),
-- Forward-fill: последний известный курс <= event_day.
fwd AS (
    SELECT n.event_day, n.currency,
           max(r.rate_day) AS picked_rate_day
    FROM needed n
    LEFT JOIN rates r
      ON r.currency = n.currency AND r.rate_day <= n.event_day
    GROUP BY n.event_day, n.currency
),
-- Backward-fill: первый известный курс > event_day (если forward промахнулся).
bwd AS (
    SELECT n.event_day, n.currency,
           min(r.rate_day) AS picked_rate_day
    FROM needed n
    LEFT JOIN rates r
      ON r.currency = n.currency AND r.rate_day > n.event_day
    GROUP BY n.event_day, n.currency
),
picked AS (
    SELECT n.event_day, n.currency,
           coalesce(f.picked_rate_day, b.picked_rate_day) AS picked_rate_day,
           CASE WHEN f.picked_rate_day IS NOT NULL THEN 'forward'
                WHEN b.picked_rate_day IS NOT NULL THEN 'backward'
                ELSE 'missing' END                        AS rate_source
    FROM needed n
    LEFT JOIN fwd f ON f.event_day = n.event_day AND f.currency = n.currency
    LEFT JOIN bwd b ON b.event_day = n.event_day AND b.currency = n.currency
),
day_rate AS (
    SELECT p.event_day, p.currency, p.rate_source, r.rate_to_tgrk
    FROM picked p
    LEFT JOIN rates r
      ON r.currency = p.currency AND r.rate_day = p.picked_rate_day
),
tx_with_rate AS (
    SELECT tx.*,
           dr.rate_to_tgrk,
           dr.rate_source
    FROM tx
    LEFT JOIN day_rate dr
      ON dr.event_day = tx.event_day AND dr.currency = tx.currency
)
SELECT
    event_day,
    sum(CASE WHEN rate_to_tgrk IS NOT NULL AND rate_to_tgrk <> 0
             THEN amount / rate_to_tgrk
             ELSE NULL END)                            AS gross_revenue_tgrk,
    count(*)                                           AS tx_cnt,
    -- Доля транзакций, для которых курса не нашлось ни forward, ни backward.
    sum(CASE WHEN rate_source = 'missing' THEN 1 ELSE 0 END) AS tx_rate_missing,
    sum(CASE WHEN rate_source = 'backward' THEN 1 ELSE 0 END) AS tx_rate_backfilled,
    -- Разбивка по валютам в native-amount (для расширенной части ТЗ).
    sum(CASE WHEN currency='TGRK' THEN 1 ELSE 0 END)   AS tx_tgrk,
    sum(CASE WHEN currency='PUNK' THEN 1 ELSE 0 END)   AS tx_punk,
    sum(CASE WHEN currency='RUB'  THEN 1 ELSE 0 END)   AS tx_rub,
    max(ingested_at)                                   AS updated_at
FROM tx_with_rate
GROUP BY event_day
