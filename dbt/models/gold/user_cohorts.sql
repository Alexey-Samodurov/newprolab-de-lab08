{#
  Gold: Когортный анализ пользователей.
  Новые (first_seen = event_day) vs возвращающиеся (ранее уже были транзакции).
  Когорта = дата первой транзакции пользователя в наборе данных.
  Метрики считаются по дням, только реальные пользователи с известным user_id.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge',
   unique_key='pk',
   options={'primaryKey': 'pk', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH real_tx AS (
    SELECT
        user_id,
        user_uuid,
        event_day,
        created_ts,
        amount,
        currency,
        status,
        is_revenue_eligible
    FROM {{ ref('transactions_clean') }}
    WHERE is_test_user = false
      AND is_user_missing = false
    {% if is_incremental() %}
      AND event_day >= date_sub(current_date(), {{ var('transactions_lookback_days', 30) }})
    {% endif %}
),
-- first_seen намеренно читает ВСЮ таблицу без incrementel-фильтра:
-- когорта пользователя (cohort_day) определяется его первой транзакцией за всё время.
-- Обрезать lookback здесь нельзя — новый пользователь для недели "назад"
-- будет ошибочно помечен returning.
first_seen AS (
    SELECT
        user_id,
        min(event_day) AS cohort_day
    FROM {{ ref('transactions_clean') }}
    WHERE is_test_user = false
      AND is_user_missing = false
    GROUP BY user_id
),
enriched AS (
    SELECT
        t.user_id,
        t.event_day,
        t.created_ts,
        t.amount,
        t.currency,
        t.status,
        t.is_revenue_eligible,
        f.cohort_day,
        CASE WHEN f.cohort_day = t.event_day THEN 'new' ELSE 'returning' END AS user_type
    FROM real_tx t
    JOIN first_seen f ON f.user_id = t.user_id
)
SELECT
    concat(event_day, '_', user_type)              AS pk,
    event_day,
    user_type,
    count(DISTINCT user_id)                        AS unique_users,
    count(*)                                       AS tx_cnt,
    sum(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_cnt,
    sum(CASE WHEN is_revenue_eligible THEN 1 ELSE 0 END) AS revenue_eligible_cnt,
    avg(amount)                                    AS avg_amount,
    current_timestamp()                            AS updated_at
FROM enriched
GROUP BY event_day, user_type
