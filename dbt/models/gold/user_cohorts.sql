{#
  Gold: дневная когортная разбивка пользователей.
  Когорта — день первой транзакции юзера в данных. Считаем, сколько в
  этот день было новых (first_seen = event_day) и возвращающихся, и их
  выручку. Только реальные пользователи с известным user_id.
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
        is_revenue_eligible,
        ingested_at
    FROM {{ ref('transactions_clean') }}
    WHERE is_test_user = false
      AND is_user_missing = false
    {% if is_incremental() %}
      AND event_day = {{ run_date() }}
    {% endif %}
),
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
        t.ingested_at,
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
    max(ingested_at)                               AS updated_at
FROM enriched
GROUP BY event_day, user_type
