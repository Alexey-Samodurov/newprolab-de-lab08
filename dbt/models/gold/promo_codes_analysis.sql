{#
  Gold: Анализ промокодов.
  Использования промокода (только успешные completed транзакции),
  vs лимиты, флаг просрочки.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge', unique_key='promo_code_id',
   options={'primaryKey': 'promo_code_id', 'preCombineField': 'promo_code_id', 'type': 'cow'}) }}

WITH usage AS (
    SELECT
        promo_code_id,
        count(*)            AS uses_total,
        sum(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS uses_completed,
        sum(CASE WHEN status='failed'    THEN 1 ELSE 0 END) AS uses_failed,
        min(created_ts)     AS first_used_at,
        max(created_ts)     AS last_used_at
    FROM {{ ref('transactions_clean') }}
    WHERE promo_code_id IS NOT NULL
      AND is_test_user = false
    GROUP BY promo_code_id
),
codes AS (
    SELECT * FROM {{ source('bronze', 'promo_codes') }}
)
SELECT
    coalesce(c.promo_code_id, u.promo_code_id) AS promo_code_id,
    c.code,
    c.max_uses,
    c.expiry_date,
    coalesce(u.uses_total, 0)     AS uses_total,
    coalesce(u.uses_completed, 0) AS uses_completed,
    coalesce(u.uses_failed, 0)    AS uses_failed,
    u.first_used_at,
    u.last_used_at,
    CASE WHEN c.expiry_date IS NOT NULL
              AND u.last_used_at IS NOT NULL
              AND u.last_used_at > to_timestamp(c.expiry_date)
         THEN true ELSE false END  AS used_after_expiry,
    CASE WHEN c.max_uses IS NOT NULL AND coalesce(u.uses_total, 0) > c.max_uses
         THEN true ELSE false END  AS over_limit
FROM codes c
FULL OUTER JOIN usage u ON c.promo_code_id = u.promo_code_id
