{#
  Gold: дневная сводка качества данных по транзакциям.
  Доля «битых» записей за день — нет user_id, юзер-зомби, невалидная
  сумма, дубль tx_id, просроченный промокод, тестовый пользователь.
  Кормит DQ-дашборд и алерты; каждый прогон считает только свой день.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge',
   unique_key='event_day',
   options={'primaryKey': 'event_day', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH src AS (
    SELECT * FROM {{ ref('transactions_clean') }}
    {% if is_incremental() %}
      WHERE event_day = {{ run_date() }}
    {% endif %}
)
SELECT
    event_day,
    count(*)                                                        AS tx_total,
    sum(CASE WHEN is_user_missing  THEN 1 ELSE 0 END)               AS tx_user_missing,
    sum(CASE WHEN is_user_unknown  THEN 1 ELSE 0 END)               AS tx_user_unknown,
    sum(CASE WHEN is_test_user     THEN 1 ELSE 0 END)               AS tx_test_user,
    sum(CASE WHEN is_test_user_inconsistent THEN 1 ELSE 0 END)      AS tx_test_user_inconsistent,
    sum(CASE WHEN is_amount_invalid THEN 1 ELSE 0 END)              AS tx_amount_invalid,
    sum(CASE WHEN is_transaction_id_duplicated THEN 1 ELSE 0 END)   AS tx_id_duplicated,
    sum(CASE WHEN is_promo_expired_at_use      THEN 1 ELSE 0 END)   AS tx_promo_expired,
    max(ingested_at)                                                AS updated_at
FROM src
GROUP BY event_day
