{#
  Gold: Распределение транзакций по часам (для столбиков).
  Включает все транзакции (completed/pending/failed) с флагом is_test_user.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge', unique_key='pk',
   options={'primaryKey': 'pk', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

SELECT
    concat(event_day, '_', cast(hour(created_ts) AS string), '_', cast(is_test_user AS string)) AS pk,
    event_day,
    hour(created_ts)            AS hour_of_day,
    is_test_user,
    count(*)                    AS tx_cnt,
    sum(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_cnt,
    sum(CASE WHEN status='failed'    THEN 1 ELSE 0 END) AS failed_cnt,
    current_timestamp()         AS updated_at
FROM {{ ref('transactions_clean') }}
GROUP BY event_day, hour(created_ts), is_test_user
