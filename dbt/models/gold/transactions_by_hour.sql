{#
  Gold: распределение всех транзакций по часам дня.
  В отличие от purchases_by_hour, считает все статусы (completed/pending/
  failed) и разрезает по is_test_user — видно нагрузку и долю провалов
  по часам, отдельно по реальным и тестовым юзерам.
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
    max(ingested_at)            AS updated_at
FROM {{ ref('transactions_clean') }}
{% if is_incremental() %}
WHERE event_day = {{ run_date() }}
{% endif %}
GROUP BY event_day, hour(created_ts), is_test_user
