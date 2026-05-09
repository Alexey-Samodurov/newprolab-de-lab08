{#
  Gold: Покупки (purchase + completed) по часам.
  ADR: считаем покупкой только status='completed' AND transaction_type='purchase'.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge', unique_key='pk',
   options={'primaryKey': 'pk', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

SELECT
    concat(event_day, '_', cast(hour(created_ts) AS string)) AS pk,
    event_day,
    hour(created_ts)                                        AS hour_of_day,
    count(*)                                                AS purchase_cnt,
    sum(amount)                                             AS gross_amount_native,
    max(ingested_at)                                        AS updated_at
FROM {{ ref('transactions_clean') }}
WHERE is_revenue_eligible = true
  AND is_test_user = false
{% if is_incremental() %}
  AND event_day = {{ run_date() }}
{% endif %}
GROUP BY event_day, hour(created_ts)
