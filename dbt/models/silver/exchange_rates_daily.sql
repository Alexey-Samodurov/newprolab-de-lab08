{#
  Silver: дневные курсы валют — последняя котировка за день.
  Базовая валюта TGRK; считаем и обратный курс, чтобы конвертировать
  суммы транзакций в TGRK на gold-слое.
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='rate_day',
    options={
      'primaryKey': 'rate_day',
      'preCombineField': 'timestamp',
      'type': 'cow'
    }
) }}

WITH ranked AS (
    SELECT
        update_id,
        timestamp,
        rate_tgrk_punk,
        rate_tgrk_rub,
        date_format(to_timestamp(from_unixtime(timestamp)), 'yyyy-MM-dd') AS rate_day,
        row_number() OVER (
            PARTITION BY date_format(to_timestamp(from_unixtime(timestamp)), 'yyyy-MM-dd')
            ORDER BY timestamp DESC
        ) AS rn
    FROM {{ source('bronze', 'exchange_rates') }}
    {% if is_incremental() %}
      WHERE timestamp >= unix_timestamp({{ run_date() }})
        AND timestamp <  unix_timestamp(date_add({{ run_date() }}, 1))
    {% endif %}
)
SELECT
    rate_day,
    update_id,
    timestamp,
    rate_tgrk_punk,
    rate_tgrk_rub
FROM ranked
WHERE rn = 1
