{#
  Gold: Сводка качества данных по дням (FIX_PLAN P1-10).

  Считаем долю «грязных» записей в transactions_clean по run_date —
  для алертов и дашборда DQ:
    * is_user_missing       — у транзакции нет user_id
    * is_user_unknown       — user_id не находится в users (zombie)
    * is_amount_invalid     — amount NULL или <=0
    * is_transaction_id_duplicated — несколько строк с одинаковым tx_id
    * is_promo_expired_at_use      — использован промокод после expiry
    * is_test_user                 — пользователь помечен как тестовый
                                     (для разрезания «реальной» выручки)

  ADR: смотрим только за event_day = run_date (daily-семантика),
  catchup-прогон считает DQ за свой день — не пересматривает прошлое.
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
