{#
  Silver: Чистые транзакции.
  Дедуп по composite_pk (берём latest по ingested_at), приведение типов и enrichment.
  ADR: транзакции с пустым/несуществующим user_id оставляем (помечаем флагом),
       чтобы бизнес мог посчитать обе стороны (с/без unmatched).
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='composite_pk',
    options={
      'primaryKey': 'composite_pk',
      'preCombineField': 'ingested_at',
      'type': 'cow'
    },
    partition_by=['event_day']
) }}

WITH src AS (
    SELECT
        composite_pk,
        transaction_id,
        user_id,
        user_uuid,
        amount,
        currency,
        transaction_type,
        promo_code_id,
        status,
        created_at,
        created_ts,
        event_day,
        ingested_at,
        row_number() OVER (
            PARTITION BY composite_pk
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'transactions') }}
    {% if is_incremental() %}
      -- daily-инкремент: ровно одна партиция за run_date.
      -- Late-arriving коррекции исходных файлов того же дня покрываются
      -- через Hudi precombine (max(ingested_at)) при следующем daily-прогоне
      -- если он попадёт на тот же event_day; иначе — dry-run-семантика
      -- (см. lab08/FIX_PLAN.md, P0-1).
      WHERE event_day = {{ run_date() }}
    {% endif %}
),
dedup AS (
    SELECT * FROM src WHERE rn = 1
),
-- DQ-флаг (FIX_PLAN P1-10): дубль `transaction_id` в рамках run_date.
-- Учитываем только не-NULL transaction_id (NULL'ы — не дубли по бизнесу,
-- а отсутствие идентификатора, для них работает composite_pk).
tx_id_dup AS (
    SELECT transaction_id
    FROM dedup
    WHERE transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING count(*) > 1
),
flagged AS (
    SELECT
        d.composite_pk,
        d.transaction_id,
        d.user_id,
        d.user_uuid,
        d.amount,
        d.currency,
        d.transaction_type,
        d.promo_code_id,
        d.status,
        d.created_at,
        d.created_ts,
        d.event_day,
        d.ingested_at,
        CASE WHEN d.user_id IS NULL THEN true ELSE false END        AS is_user_missing,
        CASE WHEN u.user_id IS NULL AND d.user_id IS NOT NULL
             THEN true ELSE false END                               AS is_user_unknown,
        CASE WHEN tu.test_user_uuid IS NOT NULL OR coalesce(u.is_test_user, false)
             THEN true ELSE false END                               AS is_test_user,
        -- DQ (FIX_PLAN P1-7): источники признака test_user расходятся.
        -- Истина — UNION (см. is_test_user выше); этот флаг показывает, что
        -- в `users.is_test_user` и `test_users.jsonl` разные ответы для одного
        -- user — нужно для DQ-алертов и аудита.
        CASE WHEN d.user_id IS NOT NULL
                  AND coalesce(u.is_test_user, false) <> (tu.test_user_uuid IS NOT NULL)
             THEN true ELSE false END                               AS is_test_user_inconsistent,
        CASE WHEN d.amount IS NULL OR d.amount <= 0
             THEN true ELSE false END                               AS is_amount_invalid,
        CASE WHEN d.status = 'completed' AND d.transaction_type = 'purchase'
             THEN true ELSE false END                               AS is_revenue_eligible,
        -- DQ (FIX_PLAN P1-10): несколько транзакций с одинаковым transaction_id
        -- внутри run_date — кандидаты на double-counting и неоднозначные cancellation joins.
        CASE WHEN d.transaction_id IS NOT NULL AND dup.transaction_id IS NOT NULL
             THEN true ELSE false END                               AS is_transaction_id_duplicated,
        -- DQ (FIX_PLAN P1-3): использование промокода после `expiry_date`.
        -- expiry_date — DATE, считаем как «истёк, если created_ts >= expiry_date+1 day»
        -- (промокод действителен в течение дня expiry_date включительно).
        CASE WHEN d.promo_code_id IS NOT NULL
                  AND pc.expiry_date IS NOT NULL
                  AND d.created_ts >= date_add(pc.expiry_date, 1)
             THEN true ELSE false END                               AS is_promo_expired_at_use
    FROM dedup d
    LEFT JOIN {{ source('bronze', 'users') }}      u   ON d.user_id = u.user_id
    LEFT JOIN {{ source('bronze', 'test_users') }} tu  ON d.user_uuid = tu.test_user_uuid
    LEFT JOIN {{ source('bronze', 'promo_codes') }} pc ON d.promo_code_id = pc.promo_code_id
    LEFT JOIN tx_id_dup dup                            ON d.transaction_id = dup.transaction_id
)
SELECT * FROM flagged
