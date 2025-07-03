WITH base_data AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp,
        actor_address
    FROM (
        -- Flow Core accounts
        SELECT 
            b.block_timestamp,
            CAST(a.value AS VARCHAR) as actor_address
        FROM flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a
        WHERE b.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
        
        UNION ALL
        
        -- EVM accounts
        SELECT 
            block_timestamp,
            from_address as actor_address
        FROM flow.core_evm.fact_transactions
        WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    )
    WHERE time_stamp < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
),

new_accounts AS (
    SELECT 
        COUNT(DISTINCT actor_address) as daily_new_accounts
    FROM (
        SELECT 
            time_stamp,
            actor_address,
            ROW_NUMBER() OVER (PARTITION BY actor_address ORDER BY time_stamp) AS rn
        FROM base_data
    )
    WHERE rn = 1
)

SELECT
    daily_new_accounts as total_new_accounts_last_24h
FROM new_accounts

-- WITH time_periods AS (
--     SELECT
--         CASE '{{Period}}'
--             WHEN 'all_time' THEN '2020-01-01'::DATE
--             WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
--             WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
--             WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
--             WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
--             ELSE CURRENT_DATE - INTERVAL '1 DAY'
--         END AS start_date
-- ),
-- new_accounts_last_24h AS (
--     -- New accounts on Flow core
--     SELECT
--         COUNT(DISTINCT CAST(value AS VARCHAR)) AS new_accounts_24h
--     FROM
--         flow.core.ez_transaction_actors AS b,
--         LATERAL FLATTEN(INPUT => b.actors) AS a,
--         time_periods tp
--     WHERE
--         block_timestamp >= tp.start_date
--         AND block_timestamp <= CURRENT_DATE
--         AND NOT EXISTS (
--             SELECT 1
--             FROM flow.core.ez_transaction_actors AS prev_b,
--             LATERAL FLATTEN(INPUT => prev_b.actors) AS prev_a
--             WHERE
--                 prev_b.block_timestamp < tp.start_date
--                 AND CAST(prev_a.value AS VARCHAR) = CAST(a.value AS VARCHAR)
--         )

--     UNION ALL

--     -- New accounts on Flow EVM
--     SELECT
--         COUNT(DISTINCT from_address) AS new_accounts_24h
--     FROM
--         flow.core_evm.fact_transactions ft
--     CROSS JOIN time_periods tp
--     WHERE
--         block_timestamp >= tp.start_date
--         AND block_timestamp <= CURRENT_DATE
--         AND NOT EXISTS (
--             SELECT 1
--             FROM flow.core_evm.fact_transactions AS prev_evm
--             WHERE 
--                 prev_evm.from_address = ft.from_address
--                 AND prev_evm.block_timestamp < tp.start_date
--         )
-- )
-- SELECT
--     SUM(new_accounts_24h) AS total_new_accounts_last_24h
-- FROM
--     new_accounts_last_24h
