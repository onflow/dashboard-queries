WITH base_data AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', b.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', b.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', b.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', b.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', b.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', b.block_timestamp)
        END AS time_stamp,
        'Cadence' as type,
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS active_users
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a
    WHERE b.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1,2

    UNION ALL

    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        'EVM' as type,
        COUNT(DISTINCT from_address) AS active_users
    FROM
        flow.core_evm.fact_transactions
    WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1,2
),

aggregated_data AS (
    SELECT
        time_stamp as day,
        SUM(active_users) AS active_users
    FROM
        base_data
    GROUP BY 1
),

final_data AS (
    SELECT
        day,
        active_users,
        AVG(active_users) OVER (
                ORDER BY day
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            )
        AS rolling_avg_active_users
    FROM
        aggregated_data
),

base_data2 AS (
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
        time_stamp as day,
        COUNT(DISTINCT actor_address) as daily_new_accounts
    FROM (
        SELECT 
            time_stamp,
            actor_address,
            ROW_NUMBER() OVER (PARTITION BY actor_address ORDER BY time_stamp) AS rn
        FROM base_data2
    )
    WHERE rn = 1
    GROUP BY time_stamp
),
final_data2 as (
SELECT
    day,
    daily_new_accounts,
    SUM(daily_new_accounts) OVER (ORDER BY day) AS total_new_accounts,
    AVG(daily_new_accounts) OVER (
        ORDER BY day
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_new_accounts
FROM new_accounts
ORDER BY day DESC
)

SELECT 
    ifnull(x.day,y.day) as date,
    active_users,
    rolling_avg_active_users,
    daily_new_accounts as new_accounts,
    rolling_avg_new_accounts,
    total_new_accounts
FROM final_data x join final_data2 y on x.day=y.day
ORDER BY date DESC
