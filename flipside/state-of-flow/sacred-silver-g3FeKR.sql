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
        type,
        active_users
    FROM
        base_data
    WHERE time_stamp < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END

    UNION ALL

    SELECT
        time_stamp as day,
        'EVM + Cadence' AS type,
        SUM(active_users) AS active_users
    FROM
        base_data
    GROUP BY 1,2
),

final_data AS (
    SELECT
        day,
        type,
        active_users,
        CASE
            WHEN type = 'EVM + Cadence' THEN AVG(active_users) OVER (
                PARTITION BY type
                ORDER BY day
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS rolling_avg
    FROM
        aggregated_data
)

SELECT 
    day,
    type,
    active_users,
    rolling_avg
FROM final_data where day<trunc(current_date,'week')
ORDER BY day DESC, type
