WITH base_transactions AS (
    SELECT
        block_timestamp,
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp,
        'Cadence' as type,
        tx_id as transaction_id
    FROM
        flow.core.fact_transactions
    WHERE
        block_timestamp >= 
            CASE '{{Period}}'
                WHEN 'all_time' THEN '2020-01-01'::DATE
                WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
                WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
                WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
                WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
                WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
            END
    UNION ALL
    SELECT
        block_timestamp,
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp,
        'EVM' as type,
        tx_hash as transaction_id
    FROM
        flow.core_evm.fact_transactions
    WHERE
        block_timestamp >= 
            CASE '{{Period}}'
                WHEN 'all_time' THEN '2020-01-01'::DATE
                WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
                WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
                WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
                WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
                WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
            END
),

time_based_aggregation AS (
    SELECT
        time_stamp,
        type,
        COUNT(DISTINCT transaction_id) as total_transactions
    FROM base_transactions
    WHERE time_stamp < 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
    GROUP BY 1, 2
),

period_comparison AS (
    SELECT
        time_stamp,
        type,
        total_transactions as current_period_transactions,
        LAG(total_transactions) OVER (PARTITION BY type ORDER BY time_stamp) as previous_period_transactions,
        SUM(total_transactions) OVER (PARTITION BY type ORDER BY time_stamp) as cumulative_transactions
    FROM time_based_aggregation
)

SELECT
    time_stamp as period,
    type,
    current_period_transactions as transactions,
    CONCAT(
        current_period_transactions, 
        ' (', 
        COALESCE(current_period_transactions - previous_period_transactions, 0),
        ')'
    ) as transactions_diff,
    CASE 
        WHEN previous_period_transactions > 0 
        THEN ((current_period_transactions - previous_period_transactions)::float / previous_period_transactions) * 100 
        ELSE 0 
    END as pcg_diff,
    cumulative_transactions as total_transactions
FROM period_comparison
ORDER BY period DESC
