WITH base_transactions AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp,
        COUNT(DISTINCT tx_id) as total_transactions
    FROM flow.core.fact_transactions
    WHERE block_timestamp >= 
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1

    UNION ALL

    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp,
        COUNT(DISTINCT tx_hash) as total_transactions
    FROM flow.core_evm.fact_transactions
    WHERE block_timestamp >= 
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1
),

aggregated_transactions AS (
    SELECT
        time_stamp,
        SUM(total_transactions) as total_transactions
    FROM base_transactions
    GROUP BY 1
),

final_metrics AS (
    SELECT
        time_stamp,
        total_transactions AS Transactions,
        LAG(total_transactions) OVER (ORDER BY time_stamp) as prev_period_transactions,
        CONCAT(
            total_transactions,
            ' (',
            COALESCE(
                total_transactions - LAG(total_transactions) OVER (ORDER BY time_stamp),
                0
            ),
            ')'
        ) AS transactions_diff,
        COALESCE(
            ((total_transactions - LAG(total_transactions) OVER (ORDER BY time_stamp)) / 
             NULLIF(LAG(total_transactions) OVER (ORDER BY time_stamp), 0)) * 100,
            0
        ) AS pcg_diff,
        SUM(total_transactions) OVER (ORDER BY time_stamp) AS Total_Transactions,
        AVG(total_transactions) OVER (
            ORDER BY time_stamp 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_avg
    FROM aggregated_transactions
    WHERE time_stamp < 
      CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END 
)

SELECT *
FROM final_metrics
ORDER BY time_stamp DESC
