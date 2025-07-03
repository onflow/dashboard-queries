WITH 
cadence_txs AS (
    SELECT 
        TRUNC(block_timestamp, 'week') AS week,
        COUNT(*) as cadence_transactions,
        COUNT(DISTINCT event_contract) as contracts_interacted_with
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL
    GROUP BY 1
),
evm_txs AS (
    SELECT 
        TRUNC(block_timestamp, 'week') AS week,
        COUNT(*) as evm_transactions,
        COUNT(DISTINCT to_address) as contracts_interacted_with
    FROM 
        flow.core_evm.fact_transactions
    WHERE 
        tx_succeeded = true
        AND to_address IS NOT NULL 
        AND input_data IS NOT NULL 
        AND input_data != '0x'
    GROUP BY 1
),
combined_metrics AS (
    SELECT 
        COALESCE(c.week, e.week) AS week,
        COALESCE(c.cadence_transactions, 0) as cadence_transactions,
        COALESCE(c.contracts_interacted_with, 0) as cadence_contracts,
        COALESCE(e.evm_transactions, 0) as evm_transactions,
        COALESCE(e.contracts_interacted_with, 0) as evm_contracts,
        COALESCE(c.cadence_transactions, 0) + COALESCE(e.evm_transactions, 0) as total_transactions,
        -- Calculate week-over-week growth
        LAG(COALESCE(c.cadence_transactions, 0) + COALESCE(e.evm_transactions, 0), 1) 
            OVER (ORDER BY COALESCE(c.week, e.week)) as prev_week_txs
    FROM 
        cadence_txs c
        FULL OUTER JOIN evm_txs e ON c.week = e.week
),
all_time as (
    SELECT 
        week,
        cadence_transactions,
        cadence_contracts,
        evm_transactions,
        evm_contracts,
        total_transactions,
        ROUND(((total_transactions - prev_week_txs) / NULLIF(prev_week_txs, 0)) * 100, 2) as wow_growth_pct,
        -- Calculate 4-week moving average for transactions
        AVG(cadence_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence_txs,
        AVG(evm_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm_txs
    FROM 
        combined_metrics
    ORDER BY 
        week DESC
),
last_year as (
    SELECT 
        week,
        cadence_transactions,
        cadence_contracts,
        evm_transactions,
        evm_contracts,
        total_transactions,
        ROUND(((total_transactions - prev_week_txs) / NULLIF(prev_week_txs, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence_txs,
        AVG(evm_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm_txs
    FROM 
        combined_metrics
    WHERE 
        week >= current_date - INTERVAL '1 YEAR'
    ORDER BY 
        week DESC
),
last_3_months as (
    SELECT 
        week,
        cadence_transactions,
        cadence_contracts,
        evm_transactions,
        evm_contracts,
        total_transactions,
        ROUND(((total_transactions - prev_week_txs) / NULLIF(prev_week_txs, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence_txs,
        AVG(evm_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm_txs
    FROM 
        combined_metrics
    WHERE 
        week >= current_date - INTERVAL '3 MONTHS'
    ORDER BY 
        week DESC
),
last_month as (
    SELECT 
        week,
        cadence_transactions,
        cadence_contracts,
        evm_transactions,
        evm_contracts,
        total_transactions,
        ROUND(((total_transactions - prev_week_txs) / NULLIF(prev_week_txs, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence_txs,
        AVG(evm_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm_txs
    FROM 
        combined_metrics
    WHERE 
        week >= current_date - INTERVAL '1 MONTH'
    ORDER BY 
        week DESC
),
last_week as (
    SELECT 
        week,
        cadence_transactions,
        cadence_contracts,
        evm_transactions,
        evm_contracts,
        total_transactions,
        ROUND(((total_transactions - prev_week_txs) / NULLIF(prev_week_txs, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence_txs,
        AVG(evm_transactions) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm_txs
    FROM 
        combined_metrics
    WHERE 
        week >= current_date - INTERVAL '1 WEEK'
    ORDER BY 
        week DESC
)
SELECT * FROM {{Period}} ORDER BY week DESC
