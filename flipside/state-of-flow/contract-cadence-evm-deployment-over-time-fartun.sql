WITH 
cadence_users AS (
    SELECT 
        TRUNC(block_timestamp, 'week') AS week,
        COUNT(DISTINCT event_data:from) as active_cadence_users,
        COUNT(DISTINCT event_contract) as contracts_interacted_with
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL
    GROUP BY 1
),
evm_users AS (
    SELECT 
        TRUNC(block_timestamp, 'week') AS week,
        COUNT(DISTINCT from_address) as active_evm_users,
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
        COALESCE(c.active_cadence_users, 0) as cadence_users,
        COALESCE(c.contracts_interacted_with, 0) as cadence_contracts,
        COALESCE(e.active_evm_users, 0) as evm_users,
        COALESCE(e.contracts_interacted_with, 0) as evm_contracts,
        COALESCE(c.active_cadence_users, 0) + COALESCE(e.active_evm_users, 0) as total_active_users,
        -- Calculate week-over-week growth
        LAG(COALESCE(c.active_cadence_users, 0) + COALESCE(e.active_evm_users, 0), 1) 
            OVER (ORDER BY COALESCE(c.week, e.week)) as prev_week_users
    FROM 
        cadence_users c
        FULL OUTER JOIN evm_users e ON c.week = e.week
),
all_time as (
    SELECT 
        week,
        cadence_users,
        cadence_contracts,
        evm_users,
        evm_contracts,
        total_active_users,
        ROUND(((total_active_users - prev_week_users) / NULLIF(prev_week_users, 0)) * 100, 2) as wow_growth_pct,
        -- Calculate 4-week moving average
        AVG(cadence_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence,
        AVG(evm_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm
    FROM 
        combined_metrics
    ORDER BY 
        week DESC
),
last_year as (
    SELECT 
        week,
        cadence_users,
        cadence_contracts,
        evm_users,
        evm_contracts,
        total_active_users,
        ROUND(((total_active_users - prev_week_users) / NULLIF(prev_week_users, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence,
        AVG(evm_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm
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
        cadence_users,
        cadence_contracts,
        evm_users,
        evm_contracts,
        total_active_users,
        ROUND(((total_active_users - prev_week_users) / NULLIF(prev_week_users, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence,
        AVG(evm_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm
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
        cadence_users,
        cadence_contracts,
        evm_users,
        evm_contracts,
        total_active_users,
        ROUND(((total_active_users - prev_week_users) / NULLIF(prev_week_users, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence,
        AVG(evm_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm
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
        cadence_users,
        cadence_contracts,
        evm_users,
        evm_contracts,
        total_active_users,
        ROUND(((total_active_users - prev_week_users) / NULLIF(prev_week_users, 0)) * 100, 2) as wow_growth_pct,
        AVG(cadence_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_cadence,
        AVG(evm_users) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_4_weeks_evm
    FROM 
        combined_metrics
    WHERE 
        week >= current_date - INTERVAL '1 WEEK'
    ORDER BY 
        week DESC
)
SELECT * FROM {{Period}} ORDER BY week DESC;
