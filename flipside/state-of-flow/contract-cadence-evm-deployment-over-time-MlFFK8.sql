WITH 
cadence_usage AS (
    SELECT 
        TRUNC(block_timestamp, 'week') AS week,
        COUNT(*) as total_cadence_invocations,
        COUNT(DISTINCT event_contract) as unique_contracts_called
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        --AND block_timestamp >= DATEADD('year', -1, CURRENT_DATE())
    GROUP BY 1
),
evm_usage AS (
    SELECT 
        TRUNC(t.block_timestamp, 'week') AS week,
        COUNT(*) as total_evm_invocations,
        COUNT(DISTINCT t.to_address) as unique_contracts_called
    FROM 
        flow.core_evm.fact_transactions t
    WHERE 
        t.tx_succeeded = true
        AND t.to_address IS NOT NULL 
        AND t.input_data IS NOT NULL 
        AND t.input_data != '0x'
        --AND t.block_timestamp >= DATEADD('year', -1, CURRENT_DATE())
    GROUP BY 1
),
combined_metrics AS (
    SELECT 
        COALESCE(c.week, e.week) AS week,
        COALESCE(c.total_cadence_invocations, 0) as cadence_invocations,
        COALESCE(c.unique_contracts_called, 0) as unique_cadence_contracts,
        COALESCE(e.total_evm_invocations, 0) as evm_invocations,
        COALESCE(e.unique_contracts_called, 0) as unique_evm_contracts,
        COALESCE(c.total_cadence_invocations, 0) + COALESCE(e.total_evm_invocations, 0) as total_invocations,
        COALESCE(c.unique_contracts_called, 0) + COALESCE(e.unique_contracts_called, 0) as total_unique_contracts,
        -- Calculate week-over-week growth
        LAG(COALESCE(c.total_cadence_invocations, 0) + COALESCE(e.total_evm_invocations, 0), 1) 
            OVER (ORDER BY COALESCE(c.week, e.week)) as prev_week_invocations
    FROM 
        cadence_usage c
        FULL OUTER JOIN evm_usage e ON c.week = e.week
),
all_time as (
SELECT 
    week,
    cadence_invocations,
    unique_cadence_contracts,
    evm_invocations,
    unique_evm_contracts,
    total_invocations,
    total_unique_contracts,
    ROUND(((total_invocations - prev_week_invocations) / NULLIF(prev_week_invocations, 0)) * 100, 2) as wow_growth_pct,
    -- Calculate 4-week moving average
    AVG(total_invocations) OVER (
        ORDER BY week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4_weeks
FROM 
    combined_metrics
ORDER BY 
    week DESC
),
last_year as (
SELECT 
    week,
    cadence_invocations,
    unique_cadence_contracts,
    evm_invocations,
    unique_evm_contracts,
    total_invocations,
    total_unique_contracts,
    ROUND(((total_invocations - prev_week_invocations) / NULLIF(prev_week_invocations, 0)) * 100, 2) as wow_growth_pct,
    -- Calculate 4-week moving average
    AVG(total_invocations) OVER (
        ORDER BY week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4_weeks
FROM 
    combined_metrics
where week>=current_date-INTERVAL '1 YEAR'
ORDER BY 1 DESC
),
last_3_months as (
SELECT 
    week,
    cadence_invocations,
    unique_cadence_contracts,
    evm_invocations,
    unique_evm_contracts,
    total_invocations,
    total_unique_contracts,
    ROUND(((total_invocations - prev_week_invocations) / NULLIF(prev_week_invocations, 0)) * 100, 2) as wow_growth_pct,
    -- Calculate 4-week moving average
    AVG(total_invocations) OVER (
        ORDER BY week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4_weeks
FROM 
    combined_metrics
where week>=current_date-INTERVAL '3 MONTHS'
ORDER BY 1 DESC
),
last_month as (
SELECT 
    week,
    cadence_invocations,
    unique_cadence_contracts,
    evm_invocations,
    unique_evm_contracts,
    total_invocations,
    total_unique_contracts,
    ROUND(((total_invocations - prev_week_invocations) / NULLIF(prev_week_invocations, 0)) * 100, 2) as wow_growth_pct,
    -- Calculate 4-week moving average
    AVG(total_invocations) OVER (
        ORDER BY week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4_weeks
FROM 
    combined_metrics
where week>=current_date-INTERVAL '1 MONTH'
ORDER BY 1 DESC
),
last_week as (
SELECT 
    week,
    cadence_invocations,
    unique_cadence_contracts,
    evm_invocations,
    unique_evm_contracts,
    total_invocations,
    total_unique_contracts,
    ROUND(((total_invocations - prev_week_invocations) / NULLIF(prev_week_invocations, 0)) * 100, 2) as wow_growth_pct,
    -- Calculate 4-week moving average
    AVG(total_invocations) OVER (
        ORDER BY week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4_weeks
FROM 
    combined_metrics
where week>=current_date-INTERVAL '1 WEEK'
ORDER BY 1 DESC
)
select * from {{Period}} order by 1 desc
