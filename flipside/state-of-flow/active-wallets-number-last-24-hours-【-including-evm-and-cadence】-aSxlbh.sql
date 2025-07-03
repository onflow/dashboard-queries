WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date,
        CURRENT_DATE AS end_date
),
active_addresses AS (
    SELECT
        COUNT(DISTINCT from_address) AS active_addresses
    FROM
        flow.core_evm.fact_transactions ft
    CROSS JOIN time_periods tp
    WHERE
        ft.block_timestamp >= tp.start_date
        AND ft.block_timestamp < tp.end_date
)
SELECT 
    active_addresses as total_active_addresses
FROM 
    active_addresses
