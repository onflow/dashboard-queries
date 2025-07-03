WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date
), active_addresses AS (
    SELECT
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS active_addresses
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a 
    JOIN time_periods tp
    WHERE
        block_timestamp >= tp.start_date

    UNION ALL

    SELECT
        COUNT(DISTINCT from_address) AS active_addresses
    FROM
        flow.core_evm.fact_transactions
    JOIN time_periods tp
    WHERE
        block_timestamp >= tp.start_date
)
SELECT
    SUM(active_addresses) AS total_active_addresses
FROM
    active_addresses
