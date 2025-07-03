WITH 
time_params AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END as start_date,
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END as end_date
),
cadence AS (
    SELECT 
        DISTINCT event_contract AS contract, 
        MIN(block_timestamp) AS debut
    FROM 
        flow.core.fact_events
    WHERE 
        block_timestamp >= (SELECT start_date FROM time_params)
        AND block_timestamp < (SELECT end_date FROM time_params)
    GROUP BY 1
),
core_new_contracts AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', debut)
            WHEN 'last_year' THEN DATE_TRUNC('week', debut)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', debut)
            WHEN 'last_month' THEN DATE_TRUNC('day', debut)
            WHEN 'last_week' THEN DATE_TRUNC('day', debut)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', debut)
        END AS date,
        COUNT(DISTINCT contract) AS total_new_cadence_contracts
    FROM 
        cadence
    GROUP BY 1
),
evms AS (
    SELECT 
        x.block_timestamp, 
        x.from_address AS creator,
        y.contract_address AS contract 
    FROM 
        flow.core_evm.fact_transactions x
    JOIN 
        flow.core_evm.fact_event_logs y 
        ON x.tx_hash = y.tx_hash 
    WHERE 
        y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        AND x.block_timestamp >= (SELECT start_date FROM time_params)
        AND x.block_timestamp < (SELECT end_date FROM time_params)
    UNION
    SELECT 
        x.block_timestamp, 
        x.from_address AS creator, 
        x.tx_hash AS contract 
    FROM 
        flow.core_evm.fact_transactions x
    WHERE 
        (x.origin_function_signature = '0x60c06040' OR x.origin_function_signature = '0x60806040')
        AND x.block_timestamp >= (SELECT start_date FROM time_params)
        AND x.block_timestamp < (SELECT end_date FROM time_params)
        AND x.tx_hash NOT IN (
            SELECT 
                x.tx_hash 
            FROM 
                flow.core_evm.fact_transactions x
            JOIN 
                flow.core_evm.fact_event_logs y 
                ON x.tx_hash = y.tx_hash 
            WHERE 
                y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        )
),
evm_new_contracts AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS date,
        COUNT(DISTINCT CASE WHEN creator LIKE '0x0000000000000000000000020000000000000000%' THEN contract END) AS total_coa_contracts,
        COUNT(DISTINCT CASE WHEN creator NOT LIKE '0x0000000000000000000000020000000000000000%' THEN contract END) AS total_non_coa_contracts
    FROM 
        evms
    GROUP BY 1
),
all_time as (
    SELECT
        COALESCE(x.date, y.date) AS day,
        COALESCE(total_new_cadence_contracts, 0) AS new_cadence_contracts,
        SUM(COALESCE(total_new_cadence_contracts, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_cadence_contracts,
        COALESCE(total_non_coa_contracts, 0) AS new_non_coa_evm_contracts,
        SUM(COALESCE(total_non_coa_contracts, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_non_coa_evm_contracts,
        COALESCE(total_coa_contracts, 0) AS new_coa_contracts,
        SUM(COALESCE(total_coa_contracts, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_coa_contracts,
        COALESCE(total_new_cadence_contracts, 0) + COALESCE(total_non_coa_contracts, 0) + COALESCE(total_coa_contracts, 0) AS full_contracts,
        SUM(COALESCE(total_new_cadence_contracts, 0) + COALESCE(total_non_coa_contracts, 0) + COALESCE(total_coa_contracts, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_full_contracts,
        AVG(COALESCE(total_new_cadence_contracts, 0) + COALESCE(total_non_coa_contracts, 0) + COALESCE(total_coa_contracts, 0)) OVER (
            ORDER BY COALESCE(x.date, y.date)
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_new_contracts
    FROM 
        core_new_contracts x 
        FULL JOIN evm_new_contracts y ON x.date = y.date
    ORDER BY 1 DESC
)
SELECT * FROM all_time
ORDER BY day DESC
