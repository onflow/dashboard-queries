WITH 
cadence AS (
    SELECT 
        DISTINCT event_contract AS contract,
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp
    FROM 
        flow.core.fact_events
    WHERE 
        block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
),
core_new_contracts AS (
    SELECT
        time_stamp AS date,
        'Cadence' as type, 
        COUNT(DISTINCT contract) AS total_new_cadence_contracts
    FROM 
        cadence
    GROUP BY 1,2
),
evms AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END as time_stamp,
        x.from_address AS creator,
        y.contract_address AS contract 
    FROM 
        flow.core_evm.fact_transactions x
    JOIN 
        flow.core_evm.fact_event_logs y 
        ON x.tx_hash = y.tx_hash 
    WHERE 
        y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    UNION
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END as time_stamp,
        x.from_address AS creator, 
        x.tx_hash AS contract 
    FROM 
        flow.core_evm.fact_transactions x
    WHERE 
        (x.origin_function_signature = '0x60c06040' OR x.origin_function_signature = '0x60806040')
        AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
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
        time_stamp AS date,
        'EVM' as type,
        COUNT(DISTINCT contract) AS total_new_evm_contracts
    FROM 
        evms
    GROUP BY 1,2
),
info AS (
    SELECT
        date as day,
        type,
        total_new_cadence_contracts AS new_contracts,
        SUM(total_new_cadence_contracts) OVER (ORDER BY date) AS total_contracts
    FROM
        core_new_contracts
    UNION
    SELECT
        date as day,
        type,
        total_new_evm_contracts AS new_contracts,
        SUM(total_new_evm_contracts) OVER (ORDER BY date) AS total_contracts
    FROM
        evm_new_contracts
)
SELECT 
    day,
    type,
    new_contracts,
    total_contracts
FROM 
    info
WHERE 
    day < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
ORDER BY 
    day DESC
