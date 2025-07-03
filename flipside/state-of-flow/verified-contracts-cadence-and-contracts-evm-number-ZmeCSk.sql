WITH 
cadence AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as time_stamp,
        COUNT(DISTINCT event_contract) AS total_new_cadence_contracts
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
    GROUP BY 1
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
        COUNT(DISTINCT CASE 
            WHEN y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%' 
            THEN y.contract_address 
            ELSE x.tx_hash 
        END) AS total_new_evm_contracts
    FROM 
        flow.core_evm.fact_transactions x
    LEFT JOIN 
        flow.core_evm.fact_event_logs y 
        ON x.tx_hash = y.tx_hash 
    WHERE 
        (y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        OR (x.origin_function_signature IN ('0x60c06040', '0x60806040')))
        AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1
)
SELECT
    COALESCE(c.time_stamp, e.time_stamp) AS day,
    COALESCE(c.total_new_cadence_contracts, 0) AS new_cadence_contracts,
    SUM(COALESCE(c.total_new_cadence_contracts, 0)) OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp)) AS total_cadence_contracts,
    COALESCE(e.total_new_evm_contracts, 0) AS new_evm_contracts,
    SUM(COALESCE(e.total_new_evm_contracts, 0)) OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp)) AS total_evm_contracts,
    COALESCE(c.total_new_cadence_contracts, 0) + COALESCE(e.total_new_evm_contracts, 0) AS full_contracts,
    SUM(COALESCE(c.total_new_cadence_contracts, 0) + COALESCE(e.total_new_evm_contracts, 0)) 
        OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp)) AS total_full_contracts,
    AVG(COALESCE(c.total_new_cadence_contracts, 0) + COALESCE(e.total_new_evm_contracts, 0)) 
        OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp) ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_new_contracts
FROM 
    cadence c
    FULL JOIN evms e ON c.time_stamp = e.time_stamp
WHERE 
    COALESCE(c.time_stamp, e.time_stamp) < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
ORDER BY 1 DESC
