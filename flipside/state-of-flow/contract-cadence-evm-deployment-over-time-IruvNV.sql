WITH 
cadence as (
    SELECT
        DISTINCT authorizers[0] AS users, 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END AS debut
    FROM
        flow.core.fact_events AS x
    JOIN 
        flow.core.fact_transactions y on x.tx_id=y.tx_id
    WHERE 
        x.event_type = 'AccountContractAdded'
        AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1, 2
),
core_new_deployers AS (
    SELECT
        debut AS date, 
        COUNT(DISTINCT users) AS new_cadence_deployers
    FROM 
        cadence
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
        END AS block_timestamp,
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
        END AS block_timestamp,
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
evms2 as (
    SELECT
        DISTINCT creator AS users, 
        block_timestamp AS debut
    FROM
        evms
    GROUP BY 1, 2
),
evm_new_deployers AS (
    SELECT 
        debut AS date,
        COUNT(DISTINCT users) AS new_evm_deployers
    FROM 
        evms2
    GROUP BY 1
),
final_results AS (
    SELECT
        COALESCE(x.date, y.date) AS day,
        COALESCE(new_cadence_deployers, 0) AS new_cadence_deployerss,
        SUM(COALESCE(new_cadence_deployers, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_cadence_deployers,
        COALESCE(new_evm_deployers, 0) AS new_evm_deployerss,
        SUM(COALESCE(new_evm_deployers, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_evm_deployers,
        COALESCE(new_cadence_deployers, 0) + COALESCE(new_evm_deployers, 0) AS full_deployers,
        SUM(COALESCE(new_cadence_deployers, 0) + COALESCE(new_evm_deployers, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_full_deployers,
        AVG(COALESCE(new_cadence_deployers, 0) + COALESCE(new_evm_deployers, 0)) OVER (
            ORDER BY COALESCE(x.date, y.date)
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_full_deployers
    FROM 
        core_new_deployers x 
        FULL JOIN evm_new_deployers y ON x.date = y.date
    WHERE 
        COALESCE(x.date, y.date) < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
)
SELECT * FROM final_results
ORDER BY day DESC
