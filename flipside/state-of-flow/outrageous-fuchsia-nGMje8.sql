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
),
 cadence AS (
    SELECT
        DISTINCT authorizers[0] AS users, 
        MIN(TRUNC(x.block_timestamp, 'week')) AS debut
    FROM
        flow.core.fact_events AS x
    JOIN 
        flow.core.fact_transactions y on x.tx_id=y.tx_id
JOIN time_periods tp
WHERE
        x.block_timestamp >= tp.start_date
    and 
        x.event_type = 'AccountContractAdded'
    GROUP BY 1
)
SELECT 
    COUNT(DISTINCT users) AS total_cadence_deployers
FROM 
    cadence 


-- alt with new actors table.
--WITH cadence AS (
--    SELECT
--        DISTINCT CAST(a.value AS VARCHAR) AS users, 
--        MIN(TRUNC(b.block_timestamp, 'week')) AS debut
--    FROM
--        flow.core.fact_events AS x
--    JOIN 
--        flow.core.ez_transaction_actors AS b ON x.tx_id = b.tx_id
--    , LATERAL FLATTEN(INPUT => b.actors) AS a
--    WHERE 
--        x.event_type = 'AccountContractAdded'
--    GROUP BY 1
--)
--SELECT 
--    COUNT(DISTINCT users) AS total_cadence_deployers
--FROM 
--    cadence
