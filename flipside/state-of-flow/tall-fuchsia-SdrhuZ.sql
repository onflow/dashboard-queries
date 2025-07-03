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
 data AS (
    -- EVM fees in the past 24 hours
    SELECT 
        SUM(tx_fee) AS fees,
        AVG(tx_fee) AS avg_tx_fee
    FROM 
        flow.core_evm.fact_transactions
    CROSS JOIN time_periods tp
    WHERE
        block_timestamp >= tp.start_date 

    UNION ALL

    -- Flow fees in the past 24 hours
    SELECT 
        SUM(y.event_data:amount) AS fees,
        AVG(y.event_data:amount) AS avg_tx_fee
    FROM 
        flow.core.fact_transactions x
    JOIN 
        flow.core.fact_events y ON x.tx_id = y.tx_id
CROSS JOIN time_periods tp
    WHERE
        x.block_timestamp >= tp.start_date 
    and 
        y.event_contract = 'A.f919ee77447b7497.FlowFees'
        AND y.event_Type = 'FeesDeducted'
)
SELECT 
    SUM(fees) AS total_fees_flow, 
    AVG(avg_tx_fee) AS avg_tx_fee_flow  
FROM 
    data
