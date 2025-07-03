WITH data AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as date,
        SUM(tx_fee) AS fees,
        AVG(tx_fee) AS avg_tx_fee
    FROM 
        flow.core_evm.fact_transactions
    WHERE 
        block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
        AND block_timestamp < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
    GROUP BY 1

    UNION

    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', y.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', y.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', y.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', y.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', y.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', y.block_timestamp)
        END as date,
        SUM(y.event_data:amount) AS fees,
        AVG(y.event_data:amount) AS avg_tx_fee
    FROM 
        flow.core.fact_transactions x
    JOIN 
        flow.core.fact_events y 
    ON 
        x.tx_id = y.tx_id
    WHERE 
        event_contract = 'A.f919ee77447b7497.FlowFees'
        AND event_type = 'FeesDeducted'
        AND y.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
        AND y.block_timestamp < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
    GROUP BY 1
)

SELECT 
    date, 
    SUM(fees) AS flow_fees, 
    AVG(avg_tx_fee) AS avg_tx_flow_fee, 
    SUM(SUM(fees)) OVER (ORDER BY date) AS total_flow_fees,
    AVG(SUM(fees)) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_28d_flow_fees
FROM 
    data
GROUP BY 
    date
ORDER BY 
    date DESC
