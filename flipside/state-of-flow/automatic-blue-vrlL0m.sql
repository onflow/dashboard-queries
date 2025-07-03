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
cadence_txs AS (
    SELECT
        count(distinct tx_id) as cadence_transactions
    FROM
        flow.core.fact_transactions ft
        CROSS JOIN time_periods tp
    WHERE
        block_timestamp >= tp.start_date
),
evm_txs AS (
    SELECT
        count(distinct tx_hash) as evm_transactions
    FROM
        flow.core_evm.fact_transactions ft
        CROSS JOIN time_periods tp
    WHERE
        block_timestamp >= tp.start_date
)
SELECT
    c.cadence_transactions as "Cadence",
    e.evm_transactions as "EVM",
    (c.cadence_transactions + e.evm_transactions) as total_transactions
FROM
    cadence_txs c
    CROSS JOIN evm_txs e;
