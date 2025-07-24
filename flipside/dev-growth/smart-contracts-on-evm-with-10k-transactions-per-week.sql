WITH contract_transactions AS (
    SELECT
        trunc(ft.block_timestamp,'week') as week,
        dc.name as contract_name,
        dc.address as contract_address,
        count(distinct ft.tx_hash) as total_transactions
    FROM
        flow.core_evm.fact_transactions ft
    JOIN 
        flow.core_evm.dim_contracts dc
        ON ft.to_address = dc.address
    WHERE
        ft.block_timestamp >= DATEADD('week', -12, CURRENT_DATE())
        AND ft.tx_succeeded = TRUE  -- Only include successful transactions
    GROUP BY
        1, 2, 3
    HAVING 
        total_transactions >= 10000
)
SELECT
    week,
    contract_name,
    contract_address,
    total_transactions,
    SUM(total_transactions) OVER (PARTITION BY contract_address ORDER BY week) as cum_transactions
FROM
    contract_transactions
ORDER BY
    week desc,
    total_transactions desc;