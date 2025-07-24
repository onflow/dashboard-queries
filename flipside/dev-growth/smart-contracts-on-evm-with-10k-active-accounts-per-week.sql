WITH contract_active_accounts AS (
    SELECT
        trunc(ft.block_timestamp,'week') as week,
        dc.name as contract_name,
        dc.address as contract_address,
        count(distinct ft.from_address) as active_accounts
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
        active_accounts >= 10000
)
SELECT
    week,
    contract_name,
    contract_address,
    active_accounts,
    SUM(active_accounts) OVER (PARTITION BY contract_address ORDER BY week) as cum_active_accounts
FROM
    contract_active_accounts
ORDER BY
    week desc,
    active_accounts desc;