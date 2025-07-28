WITH contract_active_accounts AS (
    SELECT
        DATE_TRUNC('week', ft.block_timestamp) as week,
        dc.name as contract_name,
        dc.address as contract_address,
        COUNT(DISTINCT ft.from_address) as active_accounts
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
    week as date,
    CASE 
        WHEN contract_name IS NOT NULL AND contract_name != '' 
        THEN contract_address || ' (' || contract_name || ')'
        ELSE contract_address
    END as contract_identifier,
    active_accounts
FROM
    contract_active_accounts
ORDER BY
    date DESC,
    active_accounts DESC;