WITH time_periods AS (
    SELECT
        '2020-01-01'::DATE AS start_date
), 
new_accounts_last_24h AS (
    -- New accounts on Flow core
    SELECT
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS new_accounts_24h
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a,
        time_periods tp
    WHERE
        block_timestamp >= tp.start_date
        AND block_timestamp <= CURRENT_DATE
        AND NOT EXISTS (
            SELECT 1
            FROM flow.core.ez_transaction_actors AS prev_b,
            LATERAL FLATTEN(INPUT => prev_b.actors) AS prev_a
            WHERE
                prev_b.block_timestamp < tp.start_date
                AND CAST(prev_a.value AS VARCHAR) = CAST(a.value AS VARCHAR)
        )

    UNION ALL

    -- New accounts on Flow EVM
    SELECT
        COUNT(DISTINCT from_address) AS new_accounts_24h
    FROM
        flow.core_evm.fact_transactions ft
    CROSS JOIN time_periods tp
    WHERE
        block_timestamp >= tp.start_date
        AND block_timestamp <= CURRENT_DATE
        AND NOT EXISTS (
            SELECT 1
            FROM flow.core_evm.fact_transactions AS prev_evm
            WHERE 
                prev_evm.from_address = ft.from_address
                AND prev_evm.block_timestamp < tp.start_date
        )
),
new_num as (
SELECT
    SUM(new_accounts_24h) AS total_new_accounts_last_24h
FROM
    new_accounts_last_24h
)
select total_new_accounts_last_24h
from new_num
