WITH contract_transactions AS (
    SELECT
        DATE_TRUNC('week', ft.block_timestamp) as week,
        fe.event_contract,
        COUNT(DISTINCT ft.tx_id) as total_transactions
    FROM
        flow.core.fact_transactions ft
    JOIN 
        flow.core.fact_events fe
        ON ft.tx_id = fe.tx_id
    WHERE
        ft.block_timestamp >= DATEADD('week', -12, CURRENT_DATE())
        AND fe.event_contract NOT IN (
            'A.f233dcee88fe0abe.FungibleToken',
            'A.f919ee77447b7497.FlowFees',
            'A.1654653399040a61.FlowToken',
            'A.1d7e57aa55817448.NonFungibleToken',
            'A.1d7e57aa55817448.NonFungibleToken.NFT.ResourceDestr',
            'A.e467b9dd11fa00df.EVM',
            'A.4eb8a10cb9f87357.NFTStorefrontV2',
            'A.b8ea91944fd51c43.OffersV2',
            'A.4eb8a10cb9f87357.NFTStorefront',
            'A.ead892083b3e2c6c.DapperUtilityCoin',
            'flow'
        )
    GROUP BY
        1, 2
    HAVING 
        total_transactions >= 50000
)
SELECT
    week as date,
    event_contract,
    total_transactions
FROM
    contract_transactions
ORDER BY
    date DESC,
    total_transactions DESC;