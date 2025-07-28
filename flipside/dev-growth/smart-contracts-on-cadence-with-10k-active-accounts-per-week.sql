WITH contract_users AS (
    SELECT
        trunc(ft.block_timestamp,'week') as week,
        fe.event_contract,
        count(distinct a.value) as unique_users
    FROM
        flow.core.fact_transactions ft
    JOIN 
        flow.core.fact_events fe
        ON ft.tx_id = fe.tx_id
    JOIN
        flow.core.ez_transaction_actors b
        ON ft.tx_id = b.tx_id,
        LATERAL FLATTEN(INPUT => b.actors) AS a
    WHERE
        ft.block_timestamp >= DATEADD('week', -12, CURRENT_DATE())
        AND fe.event_contract NOT IN (
            'A.f233dcee88fe0abe.FungibleToken',
            'A.f919ee77447b7497.FlowFees',
            'A.1654653399040a61.FlowToken',
            'A.1d7e57aa55817448.NonFungibleToken',
            'A.1d7e57aa55817448.NonFungibleToken.NFT.ResourceDestr',
            'A.e467b9dd11fa00df.EVM',
            'flow'
        )
        AND fe.event_contract NOT LIKE 'A.4eb8a10cb9f87357.NFTStorefrontV2.%'
        AND fe.event_contract NOT LIKE 'A.b8ea91944fd51c43.OffersV2.%'
        AND fe.event_contract NOT LIKE 'A.4eb8a10cb9f87357.NFTStorefront.%'
        AND fe.event_contract NOT LIKE 'A.ead892083b3e2c6c.DapperUtilityCoin.%'
    GROUP BY
        1, 2
)
SELECT
    week,
    event_contract,
    unique_users,
    SUM(unique_users) OVER (PARTITION BY event_contract ORDER BY week) as cum_users
FROM
    contract_users
WHERE
    unique_users >= 10000
ORDER BY
    week desc,
    unique_users desc;