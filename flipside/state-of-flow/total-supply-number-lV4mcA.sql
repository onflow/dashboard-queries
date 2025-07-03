WITH api_data AS (
    SELECT livequery.live.udf_api(
        'https://api.coingecko.com/api/v3/coins/flow/market_chart?vs_currency=usd&days=360&interval=daily'
    ) AS resp
),
prices AS (
    SELECT 
        dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date,
        value[1] AS price
    FROM api_data, LATERAL FLATTEN(input => resp:data:prices)
),
market_caps AS (
    SELECT 
        dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date,
        value[1] AS market_cap
    FROM api_data, LATERAL FLATTEN(input => resp:data:market_caps)
),
total_volumes AS (
    SELECT 
        dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date,
        value[1] AS volume
    FROM api_data, LATERAL FLATTEN(input => resp:data:total_volumes)
),
parsed_data AS (
    SELECT 
        p.date, 
        'FLOW' AS token, 
        mc.market_cap, 
        p.price, 
        tv.volume
    FROM prices p
    JOIN market_caps mc ON p.date = mc.date
    JOIN total_volumes tv ON p.date = tv.date
    WHERE p.date < CURRENT_DATE
),

total_supply_data AS (
    SELECT
        date,
        market_cap / price AS total_supply
    FROM parsed_data
),

flow_tvl_data AS (
    SELECT
        date,
        category,
        SUM(chain_tvl) AS staked_locked
    FROM external.defillama.fact_protocol_tvl
    WHERE chain ILIKE '%Flow%'
    GROUP BY date, category
),

grouped_tvl_data AS (
    SELECT
        date,
        SUM(CASE WHEN category = 'Staking Pool' THEN staked_locked ELSE 0 END) AS staked_locked,
        SUM(CASE WHEN category = 'Liquid Staking' THEN staked_locked ELSE 0 END) AS liquid_staking,
        SUM(CASE WHEN category IN ('Lending', 'Services', 'Derivatives', 'Launchpad') THEN staked_locked ELSE 0 END) AS non_staked_locked
    FROM flow_tvl_data
    GROUP BY date
),

staking_data AS (
    WITH staking AS (
        SELECT
            trunc(block_timestamp, 'day') AS date,
            SUM(amount) AS staked_volume
        FROM flow.gov.ez_staking_actions  
        WHERE action IN ('DelegatorTokensCommitted', 'TokensCommitted')
        GROUP BY 1
    ),
    unstaking AS (
        SELECT
            trunc(block_timestamp, 'day') AS date,
            SUM(amount) AS unstaked_volume
        FROM flow.gov.ez_staking_actions  
        WHERE action IN ('UnstakedTokensWithdrawn', 'DelegatorUnstakedTokensWithdrawn')
        GROUP BY 1
    )
    SELECT
        s.date,
        COALESCE(s.staked_volume, 0) - COALESCE(u.unstaked_volume, 0) AS net_staked_volume,
        SUM(COALESCE(s.staked_volume, 0) - COALESCE(u.unstaked_volume, 0)) OVER (ORDER BY s.date) + 1.6e8 AS total_staked_volume
    FROM staking s
    LEFT JOIN unstaking u ON s.date = u.date
),

combined_tvl_staking_data AS (
    SELECT
        gt.date,
        gt.staked_locked + sd.total_staked_volume AS total_staked_locked,
        gt.liquid_staking,
        gt.non_staked_locked
    FROM grouped_tvl_data gt
    JOIN staking_data sd ON gt.date = sd.date
),

supply_breakdown AS (
    SELECT
        p.date,
        'FLOW' AS token,
        MAX(p.market_cap) AS market_cap,
        MAX(p.price) AS price,
        MAX(p.volume) AS volume,
        MAX(ts.total_supply) AS total_supply,
        MAX(ct.total_staked_locked) AS staked_locked,
        MAX(ct.non_staked_locked) AS non_staked_locked,
        MAX(ct.liquid_staking) AS liquid_supply,
        MAX(ts.total_supply) - (MAX(ct.total_staked_locked) + MAX(ct.liquid_staking) + MAX(ct.non_staked_locked)) AS staked_circulating
    FROM parsed_data p
    JOIN total_supply_data ts ON p.date = ts.date
    JOIN combined_tvl_staking_data ct ON p.date = ct.date
    GROUP BY p.date
)

SELECT
    date,
    token,
    total_supply AS total_supply_actual,  -- Represents total supply
    staked_locked,  -- Staked + locked volume
    non_staked_locked,  -- Locked but not staked
    total_supply - (staked_locked + liquid_supply + non_staked_locked) AS unstaked_circulating,  -- Better label
    liquid_supply,  -- Liquid staking
    total_supply_actual - unstaked_circulating AS free_circulating_supply  -- Available for trading
FROM supply_breakdown
ORDER BY date desc;
