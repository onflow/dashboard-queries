WITH parsed_data AS (
    -- Fetching price, volume, and market cap from CoinGecko
    SELECT
        dateadd(ms, market_caps.value[0], to_timestamp('1970-01-01')) AS date,
        'FLOW' AS token,
        AVG(market_caps.value[1]) AS market_cap,
        AVG(prices.value[1]) AS price,
        AVG(total_volumes.value[1]) AS volume
    FROM
        (
            SELECT livequery.live.udf_api(
                'https://api.coingecko.com/api/v3/coins/flow/market_chart?vs_currency=usd&days=180&interval=daily'
            ) AS resp
        ),
        LATERAL FLATTEN(input => resp:data:market_caps) AS market_caps,
        LATERAL FLATTEN(input => resp:data:prices) AS prices,
        LATERAL FLATTEN(input => resp:data:total_volumes) AS total_volumes
    WHERE date < CURRENT_DATE
    GROUP BY 1, 2
),

total_supply_data AS (
    -- Calculating total supply based on market cap and price
    SELECT
        date,
        market_cap / price AS total_supply
    FROM parsed_data
),

flow_tvl_data AS (
    -- Fetching TVL data from DefiLlama for Flow staking and locking
    SELECT
        date,
        category,
        SUM(chain_tvl) AS staked_locked
    FROM external.defillama.fact_protocol_tvl
    WHERE chain ilike '%Flow%'
    GROUP BY date, category
),

-- Grouping TVL data into principal supply categories
grouped_tvl_data AS (
    SELECT
        date,
        SUM(CASE WHEN category = 'Staking Pool' THEN staked_locked ELSE 0 END) AS staked_locked,
        SUM(CASE WHEN category = 'Liquid Staking' THEN staked_locked ELSE 0 END) AS liquid_staking,
        SUM(CASE WHEN category IN ('Lending', 'Services', 'Derivatives', 'Launchpad') THEN staked_locked ELSE 0 END) AS non_staked_locked
    FROM flow_tvl_data
    GROUP BY date
),

-- Staking and unstaking volumes over time
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
    -- Calculating net staking by subtracting unstaked from staked volumes
    SELECT
        s.date,
        COALESCE(s.staked_volume, 0) - COALESCE(u.unstaked_volume, 0) AS net_staked_volume,
        sum(net_staked_volume) over (order by s.date)+1.6e8 as total_staked_volume
    FROM staking s
    LEFT JOIN unstaking u ON s.date = u.date
),

combined_tvl_staking_data AS (
    SELECT
        gt.date,
        gt.staked_locked + sd.total_staked_volume AS total_staked_locked,  -- Adjusting for net staking
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
        -- Liquid supply: Total Supply - (Staked Locked + Staked Circulating + Non-Staked Locked)
        MAX(ts.total_supply) - (MAX(ct.total_staked_locked) + MAX(ct.liquid_staking) + MAX(ct.non_staked_locked)) AS staked_circulating
    FROM
        parsed_data p
    JOIN total_supply_data ts ON p.date = ts.date
    JOIN combined_tvl_staking_data ct ON p.date = ct.date
    GROUP BY p.date
)
SELECT
    staked_locked
FROM supply_breakdown
where date=current_date-1
