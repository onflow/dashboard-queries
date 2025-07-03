WITH parsed_data AS (
    SELECT
        dateadd(ms, market_caps.value[0], to_timestamp('1970-01-01')) AS date,
        'FLOW' as token,
        AVG(market_caps.value[1]) AS market_cap,
        AVG(prices.value[1]) AS price,
        AVG(total_volumes.value[1]) AS volume
    FROM
        (
            SELECT livequery.live.udf_api(
                'https://api.coingecko.com/api/v3/coins/flow/market_chart?vs_currency=usd&days=30&interval=daily'
            ) AS resp
        ),
        LATERAL FLATTEN(input => resp:data:market_caps) AS market_caps,
        LATERAL FLATTEN(input => resp:data:prices) AS prices,
        LATERAL FLATTEN(input => resp:data:total_volumes) AS total_volumes
    WHERE date < CURRENT_DATE
    GROUP BY 1, 2
),

total_supply_data AS (
    SELECT
        date,
        market_cap * price as total_supply
from parsed_data
)

SELECT 
    'FLOW' AS token,
    MAX(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE()) THEN market_cap ELSE 0 END) AS market_cap,
    AVG(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE()) THEN price ELSE NULL END) AS price,
    AVG(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE()) THEN volume ELSE 0 END) AS volume,
    MAX(total_supply) AS total_supply
FROM
    parsed_data
LEFT JOIN total_supply_data USING(date)
GROUP BY
    token
