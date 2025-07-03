WITH cadence_contracts AS (
    SELECT 
        event_contract as contract_name,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL
    GROUP BY 
        event_contract
),
evm_contracts AS (
    SELECT 
        t.to_address as contract_address,
        c.name as contract_name,
        COUNT(DISTINCT t.from_address) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core_evm.fact_transactions t
        LEFT JOIN flow.core_evm.dim_contracts c ON t.to_address = c.address
    WHERE 
        t.tx_succeeded = true
        AND t.to_address IS NOT NULL 
        AND t.input_data IS NOT NULL 
        AND t.input_data != '0x'
    GROUP BY 
        t.to_address,
        c.name
),
all_time as (
    SELECT 
    contract_name,
    unique_users,
    total_interactions,
    avg_interactions_per_user
FROM 
    (SELECT 
        event_contract as contract_name,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL
    GROUP BY 
        event_contract)
WHERE 
    unique_users >= 5  -- Filter out contracts with very few users
QUALIFY 
    ROW_NUMBER() OVER (ORDER BY total_interactions DESC) <= 10  -- Top 10
ORDER BY 
    unique_users DESC
),
last_year as (
    SELECT 
    contract_name,
    unique_users,
    total_interactions,
    avg_interactions_per_user
FROM 
    (SELECT 
        event_contract as contract_name,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL and block_timestamp >= current_date - INTERVAL '1 YEAR'
    GROUP BY 
        event_contract)
WHERE 
      unique_users >= 5  -- Filter out contracts with very few users
QUALIFY 
    ROW_NUMBER() OVER (ORDER BY total_interactions DESC) <= 10  -- Top 10
ORDER BY 
    unique_users DESC
),
last_3_months as (
    SELECT 
    contract_name,
    unique_users,
    total_interactions,
    avg_interactions_per_user
FROM 
    (SELECT 
        event_contract as contract_name,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL and block_timestamp >= current_date - INTERVAL '3 MONTHS'
    GROUP BY 
        event_contract)
WHERE 
      unique_users >= 5  -- Filter out contracts with very few users
QUALIFY 
    ROW_NUMBER() OVER (ORDER BY total_interactions DESC) <= 10  -- Top 10
ORDER BY 
    unique_users DESC
),
last_month as (
    SELECT 
    contract_name,
    unique_users,
    total_interactions,
    avg_interactions_per_user
FROM 
    (SELECT 
        event_contract as contract_name,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL and block_timestamp >= current_date - INTERVAL '1 MONTH'
    GROUP BY 
        event_contract)
WHERE 
      unique_users >= 5  -- Filter out contracts with very few users
QUALIFY 
    ROW_NUMBER() OVER (ORDER BY total_interactions DESC) <= 10  -- Top 10
ORDER BY 
    unique_users DESC
),
last_week as (
    SELECT 
    contract_name,
    unique_users,
    total_interactions,
    avg_interactions_per_user
FROM 
    (SELECT 
        event_contract as contract_name,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((total_interactions/unique_users),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL and block_timestamp >= current_date - INTERVAL '1 WEEK'
    GROUP BY 
        event_contract)
WHERE 
      unique_users >= 5  -- Filter out contracts with very few users
QUALIFY 
    ROW_NUMBER() OVER (ORDER BY total_interactions DESC) <= 10  -- Top 10
ORDER BY 
    unique_users DESC
)
SELECT * FROM {{Period}} ORDER BY total_interactions DESC
