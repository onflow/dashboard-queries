WITH weekly_metrics AS (
    SELECT 
        t.to_address as contract_address,
        CASE WHEN c.name IS NOT NULL THEN c.name
            WHEN t.to_address='0xf45afe28fd5519d5f8c1d4787a4d5f724c0efa4d' THEN 'Punchswap'
            ELSE t.to_address 
        END as contract_name,
        DATE_TRUNC('week', block_timestamp) as week_start,
        COUNT(DISTINCT t.from_address) as unique_users,
        COUNT(*) as total_interactions,
        ROUND((COUNT(*)::float/COUNT(DISTINCT t.from_address)),2) as avg_interactions_per_user
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
        c.name,
        week_start
    HAVING 
        unique_users >= 5
),

rolling_avg AS (
    SELECT 
        contract_name,
        week_start,
        unique_users,
        total_interactions,
        avg_interactions_per_user,
        AVG(unique_users) OVER (
            PARTITION BY contract_name 
            ORDER BY week_start 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as rolling_4week_users,
        AVG(total_interactions) OVER (
            PARTITION BY contract_name 
            ORDER BY week_start 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as rolling_4week_interactions,
        AVG(avg_interactions_per_user) OVER (
            PARTITION BY contract_name 
            ORDER BY week_start 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as rolling_4week_avg_interactions
    FROM weekly_metrics
),

top_contracts_all_time AS (
    SELECT 
        contract_name
    FROM rolling_avg
    GROUP BY contract_name
    QUALIFY ROW_NUMBER() OVER (ORDER BY MAX(unique_users) DESC) <= 10
),

all_time as (
    SELECT 
        r.contract_name,
        r.week_start,
        r.unique_users as weekly_users,
        LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start) as prev_week_users,
        ((r.unique_users - LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start)) / 
         NULLIF(LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start), 0) * 100) as wow_users_change,
        r.total_interactions as weekly_interactions,
        r.avg_interactions_per_user as weekly_avg_interactions,
        ROUND(r.rolling_4week_users, 2) as rolling_4week_users,
        ROUND(r.rolling_4week_interactions, 2) as rolling_4week_interactions,
        ROUND(r.rolling_4week_avg_interactions, 2) as rolling_4week_avg_interactions
    FROM rolling_avg r
    INNER JOIN top_contracts_all_time t ON r.contract_name = t.contract_name
),

top_contracts_1_year AS (
    SELECT 
        contract_name
    FROM rolling_avg
    WHERE week_start >= current_date - INTERVAL '1 YEAR'
    GROUP BY contract_name
    QUALIFY ROW_NUMBER() OVER (ORDER BY MAX(unique_users) DESC) <= 10
),

last_year as (
    SELECT 
        r.contract_name,
        r.week_start,
        r.unique_users as weekly_users,
        LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start) as prev_week_users,
        ((r.unique_users - LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start)) / 
         NULLIF(LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start), 0) * 100) as wow_users_change,
        r.total_interactions as weekly_interactions,
        r.avg_interactions_per_user as weekly_avg_interactions,
        ROUND(r.rolling_4week_users, 2) as rolling_4week_users,
        ROUND(r.rolling_4week_interactions, 2) as rolling_4week_interactions,
        ROUND(r.rolling_4week_avg_interactions, 2) as rolling_4week_avg_interactions
    FROM rolling_avg r
    INNER JOIN top_contracts_1_year t ON r.contract_name = t.contract_name
),

top_contracts_3_months AS (
    SELECT 
        contract_name
    FROM rolling_avg
    WHERE week_start >= current_date - INTERVAL '3 MONTHS'
    GROUP BY contract_name
    QUALIFY ROW_NUMBER() OVER (ORDER BY MAX(unique_users) DESC) <= 10
),

last_3_months as (
    SELECT 
        r.contract_name,
        r.week_start,
        r.unique_users as weekly_users,
        LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start) as prev_week_users,
        ((r.unique_users - LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start)) / 
         NULLIF(LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start), 0) * 100) as wow_users_change,
        r.total_interactions as weekly_interactions,
        r.avg_interactions_per_user as weekly_avg_interactions,
        ROUND(r.rolling_4week_users, 2) as rolling_4week_users,
        ROUND(r.rolling_4week_interactions, 2) as rolling_4week_interactions,
        ROUND(r.rolling_4week_avg_interactions, 2) as rolling_4week_avg_interactions
    FROM rolling_avg r
    INNER JOIN top_contracts_3_months t ON r.contract_name = t.contract_name
),

top_contracts_1_month AS (
    SELECT 
        contract_name
    FROM rolling_avg
    WHERE week_start >= current_date - INTERVAL '1 MONTH'
    GROUP BY contract_name
    QUALIFY ROW_NUMBER() OVER (ORDER BY MAX(unique_users) DESC) <= 10
),

last_month as (
    SELECT 
        r.contract_name,
        r.week_start,
        r.unique_users as weekly_users,
        LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start) as prev_week_users,
        ((r.unique_users - LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start)) / 
         NULLIF(LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start), 0) * 100) as wow_users_change,
        r.total_interactions as weekly_interactions,
        r.avg_interactions_per_user as weekly_avg_interactions,
        ROUND(r.rolling_4week_users, 2) as rolling_4week_users,
        ROUND(r.rolling_4week_interactions, 2) as rolling_4week_interactions,
        ROUND(r.rolling_4week_avg_interactions, 2) as rolling_4week_avg_interactions
    FROM rolling_avg r
    INNER JOIN top_contracts_1_month t ON r.contract_name = t.contract_name
),

top_contracts_1_week AS (
    SELECT 
        contract_name
    FROM rolling_avg
    WHERE week_start >= current_date - INTERVAL '1 WEEK'
    GROUP BY contract_name
    QUALIFY ROW_NUMBER() OVER (ORDER BY MAX(unique_users) DESC) <= 10
),

last_week as (
    SELECT 
        r.contract_name,
        r.week_start,
        r.unique_users as weekly_users,
        LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start) as prev_week_users,
        ((r.unique_users - LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start)) / 
         NULLIF(LAG(r.unique_users) OVER (PARTITION BY r.contract_name ORDER BY r.week_start), 0) * 100) as wow_users_change,
        r.total_interactions as weekly_interactions,
        r.avg_interactions_per_user as weekly_avg_interactions,
        ROUND(r.rolling_4week_users, 2) as rolling_4week_users,
        ROUND(r.rolling_4week_interactions, 2) as rolling_4week_interactions,
        ROUND(r.rolling_4week_avg_interactions, 2) as rolling_4week_avg_interactions
    FROM rolling_avg r
    INNER JOIN top_contracts_1_week t ON r.contract_name = t.contract_name
)



SELECT * FROM {{Period}} 
ORDER BY week_start desc, weekly_users desc
