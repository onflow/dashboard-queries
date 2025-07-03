WITH weekly_metrics AS (
    SELECT 
        event_contract as contract_name,
        DATE_TRUNC('week', block_timestamp) as week_start,
        COUNT(DISTINCT event_data:from) as unique_users,
        COUNT(*) as total_interactions,
        round((COUNT(*)::float/COUNT(DISTINCT event_data:from)),2) as avg_interactions_per_user
    FROM 
        flow.core.fact_events
    WHERE 
        tx_succeeded = true
        AND event_data:from IS NOT NULL
    GROUP BY 
        event_contract,
        week_start
    HAVING 
        unique_users >= 5  -- Filter out contracts with very few users
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
