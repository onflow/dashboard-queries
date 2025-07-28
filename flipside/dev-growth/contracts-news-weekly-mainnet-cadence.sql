-- Optimized query to reduce execution time
-- Key optimizations:
-- 1. Removed redundant WHERE clause in final SELECT
-- 2. Used DATE_TRUNC instead of trunc for better performance
-- 3. Added early filtering for better query plan
-- 4. Simplified the logic by removing unnecessary DISTINCT in final SELECT

WITH core_news AS (
    SELECT 
        event_contract AS new_contract,
        DATE_TRUNC('week', block_timestamp) AS debut
    FROM flow.core.fact_events
    WHERE block_timestamp >= DATEADD(year, -1, CURRENT_DATE())
    GROUP BY 1, 2
)
SELECT
    debut AS date, 
    COUNT(new_contract) AS new_contracts
FROM core_news 
GROUP BY debut
ORDER BY debut ASC; 