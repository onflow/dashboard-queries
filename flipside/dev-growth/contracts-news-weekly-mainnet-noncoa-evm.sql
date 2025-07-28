-- Optimized query to reduce execution time
-- Key optimizations:
-- 1. Removed redundant WHERE clause in final SELECT
-- 2. Used DATE_TRUNC instead of trunc for better performance
-- 3. Added early filtering for non-COA creator to reduce data processing
-- 4. Simplified the logic by removing unnecessary DISTINCT in final SELECT
-- 5. Optimized subquery structure for better performance

WITH evm_news AS (
    SELECT 
        contract AS new_contract, 
        creator,
        DATE_TRUNC('week', block_timestamp) AS debut
    FROM (
        -- Contract creation events
        SELECT 
            x.block_timestamp, 
            x.from_address as creator,
            y.contract_address as contract 
        FROM flow.core_evm.fact_transactions x
        JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash 
        WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
            AND x.from_address NOT LIKE '0x0000000000000000000000020000000000000000%'
            AND x.block_timestamp >= DATEADD(year, -1, CURRENT_DATE())
        
        UNION
        
        -- Contract creation without events (fallback)
        SELECT 
            x.block_timestamp, 
            x.from_address as creator, 
            x.tx_hash as contract 
        FROM flow.core_evm.fact_transactions x
        WHERE (x.origin_function_signature = '0x60c06040' OR x.origin_function_signature = '0x60806040')
            AND x.from_address NOT LIKE '0x0000000000000000000000020000000000000000%'
            AND x.block_timestamp >= DATEADD(year, -1, CURRENT_DATE())
            AND x.tx_hash NOT IN (
                SELECT DISTINCT x.tx_hash 
                FROM flow.core_evm.fact_transactions x
                JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash 
                WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
            )
    )
    GROUP BY 1, 2, 3
)
SELECT
    debut AS date, 
    COUNT(new_contract) AS new_contracts
FROM evm_news 
GROUP BY debut
ORDER BY debut ASC; 