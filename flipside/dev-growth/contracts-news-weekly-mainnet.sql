WITH core_news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract AS new_contract, creator,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM (
        select x.block_timestamp, x.from_address as creator,y.contract_address as contract 
        from flow.core_evm.fact_transactions x
        join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
        where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        UNION
        select x.block_timestamp, x.from_address as creator, x.tx_hash as contract 
        from flow.core_evm.fact_transactions x
        where (x.origin_function_signature='0x60c06040' or x.origin_function_signature='0x60806040') 
        and tx_hash not in (
            select x.tx_hash 
            from flow.core_evm.fact_transactions x
            join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
            where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        )
    )
    GROUP BY 1,2
),
combined_news AS (
    SELECT new_contract, debut, 'Cadence' as source FROM core_news
    UNION ALL
    SELECT new_contract, debut, 
           CASE WHEN creator LIKE '0x0000000000000000000000020000000000000000%' 
                THEN 'COA EVM Contract' 
                else 'Non-COA EVM Contract' 
           END as source 
    FROM evm_news
)
SELECT
    debut AS date, 
    source,
    COUNT(DISTINCT new_contract) AS new_contracts,
    SUM(COUNT(DISTINCT new_contract)) OVER (partition by source ORDER BY debut) AS unique_contracts
FROM combined_news 
WHERE debut BETWEEN '2025-05-30' AND '2025-06-06'
GROUP BY debut, source
ORDER BY debut ASC;