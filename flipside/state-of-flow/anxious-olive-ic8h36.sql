WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date
),
 cadence as (
  SELECT
    DISTINCT event_contract as contract,
    min(block_timestamp) as debut
  FROM
    flow.core.fact_events
 JOIN time_periods tp
WHERE
        block_timestamp >= tp.start_date
  group by
    1
),
core_new_contracts AS (
  SELECT
    COUNT(DISTINCT contract) AS total_new_cadence_contracts
  FROM
    cadence
),
evms as (
  select
    x.block_timestamp,
    x.from_address as creator,
    y.contract_address as contract
  from
    flow.core_evm.fact_transactions x
    join flow.core_evm.fact_event_logs y on x.tx_hash = y.tx_hash
 JOIN time_periods tp
WHERE
        x.block_timestamp >= tp.start_date
    and
    y.topics [0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
  UNION
  select
    x.block_timestamp,
    x.from_address as creator,
    x.tx_hash as contract
  from
    flow.core_evm.fact_transactions x
JOIN time_periods tp
WHERE
        x.block_timestamp >= tp.start_date
  and
    (
      x.origin_function_signature = '0x60c06040'
      or x.origin_function_signature = '0x60806040'
    )
    and tx_hash not in (
      select
        x.tx_hash
      from
        flow.core_evm.fact_transactions x
        join flow.core_evm.fact_event_logs y on x.tx_hash = y.tx_hash
      where
        y.topics [0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
    )
),
evm_new_contracts AS (
  SELECT
    COUNT(DISTINCT contract) AS total_new_evm_contracts
  FROM
    evms
)
SELECT
  (
    SELECT
      total_new_cadence_contracts
    FROM
      core_new_contracts
  ) AS total_new_cadence_contracts,
  (
    SELECT
      total_new_evm_contracts
    FROM
      evm_new_contracts
  ) AS total_new_evm_contracts

/**

  SELECT DISTINCT
    event_contract as contract
  FROM
    flow.core.fact_events
  GROUP BY
    1

**/
