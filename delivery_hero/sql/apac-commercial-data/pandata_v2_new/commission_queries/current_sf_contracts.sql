/*
Author: Abbhinaya Pragasam
*/

SELECT * EXCEPT(is_latest)
  FROM
  (
    SELECT *, 
    ROW_NUMBER() over (PARTITION BY sf_account_id ORDER BY status, current_contract_start_date DESC) = 1 as is_latest,
    FROM (
      SELECT
        sf_contracts.sf_account_id,
        global_entity_id,
        status,
  /*      IF(sf_contracts.commission_type = 'Percentage',
          sf_contracts.commission_c,
          NULL
        ) AS commission_percentage, */
        DATE(sf_contracts.start_date_local) AS current_contract_start_date,
        DATE(sf_contracts.end_date_local) AS current_contract_end_date,
      FROM fulfillment-dwh-production.pandata_curated.sf_contracts
      WHERE DATE(sf_contracts.start_date_local) <= current_date()
        AND (DATE(sf_contracts.end_date_local) >= current_date() OR sf_contracts.end_date_local IS NULL)
        AND sf_contracts.commission_type = 'Percentage'
        AND sf_contracts.status != 'Draft'
    )
  )
  WHERE is_latest
