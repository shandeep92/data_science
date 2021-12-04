WITH won_opportunities as (
    select
        distinct
        sf_account_id
    from `fulfillment-dwh-production.pandata_curated.sf_opportunities` AS sf_opportunities
    where stage_name = 'Closed Won'
),
salesforce_contracts AS (
  SELECT DISTINCT
            countries.global_entity_id,
            contract.id AS contract_id,
            contract.sf_account_id,
            /* account.parent_id AS parent_account_id,*/
            contract.contract_number,
            account.vendor_code, 
            account.country_name,
            countries.country_code_iso3 AS country_code,
            contract.status,
            account.vendor_grade,
            account.name AS account_name,
            account.status AS account_status,
            contract.commission_type,
            IFNULL(contract.commission_per_order_local, 0.0) AS commission_per_order,
            commission_percentage,
            account.is_marked_for_testing_training,
            contract.termination_reason,
            /* clean this up: use created_at_local once available. dependecy on brenda's new pd_countries table */
            DATE(contract.created_at_utc) AS created_date_local,
            DATE(contract.activated_at_utc) AS activated_date_local,
            DATE(contract.start_at_utc) AS start_date_local,
            DATE(contract.last_activity_at_utc) AS last_activity_date_local,
            DATE(contract.last_modified_at_utc) AS last_modified_date_local,
            DATE(contract.end_at_utc) AS end_date_local
        FROM `fulfillment-dwh-production.pandata_curated.sf_contracts` AS contract
        JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS account
          ON account.id = contract.sf_account_id
        LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_users` AS user
               ON user.id = contract.sf_owner_id
        LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS countries
               ON countries.name = account.country_name
        WHERE countries.global_entity_id IS NOT NULL        
),
terminated_vendors AS (
    SELECT
        salesforce_contracts.country_name,
         DATE(end_date_local) as date,
        -- DATE_TRUNC(end_date_local, ISOWEEK) AS week,
        pd_vendors.vendor_code,
/**      DATE(IFNULL(end_date_local, last_modified_date_local)) as date, **/
        CASE
            WHEN pd_vendors_agg_business_types.business_type_apac IN ('restaurants', 'home_based_kitchens')
            THEN 'restaurants'
            WHEN pd_vendors_agg_business_types.business_type_apac = 'kitchens'
            THEN 'kitchen'
            WHEN pd_vendors_agg_business_types.business_type_apac = 'concepts'
            THEN 'concepts'
            WHEN pd_vendors_agg_business_types.business_type_apac = 'dmart'
            THEN 'PandaNow'
            WHEN pd_vendors_agg_business_types.business_type_apac = 'shops'
            THEN 'PandaMart'
            ELSE 'restaurants'
        END AS vendor_type,
        COUNT(DISTINCT salesforce_contracts.sf_account_id) AS total_terminated_vendors
    FROM salesforce_contracts
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` pd_vendors
           ON pd_vendors.global_entity_id = salesforce_contracts.global_entity_id
          AND pd_vendors.vendor_code = salesforce_contracts.vendor_code 
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
           ON pd_vendors_agg_business_types.uuid = pd_vendors.uuid
    LEFT JOIN won_opportunities
          /* ON won_opportunities.country_name = salesforce_contracts.country_name */
          ON won_opportunities.sf_account_id = salesforce_contracts.sf_account_id
    WHERE salesforce_contracts.account_status = 'Terminated'
      AND won_opportunities.sf_account_id is not null
      AND NOT termination_reason IN ('Onboarding Failed', 'Duplicate')
      AND NOT salesforce_contracts.is_marked_for_testing_training
    GROUP BY 1, 2, 3,4
)
SELECT * 
FROM terminated_vendors 
ORDER BY date desc
