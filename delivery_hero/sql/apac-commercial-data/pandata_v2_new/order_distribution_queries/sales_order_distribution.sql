/*
Author: Abbhinaya Pragasam
*/

WITH month AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 7 MONTH), MONTH) AS month_start
),
/*Getting first backend activated date at vendor level*/
/*
first_vendor_activation_date AS (
  SELECT
    vendor_flows.global_entity_id,
    pd_vendors.vendor_code,
    MIN(DATE(vendor_flows.start_date)) AS first_vendor_activation_date_local
  FROM `fulfillment-dwh-production.dl_pandora.ml_be_vendorflows` AS vendor_flows
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
         ON vendor_flows.global_entity_id = pd_vendors.global_entity_id
        AND vendor_flows.vendor_id = pd_vendors.vendor_id
  WHERE vendor_flows.type = 'ACTIVE'
    AND DATE(vendor_flows.created_at) <= CURRENT_DATE()
    AND vendor_flows.value = '1'
  GROUP BY
    vendor_flows.global_entity_id,
    pd_vendors.vendor_code
),
*/
sf_opportunities_won AS (
/*Getting latest activated date based on salesforce onboarding*/
  SELECT
    global_entity_id,
    sf_account_id,
    business_type,
    close_date_local,
    ROW_NUMBER() over (PARTITION BY sf_account_id ORDER BY close_date_local DESC) = 1 AS is_latest_new_vendor_opportunity
  FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities`
  WHERE business_type IN ('New Business', 'Owner Change', 'Win Back', 'Legal Form Change')
    AND is_closed
    AND LOWER(type) LIKE '%contract%'
    AND LOWER(stage_name) LIKE '%closed won%'
  GROUP BY
    global_entity_id,
    sf_account_id,
    business_type,
    close_date_local
),

sf_activation_date AS (
  SELECT
    sf_accounts.global_entity_id,
    sf_accounts.vendor_code,
    sf_accounts.vendor_grade,
    DATE(sf_opportunities_won.close_date_local) AS activated_date_local
  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
  LEFT JOIN sf_opportunities_won
         ON sf_opportunities_won.global_entity_id = sf_accounts.global_entity_id
        AND sf_accounts.id = sf_opportunities_won.sf_account_id
        AND is_latest_new_vendor_opportunity
  WHERE NOT sf_accounts.is_marked_for_testing_training
),

vendor_list_all AS (
/*Getting full vendor list excluding test & deleted vendors*/
  SELECT
    shared_countries.name AS country_name,
    --countries.company_name,
    vendors.global_entity_id,
    vendors.vendor_code,
    vendors.name AS vendor_name,
    vendors.chain_code,
    vendors.chain_name,
    vendor_grade,
    vendor_gmv_class.gmv_class,
    DATE(COALESCE(sf_activation_date.activated_date_local, /*activation.first_vendor_activation_date_local,*/ DATE(vendors.activated_at_local))) AS activation_date_local
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = vendors.global_entity_id
  /*
  LEFT JOIN first_vendor_activation_date AS activation
         ON activation.global_entity_id = vendors.global_entity_id
        AND activation.vendor_code = vendors.vendor_code
  */
  LEFT JOIN sf_activation_date
         ON sf_activation_date.global_entity_id = vendors.global_entity_id
        AND sf_activation_date.vendor_code = vendors.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = vendors.vendor_code 
        AND vendor_gmv_class.global_entity_id = vendors.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS business_types
         ON vendors.global_entity_id = business_types.global_entity_id
        AND vendors.vendor_code = business_types.vendor_code
  WHERE NOT vendors.is_test
    AND (vendors.global_entity_id LIKE 'FP_%' AND vendors.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE'))
    --AND DATE(COALESCE(sf_activation_date.activated_date_local, activation.first_vendor_activation_date_local, DATE(vendors.activated_at_local))) <= CURRENT_DATE()
    AND business_types.is_restaurants
),

daily_order_details AS (
  SELECT
    pd_orders.global_entity_id,
    shared_countries.name AS country,
    pd_orders.vendor_code,
    vendor_list_all.* EXCEPT(global_entity_id, vendor_code, country_name),
    DATE(pd_orders.created_date_local) AS date_local,
    FORMAT_DATE("%G-%V",pd_orders.created_date_local) AS week,
    DATE_TRUNC(pd_orders.created_date_local, MONTH) AS month,
    
    /*Valid Orders*/
    COUNT(DISTINCT pd_orders.uuid) AS no_of_valid_orders
    
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = pd_orders.global_entity_id
  LEFT JOIN vendor_list_all
         ON pd_orders.global_entity_id = vendor_list_all.global_entity_id
        AND pd_orders.vendor_code = vendor_list_all.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS business_types
         ON pd_orders.global_entity_id = business_types.global_entity_id
        AND pd_orders.vendor_code = business_types.vendor_code
        
  WHERE DATE(pd_orders.created_date_utc) >= (
      SELECT
        month_start
      FROM month
    )
    AND DATE(pd_orders.created_date_utc) <= CURRENT_DATE
    AND NOT pd_orders.is_test_order
    AND pd_orders.is_gross_order
    AND pd_orders.is_valid_order
    AND business_types.is_restaurants
    AND (pd_orders.global_entity_id LIKE 'FP_%' AND pd_orders.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE'))
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ORDER BY 1, 3
)

SELECT
  daily_order_details.global_entity_id,
  daily_order_details.country,
  CASE 
    WHEN LOWER(daily_order_details.vendor_grade) LIKE "%aaa%" 
    THEN 'AAA'
    ELSE 'Non-AAA'
  END AS aaa_type,
  daily_order_details.month,
  DATE_TRUNC(daily_order_details.activation_date_local, MONTH) AS activation_month,
  total_orders.total_ka_split_valid_orders,
  SUM(no_of_valid_orders) AS total_valid_orders,
  COUNT(DISTINCT vendor_code) AS no_of_new_vendors
FROM daily_order_details
LEFT JOIN (
  SELECT
    daily_order_details.global_entity_id,
    daily_order_details.country,
    CASE 
      WHEN LOWER(daily_order_details.vendor_grade) LIKE "%aaa%" 
      THEN 'AAA'
      ELSE 'Non-AAA'
    END AS aaa_type,
    daily_order_details.month,
    SUM(no_of_valid_orders) AS total_ka_split_valid_orders,
  FROM daily_order_details
  GROUP BY 1,2,3,4
) AS total_orders
ON daily_order_details.global_entity_id = total_orders.global_entity_id
AND daily_order_details.country = total_orders.country
AND CASE 
      WHEN LOWER(daily_order_details.vendor_grade) LIKE "%aaa%" 
      THEN 'AAA'
      ELSE 'Non-AAA'
    END = total_orders.aaa_type
    AND daily_order_details.month = total_orders.month
WHERE DATE(daily_order_details.date_local) >= DATE(daily_order_details.activation_date_local)
  AND DATE(daily_order_details.activation_date_local) >= (
      SELECT
        month_start
      FROM month
    )
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5
