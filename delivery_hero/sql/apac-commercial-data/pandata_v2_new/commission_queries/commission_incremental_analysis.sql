/*
Author: Abbhinaya Pragasam
*/

WITH month AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 8 MONTH), MONTH) AS month_start
),

commission_details AS (  
  SELECT
		v.global_entity_id,
		v.country_name,
    DATE_TRUNC(DATE(c.start_date_local), MONTH) AS month,
		c.sf_account_id,
		v.vendor_code,
		c.sf_opportunity_id,
		c.id,
    tier.sf_contract_id,
		c.service_type,
		c.status,
		--c.status_code,
		c.start_date_local,
		c.end_date_local,
    CASE WHEN tier.sf_contract_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_tier_contract,
    COALESCE(CAST(MAX(commission_percentage_tier)/100 AS FLOAT64), CAST(MAX(c.commission_percentage)/100 AS FLOAT64)) AS commission_percentage,
    DATE_DIFF(DATE(end_date_local), DATE(c.start_date_local), DAY) AS date_diff,
    LEAD(
      COALESCE(CAST(MAX(commission_percentage_tier)/100 AS FLOAT64), CAST(MAX(c.commission_percentage)/100 AS FLOAT64))) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_last_contract_commission_percentage,
    LEAD(c.start_date_local) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_last_contract_start_date,
    LEAD(
      CASE WHEN tier.sf_contract_id IS NOT NULL THEN TRUE ELSE FALSE END) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_last_contract_tier,
    LAG(c.start_date_local) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_next_contract_start_date
	FROM fulfillment-dwh-production.pandata_curated.sf_contracts c
  LEFT JOIN (
      SELECT
        DISTINCT sf_tiers.sf_contract_id,
        to_join.min_condition,
        COALESCE(MAX(sf_tiers.commission_percentage), MAX(sf_tiers.commission_per_order_local)) AS commission_percentage_tier
      FROM fulfillment-dwh-production.pandata_curated.sf_tiers
      LEFT JOIN (
        SELECT
          DISTINCT sf_contract_id,
          MIN(min_amount_local) AS min_condition
        FROM fulfillment-dwh-production.pandata_curated.sf_tiers
        GROUP BY 1
      ) to_join ON to_join.sf_contract_id = sf_tiers.sf_contract_id
               AND to_join.min_condition = sf_tiers.min_amount_local
      WHERE to_join.min_condition IS NOT NULL
      GROUP BY 1,2
    ) tier ON tier.sf_contract_id = c.id
	LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` v 
         ON v.id = c.sf_account_id
	LEFT JOIN fulfillment-dwh-production.pandata_curated.sf_users u 
         ON u.id = v.sf_created_by_user_id
	LEFT JOIN fulfillment-dwh-production.pandata_curated.sf_opportunities o 
         ON c.sf_opportunity_id = o.id
	WHERE NOT c.is_deleted
		AND service_type IN ('Commission Fee', 'Logistics Fee')
		AND (LOWER(u.full_name) NOT LIKE '%feng wang%'
		AND LOWER(u.full_name) NOT LIKE '%darryl chua%'
		AND LOWER(u.full_name) NOT LIKE '%test user%')
    AND (c.status IS NULL OR c.status NOT LIKE '%Terminated%')
		AND NOT v.is_deleted
		AND v.type IN ('Branch - Main', 'Branch-Main', 'Branch - Virtual Restaurant', 'Branch-Vitrual')
		AND NOT v.is_marked_for_testing_training
		AND v.global_entity_id LIKE 'FP_%'
    AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
		AND DATE(c.start_date_local) >= (
      SELECT
        month_start
      FROM month
    )
    AND DATE(c.start_date_local) <= CURRENT_DATE
		AND (LOWER(o.business_type) LIKE '%upsell%' OR LOWER(c.name) LIKE '%commission%')
		AND NOT o.is_deleted
		AND (end_date_local IS NULL OR (DATE(start_date_local) < DATE(end_date_local)))
    --AND v.vendor_code = 'w0dk'
    --AND v.global_entity_id = 15
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),

contract_details_filtered AS (
SELECT
  global_entity_id,
  country_name AS country,
  month,
  vendor_code,
  sf_account_id,
  service_type,
  start_date_local,
  end_date_local AS end_date,
  commission_percentage,
  date_diff,
  is_last_contract_commission_percentage,
  is_last_contract_start_date,
  CASE
    WHEN is_last_contract_commission_percentage IS NOT NULL AND (commission_percentage - is_last_contract_commission_percentage) > 0
    THEN 'upsell'
    WHEN is_last_contract_commission_percentage IS NOT NULL AND (commission_percentage - is_last_contract_commission_percentage) < 0
    THEN 'downsell'
    WHEN is_last_contract_commission_percentage IS NOT NULL AND (commission_percentage - is_last_contract_commission_percentage) = 0
    THEN 'no change'
  END AS type_of_commission_change,
  is_next_contract_start_date,
  ROUND(SAFE_SUBTRACT(commission_percentage, is_last_contract_commission_percentage), 5) AS commission_percentage_increment_change
FROM commission_details
WHERE is_last_contract_commission_percentage IS NOT NULL
  AND NOT is_tier_contract
  AND NOT is_last_contract_tier
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
HAVING type_of_commission_change NOT LIKE '%no change%'
ORDER BY global_entity_id, vendor_code
),

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
    DATE(sf_opportunities_won.close_date_local) AS activated_date_local
  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
  LEFT JOIN sf_opportunities_won
         ON sf_opportunities_won.global_entity_id = sf_accounts.global_entity_id
        AND sf_accounts.id = sf_opportunities_won.sf_account_id
        AND is_latest_new_vendor_opportunity
  WHERE NOT sf_accounts.is_marked_for_testing_training
),

vendor_activation_date AS (
SELECT
  pd_vendors.global_entity_id,
  pd_vendors.vendor_code,
  COALESCE(sf_activation_date.activated_date_local, DATE(pd_vendors.activated_at_local), first_valid_order) AS activation_date
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
LEFT JOIN sf_activation_date
       ON sf_activation_date.global_entity_id = pd_vendors.global_entity_id
      AND sf_activation_date.vendor_code = pd_vendors.vendor_code
LEFT JOIN (
  SELECT
    global_entity_id,
    vendor_code,
    MIN(created_date_local) AS first_valid_order
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  WHERE created_date_utc <= CURRENT_DATE
  AND is_valid_order
  AND is_gross_order
  AND NOT is_test_order
  GROUP BY 1,2
) orders
   ON orders.global_entity_id = pd_vendors.global_entity_id
  AND orders.vendor_code = pd_vendors.vendor_code
WHERE (pd_vendors.global_entity_id LIKE 'FP_%'
  --AND pd_vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG')
  )
  AND NOT is_test
GROUP BY
  pd_vendors.global_entity_id,
  pd_vendors.vendor_code,
  sf_activation_date.activated_date_local,
  pd_vendors.activated_at_local,
  first_valid_order
),

vendor_base_info AS (
SELECT 
    vendors.global_entity_id,
    shared_countries.name AS country,
    vendors.vendor_code,
    vendor_activation_date.activation_date,
    CASE
      WHEN pd_vendors_agg_business_types.business_type_apac IN ('restaurants') AND NOT pd_vendors_agg_business_types.is_home_based_kitchen
      THEN 'restaurants'
      WHEN pd_vendors_agg_business_types.is_home_based_kitchen
      THEN 'restaurants - home based kitchens'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'kitchens'
      THEN 'kitchen'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'concepts'
      THEN 'concepts'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'dmart'
      THEN 'PandaNow'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'shops'
      THEN 'PandaMart'
      ELSE 'restaurants'
    END AS business_type,
    CASE 
      WHEN LOWER(sf_accounts.vendor_grade) LIKE "%aaa%" 
      THEN 'AAA'
      ELSE 'Non-AAA'
    END AS aaa_type,
    CASE 
      WHEN vendor_gmv_class.gmv_class IS NULL 
      THEN 'NEW'
      ELSE vendor_gmv_class.gmv_class
    END AS gmv_class,
    vendors.chain_code
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` vendors
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = vendors.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
         ON sf_accounts.vendor_code = vendors.vendor_code 
        AND sf_accounts.global_entity_id = vendors.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = vendors.vendor_code 
        AND vendors.global_entity_id = vendor_gmv_class.global_entity_id
  LEFT JOIN vendor_activation_date
         ON vendor_activation_date.vendor_code = vendors.vendor_code 
        AND vendor_activation_date.global_entity_id = vendors.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = vendors.uuid
  WHERE NOT vendors.is_test
    AND vendors.global_entity_id LIKE 'FP_%'
    AND vendors.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
),

daily_order_details AS (
  SELECT
    fct_orders.global_entity_id,
    fct_orders.country_name AS country,
    fct_orders.vendor_code,
    vendor_base_info.* EXCEPT(global_entity_id, country, vendor_code),
    DATE_TRUNC(DATE(contract_details_filtered.start_date_local), MONTH) AS renego_month,
    DATE(fct_orders.created_date_local) AS date_local,
    FORMAT_DATE("%G-%V",fct_orders.created_date_local) AS week,
    DATE_TRUNC(fct_orders.created_date_local, MONTH) AS month,
    
    contract_details_filtered.commission_percentage,
    contract_details_filtered.date_diff,
    contract_details_filtered.is_last_contract_commission_percentage,
    contract_details_filtered.type_of_commission_change,
    contract_details_filtered.commission_percentage_increment_change,
    /*Commission Revenue*/
    SUM(IF(fct_orders.is_valid_order, COALESCE(order_commissions.invoiced_commission_eur, order_commissions.estimated_commission_eur), 0)) AS daily_commission_revenue_eur,
    
    /*Commissionable Base*/
    SUM(IF(fct_orders.is_valid_order, COALESCE(order_commissions.invoiced_commission_base_eur, order_commissions.estimated_commission_base_eur), 0)) AS daily_commission_base_eur,
    
    SAFE_MULTIPLY(
      SUM(IF(fct_orders.is_valid_order, COALESCE(order_commissions.invoiced_commission_eur, order_commissions.estimated_commission_eur), 0)),
      SAFE_DIVIDE(
        contract_details_filtered.commission_percentage_increment_change,
        contract_details_filtered.is_last_contract_commission_percentage
      )
    ) AS commission_increment
    
  FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` AS fct_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_report.order_commissions` AS order_commissions
         ON fct_orders.global_entity_id = order_commissions.global_entity_id
        AND fct_orders.code = order_commissions.order_code
        AND order_commissions.order_created_date_utc <= CURRENT_DATE
  INNER JOIN vendor_base_info
         ON fct_orders.global_entity_id = vendor_base_info.global_entity_id
        AND fct_orders.vendor_code = vendor_base_info.vendor_code
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders
         ON fct_orders.global_entity_id = pd_orders.global_entity_id
        AND fct_orders.uuid = pd_orders.uuid
        AND pd_orders.created_date_utc <= CURRENT_DATE
        
  LEFT JOIN contract_details_filtered
         ON contract_details_filtered.global_entity_id = fct_orders.global_entity_id
        AND contract_details_filtered.vendor_code = fct_orders.vendor_code
        AND DATE(fct_orders.created_date_local) >= DATE(contract_details_filtered.start_date_local)
        AND (DATE(fct_orders.created_date_local) < DATE(contract_details_filtered.is_next_contract_start_date)
            OR DATE(contract_details_filtered.is_next_contract_start_date) IS NULL)
        
  WHERE fct_orders.created_date_local >= (
      SELECT
        month_start
      FROM month
    )
    AND fct_orders.created_date_local <= CURRENT_DATE
    AND NOT fct_orders.is_test_order
    AND fct_orders.is_gross_order
    AND fct_orders.is_valid_order
    AND vendor_base_info.business_type = 'restaurants'
    AND pd_orders.is_own_delivery
    AND fct_orders.expedition_type = 'delivery'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  HAVING type_of_commission_change IS NOT NULL
  ORDER BY 1, 3, 9
)

SELECT
  global_entity_id,
  country,
  business_type,
  aaa_type,
  month,
  renego_month,
  type_of_commission_change,
  SUM(commission_increment) AS total_commission_increment,
  COUNT(DISTINCT vendor_code) AS no_of_renego_vendors
FROM daily_order_details
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3,4,5
