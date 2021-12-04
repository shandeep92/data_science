/*
Author: Abbhinaya Pragasam
*/

WITH month AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 7 MONTH), MONTH) AS month_start
),

renego_vendors AS (
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
    LEAD(
      c.start_date_local) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_last_contract_start_date,
    LEAD(
      CASE WHEN tier.sf_contract_id IS NOT NULL THEN TRUE ELSE FALSE END) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_last_contract_tier,
    LAG(
      c.start_date_local) over (PARTITION BY c.sf_account_id ORDER BY DATE(c.start_date_local) DESC) as is_next_contract_start_date
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
    AND DATE(c.start_date_local) <= CURRENT_DATE()
		AND (LOWER(o.business_type) LIKE '%upsell%' OR LOWER(c.name) LIKE '%commission%')
		AND NOT o.is_deleted
		AND (end_date_local IS NULL
		OR (DATE(start_date_local) < DATE(end_date_local)))
    GROUP BY 1, 2, 3,4,5,6,7,8,9,10,11,12,13
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
FROM renego_vendors
WHERE is_last_contract_commission_percentage IS NOT NULL
  AND NOT is_tier_contract
  AND NOT is_last_contract_tier
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
HAVING type_of_commission_change NOT LIKE '%no change%'
ORDER BY global_entity_id, vendor_code
),

renegotiated_vendors AS (
	SELECT
		DISTINCT com.global_entity_id ,
    com.month,
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
		com.vendor_code,
    com.sf_account_id,
    com.commission_percentage,
    com.is_last_contract_commission_percentage AS commission_percentage_last,
		1 AS is_renego,
    com.date_diff
	FROM contract_details_filtered com
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v
         ON v.global_entity_id = com.global_entity_id
        AND v.vendor_code = com.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = v.uuid
  WHERE (date_diff IS NULL OR date_diff > 9)
  AND type_of_commission_change NOT LIKE '%downsell%'
  ORDER BY vendor_code
),

current_sf_contracts AS (
  SELECT 
    c.* EXCEPT(is_last_month_contract),
    v.vendor_code
  FROM
  (
    SELECT *, 
    ROW_NUMBER() over (PARTITION BY sf_account_id ORDER BY status, current_contract_start_date DESC) = 1 as is_last_month_contract,
    FROM (
      SELECT
        DISTINCT sf_contracts.sf_account_id,
        global_entity_id,
        status,
        DATE(sf_contracts.start_date_local) AS current_contract_start_date,
        DATE(sf_contracts.end_date_local) AS current_contract_end_date,
        COALESCE(CAST(MAX(commission_percentage_tier)/100 AS FLOAT64), CAST(MAX(commission_percentage)/100 AS FLOAT64)) AS commission_percentage
      FROM `fulfillment-dwh-production.pandata_curated.sf_contracts` AS sf_contracts
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
      ) tier ON tier.sf_contract_id = sf_contracts.id
      WHERE DATE(sf_contracts.start_date_local) <= CURRENT_DATE
        AND service_type NOT IN ('Online Payment Fee')
        AND sf_contracts.status NOT IN ('Draft')
      GROUP BY 1, 2, 3, 4, 5
    )
  ) c
	LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS v 
         ON v.id = c.sf_account_id
  WHERE is_last_month_contract
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
WHERE (pd_vendors.global_entity_id LIKE 'FP_%' AND pd_vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG', 'FP_DE'))
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
    vendor_raw_data.activation_date,
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
      WHEN LOWER(sf_accounts_custom.vendor_grade) LIKE "%aaa%" 
      THEN 'AAA'
      ELSE 'Non-AAA'
    END AS aaa_type,
    CASE 
      WHEN vendor_gmv_class.gmv_class IS NULL 
      THEN NULL
      ELSE vendor_gmv_class.gmv_class
    END AS gmv_class,
    vendors.chain_code
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors 
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` sf_accounts_custom
         ON sf_accounts_custom.vendor_code = vendors.vendor_code 
        AND sf_accounts_custom.global_entity_id = vendors.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = vendors.vendor_code 
        AND vendor_gmv_class.global_entity_id = vendors.global_entity_id
  LEFT JOIN vendor_activation_date vendor_raw_data
         ON vendor_raw_data.vendor_code = vendors.vendor_code 
        AND vendor_raw_data.global_entity_id = vendors.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = vendors.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = vendors.global_entity_id
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
    DATE(fct_orders.created_at_local) AS date_local,
    FORMAT_DATE("%G-%V",fct_orders.created_at_local) AS week,
    DATE_TRUNC(fct_orders.created_at_local, MONTH) AS month,
    fct_orders.expedition_type,
    
    /*Valid Orders*/
    COUNT(DISTINCT(fct_orders.id)) AS daily_all_valid_orders,
    
    /*Valid All VF Orders*/
    COUNT(DISTINCT IF(((pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100)),
                     fct_orders.id, NULL
                     )
         ) AS daily_all_vf_orders,
     
    /*GMV (All Orders)*/
    SUM(IF(fct_orders.is_valid_order, pd_orders_agg_accounting.gmv_eur, 0)) AS daily_all_gmv_eur,
     
    /*GFV (All Orders)*/
    SUM(IF(fct_orders.is_valid_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_all_gfv_eur, 
    
    /*GFV - VF Deals*/
    SUM(IF(((pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100)),
                     pd_orders_agg_accounting.gfv_eur, 0
                     )
                     ) AS daily_vf_gfv_eur,
     
    /*GFV - Only Discount & Amount*/
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND do.discount.discount_type IN ('amount','percentage'),
                     pd_orders_agg_accounting.gfv_eur, 0) ) AS daily_vf_discount_amount_gfv_eur,
    
    /*VF Disount Value - Only Discount & Amount*/
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND do.discount.discount_type IN ('amount','percentage'), do.discount.vendor_subsidized_value_eur, 0) ) AS daily_vf_discount_amount_deal_eur,
                      
    /*VF Deal Value - VF Deals*/
    COALESCE(
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100, do.discount.vendor_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_vf_deal_value_eur,
    
    /*fp funded Value - VF Deals*/
    COALESCE(
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_fp_funded_value_eur,
    
    /*Discounted GFV - VF Deals*/
    COALESCE(
    SUM(IF(((pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND do.discount.discount_type != 'free-delivery', do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.voucher.type != 'delivery_fee', pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_discount_gfv_eur,
    
    /*Commission Revenue*/
    SUM(IF(fct_orders.is_valid_order, COALESCE(order_commissions.invoiced_commission_eur, order_commissions.estimated_commission_eur), 0)) AS daily_commission_revenue_eur,
    
    /*Commissionable Base*/
    SUM(IF(fct_orders.is_valid_order, COALESCE(order_commissions.invoiced_commission_base_eur, order_commissions.estimated_commission_base_eur), 0)) AS daily_commission_base_eur,
    
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS fct_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` pd_orders_agg_accounting
         ON pd_orders_agg_accounting.uuid = fct_orders.uuid
        AND pd_orders_agg_accounting.created_date_utc <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_report.order_commissions` AS order_commissions
         ON fct_orders.global_entity_id = order_commissions.global_entity_id
        AND fct_orders.code = order_commissions.order_code
        AND order_commissions.order_created_date_utc <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` AS pandora_pd_orders_agg_jkr_deals
         ON fct_orders.global_entity_id = pandora_pd_orders_agg_jkr_deals.global_entity_id
        AND fct_orders.uuid = pandora_pd_orders_agg_jkr_deals.uuid
        AND pandora_pd_orders_agg_jkr_deals.created_date_local <= CURRENT_DATE
  LEFT JOIN vendor_base_info
         ON fct_orders.global_entity_id = vendor_base_info.global_entity_id
        AND fct_orders.vendor_code = vendor_base_info.vendor_code
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts do
         on do.global_entity_id = fct_orders.global_entity_id
        AND do.uuid = fct_orders.uuid
        AND do.created_date_utc <= CURRENT_DATE
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers pd_orders_agg_vouchers
         ON pd_orders_agg_vouchers.uuid = fct_orders.uuid
        AND pd_orders_agg_vouchers.created_date_utc <= CURRENT_DATE
        
  WHERE fct_orders.created_date_local >= (
      SELECT
        month_start
      FROM month
    )
    AND fct_orders.created_date_utc <= CURRENT_DATE
    AND NOT fct_orders.is_test_order
    AND fct_orders.is_gross_order
    AND fct_orders.is_valid_order
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

monthly_order_merge AS (
  SELECT
    global_entity_id,
    country,
    business_type,
    vendor_code,
    activation_date,
    aaa_type,
    gmv_class,
    chain_code,
    expedition_type,
    month,
    SUM(IFNULL(daily_all_valid_orders,0)) AS total_valid_orders,
    SUM(IFNULL(daily_commission_revenue_eur,0)) AS total_commission_revenue_eur,
    SUM(IFNULL(daily_commission_base_eur,0)) AS total_commission_base_eur,
    /*Weighted Commission %*/
    SAFE_DIVIDE(
      SUM(IFNULL(daily_commission_revenue_eur,0)),
      SUM(IFNULL(daily_commission_base_eur,0))
    )*100 AS weighted_commission_perc,
    SUM(IFNULL(daily_all_gfv_eur,0)) AS total_gfv_eur
  FROM daily_order_details
  WHERE business_type = 'restaurants'
  GROUP BY global_entity_id, country, business_type, vendor_code, activation_date, aaa_type, gmv_class, chain_code, expedition_type, month
  HAVING weighted_commission_perc <= 48
  AND weighted_commission_perc >=0
  ORDER BY weighted_commission_perc DESC
)

SELECT
  CASE
    WHEN monthly_order_merge.weighted_commission_perc >= 0 AND monthly_order_merge.weighted_commission_perc <= 4
    THEN '0% <= x <= 4%'
    WHEN monthly_order_merge.weighted_commission_perc > 4 AND monthly_order_merge.weighted_commission_perc <= 8
    THEN '4% < x <= 8%'
    WHEN monthly_order_merge.weighted_commission_perc > 8 AND monthly_order_merge.weighted_commission_perc <= 12
    THEN '8% < x <= 12%'
    WHEN monthly_order_merge.weighted_commission_perc > 12 AND monthly_order_merge.weighted_commission_perc <= 16
    THEN '12% < x <= 16%'
    WHEN monthly_order_merge.weighted_commission_perc > 16 AND monthly_order_merge.weighted_commission_perc <= 20
    THEN '16% < x <= 20%'
    WHEN monthly_order_merge.weighted_commission_perc > 20 AND monthly_order_merge.weighted_commission_perc <= 24
    THEN '20% < x <= 24%'
    WHEN monthly_order_merge.weighted_commission_perc > 24 AND monthly_order_merge.weighted_commission_perc <= 28
    THEN '24% < x <= 28%'
    WHEN monthly_order_merge.weighted_commission_perc > 28 AND monthly_order_merge.weighted_commission_perc <= 32
    THEN '28% < x <= 32%'
    WHEN monthly_order_merge.weighted_commission_perc > 32 AND monthly_order_merge.weighted_commission_perc <= 36
    THEN '32% < x <= 36%'
    WHEN monthly_order_merge.weighted_commission_perc > 36 AND monthly_order_merge.weighted_commission_perc <= 40
    THEN '36% < x <= 40%'
    WHEN monthly_order_merge.weighted_commission_perc > 40 AND monthly_order_merge.weighted_commission_perc <= 44
    THEN '40% < x <= 44%'
    WHEN monthly_order_merge.weighted_commission_perc > 44 AND monthly_order_merge.weighted_commission_perc <= 48
    THEN '44% < x <= 48%'
  END AS weighted_commission_grouping,
  monthly_order_merge.*,
  renegotiated_vendors.* EXCEPT(global_entity_id, month, business_type, vendor_code, sf_account_id, date_diff, commission_percentage, is_renego),
  current_sf_contracts.commission_percentage,
  CASE
    WHEN is_renego = 1
    THEN 'Renegotiated Vendors'
    ELSE 'Not Renegotiation'
  END AS renego_vendors
FROM monthly_order_merge
LEFT JOIN renegotiated_vendors
       ON monthly_order_merge.global_entity_id = renegotiated_vendors.global_entity_id
      AND monthly_order_merge.vendor_code = renegotiated_vendors.vendor_code
      AND monthly_order_merge.month = renegotiated_vendors.month
LEFT JOIN current_sf_contracts
       ON monthly_order_merge.global_entity_id = current_sf_contracts.global_entity_id
      AND monthly_order_merge.vendor_code = current_sf_contracts.vendor_code
