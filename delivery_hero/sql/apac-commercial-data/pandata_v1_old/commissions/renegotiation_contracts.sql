WITH vendor_base_info AS (
  SELECT 
    v.global_entity_id,
    shared_countries.name AS country,
    v.vendor_code,
    v.name AS vendor_name,
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
    sf_accounts.vendor_grade AS aaa_type,
    v.chain_code,
    has_own_delivery,
    CASE 
      WHEN MAX(vendor_gmv_class.gmv_class) IS NULL 
      THEN NULL
      ELSE MAX(vendor_gmv_class.gmv_class)
    END AS gmv_class
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
         ON sf_accounts.vendor_code = v.vendor_code 
        AND sf_accounts.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = v.vendor_code 
        AND v.global_entity_id = vendor_gmv_class.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = v.uuid
  WHERE NOT v.is_test
    AND v.global_entity_id LIKE 'FP_%'
    AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND ((v.global_entity_id != 'FP_PK' AND pd_vendors_agg_business_types.business_type_apac IN ('restaurants'))
        OR (v.global_entity_id = 'FP_PK' AND pd_vendors_agg_business_types.is_restaurants AND NOT pd_vendors_agg_business_types.is_home_based_kitchen))
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
		v.global_entity_id,
		a.record_country_c,
    FORMAT_DATE("%G-%V",DATE(c.start_date)) AS week,
		c.account_id,
		v.vendor_code,
		c.id_opportunity_c,
		c.id,
		c.service_type_c,
		c.status,
		c.status_code,
		c.start_date,
		c.end_date_c,
    COALESCE(CAST(MAX(commission_percentage_tier)/100 AS FLOAT64), CAST(MAX(c.commission_c)/100 AS FLOAT64)) AS commission_percentage,
    DATE_DIFF(DATE(end_date_c), DATE(c.start_date), DAY) AS date_diff
	FROM salesforce.contract c
  LEFT JOIN (
      SELECT
        DISTINCT tier_c.id_contract_c,
        to_join.min_condition,
        COALESCE(MAX(tier_c.commission_in_percentage_c), MAX(tier_c.commission_per_order_c)) AS commission_percentage_tier
      FROM salesforce.tier_c
      LEFT JOIN (
        SELECT
          DISTINCT id_contract_c,
          MIN(min_c) AS min_condition
        FROM salesforce.tier_c
        GROUP BY 1
      ) to_join ON to_join.id_contract_c = tier_c.id_contract_c
               AND to_join.min_condition = tier_c.min_c
      WHERE to_join.min_condition IS NOT NULL
      GROUP BY 1,2
    ) tier ON tier.id_contract_c = c.id
	LEFT JOIN salesforce.account a 
  ON a.id = c.account_id
	LEFT JOIN salesforce.user u 
  ON u.id = a.created_by_id
	LEFT JOIN salesforce.opportunity o 
  ON c.id_opportunity_c = o.id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
         ON a.id = sf_accounts.id
	INNER JOIN vendor_base_info v 
          ON v.vendor_code = sf_accounts.vendor_code
	WHERE NOT c.is_deleted
		AND service_type_c IN ('Commission Fee', 'Logistics Fee')
		AND (LOWER(u.name) NOT LIKE '%feng wang%'
		AND LOWER(u.name) NOT LIKE '%darryl chua%'
		AND LOWER(u.name) NOT LIKE '%test user%')
		AND NOT a.is_deleted
		AND a.type IN ('Branch - Main', 'Branch-Main', 'Branch - Virtual Restaurant', 'Branch-Vitrual')
		AND NOT a.mark_for_testing_training_c
		AND v.global_entity_id LIKE 'FP_%'
    AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
		AND DATE(c.start_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 5 WEEK), ISOWEEK)
		AND LOWER(o.business_type_c) LIKE '%upsell%'
		AND NOT o.is_deleted
		AND (end_date_c IS NULL
		OR (DATE(start_date) < DATE(end_date_c)))
    GROUP BY 1, 2, 3,4,5,6,7,8,9,10,11,12
