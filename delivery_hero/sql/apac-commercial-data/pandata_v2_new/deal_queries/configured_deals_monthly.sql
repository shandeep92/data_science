/*
Author: Abbhinaya Pragasam
*/

WITH month_period AS (
  SELECT
    month
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH), DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH), INTERVAL 1 MONTH)) as month
  GROUP BY 1
),

business_type AS (
  SELECT
    DISTINCT 
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
      THEN 'PandaMart'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'shops'
      THEN 'shops'
      ELSE 'restaurants'
    END AS business_type
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` pd_vendors_agg_business_types
  GROUP BY 1
),

vendor_base_info AS (
SELECT 
    v.global_entity_id,
    shared_countries.name AS country,
    v.vendor_code,
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
      THEN 'PandaMart'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'shops'
      THEN 'shops'
      ELSE 'restaurants'
    END AS business_type,
    sf_accounts.vendor_grade AS aaa_type,
    v.chain_code,
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
    AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE', 'FP_SK')
    --AND ((v.global_entity_id != 'FP_PK' AND pd_vendors_agg_business_types.business_type_apac IN ('restaurants'))
     --   OR (v.global_entity_id = 'FP_PK' AND pd_vendors_agg_business_types.is_restaurants AND NOT pd_vendors_agg_business_types.is_home_based_kitchen))
    AND pd_vendors_agg_business_types.business_type_apac IN ('restaurants')
    AND NOT pd_vendors_agg_business_types.is_home_based_kitchen
  GROUP BY 1,2,3,4,5,6
),

voucher_deals_data AS (
  SELECT
    pd_vouchers.global_entity_id,
    dates.date,
    vendor_base_info.business_type,
    vendor_base_info.vendor_code AS vendor_code,
    pd_vouchers.type AS deal_type,
    pd_vouchers.value AS amount_local,
    pd_vouchers.minimum_order_value_local AS MOV,
    pd_vouchers.foodpanda_ratio AS foodpanda_ratio,
    pd_vouchers.purpose AS deal_title,
    pd_vouchers.description,
    'Full Menu' AS condition_type,
    (CASE
      WHEN expedition_types LIKE '%delivery%' AND expedition_types LIKE '%pickup%' 
      THEN 'all'
      WHEN expedition_types IS NULL 
      THEN 'all'
      ELSE expedition_types
    END) AS expedition_type,
    DATE(pd_vouchers.start_date_local) AS deal_start,
    DATE(pd_vouchers.stop_at_local) AS deal_end,
    'is_voucher_deal' AS deal_segment,
    ROW_NUMBER() OVER (PARTITION BY pd_vouchers.global_entity_id, vendor_code, dates.date ORDER BY pd_vouchers.created_at_utc DESC) = 1 AS is_latest_entry
  FROM fulfillment-dwh-production.pandata_curated.pd_vouchers,
  UNNEST(pd_vendor_uuids) AS pd_vendor_uuids,
  UNNEST(pd_vouchers.expedition_types) AS expedition_types
  INNER JOIN vendor_base_info
          ON pd_vouchers.global_entity_id = vendor_base_info.global_entity_id
         AND SPLIT(pd_vendor_uuids, '_')[OFFSET(0)] = vendor_base_info.vendor_code
  CROSS JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
  WHERE pd_vouchers.start_date_local IS NOT NULL
    AND NOT pd_vouchers.is_deleted
    AND foodpanda_ratio < 100
    AND dates.date BETWEEN DATE(pd_vouchers.start_date_local) AND DATE(pd_vouchers.stop_at_local)
    AND dates.date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
    AND dates.date < DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 7 MONTH)
    AND DATE(pd_vouchers.start_date_local) <= dates.date
    AND DATE(pd_vouchers.stop_at_local) >= dates.date
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,pd_vouchers.created_at_utc
  HAVING vendor_code IS NOT NULL
),

voucher_deals as (
  SELECT
    *
  FROM voucher_deals_data
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12, 13, 14, 15, 16 
),

voucher_config AS (
  SELECT
    COALESCE(shared_countries.global_entity_id, voucher_deals.global_entity_id) AS global_entity_id,
    COALESCE(
      FORMAT_DATE("%Y-%m (%B)",mp.month),
      FORMAT_DATE("%Y-%m (%B)",voucher_deals.date)
    ) AS month,
    voucher_deals.date,
    COALESCE(
      business_type.business_type,
      voucher_deals.business_type
    ) AS business_type,
    voucher_deals.deal_type,
    voucher_deals.amount_local AS amount_local,
    voucher_deals.MOV AS MOV,
    voucher_deals.foodpanda_ratio AS foodpanda_ratio,
    voucher_deals.vendor_code,
    voucher_deals.deal_title,
    voucher_deals.description,
    voucher_deals.condition_type,
    voucher_deals.expedition_type,
    voucher_deals.deal_start AS deal_start,
    voucher_deals.deal_end AS deal_end,
    COALESCE(
      voucher_deals.deal_segment,
      'is_voucher_deal'
    ) AS deal_segment
  FROM `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
  CROSS JOIN month_period mp
  CROSS JOIN business_type
  FULL JOIN voucher_deals
         ON shared_countries.global_entity_id = voucher_deals.global_entity_id
        AND FORMAT_DATE("%Y-%m (%B)",mp.month) = FORMAT_DATE("%Y-%m (%B)",voucher_deals.date)
        AND business_type.business_type = voucher_deals.business_type
  INNER JOIN vendor_base_info
          ON shared_countries.global_entity_id = vendor_base_info.global_entity_id
         AND voucher_deals.vendor_code = vendor_base_info.vendor_code
  WHERE is_latest_entry
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ORDER BY 1,7
),

discount_deal_data as (
  SELECT
    pd_vendors.global_entity_id,
    dates.date,
    vendor_base_info.business_type,
    vendor_discounts.discount_type AS deal_type,
    vendor_discounts.amount_local,
    vendor_discounts.minimum_order_value_local AS MOV,
    vendor_discounts.foodpanda_ratio,
    pd_vendors.vendor_code,
    vendor_discounts.title AS deal_title,
    vendor_discounts.description,
    vendor_discounts.condition_type,
    (CASE
      WHEN LOWER(expedition_types)  LIKE '%delivery%' AND LOWER(expedition_types) LIKE '%pickup%' 
      THEN 'all'
      ELSE LOWER(expedition_types)
    END) AS expedition_type,
    vendor_discounts.start_date_local AS deal_start,
    vendor_discounts.end_date_local AS deal_end,
    'is_discount_deal' AS deal_segment,
    ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY vendor_discounts.created_at_utc DESC) = 1 AS is_latest_entry
  FROM fulfillment-dwh-production.pandata_curated.pd_vendors,
  UNNEST(pd_vendors.discounts) AS vendor_discounts,
  UNNEST(vendor_discounts.expedition_types) AS expedition_types
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
         ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
  INNER JOIN vendor_base_info
          ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
         AND pd_vendors.vendor_code = vendor_base_info.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
         ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
  WHERE vendor_discounts.start_date_local IS NOT NULL
    AND (
          (pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
          OR pd_vendors.global_entity_id = 'FP_PK'
        )
    AND (NOT vendor_discounts.is_deleted OR NOT pd_discounts.is_deleted)
    AND dates.date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
    AND dates.date < DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 7 MONTH)
    AND vendor_discounts.foodpanda_ratio < 100
    AND vendor_discounts.start_date_local <= dates.date
    AND vendor_discounts.end_date_local >= dates.date
    AND LOWER(vendor_discounts.title) NOT LIKE '%testing%'
    AND LOWER(vendor_discounts.description) NOT LIKE '%test%'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12, 13, 14, 15, vendor_discounts.created_at_utc
),

discount_deal as (
  SELECT
    *
  FROM discount_deal_data
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12, 13, 14, 15, 16 
),

discount_config AS (
  SELECT
    COALESCE(shared_countries.global_entity_id, discount_deal.global_entity_id) AS global_entity_id,
    COALESCE(
      FORMAT_DATE("%Y-%m (%B)",mp.month),
      FORMAT_DATE("%Y-%m (%B)",discount_deal.date)
    ) AS month,
    discount_deal.date,
    COALESCE(
      business_type.business_type,
      discount_deal.business_type
    ) AS business_type,
    discount_deal.deal_type,
    discount_deal.amount_local AS amount_local,
    discount_deal.MOV AS MOV,
    discount_deal.foodpanda_ratio AS foodpanda_ratio,
    discount_deal.vendor_code,
    discount_deal.deal_title,
    discount_deal.description,
    discount_deal.condition_type,
    discount_deal.expedition_type,
    discount_deal.deal_start AS deal_start,
    discount_deal.deal_end AS deal_end,
    COALESCE(
      discount_deal.deal_segment,
      'is_discount_deal'
    ) AS deal_segment
  FROM `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
  CROSS JOIN month_period mp
  CROSS JOIN business_type
  FULL JOIN discount_deal
         ON shared_countries.global_entity_id = discount_deal.global_entity_id
        AND FORMAT_DATE("%Y-%m (%B)",mp.month) = FORMAT_DATE("%Y-%m (%B)",discount_deal.date)
        AND business_type.business_type = discount_deal.business_type
  LEFT JOIN vendor_base_info
         ON shared_countries.global_entity_id = vendor_base_info.global_entity_id
        AND discount_deal.vendor_code = vendor_base_info.vendor_code
  WHERE is_latest_entry
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ORDER BY 1,7
)

SELECT
 *
FROM discount_config
UNION ALL
SELECT
 *
FROM voucher_config
