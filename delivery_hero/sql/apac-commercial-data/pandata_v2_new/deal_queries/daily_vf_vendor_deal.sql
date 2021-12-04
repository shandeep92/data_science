/*
Author: Abbhinaya Pragasam
*/

WITH start_date AS (
SELECT
DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH) AS start
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
      THEN 'PandaNow'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'shops'
      THEN 'PandaMart'
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
    AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND ((v.global_entity_id != 'FP_PK' AND pd_vendors_agg_business_types.business_type_apac IN ('restaurants'))
        OR (v.global_entity_id = 'FP_PK' AND pd_vendors_agg_business_types.is_restaurants AND NOT pd_vendors_agg_business_types.is_home_based_kitchen))
  GROUP BY 1,2,3,4,5,6
  --HAVING business_type = 'restaurants'
),

dates AS (
  SELECT
    vendors.*,
    date,
    iso_year_week_string,
    weekday_name
  FROM `fulfillment-dwh-production.pandata_curated.shared_dates`
  CROSS JOIN(
    SELECT 
      DISTINCT global_entity_id, 
      vendor_code 
    FROM vendor_base_info
  ) AS vendors
  WHERE date >= (SELECT start FROM start_date)
    AND date <= CURRENT_DATE
  ORDER BY date
),

active_voucher_deal_by_day AS (
WITH voucher_deals AS (
  SELECT
    pd_vouchers.global_entity_id,
    dates.date,
    vendor_base_info.vendor_code AS vendor_code,
    pd_vouchers.foodpanda_ratio AS foodpanda_ratio,
    pd_vouchers.type,
    ROW_NUMBER() OVER (PARTITION BY pd_vouchers.global_entity_id, vendor_base_info.vendor_code, dates.date ORDER BY pd_vouchers.created_at_utc DESC) = 1 AS is_latest_entry,
    TRUE AS is_vf_voucher_deal_day
  FROM fulfillment-dwh-production.pandata_curated.pd_vouchers,
  UNNEST(pd_vendor_uuids) AS pd_vendor_uuids
  INNER JOIN vendor_base_info
          ON pd_vouchers.global_entity_id = vendor_base_info.global_entity_id
         AND SPLIT(pd_vendor_uuids, '_')[OFFSET(0)] = vendor_base_info.vendor_code
  CROSS JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
  WHERE pd_vouchers.start_date_local IS NOT NULL
    AND NOT pd_vouchers.is_deleted
    AND foodpanda_ratio < 100
    AND dates.date BETWEEN DATE(pd_vouchers.start_date_local) AND DATE(pd_vouchers.stop_at_local)
    AND dates.date >= (
          SELECT
            start
          FROM start_date)
    AND dates.date <= CURRENT_DATE
    AND DATE(pd_vouchers.start_date_local) <= dates.date
    AND DATE(pd_vouchers.stop_at_local) >= dates.date
  GROUP BY 1, 2, 3, 4, 5, pd_vouchers.created_at_utc
  HAVING vendor_code IS NOT NULL
)
  SELECT
    * EXCEPT(is_latest_entry)
  FROM voucher_deals
  WHERE is_latest_entry
  ORDER BY 1,3,2
),

gsheet AS (
  SELECT
    email,
    SPLIT(LOWER(TRIM(email)), CASE 
                     WHEN email LIKE "%foodpanda.com" THEN '.com' 
                     WHEN email LIKE "%foodpanda.my" THEN '.my' 
                  END
           )[OFFSET(0)] AS email_split
  FROM `dhh---analytics-apac.pandata_my.central_commercial_agents_raw_data`
),

rooster AS (
  SELECT 
    a.email,
    SPLIT(a.email, CASE 
                     WHEN email LIKE "%foodpanda.com" THEN '.com' 
                     WHEN email LIKE "%foodpanda.my" THEN '.my' 
                  END
           )[OFFSET(0)] AS email_split
  FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents a
  WHERE department_name = 'Commercial'
),

union_data AS (
SELECT * FROM gsheet
UNION ALL
SELECT * FROM rooster
),

central_agent AS (
SELECT DISTINCT
  email_split AS alias
FROM union_data
),

active_vf_discount_deal_by_day AS (
  WITH deal_by_day AS (
    SELECT 
      pd_vendors.global_entity_id,
      dates.date,
      pd_vendors.vendor_code,
      pd_discounts.id,
      vendor_discounts.foodpanda_ratio,
      vendor_discounts.discount_type AS type,
      vendor_discounts.condition_type,
      ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
      TRUE AS is_vf_discount_deal_day,
      discount_mgmt_created_by_type,
      central_agent.alias IS NOT NULL AS is_ssc_booking,
      vgr_deals.id IS NOT NULL AS is_self_booking
    FROM fulfillment-dwh-production.pandata_curated.pd_vendors
    LEFT JOIN pd_vendors.discounts AS vendor_discounts
    LEFT JOIN pd_vendors.menu_categories
    LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
           ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
    INNER JOIN vendor_base_info
            ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
           AND pd_vendors.vendor_code = vendor_base_info.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
           ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
    LEFT JOIN central_agent
    ON SPLIT(LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]), 
              CASE 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.com" THEN '.com' 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.my" THEN '.my' 
              END)[OFFSET(0)] = central_agent.alias
    LEFT JOIN fulfillment-dwh-production.pandata_curated.vgr_deals
           ON vgr_deals.global_entity_id = pd_vendors.global_entity_id
          AND pd_discounts.id = CAST(vgr_deals.id AS INT64)
          AND vgr_deals.created_date_utc <= CURRENT_DATE
    WHERE vendor_discounts.start_date_local IS NOT NULL
      AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
         OR pd_vendors.global_entity_id = 'FP_PK')
      AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
      AND dates.date >= (SELECT start FROM start_date)
      AND dates.date <= CURRENT_DATE
      AND vendor_discounts.start_date_local <= dates.date
      AND vendor_discounts.end_date_local >= dates.date
      AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
      AND LOWER(pd_discounts.description) NOT LIKE '%test%'
    GROUP BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10,11,12
  )
  SELECT
  * EXCEPT(is_latest_entry)
  FROM deal_by_day
  WHERE is_latest_entry
    AND foodpanda_ratio < 100
),

active_normal_delivery_deal_by_day AS (
  WITH deal_by_day AS (
    SELECT 
      pd_vendors.global_entity_id,
      dates.date,
      pd_vendors.vendor_code,
      pd_discounts.id,
      vendor_discounts.foodpanda_ratio,
      vendor_discounts.discount_type AS type,
      vendor_discounts.condition_type,
      ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
      TRUE AS is_normal_delivery_vf_deal_day,
      discount_mgmt_created_by_type,
      central_agent.alias IS NOT NULL AS is_ssc_booking,
      vgr_deals.id IS NOT NULL AS is_self_booking
    FROM fulfillment-dwh-production.pandata_curated.pd_vendors
    LEFT JOIN pd_vendors.discounts AS vendor_discounts
    LEFT JOIN pd_vendors.menu_categories
    LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
           ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
    INNER JOIN vendor_base_info
            ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
           AND pd_vendors.vendor_code = vendor_base_info.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
           ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
    LEFT JOIN central_agent
    ON SPLIT(LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]), 
              CASE 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.com" THEN '.com' 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.my" THEN '.my' 
              END)[OFFSET(0)] = central_agent.alias
    LEFT JOIN fulfillment-dwh-production.pandata_curated.vgr_deals
           ON vgr_deals.global_entity_id = pd_vendors.global_entity_id
          AND pd_discounts.id = CAST(vgr_deals.id AS INT64)
          AND vgr_deals.created_date_utc <= CURRENT_DATE
    WHERE vendor_discounts.start_date_local IS NOT NULL
      AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
         OR pd_vendors.global_entity_id = 'FP_PK')
      AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
      AND dates.date >= (SELECT start FROM start_date)
      AND dates.date <= CURRENT_DATE
      AND vendor_discounts.start_date_local <= dates.date
      AND vendor_discounts.end_date_local >= dates.date
      AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
      AND LOWER(pd_discounts.description) NOT LIKE '%test%'
      AND (LOWER(pd_discounts.expedition_types) LIKE '%delivery%' OR pd_discounts.expedition_types IS NULL)
      AND NOT pd_discounts.is_subscription_discount
      AND LOWER(pd_discounts.title) NOT LIKE '%corporate%'
      AND LOWER(pd_discounts.description) NOT LIKE '%corporate%'
      AND LOWER(pd_discounts.description) NOT LIKE '%pos uat%'
      AND LOWER(pd_discounts.description) NOT LIKE '%pickup%'
      AND pd_vendors.is_customer_type_b2c
    ORDER BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10,11,12
  )
  SELECT
  * EXCEPT(is_latest_entry)
  FROM deal_by_day
  WHERE is_latest_entry
    AND foodpanda_ratio < 100
),

active_normal_pickup_deal_by_day AS (
  WITH deal_by_day AS (
    SELECT
      pd_vendors.global_entity_id,
      dates.date,
      pd_vendors.vendor_code,
      pd_discounts.id,
      vendor_discounts.foodpanda_ratio,
      vendor_discounts.discount_type AS type,
      vendor_discounts.condition_type,
      ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
      TRUE AS is_normal_pickup_vf_deal_day,
      discount_mgmt_created_by_type,
      central_agent.alias IS NOT NULL AS is_ssc_booking,
      vgr_deals.id IS NOT NULL AS is_self_booking
    FROM fulfillment-dwh-production.pandata_curated.pd_vendors
    LEFT JOIN pd_vendors.discounts AS vendor_discounts
    LEFT JOIN pd_vendors.menu_categories
    LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
           ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
    INNER JOIN vendor_base_info
            ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
           AND pd_vendors.vendor_code = vendor_base_info.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
           ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
    LEFT JOIN central_agent
    ON SPLIT(LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]), 
              CASE 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.com" THEN '.com' 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.my" THEN '.my' 
              END)[OFFSET(0)] = central_agent.alias
    LEFT JOIN fulfillment-dwh-production.pandata_curated.vgr_deals
           ON vgr_deals.global_entity_id = pd_vendors.global_entity_id
          AND pd_discounts.id = CAST(vgr_deals.id AS INT64)
          AND vgr_deals.created_date_utc <= CURRENT_DATE
    WHERE vendor_discounts.start_date_local IS NOT NULL
      AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
         OR pd_vendors.global_entity_id = 'FP_PK')
      AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
      AND dates.date >= (SELECT start FROM start_date)
      AND dates.date <= CURRENT_DATE
      AND vendor_discounts.start_date_local <= dates.date
      AND vendor_discounts.end_date_local >= dates.date
      AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
      AND LOWER(pd_discounts.description) NOT LIKE '%test%'
      AND (LOWER(pd_discounts.expedition_types) NOT LIKE '%delivery%' OR pd_discounts.expedition_types IS NULL)
      AND NOT pd_discounts.is_subscription_discount
      AND LOWER(pd_discounts.title) NOT LIKE '%corporate%'
      AND LOWER(pd_discounts.description) NOT LIKE '%corporate%'
      AND LOWER(pd_discounts.description) NOT LIKE '%pos uat%'
      AND pd_discounts.discount_type != 'free-delivery'
      AND pd_vendors.is_customer_type_b2c
    ORDER BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10,11,12
  )
  SELECT
  * EXCEPT(is_latest_entry)
  FROM deal_by_day
  WHERE is_latest_entry
    AND foodpanda_ratio < 100
),

active_pro_deal_by_day AS (
  WITH deal_by_day AS (
    SELECT
      pd_vendors.global_entity_id,
      dates.date,
      pd_vendors.vendor_code,
      pd_discounts.id,
      vendor_discounts.foodpanda_ratio,
      vendor_discounts.discount_type AS type,
      vendor_discounts.condition_type,
      ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
      TRUE AS is_pro_vf_deal_day,
      discount_mgmt_created_by_type,
      central_agent.alias IS NOT NULL AS is_ssc_booking,
      vgr_deals.id IS NOT NULL AS is_self_booking
    FROM fulfillment-dwh-production.pandata_curated.pd_vendors
    LEFT JOIN pd_vendors.discounts AS vendor_discounts
    LEFT JOIN pd_vendors.menu_categories
    LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
           ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
    INNER JOIN vendor_base_info
            ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
           AND pd_vendors.vendor_code = vendor_base_info.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
           ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
    LEFT JOIN central_agent
    ON SPLIT(LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]), 
              CASE 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.com" THEN '.com' 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.my" THEN '.my' 
              END)[OFFSET(0)] = central_agent.alias
    LEFT JOIN fulfillment-dwh-production.pandata_curated.vgr_deals
           ON vgr_deals.global_entity_id = pd_vendors.global_entity_id
          AND pd_discounts.id = CAST(vgr_deals.id AS INT64)
          AND vgr_deals.created_date_utc <= CURRENT_DATE
    WHERE vendor_discounts.start_date_local IS NOT NULL
      AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
         OR pd_vendors.global_entity_id = 'FP_PK')
      AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
      AND dates.date >= (SELECT start FROM start_date)
      AND dates.date <= CURRENT_DATE
      AND vendor_discounts.start_date_local <= dates.date
      AND vendor_discounts.end_date_local >= dates.date
      AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
      AND LOWER(pd_discounts.description) NOT LIKE '%test%'
      AND pd_discounts.is_subscription_discount
      AND LOWER(pd_discounts.title) NOT LIKE '%corporate%'
      AND LOWER(pd_discounts.description) NOT LIKE '%corporate%'
      AND LOWER(pd_discounts.description) NOT LIKE '%pos uat%'
      AND pd_vendors.is_customer_type_b2c
    ORDER BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10,11,12
  )
  SELECT
  * EXCEPT(is_latest_entry)
  FROM deal_by_day
  WHERE is_latest_entry
    AND foodpanda_ratio < 100
),

active_corporate_delivery_deal_by_day AS (
  WITH deal_by_day AS (
    SELECT
      pd_vendors.global_entity_id,
      dates.date,
      pd_vendors.vendor_code,
      pd_discounts.id,
      vendor_discounts.foodpanda_ratio,
      vendor_discounts.discount_type AS type,
      vendor_discounts.condition_type,
      ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
      TRUE AS is_corporate_delivery_vf_deal_day,
      discount_mgmt_created_by_type,
      central_agent.alias IS NOT NULL AS is_ssc_booking,
      vgr_deals.id IS NOT NULL AS is_self_booking
    FROM fulfillment-dwh-production.pandata_curated.pd_vendors
    LEFT JOIN pd_vendors.discounts AS vendor_discounts
    LEFT JOIN pd_vendors.menu_categories
    LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
           ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
    INNER JOIN vendor_base_info
            ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
           AND pd_vendors.vendor_code = vendor_base_info.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
           ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
    LEFT JOIN central_agent
    ON SPLIT(LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]), 
              CASE 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.com" THEN '.com' 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.my" THEN '.my' 
              END)[OFFSET(0)] = central_agent.alias
    LEFT JOIN fulfillment-dwh-production.pandata_curated.vgr_deals
           ON vgr_deals.global_entity_id = pd_vendors.global_entity_id
          AND pd_discounts.id = CAST(vgr_deals.id AS INT64)
          AND vgr_deals.created_date_utc <= CURRENT_DATE
    WHERE vendor_discounts.start_date_local IS NOT NULL
      AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
         OR pd_vendors.global_entity_id = 'FP_PK')
      AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
      AND dates.date >= (SELECT start FROM start_date)
      AND dates.date <= CURRENT_DATE
      AND vendor_discounts.start_date_local <= dates.date
      AND vendor_discounts.end_date_local >= dates.date
      AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
      AND LOWER(pd_discounts.description) NOT LIKE '%test%'
      AND(LOWER(pd_discounts.expedition_types) LIKE '%delivery%' OR pd_discounts.expedition_types IS NULL)
      AND NOT pd_discounts.is_subscription_discount
      AND LOWER(pd_discounts.description) NOT LIKE '%pos uat%'
      AND LOWER(pd_discounts.description) NOT LIKE '%pickup%'
      AND pd_vendors.is_customer_type_b2b
    ORDER BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10,11,12
  )
  SELECT
  * EXCEPT(is_latest_entry)
  FROM deal_by_day
  WHERE is_latest_entry
    AND foodpanda_ratio < 100
),

active_corporate_pickup_deal_by_day AS (
  WITH deal_by_day AS (
    SELECT
      pd_vendors.global_entity_id,
      dates.date,
      pd_vendors.vendor_code,
      pd_discounts.id,
      vendor_discounts.foodpanda_ratio,
      vendor_discounts.discount_type AS type,
      vendor_discounts.condition_type,
      ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
      TRUE AS is_corporate_pickup_vf_deal_day,
      discount_mgmt_created_by_type,
      central_agent.alias IS NOT NULL AS is_ssc_booking,
      vgr_deals.id IS NOT NULL AS is_self_booking
    FROM fulfillment-dwh-production.pandata_curated.pd_vendors
    LEFT JOIN pd_vendors.discounts AS vendor_discounts
    LEFT JOIN pd_vendors.menu_categories
    LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
           ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
    INNER JOIN vendor_base_info
            ON pd_vendors.global_entity_id = vendor_base_info.global_entity_id
           AND pd_vendors.vendor_code = vendor_base_info.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
           ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
    LEFT JOIN central_agent
    ON SPLIT(LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]), 
              CASE 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.com" THEN '.com' 
                 WHEN LOWER(SPLIT(discount_mgmt_created_by_type, '-')[OFFSET(1)]) LIKE "%foodpanda.my" THEN '.my' 
              END)[OFFSET(0)] = central_agent.alias
    LEFT JOIN fulfillment-dwh-production.pandata_curated.vgr_deals
           ON vgr_deals.global_entity_id = pd_vendors.global_entity_id
          AND pd_discounts.id = CAST(vgr_deals.id AS INT64)
          AND vgr_deals.created_date_utc <= CURRENT_DATE
    WHERE vendor_discounts.start_date_local IS NOT NULL
      AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
         OR pd_vendors.global_entity_id = 'FP_PK')
      AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
      AND dates.date >= (SELECT start FROM start_date)
      AND dates.date <= CURRENT_DATE
      AND vendor_discounts.start_date_local <= dates.date
      AND vendor_discounts.end_date_local >= dates.date
      AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
      AND LOWER(pd_discounts.description) NOT LIKE '%test%'
      AND (LOWER(pd_discounts.expedition_types) NOT LIKE '%delivery%' OR pd_discounts.expedition_types IS NULL)
      AND NOT pd_discounts.is_subscription_discount
      AND LOWER(pd_discounts.description) NOT LIKE '%pos uat%'
      AND pd_discounts.discount_type != 'free-delivery'
      AND pd_vendors.is_customer_type_b2b
    ORDER BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10,11,12
  )
  SELECT
  * EXCEPT(is_latest_entry)
  FROM deal_by_day
  WHERE is_latest_entry
    AND foodpanda_ratio < 100
)
  SELECT
    dates.global_entity_id,
    dates.vendor_code,
    dates.date,
    CASE WHEN is_normal_delivery_vf_deal_day THEN TRUE ELSE FALSE END AS is_normal_delivery_vf_deal_day,
    CASE WHEN is_normal_pickup_vf_deal_day THEN TRUE ELSE FALSE END AS is_normal_pickup_vf_deal_day,
    CASE WHEN is_pro_vf_deal_day THEN TRUE ELSE FALSE END AS is_pro_vf_deal_day,
    CASE WHEN is_corporate_delivery_vf_deal_day THEN TRUE ELSE FALSE END AS is_corporate_delivery_vf_deal_day,
    CASE WHEN is_corporate_pickup_vf_deal_day THEN TRUE ELSE FALSE END AS is_corporate_pickup_vf_deal_day,
    CASE WHEN is_vf_voucher_deal_day THEN TRUE ELSE FALSE END AS is_vf_voucher_deal_day,
    CASE WHEN is_vf_discount_deal_day THEN TRUE ELSE FALSE END AS is_vf_discount_deal_day,

    CASE WHEN is_normal_delivery_vf_deal_day THEN active_normal_delivery_deal_by_day.type ELSE NULL END AS is_normal_delivery_vf_deal_type,
    CASE WHEN is_normal_pickup_vf_deal_day THEN active_normal_pickup_deal_by_day.type ELSE NULL END AS is_normal_pickup_vf_deal_type,
    CASE WHEN is_pro_vf_deal_day THEN active_pro_deal_by_day.type ELSE NULL END AS is_pro_vf_deal_type,
    CASE WHEN is_corporate_delivery_vf_deal_day THEN active_corporate_delivery_deal_by_day.type ELSE NULL END AS is_corporate_delivery_vf_deal_type,
    CASE WHEN is_corporate_pickup_vf_deal_day THEN active_corporate_pickup_deal_by_day.type ELSE NULL END AS is_corporate_pickup_vf_deal_type,
    CASE WHEN is_vf_voucher_deal_day THEN active_voucher_deal_by_day.type ELSE NULL END AS is_vf_voucher_deal_type,
    CASE WHEN is_vf_discount_deal_day THEN active_vf_discount_deal_by_day.type ELSE NULL END AS is_vf_discount_deal_type,

    CASE
      WHEN is_normal_delivery_vf_deal_day AND active_normal_delivery_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_normal_delivery_vf_deal_cofunded,
    CASE
      WHEN is_normal_pickup_vf_deal_day AND active_normal_pickup_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_normal_pickup_vf_deal_cofunded,
    CASE
      WHEN is_pro_vf_deal_day AND active_pro_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_pro_vf_deal_cofunded,
    CASE
      WHEN is_corporate_delivery_vf_deal_day AND active_corporate_delivery_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_corporate_delivery_vf_deal_cofunded,
    CASE
      WHEN is_corporate_pickup_vf_deal_day AND active_corporate_pickup_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_corporate_pickup_vf_deal_cofunded,
    CASE
      WHEN is_vf_voucher_deal_day AND active_voucher_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_vf_voucher_deal_cofunded,
    CASE
      WHEN is_vf_discount_deal_day AND active_vf_discount_deal_by_day.foodpanda_ratio > 0
      THEN TRUE ELSE FALSE
    END AS is_vf_discount_deal_cofunded,
   COALESCE(
    active_vf_discount_deal_by_day.is_ssc_booking,
    active_normal_delivery_deal_by_day.is_ssc_booking,
    active_normal_pickup_deal_by_day.is_ssc_booking,
    active_pro_deal_by_day.is_ssc_booking,
    active_corporate_delivery_deal_by_day.is_ssc_booking,
    active_corporate_pickup_deal_by_day.is_ssc_booking
   ) AS is_ssc_agent,
   
   COALESCE(
    active_vf_discount_deal_by_day.is_self_booking,
    active_normal_delivery_deal_by_day.is_self_booking,
    active_normal_pickup_deal_by_day.is_self_booking,
    active_pro_deal_by_day.is_self_booking,
    active_corporate_delivery_deal_by_day.is_self_booking,
    active_corporate_pickup_deal_by_day.is_self_booking
   ) AS is_self_booking_deal_day

  FROM dates
  LEFT JOIN active_normal_delivery_deal_by_day
         ON active_normal_delivery_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_normal_delivery_deal_by_day.vendor_code = dates.vendor_code
        AND active_normal_delivery_deal_by_day.date = dates.date
  LEFT JOIN active_normal_pickup_deal_by_day
         ON active_normal_pickup_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_normal_pickup_deal_by_day.vendor_code = dates.vendor_code
        AND active_normal_pickup_deal_by_day.date = dates.date
  LEFT JOIN active_pro_deal_by_day
         ON active_pro_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_pro_deal_by_day.vendor_code = dates.vendor_code
        AND active_pro_deal_by_day.date = dates.date
  LEFT JOIN active_corporate_delivery_deal_by_day
         ON active_corporate_delivery_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_corporate_delivery_deal_by_day.vendor_code = dates.vendor_code
        AND active_corporate_delivery_deal_by_day.date = dates.date
  LEFT JOIN active_corporate_pickup_deal_by_day
         ON active_corporate_pickup_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_corporate_pickup_deal_by_day.vendor_code = dates.vendor_code
        AND active_corporate_pickup_deal_by_day.date = dates.date
  LEFT JOIN active_voucher_deal_by_day
         ON active_voucher_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_voucher_deal_by_day.vendor_code = dates.vendor_code
        AND active_voucher_deal_by_day.date = dates.date
  LEFT JOIN active_vf_discount_deal_by_day
         ON active_vf_discount_deal_by_day.global_entity_id = dates.global_entity_id
        AND active_vf_discount_deal_by_day.vendor_code = dates.vendor_code
        AND active_vf_discount_deal_by_day.date = dates.date
  ORDER BY 1,2,3
