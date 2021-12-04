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

active_vendors_daily AS (
  SELECT
    vendor_status_by_day.date AS day,
    FORMAT_DATE("%G-%V",vendor_status_by_day.date) AS week,
    DATE_TRUNC(vendor_status_by_day.date, MONTH) AS month,
    vendor_base_info.business_type,
    vendor_base_info.aaa_type,
    vendor_base_info.gmv_class,
    vendor_status_by_day.global_entity_id,
    vendor_base_info.country,
    vendor_status_by_day.vendor_code,
    vendor_base_info.chain_code,
    TRUE AS is_daily_active
  FROM fulfillment-dwh-production.pandata_report.pandora_pd_vendors_active_status AS vendor_status_by_day
  INNER JOIN vendor_base_info
         ON vendor_status_by_day.global_entity_id = vendor_base_info.global_entity_id
        AND vendor_status_by_day.vendor_code = vendor_base_info.vendor_code
  WHERE TRUE
    AND vendor_status_by_day.is_active
    AND NOT vendor_status_by_day.is_private
    AND NOT vendor_status_by_day.is_test
    AND vendor_status_by_day.date >= (SELECT start FROM start_date)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

exchange_rate_daily AS (
  SELECT
    countries.global_entity_id,
    fx_rates.fx_rate_date,
    AVG(fx_rates.fx_rate_eur) AS exchange_rate
  FROM `fulfillment-dwh-production.pandata_curated.shared_countries` countries
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx_rates
         ON countries.currency_code_iso = fx_rates.currency_code_iso
  WHERE global_entity_id LIKE 'FP_%'
    AND global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND fx_rates.fx_rate_date >= (SELECT start FROM start_date)
    AND fx_rates.fx_rate_date <= CURRENT_DATE
  GROUP BY 1, 2
),

free_item_deal AS (
  SELECT
    active_vendors_daily.global_entity_id,
    active_vendors_daily.chain_code,
    active_vendors_daily.day,
    CAST(SAFE_DIVIDE(vendor_funded_deal_free_item_value_local, exchange_rate) AS FLOAT64) AS vf_deal_free_item_eur,
    COUNT(DISTINCT active_vendors_daily.vendor_code) AS no_of_outlets,
    SAFE_DIVIDE(
      CAST(SAFE_DIVIDE(
        vendor_funded_deal_free_item_value_local,
        exchange_rate) AS FLOAT64),
      COUNT(DISTINCT active_vendors_daily.vendor_code)
    ) AS free_item_value_vf_per_outlet
  FROM active_vendors_daily
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.gsheet_chain_free_item_value` AS free_item_value
         ON active_vendors_daily.global_entity_id = free_item_value.global_entity_id
        AND active_vendors_daily.day = free_item_value.ordered_date_local
        AND active_vendors_daily.chain_code = TRIM(free_item_value.chain_code," ")
  LEFT JOIN exchange_rate_daily
         ON active_vendors_daily.day = exchange_rate_daily.fx_rate_date
        AND active_vendors_daily.global_entity_id = exchange_rate_daily.global_entity_id
  GROUP BY 1,2,3,vendor_funded_deal_free_item_value_local,exchange_rate
  HAVING vf_deal_free_item_eur IS NOT NULL
),

daily_order_details AS (
  SELECT
    o.global_entity_id,
    o.country_name AS country,
    o.vendor_code,
    vendor_base_info.* EXCEPT(global_entity_id, country, vendor_code, chain_code),
    pd_vendors.chain_code,
    DATE(o.created_at_local) AS date_local,
    FORMAT_DATE("%G-%V",o.created_at_local) AS week,
    DATE_TRUNC(o.created_at_local, MONTH) AS month,
    /*Valid Orders*/
    COUNT(DISTINCT(o.id)) AS daily_all_valid_orders_1,
    COUNT(DISTINCT IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, o.id, NULL)) AS daily_normal_delivery_orders_2,
    COUNT(DISTINCT IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, o.id, NULL)) AS daily_normal_pickup_orders_3,
    COUNT(DISTINCT IF(o.expedition_type = 'delivery' AND cp.is_corporate_order, o.id, NULL)) AS daily_corporate_delivery_orders_4,
    COUNT(DISTINCT IF(o.expedition_type = 'pickup' AND cp.is_corporate_order, o.id, NULL)) AS daily_corporate_pickup_orders_5,
    COUNT(DISTINCT IF(pro.is_subscription_order AND NOT cp.is_corporate_order, o.id, NULL)) AS daily_normal_pro_orders_6,
    
    /*Valid All VF Orders*/
    COUNT(DISTINCT 
      IF((
      (do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
       OR  (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)),
       o.id,
       NULL)
    ) AS daily_all_vf_orders_1,
    COUNT(DISTINCT 
    IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100)
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), o.id, NULL)
    ) AS daily_vf_normal_delivery_orders_2,
    COUNT(DISTINCT IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)),  o.id, NULL)) AS daily_vf_normal_pickup_orders_3,
    COUNT(DISTINCT IF(o.expedition_type = 'delivery' AND cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)),  o.id, NULL)) AS daily_vf_corporate_delivery_orders_4,
    COUNT(DISTINCT IF(o.expedition_type = 'pickup' AND cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), o.id, NULL)) AS daily_vf_corporate_pickup_orders_5,
    COUNT(DISTINCT IF(pro.is_subscription_order AND NOT cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), o.id, NULL)) AS daily_vf_normal_pro_orders_6,
     
     /*VF Deal Types*/
     COUNT(DISTINCT IF(((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type = 'percentage') 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type = 'percentage')),
                     o.id, NULL
                     )
         ) AS daily_vf_orders_discount_percentage,
     COUNT(DISTINCT IF(((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type = 'amount') 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type = 'amount')),
                     o.id, NULL
                     )
         ) AS daily_vf_orders_discount_amount,
     COUNT(DISTINCT IF(((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type = 'free-delivery') 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type = 'delivery_fee')),
                     o.id, NULL
                     )
         ) AS daily_vf_orders_free_delivery,
     COUNT(DISTINCT IF(((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type = 'text_freegift') 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type = 'free_gift')),
                     o.id, NULL
                     )
         ) AS daily_vf_orders_free_item,
     
     /*GMV (All Orders)*/
    SUM(IF(o.is_valid_order, pd_orders_agg_accounting.gmv_eur, 0)) AS daily_all_gmv_eur,
     
    /*GFV (All Orders)*/
    SUM(IF(o.is_valid_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_all_gfv_eur_1,
    SUM(IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_delivery_all_gfv_eur_2,
    SUM(IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_pickup_all_gfv_eur_3,
    SUM(IF(o.expedition_type = 'delivery' AND cp.is_corporate_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_corporate_delivery_all_gfv_eur_4,
    SUM(IF(o.expedition_type = 'pickup' AND cp.is_corporate_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_corporate_pickup_all_gfv_eur_5,
    SUM(IF(pro.is_subscription_order AND NOT cp.is_corporate_order, pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_pro_all_gfv_eur_6,  
    
    /*GFV - VF Deals*/
    SUM(IF(((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)),
                     pd_orders_agg_accounting.gfv_eur, 0
                     )
                     ) AS daily_vf_gfv_eur_1,
    SUM(IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_delivery_vf_gfv_eur_2,
    SUM(IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_pickup_vf_gfv_eur_3,
    SUM(IF(o.expedition_type = 'delivery' AND cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_corporate_delivery_vf_gfv_eur_4,
    SUM(IF(o.expedition_type = 'pickup' AND cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_corporate_pickup_vf_gfv_eur_5,
    SUM(IF(pro.is_subscription_order AND NOT cp.is_corporate_order
                      AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_pro_vf_gfv_eur_6, 
     
    /*GFV - Only Discount & Amount*/
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'),
                     pd_orders_agg_accounting.gfv_eur, 0) ) AS daily_vf_discount_amount_gfv_eur_1,
    SUM(IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_delivery_vf_discount_amount_gfv_eur_2,
    SUM(IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_pickup_vf_discount_amount_gfv_eur_3,
    SUM(IF(o.expedition_type = 'delivery' AND cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_corporate_delivery_vf_discount_amount_gfv_eur_4,
    SUM(IF(o.expedition_type = 'pickup' AND cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_corporate_pickup_vf_discount_amount_gfv_eur_5,
    SUM(IF(pro.is_subscription_order AND NOT cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), pd_orders_agg_accounting.gfv_eur, 0)) AS daily_normal_pro_vf_discount_amount_gfv_eur_6,
    
    /*VF Disount Value - Only Discount & Amount*/
    SUM(
    IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'),
    do.discount.vendor_subsidized_value_eur, 0) ) AS daily_vf_discount_amount_deal_eur_1,
    SUM(IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), do.discount.vendor_subsidized_value_eur, 0)) AS daily_normal_delivery_vf_discount_amount_deal_eur_2,
    SUM(IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), do.discount.vendor_subsidized_value_eur, 0)) AS daily_normal_pickup_vf_discount_amount_deal_eur_3,
    SUM(IF(o.expedition_type = 'delivery' AND cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), do.discount.vendor_subsidized_value_eur, 0)) AS daily_corporate_delivery_vf_discount_amount_deal_eur_4,
    SUM(IF(o.expedition_type = 'pickup' AND cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), do.discount.vendor_subsidized_value_eur, 0)) AS daily_corporate_pickup_vf_discount_amount_deal_eur_5,
    SUM(IF(pro.is_subscription_order AND NOT cp.is_corporate_order
                      AND do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_discounts.discount_type IN ('amount','percentage'), do.discount.vendor_subsidized_value_eur, 0)) AS daily_normal_pro_vf_discount_amount_deal_eur_6,
                      
    /*VF Deal Value - VF Deals*/
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100, do.discount.vendor_subsidized_value_eur,0))
    +
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_vf_deal_value_eur_1,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0))
    +
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_normal_delivery_vf_deal_value_eur_2,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_normal_pickup_vf_deal_value_eur_3,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_corporate_delivery_vf_deal_value_eur_4,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_corporate_pickup_vf_deal_value_eur_5,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pro.is_subscription_order AND NOT cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pro.is_subscription_order AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_normal_pro_vf_deal_value_eur_6,
    
    /*fp funded Value - VF Deals*/
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_fp_funded_value_eur_1,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_normal_delivery_fp_funded_value_eur_2,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_normal_pickup_fp_funded_value_eur_3,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND cp.is_corporate_order, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'delivery' AND cp.is_corporate_order, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_corporate_delivery_fp_funded_value_eur_4,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND cp.is_corporate_order, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND o.expedition_type = 'pickup' AND cp.is_corporate_order, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_corporate_pickup_fp_funded_value_eur_5,
    COALESCE(
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pro.is_subscription_order AND NOT cp.is_corporate_order, do.discount.foodpanda_subsidized_value_eur,0))+
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pro.is_subscription_order AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.foodpanda_subsidized_value_eur,0))
    ,0) AS daily_normal_pro_fp_funded_value_eur_6,
    
    /*Discounted GFV - VF Deals*/
    COALESCE(
    SUM(IF(((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND discount_type != 'free-delivery', do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type != 'delivery_fee', pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_discount_gfv_eur_1,
    
    COALESCE(
    SUM(IF(o.expedition_type = 'delivery' AND NOT cp.is_corporate_order AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND discount_type != 'free-delivery' AND o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type != 'delivery_fee' AND o.expedition_type = 'delivery' AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_normal_delivery_discount_gfv_eur_2,
    
    COALESCE(
    SUM(IF(o.expedition_type = 'pickup' AND NOT cp.is_corporate_order AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) OR 
                     (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND discount_type != 'free-delivery' AND o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type != 'delivery_fee' AND o.expedition_type = 'pickup' AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_normal_pickup_discount_gfv_eur_3,
    
    COALESCE(
    SUM(IF(o.expedition_type = 'delivery' AND cp.is_corporate_order AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND discount_type != 'free-delivery' AND o.expedition_type = 'delivery' AND cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type != 'delivery_fee' AND o.expedition_type = 'delivery' AND cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_corporate_delivery_discount_gfv_eur_4,
    
    COALESCE(
    SUM(IF(o.expedition_type = 'pickup' AND cp.is_corporate_order AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND discount_type != 'free-delivery' AND o.expedition_type = 'pickup' AND cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type != 'delivery_fee' AND o.expedition_type = 'pickup' AND cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_corporate_pickup_discount_gfv_eur_5,
    
    COALESCE(
    SUM(IF(pro.is_subscription_order AND NOT cp.is_corporate_order AND ((do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100) 
                     OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100)), pd_orders_agg_accounting.gfv_eur, 0)) -
    SUM(IF(do.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND discount_type != 'free-delivery' AND pro.is_subscription_order AND NOT cp.is_corporate_order, do.discount.vendor_subsidized_value_eur,0)) -
    SUM(IF(pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.attributions_foodpanda_ratio < 100 AND pd_vouchers.type != 'delivery_fee' AND pro.is_subscription_order AND NOT cp.is_corporate_order, pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur,0))
    ,0) AS daily_normal_pro_discount_gfv_eur_6
    
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` pd_orders_agg_accounting
         ON pd_orders_agg_accounting.uuid = o.uuid
        AND pd_orders_agg_accounting.created_date_utc <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` pd_vendors
       ON pd_vendors.global_entity_id = o.global_entity_id
      AND pd_vendors.vendor_code = o.vendor_code
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers pd_orders_agg_vouchers
         ON pd_orders_agg_vouchers.uuid = o.uuid
        AND pd_orders_agg_vouchers.created_date_utc <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` AS pandora_pd_orders_agg_jkr_deals
         ON o.global_entity_id = pandora_pd_orders_agg_jkr_deals.global_entity_id
        AND o.uuid = pandora_pd_orders_agg_jkr_deals.uuid
        AND pandora_pd_orders_agg_jkr_deals.created_date_local <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` pd_vendors_agg_business_types
       ON pd_vendors_agg_business_types.uuid = pd_vendors.uuid
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts do
         on do.global_entity_id = o.global_entity_id
        AND do.uuid = o.uuid
        AND do.created_date_utc <= CURRENT_DATE
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
         ON pd_discounts.uuid = do.pd_discount_uuid
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_vouchers
         ON pd_vouchers.uuid = pd_orders_agg_vouchers.pd_voucher_uuid
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_cp_orders cp
         on cp.global_entity_id = o.global_entity_id
        AND cp.uuid = o.uuid
        AND cp.created_date_utc <= CURRENT_DATE
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_sb_subscriptions pro
         on pro.global_entity_id = o.global_entity_id
        AND pro.uuid = o.uuid
        AND pro.created_date_utc <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_report.order_commissions` AS order_commissions
         ON o.global_entity_id = order_commissions.global_entity_id
        AND o.code = order_commissions.order_code
        AND order_commissions.order_created_date_utc <= CURRENT_DATE
  INNER JOIN vendor_base_info
          ON o.global_entity_id = vendor_base_info.global_entity_id
         AND o.vendor_code = vendor_base_info.vendor_code
        
  WHERE o.created_date_local >= (SELECT start FROM start_date)
    AND o.created_date_utc <= CURRENT_DATE
    AND o.created_date_local <= CURRENT_DATE
    AND NOT o.is_test_order
    AND o.is_gross_order
    AND o.is_valid_order
    AND vendor_base_info.vendor_code IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

daily_orders_vf_deal_vendor AS (
  SELECT
    d.global_entity_id,
    vendor_base_info.country,
    DATE(d.date) AS date_local,
    FORMAT_DATE("%G-%V", d.date) AS week,
    DATE_TRUNC(d.date, MONTH) AS month,
    d.vendor_code,
    vendor_base_info.business_type,
    vendor_base_info.aaa_type,
    vendor_base_info.gmv_class,
    vendor_base_info.chain_code,
    IFNULL(daily_all_valid_orders_1, 0) AS daily_deal_day_all_valid_orders_1,
    IF(is_normal_delivery_vf_deal_day, daily_normal_delivery_orders_2, 0) AS daily_deal_day_normal_delivery_orders_2,
    IF(is_normal_pickup_vf_deal_day, daily_normal_pickup_orders_3, 0) AS daily_deal_day_normal_pickup_orders_3,
    IF(is_corporate_delivery_vf_deal_day, daily_corporate_delivery_orders_4, 0) AS daily_deal_day_corporate_delivery_orders_4,
    IF(is_corporate_pickup_vf_deal_day, daily_corporate_pickup_orders_5, 0) AS daily_deal_day_corporate_pickup_orders_5,
    IF(is_pro_vf_deal_day, daily_normal_pro_orders_6, 0) AS daily_deal_day_normal_pro_orders_6,
    
    IFNULL(daily_all_vf_orders_1, 0) AS daily_deal_day_all_vf_orders_1,
    IF(is_normal_delivery_vf_deal_day, daily_vf_normal_delivery_orders_2, 0) AS daily_deal_day_normal_delivery_vf_orders_2,
    IF(is_normal_pickup_vf_deal_day, daily_vf_normal_pickup_orders_3, 0) AS daily_deal_day_normal_pickup_vf_orders_3,
    IF(is_corporate_delivery_vf_deal_day, daily_vf_corporate_delivery_orders_4, 0) AS daily_deal_day_corporate_delivery_vf_orders_4,
    IF(is_corporate_pickup_vf_deal_day, daily_vf_corporate_pickup_orders_5, 0) AS daily_deal_day_corporate_pickup_vf_orders_5,
    IF(is_pro_vf_deal_day, daily_vf_normal_pro_orders_6, 0) AS daily_deal_day_normal_pro_vf_orders_6,
  FROM pandata_ap_commercial.daily_vf_deal_for_vendor d
  LEFT JOIN daily_order_details o
         ON o.global_entity_id = d.global_entity_id
        AND o.date_local = d.date
        AND o.vendor_code = d.vendor_code
  INNER JOIN vendor_base_info
          ON d.global_entity_id = vendor_base_info.global_entity_id
         AND d.vendor_code = vendor_base_info.vendor_code
   WHERE (is_vf_discount_deal_day OR is_normal_delivery_vf_deal_day OR is_normal_pickup_vf_deal_day OR is_pro_vf_deal_day OR is_corporate_delivery_vf_deal_day OR is_corporate_pickup_vf_deal_day)
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
),

vf_deal_day_data AS (
  SELECT
    active_vendors_daily.global_entity_id,
    vendor_base_info.country,
    DATE(active_vendors_daily.day) AS date_local,
    FORMAT_DATE("%G-%V", active_vendors_daily.day) AS week,
    DATE_TRUNC(active_vendors_daily.day, MONTH) AS month,
    active_vendors_daily.vendor_code,
    vendor_base_info.business_type,
    vendor_base_info.aaa_type,
    vendor_base_info.gmv_class,
    vendor_base_info.chain_code,
    
    is_normal_delivery_vf_deal_day,
    is_normal_pickup_vf_deal_day,
    is_pro_vf_deal_day,
    is_corporate_delivery_vf_deal_day,
    is_corporate_pickup_vf_deal_day,
    is_vf_voucher_deal_day,
    is_vf_discount_deal_day
  FROM active_vendors_daily
  LEFT JOIN pandata_ap_commercial.daily_vf_deal_for_vendor
         ON active_vendors_daily.global_entity_id = daily_vf_deal_for_vendor.global_entity_id
        AND active_vendors_daily.day = daily_vf_deal_for_vendor.date
        AND active_vendors_daily.vendor_code = daily_vf_deal_for_vendor.vendor_code
  INNER JOIN vendor_base_info
          ON active_vendors_daily.global_entity_id = vendor_base_info.global_entity_id
         AND active_vendors_daily.vendor_code = vendor_base_info.vendor_code
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  ORDER BY 1,2,3
)

SELECT
  COALESCE(daily_order_details.global_entity_id, daily_orders_vf_deal_vendor.global_entity_id, active_vendors_daily.global_entity_id) AS global_entity_id,
  COALESCE(daily_order_details.country, daily_orders_vf_deal_vendor.country, active_vendors_daily.country) AS country,
  COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day) AS date_local,
  COALESCE(daily_order_details.vendor_code, daily_orders_vf_deal_vendor.vendor_code, active_vendors_daily.vendor_code) AS vendor_code,
  COALESCE(daily_order_details.business_type, daily_orders_vf_deal_vendor.business_type, active_vendors_daily.business_type) AS business_type,
  CASE
    WHEN COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) IN ('A','B') AND vf_deal_day_data.is_self_booking_deal_day
    THEN 'KA Self-Booking'
    WHEN (COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) NOT IN ('A','B')
          OR COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) IS NULL) AND vf_deal_day_data.is_self_booking_deal_day
    THEN 'Long Tail Self-Booking'
    WHEN COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) IN ('A','B')
         AND (NOT vf_deal_day_data.is_self_booking_deal_day OR vf_deal_day_data.is_self_booking_deal_day IS NULL)
         AND is_ssc_agent
    THEN 'KA SSC'
    WHEN (COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) NOT IN ('A','B')
          OR COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) IS NULL)
         AND (NOT vf_deal_day_data.is_self_booking_deal_day OR vf_deal_day_data.is_self_booking_deal_day IS NULL)
         AND is_ssc_agent
    THEN 'Long Tail SSC'
    ELSE 'Local'
  END AS aaa_type,
  COALESCE(daily_order_details.gmv_class, daily_orders_vf_deal_vendor.gmv_class, active_vendors_daily.gmv_class) AS gmv_class,
  COALESCE(daily_order_details.chain_code, daily_orders_vf_deal_vendor.chain_code, active_vendors_daily.chain_code) AS chain_code,
  COALESCE(daily_order_details.week, daily_orders_vf_deal_vendor.week, FORMAT_DATE("%G-%V", active_vendors_daily.day)) AS week,
  DATE(COALESCE(daily_order_details.month, daily_orders_vf_deal_vendor.month, DATE_TRUNC(active_vendors_daily.day, MONTH))) AS month,
  daily_order_details.* EXCEPT(global_entity_id, country, date_local, vendor_code, business_type, aaa_type, gmv_class, chain_code, week, month),
  daily_orders_vf_deal_vendor.* EXCEPT(global_entity_id, country, date_local, vendor_code, business_type, aaa_type, gmv_class, chain_code, week, month),
  CASE
    WHEN active_vendors_daily.is_daily_active
    THEN TRUE
    WHEN daily_all_valid_orders_1 > 0
    THEN TRUE
    ELSE FALSE
  END AS is_daily_active,
  vf_deal_day_data.* EXCEPT(global_entity_id, date, vendor_code),
  IFNULL(free_item_deal.vf_deal_free_item_eur,0) AS vf_deal_free_item_eur,
  DATE_TRUNC(
    COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day),
    ISOWEEK) = DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 WEEK), ISOWEEK
  ) AS is_last_week,
  DATE_TRUNC(
    COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day),
    ISOWEEK) = DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 2 WEEK), ISOWEEK
  ) AS is_2last_week,
  COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day) BETWEEN DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE, ISOWEEK), INTERVAL 1 DAY), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE, ISOWEEK), INTERVAL 1 DAY) AS is_MTD,
  COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day) BETWEEN DATE_SUB(DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE, ISOWEEK), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH) AND DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE, ISOWEEK), INTERVAL 1 DAY), INTERVAL 1 MONTH) AS is_2MTD
FROM daily_order_details
FULL JOIN daily_orders_vf_deal_vendor
       ON daily_order_details.global_entity_id = daily_orders_vf_deal_vendor.global_entity_id
      AND daily_order_details.date_local = daily_orders_vf_deal_vendor.date_local
      AND daily_order_details.vendor_code = daily_orders_vf_deal_vendor.vendor_code
FULL JOIN active_vendors_daily
       ON daily_order_details.global_entity_id = active_vendors_daily.global_entity_id
      AND daily_order_details.date_local = active_vendors_daily.day
      AND daily_order_details.vendor_code = active_vendors_daily.vendor_code
LEFT JOIN pandata_ap_commercial.daily_vf_deal_for_vendor AS vf_deal_day_data
       ON COALESCE(daily_order_details.global_entity_id, daily_orders_vf_deal_vendor.global_entity_id, active_vendors_daily.global_entity_id) = vf_deal_day_data.global_entity_id
      AND COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day) = vf_deal_day_data.date
      AND COALESCE(daily_order_details.vendor_code, daily_orders_vf_deal_vendor.vendor_code, active_vendors_daily.vendor_code) = vf_deal_day_data.vendor_code
LEFT JOIN free_item_deal
       ON COALESCE(daily_order_details.global_entity_id, daily_orders_vf_deal_vendor.global_entity_id, active_vendors_daily.global_entity_id) = free_item_deal.global_entity_id
      AND COALESCE(daily_order_details.date_local, daily_orders_vf_deal_vendor.date_local, active_vendors_daily.day) = DATE(free_item_deal.day)
      AND COALESCE(daily_order_details.chain_code, daily_orders_vf_deal_vendor.chain_code, active_vendors_daily.chain_code) = free_item_deal.chain_code
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,
20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,
45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,
70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,
95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110
