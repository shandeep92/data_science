/*
Author: Abbhinaya Pragasam
*/

WITH daily AS (
  SELECT
    global_entity_id,
    business_type,
    gmv_class,
    chain_code,
    date_local,
    month,
    MAX(aaa_type) AS aaa_type,
    MAX(IFNULL(vf_deal_free_item_eur,0)) AS daily_vf_deal_free_item_eur
  FROM pandata_ap_commercial.daily_deals_performance_data
  GROUP BY 1,2,3,4,5,6
),

free_item_eur AS (
  SELECT
    global_entity_id,
    month,
    business_type,
    aaa_type,
    gmv_class,
    SUM(IFNULL(daily_vf_deal_free_item_eur,0)) AS monthly_vf_deal_free_item_eur
  FROM daily
  GROUP BY 1,2,3,4,5
)

SELECT
  daily_deals_performance_data.global_entity_id,
  COALESCE(
  daily_deals_performance_data.country, 
  CASE WHEN daily_deals_performance_data.global_entity_id = 'FP_BD'
  THEN 'Bangladesh'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_PK'
  THEN 'Pakistan'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_SG'
  THEN 'Singapore'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_MY'
  THEN 'Malaysia'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_TH'
  THEN 'Thailand'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_TW'
  THEN 'Taiwan'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_HK'
  THEN 'Hong Kong'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_PH'
  THEN 'Philippines'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_LA'
  THEN 'Laos'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_KH'
  THEN 'Cambodia'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_MM'
  THEN 'Myanmar'
  WHEN daily_deals_performance_data.global_entity_id = 'FP_JP'
  THEN 'Japan'
  END)
  AS country,
  --date_local,
  --week,
  daily_deals_performance_data.month,
  CASE
    WHEN daily_deals_performance_data.business_type IS NULL
    THEN 'restaurants'
    ELSE daily_deals_performance_data.business_type
  END AS business_type,
  daily_deals_performance_data.aaa_type,
  daily_deals_performance_data.gmv_class,
  monthly_vf_deal_free_item_eur,
  SUM(IFNULL(daily_all_valid_orders_1,0)) AS monthly_all_valid_orders_1,
  SUM(IFNULL(daily_normal_delivery_orders_2,0)) AS monthly_normal_delivery_orders_2,
  SUM(IFNULL(daily_normal_pickup_orders_3,0)) AS monthly_normal_pickup_orders_3,
  SUM(IFNULL(daily_corporate_delivery_orders_4,0)) AS monthly_corporate_delivery_orders_4,
  SUM(IFNULL(daily_corporate_pickup_orders_5,0)) AS monthly_corporate_pickup_orders_5,
  SUM(IFNULL(daily_normal_pro_orders_6,0)) AS monthly_normal_pro_orders_6,
  SUM(IFNULL(daily_all_vf_orders_1,0)) AS monthly_all_vf_orders_1,
  SUM(IFNULL(daily_vf_normal_delivery_orders_2,0)) AS monthly_vf_normal_delivery_orders_2,
  SUM(IFNULL(daily_vf_normal_pickup_orders_3,0)) AS monthly_vf_normal_pickup_orders_3,
  SUM(IFNULL(daily_vf_corporate_delivery_orders_4,0)) AS monthly_vf_corporate_delivery_orders_4,
  SUM(IFNULL(daily_vf_corporate_pickup_orders_5,0)) AS monthly_vf_corporate_pickup_orders_5,
  SUM(IFNULL(daily_vf_normal_pro_orders_6,0)) AS monthly_vf_normal_pro_orders_6,
  SUM(IFNULL(daily_vf_orders_discount_percentage,0)) AS monthly_vf_orders_discount_percentage,
  SUM(IFNULL(daily_vf_orders_discount_amount,0)) AS monthly_vf_orders_discount_amount,
  SUM(IFNULL(daily_vf_orders_free_delivery,0)) AS monthly_vf_orders_free_delivery,
  SUM(IFNULL(daily_vf_orders_free_item,0)) AS monthly_vf_orders_free_item,
  SUM(IFNULL(daily_all_gmv_eur,0)) AS monthly_all_gmv_eur,
  SUM(IFNULL(daily_all_gfv_eur_1,0)) AS monthly_all_gfv_eur_1,
  SUM(IFNULL(daily_normal_delivery_all_gfv_eur_2,0)) AS monthly_normal_delivery_all_gfv_eur_2,
  SUM(IFNULL(daily_normal_pickup_all_gfv_eur_3,0)) AS monthly_normal_pickup_all_gfv_eur_3,
  SUM(IFNULL(daily_corporate_delivery_all_gfv_eur_4,0)) AS monthly_corporate_delivery_all_gfv_eur_4,
  SUM(IFNULL(daily_corporate_pickup_all_gfv_eur_5,0)) AS monthly_corporate_pickup_all_gfv_eur_5,
  SUM(IFNULL(daily_normal_pro_all_gfv_eur_6,0)) AS monthly_normal_pro_all_gfv_eur_6,
  SUM(IFNULL(daily_vf_gfv_eur_1,0)) AS monthly_vf_gfv_eur_1,
  SUM(IFNULL(daily_normal_delivery_vf_gfv_eur_2,0)) AS monthly_normal_delivery_vf_gfv_eur_2,
  SUM(IFNULL(daily_normal_pickup_vf_gfv_eur_3,0)) AS monthly_normal_pickup_vf_gfv_eur_3,
  SUM(IFNULL(daily_corporate_delivery_vf_gfv_eur_4,0)) AS monthly_corporate_delivery_vf_gfv_eur_4,
  SUM(IFNULL(daily_corporate_pickup_vf_gfv_eur_5,0)) AS monthly_corporate_pickup_vf_gfv_eur_5,
  SUM(IFNULL(daily_normal_pro_vf_gfv_eur_6,0)) AS monthly_normal_pro_vf_gfv_eur_6,
  SUM(IFNULL(daily_vf_discount_amount_gfv_eur_1,0)) AS monthly_vf_discount_amount_gfv_eur_1,
  SUM(IFNULL(daily_normal_delivery_vf_discount_amount_gfv_eur_2,0)) AS monthly_normal_delivery_vf_discount_amount_gfv_eur_2,
  SUM(IFNULL(daily_normal_pickup_vf_discount_amount_gfv_eur_3,0)) AS monthly_normal_pickup_vf_discount_amount_gfv_eur_3,
  SUM(IFNULL(daily_corporate_delivery_vf_discount_amount_gfv_eur_4,0)) AS monthly_corporate_delivery_vf_discount_amount_gfv_eur_4,
  SUM(IFNULL(daily_corporate_pickup_vf_discount_amount_gfv_eur_5,0)) AS monthly_corporate_pickup_vf_discount_amount_gfv_eur_5,
  SUM(IFNULL(daily_normal_pro_vf_discount_amount_gfv_eur_6,0)) AS monthly_normal_pro_vf_discount_amount_gfv_eur_6,
  SUM(IFNULL(daily_vf_discount_amount_deal_eur_1,0)) AS monthly_vf_discount_amount_deal_eur_1,
  SUM(IFNULL(daily_normal_delivery_vf_discount_amount_deal_eur_2,0)) AS monthly_normal_delivery_vf_discount_amount_deal_eur_2,
  SUM(IFNULL(daily_normal_pickup_vf_discount_amount_deal_eur_3,0)) AS monthly_normal_pickup_vf_discount_amount_deal_eur_3,
  SUM(IFNULL(daily_corporate_delivery_vf_discount_amount_deal_eur_4,0)) AS monthly_corporate_delivery_vf_discount_amount_deal_eur_4,
  SUM(IFNULL(daily_corporate_pickup_vf_discount_amount_deal_eur_5,0)) AS monthly_corporate_pickup_vf_discount_amount_deal_eur_5,
  SUM(IFNULL(daily_normal_pro_vf_discount_amount_deal_eur_6,0)) AS monthly_normal_pro_vf_discount_amount_deal_eur_6,
  SUM(IFNULL(daily_vf_deal_value_eur_1,0)) AS monthly_vf_deal_value_eur_1,
  SUM(IFNULL(daily_normal_delivery_vf_deal_value_eur_2,0)) AS monthly_normal_delivery_vf_deal_value_eur_2,
  SUM(IFNULL(daily_normal_pickup_vf_deal_value_eur_3,0)) AS monthly_normal_pickup_vf_deal_value_eur_3,
  SUM(IFNULL(daily_corporate_delivery_vf_deal_value_eur_4,0)) AS monthly_corporate_delivery_vf_deal_value_eur_4,
  SUM(IFNULL(daily_corporate_pickup_vf_deal_value_eur_5,0)) AS monthly_corporate_pickup_vf_deal_value_eur_5,
  SUM(IFNULL(daily_normal_pro_vf_deal_value_eur_6,0)) AS monthly_normal_pro_vf_deal_value_eur_6,
  SUM(IFNULL(daily_fp_funded_value_eur_1,0)) AS monthly_fp_funded_value_eur_1,
  SUM(IFNULL(daily_normal_delivery_fp_funded_value_eur_2,0)) AS monthly_normal_delivery_fp_funded_value_eur_2,
  SUM(IFNULL(daily_normal_pickup_fp_funded_value_eur_3,0)) AS monthly_normal_pickup_fp_funded_value_eur_3,
  SUM(IFNULL(daily_corporate_delivery_fp_funded_value_eur_4,0)) AS monthly_corporate_delivery_fp_funded_value_eur_4,
  SUM(IFNULL(daily_corporate_pickup_fp_funded_value_eur_5,0)) AS monthly_corporate_pickup_fp_funded_value_eur_5,
  SUM(IFNULL(daily_normal_pro_fp_funded_value_eur_6,0)) AS monthly_normal_pro_fp_funded_value_eur_6,
  SUM(IFNULL(daily_discount_gfv_eur_1,0)) AS monthly_discount_gfv_eur_1,
  SUM(IFNULL(daily_normal_delivery_discount_gfv_eur_2,0)) AS monthly_normal_delivery_discount_gfv_eur_2,
  SUM(IFNULL(daily_normal_pickup_discount_gfv_eur_3,0)) AS monthly_normal_pickup_discount_gfv_eur_3,
  SUM(IFNULL(daily_corporate_delivery_discount_gfv_eur_4,0)) AS monthly_corporate_delivery_discount_gfv_eur_4,
  SUM(IFNULL(daily_corporate_pickup_discount_gfv_eur_5,0)) AS monthly_corporate_pickup_discount_gfv_eur_5,
  SUM(IFNULL(daily_normal_pro_discount_gfv_eur_6,0)) AS monthly_normal_pro_discount_gfv_eur_6,
  
  /*Deal Day*/
  SUM(
    CASE
      WHEN is_corporate_delivery_vf_deal_day OR is_corporate_pickup_vf_deal_day
           OR is_normal_delivery_vf_deal_day OR is_normal_pickup_vf_deal_day 
           OR is_pro_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_all_valid_orders_1,0),IFNULL(daily_all_valid_orders_1,0))
    END
  ) AS monthly_deal_day_all_valid_orders_1,
  SUM(
    CASE
      WHEN is_normal_delivery_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_normal_delivery_orders_2,0),IFNULL(daily_normal_delivery_orders_2,0))
    END
  ) AS monthly_deal_day_normal_delivery_orders_2,
  SUM(
    CASE
      WHEN is_normal_pickup_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_normal_pickup_orders_3,0),IFNULL(daily_normal_pickup_orders_3,0))
    END
  ) AS monthly_deal_day_normal_pickup_orders_3,
  SUM(
    CASE
      WHEN is_corporate_delivery_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_corporate_delivery_orders_4,0),IFNULL(daily_corporate_delivery_orders_4,0))
    END
  ) AS monthly_deal_day_corporate_delivery_orders_4,
  SUM(
    CASE
      WHEN is_corporate_pickup_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_corporate_pickup_orders_5,0),IFNULL(daily_corporate_pickup_orders_5,0))
    END
  ) AS monthly_deal_day_corporate_pickup_orders_5,
  SUM(
    CASE
      WHEN is_pro_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_normal_pro_orders_6,0),IFNULL(daily_normal_pro_orders_6,0))
    END
  ) AS monthly_deal_day_normal_pro_orders_6,
  SUM(
    CASE
      WHEN is_corporate_delivery_vf_deal_day OR is_corporate_pickup_vf_deal_day
           OR is_normal_delivery_vf_deal_day OR is_normal_pickup_vf_deal_day 
           OR is_pro_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_all_vf_orders_1,0),IFNULL(daily_all_vf_orders_1,0))
    END
  ) AS monthly_deal_day_all_vf_orders_1,
  SUM(
    CASE
      WHEN is_normal_delivery_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_normal_delivery_vf_orders_2,0),IFNULL(daily_vf_normal_delivery_orders_2,0))
    END
  ) AS monthly_deal_day_normal_delivery_vf_orders_2,
  SUM(
    CASE
      WHEN is_normal_pickup_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_normal_pickup_vf_orders_3,0),IFNULL(daily_vf_normal_pickup_orders_3,0))
    END
  ) AS monthly_deal_day_normal_pickup_vf_orders_3,
  SUM(
    CASE
      WHEN is_corporate_delivery_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_corporate_delivery_vf_orders_4,0),IFNULL(daily_vf_corporate_delivery_orders_4,0))
    END
  ) AS monthly_deal_day_corporate_delivery_vf_orders_4,
  SUM(
    CASE
      WHEN is_corporate_pickup_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_corporate_pickup_vf_orders_5,0),IFNULL(daily_vf_corporate_pickup_orders_5,0))
    END
  ) AS monthly_deal_day_corporate_pickup_vf_orders_5,
  SUM(
    CASE
      WHEN is_pro_vf_deal_day
      THEN GREATEST(IFNULL(daily_deal_day_normal_pro_vf_orders_6,0),IFNULL(daily_vf_normal_pro_orders_6,0))
    END
  ) AS monthly_deal_day_normal_pro_vf_orders_6,
  
  /*Vendor Count Metrics*/
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_day OR is_corporate_pickup_vf_deal_day
           OR is_normal_delivery_vf_deal_day OR is_normal_pickup_vf_deal_day 
           OR is_pro_vf_deal_day OR is_vf_voucher_deal_day
           OR is_vf_discount_deal_day OR daily_all_vf_orders_1 > 0
      THEN vendor_code
    END
   ) AS monthly_all_vf_vendor_count_1,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_delivery_vf_deal_day OR daily_vf_normal_delivery_orders_2 > 0
      THEN vendor_code
    END
   ) AS monthly_normal_delivery_vf_vendor_count_2,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_pickup_vf_deal_day OR daily_vf_normal_pickup_orders_3 > 0
      THEN vendor_code
    END
   ) AS monthly_normal_pickup_vf_vendor_count_3,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_day OR daily_vf_corporate_delivery_orders_4 > 0
      THEN vendor_code
    END
  ) AS monthly_corporate_delivery_vf_vendor_count_4,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_pickup_vf_deal_day OR daily_vf_corporate_pickup_orders_5 > 0
      THEN vendor_code
    END
  ) AS monthly_corporate_pickup_vf_vendor_count_5,
  COUNT(DISTINCT
    CASE
      WHEN is_pro_vf_deal_day OR daily_vf_normal_pro_orders_6 > 0
      THEN vendor_code
    END
  ) AS monthly_normal_pro_vf_vendor_count_6,
  COUNT(DISTINCT
    CASE
      WHEN is_daily_active OR daily_all_valid_orders_1 > 0
      THEN vendor_code
    END
  ) AS monthly_active_vendors,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_cofunded OR is_corporate_pickup_vf_deal_cofunded
           OR is_normal_delivery_vf_deal_cofunded OR is_normal_pickup_vf_deal_cofunded
           OR is_pro_vf_deal_cofunded OR is_vf_discount_deal_cofunded OR is_vf_voucher_deal_cofunded
      THEN vendor_code
    END
   ) AS monthly_all_cofunded_vf_vendor_count_1,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_delivery_vf_deal_cofunded
      THEN vendor_code
    END
   ) AS monthly_normal_delivery_cofunded_vf_vendor_count_2,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_pickup_vf_deal_cofunded
      THEN vendor_code
    END
   ) AS monthly_normal_pickup_cofunded_vf_vendor_count_3,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_cofunded
      THEN vendor_code
    END
  ) AS monthly_corporate_delivery_cofunded_vf_vendor_count_4,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_pickup_vf_deal_cofunded
      THEN vendor_code
    END
  ) AS monthly_corporate_pickup_cofunded_vf_vendor_count_5,
  COUNT(DISTINCT
    CASE
      WHEN is_pro_vf_deal_cofunded
      THEN vendor_code
    END
  ) AS monthly_normal_pro_cofunded_vf_vendor_count_6,
  
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type = 'percentage' OR is_corporate_pickup_vf_deal_type = 'percentage'
        OR is_normal_delivery_vf_deal_type = 'percentage' OR is_normal_pickup_vf_deal_type = 'percentage'
        OR is_pro_vf_deal_type = 'percentage' OR is_vf_discount_deal_type = 'percentage'
        OR is_vf_voucher_deal_type = 'percentage' OR daily_vf_orders_discount_percentage > 0
      THEN vendor_code
    END
   ) AS monthly_all_vf_discount_percentage_vendor_count_1,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_delivery_vf_deal_type = 'percentage'
      THEN vendor_code
    END
   ) AS monthly_normal_delivery_vf_discount_percentage_vendor_count_2,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_pickup_vf_deal_type = 'percentage'
      THEN vendor_code
    END
   ) AS monthly_normal_pickup_vf_discount_percentage_vendor_count_3,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type = 'percentage'
      THEN vendor_code
    END
  ) AS monthly_corporate_delivery_vf_discount_percentage_vendor_count_4,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_pickup_vf_deal_type = 'percentage'
      THEN vendor_code
    END
  ) AS monthly_corporate_pickup_vf_discount_percentage_vendor_count_5,
  COUNT(DISTINCT
    CASE
      WHEN is_pro_vf_deal_type = 'percentage'
      THEN vendor_code
    END
  ) AS monthly_normal_pro_vf_discount_percentage_vendor_count_6,
  
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type = 'amount' OR is_corporate_pickup_vf_deal_type = 'amount'
        OR is_normal_delivery_vf_deal_type = 'amount' OR is_normal_pickup_vf_deal_type = 'amount'
        OR is_pro_vf_deal_type = 'amount' OR is_vf_discount_deal_type = 'amount'
        OR is_vf_voucher_deal_type = 'amount' OR daily_vf_orders_discount_amount > 0
      THEN vendor_code
    END
   ) AS monthly_all_vf_amount_vendor_count_1,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_delivery_vf_deal_type = 'amount'
      THEN vendor_code
    END
   ) AS monthly_normal_delivery_vf_amount_vendor_count_2,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_pickup_vf_deal_type = 'amount'
      THEN vendor_code
    END
   ) AS monthly_normal_pickup_vf_amount_vendor_count_3,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type = 'amount'
      THEN vendor_code
    END
  ) AS monthly_corporate_delivery_vf_amount_vendor_count_4,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_pickup_vf_deal_type = 'amount'
      THEN vendor_code
    END
  ) AS monthly_corporate_pickup_vf_amount_vendor_count_5,
  COUNT(DISTINCT
    CASE
      WHEN is_pro_vf_deal_type = 'amount'
      THEN vendor_code
    END
  ) AS monthly_normal_pro_vf_amount_vendor_count_6,
  
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type IN ('free-delivery','delivery_fee') OR is_corporate_pickup_vf_deal_type  IN ('free-delivery','delivery_fee')
        OR is_normal_delivery_vf_deal_type  IN ('free-delivery','delivery_fee') OR is_normal_pickup_vf_deal_type  IN ('free-delivery','delivery_fee')
        OR is_pro_vf_deal_type  IN ('free-delivery','delivery_fee') OR is_vf_discount_deal_type  IN ('free-delivery','delivery_fee')
        OR is_vf_voucher_deal_type IN ('free-delivery','delivery_fee') OR daily_vf_orders_free_delivery > 0
      THEN vendor_code
    END
   ) AS monthly_all_vf_free_delivery_vendor_count_1,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_delivery_vf_deal_type IN ('free-delivery','delivery_fee')
      THEN vendor_code
    END
   ) AS monthly_normal_delivery_vf_free_delivery_vendor_count_2,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_pickup_vf_deal_type IN ('free-delivery','delivery_fee')
      THEN vendor_code
    END
   ) AS monthly_normal_pickup_vf_free_delivery_vendor_count_3,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type IN ('free-delivery','delivery_fee')
      THEN vendor_code
    END
  ) AS monthly_corporate_delivery_vf_free_delivery_vendor_count_4,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_pickup_vf_deal_type IN ('free-delivery','delivery_fee')
      THEN vendor_code
    END
  ) AS monthly_corporate_pickup_vf_free_delivery_vendor_count_5,
  COUNT(DISTINCT
    CASE
      WHEN is_pro_vf_deal_type IN ('free-delivery','delivery_fee')
      THEN vendor_code
    END
  ) AS monthly_normal_pro_vf_free_delivery_vendor_count_6,
  
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type = 'text_freegift' OR is_corporate_pickup_vf_deal_type = 'text_freegift'
        OR is_normal_delivery_vf_deal_type = 'text_freegift' OR is_normal_pickup_vf_deal_type = 'text_freegift'
        OR is_pro_vf_deal_type = 'text_freegift' OR is_vf_discount_deal_type = 'text_freegift'
        OR is_vf_voucher_deal_type = 'text_freegift' OR daily_vf_orders_free_item > 0
      THEN vendor_code
    END
   ) AS monthly_all_vf_free_item_vendor_count_1,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_delivery_vf_deal_type = 'text_freegift'
      THEN vendor_code
    END
   ) AS monthly_normal_delivery_vf_free_item_vendor_count_2,
  COUNT(DISTINCT
    CASE
      WHEN is_normal_pickup_vf_deal_type = 'text_freegift'
      THEN vendor_code
    END
   ) AS monthly_normal_pickup_vf_free_item_vendor_count_3,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_delivery_vf_deal_type = 'text_freegift'
      THEN vendor_code
    END
  ) AS monthly_corporate_delivery_vf_free_item_vendor_count_4,
  COUNT(DISTINCT
    CASE
      WHEN is_corporate_pickup_vf_deal_type = 'text_freegift'
      THEN vendor_code
    END
  ) AS monthly_corporate_pickup_vf_free_item_vendor_count_5,
  COUNT(DISTINCT
    CASE
      WHEN is_pro_vf_deal_type = 'text_freegift'
      THEN vendor_code
    END
  ) AS monthly_normal_pro_vf_free_item_vendor_count_6
FROM pandata_ap_commercial.daily_deals_performance_data
LEFT JOIN free_item_eur
       ON free_item_eur.global_entity_id = daily_deals_performance_data.global_entity_id
      AND free_item_eur.month = daily_deals_performance_data.month
      AND free_item_eur.business_type = daily_deals_performance_data.business_type
      AND free_item_eur.aaa_type = daily_deals_performance_data.aaa_type
      AND free_item_eur.gmv_class = daily_deals_performance_data.gmv_class
GROUP BY 1,2,3,4,5,6,7
