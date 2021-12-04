/*VF Deal Performance*/
SELECT
  o.global_entity_id,
  o.country_name AS country,
  DATE_TRUNC(DATE(o.created_at_local), MONTH) AS order_month,
  SAFE_DIVIDE(
    (SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND do.discount.attributions_foodpanda_ratio < 100,
    do.discount.discount_amount_eur * (100 - do.discount.attributions_foodpanda_ratio)/100, 0))
    +
    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND vo.voucher.current_foodpanda_ratio < 100,
    vo.voucher.value_eur * (100 - vo.voucher.current_foodpanda_ratio)/100, NULL))
    ),
    SUM(IF(o.is_valid_order, pd_orders_agg_accounting.gmv_eur, 0))
  ) AS vf_perc_over_gmv,
  SUM(IF(o.is_valid_order, pd_orders_agg_accounting.gmv_eur, 0)) AS gmv_eur,
  (
  SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND do.discount.attributions_foodpanda_ratio < 100,
  do.discount.discount_amount_eur * (100 - do.discount.attributions_foodpanda_ratio)/100, NULL))
  + SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND vo.voucher.current_foodpanda_ratio < 100,
  vo.voucher.value_eur * (100 - vo.voucher.current_foodpanda_ratio)/100, NULL))
  ) AS vf_deal_value_eur
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` pd_orders_agg_accounting
       ON pd_orders_agg_accounting.uuid = o.uuid
      AND pd_orders_agg_accounting.created_date_utc <= CURRENT_DATE
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts do
       on do.global_entity_id = o.global_entity_id
      AND do.uuid = o.uuid
      AND do.created_date_utc <= CURRENT_DATE
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers vo
       ON vo.uuid = o.uuid
      AND vo.created_date_utc <= CURRENT_DATE
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
       ON pd_vendors_agg_business_types.vendor_code = o.vendor_code
      AND pd_vendors_agg_business_types.global_entity_id = o.global_entity_id
WHERE o.created_date_utc <= CURRENT_DATE
  AND DATE(o.created_date_local) >= '2021-04-01'
  AND DATE(o.created_date_local) < '2021-07-01'
  AND NOT o.is_test_order
  AND o.is_gross_order
  AND o.global_entity_id LIKE 'FP_%'
  AND o.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
  AND ((o.global_entity_id != 'FP_PK' AND pd_vendors_agg_business_types.business_type_apac IN ('restaurants'))
      OR (o.global_entity_id = 'FP_PK' AND pd_vendors_agg_business_types.is_restaurants AND NOT pd_vendors_agg_business_types.is_home_based_kitchen))
GROUP BY 1, 2, 3
ORDER BY 1, 3
