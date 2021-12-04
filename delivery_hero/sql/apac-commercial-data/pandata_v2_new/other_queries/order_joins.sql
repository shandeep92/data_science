SELECT
  o.global_entity_id,
  o.country_name AS country,
  o.vendor_code AS vendor_code,
  DATE_TRUNC(DATE(o.created_at_local), MONTH) AS order_date,
  SUM(CASE WHEN o.is_failed_order_vendor THEN 1 ELSE 0 END) AS failed_order_vendor
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` pd_orders_agg_accounting
       ON pd_orders_agg_accounting.uuid = o.uuid
      AND pd_orders_agg_accounting.created_date_utc <= CURRENT_DATE
CROSS JOIN UNNEST(accounting) as accounting
LEFT JOIN (
  SELECT
   *
  FROM `fulfillment-dwh-production.pandata_curated.lg_orders` lg
  WHERE lg.created_date_utc >= '2021-01-01'
    AND lg.created_date_utc <= CURRENT_DATE
) lg
       ON o.global_entity_id = lg.global_entity_id
      AND o.code = lg.code
LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` AS pandora_pd_orders_agg_jkr_deals
       ON o.global_entity_id = pandora_pd_orders_agg_jkr_deals.global_entity_id
      AND o.uuid = pandora_pd_orders_agg_jkr_deals.uuid
      AND pandora_pd_orders_agg_jkr_deals.created_date_local <= CURRENT_DATE
LEFT JOIN `fulfillment-dwh-production.pandata_report.order_commissions` oc
       ON o.global_entity_id = oc.global_entity_id
      AND o.code = oc.order_code
      AND oc.order_created_date_utc <= CURRENT_DATE
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_status_flows` AS orders_agg_status_flows
       ON o.global_entity_id = orders_agg_status_flows.global_entity_id
      AND o.uuid = orders_agg_status_flows.uuid
      AND orders_agg_status_flows.created_date_utc <= CURRENT_DATE
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_cp_orders` pd_orders_agg_cp_orders
       ON pd_orders_agg_cp_orders.uuid = o.uuid
      AND pd_orders_agg_cp_orders.created_date_utc <= CURRENT_DATE
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts do
       on do.global_entity_id = o.global_entity_id
      AND do.uuid = o.uuid
      AND do.created_date_utc <= CURRENT_DATE
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers pd_orders_agg_vouchers
       ON pd_orders_agg_vouchers.uuid = o.uuid
      AND pd_orders_agg_vouchers.created_date_utc <= CURRENT_DATE
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts discount
       ON do.pd_discount_uuid = discount.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` countries
       ON o.global_entity_id = countries.global_entity_id
LEFT JOIN (
  SELECT
   *
  FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates`
  WHERE fx_rate_date <= CURRENT_DATE
) fx_rates
       ON fx_rates.currency_code_iso = countries.currency_code_iso
      AND fx_rates.fx_rate_date = DATE(o.ordered_at_local)
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_customers_agg_orders` pd_customers_agg_orders
       ON pd_customers_agg_orders.uuid = o.pd_customer_uuid
WHERE o.created_date_utc <= CURRENT_DATE
  AND DATE(o.created_date_local) >= '2021-01-01'
  AND NOT o.is_test_order
GROUP BY 1,2,3,4
