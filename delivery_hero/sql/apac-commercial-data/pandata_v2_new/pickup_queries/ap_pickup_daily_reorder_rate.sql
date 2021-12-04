WITH first_pickup_order AS (
  SELECT
    pd_orders.global_entity_id,
    pd_orders.pd_customer_uuid,
    pd_orders.country_name,
    MIN(pd_orders.ordered_at_local) AS first_pickup_order_date        
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN  `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` AS marketing_pd_orders_agg_acquisition_dates
         ON marketing_pd_orders_agg_acquisition_dates.uuid  = pd_orders.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = pd_orders.global_entity_id
  WHERE expedition_type = 'pickup'
    AND is_gross_order
    AND is_valid_order
    AND is_first_valid_order_pickup
    AND DATE(pd_orders.created_date_utc) >= DATE_SUB(CURRENT_DATE, INTERVAL 13 month)
    AND DATE(pd_orders.ordered_at_local) >= DATE_SUB(CURRENT_DATE, INTERVAL 13 month)
    AND shared_countries.management_entity = 'Foodpanda APAC'
 group by 1,2,3
),

pre_reorder_rate AS (
  SELECT
    pd_orders.global_entity_id,
    pd_orders.pd_customer_uuid,
    pd_orders.country_name,
    pd_orders.ordered_at_local,
    DATE_DIFF(DATE(pd_orders.ordered_at_local), DATE(first_pickup_order_date), day) AS days_since_first_order,
    DATE_ADD(DATE(first_pickup_order_date), interval 7 day) AS attributed_date,
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` as pd_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = pd_orders.global_entity_id
  LEFT JOIN first_pickup_order  
         ON pd_orders.global_entity_id = first_pickup_order.global_entity_id 
        AND pd_orders.pd_customer_uuid = first_pickup_order.pd_customer_uuid
  WHERE expedition_type = 'pickup'
  AND is_gross_order
  AND is_valid_order
  AND DATE(pd_orders.created_date_utc) >= DATE_SUB(CURRENT_DATE, INTERVAL 13 month)
  AND shared_countries.management_entity = 'Foodpanda APAC'
),

reorder_rate AS (
  SELECT
    global_entity_id,
    country_name,
    attributed_date AS date, 
    COUNT(DISTINCT CASE WHEN days_since_first_order BETWEEN 1 AND 7 THEN pd_customer_uuid ELSE NULL END)/count(DISTINCT pd_customer_uuid) AS reorder_rate,
    COUNT(DISTINCT CASE WHEN days_since_first_order BETWEEN 1 AND 7 THEN pd_customer_uuid ELSE null END) AS new_customers_8_days_ago_who_reorder,
    COUNT(DISTINCT pd_customer_uuid) AS new_customers_8_days_ago
  FROM pre_reorder_rate      
GROUP BY 1,2,3
)

SELECT 
  * 
FROM reorder_rate
ORDER BY date DESC, global_entity_id
-- ap_pickup_daily_reorder_rate
