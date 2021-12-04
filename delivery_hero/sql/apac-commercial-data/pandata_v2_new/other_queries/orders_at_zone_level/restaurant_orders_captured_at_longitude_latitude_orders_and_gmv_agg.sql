-- This query breaks down order details for restaurants on a zone level (which includes longitude and latitude data)

WITH orders AS (
    SELECT 
    o.global_entity_id                                                                           AS global_entity_id,                 
    o.country_name                                                                               AS country,
    o.latitude                                                                                   AS latitude,
    o.longitude                                                                                  As longitude,
    o.vendor_code                                                                                AS vendor_code,
    o.vendor_name                                                                                AS vendor_name,
    date(o.created_at_utc)                                                                       AS order_date,
    o.uuid                                                                                       AS order_id,
    oagg.gmv_eur                                                                                 AS gmv_eur

 FROM  `fulfillment-dwh-production.pandata_curated.pd_orders`                    AS o 
 LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS oagg
      ON o.global_entity_id = oagg.global_entity_id
      AND o.uuid = oagg.uuid
---------------------------- Filters --------------------
 WHERE o.created_date_utc <= CURRENT_DATE()
 AND oagg.created_date_utc <= CURRENT_DATE()
 AND o.is_valid_order
 AND o.is_test_order = FALSE                                                               --  Exclude test orders
 AND o.created_date_utc > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
 AND o.created_date_utc <= CURRENT_DATE()
---
 AND o.global_entity_id LIKE 'FP_%'                                                        --  All foodpanda entities
 AND o.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')                                 --- Remove foodpanda Bulgaria, foodpanda Romania, foodpanda Germany

---
 ORDER BY 2 ASC, 3 DESC),

 location AS (
     SELECT
        DISTINCT v.global_entity_id                                                         AS global_entity_id,
        --v.pd_city_id                                                                      AS city_id,
        --v.location.city_id                                                                AS location_city_id,
        v.location.city                                                                     AS city,
        --v.location.latitude                                                                 AS latitude,
        --v.location.longitude                                                                AS longitude,
        lg_zones.lg_zone_id                                                                 AS zone_id,
        lg_zones.lg_zone_name                                                               AS zone_name,
        v.vendor_code                                                                       AS vendor_code,
        v.name                                                                              AS vendor_name,
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` lg 
       ON v.global_entity_id = lg.global_entity_id 
       AND v.vendor_code = lg.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` bt 
       ON v.global_entity_id = bt.global_entity_id 
      AND v.vendor_code = bt.vendor_code
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE v.global_entity_id LIKE 'FP_%'                                                        
AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')  
AND is_closest_point = TRUE
AND v.is_active
AND v.is_private = FALSE
AND v.is_test = FALSE
AND bt.is_restaurants          
 ),
/* final table */

final AS (
 SELECT 
    o.global_entity_id,
    l.city,
    o.latitude,
    o.longitude,
    l.zone_id,
    l.zone_name,
    o.vendor_code,
    o.vendor_name,
    o.order_date,
    o.order_id,
    o.gmv_eur 
 FROM orders  as o
 JOIN location AS l
   ON  o.global_entity_id = l.global_entity_id
  AND o.vendor_code = l.vendor_code
ORDER BY 1,2,6
)

SELECT 
    FORMAT_DATE('%Y%m',DATE_TRUNC(f.order_date , MONTH))                                    AS  year_month,
    f.global_entity_id                                                                      AS  global_entity_id,
    f.city                                                                                  AS  city,
    f.latitude                                                                              AS  latitude,
    f.longitude                                                                             AS  longitude,
    f.zone_id                                                                               AS  zone_id,
    f.zone_name                                                                             AS  zone_name,
    f.vendor_code                                                                           AS  vendor_code,
    f.vendor_name                                                                           AS  vendor_name, 
    COUNT(f.order_id)                                                                       AS  number_of_orders,
    SUM(f.gmv_eur)                                                                          AS  gmv_eur
FROM final as f
----- Choose your filters ----
/*
WHERE FORMAT_DATE('%Y%m',DATE_TRUNC(f.order_date , MONTH)) = '202107'
AND f.global_entity_id  = 'FP_JP'
AND f.city IN ('Tokyo','Osaka')
*/
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 10 DESC, 11 DESC
