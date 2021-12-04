SELECT 
 --------------------- Categories ------------------------
    o.global_entity_id                                                                          AS global_entity_id,                  
    o.country_name                                                                              AS country,
 ----------------------- Dates -----------------------------
    FORMAT_DATE('%Y%m',DATE_TRUNC(o.created_date_utc, MONTH))                                   AS year_month,
 ------------------------ Aggregated Columns --------------
    COUNT(DISTINCT o.pd_customer_id)                                                            AS number_of_unique_customers,
    CASE 
      WHEN country_name = 'Taiwan'      THEN 23570000 
      WHEN country_name = 'Malaysia'    THEN 31950000
      WHEN country_name = 'Philippines' THEN 108100000
      WHEN country_name = 'Thailand'    THEN 69630000 
      WHEN country_name = 'Pakistan'    THEN 216600000
      WHEN country_name = 'Singapore'   THEN 5700000 
      WHEN country_name = 'Hong Kong'   THEN 7507000
      WHEN country_name = 'Bangladesh'  THEN 163000000 
      WHEN country_name = 'Japan'       THEN 126300000
      WHEN country_name = 'Cambodia'    THEN 16490000
      WHEN country_name = 'Laos'        THEN 7275560 
      WHEN country_name = 'Myanmar'     THEN 54409800 END                                        AS population,
    COUNT(o.uuid)                                                                                AS number_of_orders,
    ROUND(sum(oagg.gmv_eur),2)                                                                   AS gmv_eur,
    ROUND(sum(oagg.gmv_eur)/COUNT(o.uuid),2)                                                     AS gmv_per_order,
    ROUND(AVG(o.delivery_time_in_minutes),2)                                                     AS average_delivery_time_in_minutes
 FROM  `fulfillment-dwh-production.pandata_curated.pd_orders`                    AS o 
 LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS oagg
      ON o.global_entity_id = oagg.global_entity_id
      AND o.uuid = oagg.uuid
---------------------------- Filters --------------------
 WHERE o.created_date_utc <= CURRENT_DATE()
 AND oagg.created_date_utc <= CURRENT_DATE()
 AND o.is_valid_order
 AND o.is_test_order = FALSE                                                               --  Exclude test orders
---
 AND o.created_date_utc > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)                       --  Data from 2 months ago till current month
 AND o.created_date_utc <= CURRENT_DATE()
---
 AND o.global_entity_id LIKE 'FP_%'                                                        --  All foodpanda entities
 AND o.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')                                 --- Remove foodpanda Bulgaria, foodpanda Romania, foodpanda Germany
 AND o.delivery_address_city IS NOT NULL
---
 GROUP BY 1,2,3
 ORDER BY 2 ASC, 3 DESC
