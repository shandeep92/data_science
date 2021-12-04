/* Output gives me number of unique customers, number of orders, total gmv for every country for the last 4 months */

WITH customer_orders AS (
    SELECT 
        global_entity_id                                                                           AS global_entity_id,                 
        region                                                                                     AS region,
        country_name                                                                               AS country,
        warehouse.city                                                                             AS city,
        warehouse.warehouse_id                                                                     AS warehouse_id,
        warehouse.warehouse_name                                                                   AS warehouse_name,
        warehouse.store_name                                                                       AS store_name,
        order_id                                                                                   AS order_id,
        analytical_customer_id                                                                     AS customer_id,
        DATE(order_placed_localtime_at)                                                            AS order_date,
        order_value_euro.amt_gmv_eur                                                               AS gmv_eur,
        delivery_time_minutes                                                                      AS delivery_time_minutes
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` 
    -- Date filters
    WHERE DATE(order_placed_localtime_at) <= CURRENT_DATE()
    AND DATE(order_placed_localtime_at) >  DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)                --- 2 months ago till this month
    AND DATE(order_placed_localtime_at) <= CURRENT_DATE()
    -- Filters
    AND is_dmart
    AND is_sent
    AND is_failed = FALSE 
    AND is_cancelled = FALSE),

customer_orders_agg AS (
	SELECT 
	    FORMAT_DATE('%Y%m',DATE_TRUNC(co.order_date, MONTH))                                        AS order_month,
	    -- Categorical Variables
	    co.global_entity_id                                                                         AS global_entity_id,
	    co.country                                                                                  AS country,
	    --co.city                                                                                   AS city,
	    -- Aggregation
	    COUNT(co.order_id)                                                                          AS number_of_orders,
	    COUNT(DISTINCT co.customer_id)                                                              AS number_of_unique_customers,
	    ROUND(SUM(co.gmv_eur),2)                                                                    AS gmv_eur,
	    ROUND(SUM(co.gmv_eur)/COUNT(co.order_id),2)                                                 AS gmv_eur_per_order,
	    ROUND(AVG(co.delivery_time_minutes),2)                                                      AS avg_delivery_time_mins
	FROM customer_orders AS co
	GROUP BY 1,2,3
	ORDER BY 1 DESC, 6 DESC, 7 DESC    -- Month DESC, customers DESC, GMV DESC
)

SELECT 
*, 
ROUND(SAFE_DIVIDE(gmv_eur, SUM(gmv_eur) OVER(PARTITION BY order_month)) * 100,2)                  AS gmv_perc_contribution_to_DH
FROM customer_orders_agg as coagg
ORDER BY 1 DESC, 6 DESC, 7 DESC

--------------------------------------------------------------------------------------------------------------------

/* MoM GMV growth from July 2021 to August 2021 - country level */

WITH customer_orders AS (
SELECT
  global_entity_id AS global_entity_id,
  region AS region,
  country_name AS country,
  ROUND(SUM(IF(DATE(order_placed_localtime_at) BETWEEN '2021-06-01'AND '2021-06-30', order_value_euro.amt_gmv_eur, 0)),2) AS gmv_eur_june,
  ROUND(SUM(IF(DATE(order_placed_localtime_at) BETWEEN '2021-07-01'AND '2021-07-31', order_value_euro.amt_gmv_eur, 0)),2) AS gmv_eur_july,
  ROUND(SUM(IF(DATE(order_placed_localtime_at) BETWEEN '2021-08-01'AND '2021-08-30', order_value_euro.amt_gmv_eur, 0)),2) AS gmv_eur_aug
FROM
  `fulfillment-dwh-production.cl_dmart.customer_orders`
  -- Date filters
WHERE
  DATE(order_placed_localtime_at) <= CURRENT_DATE()
  -- Filters
  AND is_dmart
  AND is_sent
  AND is_failed = FALSE
  AND is_cancelled = FALSE
  GROUP BY 1,2,3
  ORDER BY 6 DESC
)

SELECT *, ROUND((SAFE_DIVIDE(gmv_eur_aug,gmv_eur_july)-1) * 100,1) AS MoM_growth
FROM customer_orders AS co