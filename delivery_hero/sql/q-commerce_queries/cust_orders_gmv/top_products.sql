/* Top products in every country for the last 4 montsh */
WITH products AS (
    SELECT 
        global_entity_id                                                                           AS global_entity_id,                 
        region                                                                                     AS region,
        country_name                                                                               AS country,
        items.sku                                                                                  AS sku_id,
        items.global_product_id                                                                    AS global_product_id,
        product.product_name                                                                       AS product_name,
        DATE(order_placed_localtime_at)                                                            AS order_date,
        order_value_euro.amt_gmv_eur                                                               AS gmv_eur,
        delivery_time_minutes                                                                      AS delivery_time_minutes
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` , UNNEST(items) AS items
    -- Date filters
    WHERE DATE(order_placed_localtime_at) <= CURRENT_DATE()
    AND DATE(order_placed_localtime_at) >  DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)                --- 2 months ago till this month
    AND DATE(order_placed_localtime_at) <= CURRENT_DATE()
    -- Filters
    AND is_dmart
    AND is_failed = FALSE 
    AND is_cancelled = FALSE)

 -- Top-selling products 
SELECT 
        FORMAT_DATE('%Y%m',DATE_TRUNC(p.order_date, MONTH))                                        AS order_month,
    -- Categorical Variables
        p.global_entity_id                                                                         AS global_entity_id,
        p.country                                                                                  AS country,
        --p.global_product_id                                                                      AS product_id,
        p.product_name                                                                             AS product_name,
        COUNT(*)                                                                                   AS number_of_orders
FROM products AS p
--WHERE p.global_entity_id = 'FP_SG'
GROUP BY 1,2,3,4
ORDER BY 1 DESC, 5 DESC