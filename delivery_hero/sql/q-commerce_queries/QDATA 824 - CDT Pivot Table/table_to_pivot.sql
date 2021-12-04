    /* Base table we want to work with in order to pivot accordingly 
    analytical_customer_id  | order_id  | order_place_at               |    sku    | product_name                      
    ---------------------------------------------------------------------------------------
     Vxq5vZwzV6GH-JFOGYb5KQ | 262732148 |  2020-04-19 04:22:17 UTC     |   903981  | Alban Laban Goat Full Fat Plastic Bottle 500 Ml  
     F2uEpWU6WbaTfmQX-jCu_Q | 262758343 |  2020-04-19 07:51:16 UTC     |   904038  |   Nadec Low Fat Yoghurt 170G
    .....
    
    */
    
    SELECT
    customer_orders.analytical_customer_id
    ,   customer_orders.order_id
    ,   customer_orders.order_placed_at
    ,   products.skus as sku
    ,   products.product_name
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` customer_orders
    JOIN UNNEST(items) AS items
    LEFT JOIN `fulfillment-dwh-production.cl_dmart.products` products
        ON customer_orders.global_entity_id = products.global_entity_id
        AND products.global_product_id = items.global_product_id
    WHERE customer_orders.global_entity_id = 'TB_KW' AND products.global_entity_id = "TB_KW"
    AND customer_orders.is_dmart IS TRUE AND products.is_dmart IS TRUE
    AND products.master_category_names.level_one = "Dairy / Chilled / Eggs"
    AND products.master_category_names.level_three = "Yoghurt"
    AND date_diff(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
    AND customer_orders.analytical_customer_id IS NOT NULL

    --------------------------------------------------
    /* Looking at number of customers and pivot table in the dataset */
    WITH cust_and_skus AS (
       SELECT
        customer_orders.analytical_customer_id
        ,   customer_orders.order_id
        ,   customer_orders.order_placed_at
        ,   products.skus as sku
        ,   products.product_name
        FROM `fulfillment-dwh-production.cl_dmart.customer_orders` customer_orders
        JOIN UNNEST(items) AS items
        LEFT JOIN `fulfillment-dwh-production.cl_dmart.products` products
               ON customer_orders.global_entity_id = products.global_entity_id
              AND products.global_product_id = items.global_product_id
        WHERE customer_orders.global_entity_id = 'TB_KW' AND products.global_entity_id = "TB_KW"
        AND customer_orders.is_dmart IS TRUE AND products.is_dmart IS TRUE
        AND products.master_category_names.level_one = "Dairy / Chilled / Eggs"
        AND products.master_category_names.level_three = "Yoghurt"
        AND date_diff(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
        AND customer_orders.analytical_customer_id IS NOT NULL
    )

    SELECT 
        COUNT(analytical_customer_id)              AS number_of_customers
        ,   COUNT(sku)                             AS number_of_skus
        ,   COUNT(DISTINCT analytical_customer_id) AS number_of_unique_customers
        ,   COUNT(DISTINCT sku)                    AS number_of_unique_skus
    FROM cust_and_skus

    -------------------------------------------------
    -- from_item: the table or subquery on which to perform a pivot operation 
    -- pivot_operator: the pivot operation to perform on a from item
    -- alias: an alias to use for an item in the query

/* 1st attempt */
    WITH pvt AS (
    SELECT
    customer_orders.analytical_customer_id
    ,   customer_orders.order_id
    ,   customer_orders.order_placed_at
    ,   products.skus  as sku
    --,   CASE WHEN products.skus IS NOT NULL THEN 1 ELSE 0 END AS sku_binary
    ,   products.product_name
    
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` customer_orders
    JOIN UNNEST(items) AS items
    LEFT JOIN `fulfillment-dwh-production.cl_dmart.products` products
        ON customer_orders.global_entity_id = products.global_entity_id
        AND products.global_product_id = items.global_product_id
    WHERE customer_orders.global_entity_id = 'TB_KW' AND products.global_entity_id = "TB_KW"
    AND customer_orders.is_dmart IS TRUE AND products.is_dmart IS TRUE
    AND products.master_category_names.level_one = "Dairy / Chilled / Eggs"
    AND products.master_category_names.level_three = "Yoghurt"
    AND date_diff(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
    AND customer_orders.analytical_customer_id IS NOT NULL
    AND analytical_customer_id = '--55GZciWzS5hC36tnUmDQ'
    
    --ORDER BY 1
    )
SELECT * EXCEPT (order_id, order_placed_at, product_name) FROM pvt 

PIVOT (
    -- aggregate
    COUNT(*) AS sku
    -- pivot column
    FOR sku IN ('902039','902044','902040')
) 

-----------------------------------------------------------------------

/* Using scripting but table too large*/

DECLARE skus STRING;
SET skus = (
  SELECT 
    CONCAT('("', STRING_AGG(DISTINCT skus, '", "'), '")'),
  FROM `fulfillment-dwh-production.cl_dmart.products`
);

EXECUTE IMMEDIATE format ("""
 SELECT * EXCEPT (order_id, order_placed_at, product_name)
 FROM (
    SELECT
    customer_orders.analytical_customer_id
    ,   customer_orders.order_id
    ,   customer_orders.order_placed_at
    ,   products.skus  as sku
    --,   CASE WHEN products.skus IS NOT NULL THEN 1 ELSE 0 END AS sku_binary
    ,   products.product_name
    
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` customer_orders
    JOIN UNNEST(items) AS items
    LEFT JOIN `fulfillment-dwh-production.cl_dmart.products` products
        ON customer_orders.global_entity_id = products.global_entity_id
        AND products.global_product_id = items.global_product_id
    WHERE customer_orders.global_entity_id = 'TB_KW' AND products.global_entity_id = "TB_KW"
    AND customer_orders.is_dmart IS TRUE AND products.is_dmart IS TRUE
    AND products.master_category_names.level_one = "Dairy / Chilled / Eggs"
    AND products.master_category_names.level_three = "Yoghurt"
    AND date_diff(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
    AND customer_orders.analytical_customer_id IS NOT NULL
    --AND analytical_customer_id = '--55GZciWzS5hC36tnUmDQ'
    --AND products.sku IN ('902039','902044','902040')
)

PIVOT 
(
    -- aggregate
    COUNT(*) AS sku
    -- pivot column
    FOR sku IN %s
)
""", skus);
    
-----------------------------------------------------------------------

/* Creating a temporary table */

BEGIN
CREATE TEMP TABLE `dh-darkstores-stg.dev_dmart.products_pivot_test` AS 
 SELECT
    customer_orders.analytical_customer_id
    ,   customer_orders.order_id
    ,   customer_orders.order_placed_at
    ,   products.skus  as sku
    --,   CASE WHEN products.skus IS NOT NULL THEN 1 ELSE 0 END AS sku_binary
    ,   products.product_name
    
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` customer_orders
    JOIN UNNEST(items) AS items
    LEFT JOIN `fulfillment-dwh-production.cl_dmart.products` products
        ON customer_orders.global_entity_id = products.global_entity_id
        AND products.global_product_id = items.global_product_id
    WHERE customer_orders.global_entity_id = 'TB_KW' AND products.global_entity_id = "TB_KW"
    AND customer_orders.is_dmart IS TRUE AND products.is_dmart IS TRUE
    AND products.master_category_names.level_one = "Dairy / Chilled / Eggs"
    AND products.master_category_names.level_three = "Yoghurt"
    AND date_diff(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
    AND customer_orders.analytical_customer_id IS NOT NULL
    --AND analytical_customer_id = '--55GZciWzS5hC36tnUmDQ'
    --AND products.sku IN ('902039','902044','902040')
    ;
    END;