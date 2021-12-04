/* Base query to work with in order to pivot */

SELECT
customer_orders.global_entity_id
,   customer_orders.analytical_customer_id
,   customer_orders.order_id
,   customer_orders.order_placed_at
,   items.sku
,   products_v2.sku
,   products_v2.product_name
FROM `fulfillment-dwh-production.cl_dmart.customer_orders` AS customer_orders, UNNEST(items) AS items
LEFT JOIN `fulfillment-dwh-production.cl_dmart.products_v2` AS products_v2 
        ON customer_orders.global_entity_id = products_v2.global_entity_id
        AND items.sku = products_v2.sku
WHERE DATE_DIFF(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
AND customer_orders.global_entity_id = 'TB_KW'
AND customer_orders.is_dmart
AND master_category_names.level_one = 'Dairy / Chilled / Eggs'
AND master_category_names.level_three = 'Yoghurt'
AND customer_orders.analytical_customer_id IS NOT NULL 
AND products_v2.sku IS NOT NULL

--------------------------------------------------------

/* Check for number of unique customers and skus - here 129 unique SKUs*/

WITH cust_and_skus AS (
    SELECT
    customer_orders.global_entity_id
    ,   customer_orders.analytical_customer_id
    ,   customer_orders.order_id
    ,   customer_orders.order_placed_at
    ,   items.sku
    --,   products_v2.sku
    ,   products_v2.product_name
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders` AS customer_orders, UNNEST(items) AS items
    LEFT JOIN `fulfillment-dwh-production.cl_dmart.products_v2` AS products_v2 
            ON customer_orders.global_entity_id = products_v2.global_entity_id
            AND items.sku = products_v2.sku
    WHERE DATE_DIFF(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
    AND customer_orders.global_entity_id = 'TB_KW'
    AND customer_orders.is_dmart
    AND master_category_names.level_one = 'Dairy / Chilled / Eggs'
    AND master_category_names.level_three = 'Yoghurt'
    AND customer_orders.analytical_customer_id IS NOT NULL 
    AND products_v2.sku IS NOT NULL
)

SELECT 
COUNT(analytical_customer_id)                  AS number_of_customers
    ,   COUNT(sku)                             AS number_of_skus
    ,   COUNT(DISTINCT analytical_customer_id) AS number_of_unique_customers
    ,   COUNT(DISTINCT sku)                    AS number_of_unique_skus
FROM cust_and_skus

-----------------------------------------------------------
/* Create a view in dh-darkstores-stg */

CREATE OR REPLACE VIEW `dh-darkstores-stg.dev_dmart.products_v2_pivot_test`
AS
SELECT
customer_orders.global_entity_id
,   customer_orders.analytical_customer_id
,   customer_orders.order_id
,   customer_orders.order_placed_at
,   items.sku
--,   products_v2.sku
,   products_v2.product_name
FROM `fulfillment-dwh-production.cl_dmart.customer_orders` AS customer_orders, UNNEST(items) AS items
LEFT JOIN `fulfillment-dwh-production.cl_dmart.products_v2` AS products_v2 
        ON customer_orders.global_entity_id = products_v2.global_entity_id
        AND items.sku = products_v2.sku
WHERE DATE_DIFF(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
AND customer_orders.global_entity_id = 'TB_KW'
AND customer_orders.is_dmart
AND master_category_names.level_one = 'Dairy / Chilled / Eggs'
AND master_category_names.level_three = 'Yoghurt'
AND customer_orders.analytical_customer_id IS NOT NULL 
AND products_v2.sku IS NOT NULL

----------------------------------------------------------------

/* Pivot table now from view */

DECLARE skus STRING;
SET skus = (
  SELECT 
    CONCAT('("', STRING_AGG(DISTINCT sku, '", "'), '")'),
  FROM `dh-darkstores-stg.dev_dmart.products_v2_pivot_test`
);

EXECUTE IMMEDIATE format ("""
 SELECT * EXCEPT (order_id, order_placed_at, product_name)
 FROM `dh-darkstores-stg.dev_dmart.products_v2_pivot_test`

PIVOT 
(
    -- aggregate
    COUNT(*) AS sku
    -- pivot column
    FOR sku IN %s
)
""", skus);

------------------
/* Final code */

DECLARE skus STRING;

CREATE TEMP TABLE customer_order
AS
SELECT
customer_orders.global_entity_id
,   customer_orders.analytical_customer_id
,   customer_orders.order_id
,   customer_orders.order_placed_at
,   items.sku
,   products_v2.product_name
FROM `fulfillment-dwh-production.cl_dmart.customer_orders` AS customer_orders, UNNEST(items) AS items
LEFT JOIN `fulfillment-dwh-production.cl_dmart.products_v2` AS products_v2 
        ON customer_orders.global_entity_id = products_v2.global_entity_id
        AND items.sku = products_v2.sku
WHERE DATE_DIFF(DATE(customer_orders.order_placed_at), CURRENT_DATE, MONTH) <= 6
AND customer_orders.global_entity_id = 'TB_KW'
AND customer_orders.is_dmart
AND master_category_names.level_one = 'Dairy / Chilled / Eggs'
AND master_category_names.level_three = 'Yoghurt'
AND customer_orders.analytical_customer_id IS NOT NULL 
AND products_v2.sku IS NOT NULL;

SET skus = (
  SELECT 
    CONCAT('("', STRING_AGG(DISTINCT sku, '", "'), '")'),
  FROM customer_order
);

EXECUTE IMMEDIATE format ("""
 SELECT * EXCEPT (order_id, order_placed_at, product_name)
 FROM customer_order

PIVOT 
(
    -- aggregate
    COUNT(*) AS sku
    -- pivot column
    FOR sku IN %s
)
