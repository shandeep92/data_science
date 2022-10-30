WITH products AS (
    SELECT global_entity_id
      , sku
      , master_category_names.level_one AS category
      , master_category_names.level_two AS subcategory
      , vi.global_product_id
   FROM `fulfillment-dwh-production.cl_dmart.products_v2` products
   LEFT JOIN UNNEST(warehouse_info) wi
   LEFT JOIN UNNEST(vendor_info) vi
   WHERE wi.is_dmart
   )
   ,

  base AS (
    SELECT orders.global_entity_id
      , items.sku
      , products.category AS category
      , products.subcategory  AS subcategory
      , items.product.brand_name AS brand
      , region
      , country_name AS country
      , items.product.product_name AS product
      , supplier.supplier_name AS supplier
      , DATE(orders.order_placed_localtime_at) AS order_date
      , DATE_TRUNC(DATE(orders.order_placed_localtime_at), WEEK(MONDAY)) AS order_week
      , warehouse.warehouse_id
      , warehouse.warehouse_name
      , warehouse.city
      , items.value_euro.unit_price_listed_eur
      , items.value_euro.unit_price_paid_eur
      , items.value_euro.cogs_eur
      , orders.analytical_customer_id
      , orders.order_id
      , CASE
          WHEN items.value_euro.unit_price_listed_eur > items.value_euro.unit_price_paid_eur
            THEN items.value_euro.unit_discount_amount_eur
            ELSE NULL
        END AS discount_amount
      , items.value_euro.total_supplier_funding_eur
      , CASE
          WHEN items.value_euro.unit_price_listed_eur = items.value_euro.unit_price_paid_eur
            THEN items.value_euro.unit_price_paid_eur
            ELSE NULL
        END AS full_price
      , items.qty_sold
      , items.value_euro.total_price_paid_eur AS gmv
      , items.value_euro.total_price_paid_net_eur AS net_retail_revenue
    FROM `fulfillment-dwh-production.cl_dmart.customer_orders_v2` orders
    LEFT JOIN UNNEST(items) AS items
    LEFT JOIN `fulfillment-dwh-production.cl_dmart._dim_supplier` supplier ON orders.global_entity_id = supplier.global_entity_id
      AND items.product.supplier_id = supplier.supplier_id
    LEFT JOIN products ON orders.global_entity_id = products.global_entity_id
      AND items.sku = products.sku
      AND items.global_product_id = products.global_product_id
    WHERE orders.is_sent
      AND is_dmart
      AND DATE(orders.order_placed_localtime_at) >= DATE_ADD(CURRENT_DATE(), INTERVAL -12 MONTH)
  )
,
calculations AS (
    SELECT region
    ,   global_entity_id
    ,   country
    ,   analytical_customer_id
    ,   order_date
    ,   order_id
    ,   category
    ,   subcategory
    ,   brand
    ,   product
    ,   sku
    ,   supplier
    ,   warehouse_id
    ,   warehouse_name
    ,   city
    ,   discount_amount * qty_sold AS gross_discount_value_before_supplier_funding
    ,   cogs_eur
    ,   total_supplier_funding_eur
    ,   unit_price_listed_eur * qty_sold AS grv_before_discount
    ,   gmv
    ,   net_retail_revenue
    ,   CASE WHEN discount_amount IS NOT NULL THEN 1 ELSE 0 END AS is_discount_order
    ,   CASE WHEN discount_amount IS NOT NULL THEN qty_sold END AS discount_qty_sold
    ,   CASE WHEN full_price IS NOT NULL THEN qty_sold END AS full_price_qty_sold
    ,   CASE WHEN discount_amount IS NOT NULL THEN gmv END AS discount_gmv
    ,   CASE WHEN discount_amount IS NOT NULL THEN unit_price_listed_eur * qty_sold END AS grv_from_discount
    ,   CASE WHEN discount_amount IS NOT NULL THEN net_retail_revenue END AS discount_net_retail_revenue
    From base
    WHERE sku is not null
)
, aggregation_by_country AS (
SELECT  category
,   COUNT(CASE WHEN is_discount_order = 1 THEN order_id END) AS total_discount_orders
,   COUNT(order_id) AS total_orders
,   SAFE_DIVIDE(COUNT(CASE WHEN is_discount_order = 1 THEN order_id END), COUNT(order_id)) AS perc_discount_orders
,   COUNT(DISTINCT analytical_customer_id) AS total_unique_customers
,   SUM(total_supplier_funding_eur) AS supplier_funding
,   SUM(gross_discount_value_before_supplier_funding) AS discount_gmv
,   SUM(gmv) AS gmv
,   SAFE_DIVIDE(SUM(total_supplier_funding_eur), SUM(gross_discount_value_before_supplier_funding)) AS perc_supplier_funding
,   SAFE_DIVIDE(sum(gmv), COUNT(order_id)) AS avg_basket_size
,   COUNT(order_id)/COUNT(DISTINCT analytical_customer_id) AS frequency
FROM calculations
GROUP BY 1
)
SELECT category
,   perc_discount_orders * 100 AS perc_discount_orders
,   IFNULL(perc_supplier_funding,0) * 100 AS perc_supplier_funding
,   avg_basket_size
,   frequency
FROM aggregation_by_country
ORDER BY 2 DESC