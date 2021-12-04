---------------------
-- Activation date --
---------------------
WITH salesforce_contract AS (
  SELECT
    a.owner_name,
    a.rdbms_id,
    a.sf_country_name AS country,
    SUBSTR(platform_performance_c.backend_id_c, 5) AS vendor_code,
    MAX(o.close_date) AS sf_activation_date,
    MAX(a.gmv_class) AS gmv_class,
  FROM salesforce.opportunity o
  LEFT JOIN pandata.sf_accounts a
         ON a.id = o.account_id
  LEFT JOIN salesforce.platform_performance_c
         ON platform_performance_c.account_c = a.id 
  WHERE o.stage_name = 'Closed Won'
    AND business_type_c IN ('New Business','Owner Change','Win Back','Legal Form Change')
    AND vendor_code IS NOT NULL
  GROUP BY 1,2,3,4
),

----------------
-- AAA Brands --
----------------
dh_sf_brands AS (
  SELECT
    a.rdbms_id,
    dim_vendors.chain_name,
    dim_vendors.chain_id,
    MIN(vendor_grade) AS vendor_grade
  FROM pandata.sf_accounts a 
  LEFT JOIN pandata.dim_vendors
         ON a.rdbms_id = dim_vendors.rdbms_id
        AND a.vendor_code = dim_vendors.vendor_code
  WHERE NOT a.is_marked_for_testing_training
    AND a.status = 'Active'
    AND dim_vendors.is_active = true
    AND dim_vendors.is_private = false
    AND dim_vendors.is_vendor_testing = false
    AND a.rdbms_id NOT IN (7,12,17,20)
  GROUP BY rdbms_id, chain_id, chain_name
  HAVING vendor_grade = "AAA"
),

foodora_sf_brands AS (
  SELECT 
    c.rdbms_id,
    dim_vendors.chain_name,
    dim_vendors.chain_id,
    IF(MAX(aaa) =1, "AAA", NULL) AS vendor_grade
  FROM pandata.dim_countries c
  LEFT JOIN il_backend_latest.v_salesforce_dim_accounts a
         ON a.rdbms_id = c.rdbms_id
  LEFT JOIN pandata.dim_vendors
         ON a.rdbms_id = dim_vendors.rdbms_id
        AND a.vendor_code = dim_vendors.vendor_code
  WHERE account_type = 'Partner Account'
    AND c.rdbms_id IN (7,12,17,20) --BD,PK,TH,PH
    AND a.account_status = 'Active'
    AND dim_vendors.is_active = true
    AND dim_vendors.is_private = false
    AND dim_vendors.is_vendor_testing = false
  GROUP BY rdbms_id, chain_id, chain_name
  HAVING vendor_grade = "AAA"
),

foodpanda_AAA_brands AS (
  SELECT
    *
  FROM dh_sf_brands
  WHERE chain_id IS NOT NULL
  UNION ALL
  SELECT
    *
  FROM foodora_sf_brands
  WHERE chain_id IS NOT NULL
),

-----------------------
-- foodpanda vendors --
-----------------------
vendors AS (
  SELECT
    dim_vendors.rdbms_id,
    dim_vendors.country_name AS country,
    dim_vendors.vendor_code,
    dim_vendors.id AS vendor_id,
    dim_vendors.vendor_name,
    dim_vendors.chain_id,
    dim_vendors.chain_name,
    dim_vendors.primary_cuisine_id,
    dim_vendors.primary_cuisine,
    dim_vendors.business_type,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE, COALESCE(DATE(sf_activation.sf_activation_date),dim_vendors.activation_date,NULL), DAY) <= 30 THEN "New vendor"
      ELSE "Existing vendor"
    END AS vendor_type,
    COALESCE(foodpanda_AAA_brands.vendor_grade, vendor_gmv_class.gmv_class, dim_vendors.sf_vendor_grade, sf_activation.gmv_class, 'D') AS gmv_class,
  FROM pandata.dim_vendors
  LEFT JOIN salesforce_contract AS sf_activation
         ON dim_vendors.rdbms_id = sf_activation.rdbms_id
        AND dim_vendors.vendor_code = sf_activation.vendor_code
  LEFT JOIN foodpanda_AAA_brands
        ON dim_vendors.rdbms_id = foodpanda_AAA_brands.rdbms_id
       AND dim_vendors.chain_id = foodpanda_AAA_brands.chain_id
  LEFT JOIN pandata_report.vendor_gmv_class
         ON dim_vendors.rdbms_id = vendor_gmv_class.rdbms_id
        AND dim_vendors.vendor_code = vendor_gmv_class.vendor_code
  WHERE is_active = true
    AND is_private = false
    AND is_vendor_testing = false
),

-----------------------
-- Cuisine coverage --
-----------------------
cuisine_coverage AS (
  SELECT
    vendors.rdbms_id,
    country,
    business_type,
    vendor_type,
    gmv_class,
    primary_cuisine_id,
    primary_cuisine,
    COUNT(DISTINCT vendor_code) AS vendor_count
  FROM vendors
  GROUP BY 1,2,3,4,5,6,7
),

----------------------------------
-- Food characteristic coverage --
----------------------------------
fc_coverage AS (
  SELECT
    vendors.rdbms_id,
    country,
    business_type,
    vendor_type,
    gmv_class,
    food_characteristic_id,
    food_characteristic_title,
    COUNT(DISTINCT vendor_code) AS vendor_count
  FROM vendors
  LEFT JOIN pandata.dim_vendor_food_characteristics
         ON vendors.rdbms_id = dim_vendor_food_characteristics.rdbms_id
        AND vendors.vendor_id = dim_vendor_food_characteristics.vendor_id
  WHERE dim_vendor_food_characteristics.is_active = true
  GROUP BY 1,2,3,4,5,6,7
),

-------------------
-- Order metrics --
-------------------
vendor_orders AS (
  SELECT
      rdbms_id,
      vendor_code,
      vendor_id,
      COUNT(DISTINCT id) AS order_count
    FROM pandata.fct_orders
    WHERE created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
      AND is_gross_order = true
      AND is_test_order = false
    GROUP by rdbms_id, vendor_code, vendor_id
),

vendor_cuisine AS (
    SELECT
      rdbms_id,
      country,
      vendor_code,
      vendor_id,
      vendor_name,
      business_type,
      vendor_type,
      gmv_class,
      primary_cuisine_id,
      primary_cuisine,
    FROM vendors
),

vendor_fc AS (
    SELECT
      dim_vendor_food_characteristics.rdbms_id,
      dim_vendor_food_characteristics.vendor_id,
      vendors.business_type,
      vendors.vendor_type,
      vendors.gmv_class,
      dim_vendor_food_characteristics.food_characteristic_id,
      dim_vendor_food_characteristics.food_characteristic_title
    FROM vendors
    LEFT JOIN pandata.dim_vendor_food_characteristics
           ON dim_vendor_food_characteristics.rdbms_id = vendors.rdbms_id
          AND dim_vendor_food_characteristics.vendor_id = vendors.vendor_id
    WHERE dim_vendor_food_characteristics.is_active = true
),


cuisine_orders AS (
  SELECT
    vendor_cuisine.rdbms_id,
    vendor_cuisine.business_type,
    vendor_cuisine.vendor_type,
    vendor_cuisine.gmv_class,
    vendor_cuisine.primary_cuisine_id,
    vendor_cuisine.primary_cuisine,
    SUM(vendor_orders.order_count) AS order_count
  FROM vendor_cuisine
  LEFT JOIN vendor_orders
         ON vendor_cuisine.rdbms_id = vendor_orders.rdbms_id
        AND vendor_cuisine.vendor_code = vendor_orders.vendor_code
  GROUP BY 1,2,3,4,5,6
),

fc_orders AS (
  SELECT
    vendor_fc.rdbms_id,
    vendor_fc.business_type,
    vendor_type,
    gmv_class,
    vendor_fc.food_characteristic_id,
    vendor_fc.food_characteristic_title,
    SUM(vendor_orders.order_count) AS order_count
  FROM vendor_fc
  LEFT JOIN vendor_orders
         ON vendor_fc.rdbms_id = vendor_orders.rdbms_id
        AND vendor_fc.vendor_id = vendor_orders.vendor_id
  GROUP BY 1,2,3,4,5,6
),

-----------------
-- CR3 metrics --
-----------------
cr_metrics AS (
  SELECT
    dim_countries.rdbms_id,
    country,
    CAST(vendor_id AS INT64) AS vendor_id,
    SUM(count_of_checkout_loaded) AS CR4_count,
    SUM(count_of_shop_menu_loaded) AS CR3_count,
  FROM pandata_ap_product_external.vendor_level_session_metrics
  LEFT JOIN pandata.dim_countries
         ON vendor_level_session_metrics.country = dim_countries.common_name
  WHERE date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
  GROUP BY rdbms_id, country, vendor_id
),

cuisine_cr3 AS (
  SELECT
    vendor_cuisine.rdbms_id,
    vendor_cuisine.business_type,
    vendor_type,
    gmv_class,
    vendor_cuisine.primary_cuisine_id,
    vendor_cuisine.primary_cuisine,
    SUM(CR3_count) AS cr3_count,
    SUM(CR4_count) AS cr4_count
  FROM vendor_cuisine
  LEFT JOIN cr_metrics
         ON vendor_cuisine.rdbms_id = cr_metrics.rdbms_id
        AND vendor_cuisine.vendor_id = cr_metrics.vendor_id
  GROUP BY 1,2,3,4,5,6
),

fc_cr3 AS (
  SELECT
    vendor_fc.rdbms_id,
    vendor_fc.business_type,
    vendor_type,
    gmv_class,
    vendor_fc.food_characteristic_id,
    vendor_fc.food_characteristic_title,
    SUM(CR3_count) AS cr3_count,
    SUM(CR4_count) AS cr4_count
  FROM vendor_fc
  LEFT JOIN cr_metrics
         ON vendor_fc.rdbms_id = cr_metrics.rdbms_id
        AND vendor_fc.vendor_id = cr_metrics.vendor_id
  GROUP BY 1,2,3,4,5,6
),

---------------------------------------------
-- Category, product and variation metrics --
---------------------------------------------
variation_metrics AS (
  WITH product_variations AS (
    SELECT
      dim_vendor_product_variations.rdbms_id,
      dim_vendor_product_variations.vendor_code,
      dim_vendor_product_variations.product_id,
      COUNT(DISTINCT product_variation_id) AS variation_count
    FROM pandata.dim_vendor_product_variations
    WHERE is_product_active = true
      AND product_type != "Hidden Product"
      AND is_vendor_deleted = false
      AND is_product_deleted = false
      AND is_productvariation_deleted = false
    GROUP BY rdbms_id,vendor_code,product_id
   )
   SELECT
    product_variations.rdbms_id,
    product_variations.vendor_code,
    vendors.business_type,
    vendors.primary_cuisine_id,
    vendors.primary_cuisine,
    SAFE_DIVIDE(COUNTIF(variation_count > 1), COUNT(DISTINCT product_id)) AS proportion_products_w_variations,
    SUM(variation_count) AS variation_count,
    COUNT(DISTINCT product_id) AS product_count,
   FROM product_variations
   LEFT JOIN vendors
           ON product_variations.rdbms_id = vendors.rdbms_id
          AND product_variations.vendor_code = vendors.vendor_code
   GROUP BY 1,2,3,4,5
),

vendor_metrics AS (
  WITH primary_metrics AS (
    SELECT
      dim_vendor_product_variations.rdbms_id,
      dim_vendors.country_name,
      dim_vendor_product_variations.vendor_code,
      dim_vendors.business_type,
      vendors.vendor_type,
      vendors.gmv_class,
      dim_vendors.primary_cuisine_id,
      dim_vendors.primary_cuisine,
      COUNT(DISTINCT master_category_id) AS category_count,
      COUNT(DISTINCT product_id) AS product_count,
      COUNT(DISTINCT product_variation_id) AS product_var_count
    FROM pandata.dim_vendor_product_variations
    LEFT JOIN pandata.dim_vendors
           ON dim_vendor_product_variations.rdbms_id = dim_vendors.rdbms_id
          AND dim_vendor_product_variations.vendor_code = dim_vendors.vendor_code
    LEFT JOIN vendors
           ON dim_vendor_product_variations.rdbms_id = vendors.rdbms_id
          AND dim_vendor_product_variations.vendor_code = vendors.vendor_code
    WHERE is_product_active = true
      AND product_type != "Hidden Product"
      AND is_vendor_deleted = false
      AND is_product_deleted = false
      AND is_productvariation_deleted = false

      -- dim_vendor filters
      AND dim_vendors.is_active = true
      AND is_private = false
      AND is_vendor_testing = false
    GROUP BY 1,2,3,4,5,6,7,8)
  SELECT
    primary_metrics.*,
    variation_metrics.variation_count
  FROM primary_metrics
  LEFT JOIN variation_metrics
         ON primary_metrics.rdbms_id = variation_metrics.rdbms_id
        AND primary_metrics.vendor_code = variation_metrics.vendor_code
),

country_quantiles AS (
  WITH quantiles AS (
    SELECT 
      rdbms_id, 
      business_type, 
      primary_cuisine_id,
      vendor_type,
      gmv_class,
      APPROX_QUANTILES(category_count, 100) AS category_percentiles,
      APPROX_QUANTILES(product_count, 100) AS product_percentiles,
      APPROX_QUANTILES(product_var_count, 100) AS product_var_percentiles,
    FROM vendor_metrics 
    GROUP BY 1,2,3,4,5
  )
  SELECT
    rdbms_id,
    business_type,
    primary_cuisine_id,
    vendor_type,
    gmv_class,
    category_percentiles[offset(50)] as median_category,
    product_percentiles[offset(50)] as median_product,
    product_var_percentiles[offset(50)] as median_product_var,
  FROM quantiles
),

cpv_metrics AS (
  SELECT
    vendor_metrics.rdbms_id,
    vendor_metrics.country_name,
    vendor_metrics.business_type,
    vendor_metrics.primary_cuisine_id,
    vendor_metrics.primary_cuisine,
    vendor_type,
    gmv_class,
    AVG(category_count) AS avg_category,
    AVG(product_count) AS avg_product,
    AVG(product_var_count) AS avg_product_var
  FROM vendor_metrics
  GROUP BY 1,2,3,4,5,6,7
),


------------------------
-- Aggregated metrics --
------------------------
cuisine_metrics AS (
  SELECT
    common_name AS country,
    cuisine_orders.*,
    cuisine_cr3.cr3_count,
    cuisine_cr3.cr4_count,
    cuisine_coverage.vendor_count,
    cpv_metrics.avg_category,
    cpv_metrics.avg_product,
    cpv_metrics.avg_product_var,
    country_quantiles.median_category,
    country_quantiles.median_product,
    country_quantiles.median_product_var,
  FROM cuisine_orders
  LEFT JOIN cuisine_cr3
         ON cuisine_orders.rdbms_id = cuisine_cr3.rdbms_id
        AND cuisine_orders.primary_cuisine_id = cuisine_cr3.primary_cuisine_id
        AND cuisine_orders.business_type = cuisine_cr3.business_type
        AND cuisine_orders.vendor_type = cuisine_cr3.vendor_type
        AND cuisine_orders.gmv_class = cuisine_cr3.gmv_class
  LEFT JOIN cuisine_coverage
         ON cuisine_orders.rdbms_id = cuisine_coverage.rdbms_id
        AND cuisine_orders.primary_cuisine_id = cuisine_coverage.primary_cuisine_id
        AND cuisine_orders.business_type = cuisine_coverage.business_type
        AND cuisine_orders.vendor_type = cuisine_coverage.vendor_type
        AND cuisine_orders.gmv_class = cuisine_coverage.gmv_class
  LEFT JOIN cpv_metrics
         ON cuisine_orders.rdbms_id = cpv_metrics.rdbms_id
        AND cuisine_orders.primary_cuisine_id = cpv_metrics.primary_cuisine_id
        AND cuisine_orders.business_type = cpv_metrics.business_type
        AND cuisine_orders.vendor_type = cpv_metrics.vendor_type
        AND cuisine_orders.gmv_class = cpv_metrics.gmv_class
  LEFT JOIN country_quantiles
         ON cuisine_orders.rdbms_id = country_quantiles.rdbms_id
        AND cuisine_orders.primary_cuisine_id = country_quantiles.primary_cuisine_id
        AND cuisine_orders.business_type = country_quantiles.business_type
        AND cuisine_orders.vendor_type = country_quantiles.vendor_type
        AND cuisine_orders.gmv_class = country_quantiles.gmv_class
  LEFT JOIN pandata.dim_countries
         ON cuisine_orders.rdbms_id = dim_countries.rdbms_id
),

fc_metrics AS (
  SELECT
    fc_orders.*,
    fc_cr3.cr3_count,
    fc_cr3.cr4_count,
    fc_coverage.vendor_count,
    common_name as country
  FROM fc_orders
  LEFT JOIN fc_cr3
         ON fc_orders.rdbms_id = fc_cr3.rdbms_id
        AND fc_orders.food_characteristic_id = fc_cr3.food_characteristic_id
        AND fc_orders.business_type = fc_cr3.business_type
        AND fc_orders.vendor_type = fc_cr3.vendor_type
        AND fc_orders.gmv_class = fc_cr3.gmv_class
  LEFT JOIN fc_coverage
         ON fc_orders.rdbms_id = fc_coverage.rdbms_id
        AND fc_orders.food_characteristic_id = fc_coverage.food_characteristic_id
        AND fc_orders.business_type = fc_coverage.business_type
        AND fc_orders.vendor_type = fc_coverage.vendor_type
        AND fc_orders.gmv_class = fc_coverage.gmv_class
  LEFT JOIN pandata.dim_countries
         ON fc_orders.rdbms_id = dim_countries.rdbms_id
)


SELECT
  DATE_TRUNC(CURRENT_DATE, WEEK) AS week,
  DATE_TRUNC(CURRENT_DATE, MONTH) AS month,
  fc_metrics.*
FROM fc_metrics
