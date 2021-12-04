WITH fx_rates AS (
  SELECT
    rdbms_id,
    currency_code,
    exchange_rate_value AS eur_rate,
  FROM il_backend_latest.v_dim_exchange_rates
  WHERE TIMESTAMP_TRUNC(exchange_rate_date , DAY) = TIMESTAMP_TRUNC("2020-05-01 00:00:00", DAY)
    AND rdbms_id IN (7,12,15,16,17,18,19,219,220,221)
),

---------------------
-- Activation date --
---------------------
salesforce_contract AS (
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
-- Brand (to get GMV class)
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
    dim_vendors.business_type,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE, COALESCE(DATE(sf_activation.sf_activation_date),dim_vendors.activation_date,NULL), DAY) <= 30 THEN "New vendor"
      ELSE "Existing vendor"
    END AS vendor_type,
    COALESCE(foodpanda_AAA_brands.vendor_grade, vendor_gmv_class.gmv_class, dim_vendors.sf_vendor_grade, sf_activation.gmv_class) AS gmv_class,
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

-------------
-- Metrics --
-------------
category_popularity AS (
  SELECT
    fct_order_product_variations.rdbms_id,
    common_name AS country,
    master_category,
    business_type,
    vendor_type,
    gmv_class,
    COUNT(fct_order_product_variations.order_id) AS order_count,
    SUM(total_price_eur) AS total_value_eur,
    COUNTIF(has_dish_image = true) AS orders_w_img_count,
    COUNTIF(product_description IS NOT NULL) AS orders_w_description_count,
    
    COUNTIF(product_variation_price_local / eur_rate < 3 AND product_description IS NOT NULL) AS orders_w_description_0_to_3,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 3 AND 7 AND product_description IS NOT NULL) AS orders_w_description_3_to_7,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 7 AND 10 AND product_description IS NOT NULL) AS orders_w_description_7_to_10,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 10 AND 15 AND product_description IS NOT NULL) AS orders_w_description_10_to_15,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 15 AND 20 AND product_description IS NOT NULL) AS orders_w_description_15_to_20,
    COUNTIF(product_variation_price_local / eur_rate >20 AND product_description IS NOT NULL) AS orders_w_description_20,
    
    COUNTIF(product_variation_price_local / eur_rate < 3 AND has_dish_image = true) AS orders_w_img_0_to_3,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 3 AND 7 AND has_dish_image = true) AS orders_w_img_3_to_7,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 7 AND 10 AND has_dish_image = true) AS orders_w_img_7_to_10,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 10 AND 15 AND has_dish_image = true) AS orders_w_img_10_to_15,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 15 AND 20 AND has_dish_image = true) AS orders_w_img_15_to_20,
    COUNTIF(product_variation_price_local / eur_rate > 20 AND has_dish_image = true) AS orders_w_img_20,
    
    COUNTIF(product_variation_price_local / eur_rate < 3) AS orders_0_to_3,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 3 AND 7 ) AS orders_3_to_7,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 7 AND 10) AS orders_7_to_10,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 10 AND 15) AS orders_10_to_15,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 15 AND 20) AS orders_15_to_20,
    COUNTIF(product_variation_price_local / eur_rate >20) AS orders_above_20,


  FROM pandata.fct_order_product_variations
  LEFT JOIN pandata.dim_vendor_product_variations
         ON fct_order_product_variations.rdbms_id = dim_vendor_product_variations.rdbms_id
        AND fct_order_product_variations.product_variation_id = dim_vendor_product_variations.product_variation_id
  LEFT JOIN vendors
         ON fct_order_product_variations.rdbms_id = vendors.rdbms_id
        AND fct_order_product_variations.vendor_id = vendors.vendor_id
  LEFT JOIN pandata.dim_countries
         ON fct_order_product_variations.rdbms_id = dim_countries.rdbms_id
  LEFT JOIN fx_rates
         ON dim_countries.rdbms_id = fx_rates.rdbms_id
  WHERE fct_order_product_variations.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    AND master_category IS NOT NULL
    AND is_menu_active = true
    AND product_type != 'Hidden Product'
    AND is_product_active = true
  GROUP BY fct_order_product_variations.rdbms_id, country, dim_vendor_product_variations.master_category, business_type, vendor_type, gmv_class
),

cuisine_metrics AS (
  SELECT
    rdbms_id,
    AVG(master_category_count) AS avg_menu_count,
    AVG(product_count) AS avg_product_count,
  FROM pandata.dim_vendors
  GROUP BY rdbms_id, primary_cuisine_id
),

translation_metrics AS (
  SELECT
    dim_vendor_product_variations.rdbms_id,
    vendor_code,
    product_id,
    COUNT(DISTINCT language_id) AS translation_count
  FROM pandata.dim_vendor_product_variations
  LEFT JOIN pandata.dim_translations
         ON dim_vendor_product_variations.rdbms_id = dim_translations.rdbms_id
        AND dim_vendor_product_variations.product_id = dim_translations.object_id
  LEFT JOIN pandata.dim_languages
         ON dim_translations.rdbms_id = dim_languages.rdbms_id
        AND dim_translations.language_id = dim_languages.id
  WHERE is_product_active = true
    AND product_type != "Hidden Product"
    AND is_vendor_deleted = false
    AND is_product_deleted = false
    AND is_productvariation_deleted = false
    AND object = "Products"
    AND dim_languages.is_active = true
  GROUP BY rdbms_id, vendor_code, product_id
),

translation_order_metrics AS (
  SELECT
    fct_order_product_variations.rdbms_id,
    master_category,
    business_type,
    vendor_type,
    gmv_class,
    COUNTIF(translation_count > 1) AS orders_w_translations_count
  FROM pandata.fct_order_product_variations
  LEFT JOIN translation_metrics
         ON fct_order_product_variations.rdbms_id = translation_metrics.rdbms_id
        AND fct_order_product_variations.product_id = translation_metrics.product_id
  LEFT JOIN pandata.dim_vendor_product_variations
         ON fct_order_product_variations.rdbms_id = dim_vendor_product_variations.rdbms_id
        AND fct_order_product_variations.product_variation_id = dim_vendor_product_variations.product_variation_id
  LEFT JOIN vendors
         ON fct_order_product_variations.rdbms_id = vendors.rdbms_id
        AND fct_order_product_variations.vendor_id = vendors.vendor_id
  WHERE fct_order_product_variations.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    AND master_category IS NOT NULL
    AND is_menu_active = true
    AND product_type != 'Hidden Product'
    AND is_product_active = true
  GROUP BY fct_order_product_variations.rdbms_id, master_category, business_type, vendor_type, gmv_class
),

category_metrics AS (
  WITH id_product_level AS (
    SELECT
      dim_vendor_product_variations.rdbms_id,
      dim_vendor_product_variations.vendor_code,
      master_category,
      vendors.business_type,
      vendors.vendor_type,
      vendors.gmv_class,
      product_id,
      COUNTIF(image_pathname IS NOT NULL AND image_pathname NOT LIKE '%placeholder%') AS img_count,
      COUNTIF(product_description IS NOT NULL) AS description_count,
      AVG(product_variation_price_local) AS avg_price_local
    FROM pandata.dim_vendor_product_variations
    LEFT JOIN vendors
           ON dim_vendor_product_variations.rdbms_id = vendors.rdbms_id
          AND dim_vendor_product_variations.vendor_code = vendors.vendor_code
    WHERE is_menu_active = true
      AND is_menu_deleted = false
      AND product_type != 'Hidden Product'
      AND is_product_active = true
      AND is_vendor_deleted = false
      AND is_product_deleted = false
      AND is_productvariation_deleted = false
      AND master_category IS NOT NULL
    GROUP BY rdbms_id, vendor_code, master_category, product_id, business_type, vendor_type, gmv_class
  ),
  product_level AS (
  SELECT
    id_product_level.rdbms_id,
    id_product_level.vendor_code,
    id_product_level.business_type,
    id_product_level.vendor_type,
    id_product_level.gmv_class,
    master_category,
    avg_price_local,
    SAFE_DIVIDE(avg_price_local, eur_rate) AS price_eur,
    id_product_level.product_id,
    img_count,
    description_count,
    translation_count,
  FROM id_product_level
  LEFT JOIN translation_metrics
         ON id_product_level.rdbms_id = translation_metrics.rdbms_id
        AND id_product_level.vendor_code = translation_metrics.vendor_code
        AND id_product_level.product_id = translation_metrics.product_id
  LEFT JOIN fx_rates
         ON id_product_level.rdbms_id = fx_rates.rdbms_id
  )
  SELECT
    product_level.rdbms_id,
    product_level.master_category,
    product_level.business_type,
    product_level.vendor_type,
    product_level.gmv_class,
    SUM(IF(price_eur >= 100, NULL, price_eur)) AS total_price,
    COUNT(DISTINCT IF(price_eur >= 100, NULL, product_id)) AS product_count,
    COUNTIF(img_count > 0) AS img_count,
    COUNTIF(description_count > 0) AS description_count,
    COUNTIF(translation_count > 1) AS translation_count,
  FROM product_level
  WHERE master_category IS NOT NULL
  GROUP BY rdbms_id, master_category, business_type, vendor_type, gmv_class
  
),

avg_pricing AS (
  WITH quantiles AS (
    SELECT
      dim_vendor_product_variations.rdbms_id,
      country,
      master_category,
      business_type,
      vendor_type,
      gmv_class,
      APPROX_QUANTILES(product_variation_price_local, 100) AS percentiles
    FROM pandata.dim_vendor_product_variations 
    LEFT JOIN vendors
           ON dim_vendor_product_variations.rdbms_id = vendors.rdbms_id
          AND dim_vendor_product_variations.vendor_code = vendors.vendor_code
    WHERE is_menu_active = true
      AND is_menu_deleted = false
      AND product_type != 'Hidden Product'
      AND is_product_active = true
      AND is_vendor_deleted = false
      AND is_product_deleted = false
      AND is_productvariation_deleted = false
      AND master_category IS NOT NULL
   GROUP BY rdbms_id, country, master_category, business_type, vendor_type, gmv_class
  )
  SELECT
    rdbms_id,
    country,
    master_category,
    business_type,
    vendor_type,
    gmv_class,
    percentiles[offset(10)] as p10,
    percentiles[offset(25)] as p25,
    percentiles[offset(50)] as p50,
    percentiles[offset(75)] as p75,
    percentiles[offset(90)] as p90,
  FROM quantiles
),

category_level AS (
  SELECT
    common_name AS country,
    category_metrics.*,
    category_popularity.order_count,
    category_popularity.total_value_eur,
    category_popularity.orders_w_img_count,
    category_popularity.orders_w_description_count,
    translation_order_metrics.orders_w_translations_count,
    orders_w_description_0_to_3,
    orders_w_description_3_to_7,
    orders_w_description_7_to_10,
    orders_w_description_10_to_15,
    orders_w_description_15_to_20,
    orders_w_description_20,
    orders_w_img_0_to_3,
    orders_w_img_3_to_7,
    orders_w_img_7_to_10,
    orders_w_img_10_to_15,
    orders_w_img_15_to_20,
    orders_w_img_20,
    orders_0_to_3,
    orders_3_to_7,
    orders_7_to_10,
    orders_10_to_15,
    orders_15_to_20,
    orders_above_20,
  FROM category_metrics
  LEFT JOIN category_popularity
         ON category_metrics.rdbms_id = category_popularity.rdbms_id
        AND category_metrics.master_category = category_popularity.master_category
        AND category_metrics.business_type = category_popularity.business_type
        AND category_metrics.vendor_type = category_popularity.vendor_type
        AND category_metrics.gmv_class = category_popularity.gmv_class
  LEFT JOIN translation_order_metrics
         ON category_metrics.rdbms_id = translation_order_metrics.rdbms_id
        AND category_metrics.master_category = translation_order_metrics.master_category
        AND category_metrics.business_type = translation_order_metrics.business_type
        AND category_metrics.vendor_type = translation_order_metrics.vendor_type
        AND category_metrics.gmv_class = translation_order_metrics.gmv_class
--   LEFT JOIN avg_pricing
--          ON category_metrics.rdbms_id = avg_pricing.rdbms_id
--         AND category_metrics.master_category = avg_pricing.master_category
--         AND category_metrics.business_type = avg_pricing.business_type 
--         AND category_metrics.vendor_type = avg_pricing.vendor_type
--         AND category_metrics.gmv_class = avg_pricing.gmv_class
  LEFT JOIN pandata.dim_countries
         ON category_metrics.rdbms_id = dim_countries.rdbms_id
),

apac_category_popularity AS (
  SELECT
    0 AS rdbms_id,
    'APAC' AS country,
    master_category,
    business_type,
    vendor_type,
    gmv_class,
    COUNT(fct_order_product_variations.order_id) AS order_count,
    SUM(total_price_eur) AS total_value_eur,
    COUNTIF(has_dish_image = true) AS orders_w_img_count,
    COUNTIF(product_description IS NOT NULL) AS orders_w_description_count,
    
    COUNTIF(product_variation_price_local / eur_rate < 3 AND product_description IS NOT NULL) AS orders_w_description_0_to_3,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 3 AND 7 AND product_description IS NOT NULL) AS orders_w_description_3_to_7,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 7 AND 10 AND product_description IS NOT NULL) AS orders_w_description_7_to_10,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 10 AND 15 AND product_description IS NOT NULL) AS orders_w_description_10_to_15,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 15 AND 20 AND product_description IS NOT NULL) AS orders_w_description_15_to_20,
    COUNTIF(product_variation_price_local / eur_rate >20 AND product_description IS NOT NULL) AS orders_w_description_20,
    
    COUNTIF(product_variation_price_local / eur_rate < 3 AND has_dish_image = true) AS orders_w_img_0_to_3,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 3 AND 7 AND has_dish_image = true) AS orders_w_img_3_to_7,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 7 AND 10 AND has_dish_image = true) AS orders_w_img_7_to_10,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 10 AND 15 AND has_dish_image = true) AS orders_w_img_10_to_15,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 15 AND 20 AND has_dish_image = true) AS orders_w_img_15_to_20,
    COUNTIF(product_variation_price_local / eur_rate > 20 AND has_dish_image = true) AS orders_w_img_20,
    
    COUNTIF(product_variation_price_local / eur_rate < 3) AS orders_0_to_3,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 3 AND 7 ) AS orders_3_to_7,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 7 AND 10) AS orders_7_to_10,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 10 AND 15) AS orders_10_to_15,
    COUNTIF(product_variation_price_local / eur_rate BETWEEN 15 AND 20) AS orders_15_to_20,
    COUNTIF(product_variation_price_local / eur_rate >20) AS orders_above_20,

  FROM pandata.fct_order_product_variations
  LEFT JOIN pandata.dim_vendor_product_variations
         ON fct_order_product_variations.rdbms_id = dim_vendor_product_variations.rdbms_id
        AND fct_order_product_variations.product_variation_id = dim_vendor_product_variations.product_variation_id
  LEFT JOIN vendors
         ON fct_order_product_variations.rdbms_id = vendors.rdbms_id
        AND fct_order_product_variations.vendor_id = vendors.vendor_id
  LEFT JOIN pandata.dim_countries
         ON fct_order_product_variations.rdbms_id = dim_countries.rdbms_id
  LEFT JOIN fx_rates
         ON dim_countries.rdbms_id = fx_rates.rdbms_id
  WHERE fct_order_product_variations.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    AND master_category IS NOT NULL
    AND is_menu_active = true
    AND product_type != 'Hidden Product'
    AND is_product_active = true
  GROUP BY dim_vendor_product_variations.master_category, business_type, vendor_type, gmv_class
),

apac_cuisine_metrics AS (
  SELECT
    0 AS rdbms_id,
    AVG(master_category_count) AS avg_menu_count,
    AVG(product_count) AS avg_product_count,
  FROM pandata.dim_vendors
  GROUP BY primary_cuisine_id
),

apac_translation_metrics AS (
  SELECT
    dim_vendor_product_variations.rdbms_id,
    vendor_code,
    product_id,
    COUNT(DISTINCT language_id) AS translation_count
  FROM pandata.dim_vendor_product_variations
  LEFT JOIN pandata.dim_translations
         ON dim_vendor_product_variations.rdbms_id = dim_translations.rdbms_id
        AND dim_vendor_product_variations.product_id = dim_translations.object_id
  LEFT JOIN pandata.dim_languages
         ON dim_translations.rdbms_id = dim_languages.rdbms_id
        AND dim_translations.language_id = dim_languages.id
  WHERE is_product_active = true
    AND product_type != "Hidden Product"
    AND is_vendor_deleted = false
    AND is_product_deleted = false
    AND is_productvariation_deleted = false
    AND object = "Products"
    AND dim_languages.is_active = true
  GROUP BY rdbms_id, vendor_code, product_id
),

apac_translation_order_metrics AS (
  SELECT
    0 AS rdbms_id,
    master_category,
    business_type,
    vendor_type,
    gmv_class,
    COUNTIF(translation_count > 1) AS orders_w_translations_count
  FROM pandata.fct_order_product_variations
  LEFT JOIN translation_metrics
         ON fct_order_product_variations.rdbms_id = translation_metrics.rdbms_id
        AND fct_order_product_variations.product_id = translation_metrics.product_id
  LEFT JOIN pandata.dim_vendor_product_variations
         ON fct_order_product_variations.rdbms_id = dim_vendor_product_variations.rdbms_id
        AND fct_order_product_variations.product_variation_id = dim_vendor_product_variations.product_variation_id
  LEFT JOIN vendors
         ON fct_order_product_variations.rdbms_id = vendors.rdbms_id
        AND fct_order_product_variations.vendor_id = vendors.vendor_id
  WHERE fct_order_product_variations.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    AND master_category IS NOT NULL
    AND is_menu_active = true
    AND product_type != 'Hidden Product'
    AND is_product_active = true
  GROUP BY master_category, business_type, vendor_type, gmv_class
),

apac_category_metrics AS (
  WITH id_product_level AS (
    SELECT
      dim_vendor_product_variations.rdbms_id,
      dim_vendor_product_variations.vendor_code,
      master_category,
      vendors.business_type,
      vendors.vendor_type,
      vendors.gmv_class,
      product_id,
      COUNTIF(image_pathname IS NOT NULL AND image_pathname NOT LIKE '%placeholder%') AS img_count,
      COUNTIF(product_description IS NOT NULL) AS description_count,
      AVG(product_variation_price_local) AS avg_price_local
    FROM pandata.dim_vendor_product_variations
    LEFT JOIN vendors
           ON dim_vendor_product_variations.rdbms_id = vendors.rdbms_id
          AND dim_vendor_product_variations.vendor_code = vendors.vendor_code
    WHERE is_menu_active = true
      AND is_menu_deleted = false
      AND product_type != 'Hidden Product'
      AND is_product_active = true
      AND is_vendor_deleted = false
      AND is_product_deleted = false
      AND is_productvariation_deleted = false
      AND master_category IS NOT NULL
    GROUP BY rdbms_id, vendor_code, master_category, product_id, business_type, vendor_type, gmv_class
  ),
  product_level AS (
  SELECT
    id_product_level.rdbms_id,
    id_product_level.vendor_code,
    id_product_level.business_type,
    id_product_level.vendor_type,
    id_product_level.gmv_class,
    master_category,
    avg_price_local,
    SAFE_DIVIDE(avg_price_local, eur_rate) AS price_eur,
    id_product_level.product_id,
    img_count,
    description_count,
    translation_count,
  FROM id_product_level
  LEFT JOIN apac_translation_metrics
         ON id_product_level.rdbms_id = apac_translation_metrics.rdbms_id
        AND id_product_level.vendor_code = apac_translation_metrics.vendor_code
        AND id_product_level.product_id = apac_translation_metrics.product_id
  LEFT JOIN fx_rates
         ON id_product_level.rdbms_id = fx_rates.rdbms_id
  )
  SELECT
    0 rdbms_id,
    product_level.master_category,
    product_level.business_type,
    product_level.vendor_type,
    product_level.gmv_class,
    SUM(IF(price_eur >= 100, NULL, price_eur)) AS total_price,
    COUNT(DISTINCT IF(price_eur >= 100, NULL, product_id)) AS product_count,
    COUNTIF(img_count > 0) AS img_count,
    COUNTIF(description_count > 0) AS description_count,
    COUNTIF(translation_count > 1) AS translation_count,
  FROM product_level
  WHERE master_category IS NOT NULL
  GROUP BY master_category, business_type, vendor_type, gmv_class
  
),

apac_avg_pricing AS (
  WITH quantiles AS (
    SELECT
      0 AS rdbms_id,
      'APAC' AS country_name,
      master_category,
      business_type,
      vendor_type,
      gmv_class,
      APPROX_QUANTILES(product_variation_price_local, 100) AS percentiles
    FROM pandata.dim_vendor_product_variations 
    LEFT JOIN vendors
           ON dim_vendor_product_variations.rdbms_id = vendors.rdbms_id
          AND dim_vendor_product_variations.vendor_code = vendors.vendor_code
    WHERE is_menu_active = true
      AND is_menu_deleted = false
      AND product_type != 'Hidden Product'
      AND is_product_active = true
      AND is_vendor_deleted = false
      AND is_product_deleted = false
      AND is_productvariation_deleted = false
      AND master_category IS NOT NULL
   GROUP BY master_category, business_type, vendor_type, gmv_class
  )
  SELECT
    rdbms_id,
    country_name,
    master_category,
    business_type,
    vendor_type,
    gmv_class,
    percentiles[offset(10)] as p10,
    percentiles[offset(25)] as p25,
    percentiles[offset(50)] as p50,
    percentiles[offset(75)] as p75,
    percentiles[offset(90)] as p90,
  FROM quantiles
),

apac_category_level AS (
  SELECT
    'APAC' AS country,
    apac_category_metrics.*,
    -- apac_avg_pricing.p50 AS avg_price,
    apac_category_popularity.order_count,
    apac_category_popularity.total_value_eur,
    apac_category_popularity.orders_w_img_count,
    apac_category_popularity.orders_w_description_count,
    apac_translation_order_metrics.orders_w_translations_count,
    orders_w_description_0_to_3,
    orders_w_description_3_to_7,
    orders_w_description_7_to_10,
    orders_w_description_10_to_15,
    orders_w_description_15_to_20,
    orders_w_description_20,
    orders_w_img_0_to_3,
    orders_w_img_3_to_7,
    orders_w_img_7_to_10,
    orders_w_img_10_to_15,
    orders_w_img_15_to_20,
    orders_w_img_20,
    orders_0_to_3,
    orders_3_to_7,
    orders_7_to_10,
    orders_10_to_15,
    orders_15_to_20,
    orders_above_20,
  FROM apac_category_metrics
  LEFT JOIN apac_category_popularity
         ON apac_category_metrics.rdbms_id = apac_category_popularity.rdbms_id
        AND apac_category_metrics.master_category = apac_category_popularity.master_category
        AND apac_category_metrics.business_type = apac_category_popularity.business_type
        AND apac_category_metrics.vendor_type = apac_category_popularity.vendor_type
        AND apac_category_metrics.gmv_class = apac_category_popularity.gmv_class
  LEFT JOIN apac_translation_order_metrics
         ON apac_category_metrics.rdbms_id = apac_translation_order_metrics.rdbms_id
        AND apac_category_metrics.master_category = apac_translation_order_metrics.master_category
        AND apac_category_metrics.business_type = apac_translation_order_metrics.business_type
        AND apac_category_metrics.vendor_type = apac_translation_order_metrics.vendor_type
        AND apac_category_metrics.gmv_class = apac_translation_order_metrics.gmv_class
--   LEFT JOIN apac_avg_pricing
--          ON apac_category_metrics.rdbms_id = apac_avg_pricing.rdbms_id
--         AND apac_category_metrics.master_category = apac_avg_pricing.master_category
--         AND apac_category_metrics.business_type = apac_avg_pricing.business_type 
--         AND apac_category_metrics.vendor_type = apac_avg_pricing.vendor_type
--         AND apac_category_metrics.gmv_class = apac_avg_pricing.gmv_class
)


SELECT
  DATE_TRUNC(CURRENT_DATE, WEEK) AS week,
  DATE_TRUNC(CURRENT_DATE, MONTH) AS month,
  category_level.*
FROM category_level

UNION ALL 

SELECT
  DATE_TRUNC(CURRENT_DATE, WEEK) AS week,
  DATE_TRUNC(CURRENT_DATE, MONTH) AS month,
  apac_category_level.*
FROM apac_category_level
