WITH menu_metrics AS (
SELECT
  dim_vendor_product_variations.rdbms_id,
  dim_vendor_product_variations.vendor_code,
  dim_vendor_product_variations.vendor_id,
  dim_vendors.business_type,

  -- Misc
  COUNT(DISTINCT product_id) AS active_item_count,
  AVG(product_variation_price_local) AS avg_price,
  
  -- Cuisine and Food characteristics
  COUNT(DISTINCT dim_vendor_product_variations.primary_cuisine) AS cuisine_count,
  COUNT(DISTINCT food_characteristic_id) AS food_characteristic_count,
  
  -- Expensive products
  COUNT(DISTINCT
     CASE
      WHEN dim_vendor_product_variations.rdbms_id = 7 AND product_variation_price_local > 9363.03 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 12 AND product_variation_price_local > 17828.32 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 15 AND product_variation_price_local > 156.76 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 16 AND product_variation_price_local > 482.69 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 17 AND product_variation_price_local > 3531.62 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 18 AND product_variation_price_local > 3326.51 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 19 AND product_variation_price_local > 860.29 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 20 AND product_variation_price_local > 5617.57 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 22 AND product_variation_price_local > 154338.76 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 219 AND product_variation_price_local > 991261.04 THEN product_id
      WHEN dim_vendor_product_variations.rdbms_id = 220 AND product_variation_price_local > 452581.25 THEN product_id
     END) AS expensive_product_count,
  COUNTIF(product_title LIKE "%Foodpanda%") AS Foodpanda_count,
  
FROM pandata.dim_vendors
LEFT JOIN pandata.dim_vendor_product_variations
       ON dim_vendor_product_variations.rdbms_id = dim_vendors.rdbms_id
      AND dim_vendor_product_variations.vendor_code = dim_vendors.vendor_code
LEFT JOIN pandata.dim_vendor_food_characteristics
       ON dim_vendors.id = dim_vendor_food_characteristics.vendor_id
      AND dim_vendors.rdbms_id = dim_vendor_food_characteristics.rdbms_id
WHERE is_product_active = true
  AND product_type != "Hidden Product"
  AND is_vendor_deleted = false
  AND is_product_deleted = false
  AND is_productvariation_deleted = false
  
  -- dim_vendor filters
  AND dim_vendors.is_active = true
  AND is_private = false
  AND is_vendor_testing = false
  AND activation_date <= CURRENT_DATE
  
  -- dim_vendor_food_characteristics
  AND dim_vendor_food_characteristics.is_active = true
GROUP BY rdbms_id, vendor_id, vendor_code, business_type
),

----------------
-- Variations --
----------------
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
    SAFE_DIVIDE(COUNTIF(variation_count > 1), COUNT(DISTINCT product_id)) AS proportion_products_w_variations,
    SUM(variation_count) AS variation_count,
    COUNT(DISTINCT product_id) AS product_count,
   FROM product_variations
   LEFT JOIN pandata.dim_vendors
           ON product_variations.rdbms_id = dim_vendors.rdbms_id
          AND product_variations.vendor_code = dim_vendors.vendor_code
   -- WHERE vendor_type = "restaurants"
   GROUP BY rdbms_id,vendor_code
  ),

------------------
-- Translations --
------------------
translation_metrics AS (
  WITH product_translations AS (
    SELECT
      dim_vendor_product_variations.rdbms_id,
      vendor_code,
      product_id,
      COUNT(DISTINCT language_id) AS translation_count,
      COUNTIF(language_title = "English") AS English_count,
      COUNTIF(language_title = "Bangla") AS Bangla_count,
      COUNTIF(language_title = "Khmer") AS Khmer_count,
      COUNTIF(language_title = "Lao") AS Lao_count,
      COUNTIF(language_title = "Burmese") AS Burmese_count,
      COUNTIF(language_title = "中文" OR language_title = "Chinese") AS Chinese_count,
      COUNTIF(language_title = "Bahasa") AS Bahasa_count,
      COUNTIF(language_title = "ไทย") AS Thai_count,
      COUNTIF(language_title = "русский") AS Russian_count,
      COUNTIF(language_title = "日本語") AS Japanese_count,      
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
  )
  SELECT
    rdbms_id,
    vendor_code,
    COUNT(DISTINCT product_id) AS  product_count,
    SAFE_DIVIDE(COUNTIF(translation_count > 1 ), COUNT(DISTINCT product_id)) AS proportion_products_w_translations,
    COUNTIF(translation_count > 1) AS count_products_w_translations,
    
    -- Product translation
    COUNTIF(English_count >= 1) AS english_count,
    COUNTIF(Bangla_count >= 1) AS bangla_count,
    COUNTIF(Khmer_count >= 1) AS khmer_count,
    COUNTIF(Lao_count >= 1) AS lao_count,
    COUNTIF(Chinese_count >= 1) AS chinese_count,
    COUNTIF(Bahasa_count >= 1) AS bahasa_count,
    COUNTIF(Thai_count >= 1) AS thai_count,
    COUNTIF(Russian_count >= 1) AS russian_count,
    COUNTIF(Japanese_count >= 1) AS japanese_count,
    
    -- Coverage
    SAFE_DIVIDE(COUNTIF(English_count >= 1), COUNT(DISTINCT product_id)) AS english_coverage,
    SAFE_DIVIDE(COUNTIF(Bangla_count >= 1), COUNT(DISTINCT product_id)) AS bangla_coverage,
    SAFE_DIVIDE(COUNTIF(Khmer_count >= 1), COUNT(DISTINCT product_id)) AS khmer_coverage,
    SAFE_DIVIDE(COUNTIF(Lao_count >= 1), COUNT(DISTINCT product_id)) AS lao_coverage,
    SAFE_DIVIDE(COUNTIF(Chinese_count >= 1), COUNT(DISTINCT product_id)) AS chinese_coverage,
    SAFE_DIVIDE(COUNTIF(Bahasa_count >= 1), COUNT(DISTINCT product_id)) AS bahasa_coverage,
    SAFE_DIVIDE(COUNTIF(Thai_count >= 1), COUNT(DISTINCT product_id)) AS thai_coverage,
    SAFE_DIVIDE(COUNTIF(Russian_count >= 1), COUNT(DISTINCT product_id)) AS russian_coverage,
    SAFE_DIVIDE(COUNTIF(Japanese_count >= 1), COUNT(DISTINCT product_id)) AS japanese_coverage,
    
  FROM product_translations
  GROUP BY rdbms_id, vendor_code
),

---------------------
-- Menu categories --
---------------------
category_metrics AS (
  WITH menu_metrics AS (
  SELECT
    rdbms_id,
    vendor_code,
    menu_category_id,
    COUNT(DISTINCT product_id) AS menu_length
  FROM pandata.dim_vendor_product_variations
  WHERE is_product_active = true
    AND product_type != "Hidden Product"
    AND is_vendor_deleted = false
    AND is_product_deleted = false
    AND is_productvariation_deleted = false
  GROUP BY rdbms_id, vendor_code, menu_category_id
  )

  SELECT
    dim_vendors.rdbms_id,
    dim_vendors.vendor_code,
    COUNT(DISTINCT menu_metrics.menu_category_id) AS menu_count,
    AVG(menu_length) AS avg_menu_length,
    MIN(menu_length) AS min_menu_length,
    COUNTIF(menu_length <= 5) AS small_menu_count,
    COUNTIF(menu_length < 5 OR menu_length IS NULL) AS menu_count_below5,
    COUNTIF(menu_length BETWEEN 5 AND 10) AS menu_count_5to10,
    COUNTIF(menu_length BETWEEN 10 AND 20) AS menu_count_10to20,
    COUNTIF(menu_length BETWEEN 20 AND 40) AS menu_count_20to40,
    COUNTIF(menu_length > 40) AS menu_count_above40,
  FROM pandata.dim_vendors
  LEFT JOIN menu_metrics
         ON menu_metrics.rdbms_Id = dim_vendors.rdbms_id
        AND menu_metrics.vendor_code = dim_vendors.vendor_code
  WHERE dim_vendors.is_active = true
    AND dim_vendors.is_deleted = false
    AND is_private = false
    AND is_vendor_testing = false
    AND activation_date <= CURRENT_DATE
  GROUP BY dim_vendors.rdbms_id, dim_vendors.vendor_code
),

---------------------------
-- Image and description --
---------------------------
id_metrics AS (
  WITH product_level AS (
    SELECT
      rdbms_id,
      vendor_code,
      product_id,
      COUNTIF(image_pathname IS NOT NULL AND image_pathname NOT LIKE '%placeholder%') AS img_count,
      COUNTIF(product_description IS NOT NULL) AS description_count,
    FROM pandata.dim_vendor_product_variations
    WHERE is_product_active = true
      AND product_type != "Hidden Product"
      AND is_vendor_deleted = false
      AND is_product_deleted = false
    GROUP BY rdbms_id, vendor_code, product_id
  )
  SELECT
    rdbms_id,
    vendor_code,
    COUNT(DISTINCT product_id) AS product_count,
    COUNTIF(img_count > 0) AS img_count,
    COUNTIF(description_count > 0) AS description_count,
    SAFE_DIVIDE(COUNTIF(img_count > 0), COUNT(DISTINCT product_id)) AS img_coverage,
    SAFE_DIVIDE(COUNTIF(description_count > 0), COUNT(DISTINCT product_id)) AS description_coverage,
  FROM product_level
  GROUP BY rdbms_id, vendor_code
),

-------------------
-- Order metrics --
-------------------
order_metrics AS (
  SELECT
    fct_order_product_variations.rdbms_id,
    fct_order_product_variations.vendor_id,
    COUNT(DISTINCT fct_order_product_variations.product_variation_id) AS items_w_at_least_1_order_count,
    SAFE_DIVIDE(COUNTIF(has_dish_image = true), COUNTIF(fct_orders.is_gross_order = true)) AS proportion_orders_with_img,
    SAFE_DIVIDE(COUNTIF(product_description IS NOT NULL), COUNTIF(fct_orders.is_gross_order = true)) AS proportion_orders_with_description,
  FROM pandata.fct_orders
  LEFT JOIN pandata.fct_order_product_variations
         ON fct_order_product_variations.rdbms_id = fct_orders.rdbms_id
        AND fct_order_product_variations.vendor_id = fct_orders.vendor_id
        AND fct_order_product_variations.order_id = fct_orders.id
  LEFT JOIN pandata.dim_vendor_product_variations
         ON fct_order_product_variations.rdbms_id = dim_vendor_product_variations.rdbms_id
        AND fct_order_product_variations.vendor_id = dim_vendor_product_variations.vendor_id
        AND fct_order_product_variations.product_variation_id = dim_vendor_product_variations.product_variation_id
  WHERE fct_order_product_variations.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    AND fct_orders.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    
    -- fct_orders filters
    AND is_test_order = false
    AND fct_orders.is_gross_order = true
    
    -- dim_vendor_product_variations filters
    AND is_product_active = true
    AND product_type != "Hidden Product"
    AND is_vendor_deleted = false
    AND is_product_deleted = false
    AND is_productvariation_deleted = false
  GROUP BY fct_order_product_variations.rdbms_id, fct_order_product_variations.vendor_id
),

gmv_metrics AS (
  SELECT
    rdbms_id,
    vendor_code,
    SUM(gmv_eur) AS gmv_eur
  FROM pandata.fct_orders
  WHERE fct_orders.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    AND is_test_order = false
    AND is_gross_order = true
  GROUP BY rdbms_id, vendor_code
),

cr_metrics AS (
  SELECT
    dim_countries.rdbms_id,
    country,
    vendor_code,
    SUM(count_of_checkout_loaded) AS CR4_count,
    SUM(count_of_shop_menu_loaded) AS CR3_count,
    SAFE_DIVIDE(SUM(count_of_checkout_loaded), SUM(count_of_shop_menu_loaded)) AS CR3
  FROM pandata_ap_product_external.vendor_level_session_metrics
  LEFT JOIN pandata.dim_countries
         ON vendor_level_session_metrics.country = dim_countries.common_name
  WHERE date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
  GROUP BY rdbms_id, country, vendor_code
),

--------------------------
-- Hero & Listing image --
--------------------------
hl_img_metrics AS (
  WITH vendor_raw AS (
  SELECT
    images.namespace,
    filename,
    SUBSTR(filename, 1, 4) AS vendor_code,
    CASE
      WHEN images.namespace = "fd-bd" THEN 7
      WHEN images.namespace = "fd-pk" THEN 12
      WHEN images.namespace = "fd-sg" THEN 15
      WHEN images.namespace = "fd-my" THEN 16
      WHEN images.namespace = "fd-th" THEN 17
      WHEN images.namespace = "fd-tw" THEN 18
      WHEN images.namespace = "fd-hk" THEN 19
      WHEN images.namespace = "fd-ph" THEN 20
      WHEN images.namespace = "fd-la" THEN 219
      WHEN images.namespace = "fd-kh" THEN 220
      WHEN images.namespace = "fd-mm" THEN 221
    END AS rdbms_id
  FROM ml_images_latest.images
  LEFT JOIN ml_images_latest.directories
         ON images.directory_id = directories.id      
  WHERE images.deleted = false
    AND ((images.filename like "%hero%") OR (images.filename like "%listing%"))
 )
 
 SELECT
    rdbms_id,
    vendor_code,
    COUNTIF(filename LIKE "%hero%") AS hero_img_count,
    COUNTIF(filename LIKE "%listing%") AS hero_listing_count,
  FROM vendor_raw
  GROUP BY rdbms_id, vendor_code
  ORDER BY COUNTIF(filename LIKE "%hero%") DESC
),

-------------------
-- Chain metrics --
-------------------
chain_metrics AS (
  SELECT
    rdbms_id,
    chain_id,
    COUNT(DISTINCT vendor_code) AS chain_count
  FROM pandata.dim_vendors
  WHERE is_active = true
    AND is_private = false
    AND is_vendor_testing = false
  GROUP BY rdbms_id, chain_id
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

--------------------
-- No active menu --
--------------------
no_menu AS (
  WITH vendor_level AS (
    SELECT
      dim_vendors.rdbms_id,
      dim_vendors.country_name,
      dim_vendors.vendor_code,
      dim_vendors.business_type,
      CASE
        WHEN DATE_DIFF(CURRENT_DATE, COALESCE(DATE(sf_activation.sf_activation_date),dim_vendors.activation_date,NULL), DAY) <= 30 THEN "New vendor"
        ELSE "Existing vendor"
      END AS vendor_type,
      COALESCE(foodpanda_AAA_brands.vendor_grade, vendor_gmv_class.gmv_class, dim_vendors.sf_vendor_grade, sf_activation.gmv_class) AS gmv_class,
      COUNT(DISTINCT menu_category_id) AS menu_count,
      COUNT(DISTINCT product_id) AS product_count,
      COUNT(DISTINCT product_variation_id) AS product_var_count
    FROM pandata.dim_vendors
    LEFT JOIN pandata.dim_vendor_product_variations
           ON dim_vendors.rdbms_id = dim_vendor_product_variations.rdbms_id
          AND dim_vendors.vendor_code = dim_vendor_product_variations.vendor_code
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
      AND is_vendor_testing = false
      AND is_private = false
      AND activation_date <= CURRENT_DATE
    GROUP BY rdbms_id, country_name, vendor_code, business_type, vendor_type, gmv_class
  )

  SELECT
    rdbms_id,
    country_name,
    business_type,
    vendor_type,
    gmv_class,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNTIF(menu_count = 0) AS no_menu_count,
    COUNTIF(product_count = 0) AS no_product
  FROM vendor_level
  GROUP BY rdbms_id, country_name, business_type, vendor_type, gmv_class
),

no_menu_apac AS (
  WITH vendor_level AS (
    SELECT
      dim_vendors.rdbms_id,
      dim_vendors.country_name,
      dim_vendors.vendor_code,
      dim_vendors.business_type,
      CASE
        WHEN DATE_DIFF(CURRENT_DATE, COALESCE(DATE(sf_activation.sf_activation_date),dim_vendors.activation_date,NULL), DAY) <= 30 THEN "New vendor"
        ELSE "Existing vendor"
      END AS vendor_type,
      COALESCE(foodpanda_AAA_brands.vendor_grade, vendor_gmv_class.gmv_class, dim_vendors.sf_vendor_grade, sf_activation.gmv_class) AS gmv_class,
      COUNT(DISTINCT menu_category_id) AS menu_count,
      COUNT(DISTINCT product_id) AS product_count,
      COUNT(DISTINCT product_variation_id) AS product_var_count
    FROM pandata.dim_vendors
    LEFT JOIN pandata.dim_vendor_product_variations
           ON dim_vendors.rdbms_id = dim_vendor_product_variations.rdbms_id
          AND dim_vendors.vendor_code = dim_vendor_product_variations.vendor_code
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
      AND is_vendor_testing = false
      AND is_private = false
      AND activation_date <= CURRENT_DATE
    GROUP BY rdbms_id, country_name, vendor_code, business_type, vendor_type, gmv_class
  )

  SELECT
    business_type,
    vendor_type,
    gmv_class,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNTIF(menu_count = 0) AS no_menu_count,
    COUNTIF(product_count = 0) AS no_product
  FROM vendor_level
  GROUP BY business_type, vendor_type, gmv_class
),

--------------
-- Toppings --
--------------
topping_metrics AS (
  WITH product_toppings AS (
  SELECT
    dim_vendor_product_variations.rdbms_id,
    dim_vendor_product_variations.vendor_code,
    product_id,
    MIN(toppingtemplate_id) AS toppingtemplate_id,
    COUNT(DISTINCT productvariationstoppingtemplates.id) AS toppings_count,
    COUNT(DISTINCT toppingtemplate_id) AS topping_template_count
  FROM pandata.dim_vendor_product_variations
  LEFT JOIN ml_backend_latest.productvariationstoppingtemplates
         ON dim_vendor_product_variations.rdbms_id = productvariationstoppingtemplates.rdbms_id
        AND dim_vendor_product_variations.product_variation_id = productvariationstoppingtemplates.productvariation_id
  GROUP BY rdbms_id, vendor_code, product_id
  )

  SELECT
    dim_vendors.rdbms_id,
    dim_vendors.vendor_code,
    SAFE_DIVIDE(COUNTIF(toppings_count > 0), COUNT(DISTINCT product_id)) AS items_w_toppings_proportion,
    SUM(toppings_count) AS toppings_count,
    COUNT(DISTINCT toppingtemplate_id) AS topping_template_count
  FROM pandata.dim_vendors
  LEFT JOIN product_toppings
         ON dim_vendors.rdbms_id = product_toppings.rdbms_id
        AND dim_vendors.vendor_code = product_toppings.vendor_code
  WHERE is_active = true
    AND is_vendor_testing = false
    AND is_private = false
  GROUP BY rdbms_id, vendor_code
),

topping_template_metrics AS (
  WITH topping_template AS (
  SELECT
    toppingtemplates.rdbms_id,
    toppingtemplates.id AS template_id,
    COUNT(DISTINCT toppingtemplateproducts.id) AS topping_count,
  FROM ml_backend_latest.toppingtemplates
  LEFT JOIN ml_backend_latest.toppingtemplateproducts
         ON toppingtemplates.rdbms_id = toppingtemplateproducts.rdbms_id
        AND toppingtemplates.id = toppingtemplateproducts.toppingtemplate_id
  WHERE toppingtemplates.deleted = 0
    AND toppingtemplateproducts.deleted = 0
  GROUP BY toppingtemplates.rdbms_id, toppingtemplates.id
  )

  SELECT
    topping_template.rdbms_id,
    "restaurants" AS business_type,
    AVG(topping_count) AS avg_topping_count,
  FROM topping_template
  GROUP BY rdbms_id
),

------------------------
-- Aggregated metrics --
------------------------
vendor_metrics AS (
  SELECT
    -- Vendor details
    dim_vendors.rdbms_id,
    dim_countries.common_name AS country,
    dim_vendors.vendor_code,
    dim_vendors.vendor_name,
    dim_vendors.chain_id,
    dim_vendors.chain_name,
    dim_vendors.activation_date,
    dim_vendors.business_type,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE, COALESCE(DATE(sf_activation.sf_activation_date),dim_vendors.activation_date,NULL), DAY) <= 30 THEN "New vendor"
      ELSE "Existing vendor"
    END AS vendor_type,
    COALESCE(foodpanda_AAA_brands.vendor_grade, vendor_gmv_class.gmv_class, dim_vendors.sf_vendor_grade, sf_activation.gmv_class) AS gmv_class,

    -- Misc metrics
    dim_vendors.product_count AS active_item_count,
    menu_metrics.avg_price,
    
    -- Product variation metrics
    variation_metrics.proportion_products_w_variations,
    proportion_products_w_variations * active_item_count AS products_w_variations,
    variation_metrics.variation_count,
    
    -- Translation metrics
    translation_metrics.proportion_products_w_translations,
    translation_metrics.count_products_w_translations,
    
    -- Menu metrics
    category_metrics.menu_count,
    category_metrics.avg_menu_length,
    category_metrics.small_menu_count,
    category_metrics.menu_count_below5,
    category_metrics.menu_count_5to10,
    category_metrics.menu_count_10to20,
    category_metrics.menu_count_20to40,
    category_metrics.menu_count_above40,
    
    -- Cuisine metrics
    cuisine_count,
    
    -- Food characteristics metrics
    food_characteristic_count,

    -- Order metrics
    order_metrics.items_w_at_least_1_order_count,
    proportion_orders_with_img,
    proportion_orders_with_description,
    
    -- Image and description metrics
    id_metrics.img_coverage,
    id_metrics.description_coverage,
    id_metrics.img_count,
    id_metrics.description_count,
    
    -- Toppings metrics
--     dim_vendors.topping_master_category_count AS toppings_count,
    topping_metrics.toppings_count,
    topping_metrics.items_w_toppings_proportion,
    topping_metrics.topping_template_count,
    
    -- Compliaance metrics
    menu_metrics.expensive_product_count,
    menu_metrics.Foodpanda_count,

    --Conversion metrics
    cr_metrics.CR4_count,
    cr_metrics.CR3_count,
    CASE 
      WHEN CR3 > 2 THEN NULL 
      WHEN CR3 BETWEEN 1 AND 2 THEN 1
      ELSE CR3
    END AS CR3,
    
    -- Hero & Listing image metrics
    hl_img_metrics.hero_img_count,
    hl_img_metrics.hero_listing_count,
    
    -- Product translation by language
    translation_metrics.product_count,
    english_count,
    bangla_count,
    khmer_count,
    lao_count,
    chinese_count,
    bahasa_count,
    thai_count,
    russian_count,
    japanese_count,
    
    -- Translation coverage by language
    english_coverage,
    bangla_coverage,
    khmer_coverage,
    lao_coverage,
    chinese_coverage,
    bahasa_coverage,
    thai_coverage,
    russian_coverage,
    japanese_coverage,
    
    -- Chain metrics
    chain_metrics.chain_count,
    
    -- GMV metrics
    gmv_metrics.gmv_eur
  
  FROM pandata.dim_vendors
  LEFT JOIN menu_metrics
         ON menu_metrics.rdbms_id = dim_vendors.rdbms_id
        AND menu_metrics.vendor_code = dim_vendors.vendor_code
  LEFT JOIN category_metrics
         ON dim_vendors.rdbms_id = category_metrics.rdbms_id
        AND dim_vendors.vendor_code = category_metrics.vendor_code
  LEFT JOIN variation_metrics
         ON dim_vendors.rdbms_id = variation_metrics.rdbms_id
        AND dim_vendors.vendor_code = variation_metrics.vendor_code
  LEFT JOIN translation_metrics
         ON dim_vendors.rdbms_id = translation_metrics.rdbms_id
        AND dim_vendors.vendor_code = translation_metrics.vendor_code
  LEFT JOIN cr_metrics
         ON dim_vendors.rdbms_id = cr_metrics.rdbms_id
        AND dim_vendors.vendor_code = cr_metrics.vendor_code
  LEFT JOIN topping_metrics
         ON dim_vendors.rdbms_id = topping_metrics.rdbms_id
        AND dim_vendors.vendor_code = topping_metrics.vendor_code
  LEFT JOIN id_metrics
         ON id_metrics.rdbms_id = dim_vendors.rdbms_id
        AND id_metrics.vendor_code = dim_vendors.vendor_code
  LEFT JOIN order_metrics 
         ON dim_vendors.rdbms_id = order_metrics.rdbms_id
        AND dim_vendors.id = order_metrics.vendor_id
  LEFT JOIN hl_img_metrics
         ON dim_vendors.rdbms_id = hl_img_metrics.rdbms_id
        AND dim_vendors.vendor_code = hl_img_metrics.vendor_code
  LEFT JOIN chain_metrics
         ON dim_vendors.rdbms_id = chain_metrics.rdbms_id
        AND dim_vendors.chain_id = chain_metrics.chain_id
  LEFT JOIN gmv_metrics
         ON dim_vendors.rdbms_id = gmv_metrics.rdbms_id
        AND dim_vendors.vendor_code = gmv_metrics.vendor_code
  LEFT JOIN salesforce_contract AS sf_activation
         ON dim_vendors.rdbms_id = sf_activation.rdbms_id
        AND dim_vendors.vendor_code = sf_activation.vendor_code
  LEFT JOIN foodpanda_AAA_brands
        ON dim_vendors.rdbms_id = foodpanda_AAA_brands.rdbms_id
       AND dim_vendors.chain_id = foodpanda_AAA_brands.chain_id
  LEFT JOIN pandata_report.vendor_gmv_class
         ON dim_vendors.rdbms_id = vendor_gmv_class.rdbms_id
        AND dim_vendors.vendor_code = vendor_gmv_class.vendor_code
  LEFT JOIN pandata.dim_countries
         ON dim_vendors.rdbms_id = dim_countries.rdbms_id
  WHERE dim_vendors.is_active = true
    AND dim_vendors.is_vendor_testing = false
    AND dim_vendors.is_private = false
  ORDER BY rdbms_id, vendor_name
),

country_quantiles AS (
  SELECT
    rdbms_id,
    business_type,
    vendor_type,
    gmv_class,
    percentiles[offset(10)] as p10,
    percentiles[offset(25)] as p25,
    percentiles[offset(50)] as p50,
    percentiles[offset(75)] as p75,
    percentiles[offset(90)] as p90,
  FROM (SELECT rdbms_id, business_type, vendor_type, gmv_class, APPROX_QUANTILES(active_item_count, 100) AS percentiles FROM vendor_metrics GROUP BY rdbms_id, business_type, vendor_type, gmv_class)

),

country_metrics AS (
  SELECT
    vendor_metrics.rdbms_id,
    vendor_metrics.country,
    vendor_metrics.business_type,
    vendor_metrics.vendor_type,
    vendor_metrics.gmv_class,
    
    -- Basics --
    COUNT(DISTINCT vendor_code) AS vendor_count,
    AVG(country_quantiles.p50) AS median_product_count,
    SUM(active_item_count) AS sum_product_count,
    SUM(menu_count) AS sum_categories_count,
    
    -- Toppings --
    AVG(avg_topping_count) AS avg_toppings_per_template,
    SUM(items_w_toppings_proportion * active_item_count) AS products_w_toppings,
    
    -- Variations --
    AVG(proportion_products_w_variations) AS avg_share_variations,
    SUM(variation_count) AS total_variations,
    SUM(proportion_products_w_variations * active_item_count) AS products_w_variations,
        
    -- Images --
    COUNT(DISTINCT IF(img_coverage = 0, vendor_code, NULL)) AS DLP0_count,
    COUNT(DISTINCT IF(img_coverage > 0.50, vendor_code, NULL)) AS DLP50_count,
    COUNT(DISTINCT IF(img_coverage > 0.70, vendor_code, NULL)) AS DLP70_count,
    SUM(img_coverage * active_item_count) AS pproducts_w_photos,
    SUM(img_count) AS products_w_img_count,
    
    --Descriptions --
    COUNT(DISTINCT IF(description_coverage = 0, vendor_code,  NULL)) AS DLD0_count,
    COUNT(DISTINCT IF(description_coverage > 0.50, vendor_code, NULL)) AS DLD50_count,
    COUNT(DISTINCT IF(description_coverage > 0.70, vendor_code, NULL)) AS DLD70_count,
    SUM(description_coverage * active_item_count) AS products_w_description,
    SUM(description_count) AS products_w_description_count,
    
    -- Translations --
    COUNT(DISTINCT IF(proportion_products_w_translations = 0 OR proportion_products_w_translations IS NULL, vendor_code, NULL)) AS DLT0_count,
    COUNT(DISTINCT IF(proportion_products_w_translations > 0.50, vendor_code, NULL)) AS DLT50_count,
    COUNT(DISTINCT IF(proportion_products_w_translations > 0.70, vendor_code, NULL)) AS DLT70_count,
    SUM(count_products_w_translations) AS products_w_translation,
    
    -- Menu
    COUNTIF(small_menu_count > 0) AS vendors_w_small_menu_count,
    SUM(menu_count) AS menu_count,
    SUM(small_menu_count) AS small_menu_count,
    
    -- Compliance metrics
    COUNTIF(expensive_product_count > 0) AS vendor_w_expensive_product_count,
    SUM(expensive_product_count) AS expensive_product_count,
    COUNTIF(Foodpanda_count > 0) AS vendors_w_Foodpanda_count,
    SUM(Foodpanda_count) AS Foodpanda_count,
    
    -- Conversion metrics
    SUM(CR3) AS sum_CR3,
    SUM(IF(img_coverage = 0, CR3, 0)) AS sum_CR3_DLP0,
    SUM(IF(img_coverage > 0.70, CR3, 0)) AS sum_CR3_DLP70,
    
    SUM(IF(description_coverage = 0, CR3, 0)) AS sum_CR3_DLD0,
    SUM(IF(description_coverage > 0.70, CR3, 0)) AS sum_CR3_DLD70,
    
    SUM(IF(proportion_products_w_translations = 0, CR3, 0)) AS sum_CR3_DLT0,
    SUM(IF(proportion_products_w_translations > 0.70, CR3, 0)) AS sum_CR3_DLT70,
    
    SUM(IF(small_menu_count > 0, CR3, 0)) AS sum_CR3_SM,
    SUM(IF(small_menu_count = 0, CR3, 0)) AS sum_CR3_woSM,
    
    SUM(IF(expensive_product_count > 0, CR3, 0)) AS sum_CR3_EP,
    SUM(IF(expensive_product_count = 0, CR3, 0)) AS sum_CR3_woEP,
    
    -- Hero & Listing photos --
    COUNT(DISTINCT IF(hero_img_count > 0, vendor_code, NULL)) AS hero_coverage,
    COUNT(DISTINCT IF(hero_listing_count > 0, vendor_code, NULL)) AS listing_coverage,
    
    -- Alvee metrics
    SUM(items_w_at_least_1_order_count) AS items_w_at_least_1_order_count,
    SUM(CR3_count) AS sum_visit_count,
    
    -- Translation count
    SUM(product_count) AS translation_product_count,
    SUM(english_count) AS english_coverage,
    SUM(Bangla_count) AS bangla_coverage,
    SUM(Khmer_count) AS khmer_coverage,
    SUM(Lao_count) AS lao_coverage,
    SUM(Chinese_count) AS chinese_coverage,
    SUM(Bahasa_count) AS bahasa_coverage,
    SUM(Thai_count) AS thai_coverage,
    SUM(Russian_count) AS russian_coverage,
    SUM(Japanese_count) AS japanese_coverage,
    
    -- Active item count bin
    COUNT(DISTINCT IF(active_item_count <= 5 OR active_item_count IS NULL, vendor_code, NULL)) AS product_count_below_5,
    COUNT(DISTINCT IF(active_item_count BETWEEN 5 AND 10, vendor_code, NULL)) AS product_count_5_to_10,
    COUNT(DISTINCT IF(active_item_count BETWEEN 11 AND 20, vendor_code, NULL)) AS product_count_11_to_20,
    COUNT(DISTINCT IF(active_item_count BETWEEN 21 AND 40, vendor_code, NULL)) AS product_count_21_to_40,
    COUNT(DISTINCT IF(active_item_count > 40, vendor_code, NULL)) AS product_count_above_40,
    COUNT(DISTINCT IF(menu_count_below5>0, vendor_code, NULL)) AS percent_vendors_menu_below5,
    COUNT(DISTINCT IF(menu_count_5to10>0, vendor_code, NULL)) AS percent_vendors_menu_5to10,
    COUNT(DISTINCT IF(menu_count_10to20>0, vendor_code, NULL)) AS percent_vendors_menu_10to20,
    COUNT(DISTINCT IF(menu_count_20to40>0, vendor_code, NULL)) AS percent_vendors_menu_20to40,
    COUNT(DISTINCT IF(menu_count_above40>0, vendor_code, NULL)) AS percent_vendors_menu_above50,
    
    
    -- Chain metrics
    COUNTIF(chain_id IS NOT NULL) AS chain_vendors,
    COUNTIF(chain_id IS NULL OR chain_count = 0) AS independent_vendors,
    COUNTIF(chain_count <= 5) AS chain_vendors_below5,
    COUNTIF(chain_count BETWEEN 6 AND 10) AS chain_vendors_6to10,
    COUNTIF(chain_count BETWEEN 10 AND 20) AS chain_vendors_10to20,
    COUNTIF(chain_count BETWEEN 20 AND 50) AS chain_vendors_20to50,
    COUNTIF(chain_count BETWEEN 50 AND 100) AS chain_vendors_50to100,
    COUNTIF(chain_count > 100) AS chain_vendors_above100,
    
    -- GMV
    SUM(gmv_eur) AS sum_gmv,
    SUM(IF(img_coverage = 0, gmv_eur, 0)) AS sum_gmv_eur_DLP0,
    SUM(IF(img_coverage > 0.70, gmv_eur, 0)) AS sum_gmv_eur_DLP70,
    
    SUM(IF(description_coverage = 0, gmv_eur, 0)) AS sum_gmv_eur_DLD0,
    SUM(IF(description_coverage > 0.70, gmv_eur, 0)) AS sum_gmv_eur_DLD70,
    
    SUM(IF(proportion_products_w_translations = 0, gmv_eur, 0)) AS sum_gmv_eur_DLT0,
    SUM(IF(proportion_products_w_translations > 0.70, gmv_eur, 0)) AS sum_gmv_eur_DLT70,
    
    SUM(IF(small_menu_count > 0, gmv_eur, 0)) AS sum_gmv_eur_SM,
    SUM(IF(small_menu_count = 0, gmv_eur, 0)) AS sum_gmv_eur_woSM,
    
    SUM(IF(expensive_product_count > 0, gmv_eur, 0)) AS sum_gmv_eur_EP,
    SUM(IF(expensive_product_count = 0, gmv_eur, 0)) AS sum_gmv_eur_woEP,
    
    AVG(no_menu.no_menu_count) AS inactive_menu_count,
    
  FROM vendor_metrics
  LEFT JOIN topping_template_metrics
         ON vendor_metrics.rdbms_id = topping_template_metrics.rdbms_id
        AND vendor_metrics.business_type = topping_template_metrics.business_type
  LEFT JOIN no_menu
         ON vendor_metrics.rdbms_id = no_menu.rdbms_id
        AND vendor_metrics.business_type = no_menu.business_type
        AND vendor_metrics.vendor_type = no_menu.vendor_type
        AND vendor_metrics.gmv_class = no_menu.gmv_class
  LEFT JOIN country_quantiles
         ON vendor_metrics.rdbms_id = country_quantiles.rdbms_id
        AND vendor_metrics.business_type = country_quantiles.business_type
        AND vendor_metrics.vendor_type = country_quantiles.vendor_type
        AND vendor_metrics.gmv_class = country_quantiles.gmv_class
  GROUP BY rdbms_id, country, business_type, vendor_type, gmv_class
  ORDER BY rdbms_id, business_type
),

apac_metrics AS (
  WITH initial AS (
    SELECT
      0 AS rdbms_id,
      'APAC' AS country,
      vendor_metrics.business_type,
      vendor_metrics.vendor_type,
      vendor_metrics.gmv_class,

      -- Basics --
      COUNT(DISTINCT CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id)) AS vendor_count,
      AVG(country_quantiles.p50) AS median_product_count,
      SUM(active_item_count) AS sum_product_count,
      SUM(menu_count) AS sum_categories_count,

      -- Toppings --
      AVG(avg_topping_count) AS avg_toppings_per_template,
      SUM(items_w_toppings_proportion * active_item_count) AS products_w_toppings,

      -- Variations --
      AVG(proportion_products_w_variations) AS avg_share_variations,
      SUM(variation_count) AS total_variations,
      SUM(proportion_products_w_variations * active_item_count) AS products_w_variations,

      -- Images --
      COUNT(DISTINCT IF(img_coverage = 0, vendor_code, NULL)) AS DLP0_count,
      COUNT(DISTINCT IF(img_coverage > 0.50, vendor_code, NULL)) AS DLP50_count,
      COUNT(DISTINCT IF(img_coverage > 0.70, vendor_code, NULL)) AS DLP70_count,
      SUM(img_coverage * active_item_count) AS pproducts_w_photos,
      SUM(img_count) AS products_w_img_count,

      --Descriptions --
      COUNT(DISTINCT IF(description_coverage = 0, vendor_code,  NULL)) AS DLD0_count,
      COUNT(DISTINCT IF(description_coverage > 0.50, vendor_code, NULL)) AS DLD50_count,
      COUNT(DISTINCT IF(description_coverage > 0.70, vendor_code, NULL)) AS DLD70_count,
      SUM(description_coverage * active_item_count) AS products_w_description,
      SUM(description_count) AS products_w_description_count,

      -- Translations --
      COUNT(DISTINCT IF(proportion_products_w_translations = 0 OR proportion_products_w_translations IS NULL, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS DLT0_count,
      COUNT(DISTINCT IF(proportion_products_w_translations > 0.50, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS DLT50_count,
      COUNT(DISTINCT IF(proportion_products_w_translations > 0.70, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS DLT70_count,
      SUM(count_products_w_translations) AS products_w_translation,

      -- Menu
      COUNTIF(small_menu_count > 0) AS vendors_w_small_menu_count,
      SUM(menu_count) AS menu_count,
      SUM(small_menu_count) AS small_menu_count,

      -- Compliance metrics
      COUNTIF(expensive_product_count > 0) AS vendor_w_expensive_product_count,
      SUM(expensive_product_count) AS expensive_product_count,
      COUNTIF(Foodpanda_count > 0) AS vendors_w_Foodpanda_count,
      SUM(Foodpanda_count) AS Foodpanda_count,

      -- Conversion metrics
      SUM(CR3) AS sum_CR3,
      SUM(IF(img_coverage = 0, CR3, 0)) AS sum_CR3_DLP0,
      SUM(IF(img_coverage > 0.70, CR3, 0)) AS sum_CR3_DLP70,

      SUM(IF(description_coverage = 0, CR3, 0)) AS sum_CR3_DLD0,
      SUM(IF(description_coverage > 0.70, CR3, 0)) AS sum_CR3_DLD70,

      SUM(IF(proportion_products_w_translations = 0, CR3, 0)) AS sum_CR3_DLT0,
      SUM(IF(proportion_products_w_translations > 0.70, CR3, 0)) AS sum_CR3_DLT70,

      SUM(IF(small_menu_count > 0, CR3, 0)) AS sum_CR3_SM,
      SUM(IF(small_menu_count = 0, CR3, 0)) AS sum_CR3_woSM,

      SUM(IF(expensive_product_count > 0, CR3, 0)) AS sum_CR3_EP,
      SUM(IF(expensive_product_count = 0, CR3, 0)) AS sum_CR3_woEP,

      -- Hero & Listing photos --
      COUNT(DISTINCT IF(hero_img_count > 0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS hero_coverage,
      COUNT(DISTINCT IF(hero_listing_count > 0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS listing_coverage,

      -- Alvee metrics
      SUM(items_w_at_least_1_order_count) AS items_w_at_least_1_order_count,
      SUM(CR3_count) AS sum_visit_count,

      -- Translation count
      SUM(product_count) AS translation_product_count,
      SUM(english_count) AS english_coverage,
      SUM(Bangla_count) AS bangla_coverage,
      SUM(Khmer_count) AS khmer_coverage,
      SUM(Lao_count) AS lao_coverage,
      SUM(Chinese_count) AS chinese_coverage,
      SUM(Bahasa_count) AS bahasa_coverage,
      SUM(Thai_count) AS thai_coverage,
      SUM(Russian_count) AS russian_coverage,
      SUM(Japanese_count) AS japanese_coverage,

      -- Active item count bin
      COUNT(DISTINCT IF(active_item_count <= 5 OR active_item_count IS NULL, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS product_count_below_5,
      COUNT(DISTINCT IF(active_item_count BETWEEN 5 AND 10, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS product_count_5_to_10,
      COUNT(DISTINCT IF(active_item_count BETWEEN 11 AND 20, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS product_count_11_to_20,
      COUNT(DISTINCT IF(active_item_count BETWEEN 21 AND 40, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS product_count_21_to_40,
      COUNT(DISTINCT IF(active_item_count > 40, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS product_count_above_40,
      COUNT(DISTINCT IF(menu_count_below5>0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS percent_vendors_menu_below5,
      COUNT(DISTINCT IF(menu_count_5to10>0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS percent_vendors_menu_5to10,
      COUNT(DISTINCT IF(menu_count_10to20>0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS percent_vendors_menu_10to20,
      COUNT(DISTINCT IF(menu_count_20to40>0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS percent_vendors_menu_20to40,
      COUNT(DISTINCT IF(menu_count_above40>0, CONCAT(vendor_metrics.vendor_code, vendor_metrics.rdbms_id), NULL)) AS percent_vendors_menu_above50,

      -- Chain metrics
      COUNTIF(chain_id IS NOT NULL) AS chain_vendors,
      COUNTIF(chain_id IS NULL OR chain_count = 0) AS independent_vendors,
      COUNTIF(chain_count <= 5) AS chain_vendors_below5,
      COUNTIF(chain_count BETWEEN 6 AND 10) AS chain_vendors_6to10,
      COUNTIF(chain_count BETWEEN 10 AND 20) AS chain_vendors_10to20,
      COUNTIF(chain_count BETWEEN 20 AND 50) AS chain_vendors_20to50,
      COUNTIF(chain_count BETWEEN 50 AND 100) AS chain_vendors_50to100,
      COUNTIF(chain_count > 100) AS chain_vendors_above100,

      -- GMV
      SUM(gmv_eur) AS sum_gmv,
      SUM(IF(img_coverage = 0, gmv_eur, 0)) AS sum_gmv_eur_DLP0,
      SUM(IF(img_coverage > 0.70, gmv_eur, 0)) AS sum_gmv_eur_DLP70,

      SUM(IF(description_coverage = 0, gmv_eur, 0)) AS sum_gmv_eur_DLD0,
      SUM(IF(description_coverage > 0.70, gmv_eur, 0)) AS sum_gmv_eur_DLD70,

      SUM(IF(proportion_products_w_translations = 0, gmv_eur, 0)) AS sum_gmv_eur_DLT0,
      SUM(IF(proportion_products_w_translations > 0.70, gmv_eur, 0)) AS sum_gmv_eur_DLT70,

      SUM(IF(small_menu_count > 0, gmv_eur, 0)) AS sum_gmv_eur_SM,
      SUM(IF(small_menu_count = 0, gmv_eur, 0)) AS sum_gmv_eur_woSM,

      SUM(IF(expensive_product_count > 0, gmv_eur, 0)) AS sum_gmv_eur_EP,
      SUM(IF(expensive_product_count = 0, gmv_eur, 0)) AS sum_gmv_eur_woEP,

    FROM vendor_metrics
    LEFT JOIN topping_template_metrics
           ON vendor_metrics.rdbms_id = topping_template_metrics.rdbms_id
          AND vendor_metrics.business_type = topping_template_metrics.business_type
    LEFT JOIN country_quantiles
           ON vendor_metrics.rdbms_id = country_quantiles.rdbms_id
          AND vendor_metrics.business_type = country_quantiles.business_type
          AND vendor_metrics.vendor_type = country_quantiles.vendor_type
          AND vendor_metrics.gmv_class = country_quantiles.gmv_class
    GROUP BY business_type, vendor_type, gmv_class
   )
   SELECT
    initial.*,
    no_menu_apac.no_menu_count AS inactive_menu_count
   FROM initial
   LEFT JOIN no_menu_apac
          ON initial.business_type = no_menu_apac.business_type
         AND initial.vendor_type = no_menu_apac.vendor_type
         AND initial.gmv_class = no_menu_apac.gmv_class
)

SELECT
  DATE_TRUNC(CURRENT_DATE, WEEK) AS week,
  DATE_TRUNC(CURRENT_DATE, MONTH) AS month,
  apac_metrics.*
FROM apac_metrics

UNION ALL

SELECT
  DATE_TRUNC(CURRENT_DATE, WEEK) AS week,
  DATE_TRUNC(CURRENT_DATE, MONTH) AS month,
  country_metrics.*
FROM country_metrics
