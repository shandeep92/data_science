WITH chain_metrics AS (
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

order_metrics AS (
  SELECT
    fct_order_product_variations.rdbms_id,
    fct_order_product_variations.vendor_id,
    COUNT(DISTINCT fct_order_product_variations.product_variation_id) AS items_w_at_least_1_order_count,
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
    
    AND is_test_order = false
    AND fct_orders.is_gross_order = true
    
    AND is_product_active = true
    AND product_type != "Hidden Product"
    AND is_vendor_deleted = false
    AND is_product_deleted = false
    AND is_productvariation_deleted = false
  GROUP BY fct_order_product_variations.rdbms_id, fct_order_product_variations.vendor_id
),

salesforce_contract AS (
  SELECT
    a.owner_name,
    a.rdbms_id,
    a.sf_country_name AS country,
    SUBSTR(platform_performance_c.backend_id_c, 5) AS vendor_code,
    MAX(o.close_date_local) AS sf_activation_date,
    MAX(a.gmv_class) AS gmv_class,
  FROM pandata.sf_opportunities o
  LEFT JOIN pandata.sf_accounts a
         ON a.id = o.sf_account_id
  LEFT JOIN salesforce.platform_performance_c
         ON platform_performance_c.account_c = a.id 
  WHERE o.stage_name = 'Closed Won'
    AND business_type IN ('New Business','Owner Change','Win Back','Legal Form Change')
    AND vendor_code IS NOT NULL
  GROUP BY 1,2,3,4
),

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
    -- AND a.rdbms_id NOT IN (7,12,17,20)
  GROUP BY rdbms_id, chain_id, chain_name
  HAVING vendor_grade = "AAA"
),

-- foodora_sf_brands AS (
--   SELECT 
--     c.rdbms_id,
--     dim_vendors.chain_name,
--     dim_vendors.chain_id,
--     IF(MAX(aaa) =1, "AAA", NULL) AS vendor_grade
--   FROM pandata.dim_countries c
--   LEFT JOIN il_backend_latest.v_salesforce_dim_accounts a
--          ON a.rdbms_id = c.rdbms_id
--   LEFT JOIN pandata.dim_vendors
--          ON a.rdbms_id = dim_vendors.rdbms_id
--         AND a.vendor_code = dim_vendors.vendor_code
--   WHERE account_type = 'Partner Account'
--     AND c.rdbms_id IN (7,12,17,20)
--     AND a.account_status = 'Active'
--     AND dim_vendors.is_active = true
--     AND dim_vendors.is_private = false
--     AND dim_vendors.is_vendor_testing = false
--   GROUP BY rdbms_id, chain_id, chain_name
--   HAVING vendor_grade = "AAA"
-- ),

foodpanda_AAA_brands AS (
  SELECT
    *
  FROM dh_sf_brands
  WHERE chain_id IS NOT NULL
  -- UNION ALL
  -- SELECT
  --   *
  -- FROM foodora_sf_brands
  -- WHERE chain_id IS NOT NULL
),

id_metrics AS (
  WITH product_level AS (
    SELECT
      rdbms_id,
      vendor_code,
      product_id,
      COUNTIF(image_pathname IS NOT NULL) AS img_count,
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
)

SELECT
  dim_vendors.rdbms_id,
    dim_vendors.country_name,
  dim_vendors.vendor_code,
  dim_vendors.vendor_name,
  dim_vendors.chain_id,
  dim_vendors.chain_name,
  dim_vendors.business_type,
  id_metrics.product_count,
  SAFE_DIVIDE(items_w_at_least_1_order_count, dim_vendors.product_count) AS items_w_at_least_1_order_proportion,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE, COALESCE(DATE(sf_activation.sf_activation_date),dim_vendors.activation_date,NULL), DAY) <= 30 THEN "New vendor"
    ELSE "Existing vendor"
  END AS vendor_type,
  COALESCE(foodpanda_AAA_brands.vendor_grade, vendor_gmv_class.gmv_class, dim_vendors.sf_vendor_grade, sf_activation.gmv_class) AS gmv_class,
  CASE
    WHEN chain_count = 0 OR chain_count IS NULL THEN "Independent vendor"
    WHEN chain_count <=5 THEN "Chain vendor (5 outlets and below)"
    WHEN chain_count BETWEEN 6 AND 10 THEN "Chain vendor (6 to 10 outlets)"
    WHEN chain_count BETWEEN 10 AND 20 THEN "Chain vendor (10 to 20 outlets)"
    WHEN chain_count BETWEEN 20 AND 50 THEN "Chain vendor (20 to 50 outlets)"
    WHEN chain_count BETWEEN 50 AND 100 THEN "Chain vendor (50 to 100 outlets)"
    WHEN chain_count > 100 THEN "Chain vendor (above 100 outlets)"
  END AS chain_category,
  chain_count,
  img_count,
  description_count,
  img_coverage,
  description_coverage
FROM pandata.dim_vendors
LEFT JOIN chain_metrics
       ON dim_vendors.rdbms_id = chain_metrics.rdbms_id
      AND dim_vendors.chain_id = chain_metrics.chain_id
LEFT JOIN salesforce_contract AS sf_activation
       ON dim_vendors.rdbms_id = sf_activation.rdbms_id
      AND dim_vendors.vendor_code = sf_activation.vendor_code
LEFT JOIN foodpanda_AAA_brands
      ON dim_vendors.rdbms_id = foodpanda_AAA_brands.rdbms_id
     AND dim_vendors.chain_id = foodpanda_AAA_brands.chain_id
LEFT JOIN pandata_report.vendor_gmv_class
       ON dim_vendors.rdbms_id = vendor_gmv_class.rdbms_id
      AND dim_vendors.vendor_code = vendor_gmv_class.vendor_code
LEFT JOIN order_metrics 
       ON dim_vendors.rdbms_id = order_metrics.rdbms_id
      AND dim_vendors.id = order_metrics.vendor_id
LEFT JOIN id_metrics
       ON dim_vendors.rdbms_id = id_metrics.rdbms_id
      AND dim_vendors.vendor_code = id_metrics.vendor_code
WHERE is_active = true
  AND is_private = false
  AND is_vendor_testing = false
