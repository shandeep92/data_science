SELECT
  vendors.global_entity_id,
  vendors.uuid AS vendor_uuid,
  vendors.vendor_code,
  COUNT(DISTINCT products.title) AS product_count,
  COUNT(DISTINCT IF(images IS NOT NULL AND ARRAY_LENGTH(images) > 0 AND JSON_EXTRACT(images[safe_offset(0)], '$.url') IS NOT NULL, products.title, NULL)) AS products_w_image_count,    
  SAFE_DIVIDE(COUNT(DISTINCT IF(images IS NOT NULL AND ARRAY_LENGTH(images) > 0 AND JSON_EXTRACT(images[safe_offset(0)], '$.url') IS NOT NULL, products.title, NULL)), COUNT(DISTINCT products.title)) AS image_coverage,
  COUNT(DISTINCT IF(products.description IS NOT NULL AND LENGTH(products.description) > 0, products.title, NULL)) AS products_w_description_count,      
  SAFE_DIVIDE(COUNT(DISTINCT IF(products.description IS NOT NULL AND LENGTH(products.description) > 0, products.title, NULL)), COUNT(DISTINCT products.title)) AS description_coverage, 
FROM fulfillment-dwh-production.pandata_curated.pd_vendors AS vendors
LEFT JOIN UNNEST(vendors.menus) AS menus
LEFT JOIN UNNEST(vendors.menu_categories) AS menu_categories
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_products AS products
       ON menu_categories.uuid = products.pd_menu_category_uuid
LEFT JOIN UNNEST(products.variations) AS variations
WHERE vendors.is_active = true
  AND vendors.is_private = false
  AND vendors.is_test = false
  AND menus.is_active = true
  AND menus.is_deleted = false
  AND menu_categories.is_deleted = false
  AND (menu_categories.title != "Toppings"
   OR menu_categories.title IS NULL)
  AND products.is_active = true
  AND products.is_deleted = false
  AND variations.is_deleted = false
  AND (variations.master_category_title != "Toppings"
  OR variations.master_category_title IS NULL)
GROUP BY global_entity_id, vendor_uuid, vendor_code
