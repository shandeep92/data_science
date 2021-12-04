SELECT 
    p.global_entity_id
    ,   p.master_id
    ,   p.chain_product_id
    ,   p.product_name
    ,   master_category_names.level_one -- record nullable
    ,   master_category_names.level_two  -- record nullable
    ,   master_category_names.level_three -- record nullable
    ,   w.warehouse_id  -- record repeated
    ,   w.warehouse_name -- record repeated
    ,   v.platform_vendor_id -- record repeated (in warehouse_info, which is in products_v2)
    ,   sup.supplier_id -- record repeated
    ,   sup.supplier_name  -- record repeated
FROM `fulfillment-dwh-production.cl_dmart.products_v2` AS p
,UNNEST(warehouse_info) AS w
,UNNEST(w.vendor_info) AS v
,UNNEST(suppliers) AS sup
LIMIT 10
---------------------------------------------------