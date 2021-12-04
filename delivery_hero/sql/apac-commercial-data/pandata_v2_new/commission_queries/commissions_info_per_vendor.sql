-- owner: grace chan


SELECT 
    o.global_entity_id as globalid,
    o.country_name as country,
    o.vendor_code as vendorcode,
    o.vendor_name as vendorname,
    v.chain_code as chaincode,
    v.chain_name as chainname,
    sum(c.estimated_commission_base_eur) as comms_base,
    sum(c.estimated_commission_eur) as comms_rev,
    avg(c.estimated_commission_percentage)	as comms_percent,
    format_date("%Y-%m",date(o.created_date_utc)) as month,
    s.is_key_vip_account as keyaccount,
    s.owner_name as accountmanager,
    s.gmv_class as gmvclass,
    count(distinct o.id) as orders,
    cast(sum(a.gfv_eur)/count(distinct o.id) AS float64) AS aov_eur,
    sum(a.gfv_eur) as gfv_eur,
    sum(a.gmv_eur) as gmv_eur
FROM fulfillment-dwh-production.pandata_curated.pd_orders o
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_vendors v
    ON o.global_entity_id = v.global_entity_id
    AND o.vendor_code = v.vendor_code
    AND o.pd_vendor_uuid = v.uuid
LEFT JOIN fulfillment-dwh-production.pandata_report.order_commissions c
    ON o.global_entity_id = c.global_entity_id
    AND o.vendor_code = c.vendor_code
    AND o.uuid = c.order_uuid
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting a
    ON o.global_entity_id = a.global_entity_id
    AND o.uuid = a.uuid
LEFT jOIN fulfillment-dwh-production.pandata_curated.sf_accounts s
    ON o.global_entity_id = s.global_entity_id
    AND o.vendor_code = s.vendor_code
WHERE 
    o.is_own_delivery = true
    AND o.is_valid_order = true
    AND o.is_pickup = false
    AND o.is_test_order = false
    AND s.status IN ('New','Active')
    AND o.created_date_utc >= '2021-06-01'
    AND a.created_date_utc >= '2021-06-01'
    AND c.order_created_date_utc >= '2021-06-01'
    AND v.is_active = TRUE
    AND v.is_private = FALSE
    AND v.vertical_type = "restaurants"
    AND format_date("%Y-%m",date(o.created_date_utc)) = '2021-06'
    AND s.vertical = 'Restaurant'
    AND s.is_marked_for_testing_training = false
    AND o.global_entity_id = 'FP_TW'
--- AND o.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')
GROUP BY 1,2,3,4,5,6,10,11,12,13
ORDER BY gfv_eur DESC
