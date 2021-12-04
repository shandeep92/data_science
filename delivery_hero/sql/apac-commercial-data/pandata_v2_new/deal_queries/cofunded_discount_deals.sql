/*
Author: Abbhinaya Pragasam
*/

SELECT 
  pd_vendors.global_entity_id,
  dates.date,
  pd_vendors.vendor_code,
  pd_discounts.id,
  vendor_discounts.foodpanda_ratio,
  vendor_discounts.discount_type AS type,
  vendor_discounts.condition_type,
  ROW_NUMBER() OVER (PARTITION BY pd_vendors.global_entity_id, pd_vendors.vendor_code, dates.date ORDER BY pd_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
  TRUE AS is_vf_discount_deal_day,
  discount_mgmt_created_by_type
FROM fulfillment-dwh-production.pandata_curated.pd_vendors
LEFT JOIN pd_vendors.discounts AS vendor_discounts
LEFT JOIN pd_vendors.menu_categories
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts
       ON pd_discounts.uuid = vendor_discounts.pd_discount_uuid
LEFT JOIN fulfillment-dwh-production.pandata_curated.shared_dates AS dates
       ON dates.date BETWEEN vendor_discounts.start_date_local AND vendor_discounts.end_date_local
WHERE vendor_discounts.start_date_local IS NOT NULL
  AND ((pd_vendors.global_entity_id != 'FP_PK' AND (vendor_discounts.is_active OR vendor_discounts.is_active))
     OR pd_vendors.global_entity_id = 'FP_PK')
  AND (NOT vendor_discounts.is_deleted OR NOT vendor_discounts.is_deleted)
  AND dates.date >= '2021-06-01'
  AND dates.date <= CURRENT_DATE
  AND vendor_discounts.start_date_local <= dates.date
  AND vendor_discounts.end_date_local >= dates.date
  AND LOWER(pd_discounts.title) NOT LIKE '%testing%'
  AND LOWER(pd_discounts.description) NOT LIKE '%test%'
  AND pd_vendors.global_entity_id LIKE 'FP_%'
GROUP BY 1,2,3,4,5,6,7,pd_discounts.created_at_utc,9,10
HAVING foodpanda_ratio < 100 AND foodpanda_ratio > 0
