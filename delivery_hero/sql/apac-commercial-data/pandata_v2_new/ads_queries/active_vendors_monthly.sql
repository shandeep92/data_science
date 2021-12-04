WITH dvzone as (
SELECT
  global_entity_id,
  vendor_code,
  name,
  lg_zones.lg_zone_id,
  lg_zones.lg_zone_name,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones`
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE is_closest_point = TRUE
ORDER BY vendor_code asc),

months as (
SELECT
DISTINCT
format_date('%Y%m',iso_date) as year_month
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.dates`
WHERE iso_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 MONTH)
AND iso_date <= DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)
),


vendors AS (

SELECT

year_month,
v.global_entity_id,
v.vendor_code,
CASE WHEN v.location.city is null then "Null" else v.location.city end as city_name,
CASE WHEN dvz.lg_zone_name is null then "Null" else dvz.lg_zone_name end as hz_name,
CASE WHEN gmvc.gmv_class is null then "New Vendors" else gmvc.gmv_class end as gmv_class,
sf.owner_name as account_owner,
CASE WHEN sf.vendor_grade = 'AAA' then 'AAA' 
WHEN sf.vendor_grade is null then 'non-AAA'
else 'non-AAA' end as vendor_grade

FROM months m
CROSS JOIN  `fulfillment-dwh-production.pandata_curated.pd_vendors` v
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = v.global_entity_id and sf.vendor_code = v.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = v.global_entity_id and gmvc.vendor_code = v.vendor_code
LEFT JOIN dvzone dvz ON dvz.global_entity_id = v.global_entity_id AND dvz.vendor_code = v.vendor_code

WHERE v.global_entity_id = 'FP_TH'

ORDER BY 3,1 asc
),


active_in_month as (

SELECT 
global_entity_id,
month,
format_date('%Y%m',month) as year_month,
format_date('%b',month) as month_name,
vendor_code,
CASE 
when active_in_month IS FALSE THEN FALSE
when (not_private_in_month IS FALSE OR not_test_in_month IS FALSE) THEN FALSE
ELSE TRUE
END AS vendor_active

FROM 

(SELECT 
s.global_entity_id,
date_trunc(date,month) as month,
s.vendor_code,

logical_or(s.is_active IS TRUE) as active_in_month,
logical_or(s.is_private IS FALSE) as not_private_in_month, -- if private then inactive
logical_or(s.is_test IS FALSE) as not_test_in_month, -- if test then inactive

FROM `fulfillment-dwh-production.pandata_report.pandora_pd_vendors_active_status` s 

WHERE date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 1 MONTH),MONTH)


GROUP BY 1,2,3
ORDER BY 3,2 asc))


SELECT
global_entity_id,
year_month,
city_name,
hz_name,
gmv_class,
account_owner,
vendor_grade,
count(distinct vendor_code) as active_vendors

FROM 
(SELECT 
v.*,
a.vendor_active,
CASE 
WHEN a.vendor_active IS NULL THEN FALSE 
ELSE a.vendor_active
end as is_vendor_active

FROM vendors v
left join active_in_month a ON v.global_entity_id = a.global_entity_id and v.vendor_code = a.vendor_code and v.year_month = a.year_month

ORDER BY vendor_code,year_month asc)

WHERE is_vendor_active IS TRUE
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3,4,5,6,7
