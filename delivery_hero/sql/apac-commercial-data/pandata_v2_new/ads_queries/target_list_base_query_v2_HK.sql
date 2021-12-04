-----median of each vendor----
with mediantable as(
select 
* 
from 
(select 
global_entity_id, 
vendor_code,
percentile_disc (gfv_local,0.5) over (PARTITION BY global_entity_id,vendor_code) as median_local
from `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` 

where created_date_local >= DATE_TRUNC(CURRENT_DATE,MONTH)
and global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

and is_valid_order=true
) 
group by 1,2,3
),

---dim_vendors_1_zone-----
dvzone as (SELECT
  lg.global_entity_id,
  c.name as country_name,
  lg.vendor_code,
  lg.name as vendor_name,
  --v.vendor_id,
  v.chain_code,
  v.chain_name,
  v.location.city as city_name,
  lg_zones.lg_zone_id,
  lg_zones.lg_zone_name,
  v.vertical_type as vendor_type,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` lg
CROSS JOIN UNNEST(lg_zones) AS lg_zones
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v on lg.global_entity_id = v.global_entity_id and v.vendor_code = lg.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` c on lg.global_entity_id = c.global_entity_id
WHERE is_closest_point = TRUE
and v.is_active=true 
and v.is_test=false 
--and is_private = false 
and v.vertical_type = 'restaurants'
and lg.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

ORDER BY vendor_code asc),


pandapro as (
WITH pro_vendor_start_date AS (
SELECT 
  v.global_entity_id,
  v.vendor_code,
  MIN(d.start_date_local) AS earliest_start_date,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v, UNNEST(discounts) as d

WHERE 
  v.vertical_type = 'restaurants'
  AND d.is_subscription_discount

GROUP BY 1,2
),

latest_live_deals AS(
SELECT 
    c.name AS country_name,
    v.global_entity_id,
    v.vendor_code AS vendor_code,
    CONCAT(v.vendor_code,v.global_entity_id) AS vendor_code_country,
    v.name AS vendor_name,
  --  v.chain.code AS chain_code,
    v.chain_name AS chain_name,
    g.gmv_class,
    g.owner_name,
    'LIVE DEAL' AS deal_status,
    d.pd_discount_uuid,
    d.discount_type,
    d.title AS discount_title,
    d.start_date_local,
    d.end_date_local,
    d.created_at_utc,
    p.earliest_start_date AS vendor_pro_live_date,
    p.earliest_start_date >= DATE_SUB(CURRENT_DATE,INTERVAL 30 DAY) AS new_pro_vendor,
    v.is_private,
    DATE_DIFF(end_date_local,start_date_local, DAY) AS duration,
    d.foodpanda_ratio,
    ROW_NUMBER() OVER(PARTITION BY v.vendor_code,v.global_entity_id ORDER BY v.created_at_utc desc ) as rn
  FROM `fulfillment-dwh-production`.pandata_curated.pd_vendors v, UNNEST(discounts) as d
  LEFT JOIN `fulfillment-dwh-production`.pandata_curated.shared_countries c
         ON c.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production`.pandata_curated.sf_accounts g
         ON g.vendor_code = v.vendor_code
        AND g.global_entity_id = v.global_entity_id
  LEFT JOIN pro_vendor_start_date AS p
         ON v.global_entity_id = p.global_entity_id
        AND v.vendor_code = p.vendor_code      
  WHERE v.is_active 
    AND v.is_test = FALSE
    AND v.vertical_type ='restaurants'
    AND d.is_subscription_discount
    AND d.is_Active 
    AND d.is_deleted = FALSE
    AND d.end_date_local >= CURRENT_DATE -1
    AND d.start_date_local <= CURRENT_DATE -1
),

latest_future_deals AS(
SELECT 
    c.name AS country_name,
    v.global_entity_id,
    v.vendor_code AS vendor_Code,
    v.name AS vendor_name,
    v.chain_name AS chain_name,
    g.gmv_class,
    g.owner_name,
    'FUTURE DEAL' AS deal_status,
    d.pd_discount_uuid,
    d.discount_type,
    d.title AS discount_title,
    d.start_date_local,
    d.end_date_local,
    d.created_at_utc,
    p.earliest_start_date AS vendor_pro_live_date, 
    p.earliest_start_date >= DATE_SUB(CURRENT_DATE,INTERVAL 30 DAY) AS new_pro_vendor,
    v.is_private,
    DATE_DIFF(end_date_local,start_date_local, DAY) AS duration,
    d.foodpanda_ratio,
    ROW_NUMBER() OVER(PARTITION BY v.vendor_code,v.global_entity_id ORDER BY d.created_at_utc desc ) as rn
  FROM `fulfillment-dwh-production`.pandata_curated.pd_vendors v, UNNEST(discounts) as d
  LEFT JOIN `fulfillment-dwh-production`.pandata_curated.shared_countries c
         ON c.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production`.pandata_curated.sf_accounts g
         ON g.vendor_code = v.vendor_code
        AND g.global_entity_id = v.global_entity_id
  LEFT JOIN pro_vendor_start_date AS p
         ON v.global_entity_id = p.global_entity_id
        AND v.vendor_code = p.vendor_code      
  WHERE 
     v.is_test = FALSE
    AND v.vertical_type = 'restaurants'
    AND d.is_subscription_discount
    AND v.is_active
    AND d.is_active
    AND d.is_deleted = FALSE
    AND d.end_date_local >= CURRENT_DATE -1
    AND d.start_date_local > CURRENT_DATE -1
    AND CONCAT(v.vendor_code,v.global_entity_id) NOT IN (SELECT DISTINCT vendor_code_country FROM latest_live_deals )
),

final as (
SELECT 
  country_name,
  global_entity_id,
  vendor_code,
  vendor_name,
  chain_name,
  gmv_class,
  owner_name,
  is_private,
  deal_status,
  CASE WHEN discount_title LIKE '%一送一%' THEN 'BOGO'
       WHEN discount_title NOT LIKE '%折%' and COUNTRY_name = 'Taiwan' then 'FREE GIFT' ELSE discount_type END as discount_type,
  discount_title,
  start_date_local,
  end_date_local,
  created_at_utc,
  new_pro_vendor,
  vendor_pro_live_date,
  duration,
  DATE_DIFF(end_date_local,start_date_local,month) AS duration_months,
  foodpanda_ratio AS discount_ratio,
  CASE WHEN foodpanda_ratio = 100 THEN 'FOODPANDA FUNDED'
       WHEN foodpanda_ratio = 0 THEN 'VENDOR FUNDED'
       ELSE 'COFUNDED'
  END AS funded_by

FROM latest_live_deals
WHERE 1=1
AND rn = 1

UNION ALL
SELECT 
  country_name,
  global_entity_id,
  vendor_code,
  vendor_name,
  chain_name,
  gmv_class,
  owner_name,
  is_private,
  deal_status,
  CASE  WHEN discount_title LIKE '%一送一%' THEN 'BOGO'
        WHEN discount_title NOT LIKE '%折%'  and COUNTRY_name = 'Taiwan' then 'FREE GIFT' ELSE discount_type END as discount_type,
  discount_title,
  start_date_local,
  end_date_local,
  created_at_utc,
  new_pro_vendor,
  vendor_pro_live_date,
  duration,
  DATE_DIFF(end_date_local,start_date_local,month) AS duration_months,
  foodpanda_ratio AS discount_ratio,
  CASE WHEN foodpanda_ratio = 100 THEN 'FOODPANDA FUNDED'
       WHEN foodpanda_ratio = 0 THEN 'VENDOR FUNDED'
       ELSE 'COFUNDED'
  END AS funded_by  
FROM latest_future_deals) 

SELECT
*
FROM (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date_local ASC) as seq
FROM final
)

where seq = 1
and end_date_local >= CURRENT_DATE()),

----fct_orders----
fct_orders as (
SELECT 
o.global_entity_id,
o.vendor_code,
format_date('%Y-%m',o.created_date_local) as order_month,
COUNT(DISTINCT case when o.is_gross_order=true then o.id end) AS gross_order,
COUNT(DISTINCT case when o.is_valid_order=true then o.id end) as successful_order ,
COUNT (DISTINCT case when pdo.is_failed_order_vendor=true then o.id end) as vendor_fails,
COUNT(DISTINCT case when o.is_valid_order= true and o.is_discount_used IS TRUE and o.discount.current_foodpanda_ratio < 100 then o.id end) as discounted_order ,
SUM (case when o.is_valid_order=true then o.gfv_local end) as gfv_local_order_mth,
AVG(case when o.is_valid_order = TRUE then o.gfv_local end) as avg_gfv_local_order_mth,
COUNT (DISTINCT case when o.is_valid_order=true then o.pd_customer_uuid end) as unique_customer,
COUNT (DISTINCT case when o.is_valid_order=true and o.is_first_valid_order_platform=true then o.pd_customer_uuid end) as new_to_fp_customer,
COUNT (DISTINCT case when o.is_valid_order=true and o.is_first_valid_order_with_this_vendor =true then o.pd_customer_uuid end) as new_to_rest_customer

FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pdo on o.global_entity_id = pdo.global_entity_id and o.uuid = pdo.uuid

where o.created_date_local >= DATE_TRUNC(CURRENT_DATE,MONTH)
and pdo.created_date_utc >= DATE_SUB(DATE_TRUNC(CURRENT_DATE,MONTH),INTERVAL 2 DAY)
and o.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')


GROUP BY 1,2,3
),

prev_mth as (
SELECT 
global_entity_id,
vendor_code,
format_date('%Y-%m',o.created_date_local) as order_month,
SUM (o.gfv_local) as gfv_local_prev_mth,

FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o

where o.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE,MONTH),INTERVAL 1 MONTH) AND o.created_date_local <= DATE_SUB(DATE_TRUNC(CURRENT_DATE,MONTH),INTERVAL 1 DAY)
and o.is_valid_order = TRUE 
and o.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

GROUP BY 1,2,3
),

----commercial metrics by vendor----
orders as (
Select 
v.country_name,
v.global_entity_id,
v.vendor_name,
--v.vendor_id,
v.vendor_code,
v.city_name,
--v.rating,
v.lg_zone_name as hurrier_zone,
v.lg_zone_id as zone_id,
v.chain_name,
sfv.owner_name,
gmvc.gmv_class,
o.order_month,
v.vendor_type,
--v.is_loyalty_enabled,
--v.loyalty_percentage,
o.gross_order,
case when o.successful_order IS NULL then 0 ELSE o.successful_order END AS successful_order,
case when o.vendor_fails IS NULL then 0 ELSE o.vendor_fails END AS vendor_fails,
o.discounted_order,
case when o.gfv_local_order_mth IS NULL then 0 ELSE o.gfv_local_order_mth END AS gfv_local_order_mth,
case when pm.gfv_local_prev_mth IS NULL then 0 ELSE pm.gfv_local_prev_mth END AS gfv_local_prev_mth ,
case when o.unique_customer IS NULL then 0 ELSE o.unique_customer END AS unique_customer,
o.new_to_fp_customer,
o.new_to_rest_customer

FROM dvzone v--, unnest(lg_zone_ids) as lg_zone_id
LEFT JOIN fct_orders o on v.global_entity_id = o.global_entity_id and v.vendor_code = o.vendor_code
LEFT JOIN prev_mth pm on v.global_entity_id = pm.global_entity_id and v.vendor_code = pm.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` sfv ON sfv.global_entity_id=v.global_entity_id AND sfv.vendor_code=v.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = v.global_entity_id and gmvc.vendor_code = v.vendor_code
--left join pandata.lg_zones hz on v.rdbms_id = hz.rdbms_id and v.lg_zone_id= hz.id

),


-- conversion_rate and sessions -- 
conversion as (
SELECT
vcr.global_entity_id,
vcr.country,
vcr.vendor_code,
format_date('%Y-%m', vcr.date_local) as order_month,
Greatest(SUM(count_of_shop_list_loaded),0) as cr2_start,
Greatest(SUM(count_of_shop_menu_loaded),0) as cr3_start,
Greatest(SUM(count_of_transaction),0) as cr4_end
FROM `fulfillment-dwh-production.pandata_report.product_vendor_session_metrics` vcr
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id=vcr.global_entity_id and v.vendor_code=vcr.vendor_code

WHERE vcr.date_local >= DATE_TRUNC(CURRENT_DATE,MONTH)

and v.is_active=true and v.is_test=false and v.vertical_type = 'restaurants'
and vcr.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

GROUP BY 1,2,3,4--,5
),

--- mode----
modetable as (
select 
order_month,
om.global_entity_id,
om.vendor_code,
om.gfv_round as lowest_mode_local,
seqnum
from 

(select
format_date('%Y-%m', o.created_date_local) as order_month,
v.global_entity_id,
v.vendor_code,
round(o.gfv_local,0) as gfv_round,
COUNT(DISTINCT case when o.is_valid_order=true then o.id end) as successful_order,
row_number () over (Partition by v.global_entity_id, v.vendor_code order by v.global_entity_id, v.vendor_code, "successful_order" desc, "gfv_round" asc) as seqnum
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o on v.global_entity_id=o.global_entity_id and v.vendor_code=o.vendor_code

WHERE o.created_date_local >= DATE_TRUNC(CURRENT_DATE(),MONTH)
and o.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

and  v.is_active=true and v.is_test=false and v.vertical_type = 'restaurants'

group by 1,2,3,4
order by v.global_entity_id, v.vendor_code, successful_order desc, gfv_round asc
) om
where seqnum = 1),


-- OPENING HOURS --
open_hours as (SELECT
global_entity_id,
vendor_code,
format_date('%Y-%m',report_date) as year_month,
days_in_month,
IFNULL(SAFE_DIVIDE(SUM(actual_open_hours),days_in_month),0) as avg_open_hrs_per_day

FROM (

SELECT 
*,
DATE_DIFF(last_day_of_month, first_day_of_month, DAY) + 1 as days_in_month,
CASE WHEN IFNULL(total_scheduled_open_hours, 0) - (IFNULL(closed_hours, 0) + IFNULL(closed_hours_on_special_day, 0)) < 0 THEN 0 ELSE IFNULL(total_scheduled_open_hours, 0) - (IFNULL(closed_hours, 0) + IFNULL(closed_hours_on_special_day, 0)) END AS actual_open_hours

FROM(

SELECT 
global_entity_id,
vendor_code,
report_date,
IFNULL(SAFE_DIVIDE(IFNULL(SUM(total_scheduled_open_seconds),0), 3600), 0) AS total_scheduled_open_hours, /* if it's a special day closure (ie full day closure), we will record total_special_day_closed_minutes*/
IFNULL(SAFE_DIVIDE(SUM(IF(total_special_day_closed_seconds IS NOT NULL, total_special_day_closed_seconds, total_unavailable_seconds)), 3600), 0) AS closed_hours, /*if it is a special delivery day, we will calculate the closed minutes by taking the difference between scheduled open minutes and special day delivery minutes*/
IFNULL(SAFE_DIVIDE(SUM(IF(total_special_day_delivery_seconds IS NOT NULL AND (total_scheduled_open_seconds - total_special_day_delivery_seconds) > 0, total_scheduled_open_seconds - total_special_day_delivery_seconds , NULL)), 3600),0) AS closed_hours_on_special_day,

DATE_TRUNC(report_date,MONTH) as first_day_of_month,

CASE WHEN CURRENT_DATE() <= DATE_SUB(DATE_ADD(DATE_TRUNC(report_date,MONTH), INTERVAL 1 MONTH),INTERVAL 1 DAY)  THEN DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) ELSE DATE_SUB(DATE_ADD(DATE_TRUNC(report_date,MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) END AS last_day_of_month

FROM `fulfillment-dwh-production.pandata_report.vendor_offline`

WHERE report_date >= DATE_TRUNC(CURRENT_DATE(),MONTH) and report_date <= DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)

GROUP BY 1,2,3 
ORDER BY 2,3 ASC)
)

GROUP BY 1,2,3,4
ORDER BY 1,2,3 asc),



-- MENU INFORMATION --
/*menu as (
SELECT
v.global_entity_id,
v.vendor_code,
count (distinct case when p.is_active is true and am.is_deleted is false then am.pd_menu_product_uuid	 end ) as total_product,
count (distinct case when p.is_active is true and am.is_deleted is false and has_dish_image is true then am.pd_menu_product_uuid end) as has_picture,
count (distinct case when p.is_active is true and am.is_deleted is false and p.description is not null then am.pd_menu_product_uuid end ) as has_description,

FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v , UNNEST(v.menu_categories) mc
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_products` p on p.global_entity_id = v.global_entity_id and mc.uuid = p.pd_menu_category_uuid
LEFT JOIN UNNEST(p.products_agg_menus) as am

WHERE v.is_active is true and v.is_test is false and mc.is_deleted is false
GROUP BY 1,2),*/

/*
menu as (SELECT
rdbms_id,
vendor_code,
count (distinct case when is_product_active is true and is_product_deleted is false then product_id end ) as total_product,
count (distinct case when is_product_active is true and is_product_deleted is false and has_dish_image is true then product_id end) as has_picture,
count (distinct case when is_product_active is true and is_product_deleted is false and product_description is not null then product_id end ) as has_description,
FROM `fulfillment-dwh-production.pandata_curated.pd_products` p
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v on p.global_entity_id = v.global_entity_id and p.
WHERE is_active is true and is_vendor_deleted is false and is_menu_deleted is false
GROUP BY 1,2),
*/
----joker bookings----
jokerbookings as (
select 
global_entity_id,
vendor_code,
cpu_billing.units
from `fulfillment-dwh-production.pandata_curated.pps_bookings`
where type = 'joker'
and status = 'open'
and global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

),

----cpc bookings----
cpcbookings as (
select 
bk.global_entity_id,
bk.vendor_code,
blcpc.click_price,
blcpc.initial_budget as initial_budget_local
from `fulfillment-dwh-production.pandata_curated.pps_bookings`  bk, UNNEST(bk.cpc_billings) blcpc

where bk.billing_type = 'CPC'
and bk.status = 'open'
and bk.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

group by 1,2,3,4
),


---- cpc ROI----

cpcroi as (WITH cpc_roi as (/*dim_vendors_1_zone*/
with dvzone as (
SELECT
  global_entity_id,
  vendor_code,
  name as vendor_name,
  lg_zones.lg_zone_id,
  lg_zones.lg_zone_name,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones`
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE is_closest_point = TRUE
and global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

ORDER BY vendor_code asc
),


cpcbookings as(
select 
bk.global_entity_id,
c.name as common_name,
bk.uuid as booking_id,
gmvc.gmv_class,
v.name as vendor_name,
bk.vendor_code,
v.chain_name,
v.chain_code,
v.location.city as city_name,
dvz.lg_zone_name as hurrier_zone,
bk.user,
format_date('W%V-%Y', date(bk.created_at_utc)) as booking_date,
bk.type,
bk.status,
cpc.initial_budget as initial_budget_local,
cpc.click_price as click_price_local,
cpc.initial_budget/cpc.click_price as budgeted_clicks,
date(bk.started_at_utc) as start_date,
case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
case 
when sf.vendor_grade = 'AAA' then 'AAA' 
when sf.vendor_grade is null then 'non-AAA'
else 'non-AAA' end as vendor_grade,
count (distinct cpc.uuid)as promo_areas_booked,
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks`  cl on cl.pps_item_uuid=bk.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id = bk.global_entity_id and v.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join dvzone dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code

where bk.uuid is not null
and bk.type = 'organic_placements'
and bk.billing_type = 'CPC'
and bk.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

/*1 line per month live*/
cpcbookings_array as(
SELECT
*,
GENERATE_DATE_ARRAY(DATE_TRUNC(start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM cpcbookings),
finalbookings as(
select 
global_entity_id,
common_name as country,
booking_id,
gmv_class,
vendor_grade,
vendor_name,
vendor_code,
chain_name,
chain_code,
city_name,
hurrier_zone,
user,
booking_date as booking_week,
type,
status,
start_date,
end_date,
DATE_SUB(DATE_TRUNC(DATE_ADD(parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) as last_day,
format_date('%b', datelive) as month,
format_date("%Y-%m",datelive) as yearmonth_live,
promo_areas_booked,
click_price_local,
budgeted_clicks,
initial_budget_local,
from cpcbookings_array ca, UNNEST(datelive_nested) AS datelive
),

/*clicks & orders*/
clicks as (
SELECT DISTINCT
bk.uuid as booking_id,
format_date ('%Y-%m',date(cpc.created_at_utc)) as click_month,
count (distinct cpc.pps_item_uuid) as active_areas,
SUM(orders) AS cpc_orders,
SUM(quantity) AS spent_clicks
FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) bil
JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS cpc 
ON bil.uuid = cpc.pps_item_uuid
group by 1,2
),
    
exchangerate as(
select 
global_entity_id,
format_timestamp ('%Y-%m',fx_rate_date) as yearmonth,
AVG (fx_rate_eur) as exchange_rate
from 
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso
where fx.fx_rate_date >= '2020-01-01')
group by 1,2
)
    
/*FINAL QUERY*/
SELECT 
global_entity_id,
vendor_code,
yearmonth_live,
month,
sum(cpc_rev_local) as cpc_rev_local,
sum(final_spent_clicks) as total_clicks_billed,
sum(cpc_orders) as total_cpc_orders

FROM (
 
    select *,
    case when final_spent_clicks*click_price_local > initial_budget_local then cast(initial_budget_local as float64) else cast(final_spent_clicks*click_price_local as float64) end as cpc_rev_local,
    cast((case when final_spent_clicks*click_price_local > initial_budget_local then initial_budget_local else final_spent_clicks*click_price_local end)/exchange_rate as float64) as cpc_rev_eur,
    
    FROM (

    select 
    fb.* except (promo_areas_booked),
    case when fb.start_date < DATE_TRUNC(fb.last_day,MONTH) then date_trunc(fb.last_day,MONTH) else fb.start_date end as use_start_date,
    case when fb.end_date > fb.last_day then last_day else end_date END AS use_end_date,
    initial_budget_local/exr.exchange_rate as initial_budget_eur,
    case when spent_clicks > budgeted_clicks then budgeted_clicks else spent_clicks end as final_spent_clicks,
    fb.promo_areas_booked,
    exr.exchange_rate,
    c.active_areas,
    c.cpc_orders,
    c.spent_clicks,
    fo.avg_gfv_local_order_mth
    from finalbookings fb
    left join clicks c on c.click_month = fb.yearmonth_live and c.booking_id=fb.booking_id
    left join exchangerate exr on exr.global_entity_id = fb.global_entity_id and exr.yearmonth =fb.yearmonth_live
    left join fct_orders fo on fb.yearmonth_live = fo.order_month and fb.global_entity_id = fo.global_entity_id and fb.vendor_code = fo.vendor_code

)

WHERE yearmonth_live = format_date('%Y-%m',DATE_TRUNC(CURRENT_DATE(),MONTH))


ORDER BY global_entity_id,vendor_code,yearmonth_live)
GROUP BY 1,2,3,4)

SELECT c.*, 
fo.avg_gfv_local_order_mth,
SAFE_DIVIDE(total_cpc_orders * fo.avg_gfv_local_order_mth,cpc_rev_local) as cpc_roi

FROM cpc_roi c
left join fct_orders fo on c.yearmonth_live = fo.order_month and c.global_entity_id = fo.global_entity_id and c.vendor_code = fo.vendor_code),


----cpp bookings----
cppbookings as (
select 
bk.global_entity_id,
bk.vendor_code,
SUM(case when bk.year_month = cast(format_date('%Y%m',date_trunc(current_date,MONTH)) as INT64) then bk.cpp_billing.price end) as order_mth_cpp_sold_local,
SUM(case when bk.year_month = cast(format_date('%Y%m',date_add(date_trunc(current_date,MONTH),INTERVAL 1 MONTH)) as INT64) then bk.cpp_billing.price end) as current_mth_cpp_sold_local,
SUM(case when bk.year_month = cast(format_date('%Y%m',date_add(date_trunc(current_date,MONTH),INTERVAL 2 MONTH)) as INT64) then bk.cpp_billing.price end) as next_mth_cpp_sold_local,

from `fulfillment-dwh-production.pandata_curated.pps_bookings`  bk
where bk.billing_type = 'CPP'
and type = 'premium_placements'
and bk.status IN ('open','new')
and bk.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')
and bk.year_month >= cast(format_date('%Y%m',date_trunc(current_date,MONTH)) as INT64)
and bk.year_month <= cast(format_date('%Y%m',date_add(date_trunc(current_date,MONTH),INTERVAL 2 MONTH)) as INT64)
group by 1,2
order by 1,2 asc
),

---eligible vendors---
vendorslive as(
select
dv.global_entity_id,
c.name as common_name,
dv.lg_zone_id,
count (distinct dv.vendor_code) as vendors_available
from dvzone dv
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id = dv.global_entity_id
group by 1,2,3
),

---click inventory---
/*clickinv as ( 
select 
country,
lg.global_entity_id,
format_date('%Y-%m', date) as order_month,
lg.lg_zone_name as zone_name,
lg.lg_city_name as lg_city_name,
lg.lg_zone_id as zone_id,
sum (case when (vendor_click_origin = 'list' or vendor_click_origin = 'List') and safe_cast (vendor_position as INt64) <11 then clicks end ) as click_inv
from `dhh-digital-analytics-dwh.shared_views_to_pandata.click_positions_apac` cl
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.name=cl.country
left join (
SELECT
DISTINCT
shared_countries.global_entity_id,
lg_cities.id AS lg_city_id,
lg_cities.name AS lg_city_name,
lg_zones.id AS lg_zone_id,
lg_zones.name AS lg_zone_name,

FROM `fulfillment-dwh-production.pandata_curated.lg_countries` AS lg_countries
LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries ON lg_countries.iso = shared_countries.country_code_iso
LEFT JOIN UNNEST(lg_countries.cities) AS lg_cities
LEFT JOIN UNNEST(lg_cities.zones) AS lg_zones
WHERE lg_zones.id IS NOT NULL 
AND lg_zones.is_active IS TRUE
AND lg_countries.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')) lg on c.global_entity_id = lg.global_entity_id and cl.zone_id= lg.lg_zone_id

where
(country = 'Singapore' or country = 'Hong Kong' or country = 'Philippines' or country =  'Thailand' or country =  'Taiwan' or country = 'Pakistan' or country = 'Bangladesh' or country = 'Cambodia' or country = 'Myanmar' or country = 'Laos' or country = 'Malaysia' or country = 'Japan')

and date >= DATE_TRUNC(CURRENT_DATE(),MONTH)
--and lc.name is not null
group by 1,2,3,4,5,6
),*/

--cpc recommendation---

cpcrec as (
SELECT 
pd_rdbms_id,
country,
lg_zone_id as zone_id,
city_zone_name,
clicks_predicted_monthly,
vendor_clicks_predicted_monthly
FROM fulfillment-dwh-staging.pandata_app.click_estimation_hk_v2
order by lg_zone_id asc),


-- Active VF discounts --

active_deals AS (
SELECT
ve.global_entity_id,
ve.vendor_code,
ve.name as vendor_name,
ve.chain_code,
ve.chain_name,
count(distinct d.uuid) as no_of_deals,

FROM  `fulfillment-dwh-production.pandata_curated.pd_discounts`  d
LEFT JOIN  `fulfillment-dwh-production.pandata_curated.pd_vendors` ve ON ve.global_entity_id = d.global_entity_id AND ve.vendor_code = d.vendor_code

WHERE
d.is_active IS TRUE
AND d.is_deleted IS FALSE
AND ve.is_test IS FALSE
AND ve.is_active IS TRUE
AND d.expedition_types != "pickup"
AND ve.vertical_type='restaurants'
AND d.foodpanda_ratio < 100
AND d.end_date_local >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 MONTH)
and d.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')

group by 1,2,3,4,5
order by 1,2 asc
)


---Orders + CVR + Mode + Median + Open hours + Menu + CPP + CPC ----

Select 
DISTINCT
orders.country_name,
orders.global_entity_id,
orders.vendor_name,
orders.vendor_code,
orders.city_name,
orders.hurrier_zone,
orders.chain_name,
orders.owner_name,
orders.gmv_class,
orders.order_month,
orders.vendor_type,
--orders.is_loyalty_enabled,
--orders.loyalty_percentage,
--concat(case when orders.is_loyalty_enabled is FALSE then "No" else "Yes" end," , ",case when orders.loyalty_percentage is null then 0 else orders.loyalty_percentage end) as loyalty_enabled_percentage,
jbk.units as pandabox_units_live,
order_mth_cpp_sold_local,
current_mth_cpp_sold_local,
next_mth_cpp_sold_local,
concat (cpc.click_price,' , ', cpc.initial_budget_local) as CPC_bid_budget,
CASE WHEN cpr.cpc_roi IS NULL THEN 0 ELSE cpr.cpc_roi END AS cpc_roi_order_mth,
(CASE WHEN SAFE_DIVIDE(cr.cr4_end, cr.cr3_start) IS NULL THEN 0 ELSE SAFE_DIVIDE(cr.cr4_end, cr.cr3_start) END) * (CASE WHEN SAFE_DIVIDE(gfv_local_prev_mth, successful_order) IS NULL THEN 0 ELSE SAFE_DIVIDE(gfv_local_prev_mth, successful_order) END) as cpcfactor_z,
orders.successful_order,
orders.gfv_local_order_mth,
orders.gfv_local_prev_mth,
CASE WHEN SAFE_DIVIDE(gfv_local_order_mth,gfv_local_prev_mth) - 1 IS NULL THEN 0 ELSE SAFE_DIVIDE(gfv_local_order_mth,gfv_local_prev_mth) - 1 END as gfv_change,
orders.new_to_rest_customer,
SAFE_DIVIDE(new_to_rest_customer, unique_customer) as NC_proportion,
SAFE_DIVIDE(successful_order, unique_customer) as frequency,
cr.cr2_start as shop_loads,
cr.cr3_start as sessions,
CASE WHEN SAFE_DIVIDE(cr.cr3_start, cr.cr2_start) IS NULL THEN 0 ELSE SAFE_DIVIDE(cr.cr3_start, cr.cr2_start) END as click_through_rate,
CASE WHEN SAFE_DIVIDE(cr.cr4_end, cr.cr3_start) IS NULL THEN 0 ELSE SAFE_DIVIDE(cr.cr4_end, cr.cr3_start) END as conversion_rate,
SAFE_DIVIDE(gfv_local_order_mth, successful_order) as avb_local,
CASE WHEN SAFE_DIVIDE(vendor_fails,gross_order) IS NULL THEN 0 ELSE SAFE_DIVIDE(vendor_fails,gross_order) END as fail_rate,
t.avg_open_hrs_per_day,
--t.total_open as total_open_hours,
--COALESCE(t.total_open,0) - COALESCE(t.total_closed,0) as actual_open_hours,
--CASE WHEN SAFE_DIVIDE(t.total_open - t.total_closed , no_of_days) IS NULL THEN 0 ELSE SAFE_DIVIDE(t.total_open - t.total_closed , no_of_days) END as avg_daily_open_hours,
/*SAFE_DIVIDE(t.total_closed, t.total_open + t.total_closed) as closed_perc,
SAFE_DIVIDE(t.self_closed, t.total_open) as selfclosed_perc,
SAFE_DIVIDE (t.monitor_unreachable, t.total_open) as offline_perc,
SAFE_DIVIDE (t.decline_closed, t.total_open) as declineclosed_perc,
menu.total_product,
SAFE_DIVIDE (menu.has_description, menu.total_product ) as description_perc,
SAFE_DIVIDE (menu.has_picture, menu.total_product ) as picture_perc,*/
least(mdt.median_local,mt.lowest_mode_local,SAFE_DIVIDE(gfv_local_prev_mth, successful_order)) as Deal_Max_MOV,
cast(cpcr.vendor_clicks_predicted_monthly as INT64) as recommended_clicks,
case when ad.vendor_code is null then "No active VF deal" else CONCAT(ad.no_of_deals," active deals") end as active_vf_deals,
case when p.vendor_code is null then "Not on Pandapro" else "On Pandapro" end as pandapro

from orders 
left join conversion cr on cr.global_entity_id=orders.global_entity_id and cr.vendor_code=orders.vendor_code and cr.order_month=orders.order_month
left join modetable mt on mt.global_entity_id=orders.global_entity_id and orders.order_month=mt.order_month and mt.vendor_code=orders.vendor_code
left join mediantable mdt on mdt.global_entity_id = orders.global_entity_id and orders.vendor_code=mdt.vendor_code
left join open_hours t on t.global_entity_id=orders.global_entity_id and t.vendor_code=orders.vendor_code and t.year_month=orders.order_month
--left join menu on menu.global_entity_id=orders.global_entity_id and menu.vendor_code=orders.vendor_code
left join jokerbookings jbk on jbk.global_entity_id=orders.global_entity_id and jbk.vendor_code=orders.vendor_code
left join cpcroi cpr on cpr.global_entity_id=orders.global_entity_id and cpr.vendor_code=orders.vendor_code
left join cpcbookings cpc on cpc.global_entity_id=orders.global_entity_id and cpc.vendor_code=orders.vendor_code
left join cppbookings cpp on cpp.global_entity_id=orders.global_entity_id and cpp.vendor_code=orders.vendor_code
left join cpcrec cpcr on cpcr.country=orders.country_name and cpcr.zone_id = orders.zone_id
left join pandapro p on p.country_name = orders.country_name and p.vendor_code = orders.vendor_code
left join active_deals ad on orders.global_entity_id = ad.global_entity_id and ad.vendor_code = orders.vendor_code 


---CHANGE GLOBAL ENTITY ID HERE---
where orders.global_entity_id = "FP_HK"
order by global_entity_id,vendor_code asc








