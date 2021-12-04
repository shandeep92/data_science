/*dim_vendors_1_zone*/
with dvzone as (
SELECT
global_entity_id,
vendor_code,
name,
lg_zones.lg_zone_id,
lg_zones.lg_zone_name,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones`
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE is_closest_point = TRUE
ORDER BY vendor_code asc
),

central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents WHERE department_name = 'Commercial'))),

available_areas as (
SELECT
global_entity_id,
vendor_name,
vendor_code,
IFNULL(COUNT(DISTINCT promo_area_id),0) as promo_areas_available
  
FROM (
SELECT 
v.global_entity_id,
v.name as vendor_name,
v.vendor_code,
vpa.pps_promo_area_uuid as promo_area_id,
vpa.coverage,
pa.name as promo_area_name,

FROM `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_vendors_promo_areas` vpa ON pa.rdbms_id = vpa.rdbms_id AND pa.uuid = vpa.pps_promo_area_uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_vendors` v ON v.rdbms_id = vpa.rdbms_id AND v.uuid = vpa.pps_vendor_uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` dv ON dv.global_entity_id = v.global_entity_id AND v.vendor_code = dv.vendor_code

WHERE v.is_active
AND dv.is_active
AND NOT dv.is_test
AND NOT is_private
AND dv.vertical_type = 'restaurants'
)
GROUP BY 1,2,3),


cpcbookings as(
select 
bk.global_entity_id,
bk.rdbms_id,
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
case when (case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking' then 'Self-Booking'
     when ct.alias is NULL then 'Local' else 'Central' end as channel,
format_date('W%V-%Y', date(bk.created_at_utc)) as booking_date,
format_date('%Y%V', date(bk.created_at_utc)) as booking_yearweek,
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
IFNULL(count(distinct cpc.uuid),0) as promo_areas_booked

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks`  cl on cl.pps_item_uuid=bk.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id = bk.global_entity_id and v.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join dvzone dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
LEFT JOIN central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where bk.uuid is not null
and bk.type = 'organic_placements'
and bk.billing_type = 'CPC'
and bk.status <> "new"
and bk.global_entity_id IN ("FP_BD","FP_PK","FP_SG","FP_MY","FP_TH","FP_TW","FP_HK","FP_PH","FP_LA","FP_KH","FP_MM","FP_JP")
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
),

/*1 line per month live*/
cpcbookings_array as(
SELECT
*,
GENERATE_DATE_ARRAY(DATE_TRUNC(start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM cpcbookings),

finalbookings as(
select 
ca.global_entity_id,
ca.rdbms_id,
common_name as country,
booking_id,
gmv_class,
vendor_grade,
ca.vendor_name,
ca.vendor_code,
chain_name,
chain_code,
city_name,
hurrier_zone,
user,
channel,
case 
when channel = 'Local' and vendor_grade = 'AAA' then "Local AAA"
when channel = "Local" and vendor_grade <> "AAA" then "Local non-AAA"
else channel
end as final_source,
booking_date as booking_week,
booking_yearweek,
type,
status,
start_date,
end_date,
DATE_SUB(DATE_TRUNC(DATE_ADD(parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) as last_day,
datelive,
format_date('%b', datelive) as month,
format_date("%Y%m",datelive) as yearmonth_live,
promo_areas_booked,
IF(promo_areas_booked > IFNULL(promo_areas_available,0), promo_areas_booked, IFNULL(promo_areas_available,0)) as promo_areas_available,
click_price_local,
budgeted_clicks,
initial_budget_local,
from cpcbookings_array ca, UNNEST(datelive_nested) AS datelive
LEFT JOIN available_areas a on a.global_entity_id = ca.global_entity_id and a.vendor_code = ca.vendor_code


),

/*clicks & orders*/
clicks as (
SELECT DISTINCT
bk.uuid as booking_id,
/*DATE_TRUNC('MONTH',cpc.created_at) AS click_month,*/
format_date ('%Y%m',date(cpc.created_at_utc)) as click_month,
count (distinct cpc.pps_item_uuid) as active_areas,
IFNULL(SUM(orders),0) AS cpc_orders,
IFNULL(SUM(quantity),0) AS spent_clicks

FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) bil
JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS cpc 
ON bil.uuid = cpc.pps_item_uuid
group by 1,2
),
    
exchangerate as(
select 
global_entity_id,
format_timestamp ('%Y%m',fx_rate_date) as yearmonth,
AVG (fx_rate_eur) as exchange_rate
from 
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso
where fx.fx_rate_date >= '2020-01-01')
group by 1,2
),

orders as (
SELECT
o.global_entity_id,
o.vendor_code,
format_date('%Y%m',created_date_local) as order_month,
COUNT(distinct o.uuid) as orders,
SUM(gfv_eur) as sum_gfv_eur,
SUM(gfv_local) as sum_gfv_local,
CASE WHEN SAFE_DIVIDE(SUM(gfv_eur),COUNT(distinct o.uuid)) IS NULL THEN 0 ELSE SAFE_DIVIDE(SUM(gfv_eur),COUNT(distinct o.uuid)) END  as afv_eur,
CASE WHEN SAFE_DIVIDE(SUM(gfv_local),COUNT(distinct o.uuid)) IS NULL THEN 0 ELSE SAFE_DIVIDE(SUM(gfv_local),COUNT(distinct o.uuid)) END as afv_local

FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on o.global_entity_id = v.global_entity_id and o.vendor_code = v.vendor_code
where created_date_local >= DATE_SUB(DATE_TRUNC(current_date(),MONTH),INTERVAL 4 MONTH)
and is_test_order = FALSE
and is_corporate_order = FALSE
and v.vertical_type = 'restaurants'

GROUP BY 1,2,3),

open_hours as (SELECT
global_entity_id,
vendor_code,
format_date('%Y%m',report_date) as year_month,
days_in_month,
SAFE_DIVIDE(SUM(actual_open_hours),days_in_month) as avg_open_hrs_per_day

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

WHERE report_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 4 MONTH),MONTH)

GROUP BY 1,2,3 
ORDER BY 2,3 ASC)
)

GROUP BY 1,2,3,4
ORDER BY 1,2,3 asc)

    
/*FINAL QUERY*/

select *, 
IFNULL(CAST((final_budget_local)/exchange_rate as FLOAT64),0) as final_budget_eur,
IFNULL(SAFE_DIVIDE(afv_local * cpc_orders,cpc_rev_local), 0) AS cpc_roi

from(

select *,

IFNULL(CAST(case when (final_spent_clicks*click_price_local) > initial_budget_local then initial_budget_local else (final_spent_clicks*click_price_local) end AS FLOAT64), 0) as cpc_rev_local,

IFNULL(CAST((case when final_spent_clicks*click_price_local > initial_budget_local then initial_budget_local else (final_spent_clicks*click_price_local) end)/exchange_rate as FLOAT64), 0) as cpc_rev_eur,

IFNULL(CAST(safe_divide(cpc_orders,final_spent_clicks) as FLOAT64), 0) as cpc_conversion,

IFNULL(CAST(safe_divide(final_spent_clicks,final_budgeted_clicks) AS FLOAT64), 0) as utilization,

IFNULL(CAST(case when (final_budgeted_clicks*click_price_local)>initial_budget_local then initial_budget_local else (final_budgeted_clicks*click_price_local) end AS FLOAT64), 0) as final_budget_local
    
FROM(
    
select *,
case when status = 'cancelled' and end_date < last_day then final_spent_clicks else budgeted_clicks end as final_budgeted_clicks
FROM(
    
select 
fb.*,
initial_budget_local/exr.exchange_rate as initial_budget_eur,
IFNULL(case when spent_clicks > budgeted_clicks then budgeted_clicks else spent_clicks end,0) as final_spent_clicks,
exr.exchange_rate,
IFNULL(c.active_areas,0) as active_areas,
IFNULL(c.cpc_orders,0) as cpc_orders,
IFNULL(c.spent_clicks,0) as spent_clicks,
IFNULL(o.afv_eur,0) as afv_eur,
IFNULL(o.afv_local,0) as afv_local,
IFNULL(oh.avg_open_hrs_per_day,0) as avg_open_hrs_per_day

from finalbookings fb
left join clicks c on c.click_month = fb.yearmonth_live and c.booking_id=fb.booking_id
left join exchangerate exr on exr.global_entity_id = fb.global_entity_id and exr.yearmonth =fb.yearmonth_live
left join orders o on fb.global_entity_id = o.global_entity_id and fb.yearmonth_live = o.order_month and fb.vendor_code = o.vendor_code
left join open_hours oh on fb.global_entity_id = oh.global_entity_id and oh.vendor_code = fb.vendor_code and oh.year_month = fb.yearmonth_live

where yearmonth_live >= '202101'

)))

ORDER BY global_entity_id,vendor_code,yearmonth_live
