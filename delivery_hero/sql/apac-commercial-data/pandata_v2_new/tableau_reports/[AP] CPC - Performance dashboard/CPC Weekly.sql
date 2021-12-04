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
ORDER BY vendor_code asc),

central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents WHERE department_name = 'Commercial'))),


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
dvz.name as hurrier_zone,
bk.user,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
case when (case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking' then 'Self-Booking'
     when ct.alias is NULL then 'Local' else 'Central' end as channel,
format_date('W%V-%Y', date(bk.created_at_utc)) as booking_date,
format_date('%Y%V', date(bk.created_at_utc)) as booking_yearweek,
bk.type,
bk.status,
/*bk.year_month,*/
/*parse_date("%Y%m",cast(bk.year_month as string)) as month_booked,*/
/*format_date('%b', parse_date("%Y%m",cast(bk.year_month as string))) as month,*/
cpc.initial_budget as initial_budget_local,
cpc.click_price as click_price_local,
cpc.initial_budget/cpc.click_price as budgeted_clicks,
date(bk.started_at_utc) as start_date,
case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
case when sf.vendor_grade = "AAA" then "AAA" else "non-AAA" end as vendor_grade,
count (distinct cpc.uuid)as promo_areas_booked,
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
AND bk.global_entity_id LIKE 'FP_%'
  AND bk.global_entity_id NOT IN ('FP_RO','FP_BG','FP_DE')

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
),
/*1 line per month live*/
cpcbookings_array as(
SELECT
*,
GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 WEEK) AS datelive_nested,
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
datelive as week,
format_date('%Y%V', datelive) as year_week,
format_date('W%V-%Y',datelive) as weekyear_live,
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
    /*DATE_TRUNC('MONTH',cpc.created_at) AS click_month,*/
    format_date ('%Y%V',date(cpc.click_date)) as click_yearweek,
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
format_timestamp ('%Y%V',fx_rate_date) as yearweek,
AVG (fx_rate_eur) as exchange_rate
from 
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso
where fx.fx_rate_date >= '2020-01-01')
group by 1,2
),


aov as (
SELECT
o.global_entity_id,
o.vendor_code,
format_date('%Y%V',o.created_date_local) as yearweek,
avg(gfv_local) as avg_gfv_local,
avg(gfv_eur) as avg_gfv_eur
FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on o.global_entity_id = v.global_entity_id and o.vendor_code = v.vendor_code
where o.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 6 MONTH)
and o.is_valid_order = TRUE
and o.is_test_order = FALSE
GROUP BY 1,2,3
)
    
  /*FINAL QUERY*/

    select *, cast((final_budget_local)/exchange_rate as float64) as final_budget_eur
    from(
    select *,
    case when final_spent_clicks*click_price_local > initial_budget_local then cast(initial_budget_local as float64) else cast(final_spent_clicks*click_price_local as float64) end as cpc_rev_local,
    cast((case when final_spent_clicks*click_price_local > initial_budget_local then initial_budget_local else final_spent_clicks*click_price_local end)/exchange_rate as float64) as cpc_rev_eur,
    cast(safe_divide(cpc_orders,final_spent_clicks) as float64) as cpc_conversion,
    cast(safe_divide(final_spent_clicks,final_budgeted_clicks) as float64) as utilization,
    case when (final_budgeted_clicks*click_price_local)>initial_budget_local then cast(initial_budget_local as float64) else cast((final_budgeted_clicks*click_price_local) as float64) end as final_budget_local,
    
    FROM(
    
    select *,
    case when status = 'cancelled' and end_date < last_day then final_spent_clicks else budgeted_clicks end as final_budgeted_clicks
    FROM(
    
    select 
    fb.* except (promo_areas_booked),
    initial_budget_local/exr.exchange_rate as initial_budget_eur,
    case when spent_clicks > budgeted_clicks then budgeted_clicks else spent_clicks end as final_spent_clicks,
    fb.promo_areas_booked,
    exr.exchange_rate,
    c.active_areas,
    c.cpc_orders,
    c.spent_clicks,
    aov.avg_gfv_local,
    aov.avg_gfv_eur
    from finalbookings fb
    left join clicks c on c.click_yearweek = fb.year_week and c.booking_id=fb.booking_id
    left join exchangerate exr on exr.global_entity_id = fb.global_entity_id  and exr.yearweek =fb.year_week
    left join aov on aov.global_entity_id  = fb.global_entity_id  and aov.vendor_code = fb.vendor_code and aov.yearweek = fb.year_week

)))

ORDER BY global_entity_id,vendor_code,year_week asc
