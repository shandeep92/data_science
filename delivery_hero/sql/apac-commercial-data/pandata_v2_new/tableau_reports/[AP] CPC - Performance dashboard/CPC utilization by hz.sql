/*dim_vendors_1_zone*/
with dvzone as (
SELECT 
  v.global_entity_id,
  v.vendor_code,
  v.name as vendor_name,
  v.location.city as city_name,
  v.location.city_id as city_id,
  v.pd_city_id,
  lg_zones.lg_zone_id,
  lg_zones.lg_zone_name,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` lg on v.global_entity_id = lg.global_entity_id and v.vendor_code = lg.vendor_code
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE is_closest_point = TRUE
and v.vertical_type = 'restaurants'
ORDER BY vendor_code asc),

cpcbookings as(
select 
bk.global_entity_id,
c.name as common_name,
bk.uuid as booking_id,
cpc.pps_promo_area_uuid as promo_area_id,
gmvc.gmv_class,
v.name as vendor_name,
v.vendor_code,
bk.user,
case when (case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking' then 'Self-Booking'
     when agent_list.email is NULL then 'Local' else 'Central' end as channel,
format_datetime('W%V-%Y', bk.created_at_utc) as booking_date,
bk.type,
bk.status,
cpc.initial_budget as initial_budget_local,
cpc.click_price as click_price_local,
cpc.initial_budget/cpc.click_price as budgeted_clicks,
date(bk.started_at_utc) as start_date,
case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks`  cl on cl.pps_item_uuid=bk.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id = bk.global_entity_id and v.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join dvzone dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
LEFT JOIN
(SELECT
country_name as country
,email
from pandata_ap_commercial.ncr_central_agent_material) agent_list on agent_list.email = bk.user

where bk.uuid is not null
and click_price is not null
and bk.type = 'organic_placements'
and bk.billing_type = 'CPC'
and bk.status <> "new"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
promo_area_id,
format_date("%Y%m",datelive) as yearmonth_live,
gmv_class,
vendor_name,
vendor_code,
user,
channel,
booking_date as booking_week,
type,
status,
start_date,
end_date,
DATE_SUB(DATE_TRUNC(DATE_ADD(parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) as last_day,
format_date('%b', datelive) as month,
click_price_local,
budgeted_clicks,
initial_budget_local,
from cpcbookings_array ca, UNNEST(datelive_nested) AS datelive
),

/*clicks & orders*/
clicks as (
  SELECT DISTINCT
    bk.uuid as booking_id,
    bil.pps_promo_area_uuid as promo_area_id,
    format_date('%Y%m',cpc.click_date) as click_month,
    count (distinct cpc.pps_item_uuid) as active_areas,
    SUM(orders) AS cpc_orders,
    SUM(quantity) AS spent_clicks
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) bil
  JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS cpc 
    ON bil.uuid = cpc.pps_item_uuid
    group by 1,2,3
    ),
    
clicks2 as (
  SELECT DISTINCT
    bk.uuid as booking_id,
    format_date('%Y%m',cpc.click_date) as click_month,
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
format_timestamp ('%Y%m',fx_rate_date) as yearmonth,
AVG (fx_rate_eur) as exchange_rate
from 
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso
where fx.fx_rate_date >= '2020-01-01')
group by 1,2
),


city as (with dvzone1 as (
SELECT 
  v.global_entity_id,
  v.vendor_code,
  v.name as vendor_name,
  v.location.city as city_name,
  v.location.city_id as city_id,
  v.pd_city_id,
  lg_zones.lg_zone_id,
  lg_zones.lg_zone_name,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` lg on v.global_entity_id = lg.global_entity_id and v.vendor_code = lg.vendor_code
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE is_closest_point = TRUE
and v.vertical_type = 'restaurants'
ORDER BY vendor_code asc),

/*lg_zone as (
with unnested as (select 
*,
ROW_NUMBER() OVER (PARTITION BY rdbms_id,vendor_code ORDER BY lg_zone_id) AS row_number
from pandata.dim_vendors v, unnest(lg_zone_ids) as lg_zone_id)

select a.*,hz.name as hurrier_zone from unnested a
left join pandata.lg_zones hz on a.rdbms_id = hz.rdbms_id and a.lg_zone_id= hz.id
where a.row_number = 1
order by a.vendor_code asc),*/

pre_final as 
(select 
bk.global_entity_id,
cpc.pps_promo_area_uuid as promo_area_id,
c.name as common_name,
bk.year_month,
bk.vendor_code,
dvz.city_name,
dvz.city_id,
dvz.pd_city_id,
dvz.lg_zone_name as hurrier_zone_name,
dvz.lg_zone_id as hurrier_zone


from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on bk.global_entity_id = dv.global_entity_id and bk.vendor_code = dv.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join dvzone dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code

where bk.uuid is not null
and date_trunc(date(bk.started_at_utc),month) >= date_sub(date_trunc(current_date,month),interval 3 month)
and  date_trunc(date(bk.started_at_utc),month) <= date_add(date_trunc(current_date,month),interval 2 month)
and dv.vertical_type = 'restaurants'
order by bk.pps_promo_area_uuid asc)



SELECT * EXCEPT (count_city,count_hz,city_seq,hz_seq)

FROM (

SELECT *,ROW_NUMBER() OVER (PARTITION BY global_entity_id,promo_area_id ORDER BY count_city desc) as city_seq, RANK() OVER (PARTITION BY global_entity_id,promo_area_id,city_id ORDER BY count_hz desc) as hz_seq
FROM (

SELECT 
global_entity_id,
common_name,
promo_area_id,
city_name,
city_id,
pd_city_id,
hurrier_zone,
count(pd_city_id) as count_city,
count(hurrier_zone) as count_hz

FROM pre_final

group by 1,2,3,4,5,6,7
order by global_entity_id,promo_area_id asc)
)

where city_seq = 1 and hz_seq = 1),


prefinal as (
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
    fb.*,
    ci.city_name,
    ci.city_id,
    ci.pd_city_id,
    ci.hurrier_zone,
    initial_budget_local/exr.exchange_rate as initial_budget_eur,
    case when cm.spent_clicks > budgeted_clicks then budgeted_clicks else cm.spent_clicks end as final_spent_clicks,
    exr.exchange_rate,
    ifnull(cm.active_areas,0) as active_areas,
    c.cpc_orders,
    ifnull(c.spent_clicks,0) as spent_clicks,
    ifnull(cm.spent_clicks,0) as spent_clicks_overall,
  
    from finalbookings fb
    left join clicks c on c.click_month = fb.yearmonth_live and c.booking_id=fb.booking_id and fb.promo_area_id = c.promo_area_id
    left join clicks2 cm on cm.click_month = fb.yearmonth_live and cm.booking_id=fb.booking_id
    left join exchangerate exr on exr.global_entity_id = fb.global_entity_id and exr.yearmonth =fb.yearmonth_live
    left join city ci on fb.global_entity_id = ci.global_entity_id and fb.promo_area_id = ci.promo_area_id

)))
 where yearmonth_live >= format_date ('%Y%m',date_sub(current_date(),INTERVAL 1 MONTH))
order by global_entity_id,booking_id,yearmonth_live,promo_area_id asc),

cpc as (
SELECT

global_entity_id,
country,
city_name,
city_id,
pd_city_id,
hurrier_zone,
yearmonth_live,
ANY_VALUE(median_bid) as median_bid,
ANY_VALUE(cpc_vendors) as cpc_vendors
FROM (
SELECT 

global_entity_id,
country,
city_name,
city_id,
pd_city_id,
hurrier_zone,
yearmonth_live,
PERCENTILE_CONT(click_price_local, 0.5) OVER (PARTITION BY global_entity_id,country,city_name,city_id,hurrier_zone,yearmonth_live) AS median_bid,
count(distinct vendor_code) OVER (PARTITION BY global_entity_id,country,city_name,city_id,hurrier_zone,yearmonth_live) as cpc_vendors

FROM prefinal)
GROUP BY 1,2,3,4,5,6,7
),

vendor as (select global_entity_id,country,city_name,city_id,pd_city_id,hurrier_zone,hurrier_zone_name,count(distinct vendor_code) as vendor_count_total
FROM(
SELECT 
  v.global_entity_id,
  c.name as country,
  v.vendor_code,
  v.name as vendor_name,
  v.location.city as city_name,
  v.location.city_id as city_id,
  v.pd_city_id,
  lg_zones.lg_zone_id as hurrier_zone,
  lg_zones.lg_zone_name as hurrier_zone_name,
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` lg on v.global_entity_id = lg.global_entity_id and v.vendor_code = lg.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` c on v.global_entity_id = c.global_entity_id
CROSS JOIN UNNEST(lg_zones) AS lg_zones
WHERE is_closest_point = TRUE
and v.global_entity_id LIKE "%FP%"
and v.global_entity_id NOT IN ('FP_DE','FP_RO','FP_BG')
and v.vertical_type = 'restaurants'
and is_active=true and is_test=false and is_private = false
ORDER BY vendor_code asc)

group by 1,2,3,4,5,6,7
order by 1,2,3,4 asc)





/*FINAL QUERY*/

SELECT a.*,b.yearmonth_live,cast(b.cpc_vendors as float64) as vendors_on_cpc,cast(median_bid as float64) as median_bid,cast(safe_divide(b.cpc_vendors,a.vendor_count_total) as float64) as vendor_utilization 
from vendor a
left join cpc b
on a.global_entity_id = b.global_entity_id
and a.pd_city_id = b.pd_city_id
and a.hurrier_zone = b.hurrier_zone


group by 1,2,3,4,5,6,7,8,9,10,11,12
order by 1,2,3,4 asc
