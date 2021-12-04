with 
CPPbk as(
with ncr as 
(select 
bk.rdbms_id,
v.name as vendor_name,
bk.vendor_code,
count (Distinct bk.uuid) as booking_id,
bk.user,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
(case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) as user_booked,
CAST(format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) as INT64) as year_month,
format_datetime('W%V-%Y', bk.created_at_utc) as booking_week_year,
format_datetime('%Y%V', bk.created_at_utc) as booking_week,
bk.type,
bk.status,
ifnull(SUM (bk.cpp_billing.price),0) as sold_price_local,
ifnull(SUM (bk.cpp_billing.suggested_price),0) as suggested_price_local,
ex.fx_rate_eur,
ifnull(SUM (bk.cpp_billing.price),0)/ex.fx_rate_eur as sold_price_eur,
ifnull(SUM (bk.cpp_billing.suggested_price),0)/ex.fx_rate_eur as suggested_price_eur,
c.name as common_name,
count (distinct bk.pps_promo_area_uuid) as promo_areas_sold,
parse_date("%Y%m",cast(bk.year_month as string)) as month_booked,
format_date('%b', parse_date("%Y%m",cast(bk.year_month as string))) as month_name,
case 
when sf.vendor_grade = 'AAA' then 'AAA' 
when sf.vendor_grade is null then 'non-AAA'
else 'non-AAA' end as vendor_grade,
gmvc.gmv_class as gmv_class,
v.location.city as city_name
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join 
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso

WHERE fx_rate_date >= "2020-01-01"
and c.global_entity_id LIKE "%FP%"
order by 1,6 asc) ex on ex.global_entity_id = bk.global_entity_id and date(bk.cpp_billing.created_at_utc) = date(ex.fx_rate_date)
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id = bk.global_entity_id and v.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code

where bk.uuid is not null
and bk.billing_type = 'CPP'
and bk.rdbms_id in (7,12,15,16,17,18,19,20,219,220,221,263)
and bk.status in ('new','open')
and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) >= '202101'
and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) <= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval 2 month)))

and (case when bk.rdbms_id in (7,12,15,16,263) and bk.type = 'premium_placements' then cpp_billing.position <= 7 
  when bk.rdbms_id in (19,20) and bk.type = 'premium_placements' then cpp_billing.position <= 10
 when bk.rdbms_id = 17 and bk.type = 'premium_placements' then cpp_billing.position <= 15
 when bk.rdbms_id = 18 and bk.type = 'premium_placements' then cpp_billing.position <= 8 
 when bk.rdbms_id in (219,220,221) and bk.type = 'premium_placements' then cpp_billing.position <= 7
 when bk.rdbms_id in (219,220,221) and bk.type = 'organic_placements' then cpp_billing.position <= 8 end)
 
GROUP BY 1,2,3,5,6,7,8,9,10,11,12,15,18,20,20,21,22,23,24
),

central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents WHERE department_name = 'Commercial')))


SELECT *,

CASE 
WHEN channel = "Local" then CONCAT(channel," ",vendor_grade) 
ELSE channel
end as final_source

FROM (
select
n.*,
case when lower(user) not like '%foodpanda%' then 'Self-Booking' 
     when c.alias is NULL then 'Local' 
     else 'Central' 
     end as channel,
     
case when user_booked = 'self-booking' then 'Automated' else 'Non-Automated' end as automated,
/*case when user_booked = 'self-booking' then 'Self-Booking'
  when count(c.alias) > 0 then 'Central' else 'Local' end as source,*/
format_date('W%V-%Y', current_date) as current_week,
case when booking_week >= format_date("%V", date_add(current_date, interval -5 week)) and booking_week <= format_date("%V", current_date) then 1 else 0 end as report_weeks
from ncr n
left join central c on n.alias = c.alias

order by year_month asc)),

CPC as (/*dim_vendors_1_zone*/
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
bk.rdbms_id,
bk.global_entity_id,
c.name as common_name,
bk.uuid as booking_id,
gmvc.gmv_class,
v.name as vendor_name,
bk.vendor_code as vendor_code,
v.chain_name,
v.chain_code,
v.location.city as city_name,
dvz.lg_zone_name as hurrier_zone,
bk.user,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
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
and bk.status <> "new"
and bk.rdbms_id IN (7,12,15,16,17,18,19,20,219,220,221,263)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
),

/*1 line per month live*/
cpcbookings_array as(
SELECT
*,
GENERATE_DATE_ARRAY(DATE_TRUNC(start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM cpcbookings),
finalbookings as(
select 
rdbms_id,
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
format_date('%b', datelive) as month_name,
format_date("%Y%m",datelive) as yearmonth_live,
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
    format_date ('%Y%m',date(cpc.created_at_utc)) as click_month,
    count (distinct cpc.pps_item_uuid) as active_areas,
    case when SUM(orders) is null then 0 else SUM(orders) END AS cpc_orders,
    case when SUM(quantity) is null then 0 else SUM(quantity) END AS spent_clicks
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) bil
  JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS cpc 
    ON bil.uuid = cpc.pps_item_uuid
    group by 1,2
    ),
    
exchangerate as (SELECT
global_entity_id,
format_timestamp ('%Y%m',fx_rate_date) as yearmonth,
AVG (fx_rate_eur) as exchange_rate
FROM 

(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso

WHERE fx_rate_date >= "2020-01-01"
and c.global_entity_id LIKE "%FP%"
order by 1,6 asc)

GROUP BY 1,2
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
and is_valid_order = TRUE
and is_corporate_order = FALSE
and v.vertical_type = 'restaurants'

GROUP BY 1,2,3)

    
/*FINAL QUERY*/

    select *, 
    cast((final_budget_local)/exchange_rate as float64) as final_budget_eur,
    CASE WHEN SAFE_DIVIDE(afv_local * cpc_orders,CASE WHEN cpc_rev_local IS NULL then 0 ELSE cpc_rev_local END) IS NULL THEN 0 ELSE SAFE_DIVIDE(afv_local * cpc_orders,CASE WHEN cpc_rev_local IS NULL then 0 ELSE cpc_rev_local END) END AS cpc_roi
    from(
    select *,
    case when final_spent_clicks*click_price_local > initial_budget_local then cast(initial_budget_local as float64) else cast(final_spent_clicks*click_price_local as float64) end as cpc_rev_local,
    cast((case when final_spent_clicks*click_price_local > initial_budget_local then initial_budget_local else final_spent_clicks*click_price_local end)/exchange_rate as float64) as cpc_rev_eur,
    cast(safe_divide(cpc_orders,final_spent_clicks) as float64) as cpc_conversion,
    CASE WHEN cast(safe_divide(final_spent_clicks,final_budgeted_clicks) as float64) IS NULL THEN 0 ELSE cast(safe_divide(final_spent_clicks,final_budgeted_clicks) as float64) END as utilization,
    case when (final_budgeted_clicks*click_price_local)>initial_budget_local then cast(initial_budget_local as float64) else cast((final_budgeted_clicks*click_price_local) as float64) end as final_budget_local
    
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
    o.afv_eur,
    o.afv_local
    from finalbookings fb
    left join clicks c on c.click_month = fb.yearmonth_live and c.booking_id=fb.booking_id
    left join exchangerate exr on exr.global_entity_id = fb.global_entity_id and exr.yearmonth =fb.yearmonth_live
    left join orders o on fb.global_entity_id = o.global_entity_id and fb.yearmonth_live = o.order_month and fb.vendor_code = o.vendor_code

where yearmonth_live >= '202101'
)))

ORDER BY rdbms_id,vendor_code,yearmonth_live),
   
pandabox as ( 

WITH 
central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents WHERE department_name = 'Commercial'))),

jb as (
SELECT *,
CASE 
WHEN channel = "Local" then CONCAT(channel," ",vendor_grade) 
ELSE channel
end as final_source
FROM (
SELECT 
  bk.uuid as id,
  bk.rdbms_id,
  bk.global_entity_id,
  c.name as country,
  bk.vendor_code as vendor_code,
  dv.name as vendor_name,
  gmvc.gmv_class,
  case 
  when sf.vendor_grade = 'AAA' then 'AAA' 
  when sf.vendor_grade is null then 'non-AAA'
  else 'non-AAA'
  end as vendor_grade,
  date(bk.created_at_utc) as created_at,
  bk.started_at_utc as start_date,
  case when bk.ended_at_utc is null then current_datetime() else bk.ended_at_utc end as end_date,
  date(bk.modified_at_utc) as modified_at,
  pps_vendor_uuid as vendor_id,
  year_month,
  user,
  case 
  when user = 'joker-sync-command' or bk.user = 'valerie.ong@foodpanda.com' then 'Self-Booking'
  when ct.alias is NULL then 'Local' else 'Central' end as channel,
  bk.type,
  bk.status,
  cpu_billing.units as units,
  ROW_NUMBER() OVER (PARTITION BY bk.rdbms_id,bk.vendor_code ORDER BY date(started_at_utc) asc) as row_no
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on bk.global_entity_id = dv.global_entity_id and bk.vendor_code = dv.vendor_code
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where  bk.type = 'joker'
and bk.global_entity_id IN ("FP_BD","FP_PK","FP_SG","FP_MY","FP_TH","FP_TW","FP_HK","FP_PH","FP_LA","FP_KH","FP_MM","FP_JP")
and dv.is_active = TRUE and dv.is_private = FALSE and dv.is_test = FALSE and dv.vertical_type = 'restaurants'
order by vendor_code asc)),

jb2 as  (
SELECT *,
CASE 
WHEN channel = "Local" then CONCAT(channel," ",vendor_grade) 
ELSE channel
end as final_source
FROM (
SELECT 
  bk.uuid as id,
  bk.rdbms_id,
  bk.global_entity_id,
  c.name as country,
  bk.vendor_code as vendor_code,
  dv.name as vendor_name,
  gmvc.gmv_class,
  case 
  when sf.vendor_grade = 'AAA' then 'AAA' 
  when sf.vendor_grade is null then 'non-AAA'
  else 'non-AAA'
  end as vendor_grade,
  date(bk.created_at_utc) as created_at,
  bk.started_at_utc as start_date,
  case when bk.ended_at_utc is null then current_datetime() else bk.ended_at_utc end as end_date,
  date(bk.modified_at_utc) as modified_at,
  pps_vendor_uuid as vendor_id,
  year_month,
  user,
  case 
  when user = 'joker-sync-command' or bk.user = 'valerie.ong@foodpanda.com' then 'Self-Booking'
  when ct.alias is NULL then 'Local' else 'Central' end as channel,
  bk.type,
  bk.status,
  cpu_billing.units as units,
  ROW_NUMBER() OVER (PARTITION BY bk.rdbms_id,bk.vendor_code ORDER BY date(started_at_utc) asc) + 1 as row_no
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on bk.global_entity_id = dv.global_entity_id and bk.vendor_code = dv.vendor_code
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where  bk.type = 'joker'
and bk.global_entity_id IN ("FP_BD","FP_PK","FP_SG","FP_MY","FP_TH","FP_TW","FP_HK","FP_PH","FP_LA","FP_KH","FP_MM","FP_JP")

and dv.is_active = TRUE and dv.is_private = FALSE and dv.is_test = FALSE and dv.vertical_type = 'restaurants'
order by vendor_code asc)),

bookings as 
(SELECT jb.*,
/*case 
when jb2.end_date = jb.start_date then DATE_ADD(jb.start_date,INTERVAL 1 DAY)
else jb.start_date
end as use_start_date,*/

jb2.start_date as prev_start_date,
jb2.end_date as prev_end_date,
jb2.user as prev_user, 
jb2.status as prev_status,
jb2.units as prev_units,
case 
when jb2.start_date is null then DATE_DIFF(jb.end_date,jb.start_date,DAY) + 1 
when jb2.end_date = jb.start_date then DATE_DIFF(jb.end_date,DATE_ADD(jb.start_date,INTERVAL 1 DAY),DAY) + 1
else DATE_DIFF(jb.end_date,jb.start_date,DAY) + 1
end as days_running,

case 
when jb2.start_date is null then jb.units * (DATE_DIFF(jb.end_date,jb.start_date,DAY) + 1) 
when jb2.end_date = jb.start_date then (DATE_DIFF(jb.end_date,DATE_ADD(jb.start_date,INTERVAL 1 DAY),DAY) + 1) * jb.units
else jb.units * (DATE_DIFF(jb.end_date,jb.start_date,DAY) + 1)
end as total_inventory

from jb
LEFT JOIN jb2 on jb.rdbms_id = jb2.rdbms_id and jb.vendor_code = jb2.vendor_code and jb.row_no = jb2.row_no

order by jb.vendor_code,jb.start_date asc),

pre_bookings as (
SELECT
*,
GENERATE_DATE_ARRAY(DATE_TRUNC(DATE(start_date), MONTH), DATE_TRUNC(DATE(end_date), MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM bookings),


final_bookings as (
SELECT 
id,
rdbms_id,
global_entity_id,
country,
vendor_code,
vendor_name,
gmv_class,
vendor_grade,
created_at,
start_date,
CASE 
WHEN start_date < datelive then datelive
else start_date
end as use_start_date,
end_date,
CASE
when end_date < DATE_SUB(DATE_ADD(datelive,INTERVAL 1 MONTH),INTERVAL 1 DAY) THEN end_date 
ELSE DATETIME_SUB(DATE_ADD(DATETIME(datelive),INTERVAL 1 MONTH),INTERVAL 1 MINUTE) end as use_end_date,
DATE_SUB(DATE_ADD(datelive,INTERVAL 1 MONTH),INTERVAL 1 DAY) as last_day,
modified_at,
datelive,
format_date('%Y%m',datelive) as year_month,
format_date('%b',datelive) as month_name,
user,
channel,
final_source,
type,
status,
units,

FROM pre_bookings,UNNEST(datelive_nested) as datelive
where datelive >= '2021-01-01'),

orders as (
select 
global_entity_id,
country_name,
vendor_code,
vendor_name,
created_at_utc,
SUM(joker_fee_eur) as pandabox_revenue_eur,
SUM(joker_fee_local) as pandabox_revenue_local,
SUM(gfv_eur) as pb_gfv_eur,
COUNT(DISTINCT id) as pandabox_orders

from `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals`
   Where 
   is_joker_used = true
   and is_valid_order = true
   and date_trunc(created_date_local, month) >= '2021-01-01'

GROUP BY 1,2,3,4,5
)


SELECT
rdbms_id,
country as country_name,
year_month as yearmonth,
month_name,
vendor_code,
channel as final_channel,
vendor_grade,
final_source,
sum(pb_rev_eur) as pandabox_revenue_eur,
sum(pb_rev_local) as pandabox_revenue_local,
count(distinct vendor_code) as number_of_vendors

FROM (

SELECT 
b.*,
DATE_DIFF(use_end_date,use_start_date,DAY) + 1 as days_running,
units * (DATE_DIFF(use_end_date,use_start_date,DAY) + 1) as total_inventory,
sum(pandabox_revenue_eur) as pb_rev_eur,
sum(pandabox_revenue_local) as pb_rev_local,
sum(pandabox_orders) as pb_orders,
sum(pb_gfv_eur) as pb_gfv_eur

from final_bookings b
left join orders o 
on b.global_entity_id = o.global_entity_id
and b.vendor_code = o.vendor_code 
and datetime(o.created_at_utc) >= datetime(b.use_start_date) 
and datetime(o.created_at_utc) <= datetime(b.use_end_date)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
order by rdbms_id,vendor_code ,start_date,year_month  asc)

GROUP BY 1,2,3,4,5,6,7,8
ORDER BY rdbms_id,yearmonth,final_source asc),
   
   cpp_noPR_final as 
   (Select
   cpp.rdbms_id,
   cpp.common_name as country,
   "CPP" as product,
   cpp.vendor_code,
   cpp.vendor_grade,
   cpp.channel as channel,
   case when cpp.channel <> "Local" then cpp.channel else concat(cpp.channel," ",cpp.vendor_grade) end as final_source,
   cpp.month_name,
   cpp.year_month,
   sum (cpp.sold_price_local) as CPP_noPR_rev_local,
   sum (cpp.sold_price_eur) as CPP_noPR_rev_eur,
  0 as cpc_rev_local,
  0 as cpc_rev_eur,
  0 as PB_rev_local,
  0 as PB_rev_eur
   from cppbk cpp
   group by 1,2,3,4,5,6,7,8,9
   ),
   
   cpcfinal as(
   Select
   cpc.rdbms_id,
   cpc.country,
   "CPC" as product,
   cpc.vendor_code,
   cpc.vendor_grade,
   cpc.channel,
   case when cpc.channel <> "Local" then cpc.channel else concat(cpc.channel," ",cpc.vendor_grade) end as final_source,
   cpc.month_name,
   cast(cpc.yearmonth_live as int64) as year_month,
   0 as CPP_noPR_rev_local,
   0 as CPP_noPR_rev_eur,
   sum(cpc.cpc_rev_local) as CPC_rev_local,
   sum(cpc.cpc_rev_eur) as CPC_rev_eur,
   0 as PB_rev_local,
   0 as PB_rev_eur
   from cpc
   group by 1,2,3,4,5,6,7,8,9
    ),
    
   pbfinal as(
   select 
   pb.rdbms_id,
   pb.country_name,
   "Pandabox" as product,
   pb.vendor_code,
   pb.vendor_grade,
   pb.final_channel as channel,
   case when pb.final_channel <> "Local" then pb.final_channel else concat(pb.final_channel," ",pb.vendor_grade) end as final_source,
   pb.month_name,
   cast(pb.yearmonth as INT64) as year_month,
   0 as CPP_noPR_rev_local,
   0 as CPP_noPR_rev_eur,
   0 as cpc_rev_local,
   0 as cpc_rev_eur,
   sum(pb.pandabox_revenue_local) as PB_rev_local,
   sum(pb.pandabox_revenue_eur) as PB_rev_eur,
   from pandabox pb
   group by 1,2,3,4,5,6,7,8,9),


   finalunion as(
   select * from CPP_noPR_final
   UNION ALL
   Select * from cpcfinal
   UNION ALL
   Select * from pbfinal)

   select *
   ,cast(format_date('%Y%m',DATE_SUB(current_date(),INTERVAL 1 MONTH)) as INT64) as prev_year_month

   from finalunion
   
   where year_month >= 202101
   and year_month <= cast(format_date('%Y%m',date_add(current_date(),INTERVAL 2 MONTH)) as INT64)
   
   and rdbms_id = 16
 
   order by rdbms_id , vendor_code, year_month asc
