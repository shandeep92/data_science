WITH pandabox as ( 
WITH 
central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `fulfillment-dwh-production.curated_data_shared_gcc_service.agents` WHERE department_name = 'Commercial'))),

jb as (
SELECT *,
CASE 
WHEN channel = "Local" then CONCAT(channel," ",vendor_grade) 
ELSE channel
end as final_source
FROM (
SELECT 
  bk.uuid as id,
  
  bk.global_entity_id,
  c.name as country,
  bk.vendor_code as vendor_code,
  dv.name as vendor_name,
  sf.owner_name,
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
  ROW_NUMBER() OVER (PARTITION BY bk.global_entity_id,bk.vendor_code ORDER BY date(started_at_utc) asc) as row_no
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
 
  bk.global_entity_id,
  c.name as country,
  bk.vendor_code as vendor_code,
  dv.name as vendor_name,
  sf.owner_name,
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
  ROW_NUMBER() OVER (PARTITION BY bk.global_entity_id,bk.vendor_code ORDER BY date(started_at_utc) asc) + 1 as row_no
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
LEFT JOIN jb2 on jb.global_entity_id = jb2.global_entity_id and jb.vendor_code = jb2.vendor_code and jb.row_no = jb2.row_no

order by jb.vendor_code,jb.start_date asc),

pre_bookings as (
SELECT
*,
GENERATE_DATE_ARRAY(DATE_TRUNC(DATE(start_date), MONTH), DATE_TRUNC(DATE(end_date), MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM bookings),


final_bookings as (
SELECT 
id,

global_entity_id,
country as country_name,
vendor_code,
vendor_name,
owner_name,
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
user as user_booked,
channel,
final_source,
type,
status,
units,

FROM pre_bookings,UNNEST(datelive_nested) as datelive
where datelive >= date_add(date_trunc(current_date, month), interval -4 month)),

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
   and date_trunc(created_date_local, month) >= date_add(date_trunc(current_date, month), interval -4 month)

GROUP BY 1,2,3,4,5
)



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
and o.created_at_utc >= b.use_start_date 
and o.created_at_utc <= b.use_end_date

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
order by global_entity_id,vendor_code ,start_date,year_month  asc)

select 
   pb.global_entity_id,
   pb.country_name,
   pb.vendor_name,
   pb.vendor_code,
   pb.owner_name,
   pb.final_source,
   pb.user_booked,
   cast(pb.year_month as INT64) as year_month,
   pb.vendor_grade,
   sum(pb.pb_rev_local) as PB_rev_local,
   sum(pb.pb_rev_eur) as PB_rev_eur,
   from pandabox pb
   group by 1,2,3,4,5,6,7,8,9
   order by 1,4 asc
