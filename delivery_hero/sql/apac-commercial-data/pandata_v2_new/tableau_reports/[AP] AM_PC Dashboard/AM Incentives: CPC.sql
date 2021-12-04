WITH CPC as (
with

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
dvz.name as vendor_name,
bk.vendor_code,
(case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) as user_booked,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
case when (case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking' then 'Self-Booking'
     when ct.alias is NULL then 'Local' else 'Central' end as channel,
format_datetime('W%V-%Y', bk.created_at_utc) as booking_date,
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
sf.owner_name as owner_name,
count (distinct cpc.uuid)as promo_areas_booked,

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks`  cl on cl.pps_item_uuid=bk.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
LEFT JOIN central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where bk.uuid is not null
and click_price is not null
and bk.billing_type = 'CPC'
and bk.status <> "new"
and bk.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_MM','FP_KH','FP_JP')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),

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
vendor_grade,
vendor_name,
vendor_code,
user_booked,
channel,
type,
status,
start_date,
end_date,
case when start_date > parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)) then start_date else parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)) end as first_day_duration,
case when end_date < DATE_SUB(DATE_TRUNC(DATE_ADD(parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) then end_date else DATE_SUB(DATE_TRUNC(DATE_ADD(parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) end as last_day_duration,
DATE_SUB(DATE_TRUNC(DATE_ADD(parse_date("%Y%m",cast(format_date("%Y%m",datelive) as string)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) as last_day,
format_date("%Y%m",datelive) as yearmonth_live,
format_date("%b",datelive) as month_name,
promo_areas_booked,
click_price_local,
budgeted_clicks,
initial_budget_local,
owner_name
from cpcbookings_array ca, UNNEST(datelive_nested) AS datelive
),

clicks as (
SELECT DISTINCT
bk.uuid as booking_id,
format_date ('%Y%m',date(cpc.click_date)) as click_month,
count (distinct cpc.pps_item_uuid) as active_areas,
SUM(orders) AS cpc_orders,
SUM(quantity) AS spent_clicks
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
)

    select 
    fb.* except (budgeted_clicks,last_day,last_day_duration,first_day_duration,promo_areas_booked),
        initial_budget_local/exr.exchange_rate as initial_budget_eur,
    c.spent_clicks,
    case when spent_clicks > budgeted_clicks then budgeted_clicks else spent_clicks end as billable_clicks,
    case when c.spent_clicks*fb.click_price_local > fb.initial_budget_local then fb.initial_budget_local else c.spent_clicks*fb.click_price_local end as cpc_rev_local,
    (case when c.spent_clicks*fb.click_price_local > fb.initial_budget_local then fb.initial_budget_local else c.spent_clicks*fb.click_price_local end)/exr.exchange_rate as cpc_rev_eur,
 
    from finalbookings fb
    left join clicks c on c.click_month = fb.yearmonth_live and c.booking_id=fb.booking_id
    left join exchangerate exr on exr.global_entity_id = fb.global_entity_id and exr.yearmonth =fb.yearmonth_live
    
    where fb.yearmonth_live >= FORMAT_DATE('%Y%m',DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 4 MONTH))
    )
    
    
    Select
   cpc.global_entity_id,
   cpc.country,
   cpc.vendor_code,
   cpc.vendor_name,
   cpc.owner_name,
   cpc.channel,
   case when cpc.channel <> "Local" then cpc.channel else CONCAT(cpc.channel," ",cpc.vendor_grade) end as final_source,
   cpc.user_booked,
   cast(cpc.yearmonth_live as int64) as year_month,
   cpc.vendor_grade,
   sum(cpc.cpc_rev_local) as CPC_rev_local,
   sum(cpc.cpc_rev_eur) as CPC_rev_eur,
 
   from cpc
   group by 1,2,3,4,5,6,7,8,9,10
   order by global_entity_id,vendor_code,year_month asc
