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


exchangerate as(
select 
global_entity_id,
date(fx_rate_date) as date,
AVG (fx_rate_eur) as exchange_rate
from 
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso
where fx.fx_rate_date >= '2020-01-01')
group by 1,2
),

cpcbookings as(
SELECT *,
CASE
when channel = "Local" then CONCAT(channel," ",vendor_grade)
else channel
end as final_source
FROM (
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
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
case when (case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking' then 'Self-Booking'
     when ct.alias is NULL then 'Local' else 'Central' end as channel,
format_date('W%V-%Y', date(bk.created_at_utc)) as booking_week,
date(bk.created_at_utc) as booking_date,
bk.type,
bk.status,
cpc.initial_budget as initial_budget_local,
cpc.click_price as click_price_local,
cpc.initial_budget/cpc.click_price as budgeted_clicks,
date(bk.started_at_utc) as start_date,
case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
DATE_DIFF(case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end,date(bk.started_at_utc),MONTH) + 1 as months,
case 
when sf.vendor_grade = 'AAA' then 'AAA' 
when sf.vendor_grade is null then 'non-AAA'
else 'non-AAA' end as vendor_grade,
count (distinct cpc.uuid)as promo_areas_booked,
avg(exchange_rate) as exchange_rate,
cpc.initial_budget/avg(exchange_rate) as initial_budget_eur

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks`  cl on cl.pps_item_uuid=bk.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id = bk.global_entity_id and v.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join dvzone dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
LEFT JOIN central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")
LEFT JOIN exchangerate exr on bk.global_entity_id = exr.global_entity_id and date(exr.date) >= date(bk.started_at_utc) and date(exr.date) <= case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end

where bk.uuid is not null
 AND bk.global_entity_id LIKE 'FP_%'
  AND bk.global_entity_id NOT IN ('FP_RO','FP_BG','FP_DE')
and bk.type = 'organic_placements'
and bk.billing_type = 'CPC'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)),

cpc1 as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date asc) as seq
FROM cpcbookings
),

cpc2 as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date asc)-1 as seq
FROM cpcbookings
),

cpc3 as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date asc)+1 as seq
FROM cpcbookings
)



SELECT a.*,
b.start_date as next_start_date,
b.end_date as next_end_date,
b.seq as next_seq,
c.start_date as prev_start_date,
c.end_date as prev_end_date,
c.seq as prev_seq,

CASE 
WHEN a.status = 'cancelled' and format_date('%Y%m',a.end_date) <> format_date('%Y%m',b.start_date) THEN format_date('%Y%m',a.end_date)
WHEN a.status = 'cancelled' and b.start_date IS NULL THEN format_date('%Y%m',a.end_date)
END AS churn_yearmonth,

CASE 
WHEN a.status = 'cancelled' and format_date('%Y%m',a.end_date) <> format_date('%Y%m',b.start_date) THEN format_date('%b',a.end_date)
WHEN a.status = 'cancelled' and b.start_date IS NULL THEN format_date('%b',a.end_date)
END AS churn_monthname,

CASE 
WHEN a.status = 'cancelled' and format_date('%Y%m',a.end_date) <> format_date('%Y%m',b.start_date) THEN format_date('W%V-%Y',a.end_date)
WHEN a.status = 'cancelled' and b.start_date IS NULL THEN format_date('W%V-%Y',a.end_date)
END AS churn_week,

CASE 
WHEN a.status = 'cancelled' and format_date('%Y%m',a.end_date) <> format_date('%Y%m',b.start_date) THEN format_date('%Y%V',a.end_date)
WHEN a.status = 'cancelled' and b.start_date IS NULL THEN format_date('%Y%V',a.end_date)
END AS churn_yearweek,

CASE
WHEN c.start_date IS NULL then format_date('%Y%m',a.start_date)
END AS new_acquisition_yearmonth,

CASE
WHEN c.start_date IS NULL then format_date('%b',a.start_date)
END AS new_acquisition_monthname,

CASE
WHEN c.start_date IS NULL then format_date('W%V-%Y',a.start_date)
END AS new_acquisition_week,

CASE
WHEN c.start_date IS NULL then format_date('%Y%V',a.start_date)
END AS new_acquisition_yearweek


FROM cpc1 a
LEFT JOIN cpc2 b 
ON a.global_entity_id = b.global_entity_id
and a.vendor_code = b.vendor_code
and a.seq = b.seq
LEFT JOIN cpc3 c
ON a.global_entity_id = c.global_entity_id
and a.vendor_code = c.vendor_code
and a.seq = c.seq
and format_date('%Y%m',a.start_date) = format_date('%Y%m',c.end_date)


ORDER BY a.global_entity_id,a.vendor_code,a.seq asc
