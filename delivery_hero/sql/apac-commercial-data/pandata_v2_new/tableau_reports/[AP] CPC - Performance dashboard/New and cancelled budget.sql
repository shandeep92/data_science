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
dvz.lg_zone_name as hurrier_zone,
bk.user,
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
LEFT JOIN exchangerate exr on bk.global_entity_id = exr.global_entity_id and date(exr.date) >= date(bk.started_at_utc) and date(exr.date) <= case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end
LEFT JOIN central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where bk.uuid is not null
and bk.type = 'organic_placements'
and bk.billing_type = 'CPC'

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)


SELECT *,
case when channel = "Local" then CONCAT(channel," ",vendor_grade)
else channel 
end as final_source,

CASE WHEN (CASE 

WHEN status = 'cancelled' and format_date('%Y%m',end_date) = format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN 
(-(initial_budget_local - LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)))
WHEN status = 'cancelled' and (format_date('%Y%m',end_date) <> format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN 
(-initial_budget_local)

END) IS NOT NULL THEN format_date('%Y%m',end_date) end as churn_yearmonth,

CASE WHEN (CASE 

WHEN status = 'cancelled' and format_date('%Y%m',end_date) = format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN 
(-(initial_budget_local - LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)))
WHEN status = 'cancelled' and (format_date('%Y%m',end_date) <> format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN 
(-initial_budget_local)

END) IS NOT NULL THEN format_date('%b',end_date) end as churn_yearmonth_name,

CASE 

WHEN status = 'cancelled' and format_date('%Y%m',end_date) = format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN 
(-(initial_budget_local - LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)))
WHEN status = 'cancelled' and (format_date('%Y%m',end_date) <> format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN 
(-initial_budget_local)

END as budget_cancelled_local,

CASE 

WHEN status = 'cancelled' and format_date('%Y%m',end_date) = format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN 
(-(initial_budget_local - LEAD(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)))
WHEN status = 'cancelled' and (format_date('%Y%m',end_date) <> format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LEAD(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN 
(-initial_budget_local)

END / exchange_rate as budget_cancelled_eur,

CASE WHEN (CASE 

WHEN LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) IS NULL THEN initial_budget_local

WHEN format_date('%Y%m',start_date) = format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN initial_budget_local - LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)

WHEN (format_date('%Y%m',start_date) <> format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN initial_budget_local

END) IS NOT NULL then format_date('%Y%m',start_date) END as new_acquisition_yearmonth,

CASE WHEN (CASE 

WHEN LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) IS NULL THEN initial_budget_local

WHEN format_date('%Y%m',start_date) = format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN initial_budget_local - LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)

WHEN (format_date('%Y%m',start_date) <> format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN initial_budget_local
END) IS NOT NULL then format_date('%b',start_date) END as new_acquisition_yearmonth_name,

CASE 
WHEN LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) IS NULL THEN initial_budget_local

WHEN format_date('%Y%m',start_date) = format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN initial_budget_local - LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)

WHEN (format_date('%Y%m',start_date) <> format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN initial_budget_local
END as new_budget_local,

CASE 
WHEN LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) IS NULL THEN initial_budget_local

WHEN format_date('%Y%m',start_date) = format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) and LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC) < initial_budget_local THEN initial_budget_local - LAG(initial_budget_local) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)

WHEN (format_date('%Y%m',start_date) <> format_date("%Y%m",LAG(end_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) OR format_date("%Y%m",LAG(start_date) OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date ASC)) IS NULL) THEN initial_budget_local

END / exchange_rate as new_budget_eur


from cpcbookings b

order by global_entity_id,vendor_code,start_date asc
