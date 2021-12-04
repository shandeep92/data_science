with ncr as 
(select 
bk.global_entity_id,
v.name as vendor_name,
bk.vendor_code,
count (Distinct bk.uuid) as booking_id,
bk.user,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
(case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) as user_booked,
format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) as year_month,
format_datetime('W%V-%Y', bk.created_at_utc) as booking_week_year,
format_datetime('%V', bk.created_at_utc) as booking_week,
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
format_date('%b', parse_date("%Y%m",cast(bk.year_month as string))) as month,
v.chain_name,
v.chain_code,
date(bk.started_at_utc) as started_at,
date(bk.ended_at_utc) as ended_at,
count (case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and cpp_billing.position <11  then bk.pps_promo_area_uuid
          when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'premium_placements' and cpp_billing.position < 8 then bk.pps_promo_area_uuid
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and cpp_billing.position < 9 then bk.pps_promo_area_uuid
          end ) as promo_areas_sold_PL,
count (case when bk.global_entity_id in ('FP_LA','FP_KH','FP_MM') and bk.type = 'organic_placements' and cpp_billing.position <= 8 then bk.pps_promo_area_uuid
    end) as promo_areas_sold_OL,
case 
when sf.vendor_grade = 'AAA' then 'AAA' 
when sf.vendor_grade is null then 'non-AAA'
else 'non-AAA' end as vendor_grade,

sf.owner_name


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
and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) >= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval -2 month)))
and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) <= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval 2 month)))

and (case when bk.global_entity_id in ('FP_BD','FP_PK','FP_SG','FP_MY','FP_JP') and bk.type = 'premium_placements' then cpp_billing.position <= 7 
  when bk.global_entity_id in ('FP_HK','FP_PH') and bk.type = 'premium_placements' then cpp_billing.position <= 10
 when bk.global_entity_id = 'FP_TH' and bk.type = 'premium_placements' then cpp_billing.position <= 15
 when bk.global_entity_id = 'FP_TW' and bk.type = 'premium_placements' then cpp_billing.position <= 8 
 when bk.global_entity_id in ('FP_LA','FP_MM','FP_KH') and bk.type = 'premium_placements' then cpp_billing.position <= 7
 when bk.global_entity_id in ('FP_LA','FP_MM','FP_KH') and bk.type = 'organic_placements' then cpp_billing.position <= 8 end)

 
GROUP BY 1,2,3,5,6,7,8,9,10,11,12,15,18,20,21,22,23,24,25,28,29
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
format_date('W%V-%Y', current_date) as current_week,
case when booking_week >= format_date("%V", date_add(current_date, interval -4 week)) and booking_week <= format_date("%V", current_date) then 1 else 0 end as report_weeks
from ncr n
left join central c on n.alias = c.alias)

order by global_entity_id, vendor_code, started_at asc