with 
central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents WHERE department_name = 'Commercial'))),

CPPbk as(
select 
bk.global_entity_id,
c.name as common_name,
v.name as vendor_name,
bk.vendor_code,
(case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) as user_booked,
bk.year_month,
date_diff(date(bk.ended_at_utc),date(bk.started_at_utc),day)+1 as duration,
bk.started_at_utc,
bk.ended_at_utc,
date_diff(date_sub(date_add(date_trunc(date(bk.started_at_utc),MONTH),INTERVAL 1 MONTH),INTERVAl 1 DAY),date(bk.started_at_utc),DAY)+1 as intended_duration,
bk.type,
bk.status,
case when (case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking' then 'Self-Booking'
     when ct.alias is NULL then 'Local' else 'Central' end as channel,
ifnull(SUM (bk.cpp_billing.price),0) as sold_price_local,
ex.fx_rate_eur,
ifnull(SUM (bk.cpp_billing.price),0)/ex.fx_rate_eur as sold_price_eur,
case 
when sf.vendor_grade = 'AAA' then 'AAA' 
when sf.vendor_grade is null then 'non-AAA'
else 'non-AAA' end as vendor_grade,
sf.owner_name as owner_name,
format_datetime('%b',bk.started_at_utc) as month_name

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
LEFT JOIN central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where bk.uuid is not null
and billing_type = 'CPP'
/*and (bk.status != 'cancelled')*/
and year_month >= CAST(FORMAT_DATE('%Y%m',DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 4 MONTH),MONTH)) AS INT64) 
AND year_month <= CAST(FORMAT_DATE('%Y%m',DATE_TRUNC(DATE_ADD(CURRENT_DATE(),INTERVAL 2 MONTH),MONTH)) AS INT64) 
AND bk.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,15,17,18,19
),

cpp_noPR_final as 
   (Select
   cpp.global_entity_id,
   cpp.common_name as country,
   cpp.vendor_code,
   cpp.vendor_name,
   cpp.owner_name,
   cpp.channel,
   case when cpp.channel <> "Local" then cpp.channel else CONCAT(cpp.channel," ",cpp.vendor_grade) end as final_source,
   cpp.user_booked,
   cpp.year_month,
   cpp.type,
   cpp.status,
   cpp.vendor_grade,


  sum (case when cpp.status <> 'cancelled' then cpp.sold_price_local end) as CPP_noPR_rev_local,
  sum (case when cpp.status <> 'cancelled' then cpp.sold_price_eur end) as CPP_noPR_rev_eur,
  0 as CPP_PR_rev_local,
  0 as CPP_PR_rev_eur
   from cppbk cpp
   group by 1,2,3,4,5,6,7,8,9,10,11,12
   ),
   
   cpp_PR_final as 
   (Select
   cpp.global_entity_id,
   cpp.common_name as country,
   cpp.vendor_code,
   cpp.vendor_name,
   cpp.owner_name,
   cpp.channel,
   case when cpp.channel <> "Local" then cpp.channel else CONCAT(cpp.channel," ",cpp.vendor_grade) end as final_source,
   cpp.user_booked,
   cpp.year_month,
   cpp.type,
   cpp.status,
   cpp.vendor_grade,

   0 as CPP_noPR_rev_local,
   0 as CPP_noPR_rev_eur,
   Sum(case when cpp.status = 'open' then cpp.sold_price_local
        when cpp.status = 'cancelled' and cpp.duration >1 then (cpp.sold_price_local/intended_duration) * cpp.duration
        end) as CPP_PR_rev_local,
   Sum(case when cpp.status = 'open' then cpp.sold_price_eur
            when cpp.status = 'cancelled' and cpp.duration >1 then (cpp.sold_price_eur/intended_duration) * cpp.duration
            end) as CPP_PR_rev_eur


   from cppbk cpp
   group by 1,2,3,4,5,6,7,8,9,10,11,12
   ),
   
   final_union as (
   SELECT * FROM cpp_noPR_final
   UNION ALL
   SELECT * FROM cpp_PR_final)
   
   SELECT * from final_union
   order by global_entity_id,vendor_code,year_month asc
