-- This query contains all AM Ads and Deals performance in EUR and over GMV
-- Powers 3 tabs in the dashboard: 
-- 1. AM Performance: Ads, Deals, Portfolio
-- 2. AM Leaderboard: Ads
-- 3. AM Performance: Ads MoM

/* AM INFORMATION */

WITH sf_account_info as (
SELECT
a.global_entity_id,
a.country_name,
a.vendor_code,
u.id as account_owner_id,
u.title as account_owner_title, 
a.owner_name as account_owner,
u.email as account_owner_email, 
a.status as sf_account_status,
FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` a
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_users` u
ON  a.global_entity_id = u.global_entity_id
AND a.sf_owner_id = u.id
WHERE is_active
AND a.global_entity_id LIKE 'FP_%'                                                        
AND a.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
),

owner_info as ( 
SELECT global_entity_id, country_name, account_owner, account_owner_email, account_owner_title
FROM (
  SELECT *, row_number() OVER (PARTITION by global_entity_id, account_owner ORDER BY account_owner_email desc) as seq
  FROM(SELECT distinct account_owner, account_owner_email,  global_entity_id, country_name, account_owner_title
  FROM sf_account_info))
  
  WHERE seq = 1),
  
  brand_no AS (
 SELECT 
 sf.global_entity_id,
 sf.country_name,
 sf.account_owner_title,
 sf.account_owner,
 sf.account_owner_email,
 v.chain_code,
 v.vendor_code,
 v.is_active,
 COUNT(DISTINCT CASE WHEN v.is_active THEN COALESCE(v.chain_code, v.vendor_code) END) OVER(PARTITION BY sf.account_owner) as no_of_brands,
 COUNT(DISTINCT CASE WHEN v.is_active THEN v.vendor_code END) OVER(PARTITION BY sf.account_owner) as no_of_active_vendors
 
 FROM sf_account_info sf
 LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v
 ON v.global_entity_id = sf.global_entity_id 
 AND v.vendor_code = sf.vendor_code
 
 WHERE v.is_active
 AND v.is_private = FALSE
 AND is_test = FALSE
 AND vertical = 'Restaurant'),

/* Deals Subqueries */

deals AS (
SELECT
  ddpd.global_entity_id,
  COALESCE(ddpd.country, 
  CASE WHEN ddpd.global_entity_id = 'FP_BD'
  THEN 'Bangladesh'
  WHEN ddpd.global_entity_id = 'FP_PK'
  THEN 'Pakistan'
  WHEN ddpd.global_entity_id = 'FP_SG'
  THEN 'Singapore'
  WHEN ddpd.global_entity_id = 'FP_MY'
  THEN 'Malaysia'
  WHEN ddpd.global_entity_id = 'FP_TH'
  THEN 'Thailand'
  WHEN ddpd.global_entity_id = 'FP_TW'
  THEN 'Taiwan'
  WHEN ddpd.global_entity_id = 'FP_HK'
  THEN 'Hong Kong'
  WHEN ddpd.global_entity_id = 'FP_PH'
  THEN 'Philippines'
  WHEN ddpd.global_entity_id = 'FP_LA'
  THEN 'Laos'
  WHEN ddpd.global_entity_id = 'FP_KH'
  THEN 'Cambodia'
  WHEN ddpd.global_entity_id = 'FP_MM'
  THEN 'Myanmar'
  WHEN ddpd.global_entity_id = 'FP_JP'
  THEN 'Japan'
  END)
  AS country,
  COALESCE(ddpd.business_type, 'restaurants') AS business_type,
  sf.owner_name as owner_name,
  format_date('%Y%m',ddpd.month) as year_month,
  SUM(IFNULL(ddpd.daily_all_gmv_eur,0)) AS montly_total_country_business_gmv_eur,
  SUM(IFNULL(ddpd.daily_vf_deal_value_eur_1,0)) + MAX(IFNULL(vf_deal_free_item_eur_monthly,0)) AS monthly_total_country_vf_deal_value_eur,
  SAFE_DIVIDE(
    SUM(IFNULL(ddpd.daily_vf_deal_value_eur_1,0)) + MAX(IFNULL(vf_deal_free_item_eur_monthly,0)),
    SUM(IFNULL(ddpd.daily_all_gmv_eur,0))
  ) AS deal_value_over_total_gmv,
  
  COUNT(DISTINCT
          CASE
            WHEN (is_corporate_delivery_vf_deal_day OR is_corporate_pickup_vf_deal_day OR is_normal_delivery_vf_deal_day OR  is_normal_pickup_vf_deal_day OR is_pro_vf_deal_day OR is_vf_voucher_deal_day OR is_vf_discount_deal_day OR daily_all_vf_orders_1 > 0)
            THEN ddpd.vendor_code
          END) AS vendor_funded_deal_vendors,
  
  COUNT(DISTINCT
          CASE
            WHEN is_daily_active OR daily_all_valid_orders_1 > 0
            THEN ddpd.vendor_code
          END) AS total_country_active_vendors,
          
  SAFE_DIVIDE(
    COUNT(DISTINCT
          CASE
            WHEN (is_corporate_delivery_vf_deal_day OR is_corporate_pickup_vf_deal_day OR is_normal_delivery_vf_deal_day OR  is_normal_pickup_vf_deal_day OR is_pro_vf_deal_day OR is_vf_voucher_deal_day OR is_vf_discount_deal_day)
            THEN ddpd.vendor_code
          END),
          
    COUNT(DISTINCT
          CASE
            WHEN is_daily_active OR daily_all_valid_orders_1 > 0
            THEN ddpd.vendor_code
          END)
  ) AS vendor_coverage,
  
  SAFE_DIVIDE(
    SUM(IFNULL(daily_deal_day_all_valid_orders_1,0)),
    SUM(IFNULL(daily_all_valid_orders_1,0))
  ) AS weighted_vendor_coverage,
  SAFE_DIVIDE(
    SUM(IFNULL(ddpd.daily_fp_funded_value_eur_1,0)),
    SUM(IFNULL(ddpd.daily_fp_funded_value_eur_1,0))+ SUM(IFNULL(ddpd.daily_vf_deal_value_eur_1,0)) + SUM(IFNULL(vf_deal_free_item_eur_monthly,0))
  ) AS co_funding_ratio,
  
  SAFE_DIVIDE(
    SUM(IFNULL(daily_vf_gfv_eur_1,0)),
    SUM(IFNULL(daily_all_vf_orders_1,0))
  ) AS afv_deal_orders,
  
  SAFE_DIVIDE(
    SUM(IF(is_vf_discount_deal_day OR is_corporate_pickup_vf_deal_day OR is_corporate_delivery_vf_deal_day OR is_pro_vf_deal_day OR is_normal_pickup_vf_deal_day OR is_normal_delivery_vf_deal_day,daily_deal_day_all_vf_orders_1,0)),
    SUM(IF(is_vf_discount_deal_day OR is_corporate_pickup_vf_deal_day OR is_corporate_delivery_vf_deal_day OR is_pro_vf_deal_day OR is_normal_pickup_vf_deal_day OR is_normal_delivery_vf_deal_day,daily_deal_day_all_valid_orders_1,0))
  ) AS deal_utilisation,
  
  SAFE_DIVIDE(
    SUM(IFNULL(daily_vf_discount_amount_deal_eur_1,0)),
    SUM(IFNULL(daily_vf_discount_amount_gfv_eur_1,0))
  ) AS avg_perc_discount,
  
FROM pandata_ap_commercial.daily_deals_performance_data AS ddpd
LEFT JOIN (
  
  WITH daily AS (
    SELECT
      global_entity_id,
      date_local, /* date */
      month, /* date trunced */
      business_type,
      chain_code,
      MAX(aaa_type) AS aaa_type,
      MAX(IFNULL(vf_deal_free_item_eur,0)) AS vf_deal_free_item_eur_daily
    FROM pandata_ap_commercial.daily_deals_performance_data
    GROUP BY 1,2,3,4,5
  ),
  
  aaa_sum AS (
  SELECT
    global_entity_id,
    month,
    business_type,
    aaa_type,
    SUM(IFNULL(vf_deal_free_item_eur_daily,0)) AS vf_deal_free_item_eur_monthly
  FROM daily
  GROUP BY 1,2,3,4
  )
  SELECT
    global_entity_id,
    month,
    COALESCE(business_type, 'restaurants') AS business_type,
    SUM(IFNULL(vf_deal_free_item_eur_monthly,0)) AS vf_deal_free_item_eur_monthly
  FROM aaa_sum
  GROUP BY 1, 2, 3
  ORDER BY 1,2,3
) AS free_item_value
   ON free_item_value.global_entity_id = ddpd.global_entity_id
  AND free_item_value.month = ddpd.month
  AND free_item_value.business_type = COALESCE(ddpd.business_type, 'restaurants')
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` sf
ON sf.country_name = ddpd.country
AND sf.vendor_code = ddpd.vendor_code


WHERE COALESCE(ddpd.business_type, 'restaurants') = 'restaurants'
AND ddpd.month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
AND ddpd.month <= CURRENT_DATE()



GROUP BY global_entity_id, country, owner_name, business_type, year_month

ORDER BY global_entity_id, country, business_type, year_month DESC),



year_month as (SELECT format_date('%Y%m',month) as year_month,format_date('%b',month) as month_name
from (
select GENERATE_DATE_ARRAY(date_sub(current_date(),INTERVAL 3 MONTH),current_date(),INTERVAL 1 MONTH) as date ),UNNEST(date) as month),


/* Ads Subqueries */
am_ads AS(

WITH CPPbk1 as (select            
bk.global_entity_id,
c.name as common_name,
sf.owner_name,
v.name as vendor_name,
v.vendor_code,
count (Distinct bk.uuid) as booking_id,
bk.user,
(case when lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) as user_booked,
format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) as year_month,
format_datetime('W%V-%Y', bk.created_at_utc) as booking_week_year,
format_datetime('%V', bk.created_at_utc) as booking_week,
bk.type,
bk.status,
ifnull(SUM(bk.cpp_billing.price),0) as sold_price_local,			
ifnull(SUM (bk.cpp_billing.suggested_price),0) as suggested_price_local,				
ex.fx_rate_eur,
ifnull(SUM (bk.cpp_billing.price),0)/ex.fx_rate_eur as sold_price_eur,					
ifnull(SUM (bk.cpp_billing.suggested_price),0)/ex.fx_rate_eur as suggested_price_eur,									
count (distinct bk.pps_promo_area_uuid) as promo_areas_sold,					
parse_date("%Y%m",cast(bk.year_month as string)) as month_booked,					
format_date('%b', parse_date("%Y%m",cast(bk.year_month as string))) as month,					
case					
when sf.vendor_grade = 'AAA' then 'AAA'					
else 'non-AAA' end as vendor_grade,
date_diff(date(bk.ended_at_utc),date(bk.started_at_utc),day)+1 as duration,
date_diff(date_sub(date_add(date_trunc(date(bk.started_at_utc),MONTH),INTERVAL 1 MONTH),INTERVAl 1 DAY),date(bk.started_at_utc),DAY)+1 as intended_duration,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias


FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk				 
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

WHERE bk.uuid IS NOT NULL
AND bk.billing_type = 'CPP'
AND bk.global_entity_id LIKE 'FP_%'
  AND bk.global_entity_id NOT IN ('FP_RO','FP_BG','FP_DE')
and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) >= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval -3 month)))									and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) <= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval 2 month)))									
GROUP BY 1,2,3,4,5,7,8,9,10,11,12,13,16,20,21,22,23,24,25),

central as (SELECT DISTINCT alias
FROM

((SELECT email,CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`)

UNION ALL

(SELECT email, CONCAT(SPLIT(email, "foodpanda")[OFFSET(0)],"foodpanda.") as alias FROM fulfillment-dwh-production.curated_data_shared_gcc_service.agents WHERE department_name = 'Commercial'))),


cppbk as (select										
n.*,										
case when lower(user) not like '%foodpanda%' then 'Self-Booking' 
     when c.alias is NULL then 'Local' 
     else 'Central' 
     end as source,
case when user_booked = 'self-booking' then 'Automated' else 'Non-Automated' end as automated,																			
format_date('W%V-%Y', current_date) as current_week,										
case when booking_week >= format_date("%V", date_add(current_date, interval -4 week)) and booking_week <= format_date("%V", current_date) then 1 else 0 end as report_weeks										
from CPPbk1 n										
left join central c on n.alias = c.alias
									
											
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27										
order by year_month asc	
),

CPC as (                             
with 
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
owner_name,
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
from cpcbookings_array ca, UNNEST(datelive_nested) AS datelive
),

clicks as (
  SELECT DISTINCT				
bk.uuid as booking_id,					
/*DATE_TRUNC('MONTH',cpc.created_at) AS click_month,*/					
format_date ('%Y%m',date(cpc.click_date)) as click_month,					
count (distinct cpc.pps_item_uuid) as active_areas,					
SUM(orders) AS cpc_orders,					
SUM(quantity) AS spent_clicks					
FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) bil					
JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS cpc					
ON bil.uuid = cpc.pps_item_uuid					
group by 1,2),

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
    ),
    
Pandabox as(
WITH jb as (
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
  gmvc.gmv_class,
  case 
  when sf.vendor_grade = 'AAA' then 'AAA' 
  when sf.vendor_grade is null then 'non-AAA'
  else 'non-AAA'
  end as vendor_grade,
  sf.owner_name as account_owner,
  date(bk.created_at_utc) as created_at,
  date(bk.started_at_utc) as start_date,
  case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
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
  gmvc.gmv_class,
  case 
  when sf.vendor_grade = 'AAA' then 'AAA' 
  when sf.vendor_grade is null then 'non-AAA'
  else 'non-AAA'
  end as vendor_grade,
  sf.owner_name as account_owner,
  date(bk.created_at_utc) as created_at,
  date(bk.started_at_utc) as start_date,
  case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
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

and dv.is_active = TRUE and dv.is_private = FALSE and dv.is_test = FALSE and dv.vertical_type = 'restaurants'
order by vendor_code asc)),

bookings as 
(SELECT jb.*,
case 
when jb2.end_date = jb.start_date then DATE_ADD(jb.start_date,INTERVAL 1 DAY)
else jb.start_date
end as use_start_date,

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
GENERATE_DATE_ARRAY(DATE_TRUNC(use_start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM bookings),

final_bookings as (
SELECT 
id,

global_entity_id,
country as country_name,
vendor_code,
vendor_name,
account_owner,
gmv_class,
vendor_grade,
created_at,
start_date,
CASE 
WHEN use_start_date < datelive then datelive
else use_start_date
end as use_start_date,
end_date,
CASE
when end_date < DATE_SUB(DATE_ADD(datelive,INTERVAL 1 MONTH),INTERVAL 1 DAY) THEN end_date 
ELSE DATE_SUB(DATE_ADD(datelive,INTERVAL 1 MONTH),INTERVAL 1 DAY) end as use_end_date,
DATE_SUB(DATE_ADD(datelive,INTERVAL 1 MONTH),INTERVAL 1 DAY) as last_day,
modified_at,
datelive,
format_date('%Y%m',datelive) as yearmonth,
format_date('%b',datelive) as month_name,
user,
channel,
final_source as final_channel,
type,
status,
units,

FROM pre_bookings,UNNEST(datelive_nested) as datelive
where datelive >= date_add(date_trunc(current_date, month), interval -5 month)),

orders as (
select 
global_entity_id,
country_name,
vendor_code,
vendor_name,
created_date_local,
SUM(joker_fee_eur) as pandabox_revenue_eur,
SUM(joker_fee_local) as pandabox_revenue_local,
SUM(gfv_eur) as pb_gfv_eur,
SUM(gfv_local) as pb_gfv_local,
COUNT(DISTINCT id) as pandabox_orders

from `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals`
   Where 
   is_joker_used = true
   and is_valid_order = true
   and date_trunc(created_date_local, month) >= date_add(date_trunc(current_date, month), interval -5 month)
   
GROUP BY 1,2,3,4,5
)

SELECT 
b.*,
DATE_DIFF(use_end_date,use_start_date,DAY) + 1 as days_running,
units * (DATE_DIFF(use_end_date,use_start_date,DAY) + 1) as total_inventory,
sum(pandabox_revenue_eur) as pandabox_revenue_eur,
sum(pandabox_revenue_local) as pandabox_revenue_local,
sum(pandabox_orders) as pb_orders,
sum(pb_gfv_eur) as pb_gfv_eur,
sum(pb_gfv_local) as pb_gfv_local

from final_bookings b
left join orders o 
on b.global_entity_id = o.global_entity_id
and b.vendor_code = o.vendor_code 
and o.created_date_local >= b.use_start_date 
and o.created_date_local <= b.use_end_date

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
order by global_entity_id,vendor_code ,start_date,yearmonth  asc),

/* COLLATING ALL ADS PRODUCTS */
   cpp_noPR_final as 
   (Select
   cpp.global_entity_id,
   cpp.common_name as country,
   cpp.owner_name,
   cpp.source,
   cpp.vendor_grade,
   cpp.user_booked,
   cast(cpp.year_month as INT64) as year_month,
  sum (case when cpp.status = 'open' then cpp.sold_price_local end) as CPP_noPR_rev_local,
  sum (case when cpp.status = 'open' then cpp.sold_price_eur end) as CPP_noPR_rev_eur,
  0 as CPP_PR_rev_local,
  0 as CPP_PR_rev_eur,
  0 as cpc_rev_local,
  0 as cpc_rev_eur,
  0 as PB_rev_local,
  0 as PB_rev_eur
   from cppbk cpp
   group by 1,2,3,4,5,6,7
   ),
   
   cpp_PR_final as 
   (Select
   
   cpp.global_entity_id,
   cpp.common_name as country,
   cpp.owner_name,
   cpp.source,
   cpp.vendor_grade,
   cpp.user_booked,
   cast(cpp.year_month as INT64) AS year_month,
   0 as CPP_noPR_rev_local,
   0 as CPP_noPR_rev_eur,
   Sum(case when cpp.status = 'open' then cpp.sold_price_local
        when cpp.status = 'cancelled' and cpp.duration >1 then (cpp.sold_price_local/intended_duration) * cpp.duration
        end) as CPP_PR_rev_local,
   Sum(case when cpp.status = 'open' then cpp.sold_price_eur
            when cpp.status = 'cancelled' and cpp.duration >1 then (cpp.sold_price_eur/intended_duration) * cpp.duration
            end) as CPP_PR_rev_local,
  0 as cpc_rev_local,
  0 as cpc_rev_eur,
  0 as PB_rev_local,
  0 as PB_rev_eur
   from cppbk cpp
   group by 1,2,3,4,5,6,7
   ),
   
   cpcfinal as(
   Select
   
   cpc.global_entity_id,
   cpc.country,
   cpc.owner_name as owner_name,
   cpc.channel,
   cpc.vendor_grade,
   cpc.user_booked,
   cast(cpc.yearmonth_live as int64) as year_month,
   0 as CPP_noPR_rev_local,
   0 as CPP_noPR_rev_eur,
   0 as CPP_PR_rev_local,
   0 as CPP_PR_rev_eur,
   sum(cpc.cpc_rev_local) as CPC_rev_local,
   sum(cpc.cpc_rev_eur) as CPC_rev_eur,
   0 as PB_rev_local,
   0 as PB_rev_eur
   from cpc
   group by 1,2,3,4,5,6,7
    ),
    
   pbfinal as(
   select 
   
   pb.global_entity_id,
   pb.country_name,
   pb.account_owner as owner_name,
   pb.final_channel,
   pb.vendor_grade,
   pb.user as user_booked,
   cast(pb.yearmonth as INT64) as year_month,
   0 as CPP_noPR_rev_local,
   0 as CPP_noPR_rev_eur,
   0 as CPP_PR_rev_local,
   0 as CPP_PR_rev_eur,
   0 as cpc_rev_local,
   0 as cpc_rev_eur,
   sum(pb.pandabox_revenue_local) as PB_rev_local,
   sum(pb.pandabox_revenue_eur) as PB_rev_eur,
   from pandabox pb
   group by 1,2,3,4,5,6,7),
   
   finalunion as(
   select * from CPP_noPR_final
   UNION ALL
   select * from cpp_PR_final
   UNION ALL
   Select * from cpcfinal
   UNION ALL
   Select * from pbfinal)

   SELECT 
   
   global_entity_id, 
   country, 
   owner_name, 
   year_month, 
   SUM(ifnull(CPP_noPR_rev_eur,0)) as CPP_rev_eur, 
   SUM(ifnull(cpc_rev_eur,0)) as CPC_rev_eur, 
   SUM(ifnull(PB_rev_eur,0)) as pandabox_rev_eur, 
   (SUM(ifnull(CPP_noPR_rev_eur,0)) + SUM(ifnull(cpc_rev_eur,0)) + SUM(ifnull(PB_rev_eur,0))) as Total_Ads_rev_eur
   from finalunion as f
   GROUP BY global_entity_id, country, owner_name, year_month
   ORDER BY global_entity_id, country, owner_name, year_month)
   

/* Final Output */

SELECT
  
  /*Categories*/
  sf.global_entity_id,
  sf.country_name AS country,
  sf.account_owner,
  sf.account_owner_title,
  sf.account_owner_email,
  y.year_month,
  y.month_name,
  
  /*brand_no.no_of_brands,
  brand_no.no_of_active_vendors,*/
  
  /*Deals*/
  COALESCE(deals.montly_total_country_business_gmv_eur,0) as gmv_eur,
  COALESCE(deals.monthly_total_country_vf_deal_value_eur,0) as vf_deal_value_eur,
  SAFE_DIVIDE(COALESCE(deals.monthly_total_country_vf_deal_value_eur,0),COALESCE(deals.montly_total_country_business_gmv_eur,0)) * 100 AS deal_value_over_gmv,
  COALESCE(deals.vendor_funded_deal_vendors,0) as deal_vendors,
  
  
  
  /* ADs */
  COALESCE(am_ads.CPP_rev_eur,0) as cpp_rev_eur,
  COALESCE(am_ads.CPC_rev_eur,0) as cpc_rev_eur,
  COALESCE(am_ads.pandabox_rev_eur,0) as pandabox_rev_eur,
  COALESCE(am_ads.Total_Ads_rev_eur,0) as ads_rev_eur,
  SAFE_DIVIDE(COALESCE(am_ads.Total_Ads_rev_eur,0),COALESCE(deals.montly_total_country_business_gmv_eur,0)) * 100 AS ads_rev_over_gmv
  
 
  
FROM sf_account_info sf
      CROSS JOIN year_month y

     /*LEFT JOIN brand_no
             ON brand_no.global_entity_id = sf.global_entity_id
             AND sf.account_owner = brand_no.account_owner*/
      

      LEFT JOIN deals
             ON sf.global_entity_id =  deals.global_entity_id
             AND sf.account_owner = deals.owner_name
             AND CAST(y.year_month AS INT64) = CAST(deals.year_month AS INT64)
    

      LEFT JOIN am_ads
            ON sf.global_entity_id = am_ads.global_entity_id
            AND sf.account_owner = am_ads.owner_name
            AND CAST(y.year_month AS INT64) = am_ads.year_month


/*WHERE sf.account_owner IN ('Abigail Jinggut', 'Nathaneal Sylvester', 'Damian Wong')
AND y.year_month = '202106'*/

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
ORDER BY 1,2,4,3,6 DESC
