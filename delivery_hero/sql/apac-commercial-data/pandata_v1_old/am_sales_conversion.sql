--- This gives you Number of Active Vendors, Sales completed for CPP, CPC, Pandabox per AM for the last 3 Months
--- Key metrics: 
-- 1. Sales_completed_per_AM = (number_of_cpp_vendors_per_AM + number_of_cpc_vendors_per_AM + number_of_pandabox_vendors_per_AM + number_of deals_per_AM)
-- 2. Sales_completed_per_AM/Number_of_Active_Vendors_per_AM gives us a ratio which can be more than 1 as a single vendor could buy multiple products



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
 
 portfolio_size AS (
 SELECT DISTINCT global_entity_id, country_name, account_owner_title, account_owner, account_owner_email, no_of_brands, no_of_active_vendors
 FROM brand_no),

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
bk.rdbms_id,
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
bk.global_entity_id,
CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.") as alias,
COUNT(DISTINCT v.vendor_code) AS cpp_vendors


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
and bk.rdbms_id in (7,12,15,16,17,18,19,20,219,220,221,263)
and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) >= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval -3 month)))									and format_datetime("%Y%m", parse_datetime("%Y%m",cast(bk.year_month as string))) <= format_datetime("%Y%m",datetime(date_add(date_trunc(current_date,month),interval 2 month)))									
GROUP BY 1,2,3,4,5,7,8,9,10,11,12,13,16,20,21,22,23,24,25,26),

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
									
											
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28									
order by year_month asc	
),

CPC as (                             
with 
cpcbookings as(
select 
bk.rdbms_id,
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
COUNT (DISTINCT bk.vendor_code) AS cpc_vendors

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk, UNNEST(bk.cpc_billings) cpc
left join `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks`  cl on cl.pps_item_uuid=bk.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dvz on dvz.global_entity_id = bk.global_entity_id and dvz.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
LEFT JOIN central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where bk.uuid is not null
and click_price is not null
and bk.billing_type = 'CPC'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),

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
cpc_vendors,
click_price_local,
budgeted_clicks,
initial_budget_local,
from cpcbookings_array ca, UNNEST(datelive_nested) AS datelive
),

clicks as (
  SELECT DISTINCT				
bk.uuid as booking_id,					
/*DATE_TRUNC('MONTH',cpc.created_at) AS click_month,*/					
format_date ('%Y%m',date(cpc.created_at_utc)) as click_month,					
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
  ROW_NUMBER() OVER (PARTITION BY bk.rdbms_id,bk.vendor_code ORDER BY date(started_at_utc) asc) as row_no
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
  ROW_NUMBER() OVER (PARTITION BY bk.rdbms_id,bk.vendor_code ORDER BY date(started_at_utc) asc) + 1 as row_no
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
LEFT JOIN jb2 on jb.rdbms_id = jb2.rdbms_id and jb.vendor_code = jb2.vendor_code and jb.row_no = jb2.row_no

order by jb.vendor_code,jb.start_date asc),

pre_bookings as (
SELECT
*,
GENERATE_DATE_ARRAY(DATE_TRUNC(use_start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested,
FROM bookings),

final_bookings as (
SELECT 
id,
rdbms_id,
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
units
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
sum(pb_gfv_local) as pb_gfv_local,
COUNT(DISTINCT b.vendor_code) AS pandabox_vendors

from final_bookings b
left join orders o 
on b.global_entity_id = o.global_entity_id
and b.vendor_code = o.vendor_code 
and o.created_date_local >= b.use_start_date 
and o.created_date_local <= b.use_end_date

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
ORDER BY rdbms_id,vendor_code ,start_date,yearmonth  asc),

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
  0 as PB_rev_eur,
  SUM(cpp.cpp_vendors) as cpp_vendors,
  0 AS cpc_vendors,
  0 AS pandabox_vendors
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
  0 as PB_rev_eur,
  SUM(cpp.cpp_vendors) as cpp_vendors,
  0 AS cpc_vendors,
  0 AS pandabox_vendors
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
   0 as PB_rev_eur,
   0 as cpp_vendors,
   SUM(cpc.cpc_vendors) AS cpc_vendors,
   0 AS pandabox_vendors
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
   0 as cpp_vendors,
   0 AS cpc_vendors,
   SUM(pb.pandabox_vendors) AS pandabox_vendors
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
   SUM(ifnull(cpp_vendors,0)) as cpp_vendors, 
   SUM(ifnull(cpc_vendors,0)) as cpc_vendors, 
   SUM(ifnull(pandabox_vendors,0)) as pandabox_vendors, 
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
  
  /* Portfolio Size*/
 
  portfolio_size.no_of_active_vendors,
  
  /*Deals*/
 
  COALESCE(deals.vendor_funded_deal_vendors,0) as deal_vendors,
  
  
  
  /* ADs */
  COALESCE(am_ads.cpp_vendors,0) as cpp_vendors,
  COALESCE(am_ads.cpc_vendors,0) as cpc_vendors,
  COALESCE(am_ads.pandabox_vendors,0) as pandabox_vendors,
  (COALESCE(deals.vendor_funded_deal_vendors,0) + COALESCE(am_ads.cpp_vendors,0) + COALESCE(am_ads.cpc_vendors,0) + COALESCE(am_ads.pandabox_vendors,0)) AS sales_completed,
  SAFE_DIVIDE((COALESCE(deals.vendor_funded_deal_vendors,0) + COALESCE(am_ads.cpp_vendors,0) + COALESCE(am_ads.cpc_vendors,0) + COALESCE(am_ads.pandabox_vendors,0)), portfolio_size.no_of_active_vendors) AS sales_over_active_vendors_ratio
  
  
  
 
  
FROM sf_account_info sf
      CROSS JOIN year_month y

      LEFT JOIN portfolio_size
             ON portfolio_size.global_entity_id = sf.global_entity_id
             AND sf.account_owner = portfolio_size.account_owner
      

      LEFT JOIN deals
             ON sf.global_entity_id =  deals.global_entity_id
             AND sf.account_owner = deals.owner_name
             AND CAST(y.year_month AS INT64) = CAST(deals.year_month AS INT64)
    

      LEFT JOIN am_ads
            ON sf.global_entity_id = am_ads.global_entity_id
            AND sf.account_owner = am_ads.owner_name
            AND CAST(y.year_month AS INT64) = am_ads.year_month


WHERE sf.account_owner_email IN ('alsaddam.hossain@foodpanda.com.bd','belal.shaikh@foodpanda.com.bd','fahad.ahmed@foodpanda.com.bd','galib.siyam@foodpanda.com.bd','jamil.hassan@foodpanda.com.bd','kashfia.ibrahim@foodpanda.com.bd','hobieb.murtuza@foodpanda.com.bd','saikate.talukdar@foodpanda.com.bd','shams.ahmed@foodpanda.com.bd','sojib.h@foodpanda.com.bd','fahad.h@foodpanda.com.bd','mushfiqur.rahman@foodpanda.com.bd','nazifa.tabassum@foodpanda.com.bd','redwanul.hoque@foodpanda.com.bd','ridwan.k@foodpanda.com.bd','shahidul.islam.2@foodpanda.com.bd','shariful.islam@foodpanda.com.bd','md.shohag.2@foodpanda.com.bd','fayjul.karim@foodpanda.com.bd','imtiazul.islam@foodpanda.com.bd','mehedi.kawsar@foodpanda.com.bd','mohammad.rokonuzzaman@foodpanda.com.bd','shafa.m@foodpanda.com.bd','shahriyar.rabby@foodpanda.com.bd','shofiul.bashar@foodpanda.com.bd','talha.akand@foodpanda.com.bd','zawad.bin@foodpanda.com.bd','saad.enamul@foodpanda.com.bd','zaeed.zubair@foodpanda.com.bd','chantreametrey.tram@foodpanda.com.kh','kimlong.chheng@foodpanda.com.kh','lytho.vorn@foodpanda.com.kh','molika.oeun@foodpanda.com.kh','monika.phor@foodpanda.com.kh','raksmeyprophea.reach@foodpanda.com.kh','samnang.soun@foodpanda.com.kh','sokunvoleak.sieng@foodpanda.com.kh','sotheara.tun@foodpanda.com.kh','sreykun.khom@foodpanda.com.kh','thida.tep@foodpanda.com.kh','uosa.bros@foodpanda.com.kh','vaddhinee.yean@foodpanda.com.kh','yuleth.yim@foodpanda.com.kh','thida.thai@foodpanda.com.kh','tichlim.tang@foodpanda.com.kh','socheata.mony@foodpanda.com.kh','sreysrors.chea@foodpanda.com.kh','vinh.dam@foodpanda.com.kh','kosal.sang@foodpanda.com.kh','sovanndararith.hong@foodpanda.com.kh','lewis.chan@foodpanda.hk','yoyo.lui@foodpanda.hk','alexandra.blez@foodpanda.hk','christmas.liu@foodpanda.hk','felix.hung@foodpanda.hk','lodgmond.hung@foodpanda.hk','sophie.ho@foodpanda.hk','ray.wong@foodpanda.hk','rico.chan@foodpanda.hk','charlie.to@foodpanda.hk','clare.lam@foodpanda.hk','cynthia.cheung@foodpanda.hk','dicky.lam@foodpanda.hk','elva.wu@foodpanda.hk','abby.so@foodpanda.hk','chantal.chan@foodpanda.hk','charles.chong@foodpanda.hk','colman.lam@foodpanda.hk','leo.mak@foodpanda.hk','marcella.ho@foodpanda.hk','rossana.lam@foodpanda.hk','vidur.yadav@foodpanda.hk','louisa.fong@foodpanda.hk','yuki.nagoya@foodpanda.co.jp','hirokazu.hanakawa@foodpanda.co.jp','ai.kameyama@foodpanda.co.jp','hitomi.hasegawa@foodpanda.co.jp','kazuki.arai@foodpanda.co.jp','kazuya.imura@foodpanda.co.jp','kenta.tamura@foodpanda.co.jp','kentaro.yama@foodpanda.co.jp','koji.yokoi@foodpanda.co.jp','kyohei.goto@foodpanda.co.jp','miki.doi@foodpanda.co.jp','satoki.takada@foodpanda.co.jp','satoshi.yamamoto@foodpanda.co.jp','takashi.hayashi@foodpanda.co.jp','tsunade.togashi@foodpanda.co.jp','yuki.tsuchida@foodpanda.co.jp','yukiko.imuta@foodpanda.co.jp','yumi.shioya@foodpanda.co.jp','yusuke.inoue@foodpanda.co.jp','yusuke.mizutani@foodpanda.co.jp','ko.murata@foodpanda.co.jp','katsuhiro.ueda@foodpanda.co.jp','tomoko.sugiyama@foodpanda.co.jp','ryo.onishi@foodpanda.co.jp','yuko.furugori@foodpanda.co.jp','boudsaba.bolivong@foodpanda.la','denphoum.sysaykeo@foodpanda.la','sinlaphone.boupha@foodpanda.la','somchit.phetsada@foodpanda.la','sonekeo.sibounheuang@foodpanda.la','sonevongsouda.luanglath@foodpanda.la','vannaleuth.dangmany@foodpanda.la','yutthasith.vongpraseuth@foodpanda.la','ruby.ang@foodpanda.my','ahmad.faiz@foodpanda.my','amira.omar@foodpanda.my','amirul.mohd.2@foodpanda.my','bernard.voon@foodpanda.my','isabell.kum@foodpanda.my','muhammad.aminuddin@foodpanda.my','atiqah.fadel@foodpanda.my','nordiamalaya@foodpanda.my','nur.ibrahim@foodpanda.my','ray.choo@foodpanda.my','sandra.chow@foodpanda.my','tay.jun@foodpanda.my','amira.shahida@foodpanda.my','asyraf.rosli@foodpanda.my','bryan.tang@foodpanda.my','choyying@foodpanda.my','geraldine.yong@foodpanda.my','guan.liew@foodpanda.my','hazelyn.mohd@foodpanda.my','kokhou@foodpanda.my','intan.nadia@foodpanda.my','jeoffry.nasir@foodpanda.my','joo.wong@foodpanda.my','kuan.gan@foodpanda.my','leyna.yusof@foodpanda.my','mark.dcruz@foodpanda.my','megala.nadaraja@foodpanda.my','mustaqeem.carlos@foodpanda.my','nathaneal.sylvester@foodpanda.my','nicholas.chin@foodpanda.my','nicky.lau@foodpanda.my','zubaidah.hassan@foodpanda.my','raymond.martin@foodpanda.my','lachesis.ang@foodpanda.my','siti.razan@foodpanda.my','subash.sivakumar@foodpanda.my','sue.khoo@foodpanda.my','sukhjeet@foodpanda.my','tengku.aziz@foodpanda.my','tracy.sta@foodpanda.my','yong.weng@foodpanda.my','yuvalin.krishnan@foodpanda.my','zoe.tan@foodpanda.my','mohd.md@foodpanda.my','abigail.jinggut@foodpanda.my','chris.chan@foodpanda.my','david.wong@foodpanda.my','farwin.h@foodpanda.my','k.dhayan@foodpanda.my','michele.kong@foodpanda.my','vivien.tan.2@foodpanda.my','rebecca.ng@foodpanda.my','ei.htay@foodpanda.com.mm','paing.theingar@foodpanda.com.mm','aung.yint@foodpanda.com.mm','htet.aung@foodpanda.com.mm','twal.kyaw@foodpanda.com.mm','zarni.hein@foodpanda.com.mm','min.kyaw@foodpanda.com.mm','aung.thuya@foodpanda.com.mm','ei.min@foodpanda.com','hnin.yee@foodpanda.com.mm','ni.zaw@foodpanda.com.mm','shun.phyu@foodpanda.com','thet.zaw@foodpanda.com','win.thaw@foodpanda.com','ayub.maniya@foodpanda.pk','abdul.wasay@foodpanda.pk','faran.ahmed@foodpanda.pk','hajra.naqvi@foodpanda.pk','haseeb.jawaid@foodpanda.pk','umar.ali@foodpanda.pk','shahzad.anjum@foodpanda.pk','shehzad.shah@foodpanda.pk','waqas.bashir@foodpanda.pk','wasim.bacha@foodpanda.pk','zaheer.khan@foodpanda.pk','muhammad.salman.2@foodpanda.pk','ammar.malik@foodpanda.pk','hamza.bukhari@foodpanda.pk','hamza.rasheed@foodpanda.pk','jibran.khero@foodpanda.pk','muhammad.ali.2@foodpanda.pk','noman.shareef@foodpanda.pk','osama.ahmad@foodpanda.pk','roshmina.hassan@foodpanda.pk','sarmad.kahut@foodpanda.pk','talha.khan@foodpanda.pk','syed.haider.3@foodpanda.pk','arslan.khan@foodpanda.pk','saad.ashraf@foodpanda.pk','sharafat.ali@foodpanda.pk','rehman.yousaf@foodpanda.pk','arslan.khalid@foodpanda.pk','aabeera.salman@foodpanda.pk','aaqil@foodpanda.pk','abdullah.quddus@foodpanda.pk','ahmed.chinoy@foodpanda.pk','basit.dandiya@foodpanda.pk','bilal.kathia@foodpanda.pk','hammad.zulfiqar@foodpanda.pk','hasnain.ahmed@foodpanda.pk','hassan.mujtaba@foodpanda.pk','syed.shah@foodpanda.pk','jarry.abbas@foodpanda.pk','junaid.mahboob@foodpanda.pk','majid.shafique@foodpanda.pk','muhammad.mustafa@foodpanda.pk','saud.dharakla@foodpanda.pk','sheikh.mehmood@foodpanda.pk','asim.agha@foodpanda.pk','usman.baig@foodpanda.pk','mohammad.sikander@foodpanda.pk','washma@foodpanda.pk','melissa.villegas@foodpanda.ph','mia.miranda@foodpanda.ph','nicole.crisostomo@foodpanda.ph','raven.roxas@foodpanda.ph','remigia.eleazar@foodpanda.ph','rex.castro@foodpanda.ph','rhenz.haldos@foodpanda.ph','robert.labalan@foodpanda.ph','r.miedes@foodpanda.ph','ron.pineda@foodpanda.ph','sasha.pellano@foodpanda.ph','trisia.visitacion@foodpanda.ph','vonryan.meneses@foodpanda.ph','wendelyn.dalagan@foodpanda.ph','zaira.ampongan@foodpanda.ph','marigold.morales@foodpanda.ph','jose.javier@foodpanda.ph','louie.lim@foodpanda.ph','m.cortes@foodpanda.ph','joshua.diaz@foodpanda.ph','j.yap@foodpanda.ph','kymond.dimaandal@foodpanda.ph','mariama.murillo@foodpanda.ph','kn.quiogue@foodpanda.ph','ralph.mandin@foodpanda.ph','nur.afifah@foodpanda.sg','saifuddin.samsuri@foodpanda.sg','wei.lee@foodpanda.sg','yi.choi@foodpanda.sg','a.pannerselvam@foodpanda.sg','chaihock.ang@foodpanda.sg','christine.swee@foodpanda.sg','durga.sasha@foodpanda.sg','leon.goh@foodpanda.sg','melody.soh@foodpanda.sg','rachel.cheong@foodpanda.sg','shiqian.tang@foodpanda.sg','vanessa.lee@foodpanda.sg','weiqi.chia@foodpanda.sg','b.koh@foodpanda.sg','amanda.ong@foodpanda.sg','cherrin.lau@foodpanda.sg','fongzhi.lim@foodpanda.sg','hui.lim@foodpanda.sg','khushboo.khiatani@foodpanda.sg','maxe.huang@foodpanda.sg','seth.ong@foodpanda.sg','apatsanun.koonkanaviwat@foodpanda.co.th','chanon.visuthajaree@foodpanda.co.th','chiraphan.phengsombun@foodpanda.co.th','chuleekorn.chan@foodpanda.co.th','isariya.khonwai@foodpanda.co.th','khunat.chumkomon@foodpanda.co.th','natchaya.saenkul@foodpanda.co.th','patchadaporn.kusakul@foodpanda.co.th','pol.dhutikraikriang@foodpanda.co.th','ratchanont.rojanavanichakorn@foodpanda.co.th','rattanaporn.thanabomrungkul@foodpanda.co.th','sirada.trongtorsak@foodpanda.co.th','sujeeporn.satyanam@foodpanda.co.th','sujittra.charoensiwawat@foodpanda.co.th','supamat.boonwattanasoontorn@foodpanda.co.th','tanaboon.thanhakitiwat@foodpanda.co.th','tanvarat.raktrakultram@foodpanda.co.th','thanaporn.bannakeit@foodpanda.co.th','thongpraw.chaiprasith@foodpanda.co.th','varisa.lee@foodpanda.co.th','watcharid.kulvipachwatana@foodpanda.co.th','chanikarn.porntanalert@foodpanda.co.th','chanwit.jongprasert@foodpanda.co.th','daochula.prueksachatpaisal@foodpanda.co.th','kanatach.ajchariyasucha@foodpanda.co.th','polake.nakanakorn@foodpanda.co.th','surattikan.thongchim@foodpanda.co.th','yosawadee.grittiyarangsan@foodpanda.co.th','kasemsant.chaovanavirat@foodpanda.co.th','navarat.chantathaweewat@foodpanda.co.th','panus.nipatasaj@foodpanda.co.th','pattarnun.meesiripeyratorn@foodpanda.co.th','raddao.samphaoyon@foodpanda.co.th','arinraya.pattamapisit@foodpanda.co.th','arisara.pumsawai@foodpanda.co.th','nattakarn.trongtorkarn@foodpanda.co.th','nattalalitta.sotana@foodpanda.co.th','panitinee.sengchuan@foodpanda.co.th','rawinnipa.patcharasetkhun@foodpanda.co.th','wipada.choorat@foodpanda.co.th','bheerawich.jittatam@foodpanda.co.th','boonsita.kwampaiboon@foodpanda.co.th','boriwat.khattagul@foodpanda.co.th','chamaiporn.thipueang@foodpanda.co.th','chanidapa.phochan@foodpanda.co.th','chokdee.chuangchod@foodpanda.co.th','chotiga.jitsamart@foodpanda.co.th','huda.suetapor@foodpanda.co.th','husna.buhas@foodpanda.co.th','jalaka.limsila@foodpanda.co.th','jariya.mongsiri@foodpanda.co.th','jasmee.leemad@foodpanda.co.th','wasu.jinajin@foodpanda.co.th','kanjana.eamsuk@foodpanda.co.th','kanwara.kraiphattasin@foodpanda.co.th','kanyakorn.sakorn@foodpanda.co.th','khakhanang.potanon@foodpanda.co.th','kodchakron.phomsri@foodpanda.co.th','kullastree.bhawatkodchakul@foodpanda.co.th','luksana.gerdpocha@foodpanda.co.th','narongsak.kareesor@foodpanda.co.th','natchaya.rattanamoonpunya@foodpanda.co.th','natita.roongroj@foodpanda.co.th','natsurang.kongnaowarat@foodpanda.co.th','natthakan.palee@foodpanda.co.th','nawapol.opapphong@foodpanda.co.th','nitchanan.srisuwan@foodpanda.co.th','nitipat.meema@foodpanda.co.th','nonlaphan.lohpeanparkpean@foodpanda.co.th','nuntharika.thawonkul@foodpanda.co.th','pakpen.sangin@foodpanda.co.th','panida.anafarang@foodpanda.co.th','pathomphon.rueangpadit@foodpanda.co.th','phattharawadee.phothinakkha@foodpanda.co.th','phummipat.chansiri@foodpanda.co.th','pichamon.boonpor@foodpanda.co.th','pijittra.khongmueang@foodpanda.co.th','piyachat.pomsuk@foodpanda.co.th','ganta.pulsiricoch@foodpanda.co.th','rujira.mathalay@foodpanda.co.th','sakunruk.sukcomedoung@foodpanda.co.th','salakjit.soniam@foodpanda.co.th','supawadee.thavonjinda@foodpanda.co.th','suphawat.ramangkur@foodpanda.co.th','suppakrit.isarapanich@foodpanda.co.th','suratchada.yimnoi@foodpanda.co.th','sureechay.pamajai@foodpanda.co.th','surintara.phanphakdeecharoen@foodpanda.co.th','sutatip.thosamoson@foodpanda.co.th','tanaporn.ruenkeaw@foodpanda.co.th','thanaphorn.sanwangthanachok@foodpanda.co.th','t.khunphitphibul@foodpanda.co.th','treephet.wijitrapab@foodpanda.co.th','tunjira.kaewkajangkul@foodpanda.co.th','nisamanee.turnhit@foodpanda.co.th','utsanee.phuwathitanon@foodpanda.co.th','wachiraporn.wilaikul@foodpanda.co.th','waenuraihan.waedoloh@foodpanda.co.th','waralak.khotmongkhon@foodpanda.co.th','wathini.mueanmat@foodpanda.co.th','worrarat.thitnongwaeng@foodpanda.co.th','nadia.lin@foodpanda.tw','amber.yang@foodpanda.tw','andy.chang@foodpanda.tw','backy.kao@foodpanda.tw','banny.yin@foodpanda.tw','carol.wu@foodpanda.tw','catherine.chang@foodpanda.tw','charlene.tu@foodpanda.tw','charlie.hsieh@foodpanda.tw','chloe.chung@foodpanda.tw','cindy.wang@foodpanda.tw','dana.yuan@foodpanda.tw','david.lee@foodpanda.tw','eva.liau@foodpanda.tw','hank.hu@foodpanda.tw','jack.huang@foodpanda.tw','jenny.wang@foodpanda.tw','jerry_cl.huang@foodpanda.tw','jessica.lin@foodpanda.tw','joel.chen@foodpanda.tw','keith.lin@foodpanda.tw','lynn.chiang@foodpanda.tw','maggie_hc.cheng@foodpanda.tw','middy.chang@foodpanda.tw','mike.kuo@foodpanda.tw','nellie.chang@foodpanda.tw','nelly.wang@foodpanda.tw','ray.kuo@foodpanda.tw','rebecca.yen@foodpanda.tw','roger.yu@foodpanda.tw','sandy.wu.2@foodpanda.tw','sasha.peng@foodpanda.tw','stacy.guo@foodpanda.tw','stanley.liu@foodpanda.tw','tina.huang@foodpanda.tw','tingting.hsu@foodpanda.tw','tracy.tseng@foodpanda.tw','troy.chao@foodpanda.tw','young.chen@foodpanda.tw','zero.lin@foodpanda.tw','nicole.huang@foodpanda.tw','jason.wang@foodpanda.tw','joy.chiang@foodpanda.tw','hardy.han@foodpanda.tw','jeanie.lu@foodpanda.tw','alan.wang@foodpanda.tw','alicia.huang@foodpanda.tw','astrid.liao@foodpanda.tw','crystal.huang@foodpanda.tw','danny.tung@foodpanda.tw','edison.lai@foodpanda.tw','elan.shi@foodpanda.tw','frank.lin@foodpanda.tw','howard.lai@foodpanda.tw','jack.liu@foodpanda.tw','jeremy.lu@foodpanda.tw','jerry.tseng@foodpanda.tw','jim.lin@foodpanda.tw','johnny.chen@foodpanda.tw','karan.hsu@foodpanda.tw','mark.chung@foodpanda.tw','nat.chen@foodpanda.tw','pony.cheng@foodpanda.tw','ray.chen@foodpanda.tw','rosie.hsu@foodpanda.tw','sean.wu@foodpanda.tw','shyan.chao@foodpanda.tw','stanley_ph.tseng@foodpanda.tw','tracy.wang@foodpanda.tw','zack.lee@foodpanda.tw','wendy.chen@foodpanda.tw','alen.liang@foodpanda.tw','sam.lee@foodpanda.tw')


GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1,2,4,3,6 DESC
