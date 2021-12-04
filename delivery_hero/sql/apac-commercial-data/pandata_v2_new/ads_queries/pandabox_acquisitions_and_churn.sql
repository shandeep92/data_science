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
  bk.vendor_code,
  dv.name as vendor_name,
  gmvc.gmv_class,
  case 
  when sf.vendor_grade = 'AAA' then 'AAA' 
  when sf.vendor_grade is null then 'non-AAA'
  else 'non-AAA'
  end as vendor_grade,
  date(bk.created_at_utc) as created_at,
  date(started_at_utc) as start_date,
  case when date(ended_at_utc) is null then current_date() else date(ended_at_utc) end as end_date,
  date(modified_at_utc) as modified_at,
  pps_vendor_uuid as vendor_id,
  year_month,
  user,
  case 
  when user = 'joker-sync-command' or bk.user = 'valerie.ong@foodpanda.com' then 'Self-Booking'
  when ct.alias is NULL then 'Local' else 'Central' end as channel,
  bk.type,
  bk.status,
  cpu_billing.units as units
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on sf.global_entity_id = bk.global_entity_id and sf.vendor_code = bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on bk.global_entity_id = dv.global_entity_id and bk.vendor_code = dv.vendor_code
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join central ct on ct.alias = CONCAT(SPLIT(bk.user, "foodpanda")[OFFSET(0)],"foodpanda.")

where  bk.type = 'joker'
and bk.rdbms_id IN (7,12,15,16,17,18,19,20,219,220,221,263)
and dv.is_active = TRUE and dv.is_private = FALSE and dv.is_test = FALSE and dv.vertical_type = 'restaurants'
order by vendor_code asc)),

jb1 as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date asc) as seq
FROM jb
),

jb2 as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date asc)-1 as seq
FROM jb
),

jb3 as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY global_entity_id,vendor_code ORDER BY start_date asc)+1 as seq
FROM jb
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


FROM jb1 a
LEFT JOIN jb2 b 
ON a.global_entity_id = b.global_entity_id
and a.vendor_code = b.vendor_code
and a.seq = b.seq
LEFT JOIN jb3 c
ON a.global_entity_id = c.global_entity_id
and a.vendor_code = c.vendor_code
and a.seq = c.seq
and format_date('%Y%m',a.start_date) = format_date('%Y%m',c.end_date)

ORDER BY a.rdbms_id,a.vendor_code,a.seq asc
