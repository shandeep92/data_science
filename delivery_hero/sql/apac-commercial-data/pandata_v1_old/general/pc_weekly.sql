WITH vendor_order_data AS (
SELECT
    v.rdbms_id,
    c.common_name AS country,
    EXTRACT(ISOWEEK FROM o.expected_delivery_at_local) AS week_no,
    CONCAT(FORMAT_DATE("%Y", DATE_ADD(date_trunc(EXTRACT(DATE FROM o.expected_delivery_at_local), isoweek),INTERVAL 6 Day)),"-",CAST(EXTRACT(ISOWEEK FROM o.expected_delivery_at_local) AS STRING)) AS week,
    FORMAT_DATE("%A",EXTRACT(DATE FROM o.expected_delivery_at_local)) AS full_day,
    FORMAT_DATE("%a %d-%b",EXTRACT(DATE FROM o.expected_delivery_at_local)) AS day_of_week,
    FORMAT_TIMESTAMP("%H - %I %p",o.expected_delivery_at_local) AS hr_of_day,
    v.vendor_code,
    v.vendor_name,
    v.chain_name,
    v.chain_code,
    is_chain_active,
    --coalesce(vc.gmv_class,'NotAssigned') as GMV_class,
    (case when o.is_failed_order_vendor is true then dr.decline_reason_code end) as decline_reason_no,
    (case when o.is_failed_order_vendor is true then dr.title end) as decline_reason,
    --(case when s.failed_order_vendor=1 then o.vendor_comment end) as vendor_comment,
    COUNT(DISTINCT case when o.is_gross_order is true then o.id end) as Gross_orders,
    COUNT(DISTINCT case when o.is_valid_order is true then o.id end) as Successful_orders,
    sum(case when o.is_valid_order is true then o.gmv_local end) as GMV_local,
    sum(case when o.is_valid_order is true then o.gfv_local end) as GFV_Local,
    COUNT(DISTINCT case when o.is_failed_order_vendor is true then o.id end) as Vendor_Fails,
    sum(case when  o.is_failed_order_vendor is true then o.gmv_local end) as Lost_Revenue_local,
    count(distinct case when ops.is_automated is true and o.status_id in (11,12,13,14,22,24) then o.id end) as automated_orders,
    count(distinct case when ops.is_automated is false then o.id end) as non_automated_orders
from pandata.fct_orders o 
LEFT JOIN pandata.dim_vendors v on v.rdbms_id=o.rdbms_id and v.id=o.vendor_id
LEFT JOIN pandata.dim_countries c on c.rdbms_id=v.rdbms_id
LEFT JOIN pandata.fct_order_ops ops on o.rdbms_id=ops.rdbms_id and o.id=ops.order_id
LEFT JOIN pandata.dim_decline_reasons dr on o.rdbms_id = dr.rdbms_id AND dr.id = o.decline_reason_id
--left join bl_region_ap.vendor_gmv_class vc on v.rdbms_id=vc.rdbms_id and v.vendor_code=vc.vendor_code
WHERE
    DATE(o.expected_delivery_at_local) >= date_trunc(DATE_SUB(current_date(), INTERVAL 2 WEEK), isoweek)
    AND DATE(o.expected_delivery_at_local) <= date_trunc(DATE_SUB(current_date(), INTERVAL 0 WEEK), isoweek)
    AND o.created_date_local >= date_trunc(DATE_SUB(current_date(), INTERVAL 3 WEEK), isoweek)
    AND ops.created_date_local >= date_trunc(DATE_SUB(current_date(), INTERVAL 3 WEEK), isoweek)
        --and v.vendor_code = 's7dg'
        --and v.vendor_code = 'w2ry'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by 2,3,4,5,6,7
),
order_code_data AS (
SELECT
    v.rdbms_id,
    c.common_name AS Country,
    EXTRACT(ISOWEEK FROM o.expected_delivery_at_local) AS week_no,
    CONCAT(CAST(EXTRACT(ISOWEEK FROM o.expected_delivery_at_local) AS STRING),"-",FORMAT_DATE("%Y", DATE_ADD(date_trunc(EXTRACT(DATE FROM o.expected_delivery_at_local), isoweek),INTERVAL 6 Day))) AS week,
    FORMAT_DATE("%A",EXTRACT(DATE FROM o.expected_delivery_at_local)) AS full_day,
    FORMAT_DATE("%a %d-%b",EXTRACT(DATE FROM o.expected_delivery_at_local)) AS day_of_week,
    FORMAT_TIMESTAMP("%H - %I %p",o.expected_delivery_at_local) AS hr_of_day,
    v.vendor_code,
    v.vendor_name,
    v.chain_name,
    v.chain_code,
    is_chain_active,
    --coalesce(vc.gmv_class,'NotAssigned') as GMV_class,
    (case when o.is_failed_order_vendor is true then dr.decline_reason_code end) as decline_reason_no,
    (case when o.is_failed_order_vendor is true then dr.title end) as decline_reason,
    --(case when s.failed_order_vendor=1 then o.vendor_comment end) as vendor_comment,
    STRING_AGG(DISTINCT (case when o.is_failed_order_vendor is true then o.order_code_google end), ', ' ) as order_code
FROM pandata.fct_orders o 
LEFT JOIN pandata.dim_vendors v on v.rdbms_id=o.rdbms_id and v.id=o.vendor_id
LEFT JOIN pandata.dim_countries c on c.rdbms_id=v.rdbms_id
LEFT JOIN pandata.dim_decline_reasons dr on o.rdbms_id = dr.rdbms_id AND dr.id = o.decline_reason_id
--left join bl_region_ap.vendor_gmv_class vc on v.rdbms_id=vc.rdbms_id and v.vendor_code=vc.vendor_code
WHERE
    EXTRACT(DATE FROM o.expected_delivery_at_local) >= date_trunc(DATE_SUB(current_date(), INTERVAL 2 WEEK), isoweek)
    AND EXTRACT(DATE FROM o.expected_delivery_at_local) <= date_trunc(DATE_SUB(current_date(), INTERVAL 0 WEEK), isoweek)
    AND o.created_date_local >= date_trunc(DATE_SUB(current_date(), INTERVAL 3 WEEK), isoweek)
    AND o.is_failed_order_vendor is true
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 2,3,4,5,6,7
)
SELECT
  vo.rdbms_id,
  vo.country,
  vo.week_no,
  vo.week,
  vo.full_day,
  vo.day_of_week,
  vo.hr_of_day,
  vo.vendor_code,
  vo.vendor_name,
  vo.chain_name,
  vo.chain_code,
  vo.is_chain_active,
  vo.decline_reason_no,
  vo.decline_reason,
  vo.Gross_orders,
  vo.Successful_orders,
  vo.GMV_local,
  vo.GFV_Local,
  vo.Vendor_Fails,
  vo.Lost_Revenue_local,
  vo.automated_orders,
  vo.non_automated_orders,
  oc.order_code
FROM vendor_order_data vo
LEFT JOIN order_code_data oc using(rdbms_ID,Country,week_no,week,full_day,day_of_week,hr_of_day,vendor_code,Vendor_name,chain_name,chain_code,is_chain_active,decline_reason_no,decline_reason)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
order by 1,2,3,4,5,6,7
