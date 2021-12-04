with gmv as (
SELECT
    o.global_entity_id,
    c.name as country,
    format_datetime('%b',datetime(created_date_local)) AS month,
    format_datetime('%m',datetime(created_date_local)) AS month_no,
    format_datetime('%Y%m',datetime(created_date_local)) AS year_month,
    SUM(gmv_eur) AS total_gmv
  FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o
  left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on o.global_entity_id = v.global_entity_id and o.vendor_code = v.vendor_code
  left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on o.global_entity_id = c.global_entity_id
  WHERE is_valid_order
    AND date_trunc(created_date_local, month) >= date_add(date_trunc(current_date, month),INTERVAL -4 month)
    AND v.vertical_type = "restaurants"
    AND v.is_private = FALSE
    AND is_corporate_order = FALSE
    AND is_test_order is FALSE  
    
  GROUP BY 1,2,3,4,5
  order by 1,5 asc)
  


SELECT global_entity_id,country, month,month_no,year_month, cast(total_gmv as float64) as total_gmv
from gmv
