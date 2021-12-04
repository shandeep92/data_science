SELECT
    o.global_entity_id,
    o.country_name,
    gmvc.gmv_class,
    case when sf.vendor_grade = 'AAA' then 'AAA' else 'non-AAA' end as vendor_grade,
    format_datetime('%b',datetime(o.created_date_local)) AS month,
    format_datetime('%Y%m',datetime(o.created_date_local)) AS yearmonth,
    SUM(o.gmv_eur) AS total_gmv_eur
  FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` o
  left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf on o.global_entity_id = sf.global_entity_id and o.vendor_code = sf.vendor_code
  left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on o.global_entity_id = v.global_entity_id and o.vendor_code = v.vendor_code
  left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on o.global_entity_id = gmvc.global_entity_id and o.vendor_code = gmvc.vendor_code
  WHERE is_valid_order is TRUE
    AND o.is_corporate_order is FALSE
    AND o.is_test_order is FALSE
    AND v.is_active = TRUE
    AND v.is_private = FALSE
    AND v.is_test = FALSE
    AND v.vertical_type = 'restaurants'
    AND date_trunc(o.created_date_local, month) >= date_add(date_trunc(current_date, month),INTERVAL -5 month)
    AND o.global_entity_id LIKE "%FP%"
    and o.global_entity_id NOT IN ("FP_DE","FP_RO","FP_BG")
  GROUP BY 1,2,3,4,5,6
  order by 1,6 asc
