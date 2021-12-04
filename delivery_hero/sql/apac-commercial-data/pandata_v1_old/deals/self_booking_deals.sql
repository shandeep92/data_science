WITH dates AS (
  SELECT
    vendors.*,
    date,
    iso_year_week_string,
    weekday_name
  FROM pandata.dim_dates
  CROSS JOIN(
    SELECT 
      DISTINCT rdbms_id, 
      id AS vendor_id 
    FROM pandata.dim_vendors
  ) AS vendors
  WHERE date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
    AND date <= CURRENT_DATE
),

vendor_status_by_day as (
  SELECT *
  FROM (
    SELECT
      dates.*,
      fct_vendor_events.* EXCEPT (rdbms_id, vendor_id),
      row_number() OVER (PARTITION BY dates.rdbms_id, dates.vendor_id, dates.date ORDER BY fct_vendor_events.created_at_utc DESC) = 1 AS is_latest_entry
    FROM dates
    LEFT JOIN pandata.fct_vendor_events
           ON fct_vendor_events.rdbms_id = dates.rdbms_id
          AND fct_vendor_events.vendor_id = dates.vendor_id
          AND fct_vendor_events.created_date_utc <= dates.date
    WHERE fct_vendor_events.vendor_id IS NOT NULL
  )
  WHERE is_latest_entry
),

active_vendors_daily AS (
  SELECT
    vendor_status_by_day.date AS day,
    dim_vendors.business_type,
    vendor_status_by_day.rdbms_id,
    dim_vendors.country_name,
    vendor_status_by_day.vendor_id,
    dim_vendors.vendor_code
  FROM vendor_status_by_day
  LEFT JOIN pandata.dim_vendors
         ON dim_vendors.rdbms_id = vendor_status_by_day.rdbms_id
        AND dim_vendors.id = vendor_status_by_day.vendor_id
  WHERE TRUE
    AND vendor_status_by_day.is_active
    AND NOT vendor_status_by_day.is_deleted
    AND NOT vendor_status_by_day.is_private
    AND NOT vendor_status_by_day.is_vendor_testing
  GROUP BY 1,2,3,4,5,6
),

active_vendors_monthly AS (
  select
    DATE_TRUNC(vendor_status_by_day.date, MONTH) As month,
    dim_vendors.business_type,
    vendor_status_by_day.rdbms_id,
    COUNT(DISTINCT vendor_status_by_day.vendor_id) as active_vendors
  FROM vendor_status_by_day
  LEFT JOIN pandata.dim_vendors
         ON dim_vendors.rdbms_id = vendor_status_by_day.rdbms_id
        AND dim_vendors.id = vendor_status_by_day.vendor_id
  WHERE TRUE
    AND vendor_status_by_day.is_active
    AND NOT vendor_status_by_day.is_deleted
    AND NOT vendor_status_by_day.is_private
    AND NOT vendor_status_by_day.is_vendor_testing
  GROUP BY 1,2,3
),

deals_data AS (
  SELECT 
    dd.*,
    c.rdbms_id,
    SUBSTR(dd.global_entity_id, 4, 2) AS country_code,
    gmvc.gmv_class AS gmv_class
  FROM `fulfillment-dwh-production.pandata_curated.vgr_deals` dd
  LEFT JOIN pandata.dim_countries c 
         ON dd.global_entity_id = c.entity_id
  LEFT JOIN pandata_report.vendor_gmv_class gmvc 
         ON c.rdbms_id = gmvc.rdbms_id
        AND dd.vendor_code = gmvc.vendor_code
  WHERE created_date_utc <= CURRENT_DATE
  ORDER BY created_at_utc DESC
),

deal_vendors AS (
  SELECT 
    rdbms_id,
    country_code,
    vendor_code
  FROM deals_data
  GROUP BY 1,2,3
),

self_booking AS (
  SELECT 
    *,
    CASE
      WHEN created_by NOT LIKE "%v-panda_pro_deals%" 
      THEN "Normal Deals"
      WHEN created_by LIKE "%v-panda_pro_deals%"
      THEN "Pro Deals"
    END AS deal_type,
    TRUE AS is_self_booking_deal
  FROM deals_data
  WHERE NOT primary_key IN ('x0kv_FP_SG', 'y5wq_FP_SG', 's3lp_FP_SG', 's0gq_FP_SG', 'y1jw_FP_SG', 'x8ms_FP_SG', 'v7jj_FP_LA', 't8hi_FP_KH', 's1dz_FP_HK', 'p8fr_FP_PH', 't3ce_FP_MM', 't8lo_FP_TH', 's4zw_FP_BD', 't6gb_FP_PK', 'x7ac_FP_TW', 't3fp_FP_TH', 'p8da_FP_PH', 't7ga_FP_BD', 's0ep_FP_HK', 'w4lf_FP_TW', 'u2td_FP_PK','w9cz_FP_JP', 'x6rs_FP_JP', 't2hd_FP_JP', 'y1jw_FP_SG_panda_pro', 's0gq_FP_SG_panda_pro', 's3lp_FP_SG_panda_pro', 'm2yq_FP_MY_panda_pro','y5wq_FP_SG_panda_pro', 'p8fr_FP_PH_panda_pro', 'p9hj_FP_PH_panda_pro', 'm0fy_FP_MY_panda_pro', 'x0kv_FP_SG_panda_pro')
),

deals_config AS (
  SELECT
    active_vendors_daily.rdbms_id,
    active_vendors_daily.country_name AS country,
    active_vendors_daily.vendor_id,
    active_vendors_daily.business_type,
    active_vendors_daily.day,
    DATE_TRUNC(active_vendors_daily.day, MONTH) As month,
    d.discount_type,
    self_booking.deal_type AS portal_deal_type,
    d.amount_local,
    d.minimum_order_value_local,
    d.foodpanda_ratio,
    self_booking.title.default AS deal_title,
    vd.description,
    vd.condition_type,
    CASE
      WHEN d.expedition_type LIKE '%delivery%' AND d.expedition_type LIKE '%pickup%'
      THEN 'all'
      ELSE d.expedition_type
    END AS expedition_type,
    d.start_date_local AS deal_start,
    d.end_date_local AS deal_end,
    vd.id AS unique_vendor_discount_id
  FROM active_vendors_daily
  LEFT JOIN self_booking
         ON active_vendors_daily.rdbms_id = self_booking.rdbms_id
        AND active_vendors_daily.vendor_code = self_booking.vendor_code 
  LEFT JOIN pandata.dim_discounts d
         ON d.rdbms_id = active_vendors_daily.rdbms_id
        AND d.id = CAST(self_booking.id AS INT64)
        AND active_vendors_daily.day >= d.start_date_local
        AND active_vendors_daily.day <= d.end_date_local
  LEFT JOIN pandata.dim_vendor_discounts vd
         ON d.rdbms_id = vd.rdbms_id
        AND d.id = vd.discount_id
  WHERE /*(
          ( d.is_active
          OR vd.is_active)
        AND (NOT d.is_deleted
            AND NOT vd.is_deleted)
        )  
    AND */
        d.foodpanda_ratio < 100
    AND self_booking.is_self_booking_deal
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
  HAVING deal_start IS NOT NULL
),

country_orders AS (
  SELECT
    o.rdbms_id,
    DATE_TRUNC(o.date_local, MONTH) As month,
    v.business_type,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order
                     THEN o.id
                   END) AS total_country_valid_order,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
                     THEN o.id
                   END) AS total_country_vf_discount_order,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND o.is_voucher_used AND o.voucher_ratio < 100
                     THEN o.id
                   END) AS total_country_vf_voucher_order,
    SUM(CASE
          WHEN o.is_valid_order
          THEN o.gfv_eur
        END) AS total_country_gfv_eur,
    ROUND(SUM(CASE
                WHEN o.is_valid_order
                THEN o.gmv_eur
              END),2) AS total_country_gmv_eur,
    ROUND(SUM(CASE
                WHEN o.is_valid_order
                THEN o.gmv_eur - (IFNULL(o.discount_value_eur, 0) + IFNULL(o.voucher_value_eur, 0))
              END),2) AS total_country_gmv_after_subsidies_eur,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
          THEN o.discount_value_eur * SAFE_DIVIDE((100 - o.discount_ratio), 100)
        END) AS total_country_discount_value,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_voucher_used AND o.voucher_ratio < 100
          THEN o.voucher_value_eur * SAFE_DIVIDE((100 - o.voucher_ratio), 100)
        END) AS total_country_voucher_value,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
          THEN o.discount_value_eur * SAFE_DIVIDE((100 - o.discount_ratio), 100)
          WHEN o.is_valid_order AND o.is_voucher_used AND o.voucher_ratio < 100
          THEN o.voucher_value_eur * SAFE_DIVIDE((100 - o.voucher_ratio), 100)
        END) AS total_country_deal_value
  FROM pandata.fct_orders o
  LEFT JOIN pandata.dim_vendors v
         ON v.rdbms_id = o.rdbms_id
        AND v.id = o.vendor_id
  WHERE o.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
  GROUP BY 1,2,3
),

orders AS (
  SELECT
    o.rdbms_id,
    o.date_local As day,
    o.vendor_code,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order
                     THEN o.id
                   END) AS valid_order,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
                     THEN o.id
                   END) AS discount_order,            
    COUNT(DISTINCT CASE
                     WHEN o.is_failed_order_vendor AND o.is_discount_used AND o.discount_ratio < 100
                     THEN o.id
                   END) AS discount_vfail_order,           
    SUM(CASE
          WHEN o.is_valid_order
          THEN o.gfv_local
        END) AS gfv_local,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
          THEN o.discount_value_eur * SAFE_DIVIDE((100-o.discount_ratio), 100)
        END) AS vendor_discount_value,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
          THEN o.discount_value_eur * SAFE_DIVIDE((100 - o.discount_ratio), 100)
          WHEN o.is_valid_order AND o.is_voucher_used AND o.voucher_ratio < 100
          THEN o.voucher_value_eur * SAFE_DIVIDE((100 - o.voucher_ratio), 100)
        END) AS vendor_deal_value,
    ROUND(SUM(CASE
                WHEN o.is_valid_order
                THEN o.gmv_eur
              END),2) AS gmv
  FROM pandata.fct_orders o
  WHERE o.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
  GROUP BY 1,2,3
),

vendors as (
  SELECT
    v.id AS vendor_id,
    v.rdbms_id,
    v.vendor_code,
    v.vendor_name,
    v.business_type,
    v.city_name,
    COALESCE(sf.owner_name) AS account_owner,
    COALESCE(mc.main_cuisine, sf.primary_cuisine) AS cuisine,
    g.gmv_class
  FROM pandata.dim_vendors v
  LEFT JOIN pandata_report.vendor_gmv_class g
         ON g.rdbms_id = v.rdbms_id
        AND g.vendor_code = v.vendor_code
  LEFT JOIN pandata.sf_accounts sf
         ON sf.rdbms_id = v.rdbms_id
        AND sf.vendor_id = v.id
  LEFT JOIN (
      SELECT
        *
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY rdbms_id,vendor_id ORDER BY main_cuisine) AS row_number
        FROM (
          SELECT 
            rdbms_id,
            vendor_id,
            cuisine_title as main_cuisine
          FROM pandata.dim_vendor_cuisines
          WHERE is_main_cuisine
        )
      )
      WHERE row_number = 1
      ORDER BY 4 DESC
    ) mc ON v.rdbms_id = mc.rdbms_id
        AND v.id = mc.vendor_id
),

vendor_deal_data_daily AS (
  Select
    deals_config.*,
    vendors.* EXCEPT(rdbms_id, business_type, vendor_id),
    orders.* EXCEPT(rdbms_id, day, vendor_code),
    country_orders.* EXCEPT(rdbms_id, month, business_type),
    active_vendors_monthly.* EXCEPT(rdbms_id, month, business_type)
  FROM deals_config
  LEFT JOIN vendors
         ON deals_config.rdbms_id = vendors.rdbms_id
        AND deals_config.vendor_id = vendors.vendor_id
  LEFT JOIN orders
         ON deals_config.rdbms_id = orders.rdbms_id
        AND vendors.vendor_code = orders.vendor_code
        AND deals_config.day = orders.day
  LEFT JOIN country_orders
         ON country_orders.rdbms_id = deals_config.rdbms_id
        AND country_orders.month = DATE_TRUNC(deals_config.day, MONTH)
        AND country_orders.business_type = deals_config.business_type
  LEFT JOIN active_vendors_monthly
         ON active_vendors_monthly.rdbms_id = deals_config.rdbms_id
        AND active_vendors_monthly.month = DATE_TRUNC(deals_config.day, MONTH)
        AND active_vendors_monthly.business_type = deals_config.business_type
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
  HAVING rdbms_id IS NOT NULL
  ORDER BY rdbms_id,vendor_code
)

SELECT
  rdbms_id,
  country,
  city_name,
  cuisine,
  business_type,
  month,
  account_owner,
  vendor_id,
  vendor_code,
  vendor_name,
  gmv_class,
  discount_type,
  portal_deal_type,
  deal_title,
  description,
  condition_type,
  expedition_type,
  deal_start,
  deal_end,
  unique_vendor_discount_id,
  amount_local,
  minimum_order_value_local,
  foodpanda_ratio,
  total_country_valid_order,
  total_country_vf_discount_order,
  total_country_vf_voucher_order,
  total_country_gfv_eur,
  total_country_gmv_eur,
  total_country_gmv_after_subsidies_eur,
  total_country_discount_value,
  total_country_voucher_value,
  total_country_deal_value,
  active_vendors,
  SUM(valid_order) AS valid_order_monthly,
  SUM(discount_order) AS discount_order_monthly,
  SUM(discount_vfail_order) AS discount_vfail_order_monthly,
  SUM(gfv_local) AS gfv_local_monthly,
  SUM(vendor_discount_value) AS vendor_discount_value_monthly,
  SUM(vendor_deal_value) AS vendor_deal_value_monthly,
  SUM(gmv) AS gmv_monthly
FROM vendor_deal_data_daily
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
