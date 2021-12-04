with month_period AS (
  SELECT
    month
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 3 MONTH), DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 4 MONTH), INTERVAL 1 MONTH)) as month
  GROUP BY 1
),

business_type AS (
  SELECT
    DISTINCT business_type
  FROM pandata.dim_vendors
  GROUP BY 1
),

deals_config as (
  SELECT
  *
  FROM pandata_ap_commercial.configured_deals_combined_monthly
  WHERE month >= FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 3 MONTH))
),

country_orders AS (
  SELECT
    o.rdbms_id,
    FORMAT_DATE("%Y-%m (%B)",o.date_local) AS month,
    v.business_type,
    gfv_percentile_50,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order
                     THEN o.id
                   END) AS country_valid_order,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100
                     THEN o.id
                   END) AS vf_discount_country_order,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND o.is_voucher_used AND o.voucher_ratio < 100
                     THEN o.id
                   END) AS vf_voucher_country_order,
    COUNT(DISTINCT CASE
                     WHEN (o.is_valid_order AND o.is_voucher_used AND o.voucher_ratio < 100) OR (o.is_valid_order AND o.is_discount_used AND o.discount_ratio < 100)
                     THEN o.id
                   END) AS vf_deal_country_order,
    SUM(CASE
          WHEN o.is_valid_order
          THEN o.gfv_local
        END) AS gfv_local,
    ROUND(SUM(CASE
                WHEN o.is_valid_order
                THEN o.gmv_local
              END),2) AS gmv
  FROM pandata.fct_orders o
  LEFT JOIN pandata.dim_vendors v
         ON o.rdbms_id = v.rdbms_id
        AND o.vendor_id = v.id
  LEFT JOIN (
    WITH order_data AS (  
      SELECT
        fct_orders.rdbms_id,
        fct_orders.country_name,
        fct_orders.date_local,
        fct_orders.customer_id,
        fct_orders.expedition_type,
        dim_vendors.business_type,
        IFNULL(dim_vendors.vendor_type, 'restaurants') AS vendor_type,
        dim_vendors.is_vendor_in_shared_kitchen,
        FORMAT_DATE('%V-%Y', date_local) AS order_week_year,
        fct_orders.order_code_google,
        fct_orders.is_valid_order,
        fct_orders.gmv_eur,
        fct_orders.gmv_local,
        fct_orders.gfv_eur,
        fct_orders.gfv_local,
        fct_orders.voucher_value_eur,
        fct_orders.voucher_ratio,  
        fct_orders.discount_value_eur,
        fct_orders.discount_ratio,
        fct_orders.is_first_valid_order,
        fct_orders.is_discount_used,
        fct_orders.is_voucher_used,
        fct_orders.is_preorder,
        fct_orders.is_failed_order
      FROM pandata.fct_orders
      LEFT JOIN pandata.dim_vendors
             ON fct_orders.rdbms_id = dim_vendors.rdbms_id
            AND fct_orders.vendor_id = dim_vendors.id 
      WHERE fct_orders.date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 3 MONTH)
        AND fct_orders.is_valid_order
        AND fct_orders.created_date_local <= CURRENT_DATE
    )
  SELECT
    rdbms_id,
    business_type,
    FORMAT_DATE("%Y-%m (%B)",date_local) AS month,
    COUNT(order_code_google) AS total_orders,
    SUM(gmv_local)/COUNT(order_code_google) AS AOV,
    approx_quantiles(gfv_local,100)[OFFSET(50)] AS gfv_percentile_50
  FROM order_data
  GROUP BY 1,2,3
  ORDER BY 1,2,3
  ) median_local ON o.rdbms_id = median_local.rdbms_id
                AND FORMAT_DATE("%Y-%m (%B)",o.date_local) = median_local.month
                AND v.business_type = median_local.business_type
  WHERE o.date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 3 MONTH)
    AND o.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 4 MONTH)
  GROUP BY 1,2,3,4
),

orders as (
  SELECT
    o.rdbms_id,
    FORMAT_DATE("%Y-%m (%B)",o.date_local) AS month,
    o.vendor_code,
    COALESCE(o.expedition_type,'delivery') AS expedition_type,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order
                     THEN o.id
                   END) AS valid_order,
    COUNT(DISTINCT CASE
                     WHEN (o.is_valid_order AND o.is_discount_used) OR (o.is_valid_order AND o.is_voucher_used)
                     THEN o.id
                   END) AS valid_order_deals,               
    AVG(CASE
          WHEN o.is_valid_order
          THEN o.gfv_local
        END) AS gfv_avg,
    ROUND(AVG(CASE
                WHEN o.is_valid_order
                THEN o.gmv_local
              END),1) as gmv_avg,
    CASE 
      WHEN COUNT(DISTINCT CASE
                             WHEN o.is_valid_order AND va.foodpanda_ratio < 100 AND o.is_discount_used AND o.discount_ratio < 100
                             THEN va.order_id
                           END) >= 1
      THEN 'is_both_discount_&_voucher'
    END AS is_both_discount_and_voucher,
  FROM pandata.fct_orders o
  LEFT JOIN (
  SELECT * EXCEPT (is_latest_entry)
  FROM (
    SELECT
      rdbms_id,
      order_id,
      voucher_id,
      foodpanda_ratio,
      customer_code,
      created_at AS voucher_created_at,
      value,
      ROW_NUMBER() OVER (PARTITION BY rdbms_id, order_id ORDER BY date DESC) = 1 AS is_latest_entry
    FROM ml_backend_latest.voucherattributions
    WHERE order_id IS NOT NULL
    )
    WHERE is_latest_entry
  ) va ON o.rdbms_id = va.rdbms_id
      AND o.id = va.order_id
  WHERE o.date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 3 MONTH)
    AND o.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 4 MONTH)
  GROUP BY 1,2,3,4
),

vendors as (
  SELECT
    c.common_name,
    v.id,
    v.rdbms_id,
    v.vendor_code,
    v.vendor_name,
    v.business_type,
    v.customers_type,
    v.city_name,
    v.latitude,
    v.longitude,
    sf.owner_name AS account_owner,
    COALESCE(mc.main_cuisine,sf.primary_cuisine) AS cuisine,
    COALESCE(g.gmv_class,'NEW') AS gmv_class
  FROM pandata.dim_vendors v
  LEFT JOIN pandata_report.vendor_gmv_class g
         ON g.rdbms_id = v.rdbms_id
        AND g.vendor_code = v.vendor_code
  LEFT JOIN pandata.dim_countries c
         ON c.rdbms_id = v.rdbms_id
  LEFT JOIN pandata.sf_accounts sf
         ON sf.rdbms_id = v.rdbms_id
        AND sf.vendor_id = v.id
  LEFT JOIN (
      SELECT *
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
            ) mc
           ON v.rdbms_id = mc.rdbms_id
          AND v.id = mc.vendor_id
  WHERE NOT v.is_deleted
    AND NOT v.is_vendor_testing
    AND v.is_active
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
), 

country_vendors AS (
  SELECT
    rdbms_id,
    common_name,
    business_type,
    COALESCE(gmv_class,'NEW') AS gmv_class,
    COUNT(DISTINCT id) AS no_of_active_vendors
  FROM vendors
  GROUP BY 1,2,3,4
),

vendors_with_events_last_month AS (
  SELECT
    ve.rdbms_id,
    v.country_name AS common_name,
    v.business_type,
    COALESCE(gmv_class,'NEW') AS gmv_class,
    COUNT(DISTINCT ve.vendor_id) AS no_of_active_vendors_last_month
  FROM pandata.fct_vendor_events ve
  LEFT JOIN pandata.dim_vendors v
         ON ve.rdbms_id = v.rdbms_id
        AND ve.vendor_id = v.id
  LEFT JOIN pandata_report.vendor_gmv_class g
         ON g.rdbms_id = v.rdbms_id
        AND g.vendor_code = v.vendor_code
  WHERE DATE(created_date_utc) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND NOT v.is_deleted
    AND NOT v.is_vendor_testing
    AND v.is_active
  GROUP BY 1,2,3,4
),

deal_per_month_country AS (
SELECT
rdbms_id,
month,
business_type,
COUNT(DISTINCT vendor_id) AS no_of_deals
FROM deals_config
GROUP BY 1,2,3
),

deal_last_month AS (
SELECT
rdbms_id,
business_type,
COUNT(DISTINCT vendor_id) AS no_of_deals_last_month
FROM deals_config
WHERE month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
GROUP BY 1,2
),

all_data AS (
Select 
  deals_config.rdbms_id,
  deals_config.business_type,
  vendors.* EXCEPT(rdbms_id,business_type),
  deals_config.month,
  COALESCE(deals_config.deal_segment,is_both_discount_and_voucher) AS deal_segment,
  country_vendors.no_of_active_vendors,
  vendors_with_events_last_month.no_of_active_vendors_last_month,
  CASE
      WHEN deals_config.month = country_orders.month
      THEN country_orders.country_valid_order
      WHEN deals_config.month >= FORMAT_DATE("%Y-%m (%B)",DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AND country_orders.month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
      THEN (country_orders.country_valid_order)*SAFE_DIVIDE(country_vendors.no_of_active_vendors,vendors_with_events_last_month.no_of_active_vendors_last_month)
  END AS country_valid_order,
  CASE
      WHEN deals_config.month = country_orders.month
      THEN country_orders.gfv_percentile_50
      WHEN deals_config.month >= FORMAT_DATE("%Y-%m (%B)",DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AND country_orders.month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
      THEN country_orders.gfv_percentile_50
  END AS country_gfv_percentile_50,
  CASE
      WHEN deals_config.month = orders.month
      THEN orders.valid_order
      WHEN deals_config.month >= FORMAT_DATE("%Y-%m (%B)",DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AND orders.month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
      THEN orders.valid_order
  END AS valid_order,
  CASE
      WHEN deals_config.month = country_orders.month
      THEN country_orders.vf_deal_country_order
      WHEN deals_config.month >= FORMAT_DATE("%Y-%m (%B)",DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AND country_orders.month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
      THEN (country_orders.vf_deal_country_order)*SAFE_DIVIDE(no_of_deals,no_of_deals_last_month)
  END AS valid_order_deals,
  CASE
      WHEN deals_config.month = orders.month
      THEN orders.gfv_avg
      WHEN deals_config.month >= FORMAT_DATE("%Y-%m (%B)",DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AND orders.month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
      THEN orders.gfv_avg
  END AS gfv_avg,
  ROUND(SAFE_DIVIDE (deals_config.MOV, CASE
                                         WHEN deals_config.month = orders.month
                                         THEN orders.gfv_avg
                                         WHEN deals_config.month >= FORMAT_DATE("%Y-%m (%B)",DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AND orders.month = FORMAT_DATE("%Y-%m (%B)",DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH))
                                         THEN orders.gfv_avg
                                       END),2) as ratio_mov_avb,
  CASE
    WHEN deals_config.foodpanda_ratio < 100
    THEN 'Vendor Funded'
    ELSE 'foodpanda Funded'
  END AS funding_type,
  deals_config.MOV, 
  deals_config.amount_local, 
  deals_config.deal_type,
  CAST(deals_config.amount_local AS FLOAT64) AS amt_local,
  deals_config.condition_type,
  CASE
    WHEN deals_config.condition_type IN ('vendor','multiple_vendors','menu','chain')
    THEN 'Full Menu'
    WHEN deals_config.condition_type IN ('productvariation','product')
    THEN 'Product'
    WHEN deals_config.condition_type IN ('menucategory','multiple_menu_categories')
    THEN 'Category'
    ELSE 'Full Menu'
  END AS condition_segment,
  deals_config.foodpanda_ratio,
  deals_config.deal_title,
  deals_config.deal_start,
  deals_config.deal_end,
  date_diff(deals_config.deal_end,deals_config.deal_start, day) as duration,
  deals_config.expedition_type,
  CASE
    WHEN deals_config.foodpanda_ratio = 0
    THEN deals_config.amount_local
    ELSE deals_config.amount_local*((1 - deals_config.foodpanda_ratio)* -1/100)
  END AS vendor_discount_perc,
  CASE
    WHEN date_diff(deals_config.deal_end,deals_config.deal_start, day) > 31
    THEN '(C) More than 1 Month'
    WHEN date_diff(deals_config.deal_end,deals_config.deal_start, day) < 28
    THEN '(A) Less than 1 Month'
    WHEN date_diff(deals_config.deal_end,deals_config.deal_start, day) IN (28,29,30,31)
    THEN '(B) 1 Month'
  END AS deal_duration_type
FROM deals_config
LEFT JOIN vendors
       ON deals_config.rdbms_id = vendors.rdbms_id
      AND deals_config.vendor_id = vendors.id
LEFT JOIN orders
       ON deals_config.rdbms_id = orders.rdbms_id
      AND vendors.vendor_code = orders.vendor_code
      AND deals_config.expedition_type = orders.expedition_type
LEFT JOIN country_orders
       ON country_orders.rdbms_id = deals_config.rdbms_id
      AND deals_config.business_type = country_orders.business_type
LEFT JOIN deal_per_month_country
       ON deal_per_month_country.rdbms_id = deals_config.rdbms_id
      AND deal_per_month_country.business_type = deals_config.business_type
      AND deal_per_month_country.month = deals_config.month
LEFT JOIN deal_last_month
       ON deal_last_month.rdbms_id = deals_config.rdbms_id
      AND deals_config.business_type = deal_last_month.business_type
LEFT JOIN country_vendors
       ON country_vendors.rdbms_id = deals_config.rdbms_id
      AND country_vendors.gmv_class = vendors.gmv_class
      AND country_vendors.business_type = vendors.business_type
LEFT JOIN vendors_with_events_last_month
       ON vendors_with_events_last_month.rdbms_id = deals_config.rdbms_id
      AND vendors_with_events_last_month.gmv_class = vendors.gmv_class
      AND vendors_with_events_last_month.business_type = vendors.business_type
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
HAVING rdbms_id IS NOT NULL
ORDER BY rdbms_id,country_valid_order DESC
)

SELECT
*
FROM all_data
UNION ALL
SELECT
rdbms_id,
business_type.business_type AS business_type,
common_name,
NULL AS id ,
NULL AS vendor_code ,
NULL AS vendor_name ,
NULL AS customers_type ,
NULL AS city_name ,
NULL AS latitude ,
NULL AS longitude ,
NULL AS account_owner ,
NULL AS cuisine ,
NULL AS gmv_class ,
FORMAT_DATE("%Y-%m (%B)",month) AS month ,
'is_voucher_deal' AS deal_segment ,
NULL AS no_of_active_vendors ,
NULL AS no_of_active_vendors_last_month,
NULL AS country_valid_order ,
NULL AS country_gfv_percentile_50 ,
NULL AS valid_order ,
NULL AS valid_order_deals ,
NULL AS gfv_avg ,
NULL AS ratio_mov_avb ,
NULL AS funding_type ,
NULL AS MOV ,
NULL AS amount_local ,
'amount' AS deal_type ,
NULL AS amt_local ,
NULL AS condition_type ,
NULL AS condition_segment ,
NULL AS foodpanda_ratio ,
NULL AS deal_title ,
NULL AS deal_start ,
NULL AS deal_end ,
NULL AS duration ,
'all' AS expedition_type ,
NULL AS vendor_discount_perc ,
NULL AS deal_duration_type 
FROM pandata.dim_countries
CROSS JOIN business_type
CROSS JOIN month_period
WHERE rdbms_id IN (220,219)
