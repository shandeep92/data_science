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
  WHERE date >= '2020-04-01'
    AND date <= CURRENT_DATE
),

active_deal_by_day AS (
  SELECT
    dim_discounts.rdbms_id,
    dates.date,
    dates.iso_year_week_string,
    dates.weekday_name,
    vendors.business_type,
    dim_discounts.discount_type,
    dim_discounts.amount_local,
    dim_discounts.minimum_order_value_local,
    dim_discounts.foodpanda_ratio,
    dim_vendor_discounts.vendor_id,
    dim_discounts.id AS discount_id,
    dim_vendor_discounts.title AS deal_title,
    dim_vendor_discounts.description,
    dim_vendor_discounts.condition_type,
    CASE
      WHEN dim_discounts.expedition_type LIKE '%delivery%' AND dim_discounts.expedition_type LIKE '%pickup%'
      THEN 'all'
      ELSE dim_discounts.expedition_type
    END AS expedition_type,
    dim_discounts.start_date_local,
    dim_discounts.end_date_local,
    ROW_NUMBER() OVER (PARTITION BY dim_discounts.rdbms_id, dim_vendor_discounts.vendor_id, dates.date ORDER BY dim_vendor_discounts.created_at_utc DESC) = 1 AS is_latest_entry,
    TRUE AS is_deal_day
  FROM pandata.dim_discounts
  LEFT JOIN pandata.dim_vendor_discounts
         ON dim_discounts.rdbms_id = dim_vendor_discounts.rdbms_id
        AND dim_discounts.id = dim_vendor_discounts.discount_id
  LEFT JOIN pandata.dim_vendors AS vendors
         ON dim_discounts.rdbms_id = vendors.rdbms_id
        AND dim_vendor_discounts.vendor_id = vendors.id
  LEFT JOIN pandata.dim_dates AS dates
         ON dates.date BETWEEN dim_discounts.start_date_local AND dim_discounts.end_date_local
  WHERE dim_discounts.start_date_local IS NOT NULL
    AND ( (dim_discounts.rdbms_id != 12 AND (dim_discounts.is_active OR dim_vendor_discounts.is_active))
        OR (dim_discounts.rdbms_id = 12))
    AND NOT dim_discounts.is_deleted
    AND dates.date >= '2020-04-01'
    AND dates.date <= CURRENT_DATE
    AND dim_discounts.start_date_local <= dates.date
    AND dim_discounts.end_date_local >= dates.date
    AND vendors.business_type = 'restaurants'
    AND (dim_discounts.expedition_type != 'pickup' OR dim_discounts.expedition_type IS NULL)
    AND LOWER(dim_vendor_discounts.title) NOT LIKE 'pro %'
    AND LOWER(dim_vendor_discounts.title) NOT LIKE '%corporate%'
    AND LOWER(dim_vendor_discounts.title) NOT LIKE '%testing%'
    AND LOWER(dim_vendor_discounts.title) NOT LIKE '%pandapro%'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE '%corporate%'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE 'pro'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE '%pandapro%'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE '% pro %'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE '%pos uat%'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE '%test%'
    AND LOWER(dim_vendor_discounts.description) NOT LIKE '%pickup%'
  ORDER BY dim_discounts.rdbms_id, dim_discounts.id, dim_vendor_discounts.vendor_id, dates.date, is_latest_entry
),

vendor_status_by_day as (
  SELECT *
  FROM (
    SELECT
      dates.*,
      fct_vendor_events.* EXCEPT (rdbms_id, vendor_id),
      ROW_NUMBER() OVER (PARTITION BY dates.rdbms_id, dates.vendor_id, dates.date ORDER BY fct_vendor_events.created_at_utc DESC) = 1 AS is_latest_entry
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
    dim_cities.city_name,
    vendor_status_by_day.vendor_id,
    dim_vendors.vendor_code,
    dim_vendors.vendor_name,
    dim_vendors.chain_code,
    dim_vendors.chain_name,
    sf_accounts.owner_name AS am_owner_name
  FROM vendor_status_by_day
  LEFT JOIN pandata.dim_vendors
         ON dim_vendors.rdbms_id = vendor_status_by_day.rdbms_id
        AND dim_vendors.id = vendor_status_by_day.vendor_id
  LEFT JOIN pandata.dim_cities
         ON dim_vendors.rdbms_id = dim_cities.rdbms_id 
        AND dim_vendors.city_id = dim_cities.id 
  LEFT JOIN `dhh---analytics-apac.pandata.sf_accounts` sf_accounts
         ON sf_accounts.vendor_code = dim_vendors.vendor_code 
        AND sf_accounts.rdbms_id = dim_vendors.rdbms_id
  WHERE TRUE
    AND vendor_status_by_day.is_active
    AND NOT vendor_status_by_day.is_deleted
    AND NOT vendor_status_by_day.is_private
    AND NOT vendor_status_by_day.is_vendor_testing
    AND dim_vendors.business_type = 'restaurants'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

daily_vendor_traffic AS (
  SELECT
    v.rdbms_id,
    vl.date,
    vl.vendor_code,
    SUM(IFNULL(count_of_shop_menu_loaded, 0)) AS cr3_start,
    SUM(count_of_checkout_loaded) cr3_end,
    SUM(count_of_transaction) cr4_end
  FROM pandata_ap_product_external.vendor_level_session_metrics vl
  LEFT JOIN pandata.dim_vendors v
         ON v.country_name = vl.country
        AND v.vendor_code = vl.vendor_code
  WHERE vl.date >= '2020-04-01'
    AND vl.date <= CURRENT_DATE
    --AND v.rdbms_id IN (15, 19)
  GROUP BY 1,2,3
),

vendor_base_info AS (
SELECT 
    v.rdbms_id,
    v.country_name AS country,
    v.vendor_code,
    v.business_type,
    CASE 
      WHEN LOWER(sf_accounts_custom.vendor_grade) LIKE "%aaa%" 
      THEN 'AAA'
      ELSE 'Non-AAA'
    END AS aaa_type,
    CASE 
      WHEN vendor_gmv_class.gmv_class IS NULL 
      THEN NULL
      ELSE vendor_gmv_class.gmv_class
    END AS gmv_class,
    v.chain_code
  FROM `dhh---analytics-apac.pandata.dim_vendors` v 
  LEFT JOIN `dhh---analytics-apac.pandata.sf_accounts` sf_accounts_custom
         ON sf_accounts_custom.vendor_code = v.vendor_code 
        AND sf_accounts_custom.rdbms_id = v.rdbms_id
  LEFT JOIN `dhh---analytics-apac.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = v.vendor_code 
        AND vendor_gmv_class.rdbms_id = v.rdbms_id
  WHERE NOT v.is_vendor_testing
),

daily_order_details AS (
  SELECT
    fct_orders.rdbms_id,
    fct_orders.country_name AS country,
    fct_orders.vendor_id,
    fct_orders.vendor_code,
    fct_orders.date_local,
    COUNT(DISTINCT IF(fct_orders.is_gross_order, fct_orders.id, 0)) AS daily_gross_orders,
    COUNT(DISTINCT IF(fct_orders.is_valid_order, fct_orders.id, 0)) AS daily_valid_orders,
    COUNT(DISTINCT IF(fct_orders.is_valid_order AND fct_orders.is_discount_used, fct_orders.id, 0)) AS daily_deal_discount_valid_orders,
    COUNT(DISTINCT IF(fct_orders.is_valid_order AND fct_orders.is_own_delivery, fct_orders.id, 0)) AS daily_foodpanda_delivery_orders,
    SUM(IF(fct_orders.is_valid_order, order_commissions.commission_eur, 0)) AS daily_commission_revenue_eur,
    SUM(IF(fct_orders.is_valid_order, fct_orders.gmv_eur, 0)) AS daily_gmv_eur,
    SUM(IF(fct_orders.is_valid_order, fct_orders.gfv_eur, 0)) AS daily_gfv_eur,
    COALESCE(
      SUM(IF(fct_orders.is_valid_order, fct_orders.gfv_eur, 0)) -
      SUM(IF(fct_orders.is_valid_order AND fct_orders.is_discount_used AND fct_orders.discount_ratio < 100 AND discount_type != 'free-delivery', fct_orders.discount_value_eur*SAFE_DIVIDE((100-fct_orders.discount_ratio),100),0)) -
      SUM(IF(fct_orders.is_valid_order AND fct_orders.is_voucher_used AND fct_orders.voucher_ratio < 100 AND voucher_type != 'delivery_fee', fct_orders.voucher_value_eur*SAFE_DIVIDE((100-fct_orders.voucher_ratio),100),0)),
      0
    ) AS daily_discount_gfv_eur,
    SUM(IF(fct_orders.is_valid_order, order_commissions.commissionable_value_eur, fct_orders.commission_base_eur)) AS daily_commission_base_eur,
    SUM(IF(fct_orders.is_valid_order AND fct_orders.is_discount_used, fct_orders.discount_value_eur, 0)) AS daily_discount_value_eur,
    COUNT(DISTINCT IF(fct_orders.is_valid_order AND fct_orders.is_first_valid_order_with_this_vendor, fct_orders.customer_id, 0)) AS daily_first_valid_order_with_this_vendor,
    COUNT(DISTINCT IF(fct_orders.is_valid_order AND fct_orders.is_first_valid_order, fct_orders.customer_id, 0)) AS daily_first_valid_order_with_foodpanda,
    SUM(IF(fct_orders.is_valid_order, fct_orders.service_fee_eur, 0)) AS daily_service_fee_eur,
    SUM(CASE
          WHEN fct_orders.is_valid_order AND fct_orders.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= fct_orders.delivery_fee_eur)
               AND (discount_type != 'free-delivery' OR discount_type IS NULL) AND (voucher_type != 'delivery_fee' OR voucher_type IS NULL)
          THEN fct_orders.delivery_fee_eur
          WHEN fct_orders.is_valid_order AND fct_orders.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) < fct_orders.delivery_fee_eur)
               AND (discount_type != 'free-delivery' OR discount_type IS NULL) AND (voucher_type != 'delivery_fee' OR voucher_type IS NULL)
          THEN SAFE_DIVIDE(total_value,fx)
          WHEN fct_orders.is_valid_order AND fct_orders.is_own_delivery AND (total_value = 0 OR total_value IS NULL)
          THEN 0
          ELSE 0
        END) AS daily_delivery_fee_eur,
    COALESCE(
      SUM(IF(fct_orders.is_valid_order AND fct_orders.is_discount_used, fct_orders.discount_value_eur,0)) 
      + SUM(IF(fct_orders.is_valid_order AND fct_orders.is_voucher_used, fct_orders.voucher_value_eur,0)),
      0
    ) AS daily_deal_value_eur,
    
    /*Local Currency*/
    SUM(IF(fct_orders.is_valid_order, fct_orders.gfv_local, 0)) AS daily_gfv_local,
    COALESCE(
      SUM(IF(fct_orders.is_valid_order, fct_orders.gfv_local, 0)) -
      SUM(
        IF(
          fct_orders.is_valid_order AND fct_orders.is_discount_used AND fct_orders.discount_ratio < 100 AND discount_type != 'free-delivery',
          fct_orders.discount_value_local*SAFE_DIVIDE((100-fct_orders.discount_ratio),
          100),
          0)) -
      SUM(
        IF(
          fct_orders.is_valid_order AND fct_orders.is_voucher_used AND fct_orders.voucher_ratio < 100 AND voucher_type != 'delivery_fee',
          fct_orders.voucher_value_local*SAFE_DIVIDE((100-fct_orders.voucher_ratio),
          100),
          0)),
      0
    ) AS daily_discount_gfv_local,
    SUM(IF(fct_orders.is_valid_order AND fct_orders.is_discount_used, fct_orders.gfv_local, 0)) AS daily_deal_orders_gfv_local,
    COALESCE(
      SUM(IF(fct_orders.is_valid_order AND fct_orders.is_discount_used, fct_orders.gfv_local, 0)) -
      SUM(
        IF(
          fct_orders.is_valid_order AND fct_orders.is_discount_used AND fct_orders.discount_ratio < 100 AND discount_type != 'free-delivery',
          fct_orders.discount_value_local*SAFE_DIVIDE((100-fct_orders.discount_ratio),
          100),
          0)) -
      SUM(
        IF(
          fct_orders.is_valid_order AND fct_orders.is_voucher_used AND fct_orders.voucher_ratio < 100 AND voucher_type != 'delivery_fee',
          fct_orders.voucher_value_local*SAFE_DIVIDE((100-fct_orders.voucher_ratio),
          100),
          0)),
      0
    ) AS daily_deal_orders_discount_gfv_local,
    
  FROM `dhh---analytics-apac.pandata.fct_orders` AS fct_orders
  LEFT JOIN `dhh---analytics-apac.pandata_report.order_commissions` AS order_commissions
         ON fct_orders.rdbms_id = order_commissions.rdbms_id
        AND fct_orders.code = order_commissions.order_code
  WHERE fct_orders.created_date_local >= '2020-04-01'
    AND fct_orders.created_date_local <= CURRENT_DATE
    AND NOT fct_orders.is_test_order
    AND fct_orders.is_gross_order
    AND fct_orders.expedition_type = 'delivery'
    AND NOT is_corporate
    AND NOT is_subscription_order
  GROUP BY 1,2,3,4,5
),

daily_vendor_sessions AS (
  SELECT
    COALESCE(vt.rdbms_id, od.rdbms_id) AS rdbms_id,
    COALESCE(od.date_local, vt.date) AS date,
    COALESCE(od.vendor_code, vt.vendor_code) AS vendor_code,
    GREATEST(vt.cr3_start, od.daily_gross_orders, 0) AS sessions,
    GREATEST(od.daily_gross_orders, 0) AS daily_gross_orders
  FROM daily_vendor_traffic vt
  FULL JOIN daily_order_details od
         ON vt.rdbms_id = od.rdbms_id
        AND od.date_local = vt.date
        AND od.vendor_code = vt.vendor_code
  GROUP BY 1,2,3, vt.cr3_start, od.daily_gross_orders
),

has_deal_days AS (
  SELECT
    active_vendors_daily.rdbms_id,
    active_vendors_daily.vendor_id,
    MIN(start_date_local) AS first_deal_start_date
  FROM active_deal_by_day
  LEFT JOIN active_vendors_daily
         ON active_vendors_daily.rdbms_id = active_deal_by_day.rdbms_id
        AND active_vendors_daily.vendor_id = active_deal_by_day.vendor_id
        AND active_vendors_daily.day = active_deal_by_day.date
  WHERE active_deal_by_day.is_latest_entry
    AND active_deal_by_day.start_date_local <= CURRENT_DATE
  GROUP BY 1, 2
  ORDER BY 1, 2
),

vendor_deals AS (
  SELECT
    active_deal_by_day.rdbms_id,
    active_deal_by_day.vendor_id,
    discount_id,
    condition_type,
    start_date_local,
    end_date_local,
    LEAD(start_date_local) OVER (PARTITION BY active_deal_by_day.rdbms_id, active_deal_by_day.vendor_id ORDER BY start_date_local) AS next_deal_start_day,
    LAG(end_date_local) OVER (PARTITION BY active_deal_by_day.rdbms_id, active_deal_by_day.vendor_id ORDER BY end_date_local) AS last_deal_end_day,
    LAG(start_date_local) OVER (PARTITION BY active_deal_by_day.rdbms_id, active_deal_by_day.vendor_id ORDER BY start_date_local) AS last_deal_start_day
  FROM active_deal_by_day
  LEFT JOIN active_vendors_daily
         ON active_vendors_daily.rdbms_id = active_deal_by_day.rdbms_id
        AND active_vendors_daily.vendor_id = active_deal_by_day.vendor_id
        AND active_vendors_daily.day = active_deal_by_day.date
  WHERE active_deal_by_day.is_latest_entry
    AND active_deal_by_day.start_date_local <= CURRENT_DATE
  GROUP BY 1, 2, 3, 4, 5, 6, active_vendors_daily.day
  HAVING active_vendors_daily.day IS NOT NULL
  ORDER BY 1, 2, start_date_local
),

country_order_data AS (  
  SELECT
    fct_orders.country_name,
    fct_orders.rdbms_id,
    fct_orders.date_local,
    fct_orders.expedition_type,
    dim_vendors.business_type,
    dim_vendors.is_vendor_in_shared_kitchen,
    fct_orders.order_code_google,
    fct_orders.gmv_eur,
    fct_orders.gmv_local,
    fct_orders.gfv_eur,
    fct_orders.gfv_local,
    fct_orders.voucher_value_eur,
    fct_orders.voucher_ratio,  
    fct_orders.discount_value_eur,
    fct_orders.discount_ratio,
    fct_orders.is_discount_used,
    fct_orders.is_voucher_used,
    fct_orders.is_preorder
  FROM pandata.fct_orders
  LEFT JOIN pandata.dim_vendors
         ON fct_orders.rdbms_id = dim_vendors.rdbms_id
        AND fct_orders.vendor_id = dim_vendors.id 
  WHERE DATE(date_local) >= '2020-04-01'
    AND NOT fct_orders.is_test_order
    AND fct_orders.is_gross_order
    AND fct_orders.is_valid_order
    AND fct_orders.expedition_type = 'delivery'
    AND NOT is_corporate
    AND NOT is_subscription_order
    AND fct_orders.date_local <= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    AND fct_orders.created_date_local >= '2020-04-01'
    AND dim_vendors.business_type = 'restaurants'
),

country_percentile AS (
  SELECT
    rdbms_id,
    business_type,
    COUNT(order_code_google) AS total_orders,
    SAFE_DIVIDE(SUM(gfv_local), COUNT(order_code_google)) AS country_afv,
    approx_quantiles(gfv_local,100)[OFFSET(10)] AS gfv_percentile_10,
    approx_quantiles(gfv_local,100)[OFFSET(15)] AS gfv_percentile_15,
    approx_quantiles(gfv_local,100)[OFFSET(20)] AS gfv_percentile_20,
    approx_quantiles(gfv_local,100)[OFFSET(25)] AS gfv_percentile_25,
    approx_quantiles(gfv_local,100)[OFFSET(30)] AS gfv_percentile_30,
    approx_quantiles(gfv_local,100)[OFFSET(35)] AS gfv_percentile_35,
    approx_quantiles(gfv_local,100)[OFFSET(40)] AS gfv_percentile_40,
    approx_quantiles(gfv_local,100)[OFFSET(50)] AS gfv_percentile_50,
    approx_quantiles(gfv_local,100)[OFFSET(60)] AS gfv_percentile_60,
    approx_quantiles(gfv_local,100)[OFFSET(70)] AS gfv_percentile_70,
    approx_quantiles(gfv_local,100)[OFFSET(75)] AS gfv_percentile_75,
    approx_quantiles(gfv_local,100)[OFFSET(80)] AS gfv_percentile_80,
    approx_quantiles(gfv_local,100)[OFFSET(90)] AS gfv_percentile_90

  FROM country_order_data
  WHERE DATE(date_local) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
    AND DATE(date_local) < DATE_TRUNC(CURRENT_DATE(), MONTH)
  GROUP BY 1,2
  ORDER BY 1,2
),

merged_data AS (
  SELECT
    active_vendors_daily.*,
    daily_order_details.* EXCEPT(rdbms_id, date_local, vendor_id, vendor_code, country, daily_gross_orders),
    daily_vendor_sessions.* EXCEPT(rdbms_id, date, vendor_code),
    active_deal_by_day.* EXCEPT(rdbms_id, iso_year_week_string, business_type, is_deal_day, vendor_id, date, weekday_name, is_latest_entry),
    vendor_base_info.aaa_type,
    vendor_base_info.gmv_class,
    COALESCE(active_deal_by_day.is_deal_day, FALSE) AS is_deal_day,
    cpo.cost_per_order_eur_delivery,
    cpo.cost_per_order_eur_cost_of_sales,
    
    country_percentile.gfv_percentile_10,
    country_percentile.gfv_percentile_25,
    country_percentile.gfv_percentile_50,
    country_percentile.gfv_percentile_75,
    country_percentile.country_afv,

    active_vendors_daily.rdbms_id||'-'||active_vendors_daily.vendor_code||'-'||CASE WHEN active_deal_by_day.discount_id IS NOT NULL THEN active_deal_by_day.discount_id||'-during-deal' ELSE COALESCE(next_deal_start_day, first_deal_start_date)||'-pre-deal' END AS uuid,
    active_vendors_daily.rdbms_id||'-'||active_vendors_daily.vendor_code||'-'||COALESCE(next_deal_start_day, first_deal_start_date) AS id_deal_comparison,
    DATE_DIFF(active_vendors_daily.day,COALESCE(next_deal_start_day, first_deal_start_date),DAY) AS distance_from_deal_start,
    last_deal_start_day

  FROM active_vendors_daily
  LEFT JOIN active_deal_by_day
         ON active_vendors_daily.rdbms_id = active_deal_by_day.rdbms_id
        AND active_vendors_daily.vendor_id = active_deal_by_day.vendor_id
        AND active_vendors_daily.day = active_deal_by_day.date
        AND active_deal_by_day.is_latest_entry
  LEFT JOIN vendor_deals
         ON active_vendors_daily.rdbms_id = vendor_deals.rdbms_id
        AND active_vendors_daily.vendor_id = vendor_deals.vendor_id
        AND active_vendors_daily.day <= vendor_deals.end_date_local
        AND active_vendors_daily.day > last_deal_end_day
  LEFT JOIN daily_order_details
         ON active_vendors_daily.rdbms_id = daily_order_details.rdbms_id
        AND active_vendors_daily.vendor_id = daily_order_details.vendor_id
        AND active_vendors_daily.day = daily_order_details.date_local
  LEFT JOIN daily_vendor_sessions
         ON active_vendors_daily.rdbms_id = daily_vendor_sessions.rdbms_id
        AND active_vendors_daily.vendor_code = daily_vendor_sessions.vendor_code
        AND active_vendors_daily.day = daily_vendor_sessions.date
  LEFT JOIN has_deal_days
         ON active_vendors_daily.rdbms_id = has_deal_days.rdbms_id
        AND active_vendors_daily.vendor_id = has_deal_days.vendor_id
  LEFT JOIN pandata_ap_commercial.apac_cost_per_order_per_month cpo
         ON cpo.month = DATE_TRUNC(daily_order_details.date_local, MONTH)
        AND cpo.rdbms_id = active_vendors_daily.rdbms_id
  LEFT JOIN country_percentile
         ON active_vendors_daily.rdbms_id = country_percentile.rdbms_id
        AND active_vendors_daily.business_type = country_percentile.business_type
  LEFT JOIN vendor_base_info
         ON active_vendors_daily.rdbms_id = vendor_base_info.rdbms_id
        AND active_vendors_daily.vendor_code = vendor_base_info.vendor_code
  --WHERE active_vendors_daily.rdbms_id = 15
  --  AND active_vendors_daily.vendor_id = 17769
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, has_deal_days.first_deal_start_date, next_deal_start_day, last_deal_start_day
  HAVING has_deal_days.first_deal_start_date IS NOT NULL
  ORDER BY active_vendors_daily.rdbms_id, active_vendors_daily.vendor_id, active_vendors_daily.day
),

combined_aggregation AS (
  SELECT
    business_type,
    rdbms_id,
    country_name,
    city_name,
    vendor_code,
    vendor_id,
    vendor_name,
    chain_code,
    chain_name,
    am_owner_name,
    aaa_type,
    gmv_class,
    discount_type,
    amount_local,
    minimum_order_value_local,
    foodpanda_ratio,
    discount_id,
    deal_title,
    description,
    condition_type,
    expedition_type,
    start_date_local,
    end_date_local,
    is_deal_day AS is_deal_grouping,
    uuid,
    last_deal_start_day,
    id_deal_comparison,
    CASE
      WHEN discount_type = 'amount'
      THEN SAFE_DIVIDE(
            amount_local,
            CASE
              WHEN minimum_order_value_local IS NOT NULL AND minimum_order_value_local > 0
              THEN minimum_order_value_local
              ELSE SAFE_DIVIDE(SUM(IFNULL(daily_gfv_local,0)), SUM(IFNULL(daily_valid_orders,0)))
            END
           )*100
      ELSE NULL
    END AS effective_amount_discount,
    
    COUNT(DISTINCT day) AS no_of_days_in_period,
    MIN(day) AS period_start,
    AVG(daily_valid_orders) AS avg_daily_valid_orders,
    AVG(daily_foodpanda_delivery_orders) AS avg_daily_foodpanda_delivery_orders,
    AVG(daily_commission_revenue_eur) AS avg_daily_commission_revenue_eur,
    AVG(daily_gmv_eur) AS avg_daily_gmv_eur,
    AVG(daily_gfv_eur) AS avg_daily_gfv_eur,
    AVG(daily_commission_base_eur) AS avg_daily_commission_base_eur,
    AVG(daily_discount_value_eur) AS avg_daily_discount_value_eur,
    AVG(daily_first_valid_order_with_this_vendor) AS avg_daily_first_valid_order_with_this_vendor,
    AVG(daily_first_valid_order_with_foodpanda) AS avg_daily_first_valid_order_with_foodpanda,
    AVG(daily_service_fee_eur) AS avg_daily_service_fee_eur,
    AVG(daily_delivery_fee_eur) AS avg_daily_delivery_fee_eur,
    AVG(daily_discount_gfv_eur) AS avg_daily_discount_gfv_eur,
    AVG(daily_deal_value_eur) AS avg_daily_deal_value_eur,
    AVG(daily_gfv_local) AS avg_daily_gfv_local,
    AVG(daily_discount_gfv_local) AS avg_daily_discount_gfv_local,
    AVG(daily_deal_orders_gfv_local) AS avg_deal_orders_gfv_local,
    AVG(daily_deal_orders_discount_gfv_local) AS avg_deal_orders_discount_gfv_local,
    AVG(sessions) AS avg_daily_sessions,
    AVG(daily_gross_orders) AS avg_daily_gross_orders,
    AVG(cost_per_order_eur_delivery) AS avg_cost_per_order_eur_delivery,
    AVG(cost_per_order_eur_cost_of_sales) AS avg_cost_per_order_eur_cost_of_sales,

    SAFE_DIVIDE(SUM(daily_gross_orders), SUM(sessions)) AS conversion_rate,

    SUM(daily_valid_orders) AS total_valid_orders,
    SUM(daily_foodpanda_delivery_orders) AS total_foodpanda_delivery_orders,
    SUM(daily_commission_revenue_eur) AS total_commission_revenue_eur,
    SUM(daily_gmv_eur) AS total_gmv_eur,
    SUM(daily_gfv_eur) AS total_gfv_eur,
    SUM(daily_commission_base_eur) AS total_commission_base_eur,
    SUM(daily_discount_value_eur) AS total_discount_value_eur,
    SUM(daily_first_valid_order_with_this_vendor) AS total_first_valid_order_with_this_vendor,
    SUM(daily_first_valid_order_with_foodpanda) AS total_first_valid_order_with_foodpanda,
    SUM(daily_service_fee_eur) AS total_service_fee_eur,
    SUM(daily_delivery_fee_eur) AS total_delivery_fee_eur,
    SUM(daily_discount_gfv_eur) AS total_discount_gfv_eur,
    SUM(daily_deal_value_eur) AS total_deal_value_eur,
    SUM(daily_gfv_local) AS total_gfv_local,
    SUM(daily_discount_gfv_local) AS total_discount_gfv_local,
    SUM(daily_deal_orders_gfv_local) AS total_deal_orders_gfv_local,
    SUM(daily_deal_orders_discount_gfv_local) AS total_deal_orders_discount_gfv_local,
    SUM(sessions) AS total_daily_sessions,
    SUM(daily_gross_orders) AS total_daily_gross_orders,
    SUM(daily_deal_discount_valid_orders) AS total_daily_deal_discount_valid_orders,
    SAFE_DIVIDE(
      SUM(daily_commission_revenue_eur -
        COALESCE(SAFE_MULTIPLY(daily_foodpanda_delivery_orders,cost_per_order_eur_delivery),0)
        - COALESCE(SAFE_MULTIPLY(daily_valid_orders,cost_per_order_eur_cost_of_sales),0)
        + daily_delivery_fee_eur
        + daily_service_fee_eur),
      SUM(daily_valid_orders)
    ) AS profit_per_order,
    SAFE_DIVIDE(
      SUM(daily_deal_discount_valid_orders),
      SUM(daily_valid_orders)
    ) AS deal_utilisation,
    SAFE_DIVIDE(
      SUM(daily_gfv_local),
      SUM(daily_valid_orders)
    ) AS afv_vendor_period,

    MAX(gfv_percentile_10) AS country_gfv_local_percentile_10,
    MAX(gfv_percentile_25) AS country_gfv_local_percentile_25,
    MAX(gfv_percentile_50) AS country_gfv_local_percentile_50,
    MAX(gfv_percentile_75) AS country_gfv_local_percentile_75,
    MAX(country_afv) AS country_gfv_local_afv
  FROM merged_data
  WHERE distance_from_deal_start >= -31
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
  ORDER BY id_deal_comparison
),

regrouping AS (
SELECT
rdbms_id||'-'||vendor_code||'-'||CASE WHEN is_deal_grouping THEN last_non_deal_day||'-during-deal' ELSE 
CASE WHEN NOT is_deal_grouping THEN period_start ELSE last_non_deal_day END||'-pre-deal' END AS uuid_all,
rdbms_id||'-'||vendor_code||'-'||CASE WHEN is_deal_grouping THEN last_non_deal_day ELSE 
CASE WHEN NOT is_deal_grouping THEN period_start ELSE last_non_deal_day END END AS id_deal_comparison_all,
*
FROM (
SELECT
last_value(IF(is_deal_grouping is FALSE, period_start, NULL) ignore nulls) 
    over (partition by rdbms_id,business_type, vendor_code order by period_start rows between unbounded preceding and 1 preceding) AS last_non_deal_day,
*
FROM combined_aggregation
ORDER BY period_start
)
)

SELECT
CASE
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 0 AND (deal_data_combined.amount_local*100) < 5)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 0 AND deal_data_combined.effective_amount_discount < 5)
  THEN '0% <= x < 5%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 5 AND (deal_data_combined.amount_local*100) < 10)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 5 AND deal_data_combined.effective_amount_discount < 10)
  THEN '5% <= x < 10%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 10 AND (deal_data_combined.amount_local*100) < 15)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 10 AND deal_data_combined.effective_amount_discount < 15)
  THEN '10% <= x < 15%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 15 AND (deal_data_combined.amount_local*100) < 20)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 15 AND deal_data_combined.effective_amount_discount < 20)
  THEN '15% <= x < 20%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 20 AND (deal_data_combined.amount_local*100) < 25)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 20 AND deal_data_combined.effective_amount_discount < 25)
  THEN '20% <= x < 25%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 25 AND (deal_data_combined.amount_local*100) < 30)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 25 AND deal_data_combined.effective_amount_discount < 30)
  THEN '25% <= x < 30%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 30 AND (deal_data_combined.amount_local*100) < 35)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 30 AND deal_data_combined.effective_amount_discount < 35)
  THEN '30% <= x < 35%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 35 AND (deal_data_combined.amount_local*100) < 40)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 35 AND deal_data_combined.effective_amount_discount < 40)
  THEN '35% <= x < 40%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 40 AND (deal_data_combined.amount_local*100) < 45)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 40 AND deal_data_combined.effective_amount_discount < 45)
  THEN '40% <= x < 45%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) > 45 AND (deal_data_combined.amount_local*100) < 50)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 45 AND deal_data_combined.effective_amount_discount < 50)
  THEN '45% <= x < 50%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 50 AND (deal_data_combined.amount_local*100) < 55)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 50 AND deal_data_combined.effective_amount_discount < 55)
  THEN '50% <= x < 55%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 55 AND (deal_data_combined.amount_local*100) < 60)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 55 AND deal_data_combined.effective_amount_discount < 60)
  THEN '55% <= x < 60%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 60 AND (deal_data_combined.amount_local*100) < 65)
       OR (deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 60 AND deal_data_combined.effective_amount_discount < 65)
  THEN '60% <= x < 65%'
  WHEN (deal_data_combined.discount_type = 'percentage' AND (deal_data_combined.amount_local*100) >= 65)
  THEN '>= 65%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 65 AND deal_data_combined.effective_amount_discount < 70
  THEN '65% <= x < 70%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 70 AND deal_data_combined.effective_amount_discount < 75
  THEN '70% <= x < 75%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 75 AND deal_data_combined.effective_amount_discount < 80
  THEN '75% <= x < 80%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 80 AND deal_data_combined.effective_amount_discount < 85
  THEN '80% <= x < 85%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 85 AND deal_data_combined.effective_amount_discount < 90
  THEN '85% <= x < 90%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 90 AND deal_data_combined.effective_amount_discount < 95
  THEN '90% <= x < 95%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount >= 95 AND deal_data_combined.effective_amount_discount <= 100
  THEN '95% <= x <= 100%'
  WHEN deal_data_combined.discount_type = 'amount' AND deal_data_combined.effective_amount_discount IS NULL
  THEN 'Incorrect Data Inputs'
  
  WHEN deal_data_combined.discount_type = 'free-delivery'
  THEN 'Free Delivery'
  
  WHEN deal_data_combined.discount_type = 'text_freegift'
  THEN 'Free Gift'
END AS deal,
CASE
  WHEN deal_data_combined.minimum_order_value_local IS NULL OR deal_data_combined.minimum_order_value_local = 0
  THEN 'No MOV'
  /*Bangladesh & Philippines*/
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=50
  THEN '<= 50'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>50 AND deal_data_combined.minimum_order_value_local<=100
  THEN '50 < x <= 100'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>100 AND deal_data_combined.minimum_order_value_local<=150
  THEN '100 < x <= 150'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>150 AND deal_data_combined.minimum_order_value_local<=200 
  THEN '150 < x <= 200'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>200 AND deal_data_combined.minimum_order_value_local<=250 
  THEN '200 < x <= 250'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>250 AND deal_data_combined.minimum_order_value_local<=300 
  THEN '250 < x <= 300'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>300 AND deal_data_combined.minimum_order_value_local<=350 
  THEN '300 < x <= 350'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>350 AND deal_data_combined.minimum_order_value_local<=450 
  THEN '350 < x <= 450'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND deal_data_combined.minimum_order_value_local>450
  THEN '> 450'
  /*Pakistan*/
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=50
  THEN '<= 50'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>50 AND deal_data_combined.minimum_order_value_local<=100
  THEN '50 < x <= 100'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>100 AND deal_data_combined.minimum_order_value_local<=150
  THEN '100 < x <= 150'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>150 AND deal_data_combined.minimum_order_value_local<=200
  THEN '150 < x <= 200'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>200 AND deal_data_combined.minimum_order_value_local<=250 
  THEN '200 < x <= 250'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>250 AND deal_data_combined.minimum_order_value_local<=350 
  THEN '250 < x <= 350'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>350 AND deal_data_combined.minimum_order_value_local<=450 
  THEN '350 < x <= 450'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>450 AND deal_data_combined.minimum_order_value_local<=550 
  THEN '450 < x <= 550'
  WHEN deal_data_combined.rdbms_id = 12 AND deal_data_combined.minimum_order_value_local>550
  THEN '> 550'
  /*Singapore*/
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=5
  THEN '<= 5'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>5 AND deal_data_combined.minimum_order_value_local<=8
  THEN '5 < x <= 8'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>8 AND deal_data_combined.minimum_order_value_local<=10
  THEN '8 < x <= 10'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>10 AND deal_data_combined.minimum_order_value_local<=12 
  THEN '10 < x <= 12'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>12 AND deal_data_combined.minimum_order_value_local<=14 
  THEN '12 < x <= 14'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>14 AND deal_data_combined.minimum_order_value_local<=16 
  THEN '14 < x <= 16'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>16 AND deal_data_combined.minimum_order_value_local<=18 
  THEN '16 < x <= 18'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>18 AND deal_data_combined.minimum_order_value_local<=22 
  THEN '18 < x <= 22'
  WHEN deal_data_combined.rdbms_id = 15 AND deal_data_combined.minimum_order_value_local>22
  THEN '> 22'
  /*Malaysia*/
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=5
  THEN '<= 5'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>5 AND deal_data_combined.minimum_order_value_local<=10
  THEN '5 < x <= 10'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>10 AND deal_data_combined.minimum_order_value_local<=12
  THEN '10 < x <= 12'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>12 AND deal_data_combined.minimum_order_value_local<=16 
  THEN '12 < x <= 16'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>16 AND deal_data_combined.minimum_order_value_local<=20 
  THEN '16 < x <= 20'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>20 AND deal_data_combined.minimum_order_value_local<=24 
  THEN '20 < x <= 24'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>24 AND deal_data_combined.minimum_order_value_local<=30 
  THEN '24 < x <= 30'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>30 AND deal_data_combined.minimum_order_value_local<=36 
  THEN '30 < x <= 36'
  WHEN deal_data_combined.rdbms_id = 16 AND deal_data_combined.minimum_order_value_local>36
  THEN '> 36'
  /*Thailand & Hong Kong*/
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=50
  THEN '<= 50'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>50 AND deal_data_combined.minimum_order_value_local<=100
  THEN '50 < x <= 100'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>100 AND deal_data_combined.minimum_order_value_local<=150
  THEN '100 < x <= 150'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>150 AND deal_data_combined.minimum_order_value_local<=200 
  THEN '150 < x <= 200'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>200 AND deal_data_combined.minimum_order_value_local<=250 
  THEN '200 < x <= 250'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>250 AND deal_data_combined.minimum_order_value_local<=300 
  THEN '250 < x <= 300'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>300 AND deal_data_combined.minimum_order_value_local<=350 
  THEN '300 < x <= 350'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>350 AND deal_data_combined.minimum_order_value_local<=400 
  THEN '350 < x <= 400'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND deal_data_combined.minimum_order_value_local>400
  THEN '> 400'
  /*Taiwan*/
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=100
  THEN '<= 100'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>100 AND deal_data_combined.minimum_order_value_local<=120
  THEN '100 < x <= 120'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>120 AND deal_data_combined.minimum_order_value_local<=140
  THEN '120 < x <= 140'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>140 AND deal_data_combined.minimum_order_value_local<=170 
  THEN '140 < x <= 170'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>170 AND deal_data_combined.minimum_order_value_local<=200 
  THEN '170 < x <= 200'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>200 AND deal_data_combined.minimum_order_value_local<=230 
  THEN '200 < x <= 230'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>230 AND deal_data_combined.minimum_order_value_local<=260 
  THEN '230 < x <= 260'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>260 AND deal_data_combined.minimum_order_value_local<=300 
  THEN '260 < x <= 300'
  WHEN deal_data_combined.rdbms_id = 18 AND deal_data_combined.minimum_order_value_local>300
  THEN '> 300'
  /*Laos*/
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=10000
  THEN '<= 10000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>10000 AND deal_data_combined.minimum_order_value_local<=15000
  THEN '10000 < x <= 15000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>15000 AND deal_data_combined.minimum_order_value_local<=17500
  THEN '15000 < x <= 17500'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>17500 AND deal_data_combined.minimum_order_value_local<=20000
  THEN '17500 < x <= 20000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>20000 AND deal_data_combined.minimum_order_value_local<=25000 
  THEN '20000 < x <= 25000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>25000 AND deal_data_combined.minimum_order_value_local<=30000
  THEN '25000 < x <= 30000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>30000 AND deal_data_combined.minimum_order_value_local<=40000
  THEN '30000 < x <= 40000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>40000 AND deal_data_combined.minimum_order_value_local<=50000 
  THEN '40000 < x <= 50000'
  WHEN deal_data_combined.rdbms_id = 219 AND deal_data_combined.minimum_order_value_local>50000
  THEN '> 50000'
  /*Cambodia*/
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=1
  THEN '<= 1'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>1 AND deal_data_combined.minimum_order_value_local<=1.5
  THEN '1 < x <= 1.5'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>1.5 AND deal_data_combined.minimum_order_value_local<=2
  THEN '1.5 < x <= 2'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>2 AND deal_data_combined.minimum_order_value_local<=2.5 
  THEN '2 < x <= 2.5'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>2.5 AND deal_data_combined.minimum_order_value_local<=3 
  THEN '2.5 < x <= 3'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>3 AND deal_data_combined.minimum_order_value_local<=4 
  THEN '3 < x <= 4'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>4 AND deal_data_combined.minimum_order_value_local<=5 
  THEN '4 < x <= 5'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>5 AND deal_data_combined.minimum_order_value_local<=7.5 
  THEN '5 < x <= 7.5'
  WHEN deal_data_combined.rdbms_id = 220 AND deal_data_combined.minimum_order_value_local>7.5
  THEN '> 7.5'
  /*Myanmar*/
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=2000
  THEN '<= 2000'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>2000 AND deal_data_combined.minimum_order_value_local<=3500
  THEN '2000 < x <= 3500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>3500 AND deal_data_combined.minimum_order_value_local<=4500
  THEN '3500 < x <= 4500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>4500 AND deal_data_combined.minimum_order_value_local<=5500 
  THEN '4500 < x <= 5500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>5500 AND deal_data_combined.minimum_order_value_local<=6500 
  THEN '5500 < x <= 6500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>6500 AND deal_data_combined.minimum_order_value_local<=7500 
  THEN '6500 < x <= 7500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>7500 AND deal_data_combined.minimum_order_value_local<=8500 
  THEN '7500 < x <= 8500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>8500 AND deal_data_combined.minimum_order_value_local<=9500 
  THEN '8500 < x <= 9500'
  WHEN deal_data_combined.rdbms_id = 221 AND deal_data_combined.minimum_order_value_local>9500
  THEN '> 9500'
  /*Japan*/
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>0 AND deal_data_combined.minimum_order_value_local<=500
  THEN '<= 500'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>500 AND deal_data_combined.minimum_order_value_local<=750
  THEN '500 < x <= 750'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>750 AND deal_data_combined.minimum_order_value_local<=1000
  THEN '750 < x <= 1000'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>1000 AND deal_data_combined.minimum_order_value_local<=1250 
  THEN '1000 < x <= 1250'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>1250 AND deal_data_combined.minimum_order_value_local<=1500 
  THEN '1250 < x <= 1500'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>1500 AND deal_data_combined.minimum_order_value_local<=1750 
  THEN '1500 < x <= 1750'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>1750 AND deal_data_combined.minimum_order_value_local<=2000 
  THEN '1750 < x <= 2000'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>2000 AND deal_data_combined.minimum_order_value_local<=2250 
  THEN '2000 < x <= 2250'
  WHEN deal_data_combined.rdbms_id = 263 AND deal_data_combined.minimum_order_value_local>2250
  THEN '> 2250'
END AS mov_grouped,
CASE
  WHEN pre_deal_data_combined.afv_vendor_period IS NULL OR pre_deal_data_combined.afv_vendor_period = 0
  THEN 'NULL AFV'
  /*Bangladesh & Philippines*/
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=50
  THEN '<=50'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>50 AND pre_deal_data_combined.afv_vendor_period<=100
  THEN '50<x<=100'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>100 AND pre_deal_data_combined.afv_vendor_period<=150
  THEN '100<x<=150'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>150 AND pre_deal_data_combined.afv_vendor_period<=200 
  THEN '150<x<=200'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>200 AND pre_deal_data_combined.afv_vendor_period<=250 
  THEN '200<x<=250'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>250 AND pre_deal_data_combined.afv_vendor_period<=300 
  THEN '250<x<=300'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>300 AND pre_deal_data_combined.afv_vendor_period<=350 
  THEN '300<x<=350'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>350 AND pre_deal_data_combined.afv_vendor_period<=450 
  THEN '350<x<=450'
  WHEN deal_data_combined.rdbms_id IN (7, 20) AND pre_deal_data_combined.afv_vendor_period>450
  THEN '>450'
  /*Pakistan*/
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=50
  THEN '<=50'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>50 AND pre_deal_data_combined.afv_vendor_period<=100
  THEN '50<x<=100'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>100 AND pre_deal_data_combined.afv_vendor_period<=150
  THEN '100<x<=150'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>150 AND pre_deal_data_combined.afv_vendor_period<=200
  THEN '150<x<=200'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>200 AND pre_deal_data_combined.afv_vendor_period<=250 
  THEN '200<x<=250'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>250 AND pre_deal_data_combined.afv_vendor_period<=350 
  THEN '250<x<=350'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>350 AND pre_deal_data_combined.afv_vendor_period<=450 
  THEN '350<x<=450'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>450 AND pre_deal_data_combined.afv_vendor_period<=550 
  THEN '450<x<=550'
  WHEN deal_data_combined.rdbms_id = 12 AND pre_deal_data_combined.afv_vendor_period>550
  THEN '>550'
  /*Singapore*/
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=5
  THEN '<=5'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>5 AND pre_deal_data_combined.afv_vendor_period<=8
  THEN '5<x<=8'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>8 AND pre_deal_data_combined.afv_vendor_period<=10
  THEN '8<x<=10'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>10 AND pre_deal_data_combined.afv_vendor_period<=12 
  THEN '10<x<=12'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>12 AND pre_deal_data_combined.afv_vendor_period<=14 
  THEN '12<x<=14'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>14 AND pre_deal_data_combined.afv_vendor_period<=16 
  THEN '14<x<=16'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>16 AND pre_deal_data_combined.afv_vendor_period<=18 
  THEN '16<x<=18'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>18 AND pre_deal_data_combined.afv_vendor_period<=22 
  THEN '18<x<=22'
  WHEN deal_data_combined.rdbms_id = 15 AND pre_deal_data_combined.afv_vendor_period>22
  THEN '>22'
  /*Malaysia*/
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=5
  THEN '<=5'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>5 AND pre_deal_data_combined.afv_vendor_period<=10
  THEN '5<x<=10'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>10 AND pre_deal_data_combined.afv_vendor_period<=12
  THEN '10<x<=12'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>12 AND pre_deal_data_combined.afv_vendor_period<=16 
  THEN '12<x<=16'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>16 AND pre_deal_data_combined.afv_vendor_period<=20 
  THEN '16<x<=20'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>20 AND pre_deal_data_combined.afv_vendor_period<=24 
  THEN '20<x<=24'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>24 AND pre_deal_data_combined.afv_vendor_period<=30 
  THEN '24<x<=30'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>30 AND pre_deal_data_combined.afv_vendor_period<=36 
  THEN '30<x<=36'
  WHEN deal_data_combined.rdbms_id = 16 AND pre_deal_data_combined.afv_vendor_period>36
  THEN '>36'
  /*Thailand & Hong Kong*/
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=50
  THEN '<=50'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>50 AND pre_deal_data_combined.afv_vendor_period<=100
  THEN '50<x<=100'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>100 AND pre_deal_data_combined.afv_vendor_period<=150
  THEN '100<x<=150'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>150 AND pre_deal_data_combined.afv_vendor_period<=200 
  THEN '150<x<=200'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>200 AND pre_deal_data_combined.afv_vendor_period<=250 
  THEN '200<x<=250'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>250 AND pre_deal_data_combined.afv_vendor_period<=300 
  THEN '250<x<=300'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>300 AND pre_deal_data_combined.afv_vendor_period<=350 
  THEN '300<x<=350'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>350 AND pre_deal_data_combined.afv_vendor_period<=400 
  THEN '350<x<=400'
  WHEN deal_data_combined.rdbms_id IN (17, 19) AND pre_deal_data_combined.afv_vendor_period>400
  THEN '>400'
  /*Taiwan*/
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=100
  THEN '<=100'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>100 AND pre_deal_data_combined.afv_vendor_period<=120
  THEN '100<x<=120'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>120 AND pre_deal_data_combined.afv_vendor_period<=140
  THEN '120<x<=140'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>140 AND pre_deal_data_combined.afv_vendor_period<=170 
  THEN '140<x<=170'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>170 AND pre_deal_data_combined.afv_vendor_period<=200 
  THEN '170<x<=200'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>200 AND pre_deal_data_combined.afv_vendor_period<=230 
  THEN '200<x<=230'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>230 AND pre_deal_data_combined.afv_vendor_period<=260 
  THEN '230<x<=260'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>260 AND pre_deal_data_combined.afv_vendor_period<=300 
  THEN '260<x<=300'
  WHEN deal_data_combined.rdbms_id = 18 AND pre_deal_data_combined.afv_vendor_period>300
  THEN '>300'
  /*Laos*/
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=10000
  THEN '<=10000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>10000 AND pre_deal_data_combined.afv_vendor_period<=15000
  THEN '10000<x<=15000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>15000 AND pre_deal_data_combined.afv_vendor_period<=17500
  THEN '15000<x<=17500'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>17500 AND pre_deal_data_combined.afv_vendor_period<=20000
  THEN '17500<x<=20000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>20000 AND pre_deal_data_combined.afv_vendor_period<=25000 
  THEN '20000<x<=25000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>25000 AND pre_deal_data_combined.afv_vendor_period<=30000
  THEN '25000<x<=30000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>30000 AND pre_deal_data_combined.afv_vendor_period<=40000
  THEN '30000<x<=40000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>40000 AND pre_deal_data_combined.afv_vendor_period<=50000 
  THEN '40000<x<=50000'
  WHEN deal_data_combined.rdbms_id = 219 AND pre_deal_data_combined.afv_vendor_period>50000
  THEN '>50000'
  /*Cambodia*/
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=1
  THEN '<=1'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>1 AND pre_deal_data_combined.afv_vendor_period<=1.5
  THEN '1<x<=1.5'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>1.5 AND pre_deal_data_combined.afv_vendor_period<=2
  THEN '1.5<x<=2'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>2 AND pre_deal_data_combined.afv_vendor_period<=2.5 
  THEN '2<x<=2.5'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>2.5 AND pre_deal_data_combined.afv_vendor_period<=3 
  THEN '2.5<x<=3'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>3 AND pre_deal_data_combined.afv_vendor_period<=4 
  THEN '3<x<=4'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>4 AND pre_deal_data_combined.afv_vendor_period<=5 
  THEN '4<x<=5'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>5 AND pre_deal_data_combined.afv_vendor_period<=7.5 
  THEN '5<x<=7.5'
  WHEN deal_data_combined.rdbms_id = 220 AND pre_deal_data_combined.afv_vendor_period>7.5
  THEN '>7.5'
  /*Myanmar*/
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=2000
  THEN '<=2000'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>2000 AND pre_deal_data_combined.afv_vendor_period<=3500
  THEN '2000<x<=3500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>3500 AND pre_deal_data_combined.afv_vendor_period<=4500
  THEN '3500<x<=4500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>4500 AND pre_deal_data_combined.afv_vendor_period<=5500 
  THEN '4500<x<=5500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>5500 AND pre_deal_data_combined.afv_vendor_period<=6500 
  THEN '5500<x<=6500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>6500 AND pre_deal_data_combined.afv_vendor_period<=7500 
  THEN '6500<x<=7500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>7500 AND pre_deal_data_combined.afv_vendor_period<=8500 
  THEN '7500<x<=8500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>8500 AND pre_deal_data_combined.afv_vendor_period<=9500 
  THEN '8500<x<=9500'
  WHEN deal_data_combined.rdbms_id = 221 AND pre_deal_data_combined.afv_vendor_period>9500
  THEN '>9500'
  /*Japan*/
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>0 AND pre_deal_data_combined.afv_vendor_period<=500
  THEN '<=500'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>500 AND pre_deal_data_combined.afv_vendor_period<=750
  THEN '500<x<=750'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>750 AND pre_deal_data_combined.afv_vendor_period<=1000
  THEN '750<x<=1000'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>1000 AND pre_deal_data_combined.afv_vendor_period<=1250 
  THEN '1000<x<=1250'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>1250 AND pre_deal_data_combined.afv_vendor_period<=1500 
  THEN '1250<x<=1500'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>1500 AND pre_deal_data_combined.afv_vendor_period<=1750 
  THEN '1500<x<=1750'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>1750 AND pre_deal_data_combined.afv_vendor_period<=2000 
  THEN '1750<x<=2000'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>2000 AND pre_deal_data_combined.afv_vendor_period<=2250 
  THEN '2000<x<=2250'
  WHEN deal_data_combined.rdbms_id = 263 AND pre_deal_data_combined.afv_vendor_period>2250
  THEN '>2250'
END AS vendor_afv_grouped,

CASE
  WHEN pre_deal_data_combined.afv_vendor_period <= deal_data_combined.country_gfv_local_percentile_10
  THEN 'Less Than Or Equal to 10th Percentile (AFV)'
  WHEN pre_deal_data_combined.afv_vendor_period > deal_data_combined.country_gfv_local_percentile_10 AND pre_deal_data_combined.afv_vendor_period <= deal_data_combined.country_gfv_local_percentile_25
  THEN '10th to 25th Percentile (AFV)'
  WHEN pre_deal_data_combined.afv_vendor_period > deal_data_combined.country_gfv_local_percentile_25 AND pre_deal_data_combined.afv_vendor_period <= deal_data_combined.country_gfv_local_percentile_50
  THEN '25th to 50th Percentile (AFV)'
  WHEN pre_deal_data_combined.afv_vendor_period > deal_data_combined.country_gfv_local_percentile_50 AND pre_deal_data_combined.afv_vendor_period <= deal_data_combined.country_gfv_local_percentile_75
  THEN '50th to 75th Percentile (AFV)'
  WHEN pre_deal_data_combined.afv_vendor_period > deal_data_combined.country_gfv_local_percentile_75
  THEN 'Greater Than 75th Percentile (AFV)'
END AS afv_segment_filter,

deal_data_combined.* EXCEPT(uuid_all, last_deal_start_day),
pre_deal_data_combined.avg_daily_gfv_local AS avg_daily_gfv_local_pre_deal,
pre_deal_data_combined.avg_daily_discount_gfv_local AS avg_daily_discount_gfv_local_pre_deal,
pre_deal_data_combined.afv_vendor_period AS afv_vendor_period_pre_deal,

ROW_NUMBER() OVER ( PARTITION BY deal_data_combined.rdbms_id,deal_data_combined.business_type, deal_data_combined.vendor_code, deal_data_combined.id_deal_comparison_all ORDER BY deal_data_combined.period_start ) AS consecutive_deal_no,

/*Deal Value Over GMV*/
SAFE_DIVIDE(deal_data_combined.total_deal_value_eur, deal_data_combined.total_gmv_eur) AS deal_value_over_gmv_per_deal,

/*Order Uplift*/
SAFE_DIVIDE(
(pre_deal_data_combined.avg_daily_sessions*deal_data_combined.conversion_rate)
- pre_deal_data_combined.avg_daily_valid_orders,
pre_deal_data_combined.avg_daily_valid_orders) AS avg_order_uplift,

SAFE_DIVIDE(deal_data_combined.avg_daily_valid_orders - pre_deal_data_combined.avg_daily_valid_orders,
pre_deal_data_combined.avg_daily_valid_orders) AS not_normalised_avg_order_uplift,

/*Conversion Rate Uplift*/
SAFE_DIVIDE(deal_data_combined.conversion_rate - pre_deal_data_combined.conversion_rate,
pre_deal_data_combined.conversion_rate) AS avg_conversion_rate_uplift,

/*GFV Uplift*/
SAFE_DIVIDE(deal_data_combined.avg_daily_gfv_local - pre_deal_data_combined.avg_daily_gfv_local,
IF(pre_deal_data_combined.avg_daily_gfv_local = 0 OR pre_deal_data_combined.avg_daily_gfv_local IS NULL, 1, pre_deal_data_combined.avg_daily_gfv_local)
) AS avg_gfv_uplift,

/*Discounted GFV Uplift*/
SAFE_DIVIDE(deal_data_combined.avg_daily_discount_gfv_local - pre_deal_data_combined.avg_daily_discount_gfv_local,
IF(pre_deal_data_combined.avg_daily_discount_gfv_local = 0 OR pre_deal_data_combined.avg_daily_discount_gfv_local IS NULL, 1, pre_deal_data_combined.avg_daily_discount_gfv_local)
) AS avg_discounted_gfv_uplift,

/*Normalised - GFV Uplift*/
SAFE_DIVIDE(
  SAFE_MULTIPLY(
    SAFE_DIVIDE(deal_data_combined.avg_daily_gfv_local, deal_data_combined.avg_daily_valid_orders),
      (pre_deal_data_combined.avg_daily_sessions*deal_data_combined.conversion_rate))
- pre_deal_data_combined.avg_daily_gfv_local,
pre_deal_data_combined.avg_daily_gfv_local
) AS avg_gfv_normalised_uplift,

/*Normalised - Discounted GFV Uplift*/
SAFE_DIVIDE(
  SAFE_MULTIPLY(
    SAFE_DIVIDE(deal_data_combined.avg_daily_discount_gfv_local, deal_data_combined.avg_daily_valid_orders), (pre_deal_data_combined.avg_daily_sessions*deal_data_combined.conversion_rate))
- pre_deal_data_combined.avg_daily_discount_gfv_local,
  pre_deal_data_combined.avg_daily_discount_gfv_local
) AS avg_discounted_gfv_normalised_uplift,

/*AFV Uplift*/
SAFE_DIVIDE(
(SAFE_DIVIDE(deal_data_combined.avg_daily_gfv_local, deal_data_combined.avg_daily_valid_orders)
- SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders)),
IF(SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) = 0 OR SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) IS NULL, 1, SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders))
) AS avg_afv_uplift,

/*Discounted AFV Uplift*/
SAFE_DIVIDE(
(SAFE_DIVIDE(deal_data_combined.avg_daily_discount_gfv_local, deal_data_combined.avg_daily_valid_orders)
- SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders)),

IF(SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) = 0 OR SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) IS NULL, 1, SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders))

) AS avg_discounted_afv_uplift,

/*Normalised - AFV Uplift*/
SAFE_DIVIDE(
SAFE_DIVIDE(deal_data_combined.avg_daily_gfv_local, (pre_deal_data_combined.avg_daily_sessions*deal_data_combined.conversion_rate))
- SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders),
IF(SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) = 0 OR SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) IS NULL, 1, SAFE_DIVIDE(pre_deal_data_combined.avg_daily_gfv_local, pre_deal_data_combined.avg_daily_valid_orders))
) AS avg_afv_normalised_uplift,

/*Normalised - Discounted AFV Uplift*/
SAFE_DIVIDE(
SAFE_DIVIDE(deal_data_combined.avg_daily_discount_gfv_local, (pre_deal_data_combined.avg_daily_sessions*deal_data_combined.conversion_rate))
- SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders),
IF(SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) = 0 OR SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders) IS NULL, 1, SAFE_DIVIDE(pre_deal_data_combined.avg_daily_discount_gfv_local, pre_deal_data_combined.avg_daily_valid_orders))
) AS avg_discounted_afv_normalised_uplift,

/*Commissionable Base Uplift*/
SAFE_DIVIDE(deal_data_combined.avg_daily_commission_base_eur - pre_deal_data_combined.avg_daily_commission_base_eur,
pre_deal_data_combined.avg_daily_commission_base_eur) AS avg_commissionable_base_uplift,

/*Commission Revenue Uplift*/
SAFE_DIVIDE(deal_data_combined.avg_daily_commission_revenue_eur - pre_deal_data_combined.avg_daily_commission_revenue_eur,
pre_deal_data_combined.avg_daily_commission_revenue_eur) AS avg_commission_revenue_uplift,

/*New Customer To Vendor Uplift*/
SAFE_DIVIDE(deal_data_combined.avg_daily_first_valid_order_with_this_vendor - pre_deal_data_combined.avg_daily_first_valid_order_with_this_vendor,
pre_deal_data_combined.avg_daily_first_valid_order_with_this_vendor) AS avg_nc_to_vendor_uplift,

/*New Customer To foodpanda Uplift*/
SAFE_DIVIDE(deal_data_combined.avg_daily_first_valid_order_with_foodpanda - pre_deal_data_combined.avg_daily_first_valid_order_with_foodpanda,
pre_deal_data_combined.avg_daily_first_valid_order_with_foodpanda) AS avg_nc_to_fp_uplift,

/*GP Uplift*/
SAFE_DIVIDE(deal_data_combined.profit_per_order - pre_deal_data_combined.profit_per_order,
pre_deal_data_combined.profit_per_order) AS profit_per_order_uplift,

FROM (
  SELECT
  *
  FROM regrouping
  WHERE is_deal_grouping
  ) deal_data_combined
INNER JOIN (
SELECT
*
FROM regrouping
WHERE NOT is_deal_grouping
AND no_of_days_in_period >= 7
) pre_deal_data_combined
ON pre_deal_data_combined.id_deal_comparison_all = deal_data_combined.id_deal_comparison_all
