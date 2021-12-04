WITH vendor_list_all AS (
/*Getting full vendor list excluding test, deleted & inactive vendors at end of last month*/
  SELECT
    countries.name AS country_name,
    countries.company_name,
    vendors.global_entity_id,
    vendors.pd_city_id,
    vendors.location.city AS vendor_location_city,
    vendors.vendor_code,
    vendors.name AS vendor_name,
    vendors.chain_code,
    vendors.chain_name
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS business_types
         ON business_types.uuid = vendors.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS countries
         ON countries.global_entity_id = vendors.global_entity_id
  WHERE NOT vendors.is_test
    AND vendors.created_date_utc < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND (
          (vendors.global_entity_id != 'FP_PK'
          AND business_types.business_type_apac IN ('restaurants'))
        OR (vendors.global_entity_id = 'FP_PK'
          AND business_types.is_restaurants
          AND NOT business_types.is_home_based_kitchen)
        )
    AND vendors.global_entity_id LIKE 'FP_%'
    AND vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG', 'FP_DE')
),

/*Getting CPC Revenue*/
cpc_bookings AS (
  SELECT
    pps_bookings.global_entity_id,
    pps_bookings.uuid AS booking_id,
    pps_bookings.vendor_code AS vendor_code,
    pps_bookings.status,
    cpc_billings.initial_budget AS initial_budget_local,
    cpc_billings.click_price AS click_price_local,
    SAFE_DIVIDE(cpc_billings.initial_budget, cpc_billings.click_price) AS budgeted_clicks,
    DATE(pps_bookings.started_at_utc) AS start_date,
    CASE
      WHEN DATE(pps_bookings.ended_at_utc) IS NULL
      THEN CURRENT_DATE()
      ELSE DATE(pps_bookings.ended_at_utc)
    END AS end_date,
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` AS pps_bookings,
        UNNEST(pps_bookings.cpc_billings) AS cpc_billings
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS pps_cpc_clicks
         ON pps_cpc_clicks.pps_item_uuid = pps_bookings.uuid
  WHERE pps_bookings.uuid IS NOT NULL
    AND pps_bookings.type = 'organic_placements'
    AND pps_bookings.billing_type = 'CPC'
    AND pps_bookings.status <> "new"
    AND (
          DATE(pps_bookings.ended_at_utc)
          >= DATE_TRUNC(
               DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
               MONTH
             )
          OR DATE(pps_bookings.ended_at_utc) IS NULL
        )
),

/*1 line per month live*/
cpc_bookings_array AS (
  SELECT
    *,
    GENERATE_DATE_ARRAY(
      DATE_TRUNC(start_date, MONTH),
      DATE_TRUNC(end_date, MONTH),
      INTERVAL 1 MONTH
    ) AS datelive_nested
  FROM cpc_bookings
),

final_cpc_bookings AS (
  SELECT
    global_entity_id,
    booking_id,
    vendor_code,
    start_date,
    end_date,
    DATE_TRUNC(date_live, MONTH) AS cpc_month,
    click_price_local,
    budgeted_clicks,
    initial_budget_local,
  FROM cpc_bookings_array,
       UNNEST(datelive_nested) AS date_live
),

cpc_clicks AS (
  SELECT DISTINCT
    pps_bookings.uuid AS booking_id,
    DATE_TRUNC(DATE(pps_cpc_clicks.created_at_utc), MONTH) AS click_month,
    IF(
      SUM(pps_cpc_clicks.quantity) IS NOT NULL,
      SUM(pps_cpc_clicks.quantity),
      0
    ) AS spent_clicks
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` AS pps_bookings,
       UNNEST(pps_bookings.cpc_billings) AS cpc_billings
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS pps_cpc_clicks
         ON cpc_billings.uuid = pps_cpc_clicks.pps_item_uuid
  GROUP BY
    booking_id,
    click_month
),

final_cpc_clicks_count AS (
  SELECT
    final_cpc_bookings.global_entity_id,
    final_cpc_bookings.vendor_code,
    final_cpc_bookings.booking_id,
    final_cpc_bookings.cpc_month,
    final_cpc_bookings.click_price_local,
    final_cpc_bookings.initial_budget_local,
    IF(
      cpc_clicks.spent_clicks > final_cpc_bookings.budgeted_clicks,
      final_cpc_bookings.budgeted_clicks,
      cpc_clicks.spent_clicks
    ) AS final_spent_clicks
  FROM final_cpc_bookings
  LEFT JOIN cpc_clicks
         ON cpc_clicks.click_month = final_cpc_bookings.cpc_month
        AND cpc_clicks.booking_id = final_cpc_bookings.booking_id
  WHERE cpc_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
    AND cpc_month < DATE_TRUNC(CURRENT_DATE(), MONTH)
),

cpc_revenue_per_vendor AS (
  SELECT
    global_entity_id,
    vendor_code,
    SUM(
      CASE
        WHEN SAFE_MULTIPLY(final_spent_clicks, click_price_local) > initial_budget_local
        THEN CAST(initial_budget_local AS FLOAT64)
        ELSE CAST(SAFE_MULTIPLY(final_spent_clicks, click_price_local) AS FLOAT64)
      END
    ) AS cpc_revenue_local
  FROM final_cpc_clicks_count
  GROUP BY global_entity_id, vendor_code
),

/*Getting CPP Revenue*/
cpp_bookings AS (
  SELECT
    global_entity_id,
    vendor_code,
    DATE(
      FORMAT_DATETIME(
        "%Y-%m-%d",
        PARSE_DATETIME(
          "%Y%m",
          CAST(year_month AS STRING)
        )
      )
    ) AS cpp_month,
    type,
    status,
    COUNT(DISTINCT uuid) AS booking_id,
    SUM(IFNULL(cpp_billing.price, 0)) AS sold_price_local
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings`
  WHERE uuid IS NOT NULL
    AND billing_type = 'CPP'
    AND status <> 'cancelled'
    AND DATE(
          FORMAT_DATETIME("%Y-%m-%d",
          PARSE_DATETIME("%Y%m", CAST(year_month AS STRING)))
        ) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
    AND DATE(
          FORMAT_DATETIME("%Y-%m-%d",
          PARSE_DATETIME("%Y%m", CAST(year_month AS STRING)))
        ) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND (
          CASE
            WHEN global_entity_id IN ('FP_BD', 'FP_PK', 'FP_SG', 'FP_MY', 'FP_JP')
                 AND type = 'premium_placements'
            THEN cpp_billing.position <= 7
            WHEN global_entity_id IN ('FP_HK', 'FP_PH')
                 AND type = 'premium_placements'
            THEN cpp_billing.position <= 10
            WHEN global_entity_id = 'FP_TH'
                 AND type = 'premium_placements'
            THEN cpp_billing.position <= 15
            WHEN global_entity_id = 'FP_TW'
                 AND type = 'premium_placements'
            THEN cpp_billing.position <= 8
            WHEN global_entity_id IN ('FP_LA', 'FP_KH', 'FP_MM')
                 AND type = 'premium_placements'
            THEN cpp_billing.position <= 7
            WHEN global_entity_id IN ('FP_LA', 'FP_KH', 'FP_MM')
                 AND type = 'organic_placements'
            THEN cpp_billing.position <= 8
          END
        )
  GROUP BY
    global_entity_id,
    vendor_code,
    cpp_month,
    type,
    status
),

cpp_revenue_per_vendor AS (
  SELECT
    global_entity_id,
    vendor_code,
    SUM(IFNULL(sold_price_local, 0)) AS cpp_revenue_local
  FROM cpp_bookings
  GROUP BY
    global_entity_id,
    vendor_code
),

/*Getting PandaBox Revenue*/
pandabox_revenue_per_vendor AS (
  SELECT
    global_entity_id,
    country_name,
    vendor_code,
    SUM(
      IF(
        is_joker_used AND is_valid_order AND NOT is_test_order,
        joker_fee_local,
        0
      )
    ) AS pandabox_revenue_local
  FROM `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals`
  WHERE DATE_TRUNC(created_date_local, MONTH)
        >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
    AND DATE_TRUNC(created_date_local, MONTH)
        < DATE_TRUNC(CURRENT_DATE(), MONTH)
  GROUP BY
    global_entity_id,
    country_name,
    vendor_code
),

vendor_performance_metrics AS (
/*Getting last month's performance metrics on order level*/
  SELECT
    vendor_list_all.global_entity_id,
    vendor_list_all.country_name,
    vendor_list_all.company_name,
    vendor_list_all.pd_city_id,
    vendor_list_all.vendor_location_city,
    vendor_list_all.vendor_code,
    vendor_list_all.vendor_name,
    vendor_list_all.chain_code,
    vendor_list_all.chain_name,
    (
      SUM(IFNULL(cpp_revenue_local, 0))
      + SUM(IFNULL(cpc_revenue_local, 0))
      + SUM(IFNULL(pandabox_revenue_local, 0))
    ) AS vendor_ads_revenue_local,
    SUM(IFNULL(orders.gmv_local, 0)) AS vendor_gmv_local,
    COUNT(DISTINCT orders.id) AS vendor_order_count
  FROM vendor_list_all
  LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` AS orders
         ON orders.global_entity_id = vendor_list_all.global_entity_id
        AND orders.vendor_code = vendor_list_all.vendor_code
        AND orders.is_valid_order
        AND NOT orders.is_test_order
        AND orders.created_date_local
            >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
        AND orders.created_date_local
            < DATE_TRUNC(CURRENT_DATE(), MONTH)
  LEFT JOIN cpp_revenue_per_vendor
         ON vendor_list_all.global_entity_id = cpp_revenue_per_vendor.global_entity_id
        AND vendor_list_all.vendor_code = cpp_revenue_per_vendor.vendor_code
  LEFT JOIN cpc_revenue_per_vendor
         ON vendor_list_all.global_entity_id = cpc_revenue_per_vendor.global_entity_id
        AND vendor_list_all.vendor_code = cpc_revenue_per_vendor.vendor_code
  LEFT JOIN pandabox_revenue_per_vendor
         ON vendor_list_all.global_entity_id = pandabox_revenue_per_vendor.global_entity_id
        AND vendor_list_all.vendor_code = pandabox_revenue_per_vendor.vendor_code
  GROUP BY
    global_entity_id,
    country_name,
    company_name,
    pd_city_id,
    vendor_location_city,
    vendor_code,
    vendor_name,
    chain_code,
    chain_name
),

chain_level_performance_country AS (
  SELECT
    global_entity_id,
    chain_code,
    SUM(IFNULL(vendor_ads_revenue_local, 0)) AS chain_country_ads_revenue_local,
    SUM(IFNULL(vendor_gmv_local, 0)) AS chain_country_gmv_local,
    SUM(IFNULL(vendor_order_count, 0)) AS chain_country_order_count
  FROM vendor_performance_metrics
  GROUP BY
    global_entity_id,
    chain_code
),

chain_level_performance_city AS (
  SELECT
    global_entity_id,
    pd_city_id,
    chain_code,
    SUM(IFNULL(vendor_ads_revenue_local, 0)) AS chain_city_ads_revenue_local,
    SUM(IFNULL(vendor_gmv_local, 0)) AS chain_city_gmv_local,
    SUM(IFNULL(vendor_order_count, 0)) AS chain_city_order_count
  FROM vendor_performance_metrics
  GROUP BY
    global_entity_id,
    pd_city_id,
    chain_code
),

/*For chain vendors, average performance metric values are used
  after chain grouping unlike standalone vendors*/
vendors_list_country_level AS (
  SELECT
    global_entity_id,
    vendor_code AS code,
    FALSE AS is_chain,
    vendor_ads_revenue_local,
    vendor_gmv_local,
    vendor_order_count
  FROM vendor_performance_metrics
  WHERE chain_code IS NULL

  UNION ALL

  SELECT
    global_entity_id,
    chain_code AS code,
    TRUE AS is_chain,
    AVG(chain_country_ads_revenue_local) AS vendor_ads_revenue_local,
    AVG(chain_country_gmv_local) AS vendor_gmv_local,
    AVG(chain_country_order_count) AS vendor_order_count
  FROM chain_level_performance_country
  WHERE chain_code IS NOT NULL
  GROUP BY
    global_entity_id,
    code,
    is_chain
),

percentile_calculation_country AS (
  SELECT
    *,
    SUM(vendor_ads_revenue_local) OVER (
      PARTITION BY
        global_entity_id
      ORDER BY
        vendor_ads_revenue_local DESC ROWS UNBOUNDED PRECEDING
    ) AS agg_up_vendor_ads_revenue_local,
    SUM(vendor_ads_revenue_local) OVER (
      PARTITION BY
        global_entity_id
    ) AS total_vendor_ads_revenue_local,

    SUM(vendor_gmv_local) OVER (
      PARTITION BY
        global_entity_id
      ORDER BY
        vendor_gmv_local DESC ROWS UNBOUNDED PRECEDING
    ) AS agg_up_vendor_gmv_local,
    SUM(vendor_gmv_local) OVER (
      PARTITION BY
        global_entity_id
    ) AS total_vendor_gmv_local,

    SUM(vendor_order_count) OVER (
      PARTITION BY
        global_entity_id
      ORDER BY
        vendor_order_count DESC ROWS UNBOUNDED PRECEDING
    ) AS agg_up_vendor_order_count,
    SUM(vendor_order_count) OVER (
      PARTITION BY
        global_entity_id
    ) AS total_vendor_order_count

  FROM vendors_list_country_level
  GROUP BY
    global_entity_id,
    code,
    is_chain,
    vendor_ads_revenue_local,
    vendor_gmv_local,
    vendor_order_count
),

vendors_list_city_level AS (
  SELECT
    global_entity_id,
    pd_city_id,
    vendor_code AS code,
    FALSE AS is_chain,
    vendor_gmv_local
  FROM vendor_performance_metrics
  WHERE chain_code IS NULL

  UNION ALL

  SELECT
    global_entity_id,
    pd_city_id,
    chain_code AS code,
    TRUE AS is_chain,
    AVG(chain_city_gmv_local) AS vendor_gmv_local
  FROM chain_level_performance_city
  WHERE chain_code IS NOT NULL
  GROUP BY
    global_entity_id,
    pd_city_id,
    code,
    is_chain
),

percentile_calculation_city AS (
  SELECT
    *,

    SUM(vendor_gmv_local) OVER (
      PARTITION BY
        global_entity_id, pd_city_id
      ORDER BY
        vendor_gmv_local DESC ROWS UNBOUNDED PRECEDING
    ) AS agg_up_vendor_gmv_local_city,
    SUM(vendor_gmv_local) OVER (
      PARTITION BY
        global_entity_id, pd_city_id
    ) AS total_vendor_gmv_local_city

  FROM vendors_list_city_level
  GROUP BY
    global_entity_id,
    pd_city_id,
    code,
    is_chain,
    vendor_gmv_local
)

/*Final Join To Get Full Vendor List with class types*/
SELECT
  vendor.global_entity_id,
  vendor.country_name,
  vendor.company_name,
  vendor.pd_city_id,
  vendor.vendor_location_city,
  vendor.vendor_code,
  vendor.vendor_name,
  vendor.chain_code,
  vendor.chain_name,
  CASE
    WHEN perc_country.agg_up_vendor_gmv_local = 0
         OR perc_country.agg_up_vendor_gmv_local
            > perc_country.total_vendor_gmv_local * 0.9
    THEN 'D'
    WHEN perc_country.agg_up_vendor_gmv_local
         > perc_country.total_vendor_gmv_local * 0.7
         AND perc_country.agg_up_vendor_gmv_local
             <= perc_country.total_vendor_gmv_local * 0.9
    THEN 'C'
    WHEN perc_country.agg_up_vendor_gmv_local
         > perc_country.total_vendor_gmv_local * 0.5
         AND perc_country.agg_up_vendor_gmv_local
             <= perc_country.total_vendor_gmv_local * 0.7
    THEN 'B'
    WHEN perc_country.agg_up_vendor_gmv_local
         <= perc_country.total_vendor_gmv_local * 0.5
    THEN 'A'
    ELSE 'D'
  END AS gmv_class,
  CASE
    WHEN perc_city.agg_up_vendor_gmv_local_city = 0
         OR perc_city.agg_up_vendor_gmv_local_city
            > perc_city.total_vendor_gmv_local_city * 0.9
    THEN 'D'
    WHEN perc_city.agg_up_vendor_gmv_local_city
         > perc_city.total_vendor_gmv_local_city * 0.7
         AND perc_city.agg_up_vendor_gmv_local_city
         <= perc_city.total_vendor_gmv_local_city * 0.9
    THEN 'C'
    WHEN perc_city.agg_up_vendor_gmv_local_city
         > perc_city.total_vendor_gmv_local_city * 0.5
         AND perc_city.agg_up_vendor_gmv_local_city
         <= perc_city.total_vendor_gmv_local_city * 0.7
    THEN 'B'
    WHEN perc_city.agg_up_vendor_gmv_local_city
         <= perc_city.total_vendor_gmv_local_city * 0.5
    THEN 'A'
    ELSE 'D'
  END AS city_gmv_class,
  CASE
    WHEN perc_country.agg_up_vendor_order_count = 0
         OR perc_country.agg_up_vendor_order_count
            > perc_country.total_vendor_order_count * 0.9
    THEN 'D'
    WHEN perc_country.agg_up_vendor_order_count
         > perc_country.total_vendor_order_count * 0.7
         AND perc_country.agg_up_vendor_order_count
             <= perc_country.total_vendor_order_count * 0.9
    THEN 'C'
    WHEN perc_country.agg_up_vendor_order_count
         > perc_country.total_vendor_order_count * 0.5
         AND perc_country.agg_up_vendor_order_count
             <= perc_country.total_vendor_order_count * 0.7
    THEN 'B'
    WHEN perc_country.agg_up_vendor_order_count
         <= perc_country.total_vendor_order_count * 0.5
    THEN 'A'
    ELSE 'D'
  END AS order_class,
  CASE
    WHEN perc_country.agg_up_vendor_ads_revenue_local = 0
         OR perc_country.agg_up_vendor_ads_revenue_local
            > perc_country.total_vendor_ads_revenue_local * 0.9
    THEN 'D'
    WHEN perc_country.agg_up_vendor_ads_revenue_local
         > perc_country.total_vendor_ads_revenue_local * 0.7
         AND perc_country.agg_up_vendor_ads_revenue_local
             <= perc_country.total_vendor_ads_revenue_local * 0.9
    THEN 'C'
    WHEN perc_country.agg_up_vendor_ads_revenue_local
         > perc_country.total_vendor_ads_revenue_local * 0.5
         AND perc_country.agg_up_vendor_ads_revenue_local
             <= perc_country.total_vendor_ads_revenue_local * 0.7
    THEN 'B'
    WHEN perc_country.agg_up_vendor_ads_revenue_local
         <= perc_country.total_vendor_ads_revenue_local * 0.5
    THEN 'A'
    ELSE 'D'
  END AS ads_class,
  vendor.vendor_ads_revenue_local,
  vendor.vendor_gmv_local,
  vendor.vendor_order_count
FROM vendor_performance_metrics AS vendor
LEFT JOIN percentile_calculation_country AS perc_country
       ON perc_country.global_entity_id = vendor.global_entity_id
      AND perc_country.code = vendor.vendor_code
      AND NOT perc_country.is_chain
LEFT JOIN percentile_calculation_city AS perc_city
       ON perc_city.global_entity_id = vendor.global_entity_id
      AND perc_city.pd_city_id = vendor.pd_city_id
      AND perc_city.code = vendor.vendor_code
      AND NOT perc_city.is_chain
WHERE vendor.chain_code IS NULL

UNION ALL

SELECT
  vendor.global_entity_id,
  vendor.country_name,
  vendor.company_name,
  vendor.pd_city_id,
  vendor.vendor_location_city,
  vendor.vendor_code,
  vendor.vendor_name,
  vendor.chain_code,
  vendor.chain_name,
  CASE
    WHEN perc_country.agg_up_vendor_gmv_local = 0
         OR perc_country.agg_up_vendor_gmv_local
            > perc_country.total_vendor_gmv_local * 0.9
    THEN 'D'
    WHEN perc_country.agg_up_vendor_gmv_local
         > perc_country.total_vendor_gmv_local * 0.7
         AND perc_country.agg_up_vendor_gmv_local
             <= perc_country.total_vendor_gmv_local * 0.9
    THEN 'C'
    WHEN perc_country.agg_up_vendor_gmv_local
         > perc_country.total_vendor_gmv_local * 0.5
         AND perc_country.agg_up_vendor_gmv_local
             <= perc_country.total_vendor_gmv_local * 0.7
    THEN 'B'
    WHEN perc_country.agg_up_vendor_gmv_local
         <= perc_country.total_vendor_gmv_local * 0.5
    THEN 'A'
    ELSE 'D'
  END AS gmv_class,
  CASE
    WHEN perc_city.agg_up_vendor_gmv_local_city = 0
         OR perc_city.agg_up_vendor_gmv_local_city
            > perc_city.total_vendor_gmv_local_city * 0.9
    THEN 'D'
    WHEN perc_city.agg_up_vendor_gmv_local_city
         > perc_city.total_vendor_gmv_local_city * 0.7
         AND perc_city.agg_up_vendor_gmv_local_city
             <= perc_city.total_vendor_gmv_local_city * 0.9
    THEN 'C'
    WHEN perc_city.agg_up_vendor_gmv_local_city
         > perc_city.total_vendor_gmv_local_city * 0.5
         AND perc_city.agg_up_vendor_gmv_local_city
             <= perc_city.total_vendor_gmv_local_city * 0.7
    THEN 'B'
    WHEN perc_city.agg_up_vendor_gmv_local_city
         <= perc_city.total_vendor_gmv_local_city * 0.5
    THEN 'A'
    ELSE 'D'
  END AS city_gmv_class,
  CASE
    WHEN perc_country.agg_up_vendor_order_count = 0
         OR perc_country.agg_up_vendor_order_count
            > perc_country.total_vendor_order_count * 0.9
    THEN 'D'
    WHEN perc_country.agg_up_vendor_order_count
         > perc_country.total_vendor_order_count * 0.7
         AND perc_country.agg_up_vendor_order_count
             <= perc_country.total_vendor_order_count * 0.9
    THEN 'C'
    WHEN perc_country.agg_up_vendor_order_count
         > perc_country.total_vendor_order_count * 0.5
         AND perc_country.agg_up_vendor_order_count
             <= perc_country.total_vendor_order_count * 0.7
    THEN 'B'
    WHEN perc_country.agg_up_vendor_order_count
         <= perc_country.total_vendor_order_count * 0.5
    THEN 'A'
    ELSE 'D'
  END AS order_class,
  CASE
    WHEN perc_country.agg_up_vendor_ads_revenue_local = 0
         OR perc_country.agg_up_vendor_ads_revenue_local
            > perc_country.total_vendor_ads_revenue_local * 0.9
    THEN 'D'
    WHEN perc_country.agg_up_vendor_ads_revenue_local
         > perc_country.total_vendor_ads_revenue_local * 0.7
         AND perc_country.agg_up_vendor_ads_revenue_local
             <= perc_country.total_vendor_ads_revenue_local * 0.9
    THEN 'C'
    WHEN perc_country.agg_up_vendor_ads_revenue_local
         > perc_country.total_vendor_ads_revenue_local * 0.5
         AND perc_country.agg_up_vendor_ads_revenue_local
             <= perc_country.total_vendor_ads_revenue_local * 0.7
    THEN 'B'
    WHEN perc_country.agg_up_vendor_ads_revenue_local
         <= perc_country.total_vendor_ads_revenue_local * 0.5
    THEN 'A'
    ELSE 'D'
  END AS ads_class,
  vendor.vendor_ads_revenue_local,
  vendor.vendor_gmv_local,
  vendor.vendor_order_count
FROM vendor_performance_metrics AS vendor
LEFT JOIN percentile_calculation_country AS perc_country
       ON perc_country.global_entity_id = vendor.global_entity_id
      AND perc_country.code = vendor.chain_code
      AND perc_country.is_chain
LEFT JOIN percentile_calculation_city AS perc_city
       ON perc_city.global_entity_id = vendor.global_entity_id
      AND perc_city.pd_city_id = vendor.pd_city_id
      AND perc_city.code = vendor.chain_code
      AND perc_city.is_chain
WHERE vendor.chain_code IS NOT NULL
