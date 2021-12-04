/*Title: Commercial Portfolio
  Author: Abbhinaya Pragasm
  Last Modified: July 2021
*/

WITH start_month AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 7 MONTH), MONTH) AS start_date
),

fc AS (
SELECT
global_entity_id,
vendor_code,
TRUE AS is_home_based_kitchens
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors`,
      UNNEST(food_characteristics) food_characteristics
WHERE LOWER(food_characteristics.type) LIKE '%characteristic%'
AND LOWER(food_characteristics.title) LIKE '%home based%'
),

vendor_with_sf_info AS (
  SELECT
    v.global_entity_id,
    shared_countries.name AS country,
    v.chain_code,
    v.chain_name,
    v.vendor_code,
    location.latitude,
    location.longitude,
    v.has_delivery_type_pickup,
    CASE
      WHEN pd_vendors_agg_business_types.business_type_apac IN ('restaurants') AND (NOT fc.is_home_based_kitchens OR NOT pd_vendors_agg_business_types.is_home_based_kitchen)
      THEN 'restaurants'
      WHEN fc.is_home_based_kitchens OR pd_vendors_agg_business_types.is_home_based_kitchen
      THEN 'restaurants - home based kitchens'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'kitchens'
      THEN 'kitchen'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'concepts'
      THEN 'concepts'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'dmart'
      THEN 'PandaNow'
      WHEN pd_vendors_agg_business_types.business_type_apac = 'shops'
      THEN 'PandaMart'
      ELSE 'restaurants'
    END AS business_type,
    TRUE AS enterprise_account,
    CASE
      WHEN sf_accounts_custom.vendor_grade LIKE "%AAA%"
      THEN 'AAA'
      ELSE 'Non-AAA'
    END AS aaa_type,
    CASE
      WHEN sf_accounts_custom.type = "Holding"
      THEN sf_accounts_custom.name
      ELSE NULL
    END AS holding_account_name,
    CASE
      WHEN sf_accounts_custom.type = "Group"
      THEN sf_accounts_custom.name
      ELSE NULL
    END AS group_account_name,
    CASE
      WHEN sf_accounts_custom.type = "Brand"
      THEN sf_accounts_custom.name
      ELSE NULL
    END AS brand_account_name,
    sf_accounts_custom.id AS sf_id,
    sf_accounts_custom.global_vendor_id AS grid_id,
    sf_accounts_custom.sf_parent_account_id AS parent_sf_account_id,
    sf_accounts_custom.name AS account_name,
    sf_accounts_custom.owner_name AS owner_name,
    sf_accounts_custom.status,
    COALESCE(
      CASE WHEN v.primary_cuisine = 'NA' THEN NULL ELSE v.primary_cuisine END,
      CASE WHEN sf_accounts_custom.primary_cuisine = 'NA' THEN NULL ELSE sf_accounts_custom.primary_cuisine END,
      CASE WHEN sf_accounts_custom.category = 'NA' THEN NULL ELSE sf_accounts_custom.category END,
      CASE WHEN sf_accounts_custom.vertical_segment = 'NA' THEN NULL ELSE sf_accounts_custom.vertical_segment END,
      CASE WHEN sf_accounts_custom.vertical = 'NA' THEN NULL ELSE sf_accounts_custom.vertical END
            ) AS primary_cuisine
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts_custom
         ON sf_accounts_custom.vendor_code = v.vendor_code
        AND sf_accounts_custom.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = v.uuid
  LEFT JOIN fc
         ON fc.vendor_code = v.vendor_code
        AND fc.global_entity_id = v.global_entity_id
  WHERE NOT v.is_test
    AND v.global_entity_id LIKE 'FP_%'
    --AND v.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
),

ve AS (
  SELECT
    global_entity_id,
    country,
    ve.chain_code,
    ve.chain_name AS chain,
    COALESCE(
      ve.chain_name,
      ve.holding_account_name,
      ve.group_account_name,
      ve.brand_account_name
    ) AS top_hierarchy_name,
    ve.vendor_code AS vendor_code,
    business_type,
    aaa_type,
    ve.enterprise_account,
    ve.holding_account_name AS sf_holding,
    ve.group_account_name AS sf_group,
    ve.brand_account_name AS sf_brand,
    ve.sf_id,
    ve.grid_id,
    ve.account_name AS outlet,
    ve.owner_name,
    ve.primary_cuisine,
    ve.latitude,
    ve.longitude,
    ve.has_delivery_type_pickup,
    ve.status
  FROM vendor_with_sf_info AS ve
),

dates AS (
  SELECT
      vendors.*,
      date,
      iso_year_week_string,
      weekday_name
  FROM `fulfillment-dwh-production.pandata_curated.shared_dates` AS shared_dates
  CROSS JOIN (
    SELECT DISTINCT
      global_entity_id,
      uuid AS pd_vendor_uuid,
      vendor_code
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors`
  ) AS vendors
  WHERE DATE >= (SELECT start_date FROM start_month)
    AND DATE <= CURRENT_DATE
),

/* vendor availability */
vendor_availability AS (
  SELECT
    uuid,
    pd_vendor_uuid,
    vendor_code,
    reason,
    start_at_utc,
    end_at_utc,
    updated_at_utc,
    DATE(updated_at_utc) AS updated_date
  FROM `fulfillment-dwh-staging.pandata_curated.pd_vendor_availabilities__historical`,
  UNNEST(unavailable_reasons) AS unnested_unavailable_reasons
  WHERE updated_date_utc >= (SELECT start_date FROM start_month)
    AND reason = 'RESTRICTED_VISIBILITY'
),

vendor_turned_private AS (
  SELECT
    *
    EXCEPT (is_latest_within_a_day)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          pd_vendor_uuid,
          updated_date
        ORDER BY updated_at_utc DESC
      ) = 1 AS is_latest_within_a_day
    FROM vendor_availability
    )
  WHERE is_latest_within_a_day
),

vendor_status_by_day as (
  SELECT *
  FROM (
    SELECT
      dates.*,
      pd_vendors__historical.is_active,
      vendor_turned_private.uuid IS NOT NULL AS is_private,
      /*pd_vendors__historical.is_private,*/
      pd_vendors__historical.is_test,
      ROW_NUMBER() OVER (PARTITION BY dates.global_entity_id, dates.vendor_code, dates.date ORDER BY pd_vendors__historical.updated_at_utc DESC) = 1 AS is_latest_entry
    FROM dates
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors__historical` AS pd_vendors__historical
           ON pd_vendors__historical.pd_vendor_uuid = dates.pd_vendor_uuid
          AND DATE(pd_vendors__historical.updated_at_utc) <= dates.date
          AND pd_vendors__historical.updated_date_utc >= '2020-01-01'
    LEFT JOIN vendor_turned_private
           ON vendor_turned_private.pd_vendor_uuid = dates.pd_vendor_uuid
          AND vendor_turned_private.start_at_utc <= dates.date
          AND (vendor_turned_private.end_at_utc >= dates.date OR vendor_turned_private.end_at_utc IS NULL)
    WHERE pd_vendors__historical.pd_vendor_uuid IS NOT NULL
  )
  WHERE is_latest_entry
),

active_status AS (
  SELECT
    vendor_status_by_day.date,
    vendor_status_by_day.global_entity_id,
    pd_vendors.uuid,
    pd_vendors.vendor_code,
    TRUE AS is_daily_active
  FROM vendor_status_by_day
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
         on pd_vendors.global_entity_id = vendor_status_by_day.global_entity_id
        AND pd_vendors.vendor_code = vendor_status_by_day.vendor_code
  WHERE TRUE
    AND vendor_status_by_day.is_active
    AND NOT vendor_status_by_day.is_private
    AND NOT vendor_status_by_day.is_test
),

vendor_sessions AS (
  SELECT
    country,
    global_entity_id,
    DATE_TRUNC(date_local, MONTH) AS month,
    vendor_code,
    SUM(count_of_shop_list_loaded) AS count_of_shop_list_loaded,
    SUM(count_of_shop_menu_loaded) AS count_of_shop_menu_loaded,
    SUM(count_of_add_cart_clicked) AS count_of_add_cart_clicked,
    SUM(count_of_checkout_loaded) AS count_of_checkout_loaded,
    SUM(count_of_transaction) AS count_of_transaction,
    SUM(count_of_shop_list_loaded_delivery) AS count_of_shop_list_loaded_delivery,
    SUM(count_of_shop_menu_loaded_delivery) AS count_of_shop_menu_loaded_delivery,
    SUM(count_of_add_cart_clicked_delivery) AS count_of_add_cart_clicked_delivery,
    SUM(count_of_checkout_loaded_delivery) AS count_of_checkout_loaded_delivery,
    SUM(count_of_transaction_delivery) AS count_of_transaction_delivery,
    SUM(count_of_shop_list_loaded_pickup) AS count_of_shop_list_loaded_pickup,
    SUM(count_of_shop_menu_loaded_pickup) AS count_of_shop_menu_loaded_pickup,
    SUM(count_of_add_cart_clicked_pickup) AS count_of_add_cart_clicked_pickup,
    SUM(count_of_checkout_loaded_pickup) AS count_of_checkout_loaded_pickup,
    SUM(count_of_transaction_pickup) AS count_of_transaction_pickup
  FROM `fulfillment-dwh-production.pandata_report.product_vendor_session_metrics`
  WHERE partition_date >= (SELECT start_date FROM start_month)
  GROUP BY 1, 2, 3, 4
),

reorder_rate AS (
  SELECT
    pd_orders.created_date_local AS order_date_rr,
    pd_orders.uuid AS order_id,
    pd_orders.global_entity_id,
    pd_orders.pd_vendor_uuid,
    pd_orders.vendor_code AS vendor_code_rr,
    pd_orders.pd_customer_uuid,
    LAG(pd_orders.created_date_local) OVER (PARTITION BY pd_orders.global_entity_id, pd_orders.pd_customer_uuid, pd_vendor_uuid ORDER BY pd_orders.created_date_local) AS last_order_date,
    LAG(pd_orders.uuid) OVER (PARTITION BY pd_orders.global_entity_id, pd_orders.pd_customer_uuid, pd_vendor_uuid  ORDER BY pd_orders.created_date_local) AS last_order_id,
    LAG(pd_orders.pd_customer_uuid) OVER (PARTITION BY pd_orders.global_entity_id, pd_orders.pd_customer_uuid, pd_vendor_uuid  ORDER BY pd_orders.created_date_local) AS last_customer_id,
    ROW_NUMBER() OVER (PARTITION BY pd_orders.global_entity_id, pd_orders.created_date_local, pd_vendor_uuid, pd_orders.pd_customer_uuid ORDER BY pd_orders.created_date_local) AS ranking
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` pd_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_cp_orders` pd_orders_agg_cp_orders
         ON pd_orders_agg_cp_orders.uuid = pd_orders.uuid
        AND pd_orders_agg_cp_orders.created_date_utc <= CURRENT_DATE
  WHERE pd_orders.created_date_utc >= (SELECT start_date FROM start_month)
    AND is_valid_order
    --AND order_source <> 'corporate'
    AND NOT is_test_order
),

rr_base AS (
  SELECT
    *
  FROM reorder_rate
  WHERE ranking = 1
),

rrate AS (
  SELECT
    global_entity_id,
    DATE_TRUNC(order_date_rr, MONTH) AS order_date_rr,
    vendor_code_rr,
    COUNT(DISTINCT pd_customer_uuid) AS reorder_a,
    COUNT(DISTINCT CASE
                     WHEN last_order_date IS NOT NULL AND DATE_DIFF(order_date_rr,last_order_date, DAY) + 1 BETWEEN 0 AND 30
                     THEN pd_customer_uuid
                     ELSE NULL
                   END) AS reorder_b
  FROM rr_base
  GROUP BY 1, 2, 3
),

exchange_rate_monthly AS (
  SELECT
    countries.global_entity_id,
    DATE_TRUNC(fx_rates.fx_rate_date, MONTH) AS month,
    AVG(fx_rates.fx_rate_eur) AS exchange_rate
  FROM `fulfillment-dwh-production.pandata_curated.shared_countries` countries
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx_rates
         ON countries.currency_code_iso = fx_rates.currency_code_iso
  WHERE global_entity_id LIKE 'FP_%'
    AND global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND fx_rates.fx_rate_date >= (SELECT start_date FROM start_month)
    AND fx_rates.fx_rate_date <= CURRENT_DATE
  GROUP BY 1, 2
),

/*Getting CPC Revenue*/
exchange_rate_daily AS (
  SELECT
    countries.global_entity_id,
    fx_rates.fx_rate_date,
    AVG(fx_rates.fx_rate_eur) AS exchange_rate
  FROM `fulfillment-dwh-production.pandata_curated.shared_countries` countries
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx_rates
         ON countries.currency_code_iso = fx_rates.currency_code_iso
  WHERE global_entity_id LIKE 'FP_%'
    --AND global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND fx_rates.fx_rate_date >= '2020-01-01'
    AND fx_rates.fx_rate_date <= CURRENT_DATE
  GROUP BY 1, 2
),

dvzone as (
    select
    v.*,
    lg_zones.lg_zone_id,
    lg_zones.lg_city_name,
    ROW_NUMBER() OVER (PARTITION BY v.global_entity_id, v.vendor_code ORDER BY lg_zone_id) AS row_number
    from `fulfillment-dwh-production.pandata_curated.pd_vendors` v
    left join `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` zones
           on v.uuid = zones.uuid
    , unnest(lg_zones) as lg_zones
),

cpc_bookings AS (
  select
    bk.global_entity_id,
    c.name as common_name,
    bk.uuid as booking_id,
    c.currency_code_iso,
    v.name as vendor_name,
    bk.vendor_code,
    dvz.chain_name,
    dvz.chain_code,
    dvz.lg_city_name AS city_name,
    hz.name AS hurrier_zone,
    bk.user,
    CASE
      WHEN
        (CASE WHEN lower(bk.user) not like '%foodpanda%' then 'self-booking' else bk.user end) = 'self-booking'
      then 'Self-Booking'
      when agent_list.email is NULL
    then 'Local' else 'Central' end as channel,
    format_date('W%V-%Y', date(bk.created_at_utc)) as booking_date,
    bk.type,
    bk.status,
    cpc.initial_budget as initial_budget_local,
    cpc.click_price as click_price_local,
    cpc.initial_budget/cpc.click_price as budgeted_clicks,
    date(bk.started_at_utc) as start_date,
    case when date(bk.ended_at_utc) is null then current_date() else date(bk.ended_at_utc) end as end_date,
    case when sf.vendor_grade = "AAA" then "AAA" else "non-AAA" end as vendor_grade,
    count (distinct cpc.uuid)as promo_areas_booked,
    from fulfillment-dwh-production.pandata_curated.pps_bookings bk, UNNEST(bk.cpc_billings) cpc
    left join fulfillment-dwh-production.pandata_curated.pps_cpc_clicks  cl on cl.pps_item_uuid=bk.uuid
    left join `fulfillment-dwh-production.pandata_curated.shared_countries` c
           on c.global_entity_id = bk.global_entity_id
    left join fulfillment-dwh-production.pandata_curated.pps_promo_areas pa on bk.pps_promo_area_uuid=pa.uuid
    left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v
           on v.global_entity_id = bk.global_entity_id
          and v.vendor_code = bk.vendor_code
    left join dvzone dvz on dvz.global_entity_id = v.global_entity_id and dvz.vendor_code = v.vendor_code
    left join (
        select global_entity_id, zone.id as lg_zone_id, zone.name as name, row_number() over (partition by global_entity_id, zone.id order by zone.created_at_local) = 1 as is_first
        from `fulfillment-dwh-production.pandata_curated.lg_countries`, unnest(cities) as cities, unnest(cities.zones) as zone
        where zone.is_active
    ) hz
           on dvz.global_entity_id = hz.global_entity_id
          and dvz.lg_zone_id = hz.lg_zone_id
          and is_first
    left join `fulfillment-dwh-production.pandata_curated.sf_accounts` sf
           on sf.global_entity_id = bk.global_entity_id
          and sf.vendor_code = bk.vendor_code
    LEFT JOIN (
      SELECT
        country_name as country
        ,email
      from `dhh---analytics-apac.pandata_ap_commercial.ncr_central_agent_material`
    ) agent_list on agent_list.email = bk.user
    where bk.uuid is not null
      and bk.type = 'organic_placements'
      and bk.billing_type = 'CPC'
      and row_number = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),

/*1 line per day live*/
cpc_bookings_array AS (
  SELECT
    *,
    GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY) AS datelive_nested
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
    date_live,
    click_price_local,
    budgeted_clicks,
    initial_budget_local,
  FROM cpc_bookings_array,
       UNNEST(datelive_nested) AS date_live
),

cpc_clicks AS (
  SELECT
    DISTINCT pps_bookings.uuid AS booking_id,
    DATE(pps_cpc_clicks.created_at_utc) AS click_date,
    SUM(IF(quantity IS NOT NULL, quantity, 0)) AS spent_clicks
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` pps_bookings,
       UNNEST(pps_bookings.cpc_billings) cpc_billings
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS pps_cpc_clicks
         ON cpc_billings.uuid = pps_cpc_clicks.pps_item_uuid
  GROUP BY
    booking_id,
    click_date
),

final_cpc_clicks_count AS (
  SELECT
    final_cpc_bookings.global_entity_id,
    final_cpc_bookings.vendor_code,
    vendor_type.business_type_apac AS vendor_type,
    final_cpc_bookings.booking_id,
    final_cpc_bookings.cpc_month,
    final_cpc_bookings.date_live,
    final_cpc_bookings.click_price_local,
    final_cpc_bookings.initial_budget_local,
    IF(cpc_clicks.spent_clicks > final_cpc_bookings.budgeted_clicks, final_cpc_bookings.budgeted_clicks, cpc_clicks.spent_clicks) AS final_spent_clicks,
    CASE
      WHEN SAFE_MULTIPLY(IF(cpc_clicks.spent_clicks > final_cpc_bookings.budgeted_clicks, final_cpc_bookings.budgeted_clicks, cpc_clicks.spent_clicks), click_price_local) > initial_budget_local
      THEN CAST(initial_budget_local AS FLOAT64)
      ELSE CAST(SAFE_MULTIPLY(IF(cpc_clicks.spent_clicks > final_cpc_bookings.budgeted_clicks, final_cpc_bookings.budgeted_clicks, cpc_clicks.spent_clicks), click_price_local) AS FLOAT64)
    END AS cpc_revenue_local,
    SAFE_DIVIDE(
      CASE
        WHEN SAFE_MULTIPLY(IF(cpc_clicks.spent_clicks > final_cpc_bookings.budgeted_clicks, final_cpc_bookings.budgeted_clicks, cpc_clicks.spent_clicks), click_price_local) > initial_budget_local
        THEN CAST(initial_budget_local AS FLOAT64)
        ELSE CAST(SAFE_MULTIPLY(IF(cpc_clicks.spent_clicks > final_cpc_bookings.budgeted_clicks, final_cpc_bookings.budgeted_clicks, cpc_clicks.spent_clicks), click_price_local) AS FLOAT64)
      END,
      exchange_rate) AS cpc_revenue_eur
  FROM final_cpc_bookings
  LEFT JOIN cpc_clicks
         ON cpc_clicks.click_date = final_cpc_bookings.date_live
        AND cpc_clicks.booking_id = final_cpc_bookings.booking_id
  LEFT JOIN exchange_rate_daily
         ON final_cpc_bookings.date_live = exchange_rate_daily.fx_rate_date
        AND final_cpc_bookings.global_entity_id = exchange_rate_daily.global_entity_id
      left join `fulfillment-dwh-production.pandata_curated.pd_vendors` as vendors
             on final_cpc_bookings.global_entity_id = vendors.global_entity_id
            and final_cpc_bookings.vendor_code = vendors.vendor_code
      left join `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` as vendor_type
             on vendors.uuid = vendor_type.uuid
),

cpc_revenue_per_vendor AS (
  SELECT
    final_cpc_clicks_count.global_entity_id,
    cpc_month AS month,
    vendor_code,
    SUM(IFNULL(cpc_revenue_local,0)) AS cpc_revenue_local,
    SUM(IFNULL(cpc_revenue_eur,0)) AS cpc_revenue_eur
  FROM final_cpc_clicks_count
  WHERE final_cpc_clicks_count.global_entity_id LIKE 'FP_%'
    --AND final_cpc_clicks_count.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND cpc_month >= (SELECT start_date FROM start_month)
  GROUP BY global_entity_id, cpc_month, vendor_code
),

/*Getting CPP Revenue*/
cpp_bookings AS (
  SELECT
    pps_bookings.global_entity_id,
    pps_bookings.vendor_code,
    DATE(FORMAT_DATETIME("%Y-%m-%d", PARSE_DATETIME("%Y%m", CAST(year_month AS STRING)))) AS cpp_month,
    pps_bookings.type,
    pps_bookings.status,
    COUNT(DISTINCT uuid) AS booking_id,
    SUM(IFNULL(cpp_billing.price,0)) AS sold_price_local,
    SAFE_DIVIDE(
      SUM(IFNULL(cpp_billing.price,0)),
      MAX(exchange_rate)
    ) AS sold_price_eur
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` AS pps_bookings
  LEFT JOIN exchange_rate_daily
         ON DATE(pps_bookings.cpp_billing.created_at_utc) = exchange_rate_daily.fx_rate_date
        AND pps_bookings.global_entity_id = exchange_rate_daily.global_entity_id
  WHERE uuid IS NOT NULL
    AND billing_type = 'CPP'
    AND status <> 'cancelled'
    AND DATE(FORMAT_DATETIME("%Y-%m-%d", PARSE_DATETIME("%Y%m", CAST(year_month AS STRING)))) >= (SELECT start_date FROM start_month)
    AND (
          CASE
            WHEN pps_bookings.global_entity_id IN ('FP_BD', 'FP_PK', 'FP_SG', 'FP_MY', 'FP_JP') AND type = 'premium_placements'
            THEN cpp_billing.position <= 7
            WHEN pps_bookings.global_entity_id IN ('FP_HK', 'FP_PH') AND type = 'premium_placements'
            THEN cpp_billing.position <= 10
            WHEN pps_bookings.global_entity_id = 'FP_TH' AND type = 'premium_placements'
            THEN cpp_billing.position <= 15
            WHEN pps_bookings.global_entity_id = 'FP_TW' AND type = 'premium_placements'
            THEN cpp_billing.position <= 8
            WHEN pps_bookings.global_entity_id IN ('FP_LA', 'FP_KH', 'FP_MM') AND type = 'premium_placements'
            THEN cpp_billing.position <= 7
            WHEN pps_bookings.global_entity_id IN ('FP_LA', 'FP_KH', 'FP_MM') AND type = 'organic_placements'
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
    cpp_bookings.global_entity_id,
    cpp_month AS month,
    vendor_code,
    SUM(IFNULL(sold_price_local,0)) AS cpp_revenue_local,
    SUM(IFNULL(sold_price_eur,0)) AS cpp_revenue_eur
  FROM cpp_bookings
  GROUP BY
    global_entity_id,
    cpp_month,
    vendor_code
),

sf_opportunities_won AS (
/*Getting latest activated date based on salesforce onboarding*/
  SELECT
    global_entity_id,
    sf_account_id,
    business_type,
    close_date_local,
    ROW_NUMBER() over (PARTITION BY sf_account_id ORDER BY close_date_local DESC) = 1 AS is_latest_new_vendor_opportunity
  FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities`
  WHERE business_type IN ('New Business', 'Owner Change', 'Win Back', 'Legal Form Change')
    AND is_closed
    AND LOWER(type) LIKE '%contract%'
    AND LOWER(stage_name) LIKE '%closed won%'
  GROUP BY
    global_entity_id,
    sf_account_id,
    business_type,
    close_date_local
),

sf_activation_date AS (
  SELECT
    sf_accounts.global_entity_id,
    sf_accounts.vendor_code,
    DATE(sf_opportunities_won.close_date_local) AS activated_date_local
  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
  LEFT JOIN sf_opportunities_won
         ON sf_opportunities_won.global_entity_id = sf_accounts.global_entity_id
        AND sf_accounts.id = sf_opportunities_won.sf_account_id
        AND is_latest_new_vendor_opportunity
  WHERE NOT sf_accounts.is_marked_for_testing_training
),

vendor_activation_date AS (
SELECT
  pd_vendors.global_entity_id,
  pd_vendors.vendor_code,
  COALESCE(sf_activation_date.activated_date_local, DATE(pd_vendors.activated_at_local), first_valid_order) AS activation_date
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
LEFT JOIN sf_activation_date
       ON sf_activation_date.global_entity_id = pd_vendors.global_entity_id
      AND sf_activation_date.vendor_code = pd_vendors.vendor_code
LEFT JOIN (
  SELECT
    global_entity_id,
    vendor_code,
    MIN(created_date_local) AS first_valid_order
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  WHERE created_date_utc <= CURRENT_DATE
  AND is_valid_order
  AND is_gross_order
  AND NOT is_test_order
  GROUP BY 1,2
) orders
   ON orders.global_entity_id = pd_vendors.global_entity_id
  AND orders.vendor_code = pd_vendors.vendor_code
WHERE (pd_vendors.global_entity_id LIKE 'FP_%'
  --AND pd_vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG')
  )
  AND NOT is_test
GROUP BY
  pd_vendors.global_entity_id,
  pd_vendors.vendor_code,
  sf_activation_date.activated_date_local,
  pd_vendors.activated_at_local,
  first_valid_order
),

listing_fee_date AS (
  SELECT
    sf_additional_charges.global_entity_id,
    sf_additional_charges.product,
    sf_additional_charges.type,
    sf_accounts.vendor_code,
    dates.date,
    sf_accounts.status,
    invoice_frequency,
    ROW_NUMBER() OVER (PARTITION BY sf_additional_charges.global_entity_id, sf_accounts.vendor_code, dates.date ORDER BY DATE(additional_charges_c.created_date) DESC) = 1 AS is_latest_entry,
    TRUE AS is_listing_fee_vendor,
    SAFE_CAST(sf_additional_charges.total_amount_local AS FLOAT64) AS total_amount_local,
    SAFE_CAST(sf_additional_charges.discount_local AS FLOAT64) AS discount_local,
    SAFE_CAST(sf_additional_charges.listed_price_local AS FLOAT64) AS listed_price_local
  FROM `fulfillment-dwh-production.pandata_curated.sf_additional_charges` AS sf_additional_charges
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
         ON sf_additional_charges.sf_account_id = sf_accounts.id
  LEFT JOIN fulfillment-dwh-staging.pandata_raw_salesforce.additional_charges_c
         ON additional_charges_c.id = sf_additional_charges.id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` vv
         ON vv.global_entity_id = sf_additional_charges.global_entity_id
        AND sf_accounts.vendor_code = vv.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS dates
         ON dates.date BETWEEN sf_additional_charges.start_date_local AND COALESCE(sf_additional_charges.termination_date_local, CURRENT_DATE)
  WHERE (
          (sf_additional_charges.global_entity_id = 'FP_BD' AND product IN ('Platform Fee'))
          OR (sf_additional_charges.global_entity_id = 'FP_PK' AND product IN ('SIM Card Fee'))
          OR (sf_additional_charges.global_entity_id = 'FP_SG' AND product IN ('Platform Fee'))
          OR (sf_additional_charges.global_entity_id = 'FP_MY' AND product IN ('Service Fee'))
          OR (sf_additional_charges.global_entity_id = 'FP_TH' AND product IN ('Listing Fee - Monthly'))
          OR (sf_additional_charges.global_entity_id = 'FP_TW' AND product IN ('期租費'))
          OR (sf_additional_charges.global_entity_id = 'FP_HK' AND product IN ('Monthly Listing Fees'))
          OR (sf_additional_charges.global_entity_id = 'FP_PH' AND product IN ('Platform Fees'))
        )
    --AND sf_additional_charges.status = "Active"
    AND NOT sf_additional_charges.is_deleted
    AND sf_additional_charges.start_date_local <= dates.date
    AND COALESCE(sf_additional_charges.termination_date_local, CURRENT_DATE) >= dates.date
    AND sf_additional_charges.type = 'Recurring Fee'
    AND sf_accounts.vendor_code IS NOT NULL
    AND dates.date >= (SELECT start_date FROM start_month)
    AND dates.date <= CURRENT_DATE
    AND ((sf_additional_charges.global_entity_id = 'FP_PH' AND LOWER(vv.name) NOT LIKE '%7%eleven%' AND LOWER(vv.name) NOT LIKE '%crunch%time%')
        OR sf_additional_charges.global_entity_id != 'FP_PH')
  GROUP BY 1,2,3,4,5,6,7, additional_charges_c.created_date, sf_additional_charges.total_amount_local, sf_additional_charges.discount_local, sf_additional_charges.listed_price_local
  ORDER BY 7 DESC
),

listing_fee_month_group AS (
  SELECT
    global_entity_id,
    DATE_TRUNC(DATE(date), MONTH) AS listing_fee_month,
    vendor_code,
    is_listing_fee_vendor,
    status,
    MAX(total_amount_local) AS total_amount_local,
    CASE
      WHEN global_entity_id = 'FP_PH' AND MAX(discount_local) > 1000
      THEN 1000
      ELSE MAX(discount_local)
    END AS discount_local,
    CASE
      WHEN global_entity_id = 'FP_PH'
      THEN 1000
      ELSE MAX(listed_price_local)
    END AS listed_price_local
  FROM listing_fee_date
  WHERE is_latest_entry
  GROUP BY 1,2,3,4,5
),

listing_fees_month AS (
  SELECT
  listing_fee_month_group.global_entity_id,
  listing_fee_month,
  listing_fee_month_group.vendor_code,
  is_listing_fee_vendor,
  CASE
    WHEN listing_fee_month_group.global_entity_id = 'FP_PH'
    THEN SAFE_SUBTRACT(listed_price_local, discount_local)
    WHEN listing_fee_month_group.global_entity_id = 'FP_TW'
    THEN total_amount_local*2
    WHEN listing_fee_month_group.global_entity_id = 'FP_SG' AND total_amount_local >= 20
    THEN total_amount_local
    WHEN listing_fee_month_group.global_entity_id = 'FP_TH' AND total_amount_local = 99
    THEN 99
    WHEN listing_fee_month_group.global_entity_id NOT IN ('FP_SG','FP_TH','FP_TW','FP_PH')
    THEN total_amount_local
  END AS total_amount_local,
  CAST(exchange_rate AS FLOAT64) AS average_exchange_rate
  FROM listing_fee_month_group
  LEFT JOIN exchange_rate_monthly
         ON listing_fee_month_group.listing_fee_month = exchange_rate_monthly.month
        AND listing_fee_month_group.global_entity_id = exchange_rate_monthly.global_entity_id
  GROUP BY 1,2,3,4,5,6
),

monthly_orders AS (
  SELECT
    pd_orders.global_entity_id,
    DATE_TRUNC(DATE(pd_orders.created_date_local), MONTH) AS order_month,
    pd_orders.vendor_code,
    COUNT(DISTINCT CASE
    WHEN pd_orders.is_valid_order AND (pd_orders.products_total_local IS NOT NULL OR pd_orders.products_total_local > 0)
    THEN pd_orders.uuid END
    ) AS total_valid_orders,
    SUM(IF(pd_orders.is_valid_order AND pd_orders.expedition_type = 'delivery', accounting.gfv_local, 0)) AS successful_gfv_delivery,
    SUM(IF(pd_orders.is_valid_order, pd_orders.products_total_local, 0)) AS successful_gfv
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
         ON accounting.uuid = pd_orders.uuid
        AND accounting.created_date_utc <= CURRENT_DATE
  WHERE pd_orders.created_date_utc <= CURRENT_DATE
    AND pd_orders.is_valid_order
    AND pd_orders.is_gross_order
    AND NOT is_test_order
    AND DATE(pd_orders.created_date_local) >= (SELECT start_date FROM start_month)
  GROUP BY 1,2,3
),

listing_fee AS (
  SELECT
    listing_fees_month.global_entity_id,
    listing_fee_month,
    listing_fees_month.vendor_code,
    listing_fees_month.is_listing_fee_vendor,
    listing_fees_month.total_amount_local,
    CASE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND (
           (listing_fees_month.global_entity_id = 'FP_BD' AND successful_gfv >= 500)
           OR (listing_fees_month.global_entity_id = 'FP_PK' AND total_valid_orders >= 4)
           OR (listing_fees_month.global_entity_id = 'FP_SG' AND successful_gfv >= 500)
           OR (listing_fees_month.global_entity_id = 'FP_TH' AND successful_gfv > 1000)

           OR (listing_fees_month.global_entity_id = 'FP_PH' AND successful_gfv_delivery >= 4000)
           )
      THEN TRUE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 1000 AND  successful_gfv < 2000
      THEN TRUE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 2000 AND  successful_gfv < 5000
      THEN TRUE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 5000
      THEN TRUE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND is_daily_active AND listing_fees_month.global_entity_id IN ('FP_HK','FP_TW')
      THEN TRUE
    END AS is_listing_fee_charged,
    CASE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 1000 AND  successful_gfv < 2000
      THEN TRUE
    END AS is_listing_fee_charged_tier_one,
    CASE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 2000 AND  successful_gfv < 5000
      THEN TRUE
    END AS is_listing_fee_charged_tier_two,
    CASE
      WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
           AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 5000
      THEN TRUE
    END AS is_listing_fee_charged_tier_three,
    SUM(
      CASE
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND (
             (listing_fees_month.global_entity_id = 'FP_BD' AND successful_gfv >= 500)
             OR (listing_fees_month.global_entity_id = 'FP_PK' AND total_valid_orders >= 4)
             OR (listing_fees_month.global_entity_id = 'FP_SG' AND successful_gfv >= 500)
             OR (listing_fees_month.global_entity_id = 'FP_TH' AND successful_gfv > 1000)

             OR (listing_fees_month.global_entity_id = 'FP_PH' AND successful_gfv_delivery >= 4000)
             )
        THEN CAST(total_amount_local AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 1000 AND  successful_gfv < 2000
        THEN CAST(20 AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 2000 AND  successful_gfv < 5000
        THEN CAST(35 AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 5000
        THEN CAST(75 AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND is_daily_active AND listing_fees_month.global_entity_id IN ('FP_HK','FP_TW')
        THEN CAST(total_amount_local AS FLOAT64)
      END
    ) AS listing_fee_revenue_local,
    SUM(
      CASE
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND (
             (listing_fees_month.global_entity_id = 'FP_BD' AND successful_gfv >= 500)
             OR (listing_fees_month.global_entity_id = 'FP_PK' AND total_valid_orders >= 4)
             OR (listing_fees_month.global_entity_id = 'FP_SG' AND successful_gfv >= 500)
             OR (listing_fees_month.global_entity_id = 'FP_TH' AND successful_gfv > 1000)

             OR (listing_fees_month.global_entity_id = 'FP_PH' AND successful_gfv_delivery >= 4000)
             )
        THEN CAST(SAFE_DIVIDE(total_amount_local, average_exchange_rate) AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 1000 AND  successful_gfv < 2000
        THEN CAST(SAFE_DIVIDE(20, average_exchange_rate) AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 2000 AND  successful_gfv < 5000
        THEN CAST(SAFE_DIVIDE(35, average_exchange_rate) AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND listing_fees_month.global_entity_id = 'FP_MY' AND successful_gfv >= 5000
        THEN CAST(SAFE_DIVIDE(75, average_exchange_rate) AS FLOAT64)
        WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
             AND is_daily_active AND listing_fees_month.global_entity_id IN ('FP_HK','FP_TW')
        THEN CAST(SAFE_DIVIDE(total_amount_local, average_exchange_rate) AS FLOAT64)
      END
    ) AS listing_fee_revenue_eur
  FROM listing_fees_month
  LEFT JOIN monthly_orders
         ON listing_fees_month.global_entity_id = monthly_orders.global_entity_id
        AND listing_fees_month.listing_fee_month = monthly_orders.order_month
        AND listing_fees_month.vendor_code = monthly_orders.vendor_code
  LEFT JOIN (
    SELECT
      global_entity_id,
      DATE_TRUNC(date, MONTH) AS month,
      vendor_code,
      MAX(is_daily_active) AS is_daily_active
    FROM active_status
    GROUP BY 1,2,3
  ) active_status
         ON active_status.global_entity_id = listing_fees_month.global_entity_id
        AND active_status.month = listing_fees_month.listing_fee_month
        AND active_status.vendor_code = listing_fees_month.vendor_code
  GROUP BY 1,2,3,4,5,6,7,8,9
),

salesforce_tickets_sales_portal AS (
  SELECT
    sf_tickets.global_entity_id,
    sf_tickets.order_code,
    TRUE AS has_order_item_issues
  FROM `fulfillment-dwh-production.pandata_curated.gcc_salesforce_cases` sf_tickets
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` shared_countries
         ON shared_countries.global_entity_id = sf_tickets.global_entity_id
        AND shared_countries.company_name = 'Foodpanda'
  WHERE sf_tickets.customer_contact_reason_1 IN ('Post-Delivery','Live Order Process')
    AND sf_tickets.customer_contact_reason_2 IN ('Wrong / Missing item', 'Food issue','Order status',NULL,'Wrong order / Never arrived')
    AND sf_tickets.customer_contact_reason_3 IN ('Wrong item', 'Missing item','Item unavailable for pickup','Cooking instructions were not followed','Issue with item replacement process','Wrong order')
    AND sf_tickets.order_code IS NOT NULL
    AND REGEXP_CONTAINS(sf_tickets.order_code,'^[0-9a-z]{{4}}-[0-9a-z]{{4}}$')
    AND DATE(sf_tickets.created_at_utc) >= (SELECT start_date FROM start_month)
    AND sf_tickets.created_date_utc <= CURRENT_DATE
  GROUP BY 1,2
),

salesforce_tickets_service_portal AS (
  SELECT
    sf_tickets.global_entity_id,
    sf_tickets.order_number AS order_code,
    TRUE AS has_order_item_issues
  FROM `fulfillment-dwh-production.pandata_curated.sf_cases` sf_tickets
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` shared_countries
         ON shared_countries.global_entity_id = sf_tickets.global_entity_id
        AND shared_countries.company_name = 'Foodpanda'
  WHERE sf_tickets.ccr1 IN ('Post-Delivery','Live Order Process')
    AND sf_tickets.ccr2 IN ('Wrong / Missing item', 'Food issue','Order status',NULL,'Wrong order / Never arrived')
    AND sf_tickets.ccr3 IN ('Wrong item', 'Missing item','Item unavailable for pickup','Cooking instructions were not followed','Issue with item replacement process','Wrong order')
    AND REGEXP_CONTAINS(sf_tickets.order_number,'^[0-9a-z]{{4}}-[0-9a-z]{{4}}$')
    AND DATE(sf_tickets.created_at_utc) >= (SELECT start_date FROM start_month)
  GROUP BY 1,2
),

salesforce_tickets_all_portals AS (
  SELECT
    global_entity_id,
    order_code,
    has_order_item_issues
  FROM salesforce_tickets_sales_portal

  UNION ALL

  SELECT
    global_entity_id,
    order_code,
    has_order_item_issues
  FROM salesforce_tickets_service_portal
),

salesforce_tickets AS (
  SELECT
    global_entity_id,
    order_code,
    has_order_item_issues
  FROM salesforce_tickets_all_portals
  GROUP BY 1,2,3
),

closed_hours_final AS (
  SELECT
    restaurant_offline_report.global_entity_id,
    DATE_TRUNC(report_date, MONTH) AS offline_date,
    restaurant_offline_report.vendor_code,
    SUM(IF(
      ((IFNULL(total_unavailable_seconds, 0))+IFNULL(total_special_day_closed_seconds, 0)) <= IFNULL(total_open_seconds, 0),
      ((IFNULL(total_unavailable_seconds, 0))+IFNULL(total_special_day_closed_seconds, 0)),
      IFNULL(total_open_seconds, 0)
      )) as total_closed_hours,
    SAFE_SUBTRACT(SUM(IFNULL(total_open_seconds, 0)),60) as total_open_hours,
    SAFE_DIVIDE(
    SAFE_SUBTRACT(
      SUM(IFNULL(total_open_seconds, 0)),
      SUM(IF(
      ((IFNULL(total_unavailable_seconds, 0))+IFNULL(total_special_day_closed_seconds, 0)) <= IFNULL(total_open_seconds, 0),
      ((IFNULL(total_unavailable_seconds, 0))+IFNULL(total_special_day_closed_seconds, 0)),
      IFNULL(total_open_seconds, 0)
      ))),
    60) AS actual_open_hours
  FROM `fulfillment-dwh-production.pandata_report.vendor_offline` restaurant_offline_report
  WHERE report_date >= (SELECT start_date FROM start_month)
  GROUP BY 1,2,3
),

o AS (
  SELECT
    o.global_entity_id,
    o.country_name AS country,
    o.vendor_code AS vendor_code,
    DATE_TRUNC(DATE(o.created_at_local), MONTH) AS order_date,
    COUNT(DISTINCT o.vendor_code) AS no_of_day_w_order,
    COUNT(DISTINCT o.id) AS no_of_all_order,
    COUNT(DISTINCT IF(o.is_valid_order, o.code, NULL)) AS no_of_valid_order,
    COUNT(DISTINCT IF(o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery', o.code, NULL)) AS od_no_of_valid_order,
    COUNT(DISTINCT IF(NOT o.is_valid_order, o.code, NULL)) AS no_of_invalid_order,
    COUNT(DISTINCT IF(o.is_failed_order, o.code, NULL)) AS no_of_failed_order,
    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND NOT pandora_pd_orders_agg_jkr_deals.is_joker_used AND NOT pandora_pd_orders_agg_jkr_deals.is_voucher_used AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used
          ,pd_orders_agg_accounting.gfv_eur,0)) AS gfv_organic_no_deal,
    SUM(IF(o.is_valid_order AND NOT o.is_failed_order, 
    COALESCE(oc.invoiced_commission_eur,oc.estimated_commission_eur)
          ,0)) AS commission_base_x_cr,
    SUM(IF( o.is_valid_order AND NOT o.is_failed_order
          , COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local)
          ,0)) AS commission_local,
    
    SUM(IF(o.is_valid_order AND NOT o.is_failed_order, 
    COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local), 0)
    ) AS commission_base_local,

    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
          , COALESCE(oc.invoiced_commission_eur,oc.estimated_commission_eur)
          ,0)) AS od_commission_base_x_cr,
    SUM(IF( o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
          , COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local)
          ,0)) AS od_commission_local,
    
    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery', 
    COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local), 0)
    ) AS od_commission_base_local,

    /*Deals & Vouchers*/
    COUNT(DISTINCT IF(o.is_valid_order AND NOT o.is_failed_order AND (
                (pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio = 0)
                OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio = 0)
                ), o.id, NULL)) AS no_of_orders_with_full_vf_deals,

    COUNT(DISTINCT IF(o.is_valid_order AND NOT o.is_failed_order AND (
                (pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND do.discount.attributions_foodpanda_ratio != 0)
                OR (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio != 0)), o.id, NULL)) AS no_of_orders_with_cofunded_vf_deals,

    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100,
    do.discount.vendor_subsidized_value_eur, NULL)) AS vf_discount_deal_value_eur,

    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100,
    pd_orders_agg_vouchers.voucher.vendor_subsidized_value_eur, NULL)) AS vf_voucher_deal_value_eur,

    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100,
    do.discount.vendor_subsidized_value_local, NULL)) AS vf_discount_deal_value_local,

    SUM(IF(o.is_valid_order AND NOT o.is_failed_order AND pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100,
    pd_orders_agg_vouchers.voucher.vendor_subsidized_value_local, NULL)) AS vf_voucher_deal_value_local,

    SAFE_DIVIDE(
    SUM(IF(o.is_valid_order, SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local)),0)),
    SUM(IF(o.is_valid_order AND SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local)) IS NOT NULL,1,0))
    ) AS commission_combined,
    
    SAFE_DIVIDE(
      SUM(IF(o.is_valid_order AND o.expedition_type = 'delivery', SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local))
          ,0)),
      SUM(IF(o.is_valid_order AND o.expedition_type='delivery' AND SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local)) IS NOT NULL
              ,1,0))) AS commission_delivery,

    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local))
          ELSE NULL
        END) AS od_commission_percentage_total,


    CASE
      WHEN SUM(CASE WHEN o.is_valid_order AND o.expedition_type='pickup' THEN 1 ELSE 0 END) =0
      THEN NULL
      ELSE SAFE_DIVIDE(SUM(CASE WHEN o.is_valid_order AND o.expedition_type = 'pickup' THEN SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local)) ELSE NULL END),
                       SUM(CASE WHEN o.is_valid_order AND o.expedition_type='pickup' AND SAFE_DIVIDE(COALESCE(oc.invoiced_commission_local,oc.estimated_commission_local), COALESCE(oc.invoiced_commission_base_local,oc.estimated_commission_base_local)) IS NOT NULL THEN 1 ELSE NULL END))
    END AS commission_pickup,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.expedition_type='pickup'
          THEN COALESCE(oc.invoiced_commission_eur,oc.estimated_commission_eur)
          ELSE NULL
        END) AS pickup_commission_revenue,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.expedition_type='pickup'
          THEN COALESCE(oc.invoiced_commission_base_eur,oc.estimated_commission_base_eur)
          ELSE NULL
        END) AS pickup_commission_base,
    
    SUM(IF(o.is_valid_order, pd_orders_agg_accounting.gmv_eur, 0)) AS gmv_eur,
    SUM(IF(o.is_valid_order AND o.expedition_type = 'delivery', pd_orders_agg_accounting.gmv_eur, 0)) AS gmv_eur_delivery,
    SUM(IF(o.is_valid_order AND o.expedition_type = 'pickup', pd_orders_agg_accounting.gmv_eur, 0)) AS gmv_eur_pickup,
    
    SUM(IF(o.is_valid_order, pd_orders_agg_accounting.gmv_local, 0)) AS gmv_local,
    SUM(IF(o.is_valid_order AND o.expedition_type = 'delivery', pd_orders_agg_accounting.gmv_local, 0)) AS gmv_local_delivery,
    SUM(IF(o.is_valid_order AND o.expedition_type = 'pickup', pd_orders_agg_accounting.gmv_local, 0)) AS gmv_local_pickup,

    SUM(pd_orders_agg_accounting.gfv_eur) AS gfv_all,
    SUM(CASE
          WHEN o.is_valid_order
          THEN pd_orders_agg_accounting.gfv_eur
          ELSE 0
        END) AS gfv_valid,
    SUM(CASE
          WHEN o.is_valid_order
          THEN pd_orders_agg_accounting.gfv_local
          ELSE 0
        END) AS gfv_valid_local,
    SUM(CASE
          WHEN o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN pd_orders_agg_accounting.gfv_eur
          ELSE 0
        END) AS od_gfv_valid,
    SUM(CASE
          WHEN o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN pd_orders_agg_accounting.gfv_local
          ELSE 0
        END) AS od_gfv_valid_local,
    SUM(CASE
          WHEN o.is_valid_order
          THEN 1
          ELSE 0
        END) AS no_order_success,
        
-----------------------------------------------------------------------------------------------------------------------------
    /*Order Accuracy Metric*/
    COUNT(DISTINCT CASE
                     WHEN salesforce_tickets.has_order_item_issues
                     THEN o.id
                   END
         ) AS orders_with_item_issues,

    SUM(CASE
           WHEN o.is_valid_order AND do.pd_discount_uuid IS NOT NULL AND do.discount.attributions_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NULL AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur)*SAFE_DIVIDE(do.discount.attributions_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND do.pd_discount_uuid IS NOT NULL AND do.discount.attributions_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur)*SAFE_DIVIDE(do.discount.attributions_foodpanda_ratio, 100),0) + COALESCE(pd_orders_agg_vouchers.voucher.value_eur*SAFE_DIVIDE(pd_orders_agg_vouchers.voucher.current_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(pd_orders_agg_vouchers.voucher.value_eur*SAFE_DIVIDE(pd_orders_agg_vouchers.voucher.current_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio = 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur)*SAFE_DIVIDE(do.discount.attributions_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio = 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(pd_orders_agg_vouchers.voucher.value_eur*SAFE_DIVIDE(pd_orders_agg_vouchers.voucher.current_foodpanda_ratio, 100),0)
         END) AS od_fully_fp_deal_value_eur,

    SUM(CASE
           WHEN o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND NOT pandora_pd_orders_agg_jkr_deals.is_voucher_used
           THEN COALESCE(SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur)*SAFE_DIVIDE(do.discount.attributions_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100
           THEN COALESCE(SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur)*SAFE_DIVIDE(do.discount.attributions_foodpanda_ratio, 100),0) + COALESCE(pd_orders_agg_vouchers.voucher.value_eur*SAFE_DIVIDE(pd_orders_agg_vouchers.voucher.current_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100
           THEN COALESCE(pd_orders_agg_vouchers.voucher.value_eur*SAFE_DIVIDE(pd_orders_agg_vouchers.voucher.current_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio < 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio = 100
           THEN COALESCE(SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur)*SAFE_DIVIDE(do.discount.attributions_foodpanda_ratio, 100),0)

           WHEN o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.is_discount_used AND do.discount.attributions_foodpanda_ratio = 100 AND pd_orders_agg_vouchers.pd_voucher_uuid IS NOT NULL AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio < 100
           THEN COALESCE(pd_orders_agg_vouchers.voucher.value_eur*SAFE_DIVIDE(pd_orders_agg_vouchers.voucher.current_foodpanda_ratio, 100),0)
         END) AS fully_fp_deal_value_eur,

    SUM(IF(o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.expedition_type = 'pickup', pd_orders_agg_accounting.gfv_eur, 0)) AS gfv_pickup,
    COUNT(DISTINCT IF(o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.expedition_type = 'pickup', o.code, NULL)) AS no_order_pickup,
    SUM(IF(o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.expedition_type = 'delivery', pd_orders_agg_accounting.gfv_eur, 0)) AS gfv_b2c,
    COUNT(DISTINCT IF(o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.expedition_type = 'delivery', o.code, NULL)) AS no_order_b2c,
    SUM(IF(o.is_valid_order AND pd_orders_agg_cp_orders.corporate_order.uuid IS NOT NULL, pd_orders_agg_accounting.gfv_eur, 0)) AS gfv_corp,
    COUNT(DISTINCT IF(o.is_valid_order AND pd_orders_agg_cp_orders.corporate_order.uuid IS NOT NULL, o.code, NULL)) AS no_order_corp,
    COUNT(DISTINCT IF(o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.expedition_type = 'delivery' AND o.is_own_delivery, o.code, NULL)) AS no_order_foodpanda_delivery,
    COUNT(DISTINCT IF(o.is_valid_order AND pandora_pd_orders_agg_jkr_deals.expedition_type = 'delivery' AND NOT o.is_own_delivery, o.code, NULL)) AS no_order_vendor_delivery,
    SUM(CASE
          WHEN o.is_valid_order
          THEN SAFE_DIVIDE(o.total_value_local, fx_rates.fx_rate_eur)
          ELSE 0
        END) AS total_value,
    SUM(CASE WHEN pd_customers_agg_orders.first_order_valid_at_utc = o.ordered_at_utc THEN 1 ELSE 0 END) AS first_valid_order_with_foodpanda,
    SUM(CASE WHEN pandora_pd_orders_agg_jkr_deals.is_first_valid_order_with_this_chain THEN 1 ELSE 0 END) AS first_valid_order_with_this_chain,
    SUM(CASE WHEN pandora_pd_orders_agg_jkr_deals.is_first_valid_order_with_this_vendor THEN 1 ELSE 0 END) AS first_valid_order_with_this_vendor,
    SUM(CASE WHEN o.is_failed_order_vendor THEN 1 ELSE 0 END) AS failed_order_vendor,
    SUM(CASE WHEN o.is_failed_order_customer THEN 1 ELSE 0 END) AS failed_order_customer,
    SUM(CASE WHEN o.is_failed_order_foodpanda THEN 1 ELSE 0 END) AS failed_order_foodpanda,
    SUM(CASE WHEN o.is_valid_order THEN 0/*To deprecate*/ ELSE 0 END) AS service_fee,
    SUM(CASE
          WHEN o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN 0/*To deprecate*/
          ELSE 0
        END) AS od_service_fee,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND NOT pandora_pd_orders_agg_jkr_deals.is_voucher_used AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND ((pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.type != 'delivery_fee') AND (pandora_pd_orders_agg_jkr_deals.is_discount_used AND discount_type != 'free-delivery'))
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND (pandora_pd_orders_agg_jkr_deals.is_discount_used AND discount_type != 'free-delivery') AND NOT pandora_pd_orders_agg_jkr_deals.is_voucher_used
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.type != 'delivery_fee') AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) < delivery_fee_eur)
          THEN SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur)
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local = 0 OR payable_total_local IS NULL)
          THEN 0
          WHEN o.is_valid_order AND o.is_own_delivery
               AND ((pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.type = 'delivery_fee') OR (pandora_pd_orders_agg_jkr_deals.is_discount_used AND discount_type = 'free-delivery'))
          THEN 0
          ELSE 0
        END) AS delivery_fee,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND NOT pandora_pd_orders_agg_jkr_deals.is_voucher_used AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used AND o.expedition_type = 'delivery'
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND ((pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.type != 'delivery_fee') AND (pandora_pd_orders_agg_jkr_deals.is_discount_used AND discount_type != 'free-delivery')) AND o.expedition_type = 'delivery'
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND (pandora_pd_orders_agg_jkr_deals.is_discount_used AND discount_type != 'free-delivery') AND NOT pandora_pd_orders_agg_jkr_deals.is_voucher_used AND o.expedition_type = 'delivery'
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) >= delivery_fee_eur)
               AND (pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.type != 'delivery_fee') AND NOT pandora_pd_orders_agg_jkr_deals.is_discount_used AND o.expedition_type = 'delivery'
          THEN delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local != 0 AND SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur) < delivery_fee_eur) AND o.expedition_type = 'delivery'
          THEN SAFE_DIVIDE(payable_total_local, fx_rates.fx_rate_eur)
          WHEN o.is_valid_order AND o.is_own_delivery AND (payable_total_local = 0 OR payable_total_local IS NULL) AND o.expedition_type = 'delivery'
          THEN 0
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND ((pandora_pd_orders_agg_jkr_deals.is_voucher_used AND pd_orders_agg_vouchers.voucher.type = 'delivery_fee') OR (pandora_pd_orders_agg_jkr_deals.is_discount_used AND discount_type = 'free-delivery'))
          THEN 0
          ELSE 0
        END) AS od_delivery_fee,

    SUM(CASE WHEN o.is_valid_order THEN COALESCE(oc.invoiced_commission_eur,oc.estimated_commission_eur) ELSE 0 END) AS commission,
    SUM(CASE WHEN o.is_valid_order THEN pd_orders_agg_vouchers.voucher.value_eur ELSE 0 END) AS voucher_value,
    SUM(CASE WHEN o.is_valid_order THEN SAFE_DIVIDE(do.discount.discount_amount_local, fx_rates.fx_rate_eur) ELSE 0 END) AS discount_value_total,
    SUM(CASE WHEN o.is_valid_order THEN CAST(pandora_pd_orders_agg_jkr_deals.joker_fee_eur AS FLOAT64) ELSE 0 END) AS joker_fee_amount_eur,
    SUM(CASE WHEN o.is_valid_order THEN CAST(pandora_pd_orders_agg_jkr_deals.joker_fee_local AS FLOAT64) ELSE 0 END) AS joker_fee_amount_local,
    SUM(CASE WHEN o.is_valid_order THEN rider_tip_eur ELSE 0 END) AS rider_tip,
    SUM(CASE WHEN pd_orders_agg_cp_orders.corporate_order.uuid IS NULL THEN IF(o.is_gross_order, 1, 0) ELSE NULL END) AS decline_a,
    COUNT(CASE
            WHEN o.is_gross_order AND o.is_failed_order_vendor AND pd_orders_agg_cp_orders.corporate_order.uuid IS NULL
            THEN o.id
            ELSE NULL
          END) AS decline_b,
    COUNT(CASE WHEN lg.order_status = 'completed' THEN lg.id ELSE NULL END) AS late_a,
    COUNT(CASE WHEN lg.order_status = 'completed' AND lg.vendor_late_in_seconds >= 300 THEN lg.id ELSE NULL END) AS late_b,
    SUM(CASE WHEN vendor_late_in_seconds >= 300 THEN 1 ELSE 0 END) AS vendor_lateness_5min_a,
    SUM(CASE WHEN vendor_late_in_seconds >= 300 THEN 1 ELSE 1 END) AS vendor_lateness_5min_b,
    SUM(CASE WHEN vendor_late_in_seconds >= 900 THEN 1 ELSE 0 END) AS vendor_lateness_15min_a,
    SUM(CASE WHEN vendor_late_in_seconds >= 900 THEN 1 ELSE 1 END) AS vendor_lateness_15min_b,
    COUNT(lg.vendor_late_in_seconds) AS vl_base,
    SUM(CASE
          WHEN o.is_valid_order
          THEN SAFE_DIVIDE(estimated_prep_time_in_seconds, 60)
          ELSE 0
        END) AS sum_ept_min,
    COUNT(lg.estimated_prep_time_in_seconds) AS ept_base,
    SUM(CASE
          WHEN o.is_valid_order
          THEN SAFE_DIVIDE(assumed_actual_preparation_time_in_seconds, 60)
          ELSE 0
        END) AS sum_apt_min,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND assumed_actual_preparation_time_in_seconds IS NOT NULL
                     THEN lg.code
                   END
         ) AS apt_base,
    SUM(CASE
          WHEN o.is_valid_order
          THEN SAFE_DIVIDE(promised_delivery_time_in_seconds, 60)
          ELSE 0
        END) AS sum_pdt_min,
    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order
                            AND actual_delivery_time_in_seconds IS NOT NULL 
                            AND NOT o.is_preorder
                     THEN lg.code
                   END
         ) AS adt_base,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_preorder
          THEN SAFE_DIVIDE(actual_delivery_time_in_seconds, 60)
          ELSE 0
        END) AS sum_adt_min,

    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order AND NOT o.is_preorder
                          AND actual_delivery_time_in_seconds IS NOT NULL
                          AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 30
                     THEN lg.code
                   END
         ) AS adt_base_30,
    SUM(CASE
          WHEN o.is_valid_order
                AND actual_delivery_time_in_seconds IS NOT NULL AND NOT o.is_preorder
                AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 30
          THEN SAFE_DIVIDE(actual_delivery_time_in_seconds, 60)
          ELSE 0
        END) AS sum_adt_min_30,


    COUNT(DISTINCT CASE
                     WHEN o.is_valid_order
                AND actual_delivery_time_in_seconds IS NOT NULL AND NOT o.is_preorder
                AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 40
                     THEN lg.code
                   END
         ) AS adt_base_40,
    SUM(CASE
          WHEN o.is_valid_order
                AND actual_delivery_time_in_seconds IS NOT NULL AND NOT o.is_preorder
                AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 40
          THEN SAFE_DIVIDE(actual_delivery_time_in_seconds, 60)
          ELSE 0
        END) AS sum_adt_min_40,
    /*Acceptance Time Metrics*/
    SUM(SAFE_DIVIDE(vendor_confirmation_time_in_seconds, 60)) AS sum_vendor_confirmation_min,
    COUNT(
           CASE
             WHEN vendor_confirmation_time_in_seconds IS NOT NULL
             THEN 1
             ELSE 0
           END
         ) AS vendor_confirmation_base,
    SUM(CASE
          WHEN vendor_confirmation_time_in_seconds IS NOT NULL AND is_failed_order_vendor
          THEN SAFE_DIVIDE(vendor_confirmation_time_in_seconds, 60)
        END
       ) AS sum_vendor_cancelled_time_min,
    COUNT(
      CASE
         WHEN vendor_confirmation_time_in_seconds IS NOT NULL AND is_failed_order_vendor
         THEN 1
         ELSE 0
       END
     ) AS vendor_cancelled_time_base
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` pd_orders_agg_accounting
         ON pd_orders_agg_accounting.uuid = o.uuid
        AND pd_orders_agg_accounting.created_date_utc <= CURRENT_DATE
      --AND pd_orders_agg_accounting.created_date_utc >= (SELECT start_date FROM start_month)
  cross join unnest(accounting) as accounting
  LEFT JOIN (
    SELECT
     *
    FROM `fulfillment-dwh-production.pandata_curated.lg_orders` lg
    WHERE lg.created_date_utc >= (SELECT start_date FROM start_month)
      AND lg.created_date_utc <= CURRENT_DATE
  ) lg
         ON o.global_entity_id = lg.global_entity_id
        AND o.code = lg.code
  LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` AS pandora_pd_orders_agg_jkr_deals
         ON o.global_entity_id = pandora_pd_orders_agg_jkr_deals.global_entity_id
        AND o.uuid = pandora_pd_orders_agg_jkr_deals.uuid
        AND pandora_pd_orders_agg_jkr_deals.created_date_local <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_report.order_commissions` oc
         ON o.global_entity_id = oc.global_entity_id
        AND o.code = oc.order_code
        AND oc.order_created_date_utc <= CURRENT_DATE
  LEFT JOIN salesforce_tickets
         ON o.global_entity_id = salesforce_tickets.global_entity_id
        AND o.code = salesforce_tickets.order_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_status_flows` AS orders_agg_status_flows
         ON o.global_entity_id = orders_agg_status_flows.global_entity_id
        AND o.uuid = orders_agg_status_flows.uuid
        AND orders_agg_status_flows.created_date_utc <= CURRENT_DATE
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_cp_orders` pd_orders_agg_cp_orders
         ON pd_orders_agg_cp_orders.uuid = o.uuid
        AND pd_orders_agg_cp_orders.created_date_utc <= CURRENT_DATE
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts do
         on do.global_entity_id = o.global_entity_id
        AND do.uuid = o.uuid
        AND do.created_date_utc <= CURRENT_DATE
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers pd_orders_agg_vouchers
         ON pd_orders_agg_vouchers.uuid = o.uuid
        AND pd_orders_agg_vouchers.created_date_utc <= CURRENT_DATE
  LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_discounts discount
         ON do.pd_discount_uuid = discount.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` countries
         ON o.global_entity_id = countries.global_entity_id
  LEFT JOIN (
    SELECT
     *
    FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates`
    WHERE fx_rate_date <= CURRENT_DATE
  ) fx_rates
         ON fx_rates.currency_code_iso = countries.currency_code_iso
        AND fx_rates.fx_rate_date = DATE(o.ordered_at_local)
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_customers_agg_orders` pd_customers_agg_orders
         ON pd_customers_agg_orders.uuid = o.pd_customer_uuid
  WHERE o.created_date_utc <= CURRENT_DATE
    AND DATE(o.created_date_local) >= (SELECT start_date FROM start_month)
    AND NOT o.is_test_order
    AND o.is_gross_order
  GROUP BY 1,2,3,4
)

SELECT
  dv.*,
  vendor_activation_date.* EXCEPT(global_entity_id, vendor_code),
  is_monthly_active,
  o.* EXCEPT(global_entity_id, country, vendor_code),
  closed_hours_final.* EXCEPT(global_entity_id, vendor_code, offline_date),
  vs.* EXCEPT(global_entity_id, country, month, vendor_code),
  rrate.* EXCEPT(global_entity_id, order_date_rr, vendor_code_rr),
  cpp_revenue_per_vendor.* EXCEPT(global_entity_id, vendor_code, month),
  cpc_revenue_per_vendor.* EXCEPT(global_entity_id, vendor_code, month),
  listing_fee.* EXCEPT(global_entity_id, vendor_code, listing_fee_month),
  cpo.cost_per_order_eur,
  cpo.cost_per_order_eur_delivery,
  cpo.cost_per_order_eur_cost_of_sales,
  cpo.revenue_per_order_eur,
  exchange_rate_monthly.exchange_rate
FROM ve AS dv

LEFT JOIN vendor_activation_date
       ON vendor_activation_date.vendor_code = dv.vendor_code
      AND vendor_activation_date.global_entity_id = dv.global_entity_id
      
LEFT JOIN o
       ON o.vendor_code = dv.vendor_code
      AND o.global_entity_id = dv.global_entity_id

LEFT JOIN closed_hours_final
       ON dv.vendor_code = closed_hours_final.vendor_code
      AND dv.global_entity_id = closed_hours_final.global_entity_id
      AND closed_hours_final.offline_date = o.order_date

LEFT JOIN vendor_sessions vs
       ON dv.vendor_code = vs.vendor_code
      AND vs.month = o.order_date
      AND vs.global_entity_id = dv.global_entity_id

LEFT JOIN rrate
       ON dv.vendor_code = rrate.vendor_code_rr
      AND rrate.order_date_rr = o.order_date
      AND rrate.global_entity_id = dv.global_entity_id

LEFT JOIN cpp_revenue_per_vendor
       ON dv.vendor_code = cpp_revenue_per_vendor.vendor_code
      AND o.order_date = cpp_revenue_per_vendor.month
      AND dv.global_entity_id = cpp_revenue_per_vendor.global_entity_id

LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` shared_countries
       ON shared_countries.global_entity_id = dv.global_entity_id

LEFT JOIN pandata_ap_commercial.apac_cost_per_order_per_month cpo
       ON cpo.month = o.order_date
      AND TRIM(cpo.country) = shared_countries.name

LEFT JOIN cpc_revenue_per_vendor
       ON dv.global_entity_id = cpc_revenue_per_vendor.global_entity_id
      AND dv.vendor_code = cpc_revenue_per_vendor.vendor_code
      AND o.order_date = cpc_revenue_per_vendor.month

LEFT JOIN exchange_rate_monthly
       ON o.order_date = exchange_rate_monthly.month
      AND dv.global_entity_id = exchange_rate_monthly.global_entity_id

LEFT JOIN listing_fee
       ON dv.global_entity_id = listing_fee.global_entity_id
      AND dv.vendor_code = listing_fee.vendor_code
      AND o.order_date = listing_fee.listing_fee_month
LEFT JOIN (
    SELECT
      global_entity_id,
      DATE_TRUNC(date, MONTH) AS month,
      vendor_code,
      MAX(is_daily_active) AS is_monthly_active
    FROM active_status
    GROUP BY 1,2,3
  ) active_status
       ON active_status.global_entity_id = dv.global_entity_id
      AND active_status.month = o.order_date
      AND active_status.vendor_code = dv.vendor_code
