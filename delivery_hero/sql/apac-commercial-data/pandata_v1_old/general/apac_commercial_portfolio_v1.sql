/*Title: Commercial Portfolio
  Author: Abbhinaya Pragasm
  Last Modified: March 2021
*/

WITH vendor_with_sf_info AS (
  SELECT 
    v.rdbms_id,
    v.chain_code,
    v.chain_name,
    v.vendor_code,
    v.id AS vendor_id,
    v.business_type,
    v.commission,
    v.is_pickup_accepted,
    TRUE AS enterprise_account,
    CASE 
      WHEN sf_accounts_custom.vendor_grade LIKE "%AAA%" 
      THEN 'AAA'
      ELSE 'Non-AAA'
    END AS aaa_type,
    CASE 
      WHEN sf_accounts_custom.type = "Holding" 
      THEN sf_accounts_custom.account_name
      ELSE NULL
    END AS holding_account_name,
    CASE 
      WHEN sf_accounts_custom.type = "Group" 
      THEN sf_accounts_custom.account_name
      ELSE NULL
    END AS group_account_name,
    CASE 
      WHEN sf_accounts_custom.type = "Brand" 
      THEN sf_accounts_custom.account_name
      ELSE NULL
    END AS brand_account_name,
    sf_accounts_custom.id AS sf_id,
    sf_accounts_custom.grid_id AS grid_id,
    sf_accounts_custom.parent_sf_account_id AS parent_sf_account_id,
    sf_accounts_custom.account_name AS account_name,
    sf_accounts_custom.owner_name AS owner_name,
    sf_accounts_custom.status,
    COALESCE(
      CASE WHEN v.primary_cuisine = 'NA' THEN NULL ELSE v.primary_cuisine END,
      CASE WHEN sf_accounts_custom.primary_cuisine = 'NA' THEN NULL ELSE sf_accounts_custom.primary_cuisine END,
      CASE WHEN sf_accounts_custom.category = 'NA' THEN NULL ELSE sf_accounts_custom.category END,
      CASE WHEN sf_accounts_custom.vertical_segment = 'NA' THEN NULL ELSE sf_accounts_custom.vertical_segment END,
      CASE WHEN sf_accounts_custom.vertical = 'NA' THEN NULL ELSE sf_accounts_custom.vertical END
            ) AS primary_cuisine
  FROM `dhh---analytics-apac.pandata.dim_vendors` v 
  LEFT JOIN `dhh---analytics-apac.pandata.sf_accounts` sf_accounts_custom
         ON sf_accounts_custom.vendor_code = v.vendor_code 
        AND sf_accounts_custom.rdbms_id = v.rdbms_id
  WHERE NOT v.is_vendor_testing
    AND NOT v.is_deleted
),

ve AS (
  SELECT
    rdbms_id,
    ve.chain_code,
    ve.chain_name AS Chain,
    COALESCE(
      ve.chain_name, 
      ve.holding_account_name, 
      ve.group_account_name, 
      ve.brand_account_name
    ) AS top_hierarchy_name,
    ve.vendor_code AS vendor_code_ve,
    ve.vendor_id,
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
    ve.primary_cuisine
  FROM vendor_with_sf_info AS ve
),

dv AS (
  SELECT
    rdbms_id AS rdbms_id_dv,
    vendor_code AS vendor_code_dv,
    latitude,
    longitude,
    primary_cuisine_id,
    is_pickup_accepted,
    is_active
  FROM `dhh---analytics-apac.pandata.dim_vendors` AS dim_vendors
),

vendor_sessions AS (
  SELECT
    country,
    DATE_TRUNC(date, MONTH) AS date_vs,
    vendor_code AS vendor_code_vs,
    SUM(count_of_shop_list_loaded) AS count_of_shop_list_loaded,
    SUM(count_of_shop_menu_loaded) AS count_of_shop_menu_loaded,
    SUM(count_of_add_cart_clicked) AS count_of_add_cart_clicked,
    SUM(count_of_checkout_loaded) AS count_of_checkout_loaded,
    SUM(count_of_transaction) AS count_of_transaction,
    SUM(count_of_shop_list_loaded_delivery) AS  count_of_shop_list_loaded_delivery,
    SUM(count_of_shop_menu_loaded_delivery) AS count_of_shop_menu_loaded_delivery,
    SUM(count_of_add_cart_clicked_delivery) AS count_of_add_cart_clicked_delivery,
    SUM(count_of_checkout_loaded_delivery) AS count_of_checkout_loaded_delivery,
    SUM(count_of_transaction_delivery) AS count_of_transaction_delivery,
    SUM(count_of_shop_list_loaded_pickup) AS count_of_shop_list_loaded_pickup,
    SUM(count_of_shop_menu_loaded_pickup) AS count_of_shop_menu_loaded_pickup,
    SUM(count_of_add_cart_clicked_pickup)as count_of_add_cart_clicked_pickup,
    SUM(count_of_checkout_loaded_pickup) AS count_of_checkout_loaded_pickup,
    SUM(count_of_transaction_pickup) AS count_of_transaction_pickup
  FROM `dhh---analytics-apac.pandata_ap_product_external.vendor_level_session_metrics`
  WHERE date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH), MONTH)
  GROUP BY 1, 2, 3
),

vendor_discount AS (
  SELECT 
    DISTINCT
    'Discount' AS category,
    rdbms_id,
    id,
    title AS customer_code,
    discount_type AS type,
    'Discount' AS purpose,
    expedition_type,
    foodpanda_ratio AS ratio_foodpanda,
    minimum_order_value_local,
    start_date_local AS start_date,
    CASE
      WHEN (is_deleted OR NOT is_active) AND end_date_local > DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
      THEN DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
      WHEN (is_deleted OR NOT is_active) AND end_date_local <= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
      THEN end_date_local
      ELSE end_date_local
    END AS end_date,
    CASE
      WHEN DATE_SUB(current_date, INTERVAL 1 DAY) >= end_date_local
      THEN 1
      ELSE 0
    END AS is_ended
  FROM pandata.dim_discounts
),

vd AS (
  SELECT  
    DISTINCT v.rdbms_id||'-'||v.vendor_code AS uuid,
    v.rdbms_id,
    v.vendor_code AS vendor_code_vd,
    DATE_TRUNC(o.date_local, MONTH) AS order_date_vd,
    COUNT(o.code) AS redemption,
    SUM(gfv_eur) AS gfv,
    SUM(discount_value_eur) AS discount_value_vd,
    SUM(CASE
          WHEN NOT vd.is_deleted
          THEN discount_value_eur * (1 - SAFE_DIVIDE(IFNULL(ratio_foodpanda,0),100))
          ELSE 0
        END) AS vendor_funded_discount_cost,
    SUM(CASE
          WHEN NOT vd.is_deleted
          THEN discount_value_eur * SAFE_DIVIDE(IFNULL(ratio_foodpanda,0),100)
          ELSE 0
        END) AS fp_funded_discount_cost
    /**SUM(IF(is_first_valid_order,1,0)) AS nc**/
  FROM vendor_discount AS v_d
  LEFT JOIN pandata.fct_orders o 
         ON v_d.rdbms_id = o.rdbms_id 
        AND v_d.id = o.discount_id 
        AND v_d.category = 'Discount'
  LEFT JOIN pandata.dim_vendors v 
         ON v_d.rdbms_id = v.rdbms_id 
        AND o.vendor_id = v.id
  LEFT JOIN pandata.dim_vendor_discounts vd 
         ON v_d.rdbms_id = vd.rdbms_id 
        AND o.vendor_id = vd.vendor_id 
        AND o.discount_id = vd.discount_id
  WHERE o.created_date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 7 MONTH), MONTH)
    AND o.is_valid_order
    AND date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH), MONTH)
  GROUP BY 1, 2, 3, 4
),

reorder_rate AS (
  SELECT
    date_local AS order_date_rr,
    id AS order_id,
    rdbms_id,
    vendor_id,
    vendor_code AS vendor_code_rr,
    customer_id,
    LAG(date_local) OVER (PARTITION BY rdbms_id, customer_id, vendor_id ORDER BY date_local) AS last_order_date,
    LAG(id) OVER (PARTITION BY rdbms_id, customer_id, vendor_id  ORDER BY date_local) AS last_order_id,
    LAG(customer_id) OVER (PARTITION BY rdbms_id, customer_id, vendor_id  ORDER BY date_local) AS last_customer_id,
    ROW_NUMBER() OVER (PARTITION BY rdbms_id, date_local, vendor_id, customer_id ORDER BY date_local) AS ranking
  FROM pandata.fct_orders
  WHERE created_date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 7 MONTH), MONTH)
    AND is_valid_order
    AND order_source <> 'corporate'
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
    rdbms_id,
    DATE_TRUNC(order_date_rr, MONTH) AS order_date_rr,
    vendor_code_rr,
    COUNT(DISTINCT customer_id) AS reorder_a,
    COUNT(DISTINCT CASE
                     WHEN last_order_date IS NOT NULL AND DATE_DIFF(order_date_rr,last_order_date, DAY) + 1 BETWEEN 0 AND 30
                     THEN customer_id
                     ELSE NULL
                   END) AS reorder_b
  FROM rr_base
  GROUP BY 1, 2, 3
),

cpc_bookings AS (

WITH dvzone AS (
  SELECT 
  *,
  ROW_NUMBER() OVER (PARTITION BY rdbms_id,vendor_code ORDER BY lg_zone_id) AS row_number
  FROM pandata.dim_vendors v,
  unnest(lg_zone_ids) AS lg_zone_id
),

cpcbookings AS (
  SELECT 
    bk.rdbms_id,
    c.common_name,
    bk.uuid AS booking_id,
    gmvc.gmv_class,
    v.vendor_name,
    json_extract_scalar(bk.parameters,'$.vendor_code') AS vendor_code,
    dvz.chain_name,
    dvz.chain_code,
    dvz.city_name,
    hz.name as hurrier_zone,
    bk.user,
    CASE
      WHEN
        CASE
          WHEN lower(bk.user) NOT LIKE '%foodpanda%'
          THEN 'self-booking'
          ELSE bk.user
        END = 'self-booking'
      THEN 'Self-Booking'
      WHEN agent_list.email is NULL
      THEN 'Local'
      ELSE 'Central'
    END AS channel,
    FORMAT_DATE('W%V-%Y', DATE(bk.created_at_utc)) AS booking_date,
    bk.type,
    bk.status,
    cpc.initial_budget AS initial_budget_local,
    cpc.click_price AS click_price_local,
    SAFE_DIVIDE(cpc.initial_budget,cpc.click_price) AS budgeted_clicks,
    DATE(bk.started_at_utc) AS start_date,
    CASE 
      WHEN date(bk.ended_at_utc) IS NULL
      THEN CURRENT_DATE() 
      ELSE date(bk.ended_at_utc) 
    END AS end_date,
    COUNT(DISTINCT cpc.uuid) AS promo_areas_booked
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk,
  UNNEST(bk.cpc_billings) cpc
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` cl
         ON cl.pps_item_uuid = bk.uuid
  LEFT JOIN pandata.dim_countries c
         ON c.rdbms_id = bk.rdbms_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
         ON bk.pps_promo_area_uuid = pa.uuid
  LEFT JOIN pandata.dim_vendors v 
         ON v.rdbms_id = bk.rdbms_id 
        AND v.vendor_code = JSON_EXTRACT_SCALAR(bk.parameters,'$.vendor_code')
  LEFT JOIN pandata_report.vendor_gmv_class gmvc 
         ON gmvc.rdbms_id = v.rdbms_id 
        AND gmvc.vendor_code = v.vendor_code
  LEFT JOIN dvzone dvz
         ON dvz.rdbms_id = v.rdbms_id
        AND dvz.vendor_code = v.vendor_code
  LEFT JOIN pandata.lg_zones hz
         ON dvz.rdbms_id = hz.rdbms_id
        AND dvz.lg_zone_id= hz.id
  LEFT JOIN pandata.sf_accounts sf
         ON sf.rdbms_id = bk.rdbms_id
        AND sf.vendor_code = json_extract_scalar(bk.parameters,'$.vendor_code')
  LEFT JOIN (
    SELECT
      country_name AS country,
      email
    FROM pandata_ap_commercial.ncr_central_agent_material
  ) agent_list ON agent_list.email = bk.user
  WHERE bk.uuid IS NOT NULL
    AND bk.type = 'organic_placements'
    AND bk.billing_type = 'CPC'
    AND row_number = 1
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

/*1 line per month live*/
cpcbookings_array AS (
  SELECT
    *,
    GENERATE_DATE_ARRAY(DATE_TRUNC(start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested,
  FROM cpcbookings
),

finalbookings AS (
  SELECT 
    rdbms_id,
    common_name AS country,
    booking_id,
    gmv_class,
    vendor_name,
    vendor_code,
    chain_name,
    chain_code,
    city_name,
    hurrier_zone,
    user,
    channel,
    booking_date AS booking_week,
    type,
    status,
    start_date,
    end_date,
    DATE_SUB(
      DATE_TRUNC(
        DATE_ADD(
          parse_date(
            "%Y%m",
            CAST(FORMAT_DATE("%Y%m",datelive) AS string)
          ),
          INTERVAL 1 MONTH
        ),
        MONTH
      ),
      INTERVAL 1 DAY
    ) AS last_day,
    FORMAT_DATE('%b', datelive) AS month,
    FORMAT_DATE("%Y%m",datelive) AS yearmonth_live,
    DATE_TRUNC(datelive, MONTH) AS month_format_final,
    promo_areas_booked,
    click_price_local,
    budgeted_clicks,
    initial_budget_local,
  FROM cpcbookings_array ca,
  UNNEST(datelive_nested) AS datelive
),

/*clicks & orders*/
clicks AS (
  SELECT DISTINCT
    bk.uuid AS booking_id,
    FORMAT_DATE('%Y%m', DATE(cpc.created_at_utc)) AS click_month,
    COUNT(DISTINCT cpc.pps_item_uuid) AS active_areas,
    SUM(orders) AS cpc_orders,
    SUM(quantity) AS spent_clicks
  FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` bk,
  UNNEST(bk.cpc_billings) bil
  JOIN `fulfillment-dwh-production.pandata_curated.pps_cpc_clicks` AS cpc 
    ON bil.uuid = cpc.pps_item_uuid
  GROUP BY 1,2
),
    
exchangerate AS (
  SELECT 
    rdbms_id,
    format_timestamp ('%Y%m',exchange_rate_date) AS yearmonth,
    AVG(exchange_rate_value) AS exchange_rate
  FROM il_backend_latest.v_dim_exchange_rates
  GROUP BY 1,2
),
 
final_booking_joins AS (
  SELECT 
    fb.* EXCEPT (promo_areas_booked),
    SAFE_DIVIDE(initial_budget_local,exr.exchange_rate) AS initial_budget_eur,
    CASE
      WHEN spent_clicks > budgeted_clicks 
      THEN budgeted_clicks 
      ELSE spent_clicks 
    END AS final_spent_clicks,
    fb.promo_areas_booked,
    exr.exchange_rate,
    c.active_areas,
    c.cpc_orders,
    c.spent_clicks,
  FROM finalbookings fb
  LEFT JOIN clicks c 
         ON c.click_month = fb.yearmonth_live 
        AND c.booking_id = fb.booking_id
  LEFT JOIN exchangerate exr 
         ON exr.rdbms_id = fb.rdbms_id 
        AND exr.yearmonth = fb.yearmonth_live
),

final_budgeted_clicks AS (
  SELECT 
    *,
    CASE 
      WHEN status = 'cancelled' AND end_date < last_day 
      THEN final_spent_clicks 
      ELSE budgeted_clicks 
    END AS final_budgeted_clicks
  FROM final_booking_joins
),

cpc_revenue_calculation AS (
  SELECT *,
    CASE
      WHEN final_spent_clicks*click_price_local > initial_budget_local
      THEN CAST(initial_budget_local AS FLOAT64) 
      ELSE CAST(final_spent_clicks*click_price_local AS FLOAT64) 
    END AS cpc_rev_local,
    CAST(
      SAFE_DIVIDE(
        (CASE 
           WHEN final_spent_clicks*click_price_local > initial_budget_local 
           THEN initial_budget_local 
           ELSE final_spent_clicks*click_price_local 
         END),
        exchange_rate
      ) AS FLOAT64
    ) AS cpc_rev_eur,
    CAST(SAFE_DIVIDE(cpc_orders,final_spent_clicks) AS FLOAT64) AS cpc_conversion
  FROM final_budgeted_clicks
)

SELECT
rdbms_id,
vendor_code,
DATE_TRUNC(month_format_final, MONTH) AS month,
SUM(cpc_rev_eur) AS price_op_cpc
FROM cpc_revenue_calculation
GROUP BY 1,2,3
),


pps AS (
  WITH cpp AS (
    SELECT
      b.rdbms_id,
      c.common_name AS country,
      b.uuid AS booking_id,
      b.type ,
      DATE(b.created_at_utc) AS booking_created_date,
      DATE(b.started_at_utc) AS start_date,
      DATE(b.ended_at_utc) AS end_date,
      business_type,
      v.vendor_code,
      v.vendor_name,
      v.chain_code,
      v.chain_name,
      CASE
        WHEN 
          (CASE 
             WHEN LOWER(b.user) NOT LIKE '%foodpanda%' 
             THEN 'self-booking'
             ELSE b.user
           END) = 'self-booking' 
        THEN 'Self-Booking'
        WHEN agent_list.email IS NULL 
        THEN 'Local'
        ELSE 'Central'
      END AS booking_source,
      b.cpp_billing.price
    FROM `fulfillment-dwh-production.pandata_curated.pps_bookings` b
    LEFT JOIN pandata.dim_countries c
           ON c.rdbms_id = b.rdbms_id
    LEFT JOIN pandata.dim_vendors v 
           ON b.rdbms_id = v.rdbms_id
          AND v.vendor_code = json_extract_scalar(b.parameters,'$.vendor_code')
    LEFT JOIN (
      SELECT
        country_name AS country,
        email
      FROM pandata_ap_commercial.ncr_central_agent_material
    ) agent_list
           ON agent_list.email = b.user
    WHERE b.uuid is not null
      AND b.billing_type = 'CPP'
      AND b.rdbms_id in (7,12,15,16,17,18,19,20,219,220,221,263)
  ),

  cpp_bookings_month AS (
    SELECT
      *,
      GENERATE_DATE_ARRAY(DATE_TRUNC(start_date, MONTH), DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AS datelive_nested
    FROM cpp
  ),

  result AS (
    SELECT
      DATE_TRUNC(datelive, MONTH) AS date_live,
      cpp_bookings_month.* EXCEPT(datelive_nested),
      DATE_SUB(
        DATE_TRUNC(
          DATE_ADD(
            DATE_TRUNC(
              datelive, 
              MONTH
            ), 
            INTERVAL 1 MONTH
          ), 
          MONTH
        ), 
        INTERVAL 1 DAY
      ) AS last_day,
    FROM cpp_bookings_month,
    UNNEST(datelive_nested) AS datelive
  ),

exchangerate AS (
  SELECT 
    rdbms_id,
    DATE_TRUNC(DATE(exchange_rate_date), MONTH) AS month,
    AVG(exchange_rate_value) AS exchange_rate
  FROM il_backend_latest.v_dim_exchange_rates
  GROUP BY 1,2
),

  final_result AS (
  SELECT
    r.*,
    CAST(SAFE_DIVIDE(r.price, exchange_rate) AS FLOAT64) AS price_eur
  FROM result r
  LEFT JOIN pandata.sf_accounts a 
         ON r.rdbms_id = a.rdbms_id
        AND r.vendor_code = a.vendor_code
    LEFT JOIN exchangerate exr 
           ON exr.rdbms_id = r.rdbms_id 
          AND exr.month = r.date_live
  ),

cpp_final AS (
  SELECT
    pp.date_live,
    pp.rdbms_id,
    pp.country,
    pp.business_type,
    pp.vendor_code,
    pp.vendor_name,
    pp.type,
    'dollar' AS budget_unit,
    SUM(pp.price_eur) AS budget,
    SUM(pp.price_eur) AS revenue
  FROM final_result pp
  GROUP BY 1,2,3,4,5,6,7,8
  )
  
  SELECT
    rdbms_id AS rdbms_id_pps,
    DATE_TRUNC(date_live, MONTH) AS start_date,
    vendor_code AS vendor_code_pps,
    SUM(CASE 
          WHEN type = 'premium_placements' OR type = 'marketing_packages'
          THEN revenue 
        END) AS price_pp_cpp,
    SUM(CASE 
          WHEN type = 'organic_placements'
          THEN revenue 
        END) AS price_op_cpp
  FROM cpp_final
  GROUP BY 1, 2, 3
),

listing_fee_metrics AS (
  WITH listing_fees AS (
    SELECT
      ac.rdbms_id,
      ac.sf_country_name AS country,
      ac.product,
      ac.type,
      vv.vendor_code,
      business_type,
      ac.start_date_local,
      COALESCE(ac.termination_date_local, CURRENT_DATE) AS end_date_local,
      ac.total_amount_local
    FROM `dhh---analytics-apac.pandata.sf_additional_charges` ac
    LEFT JOIN pandata.dim_vendors vv 
           ON vv.rdbms_id = ac.rdbms_id 
          AND ac.vendor_id = vv.id
    WHERE ( 
            (ac.rdbms_id = 7 AND product IN ('Platform Fee'))
            OR (ac.rdbms_id = 12 AND product IN ('SIM Card Fee'))
            OR (ac.rdbms_id = 15 AND product IN ('Platform Fee'))
            OR (ac.rdbms_id = 16 AND product IN ('Service Fee'))
            OR (ac.rdbms_id = 17 AND product IN ('ค่าแรกเข้า'))
            OR (ac.rdbms_id = 18 AND product IN ('期租費'))
            OR (ac.rdbms_id = 19 AND product IN ('Monthly Listing Fees'))
            OR (ac.rdbms_id = 20 AND product IN ('Platform Fees'))
          )
    AND ac.status = "Active"
    AND NOT ac.is_deleted
    AND ac.start_date_local <= CURRENT_DATE()
    AND ac.type = 'Recurring Fee'
  ),

  exchangerate AS (
    SELECT 
      rdbms_id,
      DATE_TRUNC(DATE(exchange_rate_date), MONTH) AS month,
      AVG(exchange_rate_value) AS exchange_rate
    FROM il_backend_latest.v_dim_exchange_rates
    GROUP BY 1,2
  ),

  listing_fees_month AS (
    SELECT
      *,
      GENERATE_DATE_ARRAY(
        DATE_TRUNC(start_date_local, MONTH),
        DATE_TRUNC(end_date_local, MONTH),
        INTERVAL 1 MONTH
      ) AS datelive_nested
    FROM listing_fees
  ),

  result AS (
    SELECT
      DATE_TRUNC(datelive, MONTH) AS date_live,
      listing_fees_month.* EXCEPT(datelive_nested),
      DATE_SUB(
        DATE_TRUNC(
          DATE_ADD(
            DATE_TRUNC(
              datelive,
              MONTH
            ),
            INTERVAL 1 MONTH
          ),
          MONTH
        ),
        INTERVAL 1 DAY
      ) AS last_day,
    FROM listing_fees_month,
    UNNEST(datelive_nested) AS datelive
  ),

  final_result AS (
  SELECT
    r.*,
    CAST(SAFE_DIVIDE(r.total_amount_local, exchange_rate) AS FLOAT64) AS price_eur
  FROM result r
  LEFT JOIN pandata.sf_accounts a 
         ON r.rdbms_id = a.rdbms_id
        AND r.vendor_code = a.vendor_code
    LEFT JOIN exchangerate exr 
           ON exr.rdbms_id = r.rdbms_id 
          AND exr.month = r.date_live
  )
  
  SELECT
    listing_fee.date_live,
    listing_fee.rdbms_id,
    listing_fee.country,
    listing_fee.business_type,
    listing_fee.vendor_code,
    listing_fee.type,
    SUM(listing_fee.price_eur) AS listing_fee_revenue
  FROM final_result AS listing_fee
  GROUP BY 1,2,3,4,5,6
),

time_period AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH), MONTH) AS date
),

automation_orders AS (
  SELECT
    orders.rdbms_id,
    orders.id AS order_id,
    status_flows.id AS status_flow_id,
    status_flows.user_id AS status_flow_user_id,
    status_flows.date AS status_flow_date,
    RANK() OVER (
      PARTITION BY
        orders.rdbms_id,
        orders.id
      ORDER BY status_flows.id
    ) = 1 AS is_earliest_status_flow
  FROM ml_backend_latest.statusflows AS status_flows
  INNER JOIN ml_backend_latest.orders
          ON status_flows.rdbms_id = orders.rdbms_id
         AND status_flows.order_id = orders.id
  LEFT JOIN pandata_intermediate.dim_order_status
         ON dim_order_status.rdbms_id = status_flows.rdbms_id
        AND dim_order_status.id = status_flows.status_id
  CROSS JOIN time_period
  WHERE status_flows.user_id > 0
    AND dim_order_status.is_cancelled_automatically
    AND DATE(status_flows.date) >= time_period.date
),

first_automation_user_by_order AS (
  SELECT
    automation_orders.rdbms_id,
    automation_orders.order_id,
    dim_users.id,
    dim_users.has_vendor_role,
  FROM automation_orders
  LEFT JOIN pandata_intermediate.dim_users
         ON dim_users.rdbms_id = automation_orders.rdbms_id
        AND dim_users.id = automation_orders.status_flow_user_id
  WHERE automation_orders.is_earliest_status_flow
),

order_status AS (
 SELECT
    orders.id AS order_id,
    orders.rdbms_id,
    CASE
      WHEN dim_order_status.id IN (12)
      THEN TRUE
    END AS is_vendor_cancelled,
    CASE
      WHEN dim_order_status.id IN (6)
      THEN TRUE
    END AS is_vendor_called_to_confirm_order,
    MIN(CASE
          WHEN dim_order_status.id IN (5,7,11,12,13,27,30)
          THEN status_flows.date
          ELSE NULL
        END) AS vendor_confirmation_end_at_local,
    MIN(CASE
          WHEN dim_order_status.id = 10
               OR (
                    (
                      dim_order_status.id = 4
                      AND first_automation_user_by_order.has_vendor_role
                    )
               OR dim_order_status.id = 2
                  )
          THEN status_flows.date
          ELSE NULL
        END) AS vendor_confirmation_start_at_local,

    NOT (
      LOGICAL_OR(dim_order_status.is_cancelled_automatically)
      OR (
        LOGICAL_OR(dim_order_status.is_not_automated)
        AND NOT LOGICAL_AND(dim_users.is_automated)
      )
    ) AS is_automated
  FROM ml_backend_latest.statusflows AS status_flows
  INNER JOIN ml_backend_latest.orders
          ON status_flows.rdbms_id = orders.rdbms_id
         AND status_flows.order_id = orders.id
  LEFT JOIN pandata_intermediate.dim_order_status
         ON dim_order_status.rdbms_id = status_flows.rdbms_id
        AND dim_order_status.id = status_flows.status_id
  LEFT JOIN first_automation_user_by_order
         ON orders.rdbms_id = first_automation_user_by_order.rdbms_id
        AND orders.id = first_automation_user_by_order.order_id
  LEFT JOIN pandata_intermediate.dim_users
         ON first_automation_user_by_order.rdbms_id = dim_users.rdbms_id
        AND first_automation_user_by_order.id = dim_users.id
  CROSS JOIN time_period
  WHERE dim_order_status.code IS NOT NULL
    AND dim_order_status.id IS NOT NULL
    AND DATE(status_flows.date) >= time_period.date
  GROUP BY orders.rdbms_id, orders.id, is_vendor_cancelled, is_vendor_called_to_confirm_order
),
    
vendor_confirmation_time AS(    
  SELECT
    *,
    TIMESTAMP_DIFF(vendor_confirmation_end_at_local, vendor_confirmation_start_at_local, SECOND) AS vendor_confirmation_time_in_seconds
  FROM order_status
  GROUP BY 1,2,3,4,5,6,7
),

salesforce_tickets_sales_portal AS (
  SELECT
    sf_tickets.rdbms_id,
    sf_tickets.order_id,
    TRUE AS has_order_item_issues
  FROM pandata.sf_tickets
  LEFT JOIN pandata.dim_countries
         ON dim_countries.common_name = sf_tickets.country
        AND dim_countries.company_name = 'Foodpanda'
  WHERE sf_tickets.ccr1 IN ('Post-Delivery','Live Order Process')
    AND sf_tickets.ccr2 IN ('Wrong / Missing item', 'Food issue','Order status',NULL,'Wrong order / Never arrived')
    AND sf_tickets.ccr3 IN ('Wrong item', 'Missing item','Item unavailable for pickup','Cooking instructions were not followed','Issue with item replacement process','Wrong order')
    AND sf_tickets.order_id IS NOT NULL
    AND DATE(sf_tickets.ticket_created_at_utc, dim_countries.timezone) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH), MONTH)
  GROUP BY sf_tickets.rdbms_id, sf_tickets.order_id
),

salesforce_tickets_service_portal AS (
  SELECT
    fct_orders.rdbms_id,
    fct_orders.id AS order_id,
    TRUE AS has_order_item_issues
  FROM il_contact_center_latest.v_salesforce_fct_tickets AS v_salesforce_fct_tickets
  LEFT JOIN pandata.dim_countries
         ON dim_countries.entity_id = v_salesforce_fct_tickets.global_entity_id
        AND dim_countries.company_name = 'Foodpanda'
  LEFT JOIN pandata.fct_orders
         ON fct_orders.rdbms_id = dim_countries.rdbms_id
        AND fct_orders.code = v_salesforce_fct_tickets.order_code
        AND fct_orders.created_date_local >= DATE_SUB(CURRENT_DATE, INTERVAL 11 WEEK)
  WHERE v_salesforce_fct_tickets.ccr1 IN ('Post-Delivery','Live Order Process')
    AND v_salesforce_fct_tickets.ccr2 IN ('Wrong / Missing item', 'Food issue','Order status',NULL,'Wrong order / Never arrived')
    AND v_salesforce_fct_tickets.ccr3 IN ('Wrong item', 'Missing item','Item unavailable for pickup','Cooking instructions were not followed','Issue with item replacement process','Wrong order')
    AND REGEXP_CONTAINS(v_salesforce_fct_tickets.order_code,'^[0-9a-z]{{4}}-[0-9a-z]{{4}}$')
    --check if the order code is a valid code
    AND DATE(v_salesforce_fct_tickets.ticket_created_at, dim_countries.timezone) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH), MONTH)
  GROUP BY fct_orders.rdbms_id, fct_orders.id
),

salesforce_tickets_all_portals AS (
  SELECT
    rdbms_id,
    order_id,
    has_order_item_issues
  FROM salesforce_tickets_sales_portal

  UNION ALL

  SELECT
    rdbms_id,
    order_id,
    has_order_item_issues
  FROM salesforce_tickets_service_portal
),

salesforce_tickets AS (
  SELECT
    rdbms_id,
    order_id,
    has_order_item_issues
  FROM salesforce_tickets_all_portals
  GROUP BY rdbms_id, order_id, has_order_item_issues
),

closed_hours_final AS (
  SELECT
    restaurant_offline_report.rdbms_id,
    DATE_TRUNC(report_date, MONTH) AS offline_date,
    restaurant_offline_report.vendor_id,
    restaurant_offline_report.vendor_code,
    SUM(closed_hours) as total_closed_hours,
    SUM(open_hours) as total_open_hours,
    SAFE_SUBTRACT(SUM(open_hours),SUM(closed_hours)) as actual_open_hours
  FROM pandata_ap_commercial.restaurant_offline_report
  GROUP BY rdbms_id, offline_date, vendor_id, vendor_code
),

o AS (
  SELECT
    o.rdbms_id AS rdbms_id_o,
    o.country_name AS country,
    o.vendor_code AS vendor_code_o,
    DATE_TRUNC(o.date_local, MONTH) AS order_date,
    COUNT(DISTINCT o.vendor_code) AS no_of_day_w_order,
    COUNT(DISTINCT o.id) AS no_of_all_order,
    SUM(CASE 
          WHEN o.is_valid_order 
          THEN 1 
          ELSE 0
        END) AS no_of_valid_order,
    SUM(CASE 
          WHEN o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN 1 
          ELSE 0
        END) AS od_no_of_valid_order,
    SUM(CASE 
          WHEN NOT o.is_valid_order 
          THEN 1 
          ELSE 0 
        END) AS no_of_invalid_order,
    SUM(CASE 
          WHEN o.is_failed_order 
          THEN 1 
          ELSE 0 
        END) AS no_of_failed_order,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND NOT o.is_joker_used AND NOT o.is_voucher_used AND NOT o.is_discount_used
          THEN gfv_eur
          ELSE 0
        END) AS gfv_organic,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND NOT o.is_joker_used AND o.is_voucher_used AND o.is_discount_used
          THEN gfv_eur
          ELSE 0
        END) AS gfv_with_voucher_and_discount,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND NOT o.is_joker_used AND o.is_voucher_used AND NOT o.is_discount_used
          THEN gfv_eur
          ELSE 0
        END) AS gfv_with_voucher,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND NOT o.is_joker_used AND NOT o.is_voucher_used AND o.is_discount_used
          THEN gfv_eur 
          ELSE 0 
        END) AS gfv_with_discount,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_joker_used AND o.is_voucher_used AND o.is_discount_used
          THEN gfv_eur 
          ELSE 0 
        END) AS gfv_with_discount_voucher_pandabox,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_joker_used AND NOT o.is_voucher_used AND NOT o.is_discount_used
          THEN gfv_eur 
          ELSE 0 
        END) AS gfv_with_pandabox,
    SUM(CASE WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_joker_used AND o.is_voucher_used AND NOT o.is_discount_used
          THEN gfv_eur 
          ELSE 0 
        END) AS gfv_with_pandabox_voucher,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_joker_used AND NOT o.is_voucher_used AND o.is_discount_used
          THEN gfv_eur 
          ELSE 0 
        END) AS gfv_with_pandabox_discount,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order
          THEN COALESCE(oc.commission_eur, (o.commission_base_eur * commission_percentage_combined))
          ELSE NULL
        END) AS commission_base_x_cr,
        
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN COALESCE(oc.commission_eur, (o.commission_base_eur * commission_percentage_combined))
          ELSE NULL
        END) AS od_commission_base_x_cr,

    CASE
      WHEN SUM(CASE 
                 WHEN o.is_valid_order 
                 THEN 1 
                 ELSE 0 
               END)= 0
      THEN NULL
      ELSE SAFE_DIVIDE(SUM(CASE
                             WHEN o.is_valid_order 
                             THEN commission_percentage_combined 
                             ELSE NULL 
                           END), 
                       SUM(CASE 
                             WHEN o.is_valid_order 
                             THEN 1 
                             ELSE NULL 
                           END)
                      )
    END AS commission_combined,
    CASE
      WHEN SUM(CASE WHEN o.is_valid_order AND o.expedition_type='delivery' THEN 1 ELSE 0 END) = 0
      THEN NULL
      ELSE SAFE_DIVIDE(SUM(CASE
                        WHEN o.is_valid_order AND o.expedition_type = 'delivery'
                        THEN commission_percentage_combined
                        ELSE NULL
                      END),
                  SUM(CASE
                        WHEN o.is_valid_order AND o.expedition_type='delivery'
                        THEN 1
                        ELSE NULL
                      END))
    END AS commission_delivery,
    
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN COALESCE(oc.commission_percentage, SAFE_DIVIDE(oc.commission_local, oc.commissionable_value_local),0)
          ELSE NULL
        END) AS od_commission_percentage_total,
    
    CASE
      WHEN SUM(CASE WHEN o.is_valid_order AND o.expedition_type='pickup' THEN 1 ELSE 0 END) =0
      THEN NULL
      ELSE SAFE_DIVIDE(SUM(CASE WHEN o.is_valid_order AND o.expedition_type = 'pickup' THEN commission_percentage_combined ELSE NULL END),
                       SUM(CASE WHEN o.is_valid_order AND o.expedition_type='pickup' THEN 1 ELSE NULL END))
    END AS commission_pickup,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.expedition_type='pickup'
          THEN COALESCE(oc.commission_eur, (o.commission_base_eur * commission_percentage_combined))
          ELSE NULL
        END) AS pickup_commission_revenue,
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.expedition_type='pickup'
          THEN COALESCE(oc.commissionable_value_eur, o.commission_base_eur)
          ELSE NULL
        END) AS pickup_commission_base,
    

    SUM(gfv_eur) AS gfv_all,
    SUM(CASE 
          WHEN o.is_valid_order 
          THEN gfv_eur 
          ELSE 0 
        END) AS gfv_valid,
    SUM(CASE 
          WHEN o.is_valid_order 
          THEN gfv_local 
          ELSE 0 
        END) AS gfv_valid_local,
    SUM(CASE 
          WHEN o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN gfv_eur 
          ELSE 0 
        END) AS od_gfv_valid,
    SUM(CASE 
          WHEN o.is_valid_order 
          THEN 1 
          ELSE 0 
        END) AS no_order_success,
    SUM(CASE
          WHEN o.is_valid_order 
          THEN o.gmv_eur 
          ELSE 0
        END) AS gmv_eur_total,
       
     /*VENDOR FUNDED METRICS*/

     SUM(CASE
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                AND (
                (o.is_discount_used AND o.discount_ratio < 100)
                OR (o.is_voucher_used AND o.voucher_ratio < 100)
                )
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE((100-o.discount_ratio),100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE((100-o.voucher_ratio),100),0)
         END) AS sub_vf_deal_value_eur,
         
    SUM(CASE
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                AND (
                (o.is_discount_used AND o.discount_ratio < 100 AND o.voucher_ratio != 100)
                OR
                (o.is_discount_used AND o.discount_ratio < 100 AND o.voucher_ratio IS NULL)
                OR (o.is_voucher_used AND o.voucher_ratio < 100 AND o.discount_ratio != 100)
                OR
                (o.is_voucher_used AND o.voucher_ratio < 100 AND o.discount_ratio IS NULL)
                )
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                AND (o.is_discount_used AND o.discount_ratio < 100 AND o.voucher_ratio = 100)
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0)
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                AND (o.is_voucher_used AND o.voucher_ratio < 100 AND o.discount_ratio = 100)
           THEN COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
         END) AS sub_fp_deal_value_eur,
         
    SUM(CASE 
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100)
                   )
          THEN 1 
          ELSE 0
        END) AS sub_vf_all_valid_orders,
        
    SUM(CASE 
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100)
                   )
          THEN gfv_eur
          ELSE 0
        END) AS sub_vf_gfv_eur,
        
    SUM(CASE
          WHEN o.is_valid_order AND o.expedition_type ='delivery' AND o.is_own_delivery
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100)
                   )
          THEN 1
          ELSE 0
        END
       ) AS sub_vf_foodpanda_delivery_valid_orders,
    
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100)
                   )
          THEN COALESCE(oc.commission_percentage, SAFE_DIVIDE(oc.commission_local, oc.commissionable_value_local),0)
          ELSE NULL
        END) AS sub_commission_percentage_total,
       
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100)
                   )
          THEN o.service_fee_eur
          ELSE 0
        END) AS sub_vf_service_fee,
        
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) < o.delivery_fee_eur)
          THEN SAFE_DIVIDE(total_value,fx)
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value = 0 OR total_value IS NULL)
          THEN 0
          ELSE 0 
        END) AS sub_vf_delivery_fee,
        
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100)
                   )
          THEN COALESCE(oc.commission_eur, (o.commission_base_eur * commission_percentage_combined))
          ELSE NULL
        END) AS sub_vf_commission_revenue,
       
----------------------------------------------------------------------------------------------------------------------        
    /*FULL 100% VENDOR FUNDED METRICS*/
     SUM(CASE
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE((100-o.discount_ratio),100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE((100-o.voucher_ratio),100),0)
         END) AS vf_deal_value_eur,
         
    SUM(CASE
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                   AND (
                         (o.is_discount_used AND o.discount_ratio = 0 AND o.voucher_ratio != 100) 
                         OR
                         (o.is_discount_used AND o.discount_ratio = 0 AND o.voucher_ratio IS NULL)
                         OR (o.is_voucher_used AND o.voucher_ratio = 0 AND o.discount_ratio != 100) 
                         OR
                         (o.is_voucher_used AND o.voucher_ratio = 0 AND o.discount_ratio IS NULL)
                       )
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                   AND (o.is_discount_used AND o.discount_ratio = 0 AND o.voucher_ratio = 100)
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0)
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                   AND (o.is_voucher_used AND o.voucher_ratio = 0 AND o.discount_ratio = 100)
           THEN COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
         END) AS full_vf_but_fp_deal_value_eur,
         
    SUM(CASE 
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
          THEN 1 
          ELSE 0
        END) AS vf_all_valid_orders,
    
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
          THEN COALESCE(oc.commission_percentage, SAFE_DIVIDE(oc.commission_local, oc.commissionable_value_local),0)
          ELSE NULL
        END) AS vf_commission_percentage_total,
       
    SUM(CASE 
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
          THEN gfv_eur
          ELSE 0
        END) AS vf_gfv_eur,
        
    SUM(CASE
          WHEN o.is_valid_order AND o.expedition_type ='delivery' AND o.is_own_delivery
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
          THEN 1
          ELSE 0
        END
       ) AS vf_foodpanda_delivery_valid_orders,
       
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
          THEN o.service_fee_eur
          ELSE 0
        END) AS vf_service_fee, 
        
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) < o.delivery_fee_eur)
          THEN SAFE_DIVIDE(total_value,fx)
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value = 0 OR total_value IS NULL)
          THEN 0
          ELSE 0 
        END) AS vf_delivery_fee,
        
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio = 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio = 0)
                   )
          THEN COALESCE(oc.commission_eur, (o.commission_base_eur * commission_percentage_combined))
          ELSE NULL
        END) AS vf_commission_revenue,
-----------------------------------------------------------------------------------------------------------------------------        
/*CO-FUNDING VENDOR FUNDED METRICS*/
     SUM(CASE
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                AND (
                (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0)
                OR
                (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                )
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE((100-o.discount_ratio),100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE((100-o.voucher_ratio),100),0)
         END) AS cofunded_vf_deal_value_eur,
         
    SUM(CASE
           WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
                AND (
                (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0)
                OR
                (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                )
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
         END) AS cofunded_fp_deal_value_eur,
         
    SUM(CASE 
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                   )
          THEN 1 
          ELSE 0
        END) AS cofunded_vf_all_valid_orders,
    
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                   )
          THEN COALESCE(oc.commission_percentage, SAFE_DIVIDE(oc.commission_local, oc.commissionable_value_local),0)
          ELSE NULL
        END) AS cofunded_vf_commission_percentage_total,
       
    SUM(CASE 
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                   )
          THEN gfv_eur
          ELSE 0
        END) AS cofunded_vf_gfv_eur,
        
    SUM(CASE
          WHEN o.is_valid_order AND o.expedition_type ='delivery' AND o.is_own_delivery
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                   )
          THEN 1
          ELSE 0
        END
       ) AS cofunded_vf_foodpanda_delivery_valid_orders,
       
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                   )
          THEN o.service_fee_eur
          ELSE 0
        END) AS cofunded_vf_service_fee,
        
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) < o.delivery_fee_eur)
          THEN SAFE_DIVIDE(total_value,fx)
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0 AND discount_type != 'free-delivery') 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0 AND voucher_type != 'delivery_fee')
                   )
               AND (total_value = 0 OR total_value IS NULL)
          THEN 0
          ELSE 0 
        END) AS cofunded_vf_delivery_fee,
        
    SUM(CASE
          WHEN o.is_valid_order AND NOT o.is_failed_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND (
                     (o.is_discount_used AND o.discount_ratio < 100 AND o.discount_ratio != 0) 
                     OR 
                     (o.is_voucher_used AND o.voucher_ratio < 100 AND o.voucher_ratio != 0)
                   )
          THEN COALESCE(oc.commission_eur, (o.commission_base_eur * commission_percentage_combined))
          ELSE NULL
        END) AS cofunded_vf_commission_revenue,      
-----------------------------------------------------------------------------------------------------------------------------    
    /*Order Accuracy Metric*/
    COUNT(DISTINCT CASE
                     WHEN salesforce_tickets.has_order_item_issues
                     THEN o.id
                   END
         ) AS orders_with_item_issues,
    
    SUM(CASE
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio < 100 AND NOT is_voucher_used AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0)
           
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio < 100 AND is_voucher_used AND voucher_ratio < 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
           
           WHEN o.is_valid_order AND NOT is_discount_used AND is_voucher_used AND voucher_ratio < 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
           
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio < 100 AND is_voucher_used AND voucher_ratio = 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0)
           
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio = 100 AND is_voucher_used AND voucher_ratio < 100 AND o.is_own_delivery AND o.expedition_type = 'delivery'
           THEN COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
         END) AS od_fully_fp_deal_value_eur,
    
    SUM(CASE
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio < 100 AND NOT is_voucher_used
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0)
           
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio < 100 AND is_voucher_used AND voucher_ratio < 100
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0) + COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
           
           WHEN o.is_valid_order AND NOT is_discount_used AND is_voucher_used AND voucher_ratio < 100
           THEN COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
           
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio < 100 AND is_voucher_used AND voucher_ratio = 100
           THEN COALESCE(o.discount_value_eur*SAFE_DIVIDE(o.discount_ratio, 100),0)
           
           WHEN o.is_valid_order AND is_discount_used AND o.discount_ratio = 100 AND is_voucher_used AND voucher_ratio < 100
           THEN COALESCE(o.voucher_value_eur*SAFE_DIVIDE(o.voucher_ratio, 100),0)
         END) AS fully_fp_deal_value_eur,
    
    SUM(CASE WHEN o.is_valid_order AND o.expedition_type ='pickup' THEN gfv_eur ELSE 0 END) AS gfv_pickup,
    SUM(CASE WHEN o.is_valid_order AND o.expedition_type ='pickup' THEN 1 ELSE 0 END) AS no_order_pickup,
    SUM(CASE WHEN o.is_valid_order AND o.expedition_type ='delivery' THEN gfv_eur ELSE 0 END) AS gfv_b2c,
    SUM(CASE WHEN o.is_valid_order AND o.expedition_type ='delivery' THEN 1 ELSE 0 END) AS no_order_b2c,
    SUM(CASE WHEN o.is_valid_order AND o.is_corporate THEN gfv_eur ELSE 0 END) AS gfv_corp,
    SUM(CASE WHEN o.is_valid_order AND o.is_corporate THEN 1 ELSE 0 END) AS no_order_corp,
    SUM(CASE
          WHEN o.is_valid_order AND o.expedition_type ='delivery' AND o.is_own_delivery
          THEN 1
          ELSE 0
        END
       ) AS no_order_foodpanda_delivery,
    SUM(CASE
          WHEN o.is_valid_order AND o.expedition_type ='delivery' AND NOT o.is_own_delivery
          THEN 1
          ELSE 0
        END
       ) AS no_order_vendor_delivery,
    SUM(CASE 
          WHEN o.is_valid_order 
          THEN SAFE_DIVIDE(o.total_value_local, o.fx)
          ELSE 0 
        END) AS total_value,
    SUM(CASE WHEN o.is_first_valid_order THEN 1 ELSE 0 END) AS first_valid_order_with_foodpanda,
    SUM(CASE WHEN o.is_first_valid_order_with_this_chain THEN 1 ELSE 0 END) AS first_valid_order_with_this_chain,
    SUM(CASE WHEN o.is_first_valid_order_with_this_vendor THEN 1 ELSE 0 END) AS first_valid_order_with_this_vendor,
    SUM(CASE WHEN o.is_failed_order_vendor THEN 1 ELSE 0 END) AS failed_order_vendor,
    SUM(CASE WHEN o.is_failed_order_customer THEN 1 ELSE 0 END) AS failed_order_customer,
    SUM(CASE WHEN o.is_failed_order_foodpanda THEN 1 ELSE 0 END) AS failed_order_foodpanda,
    SUM(CASE WHEN o.is_valid_order THEN o.service_fee_eur ELSE 0 END) AS service_fee,
    SUM(CASE
          WHEN o.is_valid_order AND is_own_delivery AND o.expedition_type = 'delivery'
          THEN o.service_fee_eur
          ELSE 0
        END) AS od_service_fee,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND NOT is_voucher_used AND NOT is_discount_used
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND ((is_voucher_used AND voucher_type != 'delivery_fee') AND (is_discount_used AND discount_type != 'free-delivery'))
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND (is_discount_used AND discount_type != 'free-delivery') AND NOT is_voucher_used
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND (is_voucher_used AND voucher_type != 'delivery_fee') AND NOT is_discount_used
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) < o.delivery_fee_eur)
          THEN SAFE_DIVIDE(total_value,fx)
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value = 0 OR total_value IS NULL)
          THEN 0
          WHEN o.is_valid_order AND o.is_own_delivery
               AND ((is_voucher_used AND voucher_type = 'delivery_fee') OR (is_discount_used AND discount_type = 'free-delivery'))
          THEN 0
          ELSE 0
        END) AS delivery_fee,
    SUM(CASE
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND NOT is_voucher_used AND NOT is_discount_used AND o.expedition_type = 'delivery'
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND ((is_voucher_used AND voucher_type != 'delivery_fee') AND (is_discount_used AND discount_type != 'free-delivery')) AND o.expedition_type = 'delivery'
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND (is_discount_used AND discount_type != 'free-delivery') AND NOT is_voucher_used AND o.expedition_type = 'delivery'
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) >= o.delivery_fee_eur)
               AND (is_voucher_used AND voucher_type != 'delivery_fee') AND NOT is_discount_used AND o.expedition_type = 'delivery'
          THEN o.delivery_fee_eur
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value != 0 AND SAFE_DIVIDE(total_value,fx) < o.delivery_fee_eur) AND o.expedition_type = 'delivery'
          THEN SAFE_DIVIDE(total_value,fx)
          WHEN o.is_valid_order AND o.is_own_delivery AND (total_value = 0 OR total_value IS NULL) AND o.expedition_type = 'delivery'
          THEN 0
          WHEN o.is_valid_order AND o.is_own_delivery AND o.expedition_type = 'delivery'
               AND ((is_voucher_used AND voucher_type = 'delivery_fee') OR (is_discount_used AND discount_type = 'free-delivery'))
          THEN 0
          ELSE 0
        END) AS od_delivery_fee,
    SUM(CASE WHEN o.is_valid_order THEN o.gmv_eur ELSE 0 END) AS gmv_eur,
    SUM(CASE WHEN o.is_valid_order THEN o.commission_eur ELSE 0 END) AS commission,
    SUM(CASE WHEN o.is_valid_order THEN voucher_value_eur ELSE 0 END) AS voucher_value,
    SUM(CASE WHEN o.is_valid_order THEN discount_value_eur ELSE 0 END) AS discount_value_total,
    SUM(CASE WHEN o.is_valid_order THEN CAST(joker_fee_amount_eur AS FLOAT64) ELSE 0 END) AS joker_fee_amount,
    SUM(CASE WHEN o.is_valid_order THEN rider_tip_eur ELSE 0 END) AS rider_tip,
    SUM(CASE WHEN o.is_valid_order THEN surcharge_gross_eur ELSE 0 END) AS surcharge_gross,
    SUM(CASE WHEN o.is_valid_order THEN packaging_rating ELSE 0 END) AS packaging_rating,
    SUM(CASE WHEN o.is_valid_order THEN punctuality_rating ELSE 0 END) AS punctuality_rating,
    SUM(CASE WHEN o.is_valid_order THEN restaurant_food_rating ELSE 0 END) AS restaurant_food_rating,
    SUM(CASE WHEN o.is_valid_order THEN rider_rating ELSE 0 END) AS rider_rating,
    SUM(CASE WHEN o.order_source <> 'corporate' THEN IF(o.is_gross_order, 1, 0) ELSE NULL END) AS decline_a,
    COUNT(CASE 
            WHEN o.is_gross_order AND o.status_id = 12 AND o.order_source <> 'corporate'
            THEN o.id 
            ELSE NULL 
          END) AS decline_b,
    COUNT(CASE WHEN lg.order_status = 'completed' THEN lg.id ELSE NULL END) AS late_a,
    COUNT(CASE WHEN lg.order_status = 'completed' AND lg.vendor_lateness_in_seconds >= 300 THEN lg.id ELSE NULL END) AS late_b,
    SUM(CASE WHEN vendor_lateness_in_minutes >=5 THEN 1 ELSE 0 END) AS vendor_lateness_5min_a,
    SUM(CASE WHEN vendor_lateness_in_minutes >=5 THEN 1 ELSE 1 END) AS vendor_lateness_5min_b,
    SUM(CASE WHEN vendor_lateness_in_minutes >=15 THEN 1 ELSE 0 END) AS vendor_lateness_15min_a,
    SUM(CASE WHEN vendor_lateness_in_minutes >=15 THEN 1 ELSE 1 END) AS vendor_lateness_15min_b,
    COUNT(lg.vendor_lateness_in_minutes) AS vl_base,
    SUM(CASE 
          WHEN lg.is_valid_order 
          THEN SAFE_DIVIDE(estimated_prep_time_in_seconds, 60) 
          ELSE 0 
        END) AS sum_ept_min,
    COUNT(lg.estimated_prep_time_in_seconds) AS ept_base,
    SUM(CASE 
          WHEN lg.is_valid_order 
          THEN SAFE_DIVIDE(assumed_actual_prep_time_in_seconds, 60) 
          ELSE 0 
        END) AS sum_apt_min,
    COUNT(DISTINCT CASE
                     WHEN lg.is_valid_order AND assumed_actual_prep_time_in_seconds IS NOT NULL
                     THEN lg.order_code
                   END
         ) AS apt_base,
    SUM(CASE
          WHEN lg.is_valid_order 
          THEN SAFE_DIVIDE(promised_delivery_time_in_seconds, 60) 
          ELSE 0 
        END) AS sum_pdt_min,
    COUNT(DISTINCT CASE
                     WHEN lg.is_valid_order AND actual_delivery_time_in_seconds IS NOT NULL
                     THEN lg.order_code
                   END
         ) AS adt_base,
    SUM(CASE 
          WHEN lg.is_valid_order 
          THEN SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) 
          ELSE 0 
        END) AS sum_adt_min,
        
    COUNT(DISTINCT CASE
                     WHEN lg.is_valid_order 
                          AND actual_delivery_time_in_seconds IS NOT NULL 
                          AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 30
                     THEN lg.order_code
                   END
         ) AS adt_base_30,
    SUM(CASE 
          WHEN lg.is_valid_order 
                AND actual_delivery_time_in_seconds IS NOT NULL 
                AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 30
          THEN SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) 
          ELSE 0 
        END) AS sum_adt_min_30,
        
     
    COUNT(DISTINCT CASE
                     WHEN lg.is_valid_order 
                AND actual_delivery_time_in_seconds IS NOT NULL 
                AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 40
                     THEN lg.order_code
                   END
         ) AS adt_base_40,
    SUM(CASE 
          WHEN lg.is_valid_order 
                AND actual_delivery_time_in_seconds IS NOT NULL 
                AND SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) > 40
          THEN SAFE_DIVIDE(actual_delivery_time_in_seconds, 60) 
          ELSE 0 
        END) AS sum_adt_min_40,
    /*Acceptance Time Metrics*/
    SUM(SAFE_DIVIDE(vendor_confirmation_time.vendor_confirmation_time_in_seconds, 60)) AS sum_vendor_confirmation_min,
    COUNT(
           CASE
             WHEN vendor_confirmation_time.vendor_confirmation_time_in_seconds IS NOT NULL 
             THEN 1
             ELSE 0
           END
         ) AS vendor_confirmation_base,
    SUM(CASE
          WHEN vendor_confirmation_time.vendor_confirmation_time_in_seconds IS NOT NULL AND is_vendor_cancelled
          THEN SAFE_DIVIDE(vendor_confirmation_time.vendor_confirmation_time_in_seconds, 60)
        END
       ) AS sum_vendor_cancelled_time_min,
    COUNT(CASE
           WHEN vendor_confirmation_time.vendor_confirmation_time_in_seconds IS NOT NULL AND is_vendor_cancelled
           THEN 1
           ELSE 0
         END
         ) AS vendor_cancelled_time_base,
    SUM(CASE
          WHEN vendor_confirmation_time.vendor_confirmation_time_in_seconds IS NOT NULL AND is_vendor_called_to_confirm_order
          THEN SAFE_DIVIDE(vendor_confirmation_time.vendor_confirmation_time_in_seconds, 60)
        END
       ) AS sum_vendor_missed_time_min,
    COUNT(CASE
           WHEN vendor_confirmation_time.vendor_confirmation_time_in_seconds IS NOT NULL AND is_vendor_called_to_confirm_order
           THEN 1
           ELSE 0
         END
         ) AS vendor_missed_time_base
    
  FROM `dhh---analytics-apac.pandata.fct_orders` o
  LEFT JOIN `dhh---analytics-apac.pandata.lg_orders` lg
         ON o.rdbms_id = lg.rdbms_id
        AND o.code = lg.order_code
        AND lg.created_date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 8 MONTH), MONTH)
  LEFT JOIN `dhh---analytics-apac.pandata_report.order_commissions` oc
         ON o.rdbms_id = oc.rdbms_id
        AND o.code = oc.order_code
        AND oc.date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 8 MONTH), MONTH)
  LEFT JOIN salesforce_tickets
         ON o.rdbms_id = salesforce_tickets.rdbms_id
        AND o.id = salesforce_tickets.order_id
  LEFT JOIN vendor_confirmation_time
         ON o.rdbms_id = vendor_confirmation_time.rdbms_id
        AND o.id = vendor_confirmation_time.order_id
  WHERE o.created_date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 8 MONTH), MONTH)
    AND NOT o.is_test_order
  GROUP BY 1,2,3,4
)

SELECT
  dv.*,
  o.* EXCEPT(rdbms_id_o, country, vendor_code_o),
  ve.* EXCEPT(rdbms_id, vendor_code_ve),
  closed_hours_final.* EXCEPT(rdbms_id, vendor_code, vendor_id,offline_date),
  vs.* EXCEPT(country, date_vs, vendor_code_vs),
  rrate.* EXCEPT(rdbms_id, order_date_rr, vendor_code_rr),
  pps.* EXCEPT(rdbms_id_pps, vendor_code_pps, start_date),
  vd.* EXCEPT(rdbms_id, vendor_code_vd, order_date_vd, uuid),
  cpc_bookings.* EXCEPT(rdbms_id, vendor_code, month),
  listing_fee_metrics.* EXCEPT(rdbms_id,vendor_code,date_live,country,type,business_type),
  cpo.cost_per_order_eur,
  cpo.cost_per_order_eur_delivery,
  cpo.cost_per_order_eur_cost_of_sales,
  cpo.revenue_per_order_eur
FROM dv
LEFT JOIN o
       ON o.vendor_code_o = dv.vendor_code_dv
      AND o.rdbms_id_o = dv.rdbms_id_dv
LEFT JOIN ve
       ON dv.vendor_code_dv = ve.vendor_code_ve
      AND o.rdbms_id_o = ve.rdbms_id

LEFT JOIN closed_hours_final
       ON dv.vendor_code_dv = closed_hours_final.vendor_code
      AND o.rdbms_id_o = closed_hours_final.rdbms_id   
      AND closed_hours_final.offline_date = o.order_date   
      
LEFT JOIN vendor_sessions vs
       ON dv.vendor_code_dv = vs.vendor_code_vs
      AND vs.date_vs = o.order_date
      AND vs.country = o.country
LEFT JOIN rrate
       ON dv.vendor_code_dv = rrate.vendor_code_rr
      AND rrate.order_date_rr = o.order_date
      AND rrate.rdbms_id = o.rdbms_id_o
LEFT JOIN pps
       ON dv.vendor_code_dv = pps.vendor_code_pps
      AND o.order_date = pps.start_date
      AND o.rdbms_id_o = pps.rdbms_id_pps
LEFT JOIN vd
       ON dv.vendor_code_dv = vd.vendor_code_vd
      AND o.order_date = vd.order_date_vd
      AND o.rdbms_id_o = vd.rdbms_id
LEFT JOIN pandata_ap_commercial.apac_cost_per_order_per_month cpo
       ON cpo.month = o.order_date
      AND cpo.rdbms_id = o.rdbms_id_o
LEFT JOIN cpc_bookings
       ON dv.rdbms_id_dv = cpc_bookings.rdbms_id 
      AND dv.vendor_code_dv = cpc_bookings.vendor_code
      AND o.order_date = cpc_bookings.month
LEFT JOIN listing_fee_metrics
       ON dv.rdbms_id_dv = listing_fee_metrics.rdbms_id 
      AND dv.vendor_code_dv = listing_fee_metrics.vendor_code
      AND o.order_date = listing_fee_metrics.date_live
