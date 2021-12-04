WITH month_start AS (
SELECT
DATE('2021-01-01') AS start_date
),

vendor_base_info AS (
SELECT 
    v.global_entity_id,
    countries.name AS country,
    v.vendor_code,
    'restaurants' AS business_type,
    LOWER(sf_accounts_custom.vendor_grade) AS aaa_type,
    CASE 
      WHEN vendor_gmv_class.gmv_class IS NULL 
      THEN NULL
      ELSE vendor_gmv_class.gmv_class
    END AS gmv_class,
    v.chain_code,
    v.name
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` sf_accounts_custom
         ON sf_accounts_custom.vendor_code = v.vendor_code 
        AND sf_accounts_custom.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS business_types
         ON business_types.uuid = v.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS countries
         ON countries.global_entity_id = v.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = v.vendor_code 
        AND v.global_entity_id = vendor_gmv_class.global_entity_id
  WHERE NOT v.is_test
    AND (
          (v.global_entity_id != 'FP_PK'
          AND business_types.business_type_apac IN ('restaurants'))
        OR (v.global_entity_id = 'FP_PK'
          AND business_types.is_restaurants
          AND NOT business_types.is_home_based_kitchen)
        )
    AND v.global_entity_id LIKE 'FP_%'
    AND v.global_entity_id NOT IN ('FP_RO', 'FP_BG', 'FP_DE')
),

active_vendors_daily AS (
  SELECT
    vendor_status_by_day.date AS day,
    FORMAT_DATE("%G-%V",vendor_status_by_day.date) AS week,
    DATE_TRUNC(vendor_status_by_day.date, MONTH) AS month,
    vendor_base_info.business_type,
    vendor_base_info.aaa_type,
    vendor_base_info.gmv_class,
    vendor_status_by_day.global_entity_id,
    countries.name AS country_name,
    vendor_status_by_day.vendor_code,
    vendor_base_info.chain_code,
    CASE
      WHEN vendor_status_by_day.is_active AND NOT vendor_status_by_day.is_private
      THEN TRUE
      ELSE FALSE
    END AS is_daily_active
  FROM fulfillment-dwh-production.pandata_report.pandora_pd_vendors_active_status vendor_status_by_day
  LEFT JOIN vendor_base_info
         ON vendor_status_by_day.global_entity_id = vendor_base_info.global_entity_id
        AND vendor_status_by_day.vendor_code = vendor_base_info.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS countries
         ON countries.global_entity_id = vendor_status_by_day.global_entity_id
  WHERE TRUE
    AND NOT vendor_status_by_day.is_test
    AND vendor_base_info.vendor_code IS NOT NULL
    AND (
      LOWER(vendor_base_info.name) NOT LIKE '% test%'
      OR LOWER(vendor_base_info.name) NOT LIKE 'test %'
      OR LOWER(vendor_base_info.name) NOT LIKE '%pos integra%'
      OR LOWER(vendor_base_info.name) NOT LIKE '%billing%'
    )
    AND vendor_status_by_day.date >= (
      SELECT start_date
      FROM month_start
      )
  GROUP BY 1,2,3,4,5,6,7,8,9,10,vendor_status_by_day.is_active,vendor_status_by_day.is_private
),

active_status AS (
  SELECT
    active_vendors_daily.global_entity_id,
    country_name AS country,
    day AS date_local,
    active_vendors_daily.month,
    active_vendors_daily.vendor_code,
    CASE WHEN LOWER(active_vendors_daily.aaa_type) = 'aaa' THEN 'AAA' ELSE 'Non-AAA' END AS ka_type,
    CASE WHEN active_vendors_daily.is_daily_active OR daily_all_valid_orders_1 > 0 THEN TRUE ELSE FALSE END AS is_daily_active
  FROM active_vendors_daily
  LEFT JOIN pandata_ap_commercial.daily_deals_performance_data AS daily_deals_performance_data
         ON active_vendors_daily.global_entity_id = daily_deals_performance_data.global_entity_id
        AND active_vendors_daily.day = daily_deals_performance_data.date_local
        AND active_vendors_daily.vendor_code = daily_deals_performance_data.vendor_code
  WHERE --active_vendors_daily.business_type = 'restaurants'
    --AND 
    active_vendors_daily.day >= (
      SELECT start_date
      FROM month_start
      )
),

won_opportunities as (
  select
    distinct global_entity_id,
    sf_account_id
  from `fulfillment-dwh-production.pandata_curated.sf_opportunities`
  where stage_name = 'Closed Won'
),

terminated_vendors AS (
    SELECT
      sf.global_entity_id,
      DATE_TRUNC(DATE(end_date_local), MONTH) as churned_month,
      sf.vendor_code
    FROM (
      SELECT DISTINCT
        v_dim_countries.global_entity_id,
        contract.id AS contract_id,
        contract.sf_account_id AS account_id,
        contract.contract_number,
        LOWER(TRIM(SUBSTR(platform_performance_c.backend_id_c, 5))) AS vendor_code,
        v_dim_countries.name AS country_name,
        contract.status,
        account.name AS account_name,
        account.status AS account_status,
        account.is_marked_for_testing_training AS is_account_marked_for_testing,
        contract.termination_reason AS termination_reason,
        contract.created_at_local AS created_date_local,
        contract.activated_at_local AS activated_date_local,
        contract.start_date_local AS start_date_local,
        contract.last_activity_at_utc AS last_activity_date_local,
        contract.last_modified_at_utc AS last_modified_date_local,
        contract.end_date_local AS end_date_local
      FROM `fulfillment-dwh-production.pandata_curated.sf_contracts` AS contract
      JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS account
        ON account.id = contract.sf_account_id
      LEFT JOIN `dhh---analytics-apac.salesforce.platform_performance_c` AS platform_performance_c
             ON platform_performance_c.account_c = contract.sf_account_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS v_dim_countries
         ON v_dim_countries.global_entity_id = contract.global_entity_id
      WHERE v_dim_countries.global_entity_id IS NOT NULL      
    ) sf
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS dim_vendors
           ON dim_vendors.global_entity_id = sf.global_entity_id
          AND dim_vendors.vendor_code = sf.vendor_code 
    LEFT JOIN won_opportunities
           ON won_opportunities.global_entity_id = sf.global_entity_id
          AND won_opportunities.sf_account_id = sf.account_id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS countries
           ON countries.global_entity_id = sf.global_entity_id
    WHERE sf.account_status = 'Terminated'
      AND won_opportunities.sf_account_id is not null
      AND NOT termination_reason IN ('Onboarding Failed', 'Duplicate')
      AND NOT sf.is_account_marked_for_testing
      AND DATE(end_date_local) >= (
                                    SELECT start_date
                                    FROM month_start
                                    )
    GROUP BY global_entity_id, churned_month, sf.vendor_code
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
  COALESCE(sf_activation_date.activated_date_local, MIN(DATE(pandora_pd_vendors_active_status.date)), DATE(pd_vendors.activated_at_local), first_valid_order) AS activation_date
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
LEFT JOIN fulfillment-dwh-production.pandata_report.pandora_pd_vendors_active_status
       ON pandora_pd_vendors_active_status.global_entity_id = pd_vendors.global_entity_id
      AND pandora_pd_vendors_active_status.vendor_code = pd_vendors.vendor_code
      AND pandora_pd_vendors_active_status.is_active
      AND NOT pandora_pd_vendors_active_status.is_private
      AND NOT pandora_pd_vendors_active_status.is_test
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
WHERE (pd_vendors.global_entity_id LIKE 'FP_%' AND pd_vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG', 'FP_DE'))
  AND NOT pd_vendors.is_test
    AND pandora_pd_vendors_active_status.date >= (
      SELECT start_date
      FROM month_start
      )
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
    vv.vendor_code,
    business_type,
    dates.date,
    sf_accounts.status,
    invoice_frequency,
    ROW_NUMBER() OVER (PARTITION BY sf_additional_charges.global_entity_id, vv.vendor_code, dates.date ORDER BY DATE(ac.created_date) DESC) = 1 AS is_latest_entry,
    TRUE AS is_listing_fee_vendor,
    SAFE_CAST(sf_additional_charges.total_amount_local AS FLOAT64) AS total_amount_local,
    SAFE_CAST(sf_additional_charges.discount_local AS FLOAT64) AS discount_local,
    SAFE_CAST(sf_additional_charges.listed_price_local AS FLOAT64) AS listed_price_local
  FROM `dhh---analytics-apac.salesforce.additional_charges_c` ac
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_additional_charges` AS sf_additional_charges
         ON sf_additional_charges.id = ac.id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
         ON ac.id_account_c = sf_accounts.id
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
    AND vv.vendor_code IS NOT NULL
    AND dates.date >= (
                        SELECT start_date
                        FROM month_start
                        )
    AND dates.date <= CURRENT_DATE
    AND ((sf_additional_charges.global_entity_id = 'FP_PH' AND LOWER(vv.name) NOT LIKE '%7%eleven%' AND LOWER(vv.name) NOT LIKE '%crunch%time%')
        OR sf_additional_charges.global_entity_id != 'FP_PH')
  GROUP BY 1,2,3,4,5,6,7,8, ac.created_date, sf_additional_charges.total_amount_local, sf_additional_charges.discount_local, sf_additional_charges.listed_price_local
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

exchangerate AS (
  SELECT 
    countries.global_entity_id,
    DATE_TRUNC(DATE(fx_rates.fx_rate_date), MONTH) AS month,
    AVG(fx_rates.fx_rate_eur) AS exchange_rate
  FROM `fulfillment-dwh-production.pandata_curated.shared_countries` countries
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx_rates
         ON countries.currency_code_iso = fx_rates.currency_code_iso
  WHERE global_entity_id LIKE 'FP_%'
    --AND global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')
    AND fx_rates.fx_rate_date >= (
      SELECT start_date
      FROM month_start
      )
    AND fx_rates.fx_rate_date <= CURRENT_DATE
  GROUP BY 1,2
),

listing_fees_month AS (
  SELECT
  countries.global_entity_id,
  listing_fee_month,
  listing_fee_month_group.vendor_code,
  /*
  CASE
    WHEN listing_fee_month_group.global_entity_id IN (20) AND listing_fee_month_group.status = 'Active' AND is_listing_fee_vendor
    THEN TRUE
    WHEN listing_fee_month_group.global_entity_id NOT IN (20) AND is_listing_fee_vendor
    THEN TRUE
    ELSE FALSE
  END AS is_listing_fee_vendor,
  */
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
  LEFT JOIN exchangerate exr 
         ON exr.global_entity_id = listing_fee_month_group.global_entity_id 
        AND exr.month = listing_fee_month_group.listing_fee_month
   LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS countries
           ON countries.global_entity_id = listing_fee_month_group.global_entity_id
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
    AND DATE(pd_orders.created_date_local) >= (
          SELECT start_date
          FROM month_start
          )
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
    ) AS listing_revenue
  FROM listing_fees_month
  LEFT JOIN monthly_orders
         ON listing_fees_month.global_entity_id = monthly_orders.global_entity_id
        AND listing_fees_month.listing_fee_month = monthly_orders.order_month
        AND listing_fees_month.vendor_code = monthly_orders.vendor_code
  LEFT JOIN (
  SELECT
  active_status.global_entity_id,
  month,
  vendor_code,
  MAX(is_daily_active) AS is_daily_active
  FROM active_status
  GROUP BY 1,2,3
  )active_status
         ON active_status.global_entity_id = listing_fees_month.global_entity_id
        AND active_status.month = listing_fees_month.listing_fee_month
        AND active_status.vendor_code = listing_fees_month.vendor_code
  GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT
  active_status.global_entity_id,
  active_status.month,
  listing_revenue_total,
  COUNT(DISTINCT CASE 
  WHEN is_daily_active
  THEN active_status.vendor_code END
  ) AS total_active_vendor,
  
  COUNT(DISTINCT CASE 
  WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0 AND is_daily_active
  THEN active_status.vendor_code END
  ) AS total_listing_fee_vendor_signed,
  
  COUNT(DISTINCT CASE 
  WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0 AND is_daily_active AND is_listing_fee_charged
  THEN active_status.vendor_code END
  ) AS total_listing_fee_vendor_signed_charged,
  
  COUNT(DISTINCT CASE 
  WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0 AND is_daily_active AND is_listing_fee_charged_tier_one
  THEN active_status.vendor_code END
  ) AS total_listing_fee_vendor_charged_my_tier_1,
  
  COUNT(DISTINCT CASE 
  WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0 AND is_daily_active AND is_listing_fee_charged_tier_two
  THEN active_status.vendor_code END
  ) AS total_listing_fee_vendor_charged_my_tier_2,
  
  COUNT(DISTINCT CASE 
  WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0 AND is_daily_active AND is_listing_fee_charged_tier_three
  THEN active_status.vendor_code END
  ) AS total_listing_fee_vendor_charged_my_tier_3,
  
  COUNT(DISTINCT CASE WHEN terminated_vendors.vendor_code IS NOT NULL THEN active_status.vendor_code END
  ) AS total_churn_vendor,
  
  COUNT(DISTINCT CASE WHEN terminated_vendors.vendor_code IS NOT NULL AND listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0
  THEN active_status.vendor_code END
  ) AS total_churn_vendor_with_listing_fee,
  
  COUNT(DISTINCT CASE 
  WHEN listing_fees_month.is_listing_fee_vendor AND listing_fees_month.total_amount_local > 0 AND DATE_TRUNC(DATE(activation_date), MONTH) = active_status.month
  THEN active_status.vendor_code END
  ) AS total_new_vendor_signed_listing,
  
  COUNT(DISTINCT CASE 
  WHEN DATE_TRUNC(DATE(activation_date), MONTH) = active_status.month
  THEN active_status.vendor_code END
  ) AS total_new_vendor
  
FROM active_status
LEFT JOIN listing_fee AS listing_fees_month
       ON active_status.global_entity_id = listing_fees_month.global_entity_id
      AND active_status.month = listing_fees_month.listing_fee_month
      AND active_status.vendor_code = listing_fees_month.vendor_code
LEFT JOIN (
  SELECT
    global_entity_id,
    listing_fee_month,
    SUM(listing_revenue) AS listing_revenue_total
  FROM listing_fee
  GROUP BY 1,2
) AS listing_fees_total
       ON active_status.global_entity_id = listing_fees_total.global_entity_id
      AND active_status.month = listing_fees_total.listing_fee_month
LEFT JOIN terminated_vendors
       ON active_status.global_entity_id = terminated_vendors.global_entity_id
      AND active_status.month = terminated_vendors.churned_month
      AND active_status.vendor_code = terminated_vendors.vendor_code
LEFT JOIN vendor_activation_date
       ON active_status.global_entity_id = vendor_activation_date.global_entity_id
      AND active_status.vendor_code = vendor_activation_date.vendor_code
GROUP BY 1,2,3
ORDER BY 1,2
