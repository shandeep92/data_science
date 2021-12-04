WITH month_start AS (
SELECT
DATE('2020-01-01') AS start_date
),

rating_base AS (
  SELECT 
     marvin_reviews.global_entity_id,
     vendors.vendor_code AS vendor_code,
     AVG(COALESCE(IF(ratings.name = 'restaurant_food', ratings.value, NULL), NULL)) AS avg_restaurant_food_rating,
     AVG(COALESCE(IF(ratings.name = 'packaging', ratings.value, NULL), NULL)) AS avg_packaging_rating,
     AVG(COALESCE(IF(ratings.name = 'rider', ratings.value, NULL), NULL)) AS avg_rider_rating,
     AVG(COALESCE(IF(ratings.name = 'punctuality', ratings.value, NULL), NULL)) AS avg_punctuality_rating
  FROM `fulfillment-dwh-production.pandata_curated.marvin_reviews` AS marvin_reviews
  CROSS JOIN UNNEST (ratings) AS ratings
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors
         ON vendors.vendor_code = marvin_reviews.vendor_code
        AND vendors.global_entity_id = marvin_reviews.global_entity_id
        AND NOT vendors.is_test
  WHERE marvin_reviews.created_date_utc <= CURRENT_DATE
    AND (marvin_reviews.global_entity_id LIKE 'FP_%' AND marvin_reviews.global_entity_id NOT IN ('FP_RO', 'FP_BG'))
  GROUP BY 1,2
  ORDER BY 1 DESC
),

cuisine_type AS (
SELECT
  pd_vendors.global_entity_id,
  pd_vendors.vendor_code,
  COALESCE(CAST(pd_vendors.primary_cuisine AS STRING), CAST(cuisine.cuisine_title AS STRING)) AS primary_cuisine
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
LEFT JOIN (
  WITH h AS (
    SELECT 
      global_entity_id,
      vendor_code,
      names.value AS cuisine_title,
      ROW_NUMBER() OVER (PARTITION BY global_entity_id, vendor_code ORDER BY global_entity_id, vendor_code) AS row_number
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors,
         UNNEST(cuisines) AS cuisines
    LEFT JOIN UNNEST(cuisines.names) as names
    WHERE names.locale LIKE 'en-%'
    AND cuisines.type = 'cuisine'
    ORDER BY 1,2
  )
  SELECT
  global_entity_id,
  vendor_code,
  cuisine_title
  FROM h
  WHERE row_number = 1
) cuisine
 ON pd_vendors.global_entity_id = cuisine.global_entity_id
AND pd_vendors.vendor_code = cuisine.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_countries` AS pd_countries
       ON pd_countries.global_entity_id = pd_vendors.global_entity_id
LEFT JOIN pandata.dim_vendors
       ON pd_countries.pd_rdbms_id = dim_vendors.rdbms_id
      AND pd_vendors.vendor_code = dim_vendors.vendor_code
WHERE (pd_vendors.global_entity_id LIKE 'FP_%' AND pd_vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG'))
  AND NOT pd_vendors.is_test
ORDER BY 3
),

vendor_base_info AS (
SELECT 
    v.rdbms_id,
    pd_countries.global_entity_id,
    v.country_name AS country,
    v.vendor_code,
    v.business_type,
    COALESCE(cuisine_type.primary_cuisine, v.primary_cuisine) AS primary_cuisine,
    is_key_vip_account AS aaa_type,
    CASE 
      WHEN vendor_gmv_class.gmv_class IS NULL 
      THEN NULL
      ELSE vendor_gmv_class.gmv_class
    END AS gmv_class,
    v.chain_code
  FROM `dhh---analytics-apac.pandata.dim_vendors` v
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_countries` AS pd_countries
         ON pd_countries.pd_rdbms_id = v.rdbms_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` sf_accounts_custom
         ON sf_accounts_custom.vendor_code = v.vendor_code 
        AND sf_accounts_custom.global_entity_id = pd_countries.global_entity_id
  LEFT JOIN `dhh---analytics-apac.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = v.vendor_code 
        AND vendor_gmv_class.rdbms_id = v.rdbms_id
  LEFT JOIN cuisine_type
         ON cuisine_type.vendor_code = v.vendor_code 
        AND cuisine_type.global_entity_id = pd_countries.global_entity_id
  WHERE NOT v.is_vendor_testing
),

dates AS (
  SELECT
    vendors.*,
    date,
    iso_year_week_string,
    weekday_name
  FROM pandata.dim_dates
  CROSS JOIN(
    SELECT 
      DISTINCT rdbms_id, 
      vendor_code 
    FROM pandata.dim_vendors
  ) AS vendors
  WHERE date >= (
  SELECT start_date
  FROM month_start
  )
    AND date <= CURRENT_DATE
  ORDER BY date
),

vendor_status_by_day AS (
  SELECT *
  FROM (
    SELECT
      dates.*,
      fct_vendor_events.* EXCEPT (rdbms_id, vendor_id),
      row_number() OVER (PARTITION BY dates.rdbms_id, dates.vendor_code, dates.date ORDER BY fct_vendor_events.created_at_utc DESC) = 1 AS is_latest_entry
    FROM dates
    LEFT JOIN pandata.dim_vendors AS vendors
           ON dates.rdbms_id = vendors.rdbms_id
          AND dates.vendor_code = vendors.vendor_code
    LEFT JOIN pandata.fct_vendor_events
           ON fct_vendor_events.rdbms_id = dates.rdbms_id
          AND fct_vendor_events.vendor_id = vendors.id
          AND fct_vendor_events.created_date_utc <= dates.date
    WHERE fct_vendor_events.vendor_id IS NOT NULL
  )
  WHERE is_latest_entry
),

active_vendors_daily AS (
  SELECT
    vendor_status_by_day.date AS day,
    FORMAT_DATE("%G-%V",vendor_status_by_day.date) AS week,
    DATE_TRUNC(vendor_status_by_day.date, MONTH) AS month,
    dim_vendors.business_type,
    vendor_base_info.aaa_type,
    vendor_base_info.gmv_class,
    vendor_status_by_day.rdbms_id,
    dim_vendors.country_name,
    vendor_status_by_day.vendor_code,
    dim_vendors.chain_code,
    dim_vendors.chain_name,
    CASE
      WHEN vendor_status_by_day.is_active AND NOT vendor_status_by_day.is_deleted AND NOT vendor_status_by_day.is_private
      THEN TRUE
      ELSE FALSE
    END AS is_daily_active
  FROM vendor_status_by_day
  LEFT JOIN pandata.dim_vendors
         ON dim_vendors.rdbms_id = vendor_status_by_day.rdbms_id
        AND dim_vendors.vendor_code = vendor_status_by_day.vendor_code
  LEFT JOIN vendor_base_info
         ON vendor_status_by_day.rdbms_id = vendor_base_info.rdbms_id
        AND vendor_status_by_day.vendor_code = vendor_base_info.vendor_code
  WHERE TRUE
    AND NOT vendor_status_by_day.is_vendor_testing
    AND NOT vendor_status_by_day.is_deleted
    AND (
      LOWER(dim_vendors.vendor_name) NOT LIKE '% test%'
      OR LOWER(dim_vendors.vendor_name) NOT LIKE 'test %'
      OR LOWER(dim_vendors.vendor_name) NOT LIKE '%pos integra%'
      OR LOWER(dim_vendors.vendor_name) NOT LIKE '%billing%'
    )
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,vendor_status_by_day.is_active,vendor_status_by_day.is_deleted,vendor_status_by_day.is_private
),

active_status AS (
  SELECT
    global_entity_id,
    active_vendors_daily.rdbms_id,
    country_name AS country,
    day AS date_local,
    active_vendors_daily.month,
    active_vendors_daily.vendor_code,
    active_vendors_daily.business_type,
    active_vendors_daily.gmv_class,
    active_vendors_daily.chain_code,
    active_vendors_daily.chain_name,
    CASE WHEN active_vendors_daily.aaa_type THEN 'AAA' ELSE 'Non-AAA' END AS ka_type,
    CASE WHEN active_vendors_daily.is_daily_active OR daily_all_valid_orders_1 > 0 THEN TRUE ELSE FALSE END AS is_daily_active
  FROM active_vendors_daily
  LEFT JOIN (
    SELECT
      fct_orders.rdbms_id,
      fct_orders.vendor_code,
      DATE(fct_orders.date_local) AS date_local,
      COUNT(DISTINCT(fct_orders.id)) AS daily_all_valid_orders_1
    FROM `dhh---analytics-apac.pandata.fct_orders` AS fct_orders
    WHERE fct_orders.date_local >= (
      SELECT start_date
      FROM month_start
      )
      AND fct_orders.created_date_local <= CURRENT_DATE
      AND fct_orders.date_local <= CURRENT_DATE
      AND NOT fct_orders.is_test_order
      AND fct_orders.is_gross_order
      AND fct_orders.is_valid_order
    GROUP BY 1,2,3
    ) AS daily_orders
         ON active_vendors_daily.rdbms_id = daily_orders.rdbms_id
        AND active_vendors_daily.day = daily_orders.date_local
        AND active_vendors_daily.vendor_code = daily_orders.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_countries` AS countries
         ON countries.pd_rdbms_id = active_vendors_daily.rdbms_id
  WHERE active_vendors_daily.business_type = 'restaurants'
    AND active_vendors_daily.day >= (
      SELECT start_date
      FROM month_start
      )
),

won_opportunities as (
  select
    distinct rdbms_id,
    sf_account_id
  from `dhh---analytics-apac.pandata.sf_opportunities`
  where stage_name = 'Closed Won'
),

terminated_vendors AS (
    SELECT
      global_entity_id,
      sf.vendor_code,
      MAX(DATE(end_date_local)) as last_terminated_date 
    FROM (
      SELECT DISTINCT
        v_dim_countries.rdbms_id,
        contract.id AS contract_id,
        contract.account_id,
        contract.contract_number,
        LOWER(TRIM(SUBSTR(platform_performance_c.backend_id_c, 5))) AS vendor_code,
        account.record_country_c AS country_name,
        contract.status,
        account.name AS account_name,
        account.account_status_c AS account_status,
        account.mark_for_testing_training_c AS is_account_marked_for_testing,
        contract.termination_reason_c AS termination_reason,
        contract.created_date AS created_date_local,
        contract.activated_date AS activated_date_local,
        contract.start_date AS start_date_local,
        contract.last_activity_date AS last_activity_date_local,
        contract.last_modified_date AS last_modified_date_local,
        contract.end_date_c AS end_date_local
      FROM `dhh---analytics-apac.salesforce.contract` AS contract
      JOIN `dhh---analytics-apac.salesforce.account` AS account
        ON account.id = contract.account_id
      LEFT JOIN `dhh---analytics-apac.salesforce.platform_performance_c` AS platform_performance_c
             ON platform_performance_c.account_c = contract.account_id
      LEFT JOIN `dhh---analytics-apac.il_backend_latest.v_dim_countries` AS v_dim_countries
             ON account.record_country_c = v_dim_countries.common_name
            AND LOWER(v_dim_countries.company_name) = 'foodpanda'
      WHERE v_dim_countries.rdbms_id IS NOT NULL      
    ) sf
    LEFT JOIN `dhh---analytics-apac.pandata.dim_vendors` AS dim_vendors
           ON dim_vendors.rdbms_id = sf.rdbms_id
          AND dim_vendors.vendor_code = sf.vendor_code 
    LEFT JOIN won_opportunities
           ON won_opportunities.rdbms_id = sf.rdbms_id
          AND won_opportunities.sf_account_id = sf.account_id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_countries` AS countries
           ON countries.pd_rdbms_id = sf.rdbms_id
    WHERE sf.account_status = 'Terminated'
      AND won_opportunities.sf_account_id is not null
      AND NOT termination_reason IN ('Onboarding Failed', 'Duplicate')
      AND NOT sf.is_account_marked_for_testing
      AND DATE(end_date_local) >= (
                                    SELECT start_date
                                    FROM month_start
                                    )
    GROUP BY global_entity_id, sf.vendor_code
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
  COALESCE(sf_activation_date.activated_date_local, DATE(pd_vendors.activated_at_local), MIN(DATE(vendor_flows.start_date)), first_valid_order) AS activation_date
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
LEFT JOIN `fulfillment-dwh-production.dl_pandora.ml_be_vendorflows` AS vendor_flows
       ON vendor_flows.global_entity_id = pd_vendors.global_entity_id
      AND vendor_flows.vendor_id = pd_vendors.vendor_id
      AND vendor_flows.type = 'ACTIVE'
      AND DATE(vendor_flows.created_at) <= CURRENT_DATE()
      AND vendor_flows.value = '1'
LEFT JOIN sf_activation_date
       ON sf_activation_date.global_entity_id = vendor_flows.global_entity_id
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
   ON orders.global_entity_id = vendor_flows.global_entity_id
  AND orders.vendor_code = pd_vendors.vendor_code
WHERE (pd_vendors.global_entity_id LIKE 'FP_%' AND pd_vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG'))
  AND NOT is_test
GROUP BY
  pd_vendors.global_entity_id,
  pd_vendors.vendor_code,
  sf_activation_date.activated_date_local,
  pd_vendors.activated_at_local,
  first_valid_order
),

ga_data_fixed AS (
  SELECT
    a.*,
    REGEXP_EXTRACT(userid,"-(.*)") AS user_id,
    c.index,
    c.value AS country_id,
  FROM `dhh---rps-portal.176081791.ga_sessions_*` a,
        unnest(customDimensions) AS c
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', (SELECT start_date FROM month_start)) 
        AND FORMAT_DATE('%Y%m%d', DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))
    AND c.index = 6
),

ga_data AS (
  SELECT
    a.*,
    entity_id,
    rdbms_id,
    common_name AS country
  FROM ga_data_fixed a          
  JOIN pandata.dim_countries b 
    ON a.country_id = b.entity_id
),

active_on_rp AS (
  SELECT
    DISTINCT
    CAST(FORMAT_TIMESTAMP("%Y-%m-%d", PARSE_TIMESTAMP("%Y%m%d", b.date )) AS DATE) AS period_date,
    b.rdbms_id,
    b.userid,
    1 AS active_on_rp,
    a.*
  FROM ga_data b
  JOIN `dhh---analytics-apac.ncr_restaurant_portal_latest.user_restaurant` a 
    ON cast(a.user_id AS STRING) = b.user_id 
   AND a.dwh_source_code = b.entity_id,
       UNNEST(hits) AS h
  WHERE lower(eventinfo.eventaction) NOT IN ('login.clicked','login.failed') /*remove log in failure */
    AND lower(page.pagePath) NOT LIKE '%/login?redirect=/%' /*remove log in failure */
),

daily_rp_vendors AS (
  SELECT
    period_date,
    x.rdbms_id,
    userid,
    active_on_rp,
    dwh_source_code,
    restaurant_id,
    backend_id,
    user_id,
    business_type
  FROM active_on_rp x
  LEFT JOIN (
    SELECT
      rdbms_id,
      vendor_code,
      business_type
    FROM `dhh---analytics-apac.pandata.dim_vendors`
  ) y ON x.rdbms_id = y.rdbms_id
     AND x.backend_id = y.vendor_code
),

last_login AS (
SELECT
  dim_vendors.business_type AS vendor_type,
  daily_rp_vendors.rdbms_id,
  dim_vendors.country_name,
  backend_id,
  MAX(daily_rp_vendors.period_date) as last_login
FROM daily_rp_vendors
LEFT JOIN pandata.dim_vendors
       ON dim_vendors.rdbms_id = daily_rp_vendors.rdbms_id
      AND dim_vendors.vendor_code = daily_rp_vendors.backend_id
GROUP BY vendor_type, rdbms_id, country_name, backend_id
),

last_active_date AS (
SELECT
active_status.global_entity_id,
active_status.rdbms_id,
active_status.country,
active_status.vendor_code,
dim_vendors.vendor_name,
dim_vendors.business_type,
active_status.ka_type,
vendor_gmv_class.gmv_class,
MAX(IF(is_daily_active,date_local,NULL)) AS last_active_date
FROM active_status
LEFT JOIN pandata.dim_vendors
       ON dim_vendors.rdbms_id = active_status.rdbms_id
      AND dim_vendors.vendor_code = active_status.vendor_code
LEFT JOIN pandata_report.vendor_gmv_class
       ON vendor_gmv_class.rdbms_id = active_status.rdbms_id
      AND vendor_gmv_class.vendor_code = active_status.vendor_code
GROUP BY 1,2,3,4,5,6,7,8
),

vendor_contact AS (
  SELECT
    vendors.global_entity_id,
    vendors.uuid,
    vendors.vendor_code,
    accounts.owner_name AS account_manager,
    vendors.contact_number AS vendor_contact_phone,
    owners.name AS vendor_name_first_row,
    owners.email AS vendor_email_first_row
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors
  LEFT JOIN (
  WITH h AS (
    SELECT 
      global_entity_id,
      vendor_code,
      owners.name AS name,
      owners.email AS email,
      ROW_NUMBER() OVER (PARTITION BY global_entity_id, vendor_code ORDER BY global_entity_id, vendor_code) AS row_number
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors,
         UNNEST(owners) AS owners
    ORDER BY 1,2
  )
  SELECT
  global_entity_id,
  vendor_code,
  name,
  email
  FROM h
  WHERE row_number = 1
  ) owners
  ON vendors.global_entity_id = owners.global_entity_id
  AND vendors.vendor_code = owners.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS accounts
         ON vendors.uuid = accounts.vendor_uuid
  WHERE (vendors.global_entity_id LIKE 'FP_%' AND vendors.global_entity_id NOT IN ('FP_RO', 'FP_BG'))
),

-----median of each vendor----
mediantable as (
select 
* from (select 
rdbms_id, vendor_code,
percentile_disc (gfv_local,0.5) over (PARTITION BY rdbms_id,vendor_code) as median_local
from pandata.fct_orders o
where date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH) AND date_local <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY)
and o.created_date_local >= '2020-01-01'
and is_valid_order=true
) 
group by 1,2,3
),
---dim_vendors_1_zone-----
dvzone as (
SELECT *
FROM (
select 
*,
ROW_NUMBER() OVER (PARTITION BY rdbms_id,vendor_code ORDER BY lg_zone_id) AS row_number
from pandata.dim_vendors v, unnest(lg_zone_ids) as lg_zone_id) v

WHERE 
v.is_active=true and v.is_deleted= false and v.is_vendor_testing=false and is_private = false and customers_type!='corporate'
and row_number = 1
),

fct_orders as (
SELECT 
rdbms_id,
vendor_code,
format_date('%Y-%m', o.date_local) as order_month,
COUNT(DISTINCT case when o.is_gross_order=true then o.id end) AS gross_order,
COUNT(DISTINCT case when o.is_valid_order=true then o.id end) as successful_order ,
COUNT (DISTINCT case when o.is_failed_order_vendor=true then o.id end) as vendor_fails,
COUNT(DISTINCT case when o.is_valid_order= true and o.is_discount_used IS TRUE and o.discount_ratio < 100 then o.id end) as discounted_order ,
SUM (case when o.is_valid_order=true then o.gfv_local end) as gfv_local,
COUNT (DISTINCT case when o.is_valid_order=true then o.customer_id end) as unique_customer,
COUNT (DISTINCT case when o.is_valid_order=true and o.is_first_valid_order=true then o.customer_id end) as new_to_fp_customer,
COUNT (DISTINCT case when o.is_valid_order=true and o.is_first_valid_order_with_this_vendor =true then o.customer_id end) as new_to_rest_customer

FROM pandata.fct_orders o

where o.created_date_local >= '2020-01-01'
and o.date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH) AND o.date_local <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY)
GROUP BY 1,2,3
),
----commercial metrics by vendor----
orders as (
Select 
v.country_name,
v.rdbms_id,
v.vendor_name,
v.id as vendor_id,
v.vendor_code,
v.city_name,
v.rating,
hz.name as hurrier_zone,
hz.id as zone_id,
v.chain_name,
o.order_month,
v.vendor_type,
v.is_loyalty_enabled,
v.loyalty_percentage,
o.gross_order,
o.successful_order,
o.vendor_fails,
o.discounted_order,
o.gfv_local,
o.unique_customer,
o.new_to_fp_customer,
o.new_to_rest_customer

FROM dvzone v
LEFT JOIN fct_orders o on v.rdbms_id = o.rdbms_id and v.vendor_code = o.vendor_code
left join pandata.lg_zones hz on v.rdbms_id = hz.rdbms_id and v.lg_zone_id= hz.id
),
-- conversion_rate and sessions -- 
conversion as (
SELECT
v.rdbms_id,
vcr.country,
--vcr.vendor_name,
vcr.vendor_code,
format_date('%Y-%m', vcr.date) as order_month,
Greatest(SUM(count_of_shop_list_loaded),0) as cr2_start,
Greatest(SUM(count_of_shop_menu_loaded),0) as cr3_start,
Greatest(SUM(count_of_transaction),0) as cr4_end
FROM pandata_ap_product_external.vendor_level_session_metrics vcr
left join pandata.dim_vendors v on v.country_name=vcr.country and v.vendor_code=vcr.vendor_code
WHERE vcr.date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH) AND vcr.date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY)
and v.is_active=true and v.is_deleted=false and v.is_vendor_testing=false and is_private = false and customers_type!= 'corporate'
GROUP BY 1,2,3,4
),
--- mode----
modetable as (
select 
order_month,
om.rdbms_id,
om.vendor_code,
om.gfv_round as lowest_mode_local,
seqnum
from (select
format_date('%Y-%m', o.date_local) as order_month,
v.rdbms_id,
v.vendor_code,
round(o.gfv_local,0) as gfv_round,
COUNT(DISTINCT case when o.is_valid_order=true then o.id end) as successful_order,
row_number () over (Partition by v.rdbms_id, v.vendor_code order by v.rdbms_id, v.vendor_code, "successful_order" desc, "gfv_round" asc) as seqnum
FROM pandata.dim_vendors v
LEFT JOIN pandata.fct_orders o on v.rdbms_id=o.rdbms_id and v.id=o.vendor_id
WHERE o.date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH) AND o.date_local <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY)
and  v.is_active=true and v.is_deleted=false and v.is_vendor_testing=false and v.customers_type!='corporate' and is_private = false
and o.created_date_local >= '2020-01-01'
group by 1,2,3,4
order by v.rdbms_id, v.vendor_code, successful_order desc, gfv_round asc
) om
where seqnum = 1
),

-- OPENING HOURS --
offline AS (SELECT
f.rdbms_id,
f.country,
f.vendor_id,
f.vendor_name,
f.vendor_code,
format_date('%Y-%m', f.report_date) as report_month,
SUM (f.open_hours) as total_open,
SUM (f.closed_hours) as total_closed,
SUM (case when f.close_events_vendor_device=1 then f.closed_hours_vendor_device end) as self_closed,
SUM (case when f.close_events_monitor_unreachable_offline=1 then f.closed_hours_monitor_unreachable_offline end) as monitor_unreachable,
SUM (case when f.close_events_order_declined=1 then f.closed_hours_order_declined end) as decline_closed
FROM pandata_report.restaurant_offline f
LEFT JOIN pandata.dim_vendors v on v.id=f.vendor_id and f.rdbms_id=v.rdbms_id
where v.is_active is true and v.is_deleted is false and v.is_vendor_testing is false and v.is_private is false
group by 1,2,3,4,5,6
),
-- MENU INFORMATION --
menu as (SELECT
rdbms_id,
vendor_code,
count (distinct case when is_product_active is true and is_product_deleted is false then product_id end ) as total_product,
count (distinct case when is_product_active is true and is_product_deleted is false and has_dish_image is true then product_id end) as has_picture,
count (distinct case when is_product_active is true and is_product_deleted is false and product_description is not null then product_id end ) as has_description,
FROM `pandata.dim_vendor_product_variations`
WHERE is_menu_active is true and is_live is true and is_vendor_deleted is false and is_menu_deleted is false
GROUP BY 1,2),
----joker bookings----
jokerbookings as (
select 
rdbms_id,
JSON_EXTRACT_SCALAR(parameters, "$.vendor_code") as vendor_code,
JSON_EXTRACT_SCALAR(parameters, "$.units") as units
from `fulfillment-dwh-production.pandata_curated.pps_bookings`
where type = 'joker'
and status = 'open'
),
----cpc bookings----
cpcbookings as (
select 
bk.rdbms_id,
JSON_EXTRACT_SCALAR(parameters, "$.vendor_code") as vendor_code,
blcpc.click_price,
blcpc.initial_budget as initial_budget_local
from `fulfillment-dwh-production.pandata_curated.pps_bookings`  bk, UNNEST(bk.cpc_billings) blcpc
--left join il_pps_latest.v_pps_billing_cpc blcpc on bk.rdbms_id=blcpc.rdbms_id and bk.id=blcpc.booking_id
where bk.billing_type = 'CPC'
and bk.status = 'open'
group by 1,2,3,4
),
---eligible vendors---
vendorslive as(
select
dv.rdbms_id,
common_name,
dv.lg_zone_id,
count (distinct dv.vendor_code) as vendors_available
from dvzone dv
left join pandata.dim_countries c on c.rdbms_id = dv.rdbms_id
where dv.is_active is true and is_private is false and is_vendor_testing is false and dv.is_deleted is false
and customers_type != 'corporate'
and dv.is_active is true
and dv.business_type = 'restaurants'
group by 1,2,3
),
---click inventory---
clickinv as ( 
select 
country,
format_date('%Y-%m', date) as order_month,
hz.name as zone_name,
lc.name as lg_city_name,
hz.id as zone_id,
sum (case when (vendor_click_origin = 'list' or vendor_click_origin = 'List') and safe_cast (vendor_position as INt64) <11 then clicks end ) as click_inv
from dhh-digital-analytics-dwh.shared_views_to_pandata.click_positions_apac cl
left join pandata.dim_countries c on c.common_name=cl.country
left join pandata.lg_zones hz on c.rdbms_id = hz.rdbms_id and cl.zone_id= hz.id
left join pandata.lg_cities lc on hz.rdbms_id = lc.rdbms_id and hz.lg_city_id = lc.id
where
(country = 'Singapore' or country = 'Hong Kong' or country = 'Philippines' or country =  'Thailand' or country =  'Taiwan' or country = 'Pakistan' or country = 'Bangladesh' or country = 'Cambodia' or country = 'Myanmar' or country = 'Laos' or country = 'Malaysia' or country = 'Japan')
and date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH) AND date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY)
and lc.name is not null
group by 1,2,3,4,5--,6,7--,8
),

cpcrec as(
select
ci.country,
zone_id,
vendors_available,
click_inv,
safe_divide (click_inv,(0.05*vendors_available) ) as max_clicks_pv
from clickinv ci
left join vendorslive vl on vl.common_name = ci.country and zone_id = lg_zone_id
),

vendor_data AS (
---Orders + CVR + Mode + Median + Open hours + Menu ----
Select 
orders.country_name,
orders.rdbms_id,
orders.vendor_name,
orders.vendor_code,
orders.city_name,
orders.hurrier_zone,
orders.chain_name,
orders.order_month,
orders.rating,
orders.vendor_type,
orders.is_loyalty_enabled,
orders.loyalty_percentage,
jbk.units as pandabox_units_live,
concat (cpc.click_price,' , ', cpc.initial_budget_local) as CPC_bid_budget,
SAFE_DIVIDE(cr.cr4_end, cr.cr3_start)*SAFE_DIVIDE(gfv_local, successful_order) as cpcfactor_z,
orders.successful_order,
orders.gfv_local,
orders.new_to_rest_customer,
SAFE_DIVIDE(new_to_rest_customer, unique_customer) as NC_proportion,
SAFE_DIVIDE(successful_order, unique_customer) as frequency,
cr.cr2_start as shop_loads,
cr.cr3_start as sessions,
SAFE_DIVIDE(cr.cr3_start, cr.cr2_start) as click_through_rate,
SAFE_DIVIDE(cr.cr4_end, cr.cr3_start) as conversion_rate,
SAFE_DIVIDE(gfv_local, successful_order) as avb_local,
SAFE_DIVIDE(vendor_fails, gross_order) as fail_rate,
t.total_open as total_open_hours,
SAFE_DIVIDE(t.total_closed, t.total_open) as closed_perc,
SAFE_DIVIDE(t.self_closed, t.total_open) as selfclosed_perc,
SAFE_DIVIDE (t.monitor_unreachable, t.total_open) as offline_perc,
SAFE_DIVIDE (t.decline_closed, t.total_open) as declineclosed_perc,
menu.total_product,
SAFE_DIVIDE (menu.has_description, menu.total_product ) as description_perc,
SAFE_DIVIDE (menu.has_picture, menu.total_product ) as picture_perc,
least(mdt.median_local,mt.lowest_mode_local,SAFE_DIVIDE(gfv_local, successful_order)) as Deal_Max_MOV,
cast (max_clicks_pv as int64) as max_clicks,
--cast(cpcr.vendor_clicks_predicted_monthly as INT64) as recommended_clicks
from orders 
left join conversion cr on cr.rdbms_id=orders.rdbms_id and cr.vendor_code=orders.vendor_code and cr.order_month=orders.order_month
left join modetable mt on mt.rdbms_id=orders.rdbms_id and orders.order_month=mt.order_month and mt.vendor_code=orders.vendor_code
left join mediantable mdt on mdt.rdbms_id = orders.rdbms_id and orders.vendor_code=mdt.vendor_code
left join offline t on t.rdbms_id=orders.rdbms_id and t.vendor_code=orders.vendor_code and t.report_month=orders.order_month
left join menu on menu.rdbms_id=orders.rdbms_id and menu.vendor_code=orders.vendor_code
left join jokerbookings jbk on jbk.rdbms_id=orders.rdbms_id and jbk.vendor_code=orders.vendor_code
left join cpcbookings cpc on cpc.rdbms_id=orders.rdbms_id and cpc.vendor_code=orders.vendor_code
left join cpcrec cpcr on cpcr.country=orders.country_name and cpcr.zone_id = orders.zone_id
LEFT JOIN pandata.dim_vendors v on v.rdbms_id=orders.rdbms_id and v.vendor_code=orders.vendor_code
WHERE v.business_type = 'restaurants'
order by vendor_code asc
),

percentile_calculation AS (
  SELECT
    vendor_data.rdbms_id,
    
    /*CPC*/
    (
      approx_quantiles(vendor_data.fail_rate,100)[OFFSET(CASE WHEN threshold.product = 'cost_per_click' THEN fail_rate_percentile END)]
    ) AS cpc_fail_rate,
    (
    approx_quantiles(vendor_data.click_through_rate,100)[OFFSET(CASE WHEN threshold.product = 'cost_per_click' THEN click_through_rate_percentile END)]
    ) AS cpc_click_through_rate,
    (
    approx_quantiles(vendor_data.cpcfactor_z,100)[OFFSET(CASE WHEN threshold.product = 'cost_per_click' THEN cpc_factor_percentile END)]
    ) AS cpc_cpc_factor_percentile,
    
    /*Deals*/
    (
    approx_quantiles(vendor_data.fail_rate,100)[OFFSET(CASE WHEN threshold.product = 'deals' THEN fail_rate_percentile END)]
    ) AS deals_fail_rate,
    
    /*PandaBox/Joker*/
    (
    approx_quantiles(vendor_data.fail_rate,100)[OFFSET(CASE WHEN threshold.product = 'pandabox_joker' THEN fail_rate_percentile END)]
    ) AS pandabox_joker_fail_rate,
    (
    approx_quantiles(vendor_data.click_through_rate,100)[OFFSET(CASE WHEN threshold.product = 'pandabox_joker' THEN click_through_rate_percentile END)]
    ) AS pandabox_click_through_rate,
    (
    approx_quantiles(vendor_data.conversion_rate,100)[OFFSET(CASE WHEN threshold.product = 'pandabox_joker' THEN conversion_percentile END)]
    ) AS pandabox_conversion,
    (
    approx_quantiles(vendor_data.frequency,100)[OFFSET(CASE WHEN threshold.product = 'pandabox_joker' THEN frequency_percentile END)]
    ) AS pandabox_frequency,
    
    /*CPP*/
    (
    approx_quantiles(vendor_data.fail_rate,100)[OFFSET(CASE WHEN threshold.product = 'premium_placement' THEN fail_rate_percentile END)]
    ) AS premium_placement_fail_rate,
    (
    approx_quantiles(vendor_data.conversion_rate,100)[OFFSET(CASE WHEN threshold.product = 'premium_placement' THEN conversion_percentile END)]
    ) AS premium_placement_conversion,
    gfv_local_min_price AS premium_placement_gfv_local_min_price
    
  FROM vendor_data
  LEFT JOIN pandata_ap_commercial_external.vendor_product_recommendation_threshold AS threshold
  ON vendor_data.rdbms_id = threshold.rdbms_id
  GROUP BY 1,threshold.product,fail_rate_percentile,click_through_rate_percentile,cpc_factor_percentile,conversion_percentile,frequency_percentile,gfv_local_min_price
),

threshold_grouping AS (
  SELECT
    rdbms_id,
    MAX(cpc_fail_rate) AS cpc_fail_rate,
    MAX(cpc_click_through_rate) AS cpc_click_through_rate,
    MAX(cpc_cpc_factor_percentile) AS cpc_cpc_factor_percentile,
    MAX(deals_fail_rate) AS deals_fail_rate,
    MAX(pandabox_joker_fail_rate) AS pandabox_joker_fail_rate,
    MAX(pandabox_click_through_rate) AS pandabox_click_through_rate,
    MAX(pandabox_conversion) AS pandabox_conversion,
    MAX(pandabox_frequency) AS pandabox_frequency,
    MAX(premium_placement_fail_rate) AS premium_placement_fail_rate,
    MAX(premium_placement_conversion) AS premium_placement_conversion,
    MAX(premium_placement_gfv_local_min_price) AS premium_placement_gfv_local_min_price
  FROM percentile_calculation
  GROUP BY 1
),

product_recommended AS (
SELECT
vendor_data.rdbms_id,
vendor_data.vendor_code,
CASE 
WHEN (CAST(vendor_data.pandabox_units_live AS INT64) < 1 OR vendor_data.pandabox_units_live IS NULL) AND vendor_data.frequency >= pandabox_frequency 
     AND vendor_data.fail_rate <= pandabox_joker_fail_rate AND vendor_data.conversion_rate >= pandabox_conversion 
     AND vendor_data.click_through_rate >= pandabox_click_through_rate
THEN TRUE ELSE FALSE 
END AS is_pandabox_recommended,
CASE 
WHEN vendor_data.fail_rate <= deals_fail_rate
THEN TRUE ELSE FALSE 
END AS is_deals_recommended,
CASE 
WHEN vendor_data.fail_rate <= premium_placement_fail_rate AND vendor_data.gfv_local >= premium_placement_gfv_local_min_price
AND vendor_data.conversion_rate >= premium_placement_conversion
THEN TRUE ELSE FALSE 
END AS is_cpp_recommended,
CASE 
WHEN vendor_data.fail_rate <= cpc_fail_rate AND vendor_data.click_through_rate >= cpc_click_through_rate
 AND vendor_data.cpcfactor_z >= cpc_cpc_factor_percentile
THEN TRUE ELSE FALSE 
END AS is_cpc_recommended
FROM vendor_data
LEFT JOIN threshold_grouping
       ON threshold_grouping.rdbms_id = vendor_data.rdbms_id
)

SELECT
  last_active_date.*,
  vendor_contact.* EXCEPT(global_entity_id, vendor_code),
  last_login.* EXCEPT(country_name, rdbms_id, backend_id,vendor_type),
  CASE WHEN last_login.last_login IS NOT NULL THEN TRUE ELSE FALSE END AS has_login_rps_once,
  CASE WHEN DATE_DIFF(CURRENT_DATE, last_login.last_login, DAY) < 7 AND last_login.last_login IS NOT NULL
  THEN 'FREQUENT'
  WHEN DATE_DIFF(CURRENT_DATE, last_login.last_login, DAY) >= 8 AND DATE_DIFF(CURRENT_DATE, last_login.last_login, DAY) <= 30 AND last_login.last_login IS NOT NULL
  THEN 'INFREQUENT'
  ELSE 'INACTIVE'
  END AS vendor_rps_login_type,
  vendor_activation_date.* EXCEPT(global_entity_id, vendor_code),
  DATE_DIFF(CURRENT_DATE, vendor_activation_date.activation_date, DAY) AS age_on_fp_days,
  cuisine_type.* EXCEPT(global_entity_id, vendor_code),
  rating_base.* EXCEPT(global_entity_id, vendor_code),
  CASE 
    WHEN product_recommended.is_pandabox_recommended
    THEN TRUE ELSE FALSE 
  END AS is_pandabox_recommended,
  CASE 
    WHEN product_recommended.is_deals_recommended
    THEN TRUE ELSE FALSE 
  END AS is_deals_recommended,
  CASE 
    WHEN product_recommended.is_cpp_recommended
    THEN TRUE ELSE FALSE 
  END AS is_cpp_recommended,
  CASE 
    WHEN product_recommended.is_cpc_recommended
    THEN TRUE ELSE FALSE 
  END AS is_cpc_recommended,
  CASE WHEN MAX(user_restaurant.backend_id) IS NOT NULL THEN TRUE ELSE FALSE END AS has_rps_account
FROM last_active_date
LEFT JOIN vendor_contact
       ON vendor_contact.global_entity_id = last_active_date.global_entity_id
      AND vendor_contact.vendor_code = last_active_date.vendor_code
LEFT JOIN last_login
       ON last_login.rdbms_id = last_active_date.rdbms_id
      AND last_login.backend_id = last_active_date.vendor_code
LEFT JOIN vendor_activation_date
       ON vendor_activation_date.global_entity_id = last_active_date.global_entity_id
      AND vendor_activation_date.vendor_code = last_active_date.vendor_code
LEFT JOIN rating_base
       ON rating_base.global_entity_id = last_active_date.global_entity_id
      AND rating_base.vendor_code = last_active_date.vendor_code
LEFT JOIN cuisine_type
       ON cuisine_type.global_entity_id = last_active_date.global_entity_id
      AND cuisine_type.vendor_code = last_active_date.vendor_code
LEFT JOIN product_recommended
       ON product_recommended.rdbms_id = last_active_date.rdbms_id
      AND product_recommended.vendor_code = last_active_date.vendor_code
LEFT JOIN `dhh---analytics-apac.ncr_restaurant_portal_latest.user_restaurant` AS user_restaurant
       ON user_restaurant.backend_id = last_active_date.vendor_code
      AND user_restaurant.dwh_source_code = last_active_date.global_entity_id
WHERE last_active_date.business_type = 'restaurants'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
