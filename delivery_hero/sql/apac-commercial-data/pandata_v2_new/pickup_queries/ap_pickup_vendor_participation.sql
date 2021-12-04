WITH vd_list AS (
  SELECT *
  FROM `dhh---analytics-apac.pandata_ap_special_projects.ap_pickup_curated_vendors` AS vendor_list
  WHERE NOT is_not_supposed_to_be_on_pickup
  /*Shop, pandamart, some new verticals depending ON  countries*/
),

daily_order_by_vendor AS (
  SELECT
    pd_orders.global_entity_id,
    pd_orders.country_name,
    pd_orders.pd_vendor_uuid,
    pd_orders.vendor_code,
    pd_vendors.chain_code,
    DATE(pd_orders.ordered_at_local) AS date,
    COUNT(pd_orders.uuid) AS all_valid_orders,
    COUNT(if(expedition_type='pickup',pd_orders.uuid,null)) AS pickup_valid_orders,
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
           ON shared_countries.global_entity_id = pd_orders.global_entity_id
          AND shared_countries.management_entity = 'Foodpanda APAC'
          AND shared_countries.global_entity_id like 'FP%'
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
           ON pd_vendors.uuid = pd_orders.pd_vendor_uuid   
  WHERE pd_orders.created_date_utc >=DATE_SUB(CURRENT_DATE(),INTERVAL 7 month)
    AND is_gross_order
    AND is_valid_order   
  GROUP BY 1,2,3,4,5,6
),

vendor_status_by_day_previous_months AS (
  (SELECT 
     active_vendor_excellence_ap.global_entity_id,
     active_vendor_excellence_ap.vendor_code,
     active_vendor_excellence_ap.pd_vendor_uuid,
     active_vendor_excellence_ap.date,
     active_vendor_excellence_ap.is_active,
     active_vendor_excellence_ap.is_test,
     active_vendor_excellence_ap.is_private,
     active_vendor_excellence_ap.is_delivery_accepted,
     active_vendor_excellence_ap.is_pickup_accepted,
     active_vendor_excellence_ap.last_date_of_month,
     active_vendor_excellence_ap.vendor_type,
     active_vendor_excellence_ap.is_month_use,
     vd_list.gmv_class,
     vd_list.city_name,
  FROM `dhh---analytics-apac.pandata_ap_commercial_external.active_vendor_excellence_ap`  AS active_vendor_excellence_ap
  INNER JOIN vd_list
          ON vd_list.pd_vendor_uuid = active_vendor_excellence_ap.pd_vendor_uuid
         AND active_vendor_excellence_ap.DATE >= date_sub(current_date(),interval 7 month)
  )
  UNION ALL
  (SELECT 
     active_vendor_excellence_ap.global_entity_id,
     active_vendor_excellence_ap.vendor_code,
     active_vendor_excellence_ap.pd_vendor_uuid,
     active_vendor_excellence_ap.date,
     active_vendor_excellence_ap.is_active,
     active_vendor_excellence_ap.is_test,
     active_vendor_excellence_ap.is_private,
     active_vendor_excellence_ap.is_delivery_accepted,
     active_vendor_excellence_ap.is_pickup_accepted,
     active_vendor_excellence_ap.last_date_of_month,
     active_vendor_excellence_ap.vendor_type,
     active_vendor_excellence_ap.is_month_use,
     vd_list.gmv_class,
     vd_list.city_name,    
   FROM `dhh---analytics-apac.pandata_ap_commercial_external.active_vendor_excellence_ap_current_month` AS active_vendor_excellence_ap
   INNER JOIN vd_list
          ON  vd_list.pd_vendor_uuid = active_vendor_excellence_ap.pd_vendor_uuid
--          AND active_vendor_excellence_ap.DATE >= date_sub(current_date(),interval 7 month)
   WHERE is_current_month = TRUE
   
  ) 
),

vendor_status_by_day AS (
  SELECT
   *,
   is_delivery_accepted AS has_delivery_type_all,
   is_pickup_accepted AS has_delivery_type_pickup,
  FROM vendor_status_by_day_previous_months
),

vendor_status_pu AS (
  select
    vendor_status_by_day.*,
    LAG(IF(is_pickup_accepted AND is_active, true, false)) 
        OVER (PARTITION BY 
                vendor_status_by_day.global_entity_id,
                vendor_status_by_day.vendor_code
    ORDER BY date ) AS preceding_pickup_accepted_status
  FROM vendor_status_by_day
),

vendor_status_pre AS (
  SELECT
  vendor_status_pu.*,
  LOGICAL_OR(
    NOT is_pickup_accepted AND preceding_pickup_accepted_status 
    AND is_delivery_accepted AND is_active
  ) AS has_churned_still_active_delivery,
  LOGICAL_OR(NOT is_pickup_accepted AND preceding_pickup_accepted_status) AS has_churned, 
  FROM vendor_status_pu
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
),

vendor_status AS (
  SELECT
  vendor_status_pre.*,
  date_trunc(vendor_status_pre.date,isoweek) AS week,
  date_trunc(vendor_status_pre.date,month) AS month,  
  daily_order_by_vendor.all_valid_orders,
   daily_order_by_vendor.pickup_valid_orders,
  FROM vendor_status_pre
  LEFT JOIN daily_order_by_vendor
         ON  daily_order_by_vendor.pd_vendor_uuid = vendor_status_pre.pd_vendor_uuid
        AND daily_order_by_vendor.date = vendor_status_pre.date
  WHERE is_active
   AND NOT is_test
),

chain_order_by_month AS ( 
  SELECT 
    global_entity_id,
    country_name,
    chain_code,
    date_trunc(date,month) AS month,
    SUM(all_valid_orders) AS all_valid_orders_month,
    SUM(pickup_valid_orders) AS pickup_valid_orders_month,
  FROM daily_order_by_vendor
  GROUP BY 1,2,3,4
),

chain_rank_month AS (
  SELECT 
    chain_order_by_month.*,
    ROW_NUMBER() OVER (PARTITION BY global_entity_id,month ORDER BY all_valid_orders_month DESC) AS chain_rank,
  FROM chain_order_by_month
),

vendor_x_dates AS (
  SELECT DISTINCT
    global_entity_id,
    date,
    city_name,
    gmv_class,
    pd_vendor_uuid,
    left(pd_vendor_uuid, 4) AS vendor_code,
  FROM vendor_status_by_day
),

vendors_deals_available AS (
  select
    vendor_x_dates.global_entity_id,
    vendor_x_dates.date,
    vendor_x_dates.pd_vendor_uuid,
    vendor_x_dates.city_name,
    vendor_x_dates.gmv_class,
    ap_pickup_deals_history.pd_discount_uuid,
    ap_pickup_deals_history. start_date_local,
    ap_pickup_deals_history.end_date_local ,
    ap_pickup_deals_history.expedition_type AS expedition_types,
    ap_pickup_deals_history.amount_local,
    ap_pickup_deals_history.discount_type,
    ap_pickup_deals_history.foodpanda_ratio,
    pd_discounts.condition_type,
    pd_discounts.is_subscription_discount,
    pd_discounts.platforms,
    ROW_NUMBER() OVER (PARTITION BY  vendor_x_dates.date,    vendor_x_dates.pd_vendor_uuid
                       ORDER BY 
                          if(ap_pickup_deals_history.expedition_type !='delivery',1,2),
                          pd_discounts.created_at_utc DESC
                       ) AS deals_rank_on_pickup,
    ROW_NUMBER() OVER (PARTITION BY  vendor_x_dates.date,    vendor_x_dates.pd_vendor_uuid
                       ORDER BY 
                          if(ap_pickup_deals_history.expedition_type !='pickup',1,2),
                          pd_discounts.created_at_utc DESC
                       ) AS deals_rank_on_delivery,
    ROW_NUMBER() OVER (PARTITION BY  vendor_x_dates.date,    vendor_x_dates.pd_vendor_uuid
                       ORDER BY 
                          if(ap_pickup_deals_history.expedition_type ='pickup',1,2),
                          pd_discounts.created_at_utc DESC
                       ) AS deals_rank_on_pickup_evegreen,          
  FROM vendor_x_dates
  INNER JOIN `dhh---analytics-apac.pandata_ap_special_projects.ap_pickup_deals_history` AS ap_pickup_deals_history
          ON ap_pickup_deals_history. global_entity_id= vendor_x_dates.global_entity_id
         AND  ap_pickup_deals_history. vendor_code= vendor_x_dates.vendor_code
         AND ap_pickup_deals_history.min_date_local <= vendor_x_dates.date
         AND ap_pickup_deals_history.max_date_local >= vendor_x_dates.date
         AND ap_pickup_deals_history.pd_discount_uuid is not null
         AND ap_pickup_deals_history.active_discount
         AND ap_pickup_deals_history.active_at_vendor
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_discounts` AS pd_discounts
         ON   pd_discounts.uuid = ap_pickup_deals_history.pd_discount_uuid
),

vendors_deals_available_delivery_ranked AS (
  select
    global_entity_id,
    date,
    pd_vendor_uuid,
    city_name,
    gmv_class,
    pd_discount_uuid AS delivery_deal_discount_id,
    amount_local AS delivery_deal_amount_local,
    discount_type AS delivery_deal_discount_type,
    condition_type AS delivery_deal_condition_type,
    foodpanda_ratio AS delivery_deal_foodpanda_ratio,
    is_subscription_discount AS delivery_deal_pro_exclusion,
    platforms AS specific_platforms_exclusion,
    CASE
      WHEN foodpanda_ratio=100 THEN 'full foodpanda'
      when foodpanda_ratio>0 AND foodpanda_ratio<100 THEN 'co-funded'
      when foodpanda_ratio=0 THEN 'full vendor'
      else null
    end AS delivery_deal_funding_type,
  FROM vendors_deals_available
  where deals_rank_on_delivery=1
    AND expedition_types!='pickup'
),

vendors_deals_available_pickup_ranked AS (
  select
    global_entity_id,
    date,
    pd_vendor_uuid,
    city_name,
    gmv_class,
    pd_discount_uuid AS pickup_deal_discount_id,
    amount_local AS pickup_deal_amount_local,
    discount_type AS pickup_deal_discount_type,
    condition_type AS pickup_deal_condition_type,
    foodpanda_ratio AS pickup_deal_foodpanda_ratio,
    CASE
      WHEN foodpanda_ratio=100 THEN 'full foodpanda'
      when foodpanda_ratio>0 AND foodpanda_ratio<100 THEN 'co-funded'
      when foodpanda_ratio=0 THEN 'full vendor'
      else null
    end AS pickup_deal_funding_type,
  FROM vendors_deals_available
  where deals_rank_on_pickup=1
    AND expedition_types!='delivery'
),

vendors_deals_available_pickup_evergreen_ranked AS (
  select
    global_entity_id,
    date,
    pd_vendor_uuid,
    city_name,
    gmv_class,
    pd_discount_uuid AS pickup_evergreen_deal_discount_id,
    amount_local AS pickup_evergreen_deal_amount_local,
    discount_type AS pickup_evergreen_deal_discount_type,
    condition_type AS pickup_evergreen_deal_condition_type,
    foodpanda_ratio AS pickup_evergreen_deal_foodpanda_ratio,
  FROM vendors_deals_available
  where deals_rank_on_pickup_evegreen=1
    AND expedition_types='pickup'
    AND foodpanda_ratio=100
    AND discount_type='percentage'
),

exclusions_list AS (
  select
   pd_vendor_uuid,
   is_excluded_from_stacking,
   is_no_evergreen_vendor,
  FROM `fulfillment-dwh-production.pandata_app.stack_deal__current_details` AS stack_deal__current_details
  where is_no_evergreen_vendor or is_excluded_from_stacking
),

vendors_deals_collapsed AS (
  SELECT
    vendor_x_dates.global_entity_id,
    vendor_x_dates.date,
    vendor_x_dates.pd_vendor_uuid,
    vendor_x_dates.city_name,
    vendor_x_dates.gmv_class,
    delivery_deal_discount_id,
    delivery_deal_amount_local,
    delivery_deal_discount_type,
    delivery_deal_condition_type,
    delivery_deal_foodpanda_ratio,
    delivery_deal_funding_type,
    coalesce(delivery_deal_pro_exclusion,false) AS delivery_deal_pro_exclusion,
    coalesce(coalesce(specific_platforms_exclusion,'')!='',false) AS specific_platforms_exclusion,
    pickup_deal_discount_id,
    pickup_deal_amount_local,
    pickup_deal_discount_type,
    pickup_deal_condition_type,
    pickup_deal_foodpanda_ratio,
    pickup_deal_funding_type,
    coalesce( is_excluded_from_stacking,false) AS is_excluded_from_stacking,
    coalesce( is_no_evergreen_vendor,false) AS is_no_evergreen_vendor,
    pickup_evergreen_deal_amount_local,
  FROM vendor_x_dates
  LEFT JOIN vendors_deals_available_pickup_ranked
         ON  vendors_deals_available_pickup_ranked.pd_vendor_uuid = vendor_x_dates.pd_vendor_uuid
        AND vendors_deals_available_pickup_ranked.date = vendor_x_dates.date
  LEFT JOIN vendors_deals_available_delivery_ranked
         ON  vendors_deals_available_delivery_ranked.pd_vendor_uuid = vendor_x_dates.pd_vendor_uuid
        AND vendors_deals_available_delivery_ranked.date = vendor_x_dates.date
  LEFT JOIN vendors_deals_available_pickup_evergreen_ranked
         ON  vendors_deals_available_pickup_evergreen_ranked.pd_vendor_uuid = vendor_x_dates.pd_vendor_uuid
        AND vendors_deals_available_pickup_evergreen_ranked.date = vendor_x_dates.date
  LEFT JOIN exclusions_list
         ON  exclusions_list.pd_vendor_uuid =vendor_x_dates.pd_vendor_uuid
),

vendors_deals_count AS (
  SELECT
    vendor_status.global_entity_id,
    vendor_status.date,
    vendor_status.city_name,
    vendor_status.gmv_class,    
    COUNT( IF ( 
            vendor_status.is_active AND vendor_status.has_delivery_type_pickup
            AND is_no_evergreen_vendor, deals_tagged.pd_vendor_uuid, null)) 
        AS has_evergreen_exclusion_vendors,
    COUNT( IF ( vendor_status.is_active AND vendor_status.has_delivery_type_pickup 
          AND delivery_deal_discount_type='percentage' AND is_excluded_from_stacking
         AND coalesce( delivery_deal_pro_exclusion ,false)
         AND (NOT specific_platforms_exclusion OR specific_platforms_exclusion IS NULL), deals_tagged.pd_vendor_uuid, null))
       AS stacking_exclusion_vendors,
    COUNT( IF ( is_active AND has_delivery_type_pickup AND not is_private 
         AND (NOT delivery_deal_pro_exclusion or  delivery_deal_pro_exclusion is null  )
         AND (NOT specific_platforms_exclusion OR specific_platforms_exclusion IS NULL)
         AND delivery_deal_discount_type='percentage' AND coalesce(delivery_deal_amount_local,0)>0
         AND delivery_deal_funding_type in ('co-funded','full vendor')
         , deals_tagged.pd_vendor_uuid, null)) AS vendor_with_deal_on_delivery_base,
    COUNT( IF ( is_active AND has_delivery_type_pickup AND not is_private 
         AND (NOT delivery_deal_pro_exclusion or  delivery_deal_pro_exclusion is null  )
         AND (NOT specific_platforms_exclusion OR specific_platforms_exclusion IS NULL)
         AND delivery_deal_discount_type='percentage'
         AND delivery_deal_funding_type in ('co-funded','full vendor')
         AND ((pickup_deal_funding_type ='co-funded' 
                AND COALESCE(pickup_deal_amount_local,0)>coalesce(delivery_deal_amount_local,0)
                AND delivery_deal_discount_type='percentage')
              or 
             (pickup_deal_amount_local>0 AND  
              COALESCE(pickup_deal_amount_local,0)=coalesce(delivery_deal_amount_local,0) 
              AND delivery_deal_discount_type='percentage')
             )
         , deals_tagged.pd_vendor_uuid, null)) AS vendor_with_on_par_or_stack_deal_vs_delivery_base,
    COUNT( IF ( is_active AND has_delivery_type_pickup AND not is_private 
         AND (NOT delivery_deal_pro_exclusion or  delivery_deal_pro_exclusion is null  )
         AND (NOT specific_platforms_exclusion OR specific_platforms_exclusion IS NULL)
         AND delivery_deal_discount_type='percentage'
         AND delivery_deal_funding_type in ('co-funded','full vendor')
         AND is_excluded_from_stacking
         , deals_tagged.pd_vendor_uuid, null)) AS exclusion_vendor_with_deal_on_delivery_base,
    COUNT( IF ( is_active AND has_delivery_type_pickup AND not is_private , deals_tagged.pd_vendor_uuid, null)) AS vendors_to_have_evergreen_base,
    COUNT( IF ( is_active AND has_delivery_type_pickup 
                      AND not is_private AND 
                      ((pickup_deal_discount_id IS NOT NULL AND pickup_deal_funding_type !='full vendor')
                        OR pickup_evergreen_deal_amount_local IS NOT NULL
     ), deals_tagged.pd_vendor_uuid, null)) AS vendors_with_evergreen_vs_base,
    COUNT( IF ( is_active AND has_delivery_type_pickup
                      AND not is_private AND is_no_evergreen_vendor, deals_tagged.pd_vendor_uuid, null)) AS evergreen_exclusion_vendors_vs_base,
  FROM vendor_status
  LEFT JOIN vendors_deals_collapsed AS  deals_tagged
         ON vendor_status.date = deals_tagged.date
         AND vendor_status.pd_vendor_uuid = deals_tagged.pd_vendor_uuid
 
  GROUP BY 1,2,3,4
),

orders_before_churning AS (
  SELECT
    vendor_status.pd_vendor_uuid,
    vendor_status.date AS churn_date,
    SUM(if(daily_order_by_vendor.date >= date_sub( vendor_status.date, interval 30 day) AND daily_order_by_vendor.date <= date_sub( vendor_status.date, interval 0 day) , 
       daily_order_by_vendor. pickup_valid_orders,null)) AS L30days_orders_before_churn
  FROM vendor_status
  INNER JOIN daily_order_by_vendor 
          ON vendor_status.pd_vendor_uuid = daily_order_by_vendor.pd_vendor_uuid
         AND daily_order_by_vendor.date >= date_sub( vendor_status.date, interval 30 day) 
         AND daily_order_by_vendor.date <= date_sub( vendor_status.date, interval 1 day)
  where has_churned_still_active_delivery
  GROUP BY 1,2
),

count_country_without_new AS (
  SELECT
    vd_list.country_name,
    vendor_status.global_entity_id,
    vendor_status.city_name,
    vendor_status.date,
    vendor_status.week,
    vendor_status.month,
    vendor_status.gmv_class,
    COUNT( IF(vendor_status.has_delivery_type_all AND vendor_status.is_active, vendor_status.vendor_code,null)) AS delivery_active_vendors,
    COUNT( IF(vendor_status.has_delivery_type_pickup AND vendor_status.is_active,vendor_status.vendor_code,null)) AS pickup_active_vendors,
    COUNT( IF(vendor_status.has_churned_still_active_delivery,vendor_status.vendor_code,null)) AS churned_still_on_delivery_vendors,
    SUM(all_valid_orders) AS all_vendors_orders,
    SUM(IF(vendor_status.has_delivery_type_pickup AND vendor_status.is_active,all_valid_orders,null)) AS vendors_on_pu_orders,
    COUNT(DISTINCT IF(vendor_status.has_delivery_type_all AND vendor_status.is_active AND (chain_rank<=100) AND not vendor_status.is_private
          ,vendor_status.vendor_code,null)) AS top_100_delivery_active_vendors,
    COUNT(DISTINCT IF(vendor_status.has_delivery_type_pickup AND vendor_status.is_active AND chain_rank<=100 AND not vendor_status.is_private,
          vendor_status.vendor_code,null)) AS top_100_pickup_active_vendors,
    COUNT(DISTINCT IF(vendor_status.has_delivery_type_all AND vendor_status.is_active AND is_aaa_vendor AND not vendor_status.is_private
          ,vendor_status.vendor_code,null)) AS aaa_delivery_active_vendors,
    COUNT(DISTINCT IF(vendor_status.has_delivery_type_pickup AND vendor_status.is_active AND is_aaa_vendor AND not vendor_status.is_private,
          vendor_status.vendor_code,null)) AS aaa_pickup_active_vendors,
    SUM(pickup_valid_orders) AS all_pickup_orders,
    COUNT(distinct if(vendor_status.has_churned_still_active_delivery AND not vd_list.has_delivery_type_pickup_now,vendor_status .vendor_code,null)) AS churned_not_pickup_active_now,
    COUNT(if(vendor_status.has_churned_still_active_delivery, vendor_status .vendor_code,null)) AS churned_flag_count,
    SUM(if (vendor_status.has_churned_still_active_delivery AND not vd_list.has_delivery_type_pickup_now, L30days_orders_before_churn,0)) AS orders_before_churn_30days,
    FROM vendor_status
    INNER JOIN vd_list
           ON vendor_status.pd_vendor_uuid = vd_list.pd_vendor_uuid

    LEFT JOIN chain_rank_month
           ON chain_rank_month.global_entity_id = vendor_status.global_entity_id
          AND chain_rank_month.chain_code = vd_list.chain_code
          AND chain_rank_month.month = vendor_status.month
    LEFT JOIN orders_before_churning
           ON  orders_before_churning.pd_vendor_uuid = vendor_status.pd_vendor_uuid
          AND orders_before_churning.churn_date = vendor_status.date
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1,2,3,4,5,6,7
),  


onboard_vendor_count AS (
  select  
    ap_pickup_newly_onboarded_vendors.country_name,
    ap_pickup_newly_onboarded_vendors.global_entity_id,
    new_vendor_onboarded_date AS date,
    date_trunc(new_vendor_onboarded_date,isoweek) AS week,
    date_trunc(new_vendor_onboarded_date,month) AS month,
    city_name,
    COALESCE(gmv_class,' ') AS gmv_class,
    COUNT(distinct if(true ,ap_pickup_newly_onboarded_vendors.vendor_code,null)) AS onboarded_on_fp,
    COUNT(distinct  IF( event_earliest_pickup_date<= event_earliest_start_date AND event_earliest_pickup_date is not null ,
                        ap_pickup_newly_onboarded_vendors.vendor_code,null)) AS onboarded_on_pickup,
  FROM pandata_ap_special_projects.ap_pickup_newly_onboarded_vendors
  INNER JOIN vd_list
          ON  vd_list.global_entity_id = ap_pickup_newly_onboarded_vendors.global_entity_id
         AND vd_list.vendor_code = ap_pickup_newly_onboarded_vendors.vendor_code
  GROUP BY 1,2,3,4,5,6,7
),

count_country AS (
  select 
    
    count_country_without_new.*,
    onboard_vendor_count.* except (country_name,
      global_entity_id,
      date,
      week,
      month,
      city_name,
      gmv_class),
    vendors_deals_count.* except (
      global_entity_id,
      date,
      city_name,
      gmv_class),
  FROM count_country_without_new
  LEFT JOIN onboard_vendor_count
         ON  onboard_vendor_count.global_entity_id = count_country_without_new.global_entity_id
        AND onboard_vendor_count.date = count_country_without_new.date
        AND onboard_vendor_count.city_name = count_country_without_new.city_name
        AND onboard_vendor_count.gmv_class = count_country_without_new.gmv_class
  LEFT JOIN vendors_deals_count
         ON  vendors_deals_count.global_entity_id = count_country_without_new.global_entity_id
        AND vendors_deals_count.date = count_country_without_new.date
        AND vendors_deals_count.city_name = count_country_without_new.city_name
        AND vendors_deals_count.gmv_class = count_country_without_new.gmv_class
),

count_country_participation AS (
  select 
    ap_pickup_available_country_flag_imported.country_name,
    city_name,
    date,
    ap_pickup_available_country_flag_imported.month,
    gmv_class,
    pickup_available_flag,
    onboarded_on_fp,
    onboarded_on_pickup,
    delivery_active_vendors,
    pickup_active_vendors,
    churned_still_on_delivery_vendors,
    all_vendors_orders,
    vendors_on_pu_orders,
    all_pickup_orders,
    top_100_delivery_active_vendors,
    top_100_pickup_active_vendors,
    aaa_delivery_active_vendors,
    aaa_pickup_active_vendors, 
    vendor_with_deal_on_delivery_base,
    vendor_with_on_par_or_stack_deal_vs_delivery_base,
    exclusion_vendor_with_deal_on_delivery_base,
    vendors_to_have_evergreen_base,
    vendors_with_evergreen_vs_base,
    evergreen_exclusion_vendors_vs_base,
    churned_not_pickup_active_now,
    churned_flag_count,
    orders_before_churn_30days,    
  FROM count_country
  LEFT JOIN pandata_ap_special_projects.ap_pickup_available_country_flag_imported
         ON ap_pickup_available_country_flag_imported.country_name=count_country.country_name
        AND ap_pickup_available_country_flag_imported.month=count_country.month
),

APAC_participation AS (
  SELECT 
    'APAC' AS country_name,
    'APAC' AS city_name,
    date,
    date_trunc(date,month) AS month,
    ' ' AS gmv_class,
    1 AS pickup_available_flag,
    SUM (onboarded_on_fp) AS onboarded_on_fp,
    SUM (onboarded_on_pickup) AS onboarded_on_pickup,
    SUM (delivery_active_vendors) AS delivery_active_vendors,
    SUM (pickup_active_vendors) AS pickup_active_vendors,
    SUM (churned_still_on_delivery_vendors) AS churned_still_on_delivery_vendors,
    SUM (all_vendors_orders) AS all_vendors_orders,
    SUM (vendors_on_pu_orders) AS vendors_on_pu_orders,
    SUM (all_pickup_orders) AS all_pickup_orders,
    SUM(top_100_delivery_active_vendors) AS top_100_delivery_active_vendors,
    SUM(top_100_pickup_active_vendors) AS top_100_pickup_active_vendors,
    SUM(aaa_delivery_active_vendors) AS aaa_delivery_active_vendors,
    SUM(aaa_pickup_active_vendors) AS aaa_pickup_active_vendors,
    SUM(vendor_with_deal_on_delivery_base) AS vendor_with_deal_on_delivery_base,
    SUM(vendor_with_on_par_or_stack_deal_vs_delivery_base) AS vendor_with_on_par_or_stack_deal_vs_delivery_base,
    SUM(exclusion_vendor_with_deal_on_delivery_base) AS exclusion_vendor_with_deal_on_delivery_base, 
    SUM(vendors_to_have_evergreen_base) AS vendors_to_have_evergreen_base, 
    SUM(vendors_with_evergreen_vs_base) AS vendors_with_evergreen_vs_base, 
    SUM(evergreen_exclusion_vendors_vs_base) AS evergreen_exclusion_vendors_vs_base,    
    SUM(churned_not_pickup_active_now) AS churned_not_pickup_active_now,
    SUM(churned_flag_count) AS churned_flag_count,
    SUM(orders_before_churn_30days) AS orders_before_churn_30days,    
  FROM count_country_participation
  WHERE pickup_available_flag = 1
  GROUP BY 1,2,3,4,5,6
)

SELECT * FROM count_country_participation
UNION ALL
SELECT * FROM APAC_participation
ORDER BY date DESC, country_name
--pandata_ap_special_projects.ap_pickup_daily_vendor_participation



