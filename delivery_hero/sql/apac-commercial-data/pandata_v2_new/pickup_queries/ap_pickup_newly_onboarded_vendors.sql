WITH vd_list AS (
  SELECT *
  FROM `dhh---analytics-apac.pandata_ap_special_projects.ap_pickup_curated_vendors` AS vendor_list
  WHERE NOT is_not_supposed_to_be_on_pickup
),

vendor_status_by_day AS (
  (SELECT 
     active_vendor_excellence_ap.global_entity_id,
     active_vendor_excellence_ap.vendor_code,
     active_vendor_excellence_ap.pd_vendor_uuid,
     active_vendor_excellence_ap.date,
     active_vendor_excellence_ap.is_active,
     active_vendor_excellence_ap.is_private,
     active_vendor_excellence_ap.is_test,     
     active_vendor_excellence_ap.is_delivery_accepted,
     active_vendor_excellence_ap.is_pickup_accepted,
     active_vendor_excellence_ap.last_date_of_month,
     active_vendor_excellence_ap.vendor_type,
     active_vendor_excellence_ap.is_month_use
  FROM `dhh---analytics-apac.pandata_ap_commercial_external.active_vendor_excellence_ap` AS active_vendor_excellence_ap
  INNER JOIN vd_list
          ON vd_list.pd_vendor_uuid = active_vendor_excellence_ap.pd_vendor_uuid
         AND active_vendor_excellence_ap.DATE >= date_sub(current_date(),interval 3 month)
  )
  UNION ALL
  (SELECT 
     active_vendor_excellence_ap.global_entity_id,
     active_vendor_excellence_ap.vendor_code,
     active_vendor_excellence_ap.pd_vendor_uuid,
     active_vendor_excellence_ap.date,
     active_vendor_excellence_ap.is_active,
     active_vendor_excellence_ap.is_private,
     active_vendor_excellence_ap.is_test,     
     active_vendor_excellence_ap.is_delivery_accepted,
     active_vendor_excellence_ap.is_pickup_accepted,
     active_vendor_excellence_ap.last_date_of_month,
     active_vendor_excellence_ap.vendor_type,
     active_vendor_excellence_ap.is_month_use
   FROM `dhh---analytics-apac.pandata_ap_commercial_external.active_vendor_excellence_ap_current_month` AS active_vendor_excellence_ap
   INNER JOIN vd_list
          ON vd_list.pd_vendor_uuid = active_vendor_excellence_ap.pd_vendor_uuid
--          AND active_vendor_excellence_ap.DATE >= date_sub(current_date(),interval 3 month)
   WHERE is_current_month = TRUE
  ) 
),

vendor_status_pu AS (
  select
    vendor_status_by_day.*,
    LAG(IF(is_pickup_accepted AND is_active, true, false)) OVER (PARTITION BY vendor_status_by_day.global_entity_id, vendor_status_by_day.vendor_code
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
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),

vendor_status AS (
  SELECT
    *
  FROM vendor_status_pre
  WHERE is_active
   AND NOT is_test
),

/*new vendor*/

salesforce_contract AS (
  SELECT
    a.country_name,
    a.global_entity_id,
    o.id AS opportunity_id,
    a.vendor_code,
    o.stage_name,
    o.is_closed,
    a.global_vendor_id,
    o.close_date_local AS date,
    FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities` o
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` a
           ON a.id = o. sf_account_id  
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` pd_vendors
           ON a.global_entity_id = pd_vendors. global_entity_id
          AND a.vendor_code = pd_vendors. vendor_code
    WHERE o.stage_name = 'Closed Won'
      AND o.business_type IN ('New Business','Owner Change','Win Back')
      AND date(o. close_date_local ) >= date_sub(current_date, interval 90 day)
),

new_vendors_onboarded AS (
  SELECT 
   DISTINCT
    vd_list.global_entity_id,
    vd_list.vendor_code,
    pandora_pd_vendors_agg_activation_dates.sf_activation_date_local AS sf_account_activation_date,
    COALESCE(salesforce_contract.date, 
            pandora_pd_vendors_agg_activation_dates.sf_activation_date_local,
            pd_activation_date_local,NULL
      )  AS new_vendor_onboarded_date     
  FROM vd_list
  LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_vendors_agg_activation_dates` AS pandora_pd_vendors_agg_activation_dates
          ON pandora_pd_vendors_agg_activation_dates.global_entity_id = vd_list.global_entity_id
         AND pandora_pd_vendors_agg_activation_dates.vendor_code = vd_list.vendor_code 
  LEFT JOIN salesforce_contract
         ON salesforce_contract.global_entity_id = vd_list.global_entity_id
        AND salesforce_contract.vendor_code = vd_list.vendor_code 
),

/*--- get earliest date FROM  event table WHERE vendor is onboarded fct_ordersr delivery or PU*/
vendors_earliest_date AS (
  SELECT 
    new_vendors_onboarded.global_entity_id,
    new_vendors_onboarded.vendor_code,
    new_vendors_onboarded.new_vendor_onboarded_date,
    DATE(MIN(IF(vendor_status.is_pickup_accepted  , 
          vendor_status.date ,  NULL))) AS event_earliest_pickup_date,
    DATE(MIN(IF(vendor_status.is_active ,
                vendor_status.date, NULL))) AS event_earliest_start_date, 
  FROM  new_vendors_onboarded 
  LEFT JOIN vendor_status
         ON new_vendors_onboarded.global_entity_id = vendor_status.global_entity_id
        AND new_vendors_onboarded.vendor_code = vendor_status.vendor_code
  WHERE vendor_status.is_active
    AND NOT vendor_status.is_test 
  GROUP BY 1,2 ,3
),

new_vendors_list AS (
  select 
  DISTINCT
     vendors_earliest_date.*,
     vd_list.vendor_name,
     vd_list.chain_code ,
     vd_list.is_active_now,
     vd_list.has_delivery_type_pickup_now,
     vd_list.has_delivery_now,
     vd_list.chain_name,
     vd_list.country_name,
  FROM vendors_earliest_date
  INNER JOIN vd_list
         ON vd_list.global_entity_id = vendors_earliest_date.global_entity_id
        AND vd_list.vendor_code = vendors_earliest_date.vendor_code
  WHERE new_vendor_onboarded_date >= date_sub(current_date,interval 90 day)
    AND vd_list.vendor_code is not null
)

select 
    new_vendors_list.*,
    sf_accounts.global_vendor_id,
    sf_accounts.owner_name,
    sf_accounts.status,    
    sf_accounts.last_modified_at_utc,
    sf_accounts.id AS salesforce_id,
    sf_accounts.vendor_grade AS salesforce_vendor_grade,
    sf_accounts.name AS salesforce_account_name,
    sf_accounts.has_mark_up,
    sf_accounts.is_key_vip_account,
    sf_accounts.vertical AS salesforce_vertical,
    sf_accounts.vertical_segment AS salesforce_vertical_segment,   
from new_vendors_list
LEFT JOIN  `fulfillment-dwh-production.pandata_curated.sf_accounts` sf_accounts
       ON sf_accounts.global_entity_id = new_vendors_list.global_entity_id
      AND sf_accounts.vendor_code = new_vendors_list.vendor_code
--   ap_pickup_newly_onboarded_vendors
