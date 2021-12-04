WITH vendor_first_zone AS (
  SELECT
    DISTINCT
    pd_vendors_agg_lg_zones.global_entity_id,
    pd_vendors_agg_lg_zones.vendor_code,
    lg_zones.lg_zone_id,
    lg_zones.lg_zone_name,
    row_number() over (partition by pd_vendors_agg_lg_zones.global_entity_id,
                                  pd_vendors_agg_lg_zones.vendor_code) AS areas_count
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` as pd_vendors_agg_lg_zones,  unnest(lg_zones) as lg_zones
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = pd_vendors_agg_lg_zones.global_entity_id
  WHERE shared_countries.management_entity = 'Foodpanda APAC'
),

excluded_on_pickup_vendors AS(
  SELECT
  DISTINCT
    pd_vendors.uuid AS pd_vendor_uuid,
    
    TRUE AS is_not_supposed_to_be_on_pickup,
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors, UNNEST(attributes) AS attributes
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = pd_vendors.uuid
  WHERE (attributes.type LIKE '%characteristic%'
            /*excluded islandwide delivery & hawker vendors in SG*/
            /*BD & PK home-chefs and caterers are still identified by food characteristics*/
          AND ((pd_vendors.global_entity_id='FP_SG' AND (LOWER(attributes.name) LIKE '%hawker%'or LOWER(attributes.name) LIKE '%islandwide%' ))
                OR (pd_vendors.global_entity_id IN('FP_BD' ,'FP_PK')
                  AND (
                    LOWER(attributes.name) LIKE '%meals by home%' OR LOWER(attributes.name) LIKE '%kitchen%'
                    OR LOWER(attributes.name) LIKE '%cater%' OR LOWER(attributes.name) LIKE '%home%base%'
                    )
                  )
            )
         )
    or (LOWER(pd_vendors.name ) like '%pandago%')
    or (LOWER(pd_vendors.name ) like '% test %')
    OR (pd_vendors.global_entity_id  LIKE 'ODR%')
    OR (is_kitchens OR is_dmart OR is_caterers
         OR is_shops OR is_pandago OR is_home_based_kitchen)
),

vd_list AS (
  SELECT DISTINCT
    shared_countries.name as country_name,
    pd_vendors.global_entity_id,
    pd_vendors.vendor_code,
    pd_vendors.name as vendor_name,
    pd_vendors.uuid AS pd_vendor_uuid,
    coalesce(pd_vendors.location.city,' ')  as city_name,
    pd_vendors.chain_code,
    pd_vendors.chain_name,
    -- pd_vendors.postcode,
    (pd_vendors.has_delivery_type_vendor_delivery or has_delivery_type_platform_delivery or has_delivery_type_partner_delivery) as has_delivery_now,
    pd_vendors.has_delivery_type_pickup as has_delivery_type_pickup_now,
    pd_vendors.is_active as is_active_now,
    pd_vendors.is_test as is_test_now,  
    pd_vendors.is_private as is_private_now,  
    round(pd_vendors.location.latitude/0.001,0)*0.001 AS lat,
    round(pd_vendors.location.longitude/0.001,0)*0.001 AS lon,
    pd_vendors.primary_cuisine,
    COALESCE(vendor_gmv_class.gmv_class,' ') as gmv_class,
    -- null as gmv_class,
    case when vendor_grade='AAA' then true else false end as is_aaa_vendor,
    vendor_grade as salesforce_vendor_grade,
    vendor_first_zone.lg_zone_id,
    vendor_first_zone.lg_zone_name AS area_name,
    CASE
    WHEN pd_vendors.location.city in 
         ('Dhaka',
          'Hong Kong',
          'Kuala Lumpur', 'Johor Bahru','Petaling Jaya','Kota Kinabalu','Penang',
          'Quezon City','Makati City','Davao City Davao','Taguig City','Cebu City Cebu','Manila',
          'Singapore',
          'Taichung City','New Taipei City','Taoyuan City','KaoHsiung City','Taipei City','Tainan City',
          'Bangkok','Chiang Mai','Phuket',
          'Nakhon Ratchasima','Yangon','Naypyitaw',
          'Phnom Penh','Vientiane',
          'Karachi','Lahore','Faisalabad','Rawalpindi', 'Gujranwala','Peshawar','Multan','Isalamabad','Hyderabad')
    THEN pd_vendors.location.city
    ELSE 'Smaller city'
    END AS city_name_short,
    pd_vendors.created_date_utc as vendor_created_date_local,
    COALESCE(is_not_supposed_to_be_on_pickup, FALSE) AS is_not_supposed_to_be_on_pickup,
    business_type_apac,
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors, UNNEST(attributes) AS attributes 
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON shared_countries.global_entity_id = pd_vendors.global_entity_id
  LEFT JOIN (SELECT DISTINCT 
              global_entity_id, vendor_code, vendor_grade
              FROM `fulfillment-dwh-production.pandata_curated.sf_accounts`
            ) AS sf_accounts
         ON sf_accounts.global_entity_id = pd_vendors.global_entity_id
        AND sf_accounts.vendor_code = pd_vendors.global_entity_id
  LEFT JOIN vendor_first_zone
         ON vendor_first_zone. global_entity_id = pd_vendors. global_entity_id
        AND vendor_first_zone. vendor_code = pd_vendors. vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` as vendor_gmv_class
         ON vendor_gmv_class.global_entity_id = pd_vendors.global_entity_id
        and vendor_gmv_class.vendor_code = pd_vendors.vendor_code
  LEFT JOIN excluded_on_pickup_vendors
         ON excluded_on_pickup_vendors.pd_vendor_uuid = pd_vendors.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors_agg_business_types.uuid = pd_vendors.uuid
  WHERE shared_countries.management_entity = 'Foodpanda APAC'
    AND areas_count = 1
)

select * from vd_list

-- dhh---analytics-apac.pandata_ap_special_projects.ap_pickup_curated_vendors

