/* This query gives you all essential information of AM porflio */

# Country | City of Restaurants | Chain Code | Vendor Code | Vendor Name | GMV Class | Account Owner Name | Account Owner Email | Number of Chains | Number of Vendors

WITH vendors AS (
  SELECT 
      v.global_entity_id,
      v.chain_code,
      v.vendor_code,
      v.name AS vendor_name,
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS bt
         ON v.global_entity_id = bt.global_entity_id AND v.uuid = bt.uuid
  WHERE is_active
  AND is_test = FALSE
  AND is_private = FALSE
  AND v.global_entity_id LIKE 'FP_%'
  AND v.global_entity_id NOT IN ('FP_RO','FP_BG','FP_DE')),

gmv_class AS (
  SELECT
    global_entity_id,
    country_name,
    chain_code,
    vendor_code,
    vendor_name,
    gmv_class
  FROM `fulfillment-dwh-production.pandata_report.vendor_gmv_class`),

combine_vendor_information AS (
  SELECT 
    DISTINCT v.global_entity_id,
    v.chain_code,
    v.vendor_code,
    v.vendor_name,
    vendor_gmv_class.gmv_class

  FROM vendors AS v
  LEFT JOIN gmv_class AS vendor_gmv_class
         ON v.global_entity_id = vendor_gmv_class.global_entity_id
        AND v.vendor_code = vendor_gmv_class.vendor_code

ORDER BY 1, 5),

sf_information AS (
  SELECT
    a.global_entity_id                                                        AS global_entity_id,
    a.country_name                                                            AS country,
    a.restaurant_city                                                         AS city_of_restaurant,
    a.global_vendor_id                                                        AS grid_id,
    v.chain_code                                                              AS chain_code,
    v.vendor_code                                                             AS vendor_code,
    v.name                                                                    AS vendor_name,
    u.id                                                                      AS account_owner_id,
    u.title                                                                   AS account_owner_job_title, 
    u.full_name                                                               AS account_owner_name,
    u.email                                                                   AS account_owner_email,
    a.status                                                                  AS status_of_sf_account,


 --COUNT(DISTINCT v.chain_code)  OVER(PARTITION BY a.global_entity_id, u.id)  AS number_of_chains,
 --COUNT(DISTINCT v.vendor_code) OVER(PARTITION BY a.global_entity_id, u.id)  AS number_of_vendors

  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` a
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_users` u
         ON  a.global_entity_id = u.global_entity_id
        AND a.sf_owner_id = u.id
       
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v 
         ON a.global_entity_id = v.global_entity_id
        AND a.vendor_code = v.vendor_code



  WHERE u.is_active                                             -- account_owner is still active
  AND v.is_active                                               -- vendor is active
  AND v.is_private = FALSE                                      -- account is not private
  AND v.is_test = FALSE                                         -- account is not being tested
  AND a.is_deleted = FALSE                                      -- account is not deleted
  AND a.is_marked_for_testing_training = FALSE                  -- not marked for testing
  AND a.vertical = 'Restaurant'                                 -- only restaurant vendors considered
  AND a.global_entity_id LIKE 'FP_%'                            -- consider all foodpanda entities                                                    
  AND a.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE'))    -- exclude foodpanda romania, foodpanda bulgaria, foodpanda germany


SELECT 
  c.global_entity_id,
  sf.country,
  sf.city_of_restaurant,
  sf.grid_id,
  c.chain_code,
  c.vendor_code,
  c.vendor_name,
  c.gmv_class,
  sf.account_owner_id,
  sf.account_owner_job_title,
  sf.account_owner_name,
  sf.account_owner_email,
  COUNT(DISTINCT c.chain_code)  OVER(PARTITION BY c.global_entity_id, sf.account_owner_id)  AS number_of_chains,
  COUNT(DISTINCT c.vendor_code) OVER(PARTITION BY c.global_entity_id, sf.account_owner_id)  AS number_of_vendors
FROM combine_vendor_information AS c
LEFT JOIN sf_information AS sf
ON c.global_entity_id = sf.global_entity_id
AND c.vendor_code = sf.vendor_code
---- Add filters here -- 
--WHERE sf.account_owner_name IN ('Sonekeo Sibounheuang', 'Korlakod Maneephon')
-- AND c.vendor_code IN ('v6sg','d0ud','o7gz','f8qj','ezkp')
