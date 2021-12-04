-- Query aggregates data on an AM level:
-- 1. For every AM what is the number of GMV_A, GMV_B, GMV_C, GMV_D vendors compared to breakdown on a country level for GMV_A, GMV_B, GMV_C, GMV_D vendors
-- 2. We also see average number of vendors per chain for every country (Excluding vendors w/o Chain Codes as that would mean a standalone vendor)
-- 3. Vendors_per_owner = Total_vendors_country/Number_of_owners_country
-- ** All owners include AMs and non-AMs

WITH vendors AS (
  SELECT 
      v.global_entity_id,
      v.chain_code,
      v.vendor_code,
      v.name as vendor_name,
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors`                         AS v
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS bt
          ON v.global_entity_id = bt.global_entity_id 
         AND v.uuid = bt.uuid
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

  FROM vendors        AS v
  LEFT JOIN gmv_class AS vendor_gmv_class
         ON v.global_entity_id = vendor_gmv_class.global_entity_id
        AND v.vendor_code = vendor_gmv_class.vendor_code

ORDER BY 1, 5),

sf_information AS (
  SELECT
    a.global_entity_id                                                   AS global_entity_id,
    a.country_name                                                       AS country,
    a.restaurant_city                                                    AS city_of_restaurant,
    v.chain_code                                                         AS chain_code,
    v.vendor_code                                                        AS vendor_code,
    v.name                                                               AS vendor_name,
    u.id                                                                 AS account_owner_id,
    u.title                                                              AS account_owner_job_title, 
    u.full_name                                                          AS account_owner_name,
    u.email                                                              AS account_owner_email,
    a.status                                                             AS status_of_sf_account,


 --COUNT(DISTINCT v.chain_code) OVER(PARTITION BY a.global_entity_id, u.id)  AS number_of_chains,
 --COUNT(DISTINCT v.vendor_code) OVER(PARTITION BY a.global_entity_id, u.id) AS number_of_vendors

  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` a

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_users` u
        ON  a.global_entity_id = u.global_entity_id
       AND a.sf_owner_id = u.id
       
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v 
       ON a.global_entity_id = v.global_entity_id
      AND a.vendor_code = v.vendor_code



  WHERE u.is_active                                            -- account_owner is still active
  AND v.is_active                                              -- vendor is active
  AND v.is_private = FALSE                                     -- account is not private
  AND v.is_test = FALSE                                        -- account is not being tested
  AND a.is_deleted = FALSE                                     -- account is not deleted
  AND a.is_marked_for_testing_training = FALSE                 -- not marked for testing
  AND a.vertical = 'Restaurant'                                -- only restaurant vendors considered
  AND a.global_entity_id LIKE 'FP_%'                           -- consider all foodpanda entities                                                    
  AND a.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE')),   -- exclude foodpanda romania, foodpanda bulgaria, foodpanda germany
--AND u.full_name IN ('Sonevongsouda Luanglath', 'Damian Wong')

-------------------------------- vendors per chain in a given country (excluding NULLs) -------------------
vendors_per_chain AS (
  SELECT 
    global_entity_id,
    CAST(AVG(number_of_vendors) AS INT64) AS avg_vendors_per_chain_country

  FROM( 
      SELECT  
        global_entity_id,
        chain_code,
        COUNT(vendor_code)                                                                  AS number_of_vendors
      FROM sf_information
      WHERE chain_code IS NOT NULL
      GROUP BY 1,2)
  GROUP BY 1),

-------------------------------------- dataframe from where we start doing our aggregations  -------------------
df AS (
SELECT 
  c.global_entity_id,
  sf.country,
  sf.city_of_restaurant,
  c.chain_code,
  c.vendor_code,
  c.vendor_name,
  c.gmv_class,
  sf.account_owner_id,
  sf.account_owner_job_title,
  sf.account_owner_name,
  sf.account_owner_email,
  --COUNT(DISTINCT c.chain_code) OVER(PARTITION BY c.global_entity_id, sf.account_owner_id)  AS number_of_chains,
  --COUNT(DISTINCT c.vendor_code) OVER(PARTITION BY c.global_entity_id, sf.account_owner_id) AS number_of_vendors
FROM combine_vendor_information AS c
LEFT JOIN sf_information        AS sf
       ON c.global_entity_id = sf.global_entity_id
      AND c.vendor_code = sf.vendor_code),

---breeakdown of GMV classes on an AM level ---------
am_portfolio AS (
SELECT
  global_entity_id,
  account_owner_name,
  COUNT(CASE WHEN gmv_class = 'A'     THEN vendor_code END)                            AS gmv_A_vendors_AM,
  COUNT(CASE WHEN gmv_class = 'B'     THEN vendor_code END)                            AS gmv_B_vendors_AM,
  COUNT(CASE WHEN gmv_class = 'C'     THEN vendor_code END)                            AS gmv_C_vendors_AM,
  COUNT(CASE WHEN gmv_class = 'D'     THEN vendor_code END)                            AS gmv_D_vendors_AM,
  COUNT(CASE WHEN gmv_class IS NULL   THEN vendor_code END)                            AS gmv_unclass_vendors_AM,
  
 --- Add all GMV types to find total active vendors
   (
  COUNT(CASE WHEN gmv_class = 'A'     THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class = 'B'   THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class = 'C'   THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class = 'D'   THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class IS NULL THEN vendor_code END)
   )                                                                                    AS total_vendors_AM,
  
FROM df
GROUP BY 1,2
ORDER BY 1),

---breeakdown of GMV classes on a country level ---------

country_portfolio AS (
SELECT
  global_entity_id,
  COUNT(CASE WHEN gmv_class = 'A'     THEN vendor_code END)                             AS gmv_A_vendors_country,
  COUNT(CASE WHEN gmv_class = 'B'     THEN vendor_code END)                             AS gmv_B_vendors_country,
  COUNT(CASE WHEN gmv_class = 'C'     THEN vendor_code END)                             AS gmv_C_vendors_country,
  COUNT(CASE WHEN gmv_class = 'D'     THEN vendor_code END)                             AS gmv_D_vendors_country,
  COUNT(CASE WHEN gmv_class IS NULL   THEN vendor_code END)                             AS gmv_unclass_vendors_country,
  
 -- Add all GMV types to find total active vendors
   (
  COUNT(CASE WHEN gmv_class = 'A'     THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class = 'B'   THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class = 'C'   THEN vendor_code END) 
  + COUNT(CASE WHEN gmv_class = 'D'   THEN vendor_code END)  
  + COUNT(CASE WHEN gmv_class IS NULL THEN vendor_code END)
   )                                                                                    AS total_vendors_country,
  
FROM df
GROUP BY 1
ORDER BY 1)

----------------------------------------------------------------- final query --------------------------------------

SELECT 
  *, 
  CAST((total_vendors_country/number_of_owners_country) AS INT64)                       AS vendors_per_owner_country
FROM (
   SELECT 
    a.*, 
    c.* EXCEPT(global_entity_id),
    vpc.* EXCEPT(global_entity_id),
    COUNT(DISTINCT account_owner_name) OVER(PARTITION BY a.global_entity_id)            AS number_of_owners_country,
   FROM am_portfolio            AS a
   LEFT JOIN country_portfolio  AS c    
          ON a.global_entity_id = c.global_entity_id
   LEFT JOIN vendors_per_chain  AS vpc 
          ON a.global_entity_id = vpc.global_entity_id
   ORDER BY 1) 
