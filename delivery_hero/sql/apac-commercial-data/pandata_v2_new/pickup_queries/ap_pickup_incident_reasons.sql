WITH

/* for incident before mar 21*/
-- old_incident_rate AS (
-- SELECT
-- --   pd_orders.global_entity_id,
--   DATE_TRUNC(ordered_at_local,month) AS month,
--   count(distinct pd_orders.uuid) AS gross_order,
--   count(sc.id) AS issues,
  
-- FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
-- LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
--         ON shared_countries.management_entity = 'Foodpanda APAC'
--         AND shared_countries.global_entity_id = pd_orders.global_entity_id
--         AND shared_countries.global_entity_id LIKE 'FP%'
-- LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_cases` AS sc
--        on pd_orders.created_date_utc>=date(2020,5,1)
--        and pd_orders.created_date_utc<date(2021,3,1)
--        and pd_orders.is_pickup
--        and pd_orders.is_gross_order
--        and sc.global_entity_id=pd_orders.global_entity_id
--        and pd_orders.code=sc.order_code
-- LEFT JOIN pandata_ap_special_projects.ap_pickup_available_country_flag_imported
--          on ap_pickup_available_country_flag_imported.country_name = pd_orders.country_name
--         and ap_pickup_available_country_flag_imported.month = DATE_TRUNC(pd_orders.ordered_at_local, month)
-- where true
-- and is_pickup
-- and is_gross_order
-- and pd_orders.created_date_utc>=date(2020,5,1)
-- and pickup_available_flag=1
-- and (department='Customer Service' or department is null)
-- group by 1
-- order by 1 desc
-- ),

sf_ticket AS (
  SELECT
    sc.id AS id,
    sc.global_entity_id,
    sc.created_date_utc AS ticket_date,
    sc.channel,
    sc.customer_contact_reason_1 AS ccr1,
    sc.customer_contact_reason_2 AS ccr2,
    sc.customer_contact_reason_3 AS ccr3,
    sc.order_code AS order_code,
    "Salesforce" AS Platform
  FROM `fulfillment-dwh-production.pandata_curated.gcc_salesforce_cases` AS sc
  WHERE sc.created_date_utc >= date_sub(current_date(),interval 7 month)
    AND sc.global_entity_id LIKE 'FP_%'
    AND LOWER(stakeholder) LIKE '%customer%'
  # AND sc.channel IN ('Email','Chat','Helpcenter','Social Media','Web','Facebook')
),

pandacare AS (
  SELECT  
    pc.id AS id,
    shared_countries.global_entity_id,
    pc.created_date_utc AS ticket_date,
    'Chat' AS channel,
    pc.customer_contact_reason_1 AS ccr1,
    pc.customer_contact_reason_2 AS ccr2,
    pc.customer_contact_reason_3 AS ccr3,
    pc.order_code AS order_code,
    "Pandacare" AS Platform
  from `fulfillment-dwh-production.pandata_curated.gcc_pandacare_chats` AS pc
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         on shared_countries.global_entity_id = pc.global_entity_id
  where pc.created_date_utc >= date_sub(current_date(),interval 7 month)
    and shared_countries.global_entity_id LIKE 'FP_%'
--     and pc.department = 'Customer Service'
),

chats_union AS (
  SELECT * FROM sf_ticket
  UNION ALL
  SELECT * FROM pandacare
),

pd_order AS (
  SELECT
    pd_orders.global_entity_id,
    pd_orders.code AS order_code,
    pd_orders.country_name,
    pd_orders.is_valid_order,
    pd_orders.is_preorder,
    pd_orders.payment_type.code_type AS  payment_type_code_type,
    pd_orders.ordered_at_local,
    date(pd_orders.ordered_at_local) AS date_local,
    pd_orders.expedition_type,
    pd_vendors_agg_business_types. business_type_apac,
    chats_union.id AS ticket_id,
    chats_union.ticket_date,
    chats_union.ccr1,
    chats_union.ccr2,
    chats_union.ccr3,
    chats_union.Platform,
    chats_union.Channel,
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         on shared_countries.global_entity_id = pd_orders.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
         ON pd_vendors.uuid = pd_orders.pd_vendor_uuid
  LEFT join `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_orders.pd_vendor_uuid = pd_vendors_agg_business_types.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` AS marketing_pd_orders_agg_acquisition_dates
         ON marketing_pd_orders_agg_acquisition_dates.uuid  = pd_orders.uuid
  LEFT JOIN chats_union
         ON chats_union.global_entity_id = pd_orders.global_entity_id
        AND chats_union.order_code = pd_orders.code
  WHERE DATE(pd_orders.created_date_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
    AND shared_countries.management_entity = 'Foodpanda APAC'
    AND pd_orders.global_entity_id like 'FP_%'
    AND DATE(pd_orders.created_date_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
    AND DATE(pd_orders.created_date_utc) < CURRENT_DATE()
    AND pd_orders.is_gross_order
    AND not pd_orders.is_test_order
    -- AND DATE(pd_orders.ordered_at_local) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    ORDER BY pd_orders.global_entity_id, pd_orders.code
    
),

country_summ AS (

  SELECT  
    pd_order.global_entity_id,
    pd_order.country_name,
    pickup_available_flag,
    DATE_TRUNC(date_local,day) AS date_local,
    DATE_TRUNC(date_local,month) AS month,
    DATE_TRUNC(date_local,isoweek) AS week,    
--     FORMAT_DATE("%b %Y",date_local) AS month,
--     FORMAT_DATE("%V-%Y",date_local) AS week,    
    expedition_type,
    is_preorder,
    payment_type_code_type='COD' AS is_cod_order,
    ccr1,
    ccr2,
    ccr3,
    COUNT(order_code) AS gross_orders,
    COUNT(if(not is_valid_order, order_code, NULL)) AS fail_orders,
    COUNT(ticket_id) AS issues,
    COUNT(distinct order_code) AS unique_gross_orders,
  from pd_order
  LEFT JOIN pandata_ap_special_projects.ap_pickup_available_country_flag_imported
         on ap_pickup_available_country_flag_imported.country_name = pd_order.country_name
        and ap_pickup_available_country_flag_imported.month = DATE_TRUNC(pd_order.date_local, MONTH)
  where date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(),isoweek), INTERVAL 17 WEEK)
  group by 1,2,3,4,5,6,7,8,9,10,11,12
  order by date_local desc, global_entity_id, gross_orders desc
),

ap_summ AS (

  SELECT  
    'APAC' AS global_entity_id,
    'APAC' AS country_name,
     1 AS  pickup_available_flag,
    date_local,
    DATE_TRUNC(date_local,month) AS month,
    DATE_TRUNC(date_local,isoweek) AS week,    
    expedition_type,
    is_preorder,
    is_cod_order,
    ccr1,
    ccr2,
    ccr3,
    SUM(if ( expedition_type='delivery' 
            OR (expedition_type='pickup' AND country_summ.pickup_available_flag = 1),
            gross_orders, 0)) AS gross_orders,
    SUM(if ( expedition_type='delivery'
            OR (expedition_type='pickup' AND country_summ.pickup_available_flag = 1),
            fail_orders, 0)) AS fail_orders,
    SUM(if ( expedition_type='delivery' 
            OR (expedition_type='pickup' AND country_summ.pickup_available_flag = 1),
            issues, 0)) AS issues,
    SUM(if ( expedition_type='delivery'
            OR (expedition_type='pickup' AND country_summ.pickup_available_flag = 1),
            unique_gross_orders,0 )) AS unique_gross_orders,
  from country_summ
  group by 1,2,3,4,5,6,7,8,9,10,11,12
)
select * from country_summ
union all
select * from ap_summ
order by date_local desc, global_entity_id, gross_orders desc

  -- ap_pickup_incident_reasons
 