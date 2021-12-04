/* This query gives us number of global_entity_id, entities, countries within DH

    global_entity_id | source   | region      | management_id | display_name | common_name           | number_of_entities | number_of_regions | number_of_countries
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------
     PY_EC           |   16     |   America   |   Pedidosya   | PY - Ecuador |  Ecuador              |          76        |        4          |         50 
     AP_PA           |   129    |   America   |   Pedidosya   | PY - Panama  |  Panama               |          76        |        4          |         50
     CG_AE           |   121    |   MENA      |   Talabat     | TB - UAE     |  United Arab Emirates |          76        |        4          |         50                  

There are some null values within this query:
1. Do we use a CASE WHEN statemenent to include those regions? -- Use CASE WHEN to deal with null regions!
2. Why do we have data from Zomato?
 */

SELECT 
    DISTINCT global_entity_id
    , source_id
    , CASE 
        WHEN global_entity_id IN ('IN_GR','PU_HU','CD_GR') THEN 'Europe'
        WHEN global_entity_id IN ('IN_SA','IN_BH','IN_OM','IN_LB','IN_QA','IN_KW','IN_EG') THEN 'MENA'
        WHEN global_entity_id IN ('PY_PA') THEN 'America'
        ELSE region END AS region
    , management_entity
    , display_name
    , common_name
    , COUNT(DISTINCT global_entity_id) OVER() AS number_of_entities
    , COUNT(DISTINCT region)           OVER() AS number_of_regions
    , COUNT(DISTINCT common_name)      OVER() AS number_of_countries
FROM `fulfillment-dwh-production.cl_dmart.sources` 

-----------------------------------------------------------------------------------------------------

/* 
- Quick way to get country codes to do a WHERE IN filter in ccc_master_product tables 
- Handled NULL regions in `fulfillment-dwh-production.cl_dmart.sources` with CASE WHEN statements and mapped region accordingly.
    region             | country_codes_list
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------
     Asia              |   'bd', 'hk', 'jp', 'kh', 'kr', 'la', 'mm', 'my', 'ph', 'pk', 'sg', 'th', 'tw'
     Europe            |   'at', 'bg', 'cy', 'cz', 'de', 'fi', 'gr', 'hu', 'no', 'ro', 'se'
     MENA              |   'ae', 'bh', 'eg', 'iq', 'jo', 'kw', 'lb', 'om', 'qa', 'sa', 'tr'
     America           |   'ar', 'bo', 'cl', 'cr', 'do', 'ec', 'gt', 'hn', 'ni', 'pa', 'pe', 'py', 'sv', 'uy', 've'
*/
WITH country_code AS (
    SELECT 
    DISTINCT global_entity_id
    , source_id
    , CASE 
        WHEN global_entity_id IN ('IN_GR','PU_HU','CD_GR') THEN 'Europe'
        WHEN global_entity_id IN ('IN_SA','IN_BH','IN_OM','IN_LB','IN_QA','IN_KW','IN_EG') THEN 'MENA'
        WHEN global_entity_id IN ('PY_PA') THEN 'America'
        WHEN global_entity_id IN ('BM_KR') THEN 'Asia'
        ELSE region END AS region
    , management_entity
    , display_name
    , country_code
    , common_name
FROM `fulfillment-dwh-production.cl_dmart.sources` 
)

SELECT 
    region
    , STRING_AGG(CONCAT("'", country_code,"'"),", " ORDER BY country_code) AS country_codes_list
FROM (
    SELECT 
    DISTINCT c.region
    ,   c.country_code
    FROM country_code AS c
)
GROUP BY 1

