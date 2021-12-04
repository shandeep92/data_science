/* 1. Exploring values within the column */

SELECT 
DISTINCT country_name as values_within_column -- looking at different countries within the column
FROM `fulfillment-dwh-production.cl_dmart.customer_orders`

----------------------------------------------------------------------------------
/* 2. Exploring values within the column but having it in a list form in a single row */

SELECT 
STRING_AGG(DISTINCT country_name) as values_within_column
FROM `fulfillment-dwh-production.cl_dmart.customer_orders`

-------------------------------------------------------------------------
/* Dealing with more categorical columns */

SELECT 
    STRING_AGG(DISTINCT region) AS regions,
    STRING_AGG(DISTINCT country_name) AS countries,
    STRING_AGG(DISTINCT warehouse.city) AS cities,
    STRING_AGG(DISTINCT store_type) AS store_types,
    STRING_AGG(DISTINCT payment_method) AS payment_method,
    STRING_AGG(DISTINCT order_status) AS order_status
FROM `fulfillment-dwh-production.cl_dmart.customer_orders`
WHERE DATE(order_placed_localtime_at) <= CURRENT_DATE()

------------------------------------------------------------------------------

/* String_Agg of categorical columns 

region    | country.global_entity_id | country.management_entity |    country.country    | country.country_code | country.country_iso                    
 --------------------------------------------------------------------------------------------------------------------------------------------------
 America  |        PY_VE             |         Pedidosya          |     Venezuela        |       ve             |         VE
          |        PY_HN             |         Pedidosya          |     Honduras         |       hn             |         HN           
.........
*/

WITH regions AS (
    SELECT
    DISTINCT region
    , global_entity_id
    , management_entity
    , common_name AS country
    , country_code
    , country_iso
    FROM `fulfillment-dwh-production.cl_dmart.sources`
    WHERE is_active
    ORDER BY 2
)
  
SELECT
    region
    ,  ARRAY_AGG(STRUCT(global_entity_id, management_entity, country, country_code, country_iso)) AS country
FROM regions
GROUP BY 1

---------------------------------------------------------------
/* Get unique number of values for each column

global_entity_id |	region |	country_code  |	country_name |	currency_code | store_type
      64         |    4    |      49          |      49      |        41      |      38

 */

DECLARE columns ARRAY<STRING>;
DECLARE query STRING;
SET columns = (
 WITH all_columns AS (
   SELECT column_name
   FROM `fulfillment-dwh-production.cl_dmart.INFORMATION_SCHEMA.COLUMNS`
   WHERE data_type IN ("STRING", "BOOL", "DATE", "TIMESTAMP", "INT64", "FLOAT64", "NUMERIC")
   AND table_name = 'customer_orders'
 )
 SELECT ARRAY_AGG((column_name) ) AS columns
 FROM all_columns
);

SET query = (select STRING_AGG('(select count(distinct '||x||')  from `fulfillment-dwh-production.cl_dmart.customer_orders`) '||x ) AS string_agg from unnest(columns) x );
EXECUTE IMMEDIATE
"SELECT  "|| query