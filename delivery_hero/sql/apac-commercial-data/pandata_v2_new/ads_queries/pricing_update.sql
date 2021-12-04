/* Declare global_entity_id and previous year_month */

DECLARE set_global_entity_id STRING DEFAULT 'FP_BD';
DECLARE set_year_month INT64 DEFAULT 202106;

CREATE TEMP TABLE max_position (
  n_position INT64
)
;
-- Creating a table with column n_position and 10 rows for positions 1-10 of CPP slots --
INSERT INTO max_position VALUES
(1),
(2),
(3),
(4),
(5),
(6),
(7),
(8),
(9),
(10)
;


--line 282--
--DROP TABLE IF EXISTS long_lat;
CREATE TEMP TABLE long_lat AS
SELECT
  DISTINCT 
  b.latitude,
  b.longitude
FROM
  `fulfillment-dwh-production.pandata_curated.pd_orders` a
LEFT JOIN
 `fulfillment-dwh-production.pandata_curated.ca_customer_addresses` b
ON
  a.global_entity_id = b.global_entity_id
  AND a.pd_delivery_address_uuid = b.uuid
WHERE
  a.created_date_utc >= "2018-01-01"
  AND b.created_date_utc >= "2018-01-01"
  AND (b.longitude is not null AND b.latitude is not null)
  AND a.global_entity_id = set_global_entity_id /*add in global_entity_id*/

;

  --line 293--
--DROP TABLE IF EXISTS points;
CREATE TEMP TABLE points AS 
SELECT 
  longitude,
  latitude,
  ST_GEOGPOINT(longitude, latitude) as p 
FROM long_lat
;


--line 302--
--DROP TABLE IF EXISTS polygon_table;
CREATE TEMP TABLE polygon_table AS 
SELECT DISTINCT
  pa.uuid AS promo_area_id,
  pa.name AS promo_area_name,
  pa.polygon
FROM `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
WHERE pa.global_entity_id = set_global_entity_id  /*insert global_entity_id*/
  AND pa.is_active IS TRUE 
;


--line 319--
--DROP TABLE IF EXISTS polygons;
CREATE TEMP TABLE polygons  AS 
SELECT 
  promo_area_id,
  promo_area_name,
  ST_GEOGFROMTEXT(polygon) AS poly
FROM polygon_table
;

--line 328--
--DROP TABLE IF EXISTS long_lat_mapping;
CREATE TEMP TABLE long_lat_mapping AS 
SELECT
  longitude,
  latitude,
  poly,
  promo_area_id,
  promo_area_name
FROM points AS ll
JOIN polygons AS pt
 ON ST_WITHIN(ll.p, pt.poly) IS TRUE  
 ;


 --line 342--
--DROP TABLE IF EXISTS long_lat_mapping_clean;
CREATE TEMP TABLE long_lat_mapping_clean AS 
SELECT DISTINCT
  longitude,
  latitude,
  promo_area_id,
  promo_area_name
FROM long_lat_mapping AS ll
;


--line 355--
--DROP TABLE IF EXISTS rest_orders;
CREATE TEMP TABLE rest_orders AS 
SELECT 
  rfo.global_entity_id,
  llm.promo_area_id,
  promo_area_name,
  COUNT(DISTINCT rfo.vendor_code) AS sales_area_rest,
  COUNT(CASE WHEN is_valid_order IS TRUE THEN uuid ELSE NULL END)/3 AS sales_area_orders,
  SUM(CASE WHEN is_valid_order IS TRUE THEN gmv_local ELSE 0 END)/3 AS sales_area_gmv,
  AVG(gmv_local) AS aov

FROM

(SELECT
  a.uuid,
  a.global_entity_id,
  a.created_date_local,
  a.vendor_code,
  pdo.gmv_local,
  pdo.is_valid_order,
  b.latitude,
  b.longitude,
  
FROM
  `fulfillment-dwh-production.pandata_curated.pd_orders` a
LEFT JOIN
  `fulfillment-dwh-production.pandata_report.pandora_pd_orders_agg_jkr_deals` pdo 
ON 
  a.global_entity_id = pdo.global_entity_id 
  AND a.uuid = pdo.uuid
LEFT JOIN
   `fulfillment-dwh-production.pandata_curated.ca_customer_addresses` b
ON
  a.global_entity_id = b.global_entity_id
  AND a.pd_delivery_address_uuid = b.uuid
LEFT JOIN
   `fulfillment-dwh-production.pandata_curated.pd_vendors` c
ON
  a.global_entity_id = c.global_entity_id
  AND a.vendor_code = c.vendor_code
  
WHERE
  a.created_date_utc >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL -3 MONTH)
  and b.created_date_utc >= '2018-01-01'
  and pdo.created_date_local >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL -3 MONTH)
  and c.vertical_type = 'restaurants') rfo

join 

long_lat_mapping AS llm
  ON llm.longitude = rfo.longitude
  AND llm.latitude = rfo.latitude

WHERE rfo.created_date_local >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL -3 MONTH)
and rfo.created_date_local < DATE_TRUNC(CURRENT_DATE(),MONTH)
GROUP BY 1, 2, 3
;

CREATE TEMP TABLE pricing_1 AS 
SELECT
  *,
  0.0075*sales_area_gmv as promo_area_gmv
  FROM rest_orders AS c
ORDER BY 2, 6
;


--line 405--
--DROP TABLE IF EXISTS pricing;
CREATE TEMP TABLE pricing AS 
SELECT 
DISTINCT
  p.global_entity_id,
  p.promo_area_id,
  p.promo_area_name,
  p.sales_area_orders,
  p.sales_area_gmv,
  p.sales_area_rest,
  p.promo_area_gmv,

FROM pricing_1 AS p
;

-- Getting all the promo areas available in each country --
WITH suggested as (
SELECT
global_entity_id,
promo_area_id,
promo_area_gmv as total_amt_promo_area,
'premium_placements' AS product_type

FROM pricing AS p
order by 1,2 asc),

/*
WITH suggested as (
SELECT DISTINCT
  pa.global_entity_id,
  pa.rdbms_id,
  pa.uuid AS promo_area_id,
  pa.name AS promo_area_name,
  'premium_placements' AS product_type
FROM `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa

WHERE pa.global_entity_id = set_global_entity_id
  AND pa.is_active IS TRUE ),*/
  

-- Getting the sold price by promo area and position in the previous month --
sold_price_position as (
select 
bk.rdbms_id,
bk.global_entity_id,
bk.pps_promo_area_uuid as promo_area_id,
c.name as common_name,
bk.type,
cpp_billing.position as position,

sum (case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11  then cpp_billing.price
          when (bk.rdbms_id = 7 or bk.rdbms_id = 12 or bk.rdbms_id = 15 or bk.rdbms_id = 16 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 then cpp_billing.price
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 then cpp_billing.price
          end ) as sold_price_local_PL,

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk 
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.global_entity_id=pa.global_entity_id and bk.pps_promo_area_uuid=pa.uuid 
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on dv.global_entity_id = bk.global_entity_id and bk.vendor_code = dv.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = dv.global_entity_id and gmvc.vendor_code = dv.vendor_code

where bk.uuid is not null
and bk.global_entity_id = set_global_entity_id  /*change rdbms_id*/
and bk.year_month= set_year_month
and bk.status != "cancelled"
--and bl.price > 0
and dv.vertical_type = 'restaurants'
and bk.type = 'premium_placements' 
and bk.billing_type = 'CPP'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,5 asc),


-- Getting the utilization of each promo in the previous month. We are counting the slots as utilized only if the discount provided is less than 10% --
utilization as (

with maxcap as(
with prefinal as(
select 
c.name as common_name,
pa.rdbms_id,
pa.global_entity_id,
pa.uuid as id,
p.product_type,
p.amount_local as amount,
p.position,
row_number() over (partition by c.name, pa.rdbms_id, pa.uuid, p.product_type, p.position
                   order by c.name, pa.rdbms_id, pa.uuid, p.product_type, p.position asc) as seqnum
from `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.global_entity_id=pa.global_entity_id and p.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=pa.global_entity_id
)

SELECT
common_name,
rdbms_id,
global_entity_id,
id,

sum( case when (rdbms_id = 15 or rdbms_id = 7) and  product_type = 'organic_placements' and position <11 and seqnum = 1 then amount 
          when (rdbms_id = 19) and  product_type = 'organic_placements' and position <1 and seqnum = 1 then amount 
          when (rdbms_id = 16) and  product_type = 'organic_placements' and position <7 and seqnum = 1 then amount 
         when (rdbms_id = 17 or rdbms_id = 18 or rdbms_id = 20 or rdbms_id = 12) and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then amount
         end ) as potential_OL_amount,
         
sum (case when (rdbms_id = 17 or rdbms_id = 19 or rdbms_id = 20) and product_type = 'premium_placements' and position <11 and seqnum=1  then amount
          when (rdbms_id = 7 or rdbms_id = 12 or rdbms_id = 15 or rdbms_id = 16 or rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221 or rdbms_id = 263) and  product_type = 'premium_placements' and position < 8 and seqnum = 1 then amount
          when (rdbms_id = 18) and  product_type = 'premium_placements' and position < 9 and seqnum=1 then amount
          end) as potential_PL_amount,
          
count( case when (rdbms_id = 15 or rdbms_id = 7) and  product_type = 'organic_placements' and position <11 and seqnum = 1 then amount 
            when (rdbms_id = 16) and  product_type = 'organic_placements' and position <7 and seqnum = 1 then amount 
            when (rdbms_id = 19) and  product_type = 'organic_placements' and position <1 and seqnum = 1 then amount
         when (rdbms_id = 17 or rdbms_id = 18 or rdbms_id = 20 or rdbms_id = 12) and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then amount
         end ) as potential_OL_slots,
         
count (case when (rdbms_id = 17 or rdbms_id = 19 or rdbms_id = 20) and product_type = 'premium_placements' and position <11 and seqnum=1 then amount
          when (rdbms_id = 7 or rdbms_id = 12 or rdbms_id = 15 or rdbms_id = 16 or rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221 or rdbms_id = 263) and  product_type = 'premium_placements' and position < 8 and seqnum=1 then amount
          when (rdbms_id = 18) and  product_type = 'premium_placements' and position < 9 and seqnum=1 then amount
          end ) as potential_PL_slots
from prefinal
group by 1,2,3,4
),


bookings as(
select 
bk.rdbms_id,
bk.global_entity_id,
bk.pps_promo_area_uuid as promo_area_id,
c.name as common_name,

                      
Count (distinct case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and  bk.type = 'organic_placements' and cpp_billing.position < 9 then dv.vendor_code end) as vendors_OL,

Count (distinct case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11  then dv.vendor_code
          when (bk.rdbms_id = 15 or bk.rdbms_id = 7 or bk.rdbms_id = 16 or bk.rdbms_id = 12 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 then dv.vendor_code
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 then dv.vendor_code
          end ) as vendors_PL,
          
sum (case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and  bk.type = 'organic_placements' and cpp_billing.position < 9 then cpp_billing.price
         end ) as sold_price_local_OL,

sum (case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11  then cpp_billing.price
          when (bk.rdbms_id = 7 or bk.rdbms_id = 12 or bk.rdbms_id = 15 or bk.rdbms_id = 16 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 then cpp_billing.price
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 then cpp_billing.price
          end ) as sold_price_local_PL,

count (case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and cpp_billing.position < 9 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,cpp_billing.suggested_price),0) <=0.1) then bk.pps_promo_area_uuid
          end) as promo_areas_sold_OL,

count (case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,p.amount_local),0) <=0.1) then bk.pps_promo_area_uuid
          when (bk.rdbms_id = 7 or bk.rdbms_id = 12 or bk.rdbms_id = 15 or bk.rdbms_id = 16 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,p.amount_local),0) <=0.1) then bk.pps_promo_area_uuid
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,p.amount_local),0) <=0.1) then bk.pps_promo_area_uuid
          end ) as promo_areas_sold_PL
      
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk 
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.global_entity_id=pa.global_entity_id and bk.pps_promo_area_uuid=pa.uuid 
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on dv.global_entity_id = bk.global_entity_id and bk.vendor_code = dv.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.global_entity_id=bk.global_entity_id and p.pps_promo_area_uuid=bk.pps_promo_area_uuid and p.position = cpp_billing.position


where bk.uuid is not null
and bk.global_entity_id = set_global_entity_id  /*change rdbms_id*/
and bk.year_month=set_year_month
and bk.status != "cancelled"
--and bl.price > 0
and dv.vertical_type = 'restaurants'
and p.product_type = 'premium_placements'
GROUP BY 1,2,3,4
),


vendorslive as (select
v.rdbms_id,
v.global_entity_id,
v2.pps_promo_area_uuid as promo_area_id,
count (distinct v.vendor_code) as vendors_available
from `fulfillment-dwh-production.pandata_curated.pps_vendors` v
left join `fulfillment-dwh-production.pandata_curated.pps_vendors_promo_areas` v2 on v.rdbms_id = v2.rdbms_id and v.uuid = v2.pps_vendor_uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on dv.global_entity_id = v.global_entity_id and v.vendor_code = dv.vendor_code

where dv.is_active is true and is_private is false and is_test is false
and v.is_active is true
and dv.vertical_type = 'restaurants'
group by 1,2,3)


select 
maxcap.*,
bookings.promo_areas_sold_OL,
bookings.promo_areas_sold_PL,
safe_divide(bookings.promo_areas_sold_OL,maxcap.potential_OL_slots) as utilization_perc_OL_slots,
safe_divide(bookings.promo_areas_sold_PL,maxcap.potential_PL_slots) as utilization_perc_PL_slots
from maxcap
left join bookings on maxcap.rdbms_id=bookings.rdbms_id and maxcap.id = bookings.promo_area_id
left join vendorslive on vendorslive.rdbms_id=maxcap.rdbms_id and vendorslive.promo_area_id = maxcap.id

ORDER BY rdbms_id,id asc),

-- Getting the past 3M utilization per promo area and counting the number of times the utilization was less than 30%. Promo areas where the utilization was < 30% for 3x or more consecutively do not get their prices reduced --
utilization_2 as (

with maxcap as(
with prefinal as(
select 
c.name as common_name,
pa.rdbms_id,
pa.global_entity_id,
pa.uuid as id,
p.product_type,
p.amount_local as amount,
p.position,
row_number() over (partition by c.name, pa.rdbms_id, pa.uuid, p.product_type, p.position
                   order by c.name, pa.rdbms_id, pa.uuid, p.product_type, p.position asc) as seqnum
from `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.global_entity_id=pa.global_entity_id and p.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=pa.global_entity_id
)

SELECT
common_name,
rdbms_id,
global_entity_id,
id,

sum( case when (rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221) and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then amount
         end ) as potential_OL_amount,
         
sum (case when (rdbms_id = 17 or rdbms_id = 19 or rdbms_id = 20) and product_type = 'premium_placements' and position <11 and seqnum = 1  then amount
          when (rdbms_id = 7 or rdbms_id = 12 or rdbms_id = 15 or rdbms_id = 16 or rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221 or rdbms_id = 263) and product_type = 'premium_placements' and position < 8 and seqnum = 1  then amount
          when (rdbms_id = 18) and  product_type = 'premium_placements' and position < 9 and seqnum = 1 then amount
          end) as potential_PL_amount,
          
count( case when (rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221) and  product_type = 'organic_placements' and position <9 and seqnum = 1 then amount 
         end ) as potential_OL_slots,
         
count (case when (rdbms_id = 17 or rdbms_id = 19 or rdbms_id = 20) and product_type = 'premium_placements' and position <11 and seqnum = 1 then amount
          when (rdbms_id = 7 or rdbms_id = 12 or rdbms_id = 15 or rdbms_id = 16 or rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221 or rdbms_id = 263) and  product_type = 'premium_placements' and position < 8 and seqnum = 1 then amount
          when (rdbms_id = 18) and  product_type = 'premium_placements' and position < 9 and seqnum = 1 then amount
          end ) as potential_PL_slots
          
from prefinal
group by 1,2,3,4
),


bookings as(
select 
bk.rdbms_id,
bk.global_entity_id,
bk.pps_promo_area_uuid as promo_area_id,
c.name,
bk.year_month,

Count (distinct case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and  bk.type = 'organic_placements' and cpp_billing.position < 9 then dv.vendor_code
                      when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11  then dv.vendor_code
                      when (bk.rdbms_id = 7 or bk.rdbms_id = 12 or bk.rdbms_id = 15 or bk.rdbms_id = 16 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 then dv.vendor_code
                      when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 then dv.vendor_code                   
                      end) as vendors_total,
                      
Count (distinct case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and  bk.type = 'organic_placements' and cpp_billing.position < 9 then dv.vendor_code end) as vendors_OL,

Count (distinct case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11  then dv.vendor_code
          when (bk.rdbms_id = 15 or bk.rdbms_id = 7 or bk.rdbms_id = 16 or bk.rdbms_id = 12 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 then dv.vendor_code
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 then dv.vendor_code
          end ) as vendors_PL,
          
sum (case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and  bk.type = 'organic_placements' and cpp_billing.position < 9 then cpp_billing.price
         end ) as sold_price_local_OL,

sum (case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11  then cpp_billing.price
          when (bk.rdbms_id = 7 or bk.rdbms_id = 12 or bk.rdbms_id = 15 or bk.rdbms_id = 16 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 then cpp_billing.price
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 then cpp_billing.price
          end ) as sold_price_local_PL,

count (case when (bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221) and cpp_billing.position < 9 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,cpp_billing.suggested_price),0) <=0.1) then bk.pps_promo_area_uuid
          end) as promo_areas_sold_OL,

count (case when (bk.rdbms_id = 17 or bk.rdbms_id = 19 or bk.rdbms_id = 20) and bk.type = 'premium_placements' and cpp_billing.position <11 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,p.amount_local),0) <=0.1) then bk.pps_promo_area_uuid
          when (bk.rdbms_id = 7 or bk.rdbms_id = 12 or bk.rdbms_id = 15 or bk.rdbms_id = 16 or bk.rdbms_id = 219 or bk.rdbms_id = 220 or bk.rdbms_id = 221 or bk.rdbms_id = 263) and  bk.type = 'premium_placements' and cpp_billing.position < 8 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,p.amount_local),0) <=0.1) then bk.pps_promo_area_uuid
          when (bk.rdbms_id = 18) and  bk.type = 'premium_placements' and cpp_billing.position < 9 and (1-IFNULL(SAFE_DIVIDE(cpp_billing.price,p.amount_local),0) <=0.1) then bk.pps_promo_area_uuid
          end ) as promo_areas_sold_PL

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk 
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.global_entity_id=pa.global_entity_id and bk.pps_promo_area_uuid=pa.uuid 
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on dv.global_entity_id = bk.global_entity_id and bk.vendor_code = dv.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.global_entity_id=bk.global_entity_id and p.pps_promo_area_uuid=bk.pps_promo_area_uuid and p.position = cpp_billing.position 


where bk.uuid is not null
and bk.global_entity_id = set_global_entity_id  /*change rdbms_id*/
and bk.year_month >= cast(format_date('%Y%m',date_sub(date_trunc(current_date(),MONTH),INTERVAL 3 MONTH)) as INT64) and bk.year_month <= cast(format_date('%Y%m',date_sub(date_trunc(current_date(),MONTH),INTERVAL 1 MONTH)) as INT64)
and bk.status != "cancelled"
--and bl.price > 0
and dv.vertical_type = 'restaurants'
and p.product_type = 'premium_placements'
GROUP BY 1,2,3,4,5
),


vendorslive as (select
v.rdbms_id,
v.global_entity_id,
v2.pps_promo_area_uuid as promo_area_id,
count (distinct v.vendor_code) as vendors_available

from `fulfillment-dwh-production.pandata_curated.pps_vendors` v
left join `fulfillment-dwh-production.pandata_curated.pps_vendors_promo_areas` v2 on v.global_entity_id = v2.global_entity_id and v.uuid = v2.pps_vendor_uuid
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` dv on v.global_entity_id = dv.global_entity_id and v.vendor_code = dv.vendor_code

where dv.is_active is true and is_private is false and is_test is false

and v.is_active is true
and dv.vertical_type = 'restaurants'
group by 1,2,3)

SELECT
common_name,
rdbms_id,
global_entity_id,
id,
potential_PL_amount,
potential_PL_slots,
count(case when scenario = 5 then scenario end) as scenario_5_count

FROM (
select 
maxcap.*,
bookings.year_month,
bookings.promo_areas_sold_OL,
bookings.promo_areas_sold_PL,
safe_divide(bookings.promo_areas_sold_OL,maxcap.potential_OL_slots) as utilization_perc_OL_slots,
safe_divide(bookings.promo_areas_sold_PL,maxcap.potential_PL_slots) as utilization_perc_PL_slots,
case when safe_divide(bookings.promo_areas_sold_PL,maxcap.potential_PL_slots) = 1 then 1
when safe_divide(bookings.promo_areas_sold_PL,maxcap.potential_PL_slots) >= 0.8 then 3
when safe_divide(bookings.promo_areas_sold_PL,maxcap.potential_PL_slots) >= 0.3 then 4
when safe_divide(bookings.promo_areas_sold_PL,maxcap.potential_PL_slots) < 0.3 then 5
end as scenario

from maxcap
left join bookings on maxcap.rdbms_id=bookings.rdbms_id and maxcap.id = bookings.promo_area_id
left join vendorslive on vendorslive.rdbms_id=maxcap.rdbms_id and vendorslive.promo_area_id = maxcap.id 


ORDER BY id asc) 
GROUP BY 1,2,3,4,5,6
order by 2,3 asc),

-- Getting the previous suggested price by promo area and position --
prev_suggested_position as (

with prefinal as(
select 
c.name as common_name,
pa.rdbms_id,
pa.global_entity_id,
pa.uuid as id,
p.product_type,
p.amount_local as amount,
p.position,
row_number() over (partition by c.name, pa.rdbms_id, pa.uuid, p.product_type, p.position
                   order by c.name, pa.rdbms_id, pa.uuid, p.product_type, p.position asc) as seqnum
from `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.global_entity_id=pa.global_entity_id and p.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=pa.global_entity_id

where pa.global_entity_id = set_global_entity_id 
and p.product_type = 'premium_placements'
)

SELECT
common_name,
rdbms_id,
global_entity_id,
id,
position,
sum (case when (rdbms_id = 17 or rdbms_id = 19 or rdbms_id = 20) and product_type = 'premium_placements' and position <= 10 and seqnum = 1 then amount
          when (rdbms_id = 7 or rdbms_id = 12 or rdbms_id = 15 or rdbms_id = 16 or rdbms_id = 219 or rdbms_id = 220 or rdbms_id = 221 or rdbms_id = 263) and  product_type = 'premium_placements' and position <= 10 and seqnum = 1 then amount
          when (rdbms_id = 18) and  product_type = 'premium_placements' and position <= 10 and seqnum = 1 then amount
          end) as prev_PL_suggested_position
      
from prefinal
group by 1,2,3,4,5
),

-- Cross joining all promo areas with the max_position table to get 10 rows for each promo area --
test_construct as (
SELECT 
p.*,
mp.*
FROM suggested p 
CROSS JOIN max_position mp),

test_position as (
SELECT
  c.*,
  --- parameters set to 70% - 30% for the split 1-3/ 4-5 AND 10% for price distance AND 0.75% GMV
  CASE WHEN n_position = 1 THEN ROUND(((total_amt_promo_area*0.7)/(2.10 + (1.10)*(1.10))*1.10*1.10) +4, -1) 
    ELSE CASE WHEN n_position = 2 THEN ROUND(((total_amt_promo_area*0.7)/(2.10 + (1.10)*(1.10))*1.10) +4 , -1)
      ELSE CASE WHEN n_position = 3 THEN ROUND(((total_amt_promo_area*0.7)/(2.10 + (1.10)*(1.10))) +4, -1)
        ELSE CASE WHEN n_position = 4 THEN ROUND((((total_amt_promo_area*0.3)/(2.10))*1.10) +4, -1)
          ELSE CASE WHEN n_position = 5 THEN ROUND((((total_amt_promo_area*0.3)/(2.10))), -1)
            ELSE CASE WHEN n_position > 5 THEN ROUND((((total_amt_promo_area*0.3)/(2.10))), -1) END
          END
        END
      END 
    END
  END
  AS test_pre_PL_pricing
FROM test_construct c)




SELECT 
*,

case
when global_entity_id = 'FP_BD' and test_pre_PL_pricing2 > 60000 then 60000
when global_entity_id = 'FP_BD' and test_pre_PL_pricing2 < 600 then 600
when global_entity_id = 'FP_PK' and test_pre_PL_pricing2 > 50000 then 50000
when global_entity_id = 'FP_PK' and test_pre_PL_pricing2 < 2400 then 2400
when global_entity_id = 'FP_SG' and test_pre_PL_pricing2 > 1000 then 1000
when global_entity_id = 'FP_SG' and test_pre_PL_pricing2 < 50 then 50
when global_entity_id = 'FP_MY' and test_pre_PL_pricing2 > 2000 then 2000
when global_entity_id = 'FP_MY' and test_pre_PL_pricing2 < 100 then 100
when global_entity_id = 'FP_TH' and test_pre_PL_pricing2 > 30000 then 30000
when global_entity_id = 'FP_TH' and test_pre_PL_pricing2 < 1100 then 1100
when global_entity_id = 'FP_TW' and test_pre_PL_pricing2 > 30000 then 30000
when global_entity_id = 'FP_TW' and test_pre_PL_pricing2 < 450 then 450
when global_entity_id = 'FP_HK' and test_pre_PL_pricing2 > 5000 then 5000
when global_entity_id = 'FP_HK' and test_pre_PL_pricing2 < 500 then 500
when global_entity_id = 'FP_PH' and test_pre_PL_pricing2 > 50000 then 50000
when global_entity_id = 'FP_PH' and test_pre_PL_pricing2 < 700 then 700
when global_entity_id = 'FP_LA' and test_pre_PL_pricing2 > 10000000 then 10000000
when global_entity_id = 'FP_LA' and test_pre_PL_pricing2 < 68000 then 68000
when global_entity_id = 'FP_KH' and test_pre_PL_pricing2 > 1000 then 1000
when global_entity_id = 'FP_KH' and test_pre_PL_pricing2 < 20 then 20
when global_entity_id = 'FP_MM' and test_pre_PL_pricing2 > 1500000 then 1500000
when global_entity_id = 'FP_MM' and test_pre_PL_pricing2 < 12800 then 12800
when global_entity_id = 'FP_JP' and test_pre_PL_pricing2 < 1315 then 1315
else test_pre_PL_pricing2
end as final_amount

FROM (

SELECT 
*,

CASE WHEN test_pre_PL_pricing1 > 2*prev_PL_suggested_position THEN 2*prev_PL_suggested_position
ELSE test_pre_PL_pricing1
END AS test_pre_PL_pricing2

FROM (
SELECT *,

CASE
WHEN utilization_PL >= 0.8 and test_pre_PL_pricing >= 1.1*prev_PL_suggested_position THEN test_pre_PL_pricing
WHEN utilization_PL >= 0.8 and test_pre_PL_pricing < 1.1*prev_PL_suggested_position THEN 1.1*prev_PL_suggested_position
WHEN (utilization_PL >= 0.3 AND utilization_PL < 0.8) THEN prev_PL_suggested_position
WHEN utilization_PL < 0.3 AND scenario_5_count >= 3 THEN prev_PL_suggested_position --if utilization in past 3 months is low, same price remains
WHEN utilization_PL < 0.3 THEN 0.9 * prev_PL_suggested_position
END AS test_pre_PL_pricing1,


CASE
WHEN utilization_PL >= 0.8 and test_pre_PL_pricing >= 1.1*prev_PL_suggested_position THEN 1
WHEN utilization_PL >= 0.8 and test_pre_PL_pricing < 1.1*prev_PL_suggested_position THEN 2 
WHEN (utilization_PL >= 0.3 AND utilization_PL < 0.8) OR (utilization_PL < 0.3 AND scenario_5_count >= 3) THEN 3
WHEN utilization_PL < 0.3 AND scenario_5_count < 3 THEN 4
END AS scenario

FROM (

SELECT 
a.* ,
c.id,
ifnull(c.utilization_perc_PL_slots,0) as utilization_PL, -- getting utilization per promo area
e.scenario_5_count, -- getting number of times the promo area had < 30% utilization in past 3 months
pp.prev_PL_suggested_position, -- getting the previous suggested price of that promo area and position
spp.sold_price_local_PL as sold_price_local_PL_position -- getting the previous sold price of that promo area and position


FROM test_position a

left join utilization c
on a.global_entity_id = c.global_entity_id
and a.promo_area_id = c.id

left join utilization_2 e
on a.global_entity_id = e.global_entity_id
and a.promo_area_id = e.id

LEFT JOIN prev_suggested_position pp 
on a.global_entity_id = pp.global_entity_id
and a.promo_area_id = pp.id 
and a.n_position = pp.position

LEFT JOIN sold_price_position spp 
on a.global_entity_id = spp.global_entity_id
and a.promo_area_id = spp.promo_area_id 
and a.n_position = spp.position


order by global_entity_id,promo_area_id)))

ORDER BY global_entity_id,promo_area_id,n_position asc


