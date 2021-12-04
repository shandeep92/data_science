with 
city as (
WITH dvzone as (
SELECT
  lg.global_entity_id,
  lg.vendor_code,
  lg.name as vendor_name,
  lg_zones.lg_zone_id,
  lg_zones.lg_zone_name,
  v.location.city as city_name,
  v.pd_city_id as city_id
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` lg
CROSS JOIN UNNEST(lg_zones) AS lg_zones
LEFT JOIN  `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id = lg.global_entity_id and v.vendor_code = lg.vendor_code

WHERE is_closest_point = TRUE
ORDER BY vendor_code asc
),


pre_final as 
(select 

bk.global_entity_id,
bk.pps_promo_area_uuid as promo_area_id,
c.name as common_name,
bk.year_month,
bk.vendor_code,
dvz.city_name,
dvz.city_id,
dvz.lg_zone_name as hurrier_zone

from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.global_entity_id=pa.global_entity_id and bk.pps_promo_area_uuid=pa.uuid 
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id=bk.global_entity_id and v.vendor_code=bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join dvzone dvz on dvz.global_entity_id = v.global_entity_id and dvz.vendor_code = v.vendor_code

where bk.uuid is not null
and date_trunc(date(bk.started_at_utc),month) >= date_sub(date_trunc(current_date,month),interval 3 month)
and  date_trunc(date(bk.started_at_utc),month) <= date_add(date_trunc(current_date,month),interval 2 month)
and bk.status != "cancelled"
/*and bl.price > 0*/
and v.vertical_type = 'restaurants'
order by promo_area_id asc)

SELECT * EXCEPT (count_city,count_hz,city_seq,hz_seq)

FROM (

SELECT *,ROW_NUMBER() OVER (PARTITION BY global_entity_id,promo_area_id ORDER BY count_city desc) as city_seq, RANK() OVER (PARTITION BY global_entity_id,promo_area_id,city_id ORDER BY count_hz desc) as hz_seq
FROM (

SELECT 

global_entity_id,
common_name,
promo_area_id,
city_name,
city_id,
hurrier_zone,
count(city_id) as count_city,
count(hurrier_zone) as count_hz

FROM pre_final

group by 1,2,3,4,5,6
order by global_entity_id,promo_area_id asc)
)

where city_seq = 1 and hz_seq = 1
ORDER BY global_entity_id,city_name,hurrier_zone asc),


maxcap as(
with prefinal as(
select 
c.name as common_name,
pa.global_entity_id,
pa.uuid as id,
pa.name,
ci.city_name,
ci.hurrier_zone,
p.product_type,
p.amount_local as amount,
SAFE_DIVIDE(p.amount_local,ex.fx_rate_eur) as amount_eur ,
p.position,
row_number() over (partition by c.name, pa.global_entity_id, pa.uuid, p.product_type, p.position
                   order by c.name, pa.global_entity_id, pa.uuid, p.product_type, p.position asc) as seqnum

from `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=pa.global_entity_id
left join city ci on pa.global_entity_id = ci.global_entity_id and pa.uuid = ci.promo_area_id
left join
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso

WHERE fx_rate_date >= "2020-01-01"
and c.global_entity_id LIKE "%FP%"
order by 1,6 asc) ex on ex.global_entity_id = pa.global_entity_id and CURRENT_DATE() = date(ex.fx_rate_date)
),

dates as (select distinct(year_month) as year_month
from fulfillment-dwh-production.pandata_curated.pps_bookings bk
where date_trunc(date(started_at_utc),month) >= date_sub(date_trunc(current_date,month),interval 3 month)
and  date_trunc(date(started_at_utc),month) <= date_add(date_trunc(current_date,month),interval 2 month))

SELECT
common_name,

global_entity_id,
d.year_month,
id,
name,
city_name,
hurrier_zone,

sum( case when (global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM') and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then amount end ) as potential_OL_amount,

sum( case when (global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM') and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then amount_eur end ) as potential_OL_amount_eur,

         
sum (case when (global_entity_id = 'FP_TH' or global_entity_id = 'FP_HK' or global_entity_id = 'FP_PH') and product_type = 'premium_placements' and position <11  then amount
          when (global_entity_id = 'FP_SG' or global_entity_id = 'FP_BD' or global_entity_id = 'FP_MY' or global_entity_id = 'FP_PK' or global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM' or global_entity_id = 'FP_JP') and  product_type = 'premium_placements' and position < 8 then amount
          when (global_entity_id = 'FP_TW') and  product_type = 'premium_placements' and position < 9 then amount
          end) as potential_PL_amount,
                   
sum (case when (global_entity_id = 'FP_TH' or global_entity_id = 'FP_HK' or global_entity_id = 'FP_PH') and product_type = 'premium_placements' and position <11  then amount_eur
          when (global_entity_id = 'FP_SG' or global_entity_id = 'FP_BD' or global_entity_id = 'FP_MY' or global_entity_id = 'FP_PK' or global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM' or global_entity_id = 'FP_JP') and  product_type = 'premium_placements' and position < 8 then amount_eur
          when (global_entity_id = 'FP_TW') and  product_type = 'premium_placements' and position < 9 then amount_eur
          end) as potential_PL_amount_eur,
          
count( case when (global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM') and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then id end ) as potential_OL_slots,
         
count (case when (global_entity_id = 'FP_TH' or global_entity_id = 'FP_HK' or global_entity_id = 'FP_PH') and product_type = 'premium_placements' and position <11  then id
          when (global_entity_id = 'FP_SG' or global_entity_id = 'FP_BD' or global_entity_id = 'FP_MY' or global_entity_id = 'FP_PK' or global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM' or global_entity_id = 'FP_JP') and  product_type = 'premium_placements' and position < 8 then id
          when (global_entity_id = 'FP_TW') and  product_type = 'premium_placements' and position < 9 then id
          end ) as potential_PL_slots

from prefinal
cross join dates d
group by 1,2,3,4,5,6,7),


bookings as(
select 

bk.global_entity_id,
bk.pps_promo_area_uuid as promo_area_id,
c.name as common_name,
ci.city_name,
ci.hurrier_zone,
bk.year_month,

Count ( distinct case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then v.vendor_code
                      when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then v.vendor_code
                      when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then v.vendor_code
                      when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then v.vendor_code end) as vendors_total,
          
Count ( distinct case when (bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then v.vendor_code end ) as vendors_OL,

count (distinct case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then v.vendor_code
          when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then v.vendor_code
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then v.vendor_code
          end ) as vendors_PL,
          
sum (case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then bk.cpp_billing.price end ) as sold_price_local_OL,

sum (case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then safe_divide(bk.cpp_billing.price,ex.fx_rate_eur) end ) as sold_price_eur_OL,
         
sum (case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then bk.cpp_billing.price
          when (bk.global_entity_id = 'FP_SG'  or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then bk.cpp_billing.price
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then bk.cpp_billing.price
          end ) as sold_price_local_PL,
          
sum (case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then safe_divide(bk.cpp_billing.price,ex.fx_rate_eur)
          when (bk.global_entity_id = 'FP_SG'  or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then safe_divide(bk.cpp_billing.price,ex.fx_rate_eur)
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then safe_divide(bk.cpp_billing.price,ex.fx_rate_eur)
          end ) as sold_price_eur_PL,
          
count (case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then bk.pps_promo_area_uuid end) as promo_areas_sold_OL,
          
count (case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then bk.pps_promo_area_uuid
          when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then bk.pps_promo_area_uuid
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then bk.pps_promo_area_uuid
          end ) as promo_areas_sold_PL
          
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.global_entity_id=pa.global_entity_id and bk.pps_promo_area_uuid=pa.uuid 
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id=bk.global_entity_id and v.vendor_code=bk.vendor_code
left join `dhh---analytics-apac.pandata_ap_commercial_external.datahub_vendor_gmv_class_table` gmvc on gmvc.global_entity_id = bk.global_entity_id and gmvc.vendor_code = bk.vendor_code
left join city ci on ci.global_entity_id = bk.global_entity_id and ci.promo_area_id = bk.pps_promo_area_uuid
left join
(SELECT c.global_entity_id,fx.*
FROM `fulfillment-dwh-production.pandata_curated.central_dwh_fx_rates` fx
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on fx.currency_code_iso = c.currency_code_iso

WHERE fx_rate_date >= "2020-01-01"
and c.global_entity_id LIKE "%FP%"
order by 1,6 asc) ex on ex.global_entity_id = bk.global_entity_id and date(bk.cpp_billing.created_at_utc) = date(ex.fx_rate_date)

where bk.uuid is not null
and date_trunc(date(bk.started_at_utc),month) >= date_sub(date_trunc(current_date,month),interval 3 month)
and  date_trunc(date(bk.started_at_utc),month) <= date_add(date_trunc(current_date,month),interval 2 month)
and bk.status != "cancelled"
and v.vertical_type = 'restaurants'
GROUP BY 1,2,3,4,5,6
)

select 
maxcap.*,
bookings.year_month as bk_year_month,
bookings.vendors_total,
bookings.vendors_OL,
bookings.vendors_PL,
bookings.sold_price_local_OL,
bookings.sold_price_eur_OL,
bookings.sold_price_local_PL,
bookings.sold_price_eur_PL,
bookings.promo_areas_sold_OL,
bookings.promo_areas_sold_PL

from maxcap
left join bookings on maxcap.global_entity_id=bookings.global_entity_id and maxcap.id = bookings.promo_area_id and maxcap.year_month = bookings.year_month
WHERE maxcap.global_entity_id LIKE 'FP_%'
  AND maxcap.global_entity_id NOT IN ('FP_RO','FP_BG','FP_DE')
order by global_entity_id,id,maxcap.year_month asc
