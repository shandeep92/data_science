with maxcap as(
with prefinal as(
select 
c.name as common_name,

pa.global_entity_id,
pa.uuid as id,
p.product_type,
p.amount_local as amount,
p.position,
row_number() over (partition by c.name, pa.global_entity_id, pa.uuid, p.product_type, p.position
                   order by c.name, pa.global_entity_id, pa.uuid, p.product_type, p.position asc) as seqnum
from `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa
left join `fulfillment-dwh-production.pandata_curated.pps_prices` p on p.pps_promo_area_uuid=pa.uuid
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=pa.global_entity_id

where pa.global_entity_id IN ('FP_BD','FP_PK','FP_SG','FP_MY','FP_TH','FP_TW','FP_HK','FP_PH','FP_LA','FP_KH','FP_MM','FP_JP')
),

dates as (select distinct(year_month)
from fulfillment-dwh-production.pandata_curated.pps_bookings bk
where date_trunc(date(started_at_utc),month) >= date_sub(date_trunc(current_date,month),interval 3 month)
and  date_trunc(date(started_at_utc),month) <= date_add(date_trunc(current_date,month),interval 1 month))

SELECT
common_name,

global_entity_id,
id,
d.year_month,
sum(case when (global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM') and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then amount end) as potential_OL_amount,
sum (case when (global_entity_id = 'FP_TH' or global_entity_id = 'FP_HK' or global_entity_id = 'FP_PH') and product_type = 'premium_placements' and position <11  then amount
          when (global_entity_id = 'FP_SG' or global_entity_id = 'FP_BD' or global_entity_id = 'FP_MY' or global_entity_id = 'FP_PK' or global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM' or global_entity_id = 'FP_JP') and  product_type = 'premium_placements' and position < 8 then amount
          when (global_entity_id = 'FP_TW') and  product_type = 'premium_placements' and position < 9 then amount
          end) as potential_PL_amount,

count( case when (global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM') and  product_type = 'organic_placements' and position < 9 and seqnum = 1 then id end ) as potential_OL_slots,

count (case when (global_entity_id = 'FP_TH' or global_entity_id = 'FP_HK' or global_entity_id = 'FP_PH') and product_type = 'premium_placements' and position <11  then id
          when (global_entity_id = 'FP_SG' or global_entity_id = 'FP_BD' or global_entity_id = 'FP_MY' or global_entity_id = 'FP_PK' or global_entity_id = 'FP_LA' or global_entity_id = 'FP_KH' or global_entity_id = 'FP_MM' or global_entity_id = 'FP_JP') and  product_type = 'premium_placements' and position < 8 then id
          when (global_entity_id = 'FP_TW') and  product_type = 'premium_placements' and position < 9 then id
          end ) as potential_PL_slots
from prefinal
cross join dates d
group by 1,2,3,4
),

bookings as(
select 

bk.global_entity_id,
c.name as common_name,
bk.year_month,
bk.pps_promo_area_uuid as promo_area_id,

Count ( distinct case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_TW' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then v.vendor_code
                      when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then v.vendor_code
                      when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then v.vendor_code
                      when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then v.vendor_code end) as vendors_total,
                      
Count ( distinct case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then v.vendor_code end ) as vendors_OL,

count (distinct case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then v.vendor_code
          when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then v.vendor_code
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then v.vendor_code
          end ) as vendors_PL,
          
sum (case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then bk.cpp_billing.price end ) as sold_price_local_OL,
         
sum (case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then bk.cpp_billing.price
          when (bk.global_entity_id = 'FP_SG'  or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then bk.cpp_billing.price
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then bk.cpp_billing.price
          end ) as sold_price_local_PL,
          
count (case when (bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM') and  bk.type = 'organic_placements' and bk.cpp_billing.position < 9 then bk.pps_promo_area_uuid end) as promo_areas_sold_OL,
          
count (case when (bk.global_entity_id = 'FP_TH' or bk.global_entity_id = 'FP_HK' or bk.global_entity_id = 'FP_PH') and bk.type = 'premium_placements' and bk.cpp_billing.position <11  then bk.pps_promo_area_uuid
          when (bk.global_entity_id = 'FP_SG' or bk.global_entity_id = 'FP_BD' or bk.global_entity_id = 'FP_MY' or bk.global_entity_id = 'FP_PK' or bk.global_entity_id = 'FP_LA' or bk.global_entity_id = 'FP_KH' or bk.global_entity_id = 'FP_MM' or bk.global_entity_id = 'FP_JP') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 8 then bk.pps_promo_area_uuid
          when (bk.global_entity_id = 'FP_TW') and  bk.type = 'premium_placements' and bk.cpp_billing.position < 9 then bk.pps_promo_area_uuid
          end ) as promo_areas_sold_PL


          
from `fulfillment-dwh-production.pandata_curated.pps_bookings` bk
left join `fulfillment-dwh-production.pandata_curated.shared_countries` c on c.global_entity_id=bk.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.pps_promo_areas` pa on bk.global_entity_id=pa.global_entity_id and bk.pps_promo_area_uuid=pa.uuid 
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on v.global_entity_id=bk.global_entity_id and v.vendor_code=bk.vendor_code

where bk.uuid is not null
and date_trunc(date(bk.started_at_utc),month) >= date_sub(date_trunc(current_date,month),interval 3 month)
and  date_trunc(date(bk.started_at_utc),month) <= date_add(date_trunc(current_date,month),interval 1 month)
and bk.status != "cancelled"
and v.vertical_type = 'restaurants'
GROUP BY 1,2,3,4
)

  
select 
maxcap.*,
bookings.year_month as bk_year_month,
bookings.vendors_total,
bookings.vendors_OL,
bookings.vendors_PL,
bookings.sold_price_local_OL,
bookings.sold_price_local_PL,
bookings.promo_areas_sold_OL,
bookings.promo_areas_sold_PL
from maxcap
left join bookings on maxcap.global_entity_id=bookings.global_entity_id and maxcap.id = bookings.promo_area_id and maxcap.year_month = bookings.year_month

order by maxcap.global_entity_id,maxcap.id,maxcap.year_month asc
