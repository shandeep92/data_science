/* 
INSTRUCTIONS: 
1) CHANGE GFV FILTER IN LINE 23
2) CHANGE FILTERING IN LINE 283 & 284
3) ALWAYS GENERATE LIST AT EOM AND NOT AT THE START OF THE NEXT MONTH
*/

--CHANGE rdbms_id--
WITH prefinal as (
SELECT *
FROM pandata_ap_commercial.monthly_target_list_V2
where global_entity_id = 'FP_TW'
),

--CHANGE GFV filter--
--FILTERING CRITERIA--
filters as (

SELECT
DISTINCT
global_entity_id,
percentile_disc (fail_rate,0.85) OVER (PARTITION BY global_entity_id)  as fail_rate_filter,
30000 as gfv_local_filter,
percentile_disc (conversion_rate,0.2) OVER (PARTITION BY global_entity_id)  as conversion_rate_filter,
percentile_disc (click_through_rate,0.3) OVER (PARTITION BY global_entity_id)  as joker_click_through_rate_filter,
percentile_disc (click_through_rate,0.1) OVER (PARTITION BY global_entity_id)  as cpc_click_through_rate_filter,
percentile_disc (frequency,0.5) OVER (PARTITION BY global_entity_id)  as joker_frequency_filter,
AVG (frequency) OVER (PARTITION BY global_entity_id)  as cashback_frequency_filter,
percentile_disc (cpcfactor_z,0.3) OVER (PARTITION BY global_entity_id)  as cpcfactor_z_filter,
percentile_disc (avg_open_hrs_per_day,0.25) OVER (PARTITION BY global_entity_id)  as avg_open_hours_filter

FROM prefinal
),

-- CPP SCORING --
cpp_scoring as (
SELECT
DISTINCT 
p.global_entity_id,

percentile_disc (gfv_local_order_mth,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_gfv_local ,
percentile_disc (gfv_local_order_mth,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_gfv_local ,
percentile_disc (gfv_local_order_mth,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_gfv_local, 

percentile_disc (gfv_change,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_gfv_change ,
percentile_disc (gfv_change,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_gfv_change ,
percentile_disc (gfv_change,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_gfv_change, 

percentile_disc (fail_rate,0.4) OVER (PARTITION BY p.global_entity_id) as top_percentile_fail_rate ,
percentile_disc (fail_rate,0.7) OVER (PARTITION BY p.global_entity_id) as mid_percentile_fail_rate ,
percentile_disc (fail_rate,1) OVER (PARTITION BY p.global_entity_id) as low_percentile_fail_rate,

percentile_disc (conversion_rate,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_conversion_rate ,
percentile_disc (conversion_rate,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_conversion_rate ,
percentile_disc (conversion_rate,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_conversion_rate,

FROM prefinal p
left join filters f 
on p.global_entity_id = f.global_entity_id
where p.fail_rate <= f.fail_rate_filter
and p.gfv_local_order_mth >= f.gfv_local_filter
and p.conversion_rate >= f.conversion_rate_filter
),

-- CPC SCORING -- 
cpc_scoring as (
SELECT
DISTINCT 
p.global_entity_id,

percentile_disc (cpcfactor_z,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_cpcfactor_z ,
percentile_disc (cpcfactor_z,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_cpcfactor_z ,
percentile_disc (cpcfactor_z,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_cpcfactor_z,


percentile_disc (click_through_rate,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_click_through_rate ,
percentile_disc (click_through_rate,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_click_through_rate ,
percentile_disc (click_through_rate,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_click_through_rate,

percentile_disc (avg_open_hrs_per_day,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_avg_open_hours ,
percentile_disc (avg_open_hrs_per_day,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_avg_open_hours ,
percentile_disc (avg_open_hrs_per_day,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_avg_open_hours,

percentile_disc (p.cpc_roi_order_mth,0.7) OVER (PARTITION BY p.global_entity_id) as top_percentile_cpc_roi ,
percentile_disc (p.cpc_roi_order_mth,0.4) OVER (PARTITION BY p.global_entity_id) as mid_percentile_cpc_roi ,
percentile_disc (p.cpc_roi_order_mth,0) OVER (PARTITION BY p.global_entity_id) as low_percentile_cpc_roi,


FROM prefinal p
left join filters f 
on p.global_entity_id = f.global_entity_id
where p.fail_rate <= f.fail_rate_filter
--and p.avg_daily_open_hours >= f.avg_open_hours_filter
and p.avg_open_hrs_per_day >= 4
and p.click_through_rate >= f.cpc_click_through_rate_filter
and p.cpcfactor_z >= f.cpcfactor_z_filter
and cpc_bid_budget <> ""
)

--FINAL QUERY--

SELECT 
country_name,
global_entity_id,
vendor_name,
vendor_code,
city_name,
hurrier_zone,
chain_name,
owner_name,
gmv_class,
order_month,
vendor_type,
--loyalty_enabled_percentage,
pandabox_units_live,
order_mth_cpp_sold_local,
current_mth_cpp_sold_local,
next_mth_cpp_sold_local,
CPC_bid_budget,
cpcfactor_z,
cpc_roi_order_mth,
successful_order,
gfv_local_order_mth,
gfv_local_prev_mth,
gfv_change,
new_to_rest_customer,
NC_proportion,
frequency,
shop_loads,
sessions,
click_through_rate,
conversion_rate,
avb_local,
fail_rate,
avg_open_hrs_per_day,
Deal_Max_MOV,
max_clicks,
active_vf_deals,
pandapro,
deals,
--cashback,
joker,
cpp,
cpc,
ncr_score,
deal_score,
gfv

FROM (

SELECT *,
CAST(cpp_gfv_change_score * cpp_fail_rate_score * cpp_conversion_rate_score * cpp_bought_score AS INT64) as cpp_score,

CAST(POW(POW(cpc_fail_rate_score * cpcfactor_z_score * click_through_rate_score * avg_open_hours_score * cpc_roi_score * cpc_active_score * cpc_gfv_change_score, 1/6),4) AS INT64) as cpc_score,

CAST(cpp_gfv_change_score * cpp_fail_rate_score * cpp_conversion_rate_score * cpp_bought_score AS INT64) + CAST(POW(POW(cpc_fail_rate_score * cpcfactor_z_score * click_through_rate_score * avg_open_hours_score * cpc_roi_score * cpc_active_score * cpc_gfv_change_score , 1/6),4) AS INT64) as ncr_score,

CAST(deals_score AS INT64) as deal_score,

gfv_local_order_mth as gfv

FROM (

SELECT 
p.*,

CASE
when p.fail_rate <= f.fail_rate_filter AND active_vf_deals = "No active VF deal" THEN "Yes"
ELSE "No"
end as deals, -- filtering deals

CASE
when p.fail_rate <= f.fail_rate_filter AND p.successful_order = 0 and p.frequency >= f.cashback_frequency_filter THEN "Yes"
ELSE "No"
end as cashback, -- filtering cashback

CASE
when p.fail_rate <= f.fail_rate_filter AND p.conversion_rate >= f.conversion_rate_filter AND p.click_through_rate >= f.joker_click_through_rate_filter AND p.frequency >= f.joker_frequency_filter AND p.pandabox_units_live IS NULL THEN "Yes"
ELSE "No"
end as joker, -- filtering joker

CASE
when p.fail_rate <= f.fail_rate_filter AND p.gfv_local_order_mth >= f.gfv_local_filter and p.conversion_rate >= f.conversion_rate_filter THEN "Yes"
ELSE "No"
end as cpp, -- filtering cpp

CASE
when p.fail_rate <= f.fail_rate_filter AND p.cpcfactor_z >= f.cpcfactor_z_filter and p.click_through_rate >= f.cpc_click_through_rate_filter and p.avg_open_hrs_per_day >= 4 and CPC_bid_budget IS NULL THEN "Yes"
ELSE "No"
end as cpc, -- filtering cpc
/*
CASE
when p.fail_rate <= f.fail_rate_filter AND p.cpcfactor_z >= f.cpcfactor_z_filter and p.click_through_rate >= f.cpc_click_through_rate_filter and p.avg_daily_open_hours >= f.avg_open_hours_filter and CPC_bid_budget IS NULL THEN "Yes"
ELSE "No"
end as cpc, -- filtering cpc
*/
CASE 
when gfv_local_order_mth >= f.gfv_local_filter AND gfv_change >= top_percentile_gfv_change THEN 3
when gfv_local_order_mth >= f.gfv_local_filter AND gfv_change >= mid_percentile_gfv_change THEN 2
when gfv_local_order_mth >= f.gfv_local_filter AND gfv_change >= low_percentile_gfv_change THEN 1
else 0
END AS cpp_gfv_change_score,

CASE 
when fail_rate <= top_percentile_fail_rate THEN 3
when fail_rate <= mid_percentile_fail_rate THEN 2
when fail_rate <= low_percentile_fail_rate THEN 1
else 0
END AS cpp_fail_rate_score,

CASE 
when conversion_rate >= top_percentile_conversion_rate THEN 3
when conversion_rate >= mid_percentile_conversion_rate THEN 2
when conversion_rate >= low_percentile_conversion_rate THEN 1
else 0
END AS cpp_conversion_rate_score,

CASE 
when order_mth_cpp_sold_local IS NOT NULL AND (current_mth_cpp_sold_local IS NULL OR next_mth_cpp_sold_local IS NULL) THEN 3
when next_mth_cpp_sold_local IS NULL THEN 2
ELSE 1
END AS cpp_bought_score,

CASE 
when gfv_local_order_mth >= f.gfv_local_filter AND gfv_change >= top_percentile_gfv_change THEN 3
when gfv_local_order_mth >= f.gfv_local_filter AND gfv_change >= mid_percentile_gfv_change THEN 2
when gfv_local_order_mth >= f.gfv_local_filter AND gfv_change >= low_percentile_gfv_change THEN 1
else 0
END AS cpc_gfv_change_score,


CASE 
when fail_rate <= top_percentile_fail_rate THEN 3
when fail_rate <= mid_percentile_fail_rate THEN 2
when fail_rate <= low_percentile_fail_rate THEN 1
else 0
END AS cpc_fail_rate_score,

CASE 
when cpcfactor_z >= top_percentile_cpcfactor_z THEN 3
when cpcfactor_z >= mid_percentile_cpcfactor_z THEN 2
when cpcfactor_z >= low_percentile_cpcfactor_z THEN 1
else 0
END AS cpcfactor_z_score,

CASE 
when click_through_rate >= top_percentile_click_through_rate THEN 3
when click_through_rate >= mid_percentile_click_through_rate THEN 2
when click_through_rate >= low_percentile_click_through_rate THEN 1
else 0
END AS click_through_rate_score,

CASE 
when avg_open_hrs_per_day >= top_percentile_avg_open_hours THEN 3
when avg_open_hrs_per_day >= mid_percentile_avg_open_hours THEN 2
when avg_open_hrs_per_day >= low_percentile_avg_open_hours THEN 1
else 0
END AS avg_open_hours_score,

CASE 
when cpc_roi_order_mth >= top_percentile_cpc_roi THEN 3
when cpc_roi_order_mth >= mid_percentile_cpc_roi THEN 2
when cpc_roi_order_mth >= low_percentile_cpc_roi THEN 1
else 0
END AS cpc_roi_score,

CASE 
when cpc_bid_budget <> "" THEN 0
else 1
END AS cpc_active_score,

CASE 
when ((current_mth_cpp_sold_local is not null OR cpc_bid_budget is not null) AND active_vf_deals = 'No active VF deal' AND fail_rate <= f.fail_rate_filter) then 2 -- if there is active NCR, no active deal and fail_rate <= fail_rate_filter
when (active_vf_deals = 'No active VF deal' AND p.fail_rate <= f.fail_rate_filter)  then 1 -- if there no active deal and fail_rate <= fail_rate_filter
ELSE 0
END AS deals_score



FROM prefinal p
LEFT JOIN filters f 
on p.global_entity_id = f.global_entity_id
LEFT JOIN cpp_scoring cpp
on p.global_entity_id = cpp.global_entity_id
LEFT JOIN cpc_scoring cpc
on p.global_entity_id = cpc.global_entity_id)
)

where gmv_class in ("C","D")
and owner_name = 'Thong Lai Yee'
order by 41 desc,43 desc






