-- owner: grace chan


/*list of renegotiated vendors w old and new comm rate */
with list_of_renego_vendors as (
select
    v.global_entity_id,
    a.country_name,
    c.sf_account_id,
    v.vendor_code,
    v.name,
    c.sf_opportunity_id,
    c.id as contract_id,
    c.service_type,
    c.status,
    c.is_tiered_commission,
    ROW_NUMBER() OVER (PARTITION BY v.global_entity_id, v.vendor_code ORDER BY c.start_date_local DESC) = 1 AS old_contract_start_date,
    c.end_date_local,
    o.opp_close_date_utc,
    c.commission_percentage as new_comm_rate
    from fulfillment-dwh-production.pandata_curated.sf_contracts c
    left join fulfillment-dwh-production.pandata_curated.sf_accounts a 
      ON a.id = c.sf_account_id
      AND a.global_entity_id = c.global_entity_id
    LEFT JOIN fulfillment-dwh-production.pandata_curated.sf_users u 
      ON u.id = a.sf_created_by_user_id
    left join fulfillment-dwh-production.pandata_report.sales_lead_account_opp_aggregated o 
      ON c.sf_opportunity_id = o.sf_opportunity_id
      AND c.global_entity_id = o.global_entity_id
    left join fulfillment-dwh-production.pandata_curated.pd_vendors v
      ON v.vendor_code = a.vendor_code
      AND v.global_entity_id = a.global_entity_id
    where c.is_deleted = false
    and lower(c.status) not like '%expired%'
    and lower(c.status) not like '%terminated%'
    and service_type in ('Commission Fee',/* 'Take Away Commission Fee', */ 'Logistics Fee')
    AND (lower(u.full_name) not like '%feng wang%' and lower(u.full_name) not like '%darryl chua%' and lower(u.full_name) not like '%test user%')
    and a.is_deleted = false
    AND lower(a.type) like '%branch%'
    AND a.is_marked_for_testing_training = false
    and LOWER(a.vertical) in ('restaurant')
    and date(c.start_date_local) >= '2021-01-01' 
    and date(o.opp_close_date_utc) >= '2021-05-01'
    and date(o.opp_close_date_utc) <= '2021-05-31'
    and (lower(o.opportunity_business_type) like '%upsell%' or c.name like '%commission%')
    and o.account_status <> 'Terminated'
    and (c.end_date_local is null or (date(c.start_date_local) < date(c.end_date_local)))
    and date(c.start_date_local) <= current_date() /* to exclude contracts starting in future */ 
    )
    
  ,vendor_contracts as ( /*list of renegoed vendors with all their commission contracts*/
    select
    l.global_entity_id,
    l.country_name,
    l.name as vendor_name,
    l.vendor_code,
    l.opp_close_date_utc,
    c.is_tiered_commission,
    c.sf_account_id,
    c.id as contract_id,
    c.name,
    c.start_date_local,
    c.end_date_local,
    c.commission_percentage,
    row_number() over (partition by c.sf_account_id order by c.start_date_local desc) as contract_seq,
    lag(c.name) over (partition by c.sf_account_id order by c.start_date_local desc) as new_comm_name,
    lag(c.commission_percentage) over (partition by c.sf_account_id order by c.start_date_local desc) as new_comm_rate,
    lag(c.start_date_local) over (partition by c.sf_account_id order by c.start_date_local desc) as new_start_date,
    case when c.commission_percentage = lag(c.commission_percentage) over (partition by c.sf_account_id order by c.start_date_local desc) then 1 else 0 end as fake_renego
    from fulfillment-dwh-production.pandata_curated.sf_contracts c
    join list_of_renego_vendors l 
      ON c.sf_account_id = l.sf_account_id
      AND c.global_entity_id = l.global_entity_id
    where c.is_deleted = false
    and lower(c.status) not like '%terminated%'
    and c.service_type in ('Commission Fee',/* 'Take Away Commission Fee', */ 'Logistics Fee')
    and lower(c.name) not like '%vd%'
    and date(c.start_date_local) <= current_date() /* to exclude contracts starting in future */ 
    order by 1,3 desc 
   
   )
    
SELECT
  g.country_name as country,
  g.vendor_name,
  h.vendor_code,
  m.chain_name,
  m.chain_code,
  g.sf_account_id,
  g.contract_id,
  g.name as contract_name,
  n.owner_name AS account_manager,
  n.is_key_vip_account as KA,
  date(g.start_date_local) as old_contract_start_date,
  date(g.end_date_local) as old_contract_end_date,
  g.commission_percentage as old_contract_comm_rate,
  g.new_comm_rate,
  SAFE_SUBTRACT(g.new_comm_rate, g.commission_percentage) as comm_rate_increase,
  (SAFE_SUBTRACT(g.new_comm_rate, g.commission_percentage)/100) *sum(x.gfv_eur) as commission_increase,
  date(g.new_start_date) as new_start_date,
  FORMAT_DATE("%Y-%m",date(g.opp_close_date_utc)) as renego_month,
  FORMAT_DATE("%Y-%V",date(g.opp_close_date_utc)) as renego_week,
  g.new_comm_name,
  sum(x.gfv_eur) as month_gfv_eur,
  sum(x.gmv_eur) as month_gmv_eur
FROM vendor_contracts g
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders h 
  ON g.global_entity_id = h.global_entity_id 
  AND h.vendor_code = g.vendor_code
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_vendors m 
  ON g.global_entity_id = m.global_entity_id 
  AND g.vendor_code = m.vendor_code
LEFT jOIN fulfillment-dwh-production.pandata_curated.sf_accounts n 
  ON g.global_entity_id = n.global_entity_id 
  AND g.vendor_code = n.vendor_code
LEFT JOIN fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting x 
  ON h.global_entity_id = x.global_entity_id 
  AND h.uuid = x.uuid
WHERE 
  fake_renego = 0
  AND contract_seq <> 1
  AND FORMAT_DATE("%Y-%m",date(g.new_start_date)) >= '2021-05'
  AND h.created_date_utc >= '2021-05-01'
  AND x.created_date_utc >= '2021-05-01'
  AND m.vertical = 'Restaurant'
  AND h.is_valid_order = true
  AND h.is_own_delivery = true
  AND n.is_marked_for_testing_training = false
  AND n.vertical = 'Restaurant'
  AND g.commission_percentage <> g.new_comm_rate
  AND g.new_comm_rate > 0
  AND g.country_name = 'Cambodia'
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,18,19,20

 ORDER BY month_gfv_eur DESC
