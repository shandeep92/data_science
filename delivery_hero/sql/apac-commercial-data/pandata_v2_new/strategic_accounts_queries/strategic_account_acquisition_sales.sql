with brands_that_went_active as (
select
sf_account_id,
created_date,
IF (COUNT(created_date) OVER (PARTITION BY sf_account_id) >1, 'Winback','Standard')AS Winback_Standard
from `fulfillment-dwh-production.pandata_curated.sf_account_history`
where new_value = 'Active'
and field = 'Account_Status__c'
-- and created_date < date_sub(date_trunc(created_date,month),interval 1 month) 
and created_date < date_trunc(current_date(),month)
group by 1,2
),

active_parent as (select
id as sf_brand_id,
country_name as country,
global_entity_id,
global_vendor_id,
vertical,
vertical_segment,
name as account_name,
b.created_date as brand_account_activated_date,
status,
Winback_Standard
FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` as accounts
inner join brands_that_went_active as b on accounts.id = b.sf_account_id
where global_entity_id LIKE '%FP_%'
and type in ('Group','Brand')
and global_entity_id <> 'FP_DE'
and status = 'Active'
)

select
saleslead.country,
saleslead.city,
saleslead.account_name,
saleslead.sf_account_id,
acct_parent.id AS parent_id,
acct_parent.name AS parent_name,
vendors.chain_name,
IF (vendors.chain_name IS NULL,acct_parent.name,vendors.chain_name) AS brand_name,
vendors.global_vendor_id,
saleslead.sf_opportunity_id,
saleslead.global_vendor_id as grid_id,
saleslead.vendor_type,
saleslead.is_key_vip_account,
saleslead.key_account_sub_category,
saleslead.opportunity_business_type,
saleslead.opportunity_status,
saleslead.opp_stage_name,
saleslead.account_created_date_utc,
saleslead.first_qc_submission_date_utc,
saleslead.first_qc_success_date_utc,
saleslead.no_of_qc_cases,
saleslead.activation_date,
active_parent.Winback_Standard AS parent_winback,
IF(saleslead.is_key_vip_account IS TRUE AND saleslead.opportunity_business_type IN ('New Business','Win Back'),
DATE_DIFF(saleslead.first_qc_submission_date_utc, saleslead.account_created_date_utc, DAY),NULL) AS ka_turnaroundtime,
   
IF(active_parent.sf_brand_id IS NULL,'no_active_parent','yes_active_parent') AS active_parent_check,
trial_delivery_comm_rate,
non_trial_delivery_comm_rate,
max(contract.commission_percentage) as  comm_rate
from `fulfillment-dwh-production.pandata_report.sales_lead_account_opp_aggregated` saleslead

LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` acct
    ON saleslead.global_vendor_id = acct.global_vendor_id

LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` acct_parent
    ON acct.sf_parent_account_id = acct_parent.id

LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` vendors
ON saleslead.global_vendor_id = vendors.global_vendor_id

LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_contracts` contract
    ON acct.id = contract.sf_account_id
    
LEFT JOIN active_parent
  ON acct.sf_parent_account_id = active_parent.sf_brand_id

where saleslead.sf_opportunity_id is not null
and saleslead.opportunity_business_type <> 'Upgrade/Upsell'
and saleslead.vendor_type = 'Restaurants'
and saleslead.is_key_vip_account
-- and saleslead.first_qc_success_date_utc is not null
and saleslead.is_lead_converted
-- and saleslead.first_qc_success_date_utc >= DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)   --change here for signed date
-- and saleslead.first_qc_success_date_utc < '2021-08-01'   --change here for signed date
-- and saleslead.country = 'Hong Kong'

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
order by saleslead.account_created_date_utc desc
