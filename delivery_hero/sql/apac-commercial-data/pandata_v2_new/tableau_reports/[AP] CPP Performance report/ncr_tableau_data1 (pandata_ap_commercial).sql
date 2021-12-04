-- Part of a scheduled query (Ask Valerie Ong)

SELECT `ncr_tableau_data1`.`rdbms_id` AS `rdbms_id`,
  `ncr_tableau_data1`.`global_entity_id` AS `global_entity_id`,
  `ncr_tableau_data1`.`common_name` AS `common_name`,
  `ncr_tableau_data1`.`year_month` AS `year_month`,
  `ncr_tableau_data1`.`month` AS `month`,
  `ncr_tableau_data1`.`total_ncr_revenue` AS `total_ncr_revenue`,
  `ncr_tableau_data1`.`premium_placement_revenue` AS `premium_placement_revenue`,
  `ncr_tableau_data1`.`organic_placement_revenue` AS `organic_placement_revenue`,
  `ncr_tableau_data1`.`pandabox_revenue` AS `pandabox_revenue`,
  `ncr_tableau_data1`.`cpc_revenue` AS `cpc_revenue`,
  `ncr_tableau_data1`.`onboardingfee_revenue` AS `onboardingfee_revenue`,
  `ncr_tableau_data1`.`total_ads_targets` AS `total_ads_targets`,
  `ncr_tableau_data1`.`total_ncr_targets` AS `total_ncr_targets`
FROM `dhh---analytics-apac.pandata_ap_commercial`.`ncr_tableau_data1` `ncr_tableau_data1`
