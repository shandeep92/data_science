-- Part of a scheduled query (Ask Valerie Ong)

ELECT `ncr_report_pandabox_channel`.`rdbms_id` AS `rdbms_id`,
  `ncr_report_pandabox_channel`.`global_entity_id` AS `global_entity_id`,
  `ncr_report_pandabox_channel`.`country_name` AS `country_name`,
  `ncr_report_pandabox_channel`.`yearmonth` AS `yearmonth`,
  `ncr_report_pandabox_channel`.`month_name` AS `month_name`,
  `ncr_report_pandabox_channel`.`final_channel` AS `final_channel`,
  `ncr_report_pandabox_channel`.`vendor_grade` AS `vendor_grade`,
  `ncr_report_pandabox_channel`.`final_source` AS `final_source`,
  CAST(`ncr_report_pandabox_channel`.`pandabox_revenue` AS FLOAT64) AS `pandabox_revenue`,
  `ncr_report_pandabox_channel`.`pandabox_orders` AS `pandabox_orders`,
  `ncr_report_pandabox_channel`.`number_of_vendors` AS `number_of_vendors`
FROM `dhh---analytics-apac.pandata_ap_commercial`.`ncr_report_pandabox_channel` `ncr_report_pandabox_channel`
