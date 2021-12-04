-- Scheduled query (Ask Valerie Ong)

SELECT `ncr_report_order_level`.`rdbms_id` AS `rdbms_id`,
  `ncr_report_order_level`.`vendor_name` AS `vendor_name`,
  `ncr_report_order_level`.`vendor_code` AS `vendor_code`,
  `ncr_report_order_level`.`booking_id` AS `booking_id`,
  `ncr_report_order_level`.`user` AS `user`,
  `ncr_report_order_level`.`alias` AS `alias`,
  `ncr_report_order_level`.`user_booked` AS `user_booked`,
  `ncr_report_order_level`.`year_month` AS `year_month`,
  `ncr_report_order_level`.`booking_week_year` AS `booking_week_year`,
  `ncr_report_order_level`.`booking_week` AS `booking_week`,
  `ncr_report_order_level`.`type` AS `type`,
  `ncr_report_order_level`.`status` AS `status`,
  CAST(`ncr_report_order_level`.`sold_price_local` AS FLOAT64) AS `sold_price_local`,
  CAST(`ncr_report_order_level`.`suggested_price_local` AS FLOAT64) AS `suggested_price_local`,
  CAST(`ncr_report_order_level`.`fx_rate_eur` AS FLOAT64) AS `fx_rate_eur`,
  CAST(`ncr_report_order_level`.`sold_price_eur` AS FLOAT64) AS `sold_price_eur`,
  CAST(`ncr_report_order_level`.`suggested_price_eur` AS FLOAT64) AS `suggested_price_eur`,
  `ncr_report_order_level`.`common_name` AS `common_name`,
  `ncr_report_order_level`.`promo_areas_sold` AS `promo_areas_sold`,
  `ncr_report_order_level`.`month_booked` AS `month_booked`,
  `ncr_report_order_level`.`month` AS `month`,
  `ncr_report_order_level`.`vendor_grade` AS `vendor_grade`,
  `ncr_report_order_level`.`gmv_class` AS `gmv_class`,
  `ncr_report_order_level`.`city_name` AS `city_name`,
  `ncr_report_order_level`.`channel` AS `channel`,
  `ncr_report_order_level`.`current_week` AS `current_week`,
  `ncr_report_order_level`.`report_weeks` AS `report_weeks`,
  `ncr_report_order_level`.`final_source` AS `final_source`
FROM `dhh---analytics-apac.pandata_ap_commercial`.`ncr_report_order_level` `ncr_report_order_level`
