SELECT *
FROM pandata_ap_commercial.ncr_ads_channel_target_material
where yearmonth >= CAST(FORMAT_DATE('%Y%m',DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 3 MONTH)) AS INT64)
and yearmonth <= CAST(FORMAT_DATE('%Y%m',DATE_ADD(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 2 MONTH)) AS INT64)
