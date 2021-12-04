/* Query entire promo roi table */

SELECT  
r.* EXCEPT(campaigns)
,   campaigns.*
FROM `dh-darkstores-live.cl_data_science_qcommerce._promo_roi_out_roi_reporting` AS r
WHERE date < CURRENT_DATE() 
AND campaigns.campaign_id IS NOT NULL
LIMIT 5
------------------------------------