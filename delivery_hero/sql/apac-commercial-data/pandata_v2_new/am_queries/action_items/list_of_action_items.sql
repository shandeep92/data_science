
/* 1. This gives you all the action items in Laos - Filter accordingly to get action items for a certain country/AM
   2. You need special access for sensitive fields like AM emails etc --> */

SELECT 
  a.country_name                                                  AS country,
  a.sf_owner_id                                                   AS account_owner_id, 
  a.owner_name                                                    AS account_owner_name,
  u.id                                                            AS action_item_owner_id,
  u.title                                                         AS action_item_owner_role,
  u.full_name                                                     AS action_item_owner,
  u.email                                                         AS email,
  a.global_vendor_id                                              AS grid_id,
  a.name                                                          AS vendor_name,
  ai.type                                                         AS subject, 
  ai.name                                                         AS subject_elaboration, 
  ai.status                                                       AS status, 
  date(ai.start_at_utc)                                           AS start_date, 
  date(ai.end_at_utc)                                             AS end_date,
  date_diff(date(ai.end_at_utc),date(ai.start_at_utc),day)+1      AS days_needed,
  ai.priority                                                     AS priority, 
  ai.score_weighting                                              AS score_weighting, 
  ai.description                                                  AS description
  
FROM fulfillment-dwh-production.pandata_curated.sf_action_items   AS ai
LEFT JOIN fulfillment-dwh-production.pandata_curated.sf_accounts  AS a
       ON ai.sf_account_id = a.id
LEFT JOIN fulfillment-dwh-production.pandata_curated.sf_users     AS u
      ON ai.sf_owner_id = u.id
      
------ Choose your filters -----
-- WHERE a.owner_name IN ('Phoutthalak Phengthavy','Boudsaba Bolivong','Denphoum Sysaykeo','Sinlaphone Boupha','Somchit Phetsada', 'Sonekeo Sibounheuang','Sonevongsouda Luanglath','Vannaleuth Dangmany','Yutthasith Vongpraseuth')
-- AND country_name = 'Laos'
