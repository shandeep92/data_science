/* Retrieving AM particulars but you would need special permissions for some of the fields --> 
https://jira.deliveryhero.com/plugins/servlet/desk/portal/91/create/1488 */

SELECT 
  DISTINCT 
  users.country                                   AS country, 
  users.city                                      AS city, 
  users.central_job_role                          AS central_job_role, 
  users.id                                        AS am_sf_id, 
  users.title                                     AS am_job_title,
  users.full_name                                 AS am_name, 
  users.email                                     AS am_email
FROM fulfillment-dwh-production.pandata_curated.sf_users users
--- Choose your filters (Ensure it is an active AM) --
WHERE users.country = 'Cambodia'
AND is_active
ORDER BY 1, CASE WHEN am_name Like '%Sophorn%' THEN 0 ELSE 1 END ASC, 5 ASC, 6 ASC -- Just ordered by team lead so they show up first (not important)
