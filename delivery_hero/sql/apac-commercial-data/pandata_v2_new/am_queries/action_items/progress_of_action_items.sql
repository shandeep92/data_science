/* Progess of action items in a given month */

WITH action_items AS (
     SELECT 
        u.country                                                                       AS country,
        a.sf_owner_id                                                                   AS account_owner_id, 
        a.owner_name                                                                    AS account_owner_name,
        u.id                                                                            AS action_item_owner_id,
        u.title                                                                         AS action_item_owner_role,
        u.full_name                                                                     AS action_item_owner,
        u.email                                                                         AS email,
        a.global_vendor_id                                                              AS grid_id,
        a.name                                                                          AS vendor_name,
        ai.type                                                                         AS type, 
        ai.name                                                                         AS action_item_name, 
        ai.status                                                                       AS type, 
        date(ai.start_at_utc)                                                           AS start_date, 
        date(ai.end_at_utc)                                                             AS end_date,
        date_diff(date(ai.end_at_utc),date(ai.start_at_utc),day)+1                      AS days_needed,
        ai.priority                                                                     AS priority, 
        ai.score_weighting                                                              AS score_weighting, 
        ai.description                                                                  AS description
    FROM `fulfillment-dwh-production.pandata_curated.sf_action_items` ai
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` a
           ON ai.sf_account_id = a.id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_users` u
           ON ai.sf_owner_id = u.id                                 
    WHERE a.is_marked_for_testing_training = FALSE                 
    AND a.vertical = 'Restaurant'                                
    AND a.global_entity_id LIKE 'FP_%'                                                                             
    AND a.global_entity_id NOT IN ('FP_BG', 'FP_RO', 'FP_DE'))

SELECT 
    FORMAT_DATE('%Y%m',DATE_TRUNC(end_date, MONTH))                                     AS yearmonth, 
    country                                                                             AS country, 
    action_item_owner_role                                                              AS action_item_owner_role , 
    action_item_owner                                                                   AS action_item_owner,
    type                                                                                AS type,
    COUNT(type)                                                                         AS total_no_action_items,
    COUNT(CASE WHEN status = 'New'                    THEN type END)                    AS action_items_new, 
    COUNT(CASE WHEN status = 'In Progess'             THEN type END)                    AS action_items_in_progress, 
    COUNT(CASE WHEN status = 'Successful'             THEN type END)                    AS action_items_successful, 
    COUNT(CASE WHEN status = 'Not Successful'         THEN type END)                    AS action_items_not_successful, 
    COUNT(CASE WHEN status = 'On Hold'                THEN type END)                    AS action_items_on_hold, 
    COUNT(CASE WHEN status = 'Unreachable'            THEN type END)                    AS action_items_unreachable,
    SAFE_DIVIDE(COUNT(CASE WHEN status = 'Successful' THEN type END),COUNT(type)) * 100 AS successful_action_items_perc
FROM action_items
--- Choose your filters -- 
-- WHERE action_item_owner IN ('Anjude De Leon','Joshua Nicolai Lim')
-- AND FORMAT_DATE('%Y%m',DATE_TRUNC(end_date, MONTH)) = '202108'
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC, 2
