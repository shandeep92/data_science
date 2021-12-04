-- Root Cause for Unsuccessful action items

SELECT 
  a.sf_owner_id                                                       AS account_owner_id, 
  a.owner_name                                                        AS account_owner_name,
  u.id                                                                AS action_item_owner_id,
  u.title                                                             AS action_item_owner_role,
  u.full_name                                                         AS action_item_owner,
  u.email                                                             AS email,
  a.global_vendor_id                                                  AS grid_id,
  a.name                                                              AS vendor_name,
  ai.type                                                             AS type, 
  ai.name                                                             AS action_item_name, 
  ai.status                                                           AS status, 
  ai.root_cause                                                       AS root_cause,
  date(ai.start_at_utc)                                               AS start_date, 
  date(ai.end_at_utc)                                                 AS end_date,
  date_diff(date(ai.end_at_utc),date(ai.start_at_utc),day)+1          AS days_needed,
  ai.priority                                                         AS priority, 
  ai.score_weighting                                                  AS score_weighting, 
  ai.description                                                      AS description,
FROM `fulfillment-dwh-production.pandata_curated.sf_action_items` ai
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` a
       ON ai.sf_account_id = a.id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_users` u
       ON ai.sf_owner_id = u.id
WHERE ai.root_cause IS NOT NULL
--------- Choose your filters (Country/AM/Action Item type) ---- 
-- AND u.full_name IN ('Anjude De Leon','Joshua Nicolai Lim','Glaiza Mae Tacusalme','Christine Lacorte','Julien Brent Domingo','Danielle Flor Cunanan','Kristine Erica Ularte','Alleza Marra Santos','Mikee Cortes','Carlo Emmanuel Toledo','Chelsea Sio','Denzel Abad','Christine Jade Legaspi','Joseph Mendoza','Remigia Eleazar','Robert John Labalan','Ron Angelo Pineda','Mark Hanzel Dineros','Vonryan Meneses','Kathlyn Marie Rubio','Raven Roxas','Elver Derequito','Bernice Carlos','Mia Ysabel Andrea Miranda','Jose Mari Orlanda','Nicole Crisostomo','Bianca Laraya','Charmie Dacumos','Jizel Mistral Hacutina','Rhenz Frederick Haldos','Mary Annvic Cortes','Knightt Sjorgen Coloma','Rex Castro','James Paulo Pelagio','Wendelyn Dalagan','Fleurdelaine Pineda','Kyle Henry Lopez','Chyeanne Chua','Christian Sebastian','Jowyna Yap','Reginald Alejandro','Trixia Dichoso','Gypsy Anne Machacon','Zaira Ampongan','Trisia Ann Visitacion','Nikka Quiogue','John Oriel Bonzon','Eden Soriano','','Chino Bustamante','Janica Aira Galaroza','Johnmel Valerozo','Joseph James Danielle Matanguihan','Maria Betina Austria','Maricho Cadagat','Medyn Bagobo Lleve','Melissa Villegas','Roxan Beldua','Andreu Sebastien Sevillo','Bill Vendiola','Emmanuelle Enki San Jose','Jakelee Emmanuel Abing','Gel Ape','Mark Mallari','Brylle Cuares','Kymond Dimaandal','Paolo Biondi Te','Jasmin Valentin','Luis Del Rosario','Kerwin Tolentino','Sasha Raisa Pellano','Marie Genevieve Vicedo','Mariska Deanon','Mark Garcia','Nico Rafael Arciaga','Rashed Rubio','Renz Fernan Salenga','Ruskin Relucio','Therese Bantanto','Martian Earl Muyco','Kim Thomas Roa')


