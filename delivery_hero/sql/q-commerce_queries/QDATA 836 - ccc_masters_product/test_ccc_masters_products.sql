
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_data_science_qcommerce._ccc_master_products`
AS

WITH combined_ccc_masters_products_regions AS (
    SELECT * FROM `{{ params.project_id }}.cl_data_science_qcommerce._ccc_master_products_asia`
    UNION ALL  
    SELECT * FROM `{{ params.project_id }}.cl_data_science_qcommerce._ccc_master_products_europe`
    UNION ALL 
    SELECT * FROM `{{ params.project_id }}.cl_data_science_qcommerce._ccc_master_products_mena`
    UNION ALL 
    SELECT * FROM `{{ params.project_id }}.cl_data_science_qcommerce._ccc_master_products_america`
)

SELECT *
FROM combined_ccc_masters_products_regions