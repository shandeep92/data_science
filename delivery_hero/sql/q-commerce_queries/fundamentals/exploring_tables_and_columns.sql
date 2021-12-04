/* 1. Looking at column names and the respective data types */

SELECT 
    table_catalog, 
    table_schema, 
    table_name, 
    column_name, 
    data_type
FROM `fulfillment-dwh-production.cl_dmart`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
WHERE table_name = 'customer_orders'
-- AND column_name LIKE '%gmv%' --change field name being searched

----------------------------------------------------------------------------------
/* 2. An aggreated version of the above SQL statement */

SELECT 
	table_catalog, 
	table_schema, 
	table_name, 
	STRING_AGG(column_name, ', ') AS columns, 
	data_type
FROM `fulfillment-dwh-production.cl_dmart`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
WHERE table_name = 'customer_orders'
GROUP BY 1,2,3,5

---------------------------------------------------------------------------------

/* 3. Shorter form of 2. To check for which columns to join on. Create a table name and column */

SELECT table_catalog, table_name, STRING_AGG(column_name, ', ') AS columns
FROM fulfillment-dwh-production.cl_dmart.INFORMATION_SCHEMA.COLUMNS
WHERE table_catalog LIKE '%fulfillment-dwh-production%'
GROUP BY 1,2;