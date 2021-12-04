# Commercial Data


### Tableau Dashboards
**[Folder containing Tableau reports and data sources powering them](https://github.com/deliveryhero/apac-commercial-data/tree/main/pandata_v2_new/tableau_reports)**

-------------------------------------------------------------------------------------------------------------------------------------------------

### SQL Queries

This repository seeks to consolidate all the queries within the commercial team.
Please refer to the documentation **[here](https://confluence.deliveryhero.com/display/AD/PanData+v2)** for a detailed guide on how to navigate PanData V2.

**Useful Queries**

**To look for specific columns within the database**
```sql
SELECT table_catalog, table_schema, table_name, column_name
FROM `fulfillment-dwh-production.pandata_curated`.INFORMATION_SCHEMA.COLUMNS
WHERE column_name LIKE '%gfv%' --change field name being searched
ORDER BY 3
```

|table_catalog|table_schema|table_name|column_name|
|---|---|---|---|
|fulfillment-dwh-production|pandata_curated|pd_orders_agg_accounting|initial_gfv_local|
|fulfillment-dwh-production|pandata_curated|pd_orders_agg_accounting|gfv_local|
|fulfillment-dwh-production|pandata_curated|pd_orders_agg_accounting|initial_gfv_eur|
|fulfillment-dwh-production|pandata_curated|pd_orders_agg_accounting|gfv_eur|

-------------------------------------------------------------------------------------------------------------------------------------------------
### Navigating GitHub

**Contributing to the Repository**

- To add a folder to the repository (e.g AM Queries): **[Link](https://github.community/t/add-a-folder/2304)**
- To add a file to the repository: **[Link](https://docs.github.com/en/github/managing-files-in-a-repository/managing-files-on-github/adding-a-file-to-a-repository)**
- Copy and paste your queries into a file and ensure that you add ".sql" after commmitting so that it stores as a sql file.
    - e.g. Not "number_of_active_vendors" **but** "number_of_active_vendors.sql"

