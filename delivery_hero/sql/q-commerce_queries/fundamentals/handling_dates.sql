/* Date formats 
1 | 2021-08-05 | 2021Q3 | 202108 | Aug | W31-2021 | Thursday
*/

SELECT 
    DATE(order_placed_localtime_at)                       AS date, 
    FORMAT_DATETIME('%YQ%Q',order_placed_localtime_at)    AS year_quarter,
    FORMAT_DATETIME('%Y%m',order_placed_localtime_at)     AS year_month,
    FORMAT_DATETIME('%b',order_placed_localtime_at)       AS month_name,
    FORMAT_DATETIME('W%V-%Y', order_placed_localtime_at)  AS week_year,
    FORMAT_DATETIME('%A', order_placed_localtime_at)      AS weekday_name
FROM`fulfillment-dwh-production.cl_dmart.customer_orders`
WHERE DATE(order_placed_localtime_at) <= CURRENT_DATE()
AND   DATE(order_placed_localtime_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)                       --  Data from 1 months ago till current month
AND   DATE(order_placed_localtime_at) <= CURRENT_DATE()
ORDER BY 5

----------------------------------------------------------------------------------
/* 2. The GENERATE_DATE_ARRAY function accepts the following data types as inputs: 
GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL INT64_expr date_part])
1. start_date must be a DATE
2. end_date must be a DATE
3. INT64_expr must be an INT64
4. date_part must be either DAY, WEEK, MONTH, QUARTER, or YEAR.

Row | year_month | month_name
1   | 202106     | Jun
..
4   | 202109     | Sep
*/

With year_month AS (
    SELECT format_date('%Y%m',month) as year_month,
    FORMAT_DATE('%b',month) as month_name
FROM(
    SELECT 
    GENERATE_DATE_ARRAY(
    DATE_SUB(CURRENT_DATE(),INTERVAL 3 MONTH), -- start date: 3 months ago from today
    CURRENT_DATE(), -- end date: current date
    INTERVAL 1 MONTH) AS date -- in 1 month intervals
    ),
    UNNEST(DATE) as month) -- unnest the list form

-------------------------------------------------------------------------------------
/* 3. Similar ouput as 2 but using DATE_TRUNC & DATE_SUB() */

Row | year_month | month_name
1   | 202106     | Jun
..
4   | 202109     | Sep

SELECT 
    DISTINCT FORMAT_DATE('%Y%m',DATE_TRUNC(order_placed_localtime_at, MONTH)) AS year_month             -- Date trunc to get it all in month format
FROM `fulfillment-dwh-production.cl_dmart.customer_orders` 
WHERE DATE(order_placed_localtime_at) <= CURRENT_DATE()
AND   DATE(order_placed_localtime_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)                       --  Data from 3 months ago till current month
AND   DATE(order_placed_localtime_at) <= CURRENT_DATE()

---------------------------------------------------------------------------------------

/* 4. Look at the difference between two dates (e.g order date and invoice date)

DATE_DIFF(date_expression_a, date_expression_b, date_part) -
Creating a new column where I'm finding the number of days difference and adding an additional day -
e.g. 2020-10-25T03:41:10.096598 to 2020-10-28T04:21:15.404172 - new column will reflect '4' */

SELECT 
    DATE_DIFF(invoice_date, -- end date
    DATE(order_placed_at),  -- start date
    day)+1 AS difference_between_order_and_invoice_date, 
FROM `fulfillment-dwh-production.cl_dmart.customer_orders` 
WHERE DATE(order_placed_localtime_at) <= CURRENT_DATE()
AND   DATE(order_placed_localtime_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)                       --  Data from 3 months ago till current month
AND   DATE(order_placed_localtime_at) <= CURRENT_DATE()
AND   DATE_DIFF(invoice_date,DATE(order_placed_at),day)+1 IS NOT NULL
AND   DATE_DIFF(invoice_date,DATE(order_placed_at),day)+1 > 1
ORDER BY 1
