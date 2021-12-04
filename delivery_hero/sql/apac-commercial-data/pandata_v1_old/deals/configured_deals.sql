WITH month_period AS (
  SELECT
    month
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH), DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH), INTERVAL 1 MONTH)) as month
  GROUP BY 1
),

business_type AS (
  SELECT
    DISTINCT business_type
  FROM pandata.dim_vendors
  GROUP BY 1
),

vendor_base_info AS (
SELECT 
    v.rdbms_id,
    v.country_name AS country,
    v.vendor_code,
    v.id AS vendor_id,
    v.business_type,
    CASE 
      WHEN vendor_gmv_class.gmv_class IS NULL 
      THEN NULL
      ELSE vendor_gmv_class.gmv_class
    END AS gmv_class,
    v.chain_code
  FROM `dhh---analytics-apac.pandata.dim_vendors` v 
  LEFT JOIN `dhh---analytics-apac.pandata.sf_accounts` sf_accounts_custom
         ON sf_accounts_custom.vendor_code = v.vendor_code 
        AND sf_accounts_custom.rdbms_id = v.rdbms_id
  LEFT JOIN `dhh---analytics-apac.pandata_report.vendor_gmv_class` vendor_gmv_class
         ON vendor_gmv_class.vendor_code = v.vendor_code 
        AND vendor_gmv_class.rdbms_id = v.rdbms_id
  WHERE NOT v.is_vendor_testing
),

voucher_deals AS (
  SELECT
    dim_vouchers.rdbms_id,
    dates.date,
    v.business_type,
    dim_vouchers.type AS deal_type,
    dim_vouchers.value AS amount_local,
    dim_vouchers.minimum_order_value_local AS MOV,
    dim_vouchers.foodpanda_ratio AS foodpanda_ratio,
    vendor_id,
    dim_vouchers.purpose AS deal_title,
    dim_vouchers.description,
    'Full Menu' AS condition_type,
    (CASE
      WHEN dim_vouchers.expedition_type LIKE '%delivery%'
           AND dim_vouchers.expedition_type LIKE '%pickup%' 
      THEN 'all'
      WHEN dim_vouchers.expedition_type IS NULL 
      THEN 'all'
      ELSE dim_vouchers.expedition_type
    END) AS expedition_type,
    DATE(dim_vouchers.start_date_local) AS deal_start,
    DATE(dim_vouchers.stop_at_local) AS deal_end,
    'is_voucher_deal' AS deal_segment,
    ROW_NUMBER() OVER (PARTITION BY dim_vouchers.rdbms_id, vendor_id, dates.date ORDER BY dim_vouchers.created_at_local DESC) = 1 AS is_latest_entry
  FROM pandata.dim_vouchers
  CROSS JOIN UNNEST(specific_vendor_ids) AS vendor_id
  LEFT JOIN pandata.dim_vendors v
         ON dim_vouchers.rdbms_id = v.rdbms_id
        AND vendor_id = v.id
  CROSS JOIN pandata.dim_dates AS dates
  WHERE dim_vouchers.start_date_local IS NOT NULL
    AND NOT dim_vouchers.is_deleted
    AND foodpanda_ratio < 100
    AND dates.date BETWEEN DATE(dim_vouchers.start_date_local) AND DATE(dim_vouchers.stop_at_local)
    AND dates.date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
    AND dates.date < DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 7 MONTH)
    AND DATE(dim_vouchers.start_date_local) <= dates.date
    AND DATE(dim_vouchers.stop_at_local) >= dates.date
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,dim_vouchers.created_at_local
  HAVING vendor_id IS NOT NULL
),

voucher_config AS (
  SELECT
    COALESCE(dim_countries.rdbms_id, voucher_deals.rdbms_id) AS rdbms_id,
    COALESCE(
      FORMAT_DATE("%Y-%m (%B)",mp.month),
      FORMAT_DATE("%Y-%m (%B)",voucher_deals.date)
    ) AS month,
    voucher_deals.date,
    COALESCE(
      business_type.business_type,
      voucher_deals.business_type
    ) AS business_type,
    voucher_deals.deal_type,
    voucher_deals.amount_local AS amount_local,
    voucher_deals.MOV AS MOV,
    voucher_deals.foodpanda_ratio AS foodpanda_ratio,
    voucher_deals.vendor_id,
    voucher_deals.deal_title,
    voucher_deals.description,
    voucher_deals.condition_type,
    voucher_deals.expedition_type,
    voucher_deals.deal_start AS deal_start,
    voucher_deals.deal_end AS deal_end,
    COALESCE(
      voucher_deals.deal_segment,
      'is_voucher_deal'
    ) AS deal_segment
  FROM pandata.dim_countries
  CROSS JOIN month_period mp
  CROSS JOIN business_type
  FULL JOIN voucher_deals
         ON dim_countries.rdbms_id = voucher_deals.rdbms_id
        AND FORMAT_DATE("%Y-%m (%B)",mp.month) = FORMAT_DATE("%Y-%m (%B)",voucher_deals.date)
        AND business_type.business_type = voucher_deals.business_type
  LEFT JOIN vendor_base_info
         ON dim_countries.rdbms_id = vendor_base_info.rdbms_id
        AND voucher_deals.vendor_id = vendor_base_info.vendor_id
  WHERE is_latest_entry
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ORDER BY 1,7
),

discount_deal as (
  SELECT
    dim_discounts.rdbms_id,
    dates.date,
    vendors.business_type,
    dim_discounts.discount_type AS deal_type,
    dim_discounts.amount_local,
    dim_discounts.minimum_order_value_local AS MOV,
    dim_discounts.foodpanda_ratio,
    dim_vendor_discounts.vendor_id,
    dim_vendor_discounts.title AS deal_title,
    dim_vendor_discounts.description,
    dim_vendor_discounts.condition_type,
    (CASE
      WHEN dim_discounts.expedition_type LIKE '%delivery%' AND dim_discounts.expedition_type LIKE '%pickup%' 
      THEN 'all'
      ELSE dim_discounts.expedition_type
    END) AS expedition_type,
    dim_discounts.start_date_local AS deal_start,
    dim_discounts.end_date_local AS deal_end,
    'is_discount_deal' AS deal_segment,
    ROW_NUMBER() OVER (PARTITION BY dim_discounts.rdbms_id, dim_vendor_discounts.vendor_id, dates.date ORDER BY dim_vendor_discounts.created_at_utc DESC) = 1 AS is_latest_entry
  FROM pandata.dim_discounts
  LEFT JOIN pandata.dim_vendor_discounts
         ON dim_discounts.rdbms_id = dim_vendor_discounts.rdbms_id
        AND dim_discounts.id = dim_vendor_discounts.discount_id
  LEFT JOIN pandata.dim_vendors AS vendors
         ON dim_discounts.rdbms_id = vendors.rdbms_id
        AND dim_vendor_discounts.vendor_id = vendors.id
  LEFT JOIN pandata.dim_dates AS dates
         ON dates.date BETWEEN dim_discounts.start_date_local AND dim_discounts.end_date_local
  WHERE dim_discounts.start_date_local IS NOT NULL
    AND ((dim_discounts.rdbms_id != 12 AND (dim_discounts.is_active OR dim_vendor_discounts.is_active))
         OR dim_discounts.rdbms_id = 12)
    AND (NOT dim_discounts.is_deleted OR NOT dim_vendor_discounts.is_deleted)
    AND dates.date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH)
    AND dates.date < DATE_ADD(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 7 MONTH)
    AND foodpanda_ratio < 100
    AND dim_discounts.start_date_local <= dates.date
    AND dim_discounts.end_date_local >= dates.date
    AND LOWER(dim_discounts.title) NOT LIKE '%testing%'
    AND LOWER(dim_discounts.description) NOT LIKE '%test%'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12, 13, 14, 15, dim_vendor_discounts.created_at_utc
),

discount_config AS (
  SELECT
    COALESCE(dim_countries.rdbms_id, discount_deal.rdbms_id) AS rdbms_id,
    COALESCE(
      FORMAT_DATE("%Y-%m (%B)",mp.month),
      FORMAT_DATE("%Y-%m (%B)",discount_deal.date)
    ) AS month,
    discount_deal.date,
    COALESCE(
      business_type.business_type,
      discount_deal.business_type
    ) AS business_type,
    discount_deal.deal_type,
    discount_deal.amount_local AS amount_local,
    discount_deal.MOV AS MOV,
    discount_deal.foodpanda_ratio AS foodpanda_ratio,
    discount_deal.vendor_id,
    discount_deal.deal_title,
    discount_deal.description,
    discount_deal.condition_type,
    discount_deal.expedition_type,
    discount_deal.deal_start AS deal_start,
    discount_deal.deal_end AS deal_end,
    COALESCE(
      discount_deal.deal_segment,
      'is_discount_deal'
    ) AS deal_segment
  FROM pandata.dim_countries
  CROSS JOIN month_period mp
  CROSS JOIN business_type
  FULL JOIN discount_deal
         ON dim_countries.rdbms_id = discount_deal.rdbms_id
        AND FORMAT_DATE("%Y-%m (%B)",mp.month) = FORMAT_DATE("%Y-%m (%B)",discount_deal.date)
        AND business_type.business_type = discount_deal.business_type
  LEFT JOIN vendor_base_info
         ON dim_countries.rdbms_id = vendor_base_info.rdbms_id
        AND discount_deal.vendor_id = vendor_base_info.vendor_id
  WHERE is_latest_entry
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ORDER BY 1,7
)

SELECT
 *
FROM discount_config
UNION ALL
SELECT
 *
FROM voucher_config
