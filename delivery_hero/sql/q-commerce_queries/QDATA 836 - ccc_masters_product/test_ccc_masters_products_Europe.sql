 /*
The following query is meant to get basic metrics for Catalog data completeness.
Following the scoring system specified in https://docs.google.com/document/d/1k6BuQnQbsT-66aJK8WCtwwc1VXYR4YKoVm-uod1Wz2Q.
Many of the metrics are based on the product-level availability of language/country-specific fields for the 
language/country the vendor serves.
This is the first of the ccc scripts

Features:
shop type
brand
	-n languages
product title
	-n languages
	-duplicates
description
	-n languages
image urls
contents 
	-numberofunits
	-contentsvalue
	-contentsunit
weight
	-weightvalue
	-weightunit
volume
	-lengthincm
	-widthincm
	-heightincm
piecebarcode
vat_rate

The query does the following:
-collect features of master catalog products
-generate duplicates at the country_code/master_product_id unnesting the JSON string fields for:
	-all brand name languages for product
	-all product name languages
	-all description languages for product
	-all vat_rate country codes for product
*/


CREATE TEMP FUNCTION jsonKeys(input STRING)
RETURNS Array<String>
LANGUAGE js AS """
	if (input == null){
		return null
	}
	return Object.keys(JSON.parse(input));
""";

CREATE TEMP FUNCTION CheckGTIN(gtin_raw STRING, len INT64)
  RETURNS BOOL 
  LANGUAGE js AS """

	//null handler 
	if (gtin_raw == null){
		return false
	}

	// pad gtin
	gtin = gtin_raw.padStart(len, '0')
	
	// Define fixed variables 
	var CheckDigitArray = [];
	var gtinMaths = [3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3];
	var modifier = 17 - (gtin.length - 1); // Gets the position to place first digit in array
	var gtinCheckDigit = gtin.slice(-1);   // Get provided check digit
	var BarcodeArray = gtin.split("");     // Split barcode at each digit into array
	var gtinLength = gtin.length;
	var tmpCheckDigit = 0;
	var tmpCheckSum = 0;
	var tmpMath = 0;
	
	// Run through and put digits into multiplication table
	for (i=0; i < (gtinLength - 1); i++) {
		CheckDigitArray[modifier + i] = BarcodeArray[i];  // Add barcode digits to Multiplication Table
	}
	
	// Calculate "Sum" of barcode digits
	for (i=modifier; i < 17; i++) {
		tmpCheckSum += (CheckDigitArray[i] * gtinMaths[i]);
	}
		
	// Difference from Rounded-Up-To-Nearest-10 - Fianl Check Digit Calculation
	tmpCheckDigit = (Math.ceil(tmpCheckSum / 10) * 10) - parseInt(tmpCheckSum);
	
	// Check if last digit is same as calculated check digit
	if (gtin.slice(-1) == tmpCheckDigit){
		return true
	} else {
		return false
	};

""";
-- master products table
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_data_science_qcommerce._ccc_master_products_europe`
AS

WITH enrich AS (
	SELECT products.id || '_' || products.country_code AS uuid
		, products.id AS master_product_id
		, products.country_code AS country_code
		, products.title AS title
		, brand.name AS brand_name
		, products.description AS description 
		, ARRAY_LENGTH(split(products.images, 'imageServiceUrl')) - 1 AS n_img_urls 
		, products.number_of_units AS number_of_units
		, products.contents_value AS contents_value
		, IF(products.contents_unit = ''
			, NULL
			, products.contents_unit) AS contents_unit
		, products.weight_value AS weight_value
		, IF(products.weight_unit = ''
			,  NULL
			, products.weight_unit) AS weight_unit
		, products.length_in_cm AS length_in_cm
		, products.width_in_cm AS width_in_cm
		, products.height_in_cm AS height_in_cm
		, barcode.piece_barcode AS piece_barcode
		, CheckGTIN(barcode.piece_barcode, 14) AS barcode_is_valid
		, products.vat_rate AS vat_rate
		, products.code AS master_code
	FROM `fulfillment-dwh-production.dl_dmart.catalog_master_product` AS products
	LEFT JOIN `fulfillment-dwh-production.dl_dmart.catalog_brand` AS brand
		ON products.brand_id                                = brand.id
		AND products.country_code                           = brand.country_code
	LEFT JOIN `fulfillment-dwh-production.dl_dmart.catalog_master_product_barcode` AS mb
		ON products.id                                      = mb.master_product_id
		AND products.country_code                           = mb.country_code
	LEFT JOIN `fulfillment-dwh-production.dl_dmart.catalog_barcode` AS barcode
		ON mb.barcode_id                                    = barcode.id 
		AND mb.country_code                                 = barcode.country_code
    -- Filtering for Europe
   WHERE products.country_code IN (SELECT DISTINCT country_code FROM `fulfillment-dwh-production.cl_dmart.sources` WHERE region = 'Europe')
), unnested AS (
	-- using regex to get the values of dynamic JSON keys as BigQuey doesn't support it
	SELECT enrich.* EXCEPT(title, brand_name, description, vat_rate)
		, title_locale AS title_locale
		, SPLIT(title_locale,'_')[OFFSET(0)] AS title_language
		, REGEXP_EXTRACT(enrich.title
			, CONCAT(r"\"", title_locale, r"\":\s*\"(.*?)\"")) AS title
		, brand_name_locale AS brand_name_locale
		, SPLIT(brand_name_locale,'_')[OFFSET(0)] AS brand_name_language
		, REGEXP_EXTRACT(enrich.brand_name
			, CONCAT(r"\"", brand_name_locale, r"\":\s*\"(.*?)\"")) AS brand_name 
		, description_locale AS description_locale
		, SPLIT(description_locale,'_')[OFFSET(0)] AS description_language
		, REGEXP_EXTRACT(enrich.description
			, CONCAT(r"\"", description_locale, r"\":\s*\"(.*?)\"")) AS description
		, vat_country_code AS vat_country_code
		, REGEXP_EXTRACT(enrich.vat_rate
			, CONCAT(r"\"", vat_country_code, r"\":\s*(.*?)[}|,]")) AS vat_value
	FROM enrich
		LEFT JOIN UNNEST(jsonKeys(enrich.title)) AS title_locale
		LEFT JOIN UNNEST(jsonKeys(enrich.description)) AS description_locale
		LEFT JOIN UNNEST(jsonKeys(enrich.vat_rate)) AS vat_country_code
		LEFT JOIN UNNEST(jsonKeys(enrich.brand_name)) AS brand_name_locale
), pre_process AS (
	-- boolean transformations for ease of processing down the line
	SELECT unnested.* EXCEPT(description, vat_value)
		, IF(unnested.title = ''
			OR unnested.title IS NULL, FALSE, TRUE) AS is_valid_title
		, IF(unnested.brand_name = ''
			OR unnested.brand_name IN 
        ('Unknown Brand', 'Marca dummy', '-', 'DUMMY', 'dummy')
			OR unnested.brand_name IS NULL, FALSE, TRUE) AS is_valid_brand_name
		, IF(unnested.description = ''
			OR unnested.description IS NULL, FALSE, TRUE) AS is_valid_description
		, CAST(IF(unnested.vat_value = '', NULL, unnested.vat_value)  AS FLOAT64) AS vat_value
		, IF(unnested.vat_value = ''
			OR unnested.vat_value IS NULL, FALSE, TRUE) AS is_valid_vat_value
		-- for duplicate checking. unique instance of brand/item name per master product
		-- later comparing brand/title duplicates where this field = 1
		, ROW_NUMBER() OVER(
			PARTITION BY unnested.uuid
				, unnested.title_language
				, IF(unnested.brand_name = '', NULL, unnested.brand_name) #brand name
				, IF(unnested.title = '', NULL, unnested.title) #title
			ORDER BY IF(unnested.title = '', NULL, unnested.title)) AS unique_lang_name_instance
	FROM unnested
	WHERE unnested.title_locale != 'es-CL'
), name_duplicates AS (
	-- counts duplicates for product name in particular languages with different master_product_ids
	SELECT pre_process.brand_name AS brand_name
		, pre_process.title AS title
		, pre_process.title_language AS title_language
		, COUNT(1) AS n_duplicates
		, STRING_AGG(DISTINCT pre_process.uuid) AS duplicate_product_ids
	FROM pre_process
	WHERE pre_process.unique_lang_name_instance = 1
	AND pre_process.title != ''
	GROUP BY 1,2,3
)

SELECT pre_process.uuid AS uuid
	, pre_process.brand_name_language AS brand_name_language
	, pre_process.title_language AS title_language
	, pre_process.description_language AS description_language
	, pre_process.vat_country_code AS vat_country_code
	, MAX(pre_process.master_code) AS master_code
	, MAX(pre_process.master_product_id) AS master_product_id
	, MAX(pre_process.country_code) AS country_code
	, MAX(pre_process.brand_name) AS brand_name
	, LOGICAL_OR(pre_process.is_valid_brand_name) AS is_valid_brand_name
	, MAX(pre_process.title) AS title
	, LOGICAL_OR(pre_process.is_valid_title) AS is_valid_title
	, LOGICAL_OR(pre_process.is_valid_description) AS is_valid_description
	, MAX(name_duplicates.n_duplicates) -1 AS n_title_duplicates
	, MAX(name_duplicates.duplicate_product_ids) AS duplicate_product_ids
	, MAX(pre_process.n_img_urls) AS n_img_urls
	, MAX(pre_process.number_of_units) AS number_of_units
	, MAX(pre_process.contents_value) AS contents_value
	, MAX(pre_process.contents_unit) AS contents_unit
	, MAX(pre_process.weight_value) AS weight_value
	, MAX(pre_process.weight_unit) AS weight_unit
	, MAX(pre_process.length_in_cm) AS length_in_cm
	, MAX(pre_process.width_in_cm) AS width_in_cm
	, MAX(pre_process.height_in_cm) AS height_in_cm
	, COUNT(DISTINCT IF(pre_process.barcode_is_valid
		, pre_process.piece_barcode
		, NULL)) AS n_valid_barcodes
	, COUNT(DISTINCT pre_process.piece_barcode) AS n_barcodes
	, MAX(IF(pre_process.barcode_is_valid
		, pre_process.piece_barcode
		, NULL)) AS sample_barcode
	, LOGICAL_OR(pre_process.is_valid_vat_value) AS is_valid_vat_value
FROM pre_process
LEFT JOIN name_duplicates
	ON pre_process.brand_name = name_duplicates.brand_name
	AND pre_process.title = name_duplicates.title
	AND pre_process.title_language = name_duplicates.title_language
GROUP BY 1,2,3,4,5
