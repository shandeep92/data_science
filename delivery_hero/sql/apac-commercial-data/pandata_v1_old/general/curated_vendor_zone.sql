CREATE TEMP FUNCTION PROPER(str STRING) AS (( 
  SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(w,1,1)), LOWER(SUBSTR(w,2))), ' ' ORDER BY pos) 
  FROM UNNEST(SPLIT(str, ' ')) w WITH OFFSET pos
));

WITH vendors AS (
SELECT
    rdbms_id,
    id,
    vendor_code,
    ST_GEOGPOINT(longitude, latitude) AS vendor_location
FROM pandata.dim_vendors
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
),
zones AS (
SELECT
    rdbms_id,
    id,
    name,
    shape AS zone_polygon,
    area
FROM pandata.lg_zones
WHERE shape IS NOT NULL
  AND is_active
)
SELECT
    v.rdbms_id, 
    v.id AS vendor_id,
    v.vendor_code,
    h.id AS zone_id,
    PROPER(TRIM(REPLACE(REPLACE(REPLACE(LOWER(h.name),'walker',''),'rider',''),'halal',''))) AS zone_name,
    ST_CONTAINS(zone_polygon, vendor_location) as within_zone,
    h.area
FROM vendors v
JOIN zones h ON v.rdbms_id = h.rdbms_id
WHERE ST_CONTAINS(zone_polygon, vendor_location) IS TRUE
