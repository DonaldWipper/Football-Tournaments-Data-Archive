CREATE VIEW uefa_staging.stadiums AS
SELECT DISTINCT
    replace(JSON_EXTRACT(stadium, '$.id'), '"', '') as id,
    replace(JSON_EXTRACT(JSON_EXTRACT(stadium, '$.city'), '$.id'), '"', '') as city_id,
    replace(JSON_EXTRACT(JSON_EXTRACT(JSON_EXTRACT(JSON_EXTRACT(stadium, '$.city'), '$.translations'), '$.name'), '$.EN'), '"', '') as city_name,
    replace(JSON_EXTRACT(JSON_EXTRACT(JSON_EXTRACT(stadium, '$.translations'), '$.name'), '$.EN'), '"', '') as name,
    JSON_EXTRACT(JSON_EXTRACT(stadium, '$.geolocation'), '$.latitude') as lat,
    JSON_EXTRACT(JSON_EXTRACT(stadium, '$.geolocation'), '$.longitude') as lng,
    seasonYear,
    stadium

FROM  uefa_staging.timeline_all_uefa p