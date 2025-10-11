WITH source AS (
    SELECT * FROM {{ source('raw', 'json_files') }}
)

, location_data AS (
	SELECT
		YEAR AS season_year
		, (elem -> 'location' ->> 'id')::INT AS location_id
		, (elem ->> 'id')::INT AS team_id
		, (elem -> 'location' ->> 'name')::TEXT AS location_name
		, (elem -> 'location' ->> 'capacity')::INT AS capacity
		, (elem -> 'location' ->> 'grass')::BOOL AS is_grass_field
		, (elem -> 'location' ->> 'dome')::BOOL AS is_dome
		, (elem -> 'location' ->> 'elevation')::NUMERIC AS elevation
		, (elem -> 'location' ->> 'constructionYear')::INT AS construction_year
		, (elem -> 'location' ->> 'city')::TEXT AS city
		, (elem -> 'location' ->> 'state')::TEXT AS state
		, (elem -> 'location' ->> 'zip')::TEXT AS zip_code
		, (elem -> 'location' ->> 'countryCode')::TEXT AS country
		, (elem -> 'location' ->> 'timezone')::TEXT AS timezone
		, (elem -> 'location' ->> 'latitude')::DOUBLE PRECISION AS latitude
		, (elem -> 'location' ->> 'longitude')::DOUBLE PRECISION AS longitude
	FROM SOURCE src 
	, jsonb_array_elements(data) AS elem
	WHERE src.source_type = 'team_info'
)

SELECT 
	*
FROM location_data
ORDER BY
	season_year
	, location_id