WITH source AS (
    SELECT * FROM {{ source('raw', 'json_files') }}
)

, team_data AS (
	SELECT
		YEAR AS season_year
		, (elem ->> 'id')::INT AS team_id
		, elem ->> 'school' AS team_name
		, elem ->> 'abbreviation' AS team_abbreviation
		, elem ->> 'classification' AS school_classification
		, elem ->> 'conference' AS conference
		, elem ->> 'division' AS conference_division
		, elem ->> 'color' AS primary_color
		, elem ->> 'alternateColor' AS alternate_color
		, elem ->> 'mascot' AS mascot
		, (elem -> 'logos' ->> 0)::TEXT AS logo_1
		, (elem -> 'logos' ->> 1)::TEXT AS logo_2
	FROM SOURCE src 
	, jsonb_array_elements(data) AS elem
	WHERE src.source_type = 'team_info'
)

SELECT 
	*
FROM team_data
ORDER BY
	season_year
	, team_id