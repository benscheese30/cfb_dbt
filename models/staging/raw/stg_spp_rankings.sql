WITH source AS (
    SELECT * FROM {{ source('raw', 'json_files') }}
)

, team_details AS (
	SELECT * FROM {{ ref('stg_team_details') }} td
)

, spp_json AS (
	SELECT
	    year AS season_year
	    , CASE WHEN week = 'minus_1' THEN -1::INT ELSE week::INT END AS season_week
	    , data AS raw_json
	FROM source src
	WHERE src.source_type = 'sp_pull_rankings'
)

, spp_data AS (
	SELECT 
		season_year
		, season_week
		, elem ->> 'team' AS team_name
		, (elem ->> 'ranking')::NUMERIC AS spp_rank
		, (elem ->> 'rating')::NUMERIC AS spp_rating
		, (elem -> 'offense' ->> 'rating')::NUMERIC AS offense_rating
		, (elem -> 'defense' ->> 'rating')::NUMERIC AS defense_rating
		, (elem -> 'specialTeams' ->> 'rating')::NUMERIC AS special_teams_rating
	FROM spp_json
	, jsonb_array_elements(raw_json) AS elem
)

SELECT
	spp.season_year
	, spp.season_week
	, spp.team_name
	, td.team_id
	, spp.spp_rank
	, spp.spp_rating
	, spp.offense_rating
	, spp.defense_rating
	, spp.special_teams_rating
FROM spp_data spp
LEFT JOIN team_details td ON spp.team_name = td.team_name
WHERE spp.team_name != 'nationalAverages'
ORDER BY 
	season_year
	, season_week
    , spp_rank
