WITH source AS (
    SELECT * FROM {{ source('raw', 'json_files') }}
)

, team_details AS (
	SELECT * FROM {{ ref('stg_team_details') }}
)

, fpi_json AS (
	SELECT
	    year AS season_year
	    , week::INT AS season_week
	    , data AS raw_json
	FROM source src
	WHERE src.source_type = 'fpi_rankings'
)

, fpi_data AS (
	SELECT 
		season_year
		, season_week
		, elem ->> 'team' AS team_name
	    , (elem -> 'resumeRanks' ->> 'fpi')::INT AS fpi_rank
		, (elem ->> 'fpi')::NUMERIC AS fpi_rating
		, (elem -> 'resumeRanks' ->> 'gameControl')::INT AS game_control_rank
		, (elem -> 'resumeRanks' ->> 'strengthOfRecord')::INT AS sor
		, (elem -> 'resumeRanks' ->> 'strengthOfSchedule')::INT AS sos
		, (elem -> 'resumeRanks' ->> 'averageWinProbability')::INT AS avg_win_probability_rank
		, (elem -> 'resumeRanks' ->> 'remainingStrengthOfSchedule')::INT AS remaining_sos
		, (elem -> 'efficiencies' ->> 'overall')::NUMERIC AS overall_efficiency_rating
		, (elem -> 'efficiencies' ->> 'offense')::NUMERIC AS offense_efficiency_rating
		, (elem -> 'efficiencies' ->> 'defense')::NUMERIC AS defense_efficiency_rating
		, (elem -> 'efficiencies' ->> 'specialTeams')::NUMERIC AS special_teams_efficiency_rating
	FROM fpi_json
	, jsonb_array_elements(raw_json) AS elem
)

SELECT
	fd.season_year
	, fd.season_week
	, fd.team_name
	, td.team_id
	, td.team_abbreviation
	, td.conference
	, td.school_classification
	, fd.fpi_rank
	, fd.fpi_rating
	, fd.game_control_rank
	, fd.sor
	, fd.sos
	, fd.avg_win_probability_rank
	, fd.remaining_sos
	, fd.overall_efficiency_rating
	, fd.offense_efficiency_rating
	, fd.defense_efficiency_rating
	, fd.special_teams_efficiency_rating
FROM fpi_data fd
LEFT JOIN team_details td ON fd.team_name = td.team_name
ORDER BY 
	season_year
	, season_week
    , fpi_rank
