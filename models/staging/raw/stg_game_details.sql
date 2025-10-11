WITH source AS (
    SELECT * FROM {{ source('raw', 'json_files') }}
)

, game_data AS (
    SELECT
        YEAR AS season_year
        , (elem ->> 'id')::BIGINT AS game_id
        , (elem ->> 'season')::INT AS season
        , (elem ->> 'week')::INT AS season_week
        , (elem ->> 'seasonType')::TEXT AS season_type
        , COALESCE((elem ->> 'seasonType')::TEXT = 'regular', FALSE) AS is_regular_season
        , (elem ->> 'startDate')::TIMESTAMP AS kick_off_ts
        , (elem ->> 'startTimeTBD')::BOOL AS is_start_time_tbd
        , (elem ->> 'neutralSite')::BOOL AS is_neutral_site
        , (elem ->> 'conferenceGame')::BOOL AS is_conference_game
        , (elem ->> 'attendance')::INT AS attendance
        , (elem ->> 'venueId')::INT AS location_id
        , (elem ->> 'venue')::TEXT AS location_name
        , (elem ->> 'homeId')::INT AS home_team_id
        , (elem ->> 'homeTeam')::TEXT AS home_team
        , (elem ->> 'homeConference')::TEXT AS home_conference
        , (elem ->> 'homeClassification')::TEXT AS home_classification
        , (elem ->> 'homePregameElo')::NUMERIC AS home_pregame_elo
        , (elem ->> 'homePostgameElo')::NUMERIC AS home_postgame_elo
        , (elem ->> 'homePostgameWinProbability')::NUMERIC AS home_postgame_win_probability
        , (elem ->> 'homePoints')::INT AS home_points
        , (elem ->> 'homeLineScores')::JSONB AS home_line_scores
        , (elem ->> 'awayId')::INT AS away_team_id
        , (elem ->> 'awayTeam')::TEXT AS away_team
        , (elem ->> 'awayConference')::TEXT AS away_conference
        , (elem ->> 'awayClassification')::TEXT AS away_classification
        , (elem ->> 'awayPregameElo')::NUMERIC AS away_pregame_elo
        , (elem ->> 'awayPostgameElo')::NUMERIC AS away_postgame_elo
        , (elem ->> 'awayPostgameWinProbability')::NUMERIC AS away_postgame_win_probability
        , (elem ->> 'awayPoints')::INT AS away_points
        , (elem ->> 'awayLineScores')::JSONB AS away_line_scores
        , (elem ->> 'excitementIndex')::NUMERIC AS excitement_index
        , (elem ->> 'completed')::BOOL AS is_game_completed
    FROM source src
    , jsonb_array_elements(data) AS elem
    WHERE src.file_name IN ('2025_game_list.json')
)

SELECT 
	*
FROM game_data
ORDER BY
	season_year
	, game_id