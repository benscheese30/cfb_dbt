WITH source AS (
    SELECT * FROM {{ source('raw', 'json_files') }}
)

, base_data AS (
    SELECT
        YEAR AS season_year
        , week::INT AS season_week
        , (regexp_match(elem ->> '$ref', 'events/(\d+)/competitions'))[1]::BIGINT AS game_id
        , elem ->> 'name' AS game_name
        , elem ->> 'shortName' AS game_abbr_name
        , (regexp_match(elem -> 'awayTeam' -> 'team' ->> '$ref', 'teams/(\d+)'))[1]::INT AS away_team_id
        , (regexp_match(elem -> 'homeTeam' -> 'team' ->> '$ref', 'teams/(\d+)'))[1]::INT AS home_team_id
        , elem -> 'awayTeam' -> 'statistics' AS away_statistics
        , elem -> 'homeTeam' -> 'statistics' AS home_statistics
        , (elem ->> 'lastModified')::TIMESTAMP AS last_modified
    FROM source src
    , jsonb_array_elements(data) AS elem
    WHERE src.source_type = 'fpi_predictor'
)

, away_stats AS (
    SELECT
        season_year
        , season_week
        , game_id
        , away_team_id AS team_id
        , FALSE AS is_home_game
        , game_name
        , game_abbr_name
        , MAX(CASE WHEN stat ->> 'name' = 'gameProjection' THEN (stat ->> 'value')::NUMERIC END) AS win_probability
        , MAX(CASE WHEN stat ->> 'name' = 'gameQuality' THEN (stat ->> 'value')::NUMERIC END) AS game_quality
        , MAX(CASE WHEN stat ->> 'name' = 'matchupQuality' THEN (stat ->> 'value')::NUMERIC END) AS matchup_quality
        , MAX(CASE WHEN stat ->> 'name' = 'oppSeasonStrengthFbsRank' THEN (stat ->> 'value')::NUMERIC END) AS opp_season_strength_fbs_rank
        , MAX(CASE WHEN stat ->> 'name' = 'oppSeasonStrengthRating' THEN (stat ->> 'value')::NUMERIC END) AS opp_season_strength_rating
        , MAX(CASE WHEN stat ->> 'name' = 'teamAdjAvgWp' THEN (stat ->> 'value')::NUMERIC END) AS team_adj_avg_wp
        , MAX(CASE WHEN stat ->> 'name' = 'teamAdjGameScore' THEN (stat ->> 'value')::NUMERIC END) AS team_adj_game_score
        , MAX(CASE WHEN stat ->> 'name' = 'teamAdjWinPct' THEN (stat ->> 'value')::NUMERIC END) AS team_adj_win_pct
        , MAX(CASE WHEN stat ->> 'name' = 'teamAvgWp' THEN (stat ->> 'value')::NUMERIC END) AS team_avg_wp
        , MAX(CASE WHEN stat ->> 'name' = 'teamChanceLoss' THEN (stat ->> 'value')::NUMERIC END) AS team_chance_loss
        , MAX(CASE WHEN stat ->> 'name' = 'teamDefEff' THEN (stat ->> 'value')::NUMERIC END) AS team_def_eff
        , MAX(CASE WHEN stat ->> 'name' = 'teamOffEff' THEN (stat ->> 'value')::NUMERIC END) AS team_off_eff
        , MAX(CASE WHEN stat ->> 'name' = 'teamPctHfa' THEN (stat ->> 'value')::NUMERIC END) AS team_pct_hfa
        , MAX(CASE WHEN stat ->> 'name' = 'teamPredPtDiff' THEN (stat ->> 'value')::NUMERIC END) AS team_pred_pt_diff
        , MAX(CASE WHEN stat ->> 'name' = 'teamRawGameScore' THEN (stat ->> 'value')::NUMERIC END) AS team_raw_game_score
        , MAX(CASE WHEN stat ->> 'name' = 'teamSTEff' THEN (stat ->> 'value')::NUMERIC END) AS team_st_eff
        , MAX(CASE WHEN stat ->> 'name' = 'teamTotEff' THEN (stat ->> 'value')::NUMERIC END) AS team_tot_eff
        , last_modified
    FROM base_data
    , jsonb_array_elements(away_statistics) AS stat
    GROUP BY season_year, season_week, game_id, away_team_id, game_name, game_abbr_name, last_modified
)

, home_stats AS (
    SELECT
        season_year
        , season_week
        , game_id
        , home_team_id AS team_id
        , TRUE AS is_home_game
        , game_name
        , game_abbr_name
        , MAX(CASE WHEN stat ->> 'name' = 'gameProjection' THEN (stat ->> 'value')::NUMERIC END) AS win_probability
        , MAX(CASE WHEN stat ->> 'name' = 'gameQuality' THEN (stat ->> 'value')::NUMERIC END) AS game_quality
        , MAX(CASE WHEN stat ->> 'name' = 'matchupQuality' THEN (stat ->> 'value')::NUMERIC END) AS matchup_quality
        , MAX(CASE WHEN stat ->> 'name' = 'oppSeasonStrengthFbsRank' THEN (stat ->> 'value')::NUMERIC END) AS opp_season_strength_fbs_rank
        , MAX(CASE WHEN stat ->> 'name' = 'oppSeasonStrengthRating' THEN (stat ->> 'value')::NUMERIC END) AS opp_season_strength_rating
        , MAX(CASE WHEN stat ->> 'name' = 'teamAdjAvgWp' THEN (stat ->> 'value')::NUMERIC END) AS team_adj_avg_wp
        , MAX(CASE WHEN stat ->> 'name' = 'teamAdjGameScore' THEN (stat ->> 'value')::NUMERIC END) AS team_adj_game_score
        , MAX(CASE WHEN stat ->> 'name' = 'teamAdjWinPct' THEN (stat ->> 'value')::NUMERIC END) AS team_adj_win_pct
        , MAX(CASE WHEN stat ->> 'name' = 'teamAvgWp' THEN (stat ->> 'value')::NUMERIC END) AS team_avg_wp
        , MAX(CASE WHEN stat ->> 'name' = 'teamChanceLoss' THEN (stat ->> 'value')::NUMERIC END) AS team_chance_loss
        , MAX(CASE WHEN stat ->> 'name' = 'teamDefEff' THEN (stat ->> 'value')::NUMERIC END) AS team_def_eff
        , MAX(CASE WHEN stat ->> 'name' = 'teamOffEff' THEN (stat ->> 'value')::NUMERIC END) AS team_off_eff
        , MAX(CASE WHEN stat ->> 'name' = 'teamPctHfa' THEN (stat ->> 'value')::NUMERIC END) AS team_pct_hfa
        , MAX(CASE WHEN stat ->> 'name' = 'teamPredPtDiff' THEN (stat ->> 'value')::NUMERIC END) AS team_pred_pt_diff
        , MAX(CASE WHEN stat ->> 'name' = 'teamRawGameScore' THEN (stat ->> 'value')::NUMERIC END) AS team_raw_game_score
        , MAX(CASE WHEN stat ->> 'name' = 'teamSTEff' THEN (stat ->> 'value')::NUMERIC END) AS team_st_eff
        , MAX(CASE WHEN stat ->> 'name' = 'teamTotEff' THEN (stat ->> 'value')::NUMERIC END) AS team_tot_eff
        , last_modified
    FROM base_data
    , jsonb_array_elements(home_statistics) AS stat
    GROUP BY season_year, season_week, game_id, home_team_id, game_name, game_abbr_name, last_modified
)

, game_metrics AS (
	SELECT * FROM away_stats
	UNION ALL
	SELECT * FROM home_stats
)

SELECT
	*
FROM game_metrics
ORDER BY 
    game_id
    , season_week
    , team_id