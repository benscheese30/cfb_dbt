WITH team_records AS (
	SELECT
		team_id
		, SUM(CASE WHEN is_win THEN 1 ELSE 0 END) AS wins
		, COUNT(game_id) AS games_played
		, ROUND(SUM(CASE WHEN is_win THEN 1 ELSE 0 END) / COUNT(game_id)::NUMERIC, 5) AS win_pct
	FROM {{ ref('int_team_fpi_postgame_details')}}
	GROUP BY 
		team_id
)

, bowl_eligible_teams AS (
	SELECT 
		*
		, CASE WHEN wins > 5 THEN TRUE ELSE FALSE END AS is_bowl_eligible
	FROM team_records
)

, opp_records AS (
	SELECT
		fpd.team_id
		, COALESCE(team_classification = 'fbs') AS is_fbs 
		, fpd.game_id
		, opp_team
		, opp_classification
		, COALESCE(opp_classification = 'fbs') AS is_opp_fbs
		, is_conference_game
		, is_win
		, bet.wins
		, bet.games_played
		, bet.win_pct
		, bet.is_bowl_eligible
	FROM cfb_intermediate.int_team_fpi_postgame_details fpd
	LEFT JOIN bowl_eligible_teams bet ON fpd.opp_team_id = bet.team_id
	ORDER BY 
		fpd.team_id
		, fpd.game_id
)

, win_totals AS (
    SELECT
        team_id
        , SUM(is_win::INTEGER) AS wins
        , COUNT(game_id) AS games
        , SUM(is_bowl_eligible::INTEGER) AS bowl_eligible_teams
        , SUM(CASE WHEN is_win AND is_bowl_eligible THEN 1 ELSE 0 END) AS bowl_eligible_team_wins
        , SUM(CASE WHEN is_win AND NOT is_bowl_eligible THEN 1 ELSE 0 END) AS non_bowl_eligible_team_wins
        , SUM(wins) AS all_overall_wins
        , SUM(games_played) AS all_overall_games
        , SUM(CASE WHEN is_opp_fbs THEN wins ELSE 0 END) AS opp_overall_wins
        , SUM(CASE WHEN is_opp_fbs THEN games_played ELSE 0 END) AS opp_overall_games
        , SUM(CASE WHEN is_win AND is_opp_fbs THEN wins ELSE 0 END) AS sov_overall_opp_wins
        , SUM(CASE WHEN is_win AND is_opp_fbs THEN games_played ELSE 0 END) AS sov_overall_opp_games
        , SUM(CASE WHEN is_opp_fbs AND NOT is_conference_game THEN wins ELSE 0 END) AS ooc_wins
        , SUM(CASE WHEN is_opp_fbs AND NOT is_conference_game THEN games_played ELSE 0 END) AS ooc_games
        , SUM(CASE WHEN is_win AND is_opp_fbs AND NOT is_conference_game THEN wins ELSE 0 END) AS sov_ooc_opp_wins
        , SUM(CASE WHEN is_win AND is_opp_fbs AND NOT is_conference_game THEN games_played ELSE 0 END) AS sov_ooc_opp_games
        , SUM(CASE WHEN is_opp_fbs AND is_conference_game THEN wins ELSE 0 END) AS conf_wins
        , SUM(CASE WHEN is_opp_fbs AND is_conference_game THEN games_played ELSE 0 END) AS conf_games
        , SUM(CASE WHEN is_win AND is_opp_fbs AND is_conference_game THEN wins ELSE 0 END) AS sov_conf_opp_wins
        , SUM(CASE WHEN is_win AND is_opp_fbs AND is_conference_game THEN games_played ELSE 0 END) AS sov_conf_opp_games
        , SUM(CASE WHEN is_opp_fbs AND is_bowl_eligible THEN wins ELSE 0 END) bowl_team_wins
        , SUM(CASE WHEN is_opp_fbs AND is_bowl_eligible THEN games_played ELSE 0 END) AS bowl_team_games
        , SUM(CASE WHEN is_win AND is_opp_fbs AND is_bowl_eligible THEN wins ELSE 0 END) AS sov_bowl_opp_wins
        , SUM(CASE WHEN is_win AND is_opp_fbs AND is_bowl_eligible THEN games_played ELSE 0 END) AS sov_bowl_opp_games
        , SUM(CASE WHEN is_opp_fbs AND NOT is_bowl_eligible THEN wins ELSE 0 END) non_bowl_wins
        , SUM(CASE WHEN is_opp_fbs AND NOT is_bowl_eligible THEN games_played ELSE 0 END) AS non_bowl_games
        , SUM(CASE WHEN is_win AND is_opp_fbs AND NOT is_bowl_eligible THEN wins ELSE 0 END) AS sov_non_bowl_opp_wins
        , SUM(CASE WHEN is_win AND is_opp_fbs AND NOT is_bowl_eligible THEN games_played ELSE 0 END) AS sov_non_bowl_opp_games
    FROM opp_records
    WHERE is_fbs
    GROUP BY team_id
)

, sos_sov_calcs AS (
    SELECT
        *
        , wins / NULLIF(games, 0)::NUMERIC AS win_pct
        , bowl_eligible_team_wins / NULLIF(wins, 0)::NUMERIC AS pct_of_wins_are_bowl_teams
        , bowl_eligible_teams / NULLIF(games, 0)::NUMERIC AS pct_of_games_are_bowl_teams
        , bowl_eligible_team_wins / NULLIF(bowl_eligible_teams, 0)::NUMERIC AS bowl_win_pct
        , opp_overall_wins / NULLIF(opp_overall_games, 0)::NUMERIC AS overall_sos
        , sov_overall_opp_wins / NULLIF(sov_overall_opp_games, 0)::NUMERIC AS overall_sov
        , ooc_wins / NULLIF(ooc_games, 0)::NUMERIC AS ooc_sos
        , sov_ooc_opp_wins / NULLIF(sov_ooc_opp_games, 0)::NUMERIC AS ooc_sov
        , conf_wins / NULLIF(conf_games, 0)::NUMERIC AS conf_sos
        , sov_conf_opp_wins / NULLIF(sov_conf_opp_games, 0)::NUMERIC AS conf_sov
        , bowl_team_wins / NULLIF(bowl_team_games, 0)::NUMERIC AS bowl_sos
        , sov_bowl_opp_wins / NULLIF(sov_bowl_opp_games, 0)::NUMERIC AS bowl_sov
        , non_bowl_wins / NULLIF(non_bowl_games, 0)::NUMERIC AS non_bowl_sos
        , sov_non_bowl_opp_wins / NULLIF(sov_non_bowl_opp_games, 0)::NUMERIC AS non_bowl_sov
    FROM win_totals
)

SELECT
    team_id
    , wins
    , games
    , bowl_eligible_teams
    , bowl_eligible_team_wins
    , non_bowl_eligible_team_wins
    , all_overall_wins
    , all_overall_games
    , opp_overall_wins
    , opp_overall_games
    , sov_overall_opp_wins
    , sov_overall_opp_games
    , overall_sos
    , overall_sov
    , ooc_wins
    , ooc_games
    , sov_ooc_opp_wins
    , sov_ooc_opp_games
    , ooc_sos
    , ooc_sov
    , conf_wins
    , conf_games
    , sov_conf_opp_wins
    , sov_conf_opp_games
    , conf_sos
    , conf_sov
    , bowl_team_wins
    , bowl_team_games
    , sov_bowl_opp_wins
    , sov_bowl_opp_games
    , bowl_sos
    , bowl_sov
    , non_bowl_wins
    , non_bowl_games
    , sov_non_bowl_opp_wins
    , sov_non_bowl_opp_games
    , non_bowl_sos
    , non_bowl_sov
FROM sos_sov_calcs