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

SELECT
	team_id
	, SUM(is_win::INTEGER) AS wins
	, COUNT(game_id) AS games
	, SUM(is_bowl_eligible::INTEGER) AS bowl_eligible_teams
	, SUM(CASE WHEN is_win AND is_bowl_eligible THEN 1 ELSE 0 END) AS bowl_eligible_team_wins
	, SUM(CASE WHEN is_win AND NOT is_bowl_eligible THEN 1 ELSE 0 END) AS non_bowl_eligible_team_wins
	, SUM(wins) AS all_overall_wins
	, SUM(games_played) AS all_overall_games
	, SUM(CASE WHEN is_opp_fbs THEN wins ELSE 0 END) AS overall_wins
	, SUM(CASE WHEN is_opp_fbs THEN games_played ELSE 0 END) AS overall_games
	, SUM(CASE WHEN is_opp_fbs AND NOT is_conference_game THEN wins ELSE 0 END) AS ooc_wins
	, SUM(CASE WHEN is_opp_fbs AND NOT is_conference_game THEN games_played ELSE 0 END) AS ooc_games
	, SUM(CASE WHEN is_opp_fbs AND is_conference_game THEN wins ELSE 0 END) AS conf_wins
	, SUM(CASE WHEN is_opp_fbs AND is_conference_game THEN games_played ELSE 0 END) AS conf_games
	, SUM(CASE WHEN is_opp_fbs AND is_bowl_eligible THEN wins ELSE 0 END) bowl_team_wins
	, SUM(CASE WHEN is_opp_fbs AND is_bowl_eligible THEN games_played ELSE 0 END) AS bowl_team_games
	, SUM(CASE WHEN is_opp_fbs AND NOT is_bowl_eligible THEN wins ELSE 0 END) non_bowl_wins
	, SUM(CASE WHEN is_opp_fbs AND is_bowl_eligible THEN games_played ELSE 0 END) AS non_bowl_games
FROM opp_records
WHERE is_fbs
GROUP BY team_id