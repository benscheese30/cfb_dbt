WITH dts AS (
	SELECT 
		tfpd.team_id 
		, t.wk
	FROM {{ ref('int_team_fpi_postgame_details') }} tfpd
	CROSS JOIN GENERATE_SERIES(1, 14) AS t(wk)
	WHERE team_classification = 'fbs'
	GROUP BY 
		tfpd.team_id
		, t.wk
)

, total_points AS (
	SELECT
		dts.wk
		, dts.team_id
		, tfpd.game_id 
		, tfpd.team_conference
		, tfpd.team_classification 
		, tfpd.team_points 
		, SUM(tfpd.team_points) OVER(PARTITION BY dts.team_id ORDER BY dts.wk, game_id) AS cumulative_team_points
		, tfpd.opp_points 
		, SUM(tfpd.opp_points) OVER(PARTITION BY dts.team_id ORDER BY dts.wk, game_id) AS cumulative_opp_points
		, COUNT(tfpd.game_id) OVER(PARTITION BY dts.team_id ORDER BY dts.wk, game_id) AS games_played
		, AVG(tfpd.team_points - tfpd.opp_points) OVER(PARTITION BY dts.team_id ORDER BY dts.wk, game_id) AS game_scoring_diff
	FROM dts
	LEFT JOIN {{ ref('int_team_fpi_postgame_details') }} tfpd ON dts.wk = tfpd.season_week AND tfpd.team_id  = dts.team_id
)

, scoring_efficiencies AS (
	SELECT
		*
		, cumulative_team_points - cumulative_opp_points AS season_scoring_diff
		, cumulative_team_points / games_played::DECIMAL AS scoring_off
		, cumulative_opp_points / games_played::DECIMAL AS scoring_def
	FROM total_points
)

, season_ranks AS (
	SELECT 
		*
		, RANK() OVER(PARTITION BY wk ORDER BY scoring_off DESC) AS overall_scoring_off_rnk
		, RANK() OVER(PARTITION BY wk ORDER BY scoring_def ASC) AS overall_scoring_def_rnk
		, RANK() OVER(PARTITION BY wk, team_conference ORDER BY scoring_off DESC) AS conf_scoring_off_rnk
		, RANK() OVER(PARTITION BY wk, team_conference ORDER BY scoring_def ASC) AS conf_scoring_def_rnk
	FROM scoring_efficiencies
)

SELECT *
FROM season_ranks


