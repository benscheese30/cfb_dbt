SELECT
	sfgm.season_year
	, sgd.season_week
    , sfgm.game_id 
	, sfgm.game_abbr_name
	, sfgm.team_id
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.home_team
		ELSE sgd.away_team
	END AS team_name
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.home_conference 
		ELSE sgd.away_conference 
	END AS team_conference
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.home_classification 
		ELSE sgd.away_classification 
	END AS team_classification 
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.away_team_id 
		ELSE sgd.home_team_id 
	END AS opp_team_id
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.away_team
		ELSE sgd.home_team
	END AS opp_team
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.away_conference 
		ELSE sgd.home_conference 
	END AS opp_conference
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.away_classification 
		ELSE sgd.home_classification 
	END AS opp_classification 
	, sfgm.is_home_game
	, sgd.is_regular_season 
	, sgd.is_neutral_site 
	, sgd.is_conference_game 
	, JSONB_ARRAY_LENGTH(sgd.home_line_scores) > 4 AS is_overtime
	, CASE
		WHEN sfgm.is_home_game
		THEN sgd.home_points > sgd.away_points
		ELSE sgd.away_points > sgd.home_points
	END AS is_win
	, sgd.kick_off_ts
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.home_points
		ELSE sgd.away_points 
	END AS team_points
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.home_line_scores 
		ELSE sgd.away_line_scores 
	END AS team_line_scores
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.away_points
		ELSE sgd.home_points 
	END AS opp_points
	, CASE
		WHEN sfgm.is_home_game 
		THEN sgd.away_line_scores 
		ELSE sgd.home_line_scores 
	END AS opp_line_scores
	, CASE
		WHEN sfgm.is_home_game
		THEN sgd.home_pregame_elo 
		ELSE sgd.away_pregame_elo 
	END AS team_pregame_elo
	, CASE
		WHEN sfgm.is_home_game
		THEN sgd.home_postgame_elo 
		ELSE sgd.away_postgame_elo 
	END AS team_postgame_elo
	, sgd.excitement_index
	, sfgm.win_probability 
	, sfgm.game_quality 
	, sfgm.matchup_quality 
	, sfgm.opp_season_strength_rating 
	, sfgm.opp_season_strength_fbs_rank 
	, sfgm.team_adj_avg_wp 
	, sfgm.team_adj_game_score 
	, sfgm.team_adj_win_pct 
	, sfgm.team_avg_wp 
	, sfgm.team_def_eff 
	, sfgm.team_off_eff 
	, sfgm.team_st_eff 
	, sfgm.team_tot_eff 
	, sfgm.team_pred_pt_diff 
	, sfgm.team_raw_game_score 
FROM {{ ref('stg_fpi_game_metrics') }} sfgm  
JOIN {{ ref('stg_game_details') }} sgd ON sfgm.game_id = sgd.game_id AND sfgm.season_week = sgd.season_week + 1