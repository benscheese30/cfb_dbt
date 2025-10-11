SELECT
	sfgm.season_year
	, sfgm.season_week
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
	, CASE WHEN sgd.is_start_time_tbd THEN NULL ELSE sgd.kick_off_ts END AS kick_off_ts
	, sfgm.win_probability
	, sfgm.matchup_quality 
	, sfgm.opp_season_strength_rating 
	, sfgm.opp_season_strength_fbs_rank
	, sfgm.team_pred_pt_diff 
	, sfgm.last_modified 
FROM {{ ref('stg_fpi_game_metrics') }} sfgm  
JOIN {{ ref('stg_game_details') }} sgd ON sfgm.game_id = sgd.game_id AND sfgm.season_week <= sgd.season_week
ORDER BY 
	sfgm.season_year 
	, sfgm.team_id 
	, sfgm.game_id
	, sfgm.season_week