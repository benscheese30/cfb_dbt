SELECT
	sfr.season_year 
	, sfr.season_week 
	, sfr.team_id
	, sfr.fpi_rank 
	, sfr.fpi_rating 
	, sfr.game_control_rank AS fpi_game_control_rank
	, sfr.sor AS fpi_sor
	, sfr.sos AS fpi_sos
	, sfr.remaining_sos AS fpi_remaining_sos
	, sfr.avg_win_probability_rank AS fpi_avg_win_probability_rank 
	, sfr.overall_efficiency_rating AS fpi_eff
	, sfr.offense_efficiency_rating AS fpi_offense_eff
	, sfr.defense_efficiency_rating AS fpi_defense_eff
	, sfr.special_teams_efficiency_rating AS fpi_st_eff
	, ssr.spp_rank 
	, ssr.spp_rating 
	, ssr.offense_rating AS spp_offense_eff
	, ssr.defense_rating AS spp_defense_eff
	, ssr.special_teams_rating spp_st_eff 
FROM {{ ref('stg_fpi_rankings') }} sfr
FULL JOIN {{ ref('stg_spp_rankings') }} ssr ON ssr.season_year = sfr.season_year AND ssr.season_week = sfr.season_week AND ssr.team_id = sfr.team_id