with first_base as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."firstBase"
	),

second_base as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."secondBase"
	),

third_base as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."thirdBase"
	),

short_stop as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."shortStop"
	),

catcher as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."catcher"
	),

outfield as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."outfield"
	),

start_pitcher as (
	select "Player" as player
		,"Team" as team
		,"IP" as pitcher_ip
		,"K" as pitcher_k
		,"W" as pitcher_w
		,"L" as pitcher_l
		,"ERA" as pitcher_era
		,"WHIP" as pitcher_whip
		,"ER" as pitcher_er
		,"H" as pitcher_h
		,"BB" as pitcher_bb
		,"HR" as pitcher_hr
		,"G" as pitcher_g
		,"GS" as pitcher_gs
		,"QS" as pitcher_qs
		,"CG" as pitcher_cg
	from "staging"."startPitcher"
	),

relief_pitcher as (
	select "Player" as player
		,"Team" as team
		,"IP" as pitcher_ip
		,"K" as pitcher_k
		,"SV" as pitcher_sv
		,"BS" as pitcher_sv
		,"HD" as pitcher_sv
		,"ERA" as pitcher_era
		,"WHIP" as pitcher_whip
		,"ER" as pitcher_er
		,"H" as pitcher_h
		,"BB" as pitcher_bb
		,"HR" as pitcher_hr
		,"G" as pitcher_g
		,"W" as pitcher_w
		,"L" as pitcher_l
	from "staging"."reliefPitcher"
	),

designated_hitter as (
	select "Player" as player
		,"Team" as team
		,"AB" as hitter_ab
		,"R" as hitter_r
		,"HR" as hitter_hr
		,"RBI" as hitter_rbi
		,"SB" as hitter_sb
		,"AVG" as hitter_avg
		,"OBP" as hitter_obp
		,"H" as hitter_h
		,"2B" as hitter_2b
		,"3B" as hitter_3b
		,"BB" as hitter_bb
		,"SO" as hitter_so
		,"SLG" as hitter_slg
		,"OPS" as hitter_ops
	from "staging"."designatedHit"
	),
	

--need to change these below to unions for hitters/pitchers
select *
from player_names
left join first_base on player_names."player"=first_base."player"
left join second_base on player_names."player"=second_base."player"
left join third_base on player_names."player"=third_base."player"
left join short_stop on player_names."player"=short_stop."player"
left join catcher on player_names."player"=catcher."player"
left join outfield on player_names."player"=outfield."player"
left join start_pitcher on player_names."player"=start_pitcher."player"
left join relief_pitcher on player_names."player"=relief_pitcher."player"
left join designated_hitter on player_names."player"=designated_hitter."player"
where player_names."player" like '%Shohei%'







player_projections as (
	select ap."Player" as player
		,ap."Team" as team
		,ap."PTS" as proj_pts
		,ap."REB" as proj_reb
		,ap."AST" as proj_ast
		,ap."BLK" as proj_blk
		,ap."STL" as proj_stl
		,stat."FGA" as proj_fga
		,stat."FGA" * ap."FG%" as proj_fgm
		,ap."FG%" as "proj_fg%"
		,stat."FTA" as proj_fta
		,stat."FTA" * ap."FT%" as proj_ftm
		,ap."FT%" as "proj_ft%"
		,ap."3PM" as proj_3pm
		,ap."TO" as proj_tov
		,ap."GP" as proj_gp
		,ap."MIN" as proj_mins
	from "staging"."allPlayers" ap
	inner join "staging"."proBasketballRefStats" stat on ((split_part(stat."Player", ' ', 1)=split_part(ap."Player", ' ', 1)) and (split_part(stat."Player", ' ', 2)=split_part(ap."Player", ' ', 2)))
	),

all_players as (
	select player_projections.*
		,case
			when point_guards."Player" is null
			then 0
			else 1
			end as pos_pg
		,case
			when shooting_guards."Player" is null
			then 0
			else 1
			end as pos_sg
		,case
			when small_forwards."Player" is null
			then 0
			else 1
			end as pos_sf
		,case
			when power_forwards."Player" is null
			then 0
			else 1
			end as pos_pf
		,case
			when centers."Player" is null
			then 0
			else 1
			end as pos_c
		,case
			when guards."Player" is null
			then 0
			else 1
			end as pos_g
		,case
			when forwards."Player" is null
			then 0
			else 1
			end as pos_f
	from player_projections
	left join point_guards on (player_projections."player"=point_guards."Player")
	left join shooting_guards on (player_projections."player"=shooting_guards."Player")
	left join small_forwards on (player_projections."player"=small_forwards."Player")
	left join power_forwards on (player_projections."player"=power_forwards."Player")
	left join centers on (player_projections."player"=centers."Player")
	left join guards on (player_projections."player"=guards."Player")
	left join forwards on (player_projections."player"=forwards."Player")
	),

all_players_fantasy_pts as (
	select all_players.*
		,(all_players."proj_pts" + all_players."proj_3pm" + all_players."proj_reb" + (2 * all_players."proj_ast") + (4 * all_players."proj_stl") + (4 * all_players."proj_blk") - (2 * all_players."proj_tov") + (2 * all_players."proj_fgm") - all_players."proj_fga" + all_players."proj_ftm" - all_players."proj_fta") as proj_fantasy_pts
	from all_players
	)
	
select *
from all_players_fantasy_pts
