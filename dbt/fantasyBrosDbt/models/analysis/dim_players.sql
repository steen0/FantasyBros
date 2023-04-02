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

pitchers as (
	select "Player" as player
		,"Team" as team
		,"IP" as pitcher_ip
		,"K" as pitcher_k
		,"W" as pitcher_w
		,"SV" as pitcher_sv
		,"L" as pitcher_l
		,"ERA" as pitcher_era
		,"WHIP" as pitcher_whip
		,"ER" as pitcher_er
		,"H" as pitcher_h
		,"BB" as pitcher_bb
		,"HR" as pitcher_hr
		,"G" as pitcher_g
		,"GS" as pitcher_gs
		,"CG" as pitcher_cg
	from "staging"."pitchers"
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
	
hitters as (
	select *
	from first_base
	union
	select *
	from second_base
	union
	select *
	from third_base
	union
	select *
	from short_stop
	union
	select *
	from catcher
	union
	select *
	from outfield
	union
	select *
	from designated_hitter
	),
	
all_players as (
	select 
		CASE
			WHEN hitters."player" is null
			THEN pitchers."player"
			ELSE hitters."player"
			END AS "player"
		,CASE
			WHEN hitters."team" is null
			THEN pitchers."team"
			ELSE hitters."team"
			END AS "team"
		,COALESCE("hitter_ab", 0) as hitter_ab
		,COALESCE("hitter_r", 0) as hitter_r
		,COALESCE("hitter_hr", 0) as hitter_hr
		,COALESCE("hitter_rbi", 0) as hitter_rbi
		,COALESCE("hitter_sb", 0) as hitter_sb
		,COALESCE("hitter_avg", 0) as hitter_avg
		,COALESCE("hitter_obp", 0) as hitter_obp
		,COALESCE("hitter_h", 0) as hitter_h
		,COALESCE("hitter_2b", 0) as hitter_2b
		,COALESCE("hitter_3b", 0) as hitter_3b
		,COALESCE("hitter_bb", 0) as hitter_bb
		,COALESCE("hitter_so", 0) as hitter_so
		,COALESCE("hitter_slg", 0) as hitter_slg
		,COALESCE("hitter_ops", 0) as hitter_ops
		,COALESCE("pitcher_ip", 0) as pitcher_ip
		,COALESCE("pitcher_k", 0) as pitcher_k
		,COALESCE("pitcher_w", 0) as pitcher_w
		,COALESCE("pitcher_sv", 0) as pitcher_sv
		,COALESCE("pitcher_l", 0) as pitcher_l
		,COALESCE("pitcher_era", 0) as pitcher_era
		,COALESCE("pitcher_whip", 0) as pitcher_whip
		,COALESCE("pitcher_er", 0) as pitcher_er
		,COALESCE("pitcher_h", 0) as pitcher_h
		,COALESCE("pitcher_bb", 0) as pitcher_bb
		,COALESCE("pitcher_hr", 0) as pitcher_hr
		,COALESCE("pitcher_g", 0) as pitcher_g
		,COALESCE("pitcher_gs", 0) as pitcher_gs
		,COALESCE("pitcher_cg", 0) as pitcher_cg
	from hitters
	full outer join pitchers on ((hitters."player" = pitchers."player") and (hitters."team" = pitchers."team"))
	),

all_players_w_pos as (
	select all_players.*
		,CASE
			WHEN first_base."player" is null
			THEN 0
			ELSE 1
			END AS pos_1b
		,CASE
			WHEN second_base."player" is null
			THEN 0
			ELSE 1
			END AS pos_2b
		,CASE
			WHEN third_base."player" is null
			THEN 0
			ELSE 1
			END AS pos_3b
		,CASE
			WHEN short_stop."player" is null
			THEN 0
			ELSE 1
			END AS pos_ss
		,CASE
			WHEN catcher."player" is null
			THEN 0
			ELSE 1
			END AS pos_c
		,CASE
			WHEN outfield."player" is null
			THEN 0
			ELSE 1
			END AS pos_of
		,CASE
			WHEN designated_hitter."player" is null
			THEN 0
			ELSE 1
			END AS pos_dh
		,CASE
			WHEN pitchers."player" is null
			THEN 0
			ELSE 1
			END AS pos_p
	from all_players
	left join first_base on ((all_players."player" = first_base."player") and (all_players."team" = first_base."team"))
	left join second_base on ((all_players."player" = second_base."player") and (all_players."team" = second_base."team"))
	left join third_base on ((all_players."player" = third_base."player") and (all_players."team" = third_base."team"))
	left join short_stop on ((all_players."player" = short_stop."player") and (all_players."team" = short_stop."team"))
	left join catcher on ((all_players."player" = catcher."player") and (all_players."team" = catcher."team"))
	left join outfield on ((all_players."player" = outfield."player") and (all_players."team" = outfield."team"))
	left join designated_hitter on ((all_players."player" = designated_hitter."player") and (all_players."team" = designated_hitter."team"))
	left join pitchers on ((all_players."player" = pitchers."player") and (all_players."team" = pitchers."team"))
	),

all_players_fantasy_pts as (
	select all_players_w_pos.*
		,(("hitter_h" - "hitter_hr" - "hitter_2b" - "hitter_3b") + (2 * ("hitter_2b")) + (3 * ("hitter_3b")) + (4 * ("hitter_hr")) + "hitter_rbi" + "hitter_r" + "hitter_sb" + "hitter_bb" - "hitter_so" + (3 * "pitcher_ip") + (2 * "pitcher_w") - (2 * "pitcher_l") + (5 * "pitcher_sv") - (2 * "pitcher_er") - "pitcher_h" + "pitcher_k" - "pitcher_bb") as proj_fantasy_pts
	from all_players_w_pos
	)
	
select *
from all_players_fantasy_pts