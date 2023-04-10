with first_base_bench as (
	select '1B' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_1b" = 1
	),
	
second_base_bench as (
	select '2B' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_2b" = 1
	),
	
third_base_bench as (
	select '3B' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_3b" = 1
	),
	
short_stop_bench as (
	select 'SS' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_ss" = 1
	),
	
catcher_bench as (
	select 'C' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_c" = 1
	),
	
outfield_bench as (
	select 'OF' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_of" = 1
	),
	
designated_hitter_bench as (
	select 'DH' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_dh" = 1
	),
	
pitcher_bench as (
	select 'P' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_1b" = 1
	),

utility_bench as (
	select 'U' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "analysis"."dim_players" players
	where players."pos_1b" = 1
	or players."pos_2b" = 1
	or players."pos_3b" = 1
	or players."pos_ss" = 1
	or players."pos_c" = 1
	or players."pos_of" = 1
	or players."pos_dh" = 1
	or players."pos_p" = 1
	),
	
total_bench as (
	select *
	from first_base_bench
	where first_base_bench."rank_by_proj" = 11
	union
	select *
	from second_base_bench
	where second_base_bench."rank_by_proj" = 11
	union
	select *
	from third_base_bench
	where third_base_bench."rank_by_proj" = 11
	union
	select *
	from short_stop_bench
	where short_stop_bench."rank_by_proj" = 11
	union
	select *
	from catcher_bench
	where catcher_bench."rank_by_proj" = 11
	union
	select *
	from outfield_bench
	where outfield_bench."rank_by_proj" = 31
	union
	select *
	from pitcher_bench
	where pitcher_bench."rank_by_proj" = 51
	union
	select *
	from utility_bench
	where utility_bench."rank_by_proj" = 11
	)
	
select total_bench."pos"
	,total_bench."proj_benchmark_pts"
from total_bench