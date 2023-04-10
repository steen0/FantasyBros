with player_benchmarks as (
	select players."player"
		,players."team"
		,players."pos_1b"
		,players."pos_2b"
		,players."pos_3b"
		,players."pos_ss"
		,players."pos_c"
		,players."pos_of"
		,players."pos_dh"
		,players."pos_p"
		,players."proj_fantasy_pts"
		,first_base_bench."proj_benchmark_pts" as pos_1b_bench_pts
		,second_base_bench."proj_benchmark_pts" as pos_2b_bench_pts
		,third_base_bench."proj_benchmark_pts" as pos_3b_bench_pts
		,short_stop_bench."proj_benchmark_pts" as pos_ss_bench_pts
		,catcher_bench."proj_benchmark_pts" as pos_c_bench_pts
		,outfield_bench."proj_benchmark_pts" as pos_of_bench_pts
		,designated_hitter_bench."proj_benchmark_pts" as pos_dh_bench_pts
		,pitcher_bench."proj_benchmark_pts" as pos_p_bench_pts
		,utility_bench."proj_benchmark_pts" as pos_utility_bench_pts
		,CASE
			WHEN players."pos_1b" = 1
			THEN players."proj_fantasy_pts" - first_base_bench."proj_benchmark_pts"
			ELSE null
			END AS value_over_1b
		,CASE
			WHEN players."pos_2b" = 1
			THEN players."proj_fantasy_pts" - second_base_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_2b
		,CASE
			WHEN players."pos_3b" = 1
			THEN players."proj_fantasy_pts" - third_base_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_3b
		,CASE
			WHEN players."pos_ss" = 1
			THEN players."proj_fantasy_pts" - short_stop_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_ss
		,CASE
			WHEN players."pos_c" = 1
			THEN players."proj_fantasy_pts" - catcher_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_c
		,CASE
			WHEN players."pos_of" = 1
			THEN players."proj_fantasy_pts" - outfield_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_of
		,CASE
			WHEN players."pos_dh" = 1
			THEN players."proj_fantasy_pts" - designated_hitter_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_dh
		,CASE
			WHEN players."pos_p" = 1
			THEN players."proj_fantasy_pts" - pitcher_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_p
		,CASE
			WHEN ((players."pos_1b" + players."pos_2b" + players."pos_3b" + players."pos_ss" + players."pos_c" + players."pos_of") > 0)
			THEN players."proj_fantasy_pts" - utility_bench."proj_benchmark_pts"
			ELSE null
			END as value_over_utility
	from "analysis"."dim_players" players
	left join "analysis"."dim_benchmarks" as first_base_bench on ((players."pos_1b"=1) and (first_base_bench."pos"='1B'))
	left join "analysis"."dim_benchmarks" as second_base_bench on ((players."pos_2b"=1) and (second_base_bench."pos"='2B'))
	left join "analysis"."dim_benchmarks" as third_base_bench on ((players."pos_3b"=1) and (third_base_bench."pos"='3B'))
	left join "analysis"."dim_benchmarks" as short_stop_bench on ((players."pos_ss"=1) and (short_stop_bench."pos"='SS'))
	left join "analysis"."dim_benchmarks" as catcher_bench on ((players."pos_c"=1) and (catcher_bench."pos"='C'))
	left join "analysis"."dim_benchmarks" as outfield_bench on ((players."pos_of"=1) and (outfield_bench."pos"='OF'))
	left join "analysis"."dim_benchmarks" as designated_hitter_bench on ((players."pos_dh"=1) and (designated_hitter_bench."pos"='DH'))
	left join "analysis"."dim_benchmarks" as pitcher_bench on ((players."pos_p"=1) and (pitcher_bench."pos"='P'))
	left join "analysis"."dim_benchmarks" as utility_bench on (((players."pos_1b" + players."pos_2b" + players."pos_3b" + players."pos_ss" + players."pos_c" + players."pos_of") > 0) and (f_bench."pos"='U'))
	),
	
projections_over_time as (
	SELECT history."player"
		,CASE WHEN EXISTS (SELECT history."projection_date" FROM "analysis"."dim_players_history" history WHERE history."projection_date"=(CURRENT_DATE-1))
			THEN SUM(CASE WHEN history."projection_date"=(CURRENT_DATE) THEN history."proj_fantasy_pts" ELSE 0.0 END)-SUM(CASE WHEN history."projection_date"=(CURRENT_DATE-1) THEN history."proj_fantasy_pts" ELSE 0.0 END) 
			ELSE 0.0
			END AS one_day_projection_delta
		,CASE WHEN EXISTS (SELECT history."projection_date" FROM "analysis"."dim_players_history" history WHERE history."projection_date"=(CURRENT_DATE-5))
			THEN SUM(CASE WHEN history."projection_date"=(CURRENT_DATE) THEN history."proj_fantasy_pts" ELSE 0.0 END)-SUM(CASE WHEN history."projection_date"=(CURRENT_DATE-5) THEN history."proj_fantasy_pts" ELSE 0.0 END) 
			ELSE 0.0
			END AS five_day_projection_delta
		,CASE WHEN EXISTS (SELECT history."projection_date" FROM "analysis"."dim_players_history" history WHERE history."projection_date"=(CURRENT_DATE-10))
			THEN SUM(CASE WHEN history."projection_date"=(CURRENT_DATE) THEN history."proj_fantasy_pts" ELSE 0.0 END)-SUM(CASE WHEN history."projection_date"=(CURRENT_DATE-10) THEN history."proj_fantasy_pts" ELSE 0.0 END) 
			ELSE 0.0
			END AS ten_day_projection_delta
	FROM "analysis"."dim_players_history" history
	GROUP BY history."player"
	),
	
valuations as (
	select player_benchmarks.*
		,projections_over_time.one_day_projection_delta
		,projections_over_time.five_day_projection_delta
		,projections_over_time.ten_day_projection_delta
	from player_benchmarks
	left join projections_over_time on (player_benchmarks."player"=projections_over_time."player")
	)
	
select *
from valuations