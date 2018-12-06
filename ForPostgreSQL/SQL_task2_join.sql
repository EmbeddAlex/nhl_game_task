--Задание со звездочкой
--Вывести по 5 команд в разрезе года


SELECT * FROM
(
	SELECT ROW_NUMBER() OVER (PARTITION BY years ORDER BY METRIC DESC) row_num, *
	FROM
	(
		SELECT short_name, team_name, EXTRACT(YEAR FROM date_time) AS years,
			sum(CASE WHEN primary_position = 'D' THEN goals ELSE 0 END) / sum(goals) as METRIC
		FROM team_info
		FULL OUTER JOIN game_skater_stats ON game_skater_stats.team_id = team_info.team_id
		LEFT OUTER JOIN player_info ON player_info.player_id = game_skater_stats.player_id
		LEFT OUTER JOIN game ON game.game_id = game_skater_stats.game_id
		GROUP by short_name, team_name, years
		ORDER by years DESC, METRIC DESC
	) table_1
)table_2
WHERE row_num <=5