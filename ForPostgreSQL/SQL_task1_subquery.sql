--Нужно вывести 5 команд (shortName + shortName из team_info.csv)
--в которых номинальные защитники (primaryPosition = "D" из player_info.csv) 
--забили (goals из game_skater_stats.csv) наибольший процент голов 
--(метрика (голы, забитые защитниками) / (голы, забитые всей командой) максимальное).
--Команды должны быть отсортированы в порядке убывания метрики.


SELECT short_name, team_name,
	(
	SELECT SUM(goals)
	FROM game_skater_stats
	WHERE team_id = team_info.team_id AND
		(
		SELECT primary_position
		FROM player_info
		WHERE player_id = game_skater_stats.player_id 
		) = 'D'
	)/(
	SELECT SUM(goals)
	FROM game_skater_stats
	WHERE team_id = team_info.team_id
	) AS METRIC 
FROM team_info 
ORDER BY METRIC DESC NULLS LAST
limit 5;
