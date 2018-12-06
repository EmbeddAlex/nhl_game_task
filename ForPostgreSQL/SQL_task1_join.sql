--Нужно вывести 5 команд (shortName + shortName из team_info.csv)
--в которых номинальные защитники (primaryPosition = "D" из player_info.csv)
--забили (goals из game_skater_stats.csv) наибольший процент голов
--(метрика (голы, забитые защитниками) / (голы, забитые всей командой) максимальное).
--Команды должны быть отсортированы в порядке убывания метрики.


SELECT short_name, team_name,
	sum(CASE WHEN primary_position = 'D' THEN goals ELSE 0 END) / sum(goals) as METRIC
FROM team_info
JOIN game_skater_stats ON game_skater_stats.team_id = team_info.team_id
JOIN player_info ON player_info.player_id = game_skater_stats.player_id
GROUP by team_info.short_name, team_info.team_name
ORDER by METRIC DESC
LIMIT 5;