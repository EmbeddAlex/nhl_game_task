from pyspark.sql import SQLContext, Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import doctest

"""
First task:
to display 5 teams(shortName + shortName from team_info.csv) in which
nominal defenders(primaryPosition="D" from player_info.csv) scored (targets from game_skater_stats.csv)
the largest percentage of goals(metric = (goals scored by defenders) / (goals scored by the whole team)
Teams must be sorted in descending order of metrics

Second additional task:
to display 5 teams per year
"""


def spark_app():

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    sql_context = SQLContext(spark)

    # loading *.CSV files as DataFrames
    team_info = sql_context.read.csv("src/team_info.csv", header=True, sep=",")
    game_skater_stats = sql_context.read.csv("src/game_skater_stats.csv", header=True, sep=",")
    player_info = sql_context.read.csv("src/player_info.csv", header=True, sep=",")

    # joining tables
    data_frame = team_info. \
        join(game_skater_stats, game_skater_stats.team_id == team_info.team_id). \
        join(player_info, game_skater_stats.player_id == player_info.player_id)

    # first task:
    data_frame.groupby('shortName', 'teamName'). \
        agg((f.sum(f.when(data_frame.primaryPosition == 'D', data_frame.goals).otherwise(0)) / f.sum('goals')).alias('METRIC')). \
        sort('METRIC', ascending=False).limit(5).show()

    # additional task
    game = sql_context.read.csv("src/game.csv", header=True, sep=",")
    data_frame = data_frame.join(game, game_skater_stats.game_id == game.game_id)

    data_frame_2 = data_frame. \
        groupby('shortName', 'teamName', f.year(data_frame.date_time).alias('years')). \
        agg((f.sum(f.when(data_frame.primaryPosition == 'D', data_frame.goals).otherwise(0)) / f.sum('goals')).alias('METRIC'))

    window = Window.partitionBy(f.col('years')).orderBy((f.col('METRIC')).desc())
    data_frame_2.select(f.col('shortName'), f.col('teamName'), f.col('years'), f.col('METRIC'), f.row_number().
                        over(window).alias('row_number')).where(f.col('row_number') <= 5).show()

    spark.stop()


if __name__ == "__main__":
    spark_app()
    # To run the doctest, uncomment the line below:
    doctest.testfile("test.txt")

