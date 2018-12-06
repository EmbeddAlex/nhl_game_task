from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


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

    data_frame.printSchema()

    spark.stop()


if __name__ == "__main__":
    spark_app()


