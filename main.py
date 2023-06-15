import os
from pyspark.sql import DataFrame

import feature_ipl
from spark_session import create_spark_session
import feature_ipl as feat_ipl


if __name__ == '__main__':
    root_dataset = "C:\\Users\\raziuddin.khazi\\Documents\\Projects\\My_projects\\datasets\\"
    ipl_dataset = os.path.join(root_dataset, "ipl_data_2017\\")

    spark = create_spark_session()

    df_match = spark.read.format("csv").option("header", True).load(ipl_dataset + "Match.csv")
    df_match = df_match.withColumnRenamed("match_id", "cricket_match_id")

    df_player_match = spark.read.format("csv").option("header", True).load(ipl_dataset + "Player_match.csv")
    df_player_match = df_player_match.withColumnRenamed("Match_Id", "match_id_2")
    df_player_match = df_player_match.withColumnRenamed("Country_Name", "Player_Country")

    #df_players_info = spark.read.format("csv").option("header", True).load(ipl_dataset + "Player.csv")
    #df_players_info = df_players_info.withColumnRenamed("Country_Name", "Player_Country_Name")

    df_playing_11 = feat_ipl.match_playing_11(df_match, df_player_match)
    df_matches_date_venue = feat_ipl.match_id_date_stadium(df_match, "Feroz Shah Kotla")

    feat_ipl.highest_player_of_match(df_match, df_player_match)





