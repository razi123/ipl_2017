from pyspark.sql import DataFrame
import datetime as dt
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def match_playing_11(df_match: DataFrame, df_player_match: DataFrame):
    """

    :param df_match:
    :param df_player_match:
    :return:
    Playing 11 of the 2 teams, based on match id
    """
    df_playing_11 = df_match.join(df_player_match, df_match.cricket_match_id == df_player_match.match_id_2, "inner")
    Window_func = Window.partitionBy("cricket_match_id").orderBy("Player_Id")
    df_playing_11.withColumn("row_number", f.row_number().over(Window_func)) \
        .where(f.col("cricket_match_id") == "335987")
    df_playing_11 = df_playing_11.select("cricket_match_id",
                "Player_team",
                "Player_Id",
                "Player_Name",
                )

    return df_playing_11


def match_id_date_stadium(df: DataFrame, venue: str):
    """

    :param df:
    :param venue:
    :return:
    match dates in sorted order with venue
    """
    df = df.select("Season_Year",
                   "cricket_match_id",
                   "match_date",
                   "Venue_Name")

    window_func = Window.partitionBy("Season_Year").orderBy("match_date")

    df.withColumn("row_number", f.row_number().over(window_func))\
        .filter(f.col("Venue_Name") == venue)\
        .count()

    return df


def highest_player_of_match(df_match: DataFrame, df_player_match:DataFrame):
    """

    :param df_match:
    :param df_player_match:
    :param year:
    :return:
    """
    df_player_of_match = df_match \
        .join(df_player_match, df_match.cricket_match_id == df_player_match.match_id_2, "left") \
        .where(f.col("ManOfMach") == f.col("Player_Name")) \
        .select(df_player_match["Season_Year"],
                "cricket_match_id",
                "ManOfMach",
                "Player_team",
                "Player_Country")

    window_season = Window.partitionBy([df_player_match["Season_Year"], "ManOfMach"]).orderBy("ManOfMach")
    df_player_of_match = df_player_of_match.withColumn("season_mom", f.row_number().over(window_season)).\
        orderBy(f.col("Season_Year")) \
        .select("Season_Year",
                "ManOfMach",
                "Player_team",
                "Player_Country",
                "Season_mom")\

    df_player_of_match.show()

    max_mom = f.max(f.col("Season_mom")).over(Window.partitionBy([df_player_match["Season_Year"], "ManOfMach"]))
    df_player_of_match.filter(f.col("Season_mom") == max_mom).show()

            # which player has highest man of the match
    df_player_of_match.groupby("ManOfMach").count().orderBy(f.col("count").desc()).show(10)
    df_player_of_match.groupby(["ManOfMach", "Player_team", "Player_Country"]).count().orderBy(f.col("count").desc()).show(10)
    #win_highest_mom = Window.partitionBy("ManOfMach")
    #df_player_of_match.withColumn("max_mom", f.count("ManOfMach").over(win_highest_mom)).show()





