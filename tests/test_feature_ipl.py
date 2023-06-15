from config.spark_session import SparkSession
from spark.sql import DataFrame
from spark.sql.functions import f
from spark.sql.types import StructType, StructField, StringType, IntegerType

import feature_ipl as feature_ipl
from config import utils as spark_utils


def test_match_playing_11():
    """
    Test the match playing 11
    """
    schema_1 = StructType([
        StructField("Match_SK", IntegerType(), True),
        StructField("match_id", IntegerType(), True),
        StructField("Player_Name", StringType(), True)
    ])

    schema_2 = StructType([
        StructField("cricket_match_id", IntegerType(), True),
        StructField("Player_Id", IntegerType, True),
        StructField("Player_Name", StringType(), True),
        StructField("Player_Team", StringType(), True),
    ])


# Create a list of tuples containing the data
    data1 = [
        (1, 335987, "R Dravid"),
        (2, 335987, "W Jaffer"),
        (3, 335987, "V Kohli"),
        (4, 335997, "B Akhil"),
    ]

# Create a list of tuples containing the data
    data2 = [
        (335987, 6, "BB McCullum", "Kolkata Knight Riders"),
        (335987, 7, "RT Pointing", "Kolkata Knight Riders"),
        (335987, 8, "DJ Hussey", "Kolkata Knight Riders"),
        (335987, 9, "Mohammad Hafeez", "Kolkata Knight Riders")
    ]


    df_match = spark.createDataFrame(data1, schema_1)
    df_player_match = spark.createDataFrame(data2, schema_2)

    df_result = feature_ipl.match_playing_11(df_match=df_match, df_player_match=df_player_match)




if __name__ == '__test_feature_ipl__':
    spark_utils.create_spark_session()