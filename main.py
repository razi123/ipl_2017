import os
from pyspark.sql import DataFrame
from spark_session import create_spark_session


if __name__ == '__main__':
    root_dataset = "C:\\Users\\raziuddin.khazi\\Documents\\Projects\\My_projects\\datasets\\"
    ipl_dataset = os.path.join(root_dataset, "ipl_data_2017\\")

    spark = create_spark_session()

    df_match = spark.read.format("csv").option("header", True).load(ipl_dataset + "Match.csv")
    df_match.show(3)



