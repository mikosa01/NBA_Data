import sys 
from pyspark.sql import SparkSession

from transformations import games_transform


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: save_clean_game.py <input_path> <output_path>")
        sys.exit(1)

    dataset_input_path = sys.argv[1]
    dataset_output_path = sys.argv[2]

    spark = SparkSession.builder.appName("NBA Game Data").getOrCreate()

    input_dataframe = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataset_input_path)

    games_transform.run(spark, dataset_input_path, dataset_output_path)