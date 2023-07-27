import sys 
from pyspark.sql import SparkSession

from transformations import stats_transform


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: save_clean_stats.py <input_path> <output_path>")
        sys.exit(1)

    dataset_input_path = sys.argv[1]
    dataset_output_path = sys.argv[2]

    spark = SparkSession.builder.appName("NBA Game Data").getOrCreate()

    input_dataframe = spark.read.format("json").load(dataset_input_path)

    stats_transform.run(spark, dataset_input_path, dataset_output_path)