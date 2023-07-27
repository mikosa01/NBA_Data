import sys
from transformations import teams_transform
from pyspark.sql import SparkSession

if __name__ == '__main__':
    arguement = sys.argv

    dataset_input_path = arguement[1]
    dataset_output_path = arguement[2]

    spark = SparkSession.builder.appName('NBA Team Data')\
                        .getOrCreate()
    input_dataframe = spark.read.format('json').load(dataset_input_path)

    teams_transform.run(spark, dataset_input_path, dataset_output_path)




    