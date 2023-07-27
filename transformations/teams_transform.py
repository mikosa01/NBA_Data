from pyspark.sql import functions as f 

def drop_duplicates(data):
    return data.dropDuplicates()

def drop_col(data, colName1,colName2,colName3):
    return data.drop(*[colName1, colName2, colName3])

def run(spark, dataset_input_path, dataset_output_path):
    input_dataframe = spark.read.json(dataset_input_path)
    clean_df = drop_duplicates(input_dataframe)
    output_dataframe = drop_col(clean_df, 'city', 'conference', 'division')

    output_dataframe.write.csv(dataset_output_path, header = True)