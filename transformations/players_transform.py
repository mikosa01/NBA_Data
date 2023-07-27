from pyspark.sql.functions import get_json_object


def team_id(data, colName): 
        return data.withColumn('team_id', get_json_object(colName, '$.id')\
                               .cast('int'))\
                               .drop(colName)

def drop_duplicates(data):
    return data.dropDuplicates()

def drop_column(data):
        return data.drop(*['height_feet', 'height_inches', 'weight_pounds'])

#def drop_nan_rows(data):
#        return data.na.drop(how = 'any')


def run(spark, dataset_input_path, dataset_output_path): 
        input_dataframe = spark.read.csv(dataset_input_path, header = True, inferSchema = True)
        clean_df = team_id(input_dataframe,'team')
        clean_df = drop_duplicates(clean_df)
        clean_df = drop_column(clean_df)
        #clean_df = drop_nan_rows(clean_df)

        clean_df.write.csv(dataset_output_path, header = True)
