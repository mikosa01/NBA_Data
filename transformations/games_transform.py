from pyspark.sql.functions import get_json_object, to_date

def home_team_id(data, colName): 
        return data.withColumn('home_team_id', get_json_object(colName, '$.id')\
                               .cast('int'))\
                               .drop(colName)

def visit_team_id(data, colName):
        return data.withColumn('visitor_team_id', get_json_object(colName, '$.id')\
                              .cast('int'))\
                              .drop(colName)

def match_date(data, colName): 
        return data.withColumn('match_date', to_date(colName)\
                               .cast('string'))\
                   .drop(colName)

def drop_duplicates(data):
        return data.dropDuplicates()

def drop_column(data, col1):
        return data.drop(col1)


# def drop_row(data): 
#         return data.na.drop(how="any")

def run(spark, dataset_input_path, dataset_output_path): 
        input_dataframe = spark.read.csv(dataset_input_path, header = True, inferSchema = True)
        clean_df = home_team_id(input_dataframe,'home_team')
        clean_df = visit_team_id(clean_df, 'visitor_team')
        clean_df = match_date(clean_df,  'date')
        clean_df = drop_duplicates(clean_df)
        output_dataframe = drop_column(clean_df, 'time')
        # output_dataframe = drop_row(clean_df)

        output_dataframe.write.csv(dataset_output_path, header = True)
         
