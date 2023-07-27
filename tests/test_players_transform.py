from transformations import players_transform
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('NBA Game Data Unit Test').getOrCreate()

def test_teams_id(): 
    schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("team", StringType(), True),
    StructField("name", StringType(), True)])
    data = [(1, "{'id': 1, 'abbreviation': 'BOS', \
             'city': 'Boston', 'conference': 'East'}", 'John')]
    df = spark.createDataFrame(data, schema)
    result = players_transform.team_id(df, 'team')

    expected_record = df.drop('team')
    expected= expected_record.withColumn('team_id', f.lit(1))
    assert result.collect() == expected.collect()


def test_drop_duplicates():
    data = [(1, 'value1'), (1, "value1"), (3, 'value')]
    df = spark.createDataFrame(data, ["col1", "col2"])

    result = players_transform.drop_duplicates(df)

    expected = df.dropDuplicates()

    assert result.collect() == expected.collect()

    
def test_drop_column():
    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('height_feet', IntegerType(), True), 
        StructField('height_inches', IntegerType(), True), 
        StructField('weight_pounds', IntegerType(), True)
    ])
    data = [(1, None, None, None)]
    df = spark.createDataFrame(data, schema)
    result = players_transform.drop_column(df)

    expected_record = df.drop(*['height_feet', 'height_inches', 'weight_pounds'])
    assert result.collect() == expected_record.collect()


# def test_drop_nan_rows(): 
#     record = dict (id = 1, 
#                   time = "", 
#                   season = 2018)
#     df = spark.createDataFrame([Row(**record)])
#     result = players_transform.drop_nan_rows(df)
#     expected_record = Row(**record)
#     expected = spark.createDataFrame([expected_record])
#     expected.dropna()
#     assert result.collect() == expected.collect()