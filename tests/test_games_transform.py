from transformations import games_transform
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName('NBA Game Data Unit Test').getOrCreate()

def test_home_teams_id(): 
    schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("home_team", StringType(), True),
    StructField("saeson", IntegerType(), True)])
    data = [(1, "{'id': 1, 'abbreviation': 'BOS', \
             'city': 'Boston', 'conference': 'East'}", 2018)]
    df = spark.createDataFrame(data, schema)
    result = games_transform.home_team_id(df, 'home_team')

    expected_record = df.drop('home_team')
    expected= expected_record.withColumn('home_team_id', f.lit(1))
    assert result.collect() == expected.collect()

def test_visit_teams_id(): 
    schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("visitor_team", StringType(), True),
    StructField("saeson", IntegerType(), True)])
    data = [(1, "{'id': 1, 'abbreviation': 'BOS', \
             'city': 'Boston', 'conference': 'East'}", 2018)]
    df = spark.createDataFrame(data, schema)
    result = games_transform.visit_team_id(df, 'visitor_team')

    expected_record = df.drop('visitor_team')
    expected= expected_record.withColumn('visitor_team_id', f.lit(1))
    assert result.collect() == expected.collect()
    

def test_match_date():
    record = dict(id = 1, 
                  date = "2019-01-30 00:00:00", 
                  season = 2018)
    df = spark.createDataFrame([Row(**record)])
    result = games_transform.match_date(df, 'date')
    expected_record = {key: value for key, value in record.items() if key != 'date'}
    expected_record['match_date'] = "2019-01-30"
    expected_records = Row(**expected_record)
    expected = spark.createDataFrame([expected_records])
    assert result.collect() == expected.collect()

def test_drop_duplicates():
    data = [(1, 'value1'), (1, "value1"), (3, 'value')]
    df = spark.createDataFrame(data, ["col1", "col2"])

    result = games_transform.drop_duplicates(df)

    expected = df.dropDuplicates()

    assert result.collect() == expected.collect()

def test_drop_column():
    record = dict (id = 1, 
                  time = " ", 
                  season = 2018)
    df = spark.createDataFrame([Row(**record)])
    result = games_transform.drop_column(df, 'time')
    expected_record = Row(**{key: value for key, value in record.items() if key != 'time'})
    expected = spark.createDataFrame([expected_record])
    assert result.collect() == expected.collect()


# def test_drop_rows(): 
#     record = dict (id = 1, 
#                   time = "", 
#                   season = 2018)
#     df = spark.createDataFrame([Row(**record)])
#     result = games_transform.drop_row(df)
#     expected_record = Row(**record)
#     expected = spark.createDataFrame([expected_record])
#     expected.dropna()
#     assert result.collect() == expected.collect()