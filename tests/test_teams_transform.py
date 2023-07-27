from transformations import teams_transform as tt
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('NBA Team Data Unit Test').getOrCreate()

def test_drop_duplicates():
    data = [(1, 'value1'), (1, "value1"), (3, 'value')]
    df = spark.createDataFrame(data, ["col1", "col2"])

    result = tt.drop_duplicates(df)

    expected = df.dropDuplicates()

    assert result.collect() == expected.collect()

def test_drop_col():
    data = [(1, 'value1', 'name1', 'version1', 'position'), (2, 'value2', 'name2', 'version2', 'position2')]
    df = spark.createDataFrame(data, ["col1", "col2", 'city', 'conference', 'division'])

    result = tt.drop_col(df, 'city', 'conference', 'division')

    expected = df.drop(*['city', 'conference', 'division'])

    assert result.collect() == expected.collect()



