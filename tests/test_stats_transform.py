from transformations import stats_transform as st
from pyspark.sql import SparkSession, Row, functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('NBA Game Data Unit Test').getOrCreate()

def test_game_id():
        # Create test DataFrame
    data = [(1, {"id": 9}), (2, {"id": 6})]
    df = spark.createDataFrame(data, ["id", "data"])
    
    # Apply function
    result = st.game_id(df, "data")
    
    # Validate the result
    assert result.columns == ["id", 'data', "game_id"]
    assert result.collect() == [(1, {"id": 9}, 9), (2,{"id": 6}, 6)]


# Test home_team_id function
def test_home_team_id():
    # Create test DataFrame
    data = [(1, {"home_team_id": 9}), (2, {"home_team_id": 6})]
    df = spark.createDataFrame(data, ["id", "data"])
    
    # Apply function
    result = st.home_team_id(df, "data")
    
    # Validate the result
    assert result.columns == ["id", 'data', "home_team_id"]
    assert result.collect() == [(1, {"home_team_id": 9}, 9), (2,{"home_team_id": 6}, 6)]

# Test visitor_team_id function
def test_visitor_team_id():
    # Create test DataFrame
    data = [(1, {"visitor_team_id": 6}), (2, {"visitor_team_id": 9})]
    df = spark.createDataFrame(data, ["id", "data"])
    
    # Apply function
    result = st.visitor_team_id(df, "data")
    
    # Validate the result
    assert result.columns == ["id", 'data', "visitor_team_id"]
    assert result.collect() == [(1, {"visitor_team_id": 6}, 6),\
                                 (2, {"visitor_team_id": 9}, 9)]

# Test home_team_score function
def test_home_team_score():
    # Create test DataFrame
    data = [(1, {"home_team_score": 23}), (2, {"home_team_score": 56})]
    df = spark.createDataFrame(data, ["id", "data"])
    
    # Apply function
    result = st.home_team_score(df, "data")
    
    # Validate the result
    assert result.columns == ["id",'data', "home_team_score"]
    assert result.collect() == [(1, {"home_team_score": 23}, 23),\
                                 (2,{"home_team_score": 56}, 56)]

# Test visitor_team_score function
def test_visitor_team_score():
    # Create test DataFrame
    data = [(1, {"visitor_team_score": 50}), (2, {"visitor_team_score": 59})]
    df = spark.createDataFrame(data, ["id", "data"])
    
    # Apply function
    result = st.visitor_team_score(df, "data")
    
    # Validate the result
    assert result.columns == ["id",'data', "visitor_team_score"]
    assert result.collect() == [(1, {"visitor_team_score": 50}, 50), (2, {"visitor_team_score": 59}, 59)]

# Test visitor_team_score function
def season():
    # Create test DataFrame
    data = [(1, {"season": 2015}), (2, {"season": 2000})]
    df = spark.createDataFrame(data, ["id", "data"])
    
    # Apply function
    result = st.visitor_team_score(df, "data")
    
    # Validate the result
    assert result.columns == ["id", "season"]
    assert result.collect() == [(1, 2015), (2, 2000)]



# Test player_id function

# Test player_id function
def test_player_id():
    # Create test DataFrame
    data = [(1, {"id": 123}), (2, {"id": 456})]
    df = spark.createDataFrame(data, ["colName", "data"])
    
    # Apply function
    result = st.player_id(df, "data")
    
    # Validate the result
    assert result.columns == ["colName", "player_id"]
    assert result.collect() == [(1, 123), (2, 456)]

# Test team_abbrev function
def test_team_id():
    # Create test DataFrame
    data = [(1, {"id": 1}), (2, {"id": 2})]
    df = spark.createDataFrame(data, ["colName", "data"])
    
    # Apply function
    result = st.team_id(df, "data")
    
    # Validate the result
    assert result.columns == ["colName", "team_id"]
    assert result.collect() == [(1, 1), (2, 2)]

# Test team_score function
def test_team_score():
    #Create test DataFrame
    data = [(1, 3, 3, 5, 40, 30), (2, 4, 3, 4, 40, 60)]
    df = spark.createDataFrame(data, ['id', 'team_id', 'home_team_id', 'visitor_team_id', \
                                      'home_team_score', 'visitor_team_score'])

    # Apply function
    result =st.team_score(df, 'team_id')

    # Validate the result
    assert result.columns == ['id', 'team_id', 'home_team_id', 'visitor_team_id', \
                                      'home_team_score', 'visitor_team_score', 'team_score']
    assert result.collect() ==[(1, 3, 3, 5, 40, 30, 40), (2, 4, 3, 4, 40, 60, 60)]
    

# Test minutes_played function
def test_minutes_played():
    # Create test DataFrame
    data = [("10:30",), ("15:45",)]
    df = spark.createDataFrame(data, ["colName"])
    
    # Apply function
    result = st.minutes_played(df, "colName")
    
    # Validate the result
    assert result.columns == ["min_played"]
    assert result.collect() == [(10.5,), (15.75,)]

def test_drop_duplicates():
    data = [(1, 'value1'), (1, "value1"), (3, 'value')]
    df = spark.createDataFrame(data, ["col1", "col2"])

    result = st.drop_duplicates(df)

    expected = df.dropDuplicates()

    assert result.collect() == expected.collect()


# Test drop_null_rows function
def test_drop_null_rows():
    # Create test DataFrame with null values
    data = [(1, None), (2, "value"), (3, None)]
    df = spark.createDataFrame(data, ["col1", "col2"])
    
    # Apply function
    result = st.drop_null_rows(df)
    
    # Validate the result
    assert result.count() == 1
    assert result.columns == ["col1", "col2"]
    assert result.collect() == [(2, "value")]



def test_drop_columns():
    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('height_feet', IntegerType(), True), 
        StructField('height_inches', IntegerType(), True), 
        StructField('weight_pounds', IntegerType(), True)
    ])
    data = [(1, None, None, None)]
    df = spark.createDataFrame(data, schema)
    result = st.drop_columns(df, 'height_feet', 'height_inches', 'weight_pounds')

    expected_record = df.drop(*['height_feet', 'height_inches', 'weight_pounds'])
    assert result.collect() == expected_record.collect()
    
def test_efficient_field_goal():
    data = [(1, 6, 5, 8)]
    df = spark.createDataFrame(data, ['id', 'fgm', 'fg3m', 'fga'])

    result = st.efficient_field_goal(df)

    expected = df.withColumn('eFG%', \
                        ((col('fgm') + 0.5 * col('fg3m'))/ col('fga'))*100)


    assert result.columns == expected.columns
    assert result.collect() == expected.collect()

def test_true_shooting():
    data = [(1, 6, 5, 8)]
    df = spark.createDataFrame(data, ['id', 'pts', 'fta', 'fga'])

    result = st.true_shooting(df)

    expected = df.withColumn('TS%', \
                        ((0.5 * col('pts'))/( col('fga') + 0.44 * col('fta')))*100)

    assert result.columns == expected.columns
    assert result.collect() == expected.collect()

def test_performance_efficiency_rating():
    data = [(10, 20, 5, 8, 7, 6, 7, 2, 9, 3, 1)]
    df =spark.createDataFrame(data, ['min_played', 'pts', 'reb', 'ast', 'stl', 'blk', 'fta', 'ftm', 'fga', 'fgm', 'turnover'])

    result = st.performance_efficiency_rating(df)

    expected =df.withColumn('PER', (1 / col('min_played')*(col('pts') + (3 * col('reb')) + (1.5 * col('ast')) + col('stl') + col('blk') - (col('fga') - col('fgm'))- (col('fta')-col('ftm')) - col('turnover'))))

    assert result.columns == expected.columns
    assert result.collect() == expected.collect()