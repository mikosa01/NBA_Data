from pyspark.sql.functions import col, split, when, mean, sum

def game_id(data, colName):
    return data.withColumn('game_id', col(colName).getField('id'))

def home_team_id(data, colName):
    return data.withColumn("home_team_id", col(colName).getField("home_team_id"))

def visitor_team_id(data, colName):
    return data.withColumn('visitor_team_id', col(colName).getField("visitor_team_id"))

def home_team_score(data, colName):
    return data.withColumn("home_team_score", col(colName).getField("home_team_score"))

def visitor_team_score(data, colName):
    return data.withColumn("visitor_team_score", col(colName).getField("visitor_team_score"))

def season(data, colName):
    return data.withColumn('season', col(colName).getField('season'))\
               .drop(colName)

def player_id(data, colName):
    return data.withColumn('player_id', col(colName).getField('id'))\
                .drop(colName)

def team_id (data, colName):
    return data.withColumn('team_id', col(colName).getField('id'))\
                .drop(colName)

def offensive_rebound(data, col1, col2):
    return data.withColumn('oreb', col(col1) - col(col2))

def team_score(data, colName1, colName2):
    return data.withColumn("team_score", when(col('team_id') == col('home_team_id'), col(colName1))\
                           .otherwise(col(colName2)))

def minutes_played(data, colName):
  data = data.withColumn("minutes_seconds", split(col(colName), ":"))
  data = data.withColumn("minutes", data["minutes_seconds"].getItem(0).cast("float"))
  data = data.withColumn("seconds", data["minutes_seconds"].getItem(1).cast("float"))
  data = data.withColumn("min_played", when(data["seconds"].isNull(), data["minutes"])\
                         .otherwise(data["minutes"] + data["seconds"] / 60))
  data = data.drop("minutes_seconds", "minutes", "seconds", colName)
  return data

def drop_duplicates(data):
    return data.dropDuplicates()

def drop_columns(data, colName, colName1, colName2):
    return data.drop(colName, colName1, colName2)

def drop_null_rows(data): 
    return data.na.drop(how="any")

def efficient_field_goal(data):
    return data.withColumn('eFG%', \
                        ((col('fgm') + 0.5 * col('fg3m'))/ col('fga'))*100)

def true_shooting(data):
    return data.withColumn('TS%', \
                        ((0.5 * col('pts'))/
                         ( col('fga') + 0.44 * col('fta')))*100)
         
def offensive_rebound(data, col1, col2):
    return data.withColumn('oreb', col(col1) - col(col2))

def performance_efficiency_rating(data):
    return data.withColumn('PER', (1 / col('min_played')*(col('pts') + (3 * col('reb')) +\
                     (1.5 * col('ast')) + col('stl') + col('blk') - (col('fga') - \
                     col('fgm'))- (col('fta')-col('ftm')) - col('turnover'))))


def run(spark, dataset_input_path, dataset_output_path): 
   input_dataframe = spark.read.json(dataset_input_path)
   clean_df = game_id(input_dataframe,'game')
   clean_df = home_team_id(clean_df, 'game')
   clean_df = home_team_score(clean_df, 'game')
   clean_df = visitor_team_id(clean_df, 'game')
   clean_df = visitor_team_score(clean_df, 'game')
   clean_df = season(clean_df, 'game')
   clean_df = player_id(clean_df, 'player')
   clean_df = team_id(clean_df,  'team')
   clean_df = team_score(clean_df, "home_team_score", "visitor_team_score")
   clean_df = minutes_played(clean_df,  'min')
   clean_df = drop_duplicates(clean_df)
   clean_df = drop_columns(clean_df, 'oreb', 'fg3_pct', 'ft_pct')
   clean_df = efficient_field_goal(clean_df)
   clean_df = true_shooting(clean_df)
   clean_df = offensive_rebound(clean_df, 'reb', 'dreb')
   clean_df = performance_efficiency_rating(clean_df)  
   output_dataframe = drop_null_rows(clean_df)  

   
   output_dataframe.write.csv(dataset_output_path, header = True)