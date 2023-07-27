from load import load

path = '/home/mikosa/NBA_Data/resources/clean_data/games/clean_games.csv'

games_table_create = ("""create table if not exist games(
id int primary key, 
home_team_score int, 
period int, 
postseason varchar, 
season int, 
status varchar, 
visitor_team_score int, 
home_team_id int, 
visitor_team_id int, 
match_date date
)""")

games_insert_data = ("""insert into games(
id,
home_team_score,
period,
postseason,
season,
status,
visitor_team_score,
home_team_id,
visitor_team_id,
match_date
) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

load.run(games_table_create, games_insert_data, path)