





team_table_create = ("""create table if not exists teams(
abbreviation varchar,
full_name varchar,
id int primary key,
name varchar
)""")

team_table_insert = ("""insert into teams(
abbreviation,
full_name,
id,
name,
) 
values(%s, %s, %s, %s)""")





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
id
home_team_score
period
postseason
season
status
visitor_team_score
home_team_id
visitor_team_id
match_date
) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")




stats_create_table = ("""create table if not exists stats(
ast int, 
blk int, 
dreb int, 
fg3a int, 
fg3m int, 
fg_pct float, 
fga int, 
fgm int, 
fta int, 
ftm int, 
id int primary key
pf int, 
pts int, 
reb int, 
stl int, 
turnover int, 
game_id int foreign key references game(id),
player_id int foreign key references player(id)
team_id int foreign key references team(id)
min_played int
)""")


stats_insert_table = ("""insert into stats(
ast, blk, dreb, fg3a, fg3m,
fg_pct, fga, fgm, fta, ftm, id,
pf, pts, reb, stl, turnover,
game_id, player_id, team_id,
min_played
) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")


players_create_table = ("""create table if not exits players(
id int primary key
first_name varchar, 
last_name varchar, 
position varchar,
team_id int foreign key reference team(id)
)""")

players_insert_table = ("""insert into player(
id
first_name
last_name
position
team_id
) values (%s, %s, %s, %s, %s)""")