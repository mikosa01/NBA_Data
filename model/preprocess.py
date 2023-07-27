from sklearn.preprocessing import StandardScaler


def drop_cols (data):
    return data.drop(['eFG%', 'TS%', 'visitor_team_id', 'home_team_id', \
            'game_id', 'player_id', 'team_id', 'season', 'home_team_score',
            'visitor_team_score', 'fg_pct', 'id'], axis = 1)

def stand_scaler (data, y):
    stand = StandardScaler()
    data_scaler = stand.fit_transform(data)
    return data_scaler, y
    