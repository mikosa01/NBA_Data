import pickle 
import os
from model.preprocess import drop_cols, stand_scaler
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import RandomizedSearchCV


def tt_split(data):
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    return X, y

def model (x, y):
    params_dist = {
    'max_depth' : [4, 6, 8, 10, 12, 14, 16], 
    'max_leaf_nodes' : [1000, 2000, 3000], 
    'min_samples_leaf' : [20, 30, 40, 50], 
    'min_samples_split' : [30, 40, 50]
    }

    gdr = GradientBoostingRegressor()
    random_search = RandomizedSearchCV(gdr, params_dist, cv = 5)
    random_search.fit(x, y)
    return random_search

def run (pd, dataset_input_path, model_output_path):
    input_dataset = pd.read_csv(dataset_input_path)
    new_df = drop_cols(input_dataset)
    x, y = tt_split(new_df)
    x_scaler, y = stand_scaler(x, y)
    output_model = model(x_scaler, y)
    
    file_path = os.path.join(model_output_path, 'model.pkl')
    with open(file_path, 'wb') as file:
        pickle.dump(output_model, file)


     



