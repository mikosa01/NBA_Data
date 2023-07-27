import sys 
from model import ml_model
import pandas  as pd


if __name__ == '__main__':
    arguement = sys.argv

    dataset_input_path = arguement[1]
    model_output_path = arguement[2]

    input_dataset = pd.read_csv(dataset_input_path)

    ml_model.run(pd, dataset_input_path, model_output_path)