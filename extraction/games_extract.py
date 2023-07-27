import os
import json 
import requests
import csv 


header = {
	"X-RapidAPI-Key": "056503acdcmshde58299aae5c4afp185e60jsn94893d801730",
	"X-RapidAPI-Host": "free-nba.p.rapidapi.com" 
    }
json_path='/home/mikosa/NBA_Data/resources/raw_data/json/games'
csv_path1 ='/home/mikosa/NBA_Data/resources/raw_data/csv/'
input_url="https://free-nba.p.rapidapi.com/games"


def request (url, header): 
    all_data = []
    for i in range(0, 695):
        querystring = {"page":i, "per_page": 100}
        response = requests.get(url, headers=header, params=querystring)
        if response.status_code == 200:
            data = json.loads(response.content)
            all_data.extend(data['data'])
        else:
            print(f"Error retrieving data from page {i}: {response.content}")
    return all_data


def FileLocation(all_data, raw_path, filename):
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
    filepath = os.path.join(raw_path, f"{filename}.json")
    with open(filepath, 'w') as outfile:
        json.dump(all_data, outfile)
    
    with open (filepath, 'r') as f: 
        data = json.load(f)
    
    csv_path = os.path.join(csv_path1, f"{filename}.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)

    # Write the header row based on the keys of the first item in the JSON array
        header = data[0].keys()
        writer.writerow(header)

    # Write each row of data to the CSV file
        for item in data:
            writer.writerow(item.values())

def run(filename): 
    raw_json = request(input_url, header)
    output_file = FileLocation(raw_json, json_path, filename)