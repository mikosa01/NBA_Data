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
    """
    Fetches data from a paginated API endpoint and aggregates the results.

    This function makes sequential requests to a paginated API endpoint, retrieving data
    from each page and aggregating it into a single list. It handles HTTP errors and
    combines data from multiple pages into a single result.

    Args:
        url (str): The URL of the paginated API endpoint.
        header (dict): A dictionary containing the headers to be included in the request.

    Returns:
        list: A list containing all the aggregated data from the paginated API.
        
    Raises:
        Any exceptions raised during the HTTP requests are propagated.

    """
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
    """
    Writes JSON data to a file and converts it to a CSV file.

    This function takes a list of JSON data, writes it to a JSON file, reads it back,
    converts it to CSV format, and writes the CSV data to a separate CSV file.

    Args:
        all_data (list): A list of JSON data to be written to a file and converted to CSV.
        raw_path (str): The directory path where the JSON file will be saved.
        filename (str): The desired filename (without extension) for the JSON and CSV files.
        csv_path1 (str): The directory path where the CSV file will be saved.

    Returns:
        None

    Example:
        all_data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        raw_path = "/path/to/json/files"
        filename = "data"
        csv_path1 = "/path/to/csv/files"
        FileLocation(all_data, raw_path, filename, csv_path1)
        
        # This will create 'data.json' in 'raw_path' and 'data.csv' in 'csv_path1'.
    """
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
        header = data[0].keys()
        writer.writerow(header)
        for item in data:
            writer.writerow(item.values())

def run(filename): 
    """
    Fetches data from an API, processes it, and saves it as JSON and CSV files.

    This function orchestrates the process of fetching data from a paginated API endpoint,
    processing the data, and then saving it as both a JSON file and a CSV file.

    Args:
        filename (str): The desired filename (without extension) for the JSON and CSV files.

    Returns:
        None

    Example:
        input_url = "https://example-api.com/data"
        header = {"Authorization": "Bearer <token>"}
        json_path = "/path/to/json/files"
        filename = "data"
        run(filename)

        # This will fetch data from the API, save it as 'data.json' in 'json_path',
        # and convert it to 'data.csv' in 'csv_path1' (as defined in the FileLocation function).
    """
    raw_json = request(input_url, header)
    output_file = FileLocation(raw_json, json_path, filename)