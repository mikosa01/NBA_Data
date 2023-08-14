import os
import json 
import requests
import csv 


json_path='/home/mikosa/NBA_Data/resources/raw_data/json/stats'
csv_path1 ='/home/mikosa/NBA_Data/resources/raw_data/csv/'


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
    for i in range(0, 120):
        querystring = {"page":i, "per_page":100}
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

def run(filename, header, input_url): 
    raw_json = request(input_url, header)
    output_file = FileLocation(raw_json, json_path, filename)