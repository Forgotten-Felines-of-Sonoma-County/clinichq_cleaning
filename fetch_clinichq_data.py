import requests
import json
import dotenv
import os

dotenv.load_dotenv()

# Define the URL and headers
url = "https://api.clinichq.com/appointments"
headers = {
    "accept": "application/json"
}

# Define the query parameters
params = {
    "code": os.getenv("CLINICHQ_API_KEY"),
    "dateFrom": "2013-01-01T08:00:00.000Z",
    "dateTo": "2025-01-20T08:00:00.000Z"
}

# Send the GET request
response = requests.get(url, headers=headers, params=params)

# Create directory if it doesn't exist
data_dir = os.path.join(os.getcwd(), 'data')
os.makedirs(data_dir, exist_ok=True)

# Save the JSON response to a file
if response.status_code == 200:
    output_file = os.path.join(data_dir, 'clinichq_raw_data.json')
    with open(output_file, 'w') as json_file:
        json.dump(response.json(), json_file, indent=4)
    print(f"JSON file saved as '{output_file}'")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}, Error: {response.text}")
