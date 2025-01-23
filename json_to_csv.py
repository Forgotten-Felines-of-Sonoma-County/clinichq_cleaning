# File paths
INPUT_FILE = 'data/dirty_cats.json'
OUTPUT_CSV_FILE = 'data/dirty_cats.csv'

import pandas as pd
import json

def convert_to_csv():
    try:
        # Open and load the JSON file
        with open(INPUT_FILE, 'r') as file:
            data = json.load(file)  # Load the JSON content as a Python dictionary
        
        # Check if 'dirty_cats' exists in the data
        if 'dirty_cats' not in data or not isinstance(data['dirty_cats'], list):
            print("Error: 'dirty_cats' key not found or is not a list in the input file.")
            return

        # Extract the list of cat records
        records = data['dirty_cats']
        
        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(records)
        
        # Save the DataFrame to a CSV file
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        
        print(f"Conversion completed successfully! File saved as '{OUTPUT_CSV_FILE}'")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found!")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the input file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    convert_to_csv()
