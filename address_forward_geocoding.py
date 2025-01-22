import json
import requests
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GEOCODE_API_KEY = os.getenv('GEOCODE_API_KEY')
GEOCODING_URL = "https://geocode.maps.co/search"

# File paths
INPUT_FILE = "data/addresses.json"
OUTPUT_FILE = "data/address_geocode.json"
FAILED_GEOCODE_FILE = "data/failed_geocode.json"

def build_address(address_fields: list) -> str:
    """
    Combine address fields into a single string.
    """
    return ' '.join(field.strip() for field in address_fields if field)


def geocode_address(address: str) -> dict | None:
    """
    Convert address to coordinates using the geocoding API.
    """
    try:
        url = f"{GEOCODING_URL}?q={address}&api_key={GEOCODE_API_KEY}"
        
        response = requests.get(url)
        response.raise_for_status()
        results = response.json()
        
        if results and len(results) > 0:
            return {
                'address': address,
                'latitude': results[0]['lat'],
                'longitude': results[0]['lon']
            }
        return None
        
    except Exception as e:
        print(f"Error geocoding address '{address}': {str(e)}")
        return None


def process_address_list(output_file: str = OUTPUT_FILE) -> None:
    """
    Process a list of addresses from input JSON file and save geocoded results.
    Also saves failed geocoding attempts to a separate file.
    
    Args:
        output_file (str): Path to output JSON file for geocoded results
    """
    try:
        # Read addresses from input file
        with open(INPUT_FILE, 'r') as f:
            address_data = json.load(f)
        
        geocoded_results = {
            "total_addresses": 0,
            "addresses": []
        }
        
        failed_addresses = {
            "total_failed": 0,
            "addresses": []
        }
        
        # Process each address
        for address in address_data["addresses"]:
            result = geocode_address(address)
            if result:
                geocoded_results["addresses"].append({
                    "address": result["address"],
                    "latitude": result["latitude"],
                    "longitude": result["longitude"]
                })
                print(f"Successfully geocoded: {address}")
            else:
                failed_addresses["addresses"].append(address)
                print(f"Failed to geocode: {address}")
            
            # Add a small delay to avoid hitting API rate limits
            time.sleep(1)
        
        # Update total counts
        geocoded_results["total_addresses"] = len(geocoded_results["addresses"])
        failed_addresses["total_failed"] = len(failed_addresses["addresses"])
        
        # Save successful results
        with open(output_file, 'w') as f:
            json.dump(geocoded_results, f, indent=2)
            
        # Save failed addresses
        with open(FAILED_GEOCODE_FILE, 'w') as f:
            json.dump(failed_addresses, f, indent=2)
            
        print(f"\nProcessed {geocoded_results['total_addresses']} addresses")
        print(f"Failed to process {failed_addresses['total_failed']} addresses")
        
    except Exception as e:
        print(f"Error processing address list: {str(e)}")


def main():
    """Main function to process the address list."""
    process_address_list()


if __name__ == "__main__":
    main()