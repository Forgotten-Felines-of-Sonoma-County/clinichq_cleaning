import json
import requests
import os
from dotenv import load_dotenv
# Load environment variables from .env
load_dotenv()
# Ensure this is set in your .env
MAPBOX_ACCESS_TOKEN = os.getenv('MAPBOX_ACCESS_TOKEN')
# Mapbox /forward endpoint
MAPBOX_FORWARD_URL = "https://api.mapbox.com/search/searchbox/v1/forward"


def build_address(address_fields):
    """
    Optionally combine multiple address fields (e.g. street, city, state, zip)
    into one string. If you already have a single address string, you can skip this.
    """
    return ' '.join(field.strip() for field in address_fields if field)


def geocode_address(address):
    """
    Use Mapbox's Search Box API with auto_complete=true to convert an
    address string into geographic coordinates and extract address components.
    """
    print(f"Using Mapbox with auto_complete=true for: {address}")
    params = {
        "q": address,
        "access_token": MAPBOX_ACCESS_TOKEN,
        "limit": 1,            # Return just the top match
        "language": "en",
        "auto_complete": "true"
    }

    try:
        response = requests.get(MAPBOX_FORWARD_URL, params=params)
        response.raise_for_status()
        data = response.json()
        # Export data to JSON file
        os.makedirs('data', exist_ok=True)
        with open('data/address_data.json', 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Data exported to data/address_data.json")

        features = data.get("features", [])
        if not features:
            print(f"No match found for: {address}")
            return None

        # Extract the first match's data
        feature = features[0]
        coords = feature["geometry"]["coordinates"]  # [longitude, latitude]
        properties = feature["properties"]
        context = properties.get("context", {})

        return {
            "address": address,
            "corrected_address": properties.get("full_address"),
            "latitude": coords[1],
            "longitude": coords[0],
            "region": context.get("region", {}).get("region_code"),
            "postcode": context.get("postcode", {}).get("name"),
            "district": context.get("district", {}).get("name"),
            "city": context.get("place", {}).get("name"),
            "neighborhood": context.get("neighborhood", {}).get("name"),
            "street": context.get("street", {}).get("name")
        }
    except Exception as e:
        print(f"Error while geocoding with Mapbox: {str(e)}")
        return None


# ------------------- TEST A SINGLE ADDRESS -------------------
if __name__ == "__main__":
    # slightly misspelled to test auto-complete
    test_address = "1320 Dturk Av, Saa Ros, CA, 95404"
    result = geocode_address(test_address)
    if result:
        print("\nGeocoded Address Details:")
        for key, value in result.items():
            print(f" - {key.title()}: {value}")
    else:
        print("Failed to geocode the address using Mapbox.")
