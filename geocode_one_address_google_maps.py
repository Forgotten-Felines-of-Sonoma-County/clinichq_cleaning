import os
from dotenv import load_dotenv
import requests
import json

# Load environment variables from .env
load_dotenv()
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Google Maps Geocoding API endpoint
GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def geocode_address(address):
    """
    Use Google Maps Geocoding API to convert an address string into geographic coordinates.
    """
    print(f"Geocoding address: {address}")
    params = {
        "address": address,
        "key": GOOGLE_MAPS_API_KEY,
        "language": "en",
        "region": "us",  # Bias results to the United States
        "components": "administrative_area:CA|country:US"  # Restrict to California, USA
    }

    try:
        response = requests.get(GOOGLE_GEOCODE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Pretty print the entire response
        print("\nRaw API Response:")
        print(json.dumps(data, indent=2))

        results = data.get("results", [])
        if not results:
            print(f"No match found for: {address}")
            return None

        result = results[0]
        location = result["geometry"]["location"]
        address_components = {comp["types"][0]: {
            "long_name": comp["long_name"],
            "short_name": comp["short_name"]
        } for comp in result["address_components"]}

        parsed_result = {
            "full_address": result.get("formatted_address"),
            "latitude": location["lat"],
            "longitude": location["lng"],
            "region": address_components.get("administrative_area_level_1", {}).get("long_name"),
            "postcode": address_components.get("postal_code", {}).get("long_name"),
            "city": address_components.get("locality", {}).get("long_name"),
            "street": address_components.get("route", {}).get("short_name"),
            "street_number": address_components.get("street_number", {}).get("long_name")
        }

        return parsed_result

    except Exception as e:
        print(f"Error while geocoding with Google Maps: {str(e)}")
        return None


if __name__ == "__main__":
    address = "5340 Skylane Boulevard, Santa Rosa, Santa Rosa, CA, 95403"
    result = geocode_address(address)

    if result:
        print("\nGeocoding Results:")
        for key, value in result.items():
            print(f"{key}: {value}")
    else:
        print("Failed to geocode address")
