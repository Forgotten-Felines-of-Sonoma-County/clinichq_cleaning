import os
from dotenv import load_dotenv
import requests

# Load environment variables from .env
load_dotenv()
MAPBOX_ACCESS_TOKEN = os.getenv('MAPBOX_ACCESS_TOKEN')

# Mapbox /forward endpoint
MAPBOX_FORWARD_URL = "https://api.mapbox.com/search/geocode/v6/forward"


def geocode_address(address):
    """
    Use Mapbox's Search Box API to convert an address string into geographic coordinates.
    """
    print(f"Geocoding address: {address}")
    params = {
        "q": address,
        "access_token": MAPBOX_ACCESS_TOKEN,
        "limit": 1,
        "language": "en",
        "auto_complete": "true",
        "proximity": "-122.720306,38.444660"  # Center point in Sonoma County
    }

    try:
        response = requests.get(MAPBOX_FORWARD_URL, params=params)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        if not features:
            print(f"No match found for: {address}")
            return None, "No match found"

        feature = features[0]
        coords = feature["geometry"]["coordinates"]
        properties = feature["properties"]
        context = properties.get("context", {})

        result = {
            "full_address": properties.get("full_address"),
            "latitude": coords[1],
            "longitude": coords[0],
            "region": context.get("region", {}).get("region_code"),
            "postcode": context.get("postcode", {}).get("name"),
            "district": context.get("district", {}).get("name"),
            "city": context.get("place", {}).get("name"),
            "neighborhood": context.get("neighborhood", {}).get("name"),
            "street": context.get("street", {}).get("name")
        }

        return result, None

    except Exception as e:
        print(f"Error while geocoding with Mapbox: {str(e)}")
        return None, f"Error: {str(e)}"


if __name__ == "__main__":
    address = "3697 Ross Rd, Sebastopol, CA 95472"
    result, error = geocode_address(address)

    if error:
        print(f"Error: {error}")
    else:
        print("\nGeocoding Results:")
        for key, value in result.items():
            print(f"{key}: {value}")
