import os
import json
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get environment variables
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

if not url or not key:
    raise ValueError("Missing required environment variables SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(url, key)

# Load the processed data
def load_processed_data():
    with open('data/processed_cat_data.json', 'r') as file:
        data = json.load(file)
        return data.get('records', [])
    
def upsert_owner(owner_data):
    """Upsert owner and return the owner_id"""
    # First, try to find existing owner by either phone number
    cell_phone = owner_data['owner_cell_phone']
    home_phone = owner_data['owner_home_phone']
    
    existing_owner = None
    if cell_phone:
        existing_owner = supabase.table('owners')\
            .select('owner_id')\
            .eq('cell_phone', cell_phone)\
            .execute()\
            .data
    
    if not existing_owner and home_phone:
        existing_owner = supabase.table('owners')\
            .select('owner_id')\
            .eq('home_phone', home_phone)\
            .execute()\
            .data
    
    owner_record = {
        'first_name': owner_data['owner_first_name'],
        'last_name': owner_data['owner_last_name'],
        'cell_phone': cell_phone,
        'home_phone': home_phone,
        'full_address': owner_data['owner_address'],
        'last_updated': owner_data['last_updated']
    }
    
    if existing_owner:
        # If owner exists, use their ID for the upsert
        owner_id = existing_owner[0]['owner_id']
        owner_record['owner_id'] = owner_id
        
    result = supabase.table('owners').upsert(
        owner_record,
        on_conflict='owner_id'  # Use the primary key for conflict resolution
    ).execute()
    
    return result.data[0]['owner_id']

def upsert_cat(cat_data, owner_id):
    """Upsert cat data with owner_id"""
    cat_record = {
        'microchip': cat_data['microchip'],
        'sex': cat_data['sex'],
        'cat_name': cat_data['cat_name'],
        'age_years': cat_data['age_years'],
        'age_months': cat_data['age_months'],
        'breed': cat_data['breed'],
        'primary_color': cat_data['primary_color'],
        'secondary_color': cat_data['secondary_color'],
        'spayed_neutered': cat_data['spayed_neutered'],
        'full_address': cat_data['full_address'],
        'last_updated': cat_data['last_updated'],
        'owner_id': owner_id
    }
    
    result = supabase.table('cats').upsert(
        cat_record,
        on_conflict='microchip'
    ).execute()

def upsert_appointment(appointment_data):
    """Upsert appointment data"""
    appointment_record = {
        'microchip': appointment_data['microchip'],
        'appointment_type': appointment_data['appointment_type'],
        'checkout_status': appointment_data['checkout_status'],
        'date': appointment_data['date']
    }
    
    result = supabase.table('appointments').upsert(
        appointment_record
    ).execute()

def process_record(record):
    """Process a single record by upserting owner, cat, and appointment data"""
    try:
        # First upsert owner and get owner_id
        owner_id = upsert_owner(record['owner'])
        
        # Add owner_id to cat data and upsert
        upsert_cat(record['cat'], owner_id)
        
        # Finally upsert appointment
        upsert_appointment(record['appointment'])
        
        return True
    except Exception as e:
        print(f"Error processing record: {e}")
        return False

# Main execution
if __name__ == "__main__":
    processed_data = load_processed_data()
    total_records = len(processed_data)
    print(f"Loaded {total_records} records from processed_cat_data.json")
    
    successful = 0
    for i, record in enumerate(processed_data, 1):
        print(f"Processing record {i}/{total_records}")
        if process_record(record):
            successful += 1
    
    print(f"Successfully processed {successful} out of {total_records} records")

