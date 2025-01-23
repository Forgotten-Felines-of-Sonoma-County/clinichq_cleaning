import json
from datetime import datetime
import os

# Status and Type mappings
CHECKOUT_STATUS = {
    1: "Pending",
    2: "In Progress",
    3: "Checked Out",
    4: "Canceled",
    5: "No Show"
}

APPOINTMENT_TYPE = {
    1: "Spay Or Neuter",
    2: "Recheck",
    3: "Wellness"
}

def calculate_current_age(record_date_str, original_years, original_months):
    """Calculate current age based on record date plus the original age"""
    record_date = datetime.strptime(record_date_str, "%Y-%m-%dT%H:%M:%S")
    today = datetime.now()
    
    # Calculate time difference
    years = today.year - record_date.year
    months = today.month - record_date.month
    
    if today.day < record_date.day:
        months -= 1
    
    if months < 0:
        years -= 1
        months += 12
    
    # Add original age
    total_months = (years * 12 + months) + (original_years * 12 + original_months)
    
    # Convert back to years and months
    final_years = total_months // 12
    final_months = total_months % 12
    
    return final_years, final_months

def process_clinic_data():
    # Read JSON file and parse string if needed
    with open('data/clinichq_raw_data.json', 'r') as file:
        all_data = json.load(file)
        # If the data is a string, parse it again
        if isinstance(all_data, str):
            all_data = json.loads(all_data)
        # If it's not a list, wrap it in a list
        if not isinstance(all_data, list):
            all_data = [all_data]
    
    total_records = len(all_data)
    print(f"Starting to process {total_records} records...")
    
    # Initialize containers for clean and dirty data
    formatted_data = []
    dirty_cats = []
    
    # Process each record
    for i, data in enumerate(all_data, 1):
        print(f"Processing record {i}/{total_records}")
        
        # Only process records with checkoutStatus = 3 (Checked Out)
        if data.get('checkoutStatus') != 3:
            continue
            
        record_date = data.get('date')
        microchip = data.get('microchipNumber')
        
        # Collect all validation issues
        issues = []
        
        # Check for record date and microchip
        if not record_date:
            issues.append("Missing record date")
        if not (microchip and microchip.strip()):
            issues.append("Missing microchip")
        else:
            # Only try to convert microchip if it exists
            try:
                microchip_number = int(microchip)
            except (ValueError, TypeError):
                issues.append(f"Invalid microchip format: {microchip}")

        # Check for required address fields
        required_address_fields = ['ownerAddressLine1', 'ownerCity', 'ownerState', 'ownerZip']
        missing_fields = [field for field in required_address_fields if not data.get(field)]
        if missing_fields:
            issues.append(f"Missing address fields: {', '.join(missing_fields)}")

        # Check for valid checkout status and appointment type
        checkout_status = data.get('checkoutStatus')
        if not checkout_status or checkout_status not in CHECKOUT_STATUS:
            issues.append(f"Invalid checkout status: {checkout_status}")

        appointment_type = data.get('appointmentType')
        if not appointment_type or appointment_type not in APPOINTMENT_TYPE:
            issues.append(f"Invalid appointment type: {appointment_type}")

        # If any issues exist, add to dirty_cats with combined reason and skip
        if issues:
            data["reason"] = " | ".join(issues)
            dirty_cats.append(data)
            continue

        # Calculate current age based on record date plus original age
        original_years = data.get('ageYears', 0) or 0  # Default to 0 if None or empty
        original_months = int(data.get('ageMonths', 0) or 0)  # Default to 0 if None or empty
        current_years, current_months = calculate_current_age(
            record_date, 
            original_years, 
            original_months
        )
        
        # Format address with commas
        full_address = ", ".join(filter(None, [
            data.get('ownerAddressLine1'),
            data.get('ownerAddressLine2'),
            data.get('ownerCity'),
            data.get('ownerState'),
            data.get('ownerZip')
        ]))

        # Convert spayedNeutered to boolean (assuming 1 is true, other values are false)
        spayed_neutered = data['spayedNeutered'] == 1
        
        # Clean cat name by removing microchip number
        cat_name = data['animalName'].replace(str(microchip), '').strip()
        
        # Create a single object containing all related data
        formatted_record = {
            "cat": {
                "microchip": int(microchip),
                "sex": data['sex'],
                "cat_name": cat_name,
                "age_years": current_years,
                "age_months": current_months,
                "breed": data['breed'],
                "primary_color": data['primaryColor'],
                "secondary_color": data['secondaryColor'],
                "spayed_neutered": spayed_neutered,
                # "owner_id": 1,  # This should be dynamically assigned
                "full_address": full_address,
                "last_updated": record_date
            },
            "appointment": {
                # "appointment_id": 1,  # This should be dynamically assigned
                "microchip": int(microchip),
                "appointment_type": APPOINTMENT_TYPE[data['appointmentType']],
                "checkout_status": CHECKOUT_STATUS[data['checkoutStatus']],
                "date": record_date
            },
            "owner": {
                # "owner_id": 1,  # This should be dynamically assigned
                "owner_first_name": data['ownerFirstName'],
                "owner_last_name": data['ownerLastName'],
                "owner_cell_phone": data['ownerCellPhone'],
                "owner_home_phone": data['ownerHomePhone'],
                "owner_address": full_address,
                "last_updated": record_date
            }
        }
        
        formatted_data.append(formatted_record)

    return formatted_data, dirty_cats

if __name__ == "__main__":
    clean_data, dirty_data = process_clinic_data()
    
    # Export clean data if any exists
    if clean_data:
        output_file = 'data/processed_cat_data.json'
        with open(output_file, 'w') as f:
            json.dump({"records": clean_data}, f, indent=2)
        print(f"Clean data has been exported to {output_file}")
    
    # Export dirty data if any exists
    if dirty_data:
        output_file = 'data/dirty_cats.json'
        with open(output_file, 'w') as f:
            json.dump({"dirty_cats": dirty_data}, f, indent=2)
        print(f"Dirty cats data has been exported to {output_file}") 