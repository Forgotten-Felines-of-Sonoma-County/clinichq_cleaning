import pandas as pd
import os
import time
from anthropic import Anthropic
from tqdm import tqdm

'''
#just full address and entry index .csv file
df = pd.read_csv("data/reverse_geocoding_cache_rows.csv")

#create a new column for cleaned addresses
df['cleaned_address'] = ""

# Keep only the index and full_address columns
df = df[['id', 'full_address']]

# Create a new column for cleaned addresses (EMPTY)
df['cleaned_full_address'] = ""

# Save the modified dataframe back to CSV
output_file = "data/preprocessed_reverse_geocode_addresses.csv"
df.to_csv(output_file, index=False)

print(f"Processed {len(df)} addresses and saved to {output_file}")
'''


def clean_addresses(input_file, output_file, api_key, start_idx, end_idx, batch_size=50):
    """
    Process a CSV file to remove apartment/suite information from addresses
    using the Anthropic Claude API.
    """
    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"The file {input_file} does not exist")
    
    # Read the CSV file
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    
    # Ensure end_idx doesn't exceed dataframe length
    end_idx = min(end_idx, len(df))
    if start_idx >= end_idx:
        print(f"Start index {start_idx} is greater than or equal to end index {end_idx}. Nothing to process.")
        return
    
    total_rows = end_idx - start_idx
    total_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"Processing rows {start_idx} to {end_idx} in {total_batches} batches of {batch_size}...")
    
    # Create output file with headers if it doesn't exist
    if not os.path.exists(output_file):
        headers = ['id', 'full_address', 'cleaned_full_address']
        empty_df = pd.DataFrame(columns=headers)
        empty_df.to_csv(output_file, index=False)
        print(f"Created new output file: {output_file}")
    
    # Process in batches
    for batch_num in range(total_batches):
        batch_start = start_idx + (batch_num * batch_size)
        batch_end = min(batch_start + batch_size, end_idx)
        
        print(f"\nProcessing batch {batch_num+1}/{total_batches} (rows {batch_start} to {batch_end})...")
        
        # Process batch of addresses
        batch_df = df.iloc[batch_start:batch_end].copy()
        batch_addresses = batch_df['full_address'].tolist()
        
        # Create the prompt with the batch addresses
        addresses_text = "\n".join([f"{i+1}. {addr}" for i, addr in enumerate(batch_addresses)])
        prompt = f"""
        I have an input csv file that I want to complete by filling in the cleaned_full_address column
        Also do not overwrite the full_address column
        DO NOT ADD THE COLUMN HEADER AGAIN
        DO NOT OUTPUT ANY TEXT RESPONSE OTHER THAN THE CLEANED ADDRESSES
        cleaned_full_address column is string

        Examples of what to remove:
        - Apt, Apartment, Unit designations
        - Suite, Ste designations
        - # followed by numbers
        - Any text in a format that appears to be a secondary address line
        
        here is an example of what the output addresses should look like:
        EXAMPLE: 
        INPUT into full_address column: "1814G Empire Industrial Ct, Santa Rosa, CA 95403, USA"
        OUTPUT into cleaned_full_address column: "1814 Empire Industrial Ct, Santa Rosa, CA 95403, USA"
        Here are the addresses to clean:
        {addresses_text}
        """
        
        # Initialize Anthropic client
        client = Anthropic(api_key=api_key)
        
        try:
            response = client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=2000,  # Increased max tokens
                temperature=0,
                system="You are a helpful assistant that cleans address data by removing apartment/suite information.",
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            # Process the response and create a new dataframe for the batch
            cleaned_addresses = response.content[0].text.strip().split('\n')
            
            # Validate response length matches batch size
            if len(cleaned_addresses) != len(batch_addresses):
                print(f"Warning: Expected {len(batch_addresses)} addresses but got {len(cleaned_addresses)}. Some addresses may be missing.")
            
            batch_results = []
            for i, cleaned_address in enumerate(cleaned_addresses):
                if i >= len(batch_df):
                    break
                    
                # Remove the numbering (e.g., "1. ") from the response
                if '. ' in cleaned_address:
                    cleaned_address = cleaned_address.split('. ', 1)[1]
                
                row_id = batch_df.iloc[i]['id']
                original_address = batch_df.iloc[i]['full_address']
                
                batch_results.append({
                    'id': row_id,
                    'full_address': original_address,
                    'cleaned_full_address': cleaned_address
                })
            
            # Append batch results to the output file
            batch_df = pd.DataFrame(batch_results)
            batch_df.to_csv(output_file, mode='a', header=False, index=False)
            print(f"Successfully appended {len(batch_results)} rows to {output_file}")
                
        except Exception as e:
            print(f"Error processing batch: {str(e)}")
        
        # Add a small delay between batches to avoid rate limiting
        if batch_num < total_batches - 1:
            print("Waiting 1 second before next batch...")
            time.sleep(1)
    
    print(f"\nAll {total_batches} batches processed successfully!")

if __name__ == "__main__":
    # Configuration
    input_file = "data/preprocessed_reverse_geocode_addresses.csv"
    output_file = "data/reverse_geocoding_cache_rows_cleaned_1.csv"
    
    # Get API key from environment variable or input
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        api_key = input("Please enter your Anthropic API key: ")
    
    # Process rows 0 to 1249 in batches of 50
    start_idx = 1249
    end_idx = 1250
    batch_size = 50
    clean_addresses(input_file, output_file, api_key, start_idx, end_idx, batch_size)
