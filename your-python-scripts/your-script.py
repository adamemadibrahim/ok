import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor

# Define paths for input and output folders
folder_path = './input_files'  # Assuming input files are in the input_files directory
output_folder = './output_files'  # The output files will be saved in the output_files directory

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Function to extract the business category from a given link
def get_business_category_from_link(link):
    try:
        # Make a GET request to the link
        response = requests.get(link, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses

        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the business category using the provided selector
        category_element = soup.select_one('#contact-details > div.contact-details > div.media-object.clearfix.inside-gap-medium.image-on-right > div > h2 > a')

        if category_element:
            return category_element.text.strip()
        else:
            return None
    except Exception as e:
        return f"Error: {e}"

# Process a single file
def process_file(file_name):
    print(f"Processing file: {file_name}")
    file_path = os.path.join(folder_path, file_name)

    # Load the CSV file
    data = pd.read_csv(file_path)

    # Check for 'Link' and 'Business Name' columns in the file
    if 'Link' not in data.columns or 'Business Name' not in data.columns:
        print(f"Skipping {file_name}: Required columns 'Link' or 'Business Name' are missing.")
        return

    # Use ThreadPoolExecutor to fetch data in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        data['Business Category'] = list(executor.map(get_business_category_from_link, data['Link']))

    # Insert the new column in the correct place
    column_order = list(data.columns)
    column_order.insert(column_order.index('Business Name') + 1, 'Business Category')
    data = data[column_order]

    # Save the updated DataFrame to a new CSV file in the output folder
    output_file = os.path.join(output_folder, file_name)
    data.to_csv(output_file, index=False)

    print(f"Finished processing {file_name}. Output saved to {output_file}\n")

# Process all CSV files in the input folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
for file_name in csv_files:
    process_file(file_name)

print("All files processed.")
