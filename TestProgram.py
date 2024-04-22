import csv
import itertools
from selenium import webdriver
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from google.cloud import storage

# Function to fetch borrow data for a batch of symbols
def fetch_batch_data(batch_num, symbols, result_list, not_found_set):
    print(f"Starting batch {batch_num}")
    driver = webdriver.Chrome()  # Create a new Chrome driver instance for each thread
    try:
        for symbol_id, symbol in enumerate(symbols, 1):
            # Construct the URL with the symbol
            url = f'https://www.shortablestocks.com/?{symbol}'

            # Open the webpage
            driver.get(url)

            # Wait for the data to load
            wait = WebDriverWait(driver, 10)
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='borrowdata']//div[@id='borrowstuff']//table")))
            except:
                not_found_set.add(symbol)  # Add symbol to not found set
                continue

            # Find the table element
            table = driver.find_element(By.XPATH, "//div[@id='borrowdata']//div[@id='borrowstuff']//table")

            # Get all rows in the table
            rows = table.find_elements(By.TAG_NAME, 'tr')

            # Read today's date from the first row
            today_date_str = rows[1].find_elements(By.TAG_NAME, 'td')[3].text.split()[0]  # Extract date part only
            today_date = datetime.strptime(today_date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0)  # Set time to 0:00:00

            # Find the previous date 41 rows below the first row
            previous_date_str = rows[42].find_elements(By.TAG_NAME, 'td')[3].text.split()[0]  # Extract date part only
            previous_date = datetime.strptime(previous_date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0)  # Set time to 0:00:00

            # Extract the data and compute the difference
            available_values = []
            for row in rows[1:]:  # Start from index 1 to skip the header row
                cols = row.find_elements(By.TAG_NAME, 'td')
                updated_time = datetime.strptime(cols[3].text, '%Y-%m-%d %H:%M:%S')  # Parse Updated time

                # Check if the time is between previous date 17:45 and today's date 8:05
                if previous_date + timedelta(hours=17, minutes=45) <= updated_time <= today_date + timedelta(hours=8, minutes=5):
                    available = int(cols[2].text.replace(',', ''))  # Extract Available value and remove commas
                    available_values.append((available, updated_time))
       
            # Sort the available values by updated time in descending order
            available_values.sort(key=lambda x: x[1], reverse=True)
     
            # Calculate the difference between the latest available yesterday and the earliest available today
            latest_yesterday_available = available_values[0][0] if available_values else 0
            earliest_today_available = available_values[-1][0] if available_values else 0
            difference = latest_yesterday_available - earliest_today_available

            # Append the batch number, symbol ID, symbol, and difference to the result list
            result_list.append((symbol, difference , available_values[0][1] , available_values[0][0], available_values[-1][1], available_values[-1][0]))
    except Exception as e:
        print(f"Error processing symbols in batch {batch_num}: {e}")
    finally:
        driver.quit()  # Quit the Chrome driver instance after processing the batch
        print(f"Batch {batch_num} completed")

# Initialize Google Cloud Storage client
client = storage.Client()

# Define your GCS bucket and file paths
bucket_name = 'ramanastock'
nasdaq_symbols_file_path = '/home/ramanasurivattipalli/NASDAQ_SYMBOL.csv'  # Change the path to a suitable location
output_file_name = '/home/ramanasurivattipalli/Test_output'
notfound_file_name = '/home/ramanasurivattipalli/Test_notfound'

# Get the bucket
bucket = client.get_bucket(bucket_name)

# Download NASDAQ_SYMBOL.csv from GCS
nasdaq_symbols_blob = bucket.blob('NASDAQ_SYMBOL.csv')
nasdaq_symbols_blob.download_to_filename(nasdaq_symbols_file_path)

# Open the CSV file and read symbols
with open(nasdaq_symbols_file_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    symbols = [row['Symbol'] for row in itertools.islice(reader, 5)]  # Read all symbols from the CSV file

# Create batches of symbols (adjust batch_size as needed)
batch_size = 50  # Increase batch size to 50 symbols
symbol_batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]

# Initialize lists and sets to store results
result_list = []
not_found_set = set()

# Process symbol batches in parallel
with ThreadPoolExecutor(max_workers=20) as executor:  # Reduce max_workers to 20
    futures = []
    for batch_num, batch in enumerate(symbol_batches, 1):
        future = executor.submit(fetch_batch_data, batch_num, batch, result_list, not_found_set)
        futures.append(future)

    # Wait for all threads to complete
    for future in as_completed(futures):
        future.result()

# Print the unsorted list of symbols with differences
print("Unsorted list of symbols with differences:", result_list)

# Print symbols that were not found
print("Symbols not found:", not_found_set)

# Append today's date to output file name
today_date = datetime.now().strftime("%Y-%m-%d")
output_file_name_with_date = f'{output_file_name}_{today_date}.csv'
notfound_file_name_with_date = f'{notfound_file_name}_{today_date}.csv'

# Write the output set to a CSV file with formatted columns
with open(output_file_name_with_date, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([ "Symbol", "Difference", "latest_yesterday","latest_yesterday_available","earliest_today","earliest_today_available"])
    for row in result_list:
        writer.writerow(row)

print(f"Output written to {output_file_name_with_date}")

# Write the notfound set to a CSV file
with open(notfound_file_name_with_date, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Symbol"])
    for symbol in not_found_set:
        writer.writerow([symbol])

print(f"Notfound symbols written to {notfound_file_name_with_date}")

# Upload output files to GCS
output_blob = bucket.blob(output_file_name_with_date)
output_blob.upload_from_filename(output_file_name_with_date)

notfound_blob = bucket.blob(notfound_file_name_with_date)
notfound_blob.upload_from_filename(notfound_file_name_with_date)

print("Output files uploaded to Google Cloud Storage.")
