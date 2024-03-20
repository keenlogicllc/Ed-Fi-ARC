import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from requests.exceptions import RequestException
import csv
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load .env file from the script's directory
dotenv_path = os.path.join(script_dir, '.env')
load_dotenv(dotenv_path)

# Establishes Token permissions and BASE_URL from .env
base_url = os.getenv('BASE_URL')
access_token = None
token_expiration = 0

# Defines access token and API request
def get_access_token():
    global access_token, token_expiration
    access_url = os.getenv('ACCESS_URL')
    api_key = os.getenv('EDFI_API_KEY')
    api_secret = os.getenv('EDFI_API_SECRET')
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'grant_type': 'client_credentials'}
    response = requests.post(access_url, headers=headers, auth=(api_key, api_secret), data=payload)
    
    if response.status_code == 200:
        token_info = response.json()
        access_token = token_info['access_token']
        # Adjust as per the API's token expiration policy
        token_expiration = datetime.now().timestamp() + 3600
        print("Successfully retrieved access token.")
    else:
        raise Exception(f"Failed to retrieve access token: {response.status_code}, {response.text}")

# Authenticated GET request and Retry logic function
def make_authenticated_request(api_url, retries=5, backoff_factor=2):
    global access_token, token_expiration
    if not access_token or datetime.now().timestamp() >= token_expiration:
        get_access_token()
    headers = {'Authorization': f'Bearer {access_token}'}

    # Set up retry strategy
    retry_strategy = Retry(
        total=retries,
        status_forcelist=[500],  # Retrying only for 500 errors
        allowed_methods=["GET"],  # Retrying on GET
        backoff_factor=backoff_factor  # Exponential backoff
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    try:
        response = http.get(api_url, headers=headers)
        if response.status_code in [200, 201]:
            return response, None
        else:
            error_details = {
                'status_code': response.status_code,
                'url': response.url,
                'response_body': response.text,
            }
            return None, error_details
    except RequestException as e:
        print(f"Request failed: {e}")
        return None, None

def read_last_run_data(last_run_filename):
    """Reads the last run's data for comparison."""
    last_data = {}
    try:
        with open(last_run_filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                last_data[row['Endpoint']] = int(row['Record Count'])
    except FileNotFoundError:
        print("No previous data file found.")
    return last_data
    
def main():

    script_dir = os.path.dirname(os.path.abspath(__file__))
    last_run_file = os.path.join(script_dir, 'last_run.txt')
    current_run_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # Try to read the last run date
    try:
        with open(last_run_file, 'r') as file:
            last_run_date = file.read().strip()
    except FileNotFoundError:
        last_run_date = 'Never'

    export_base_path = os.path.join(script_dir, 'exports')
    os.makedirs(export_base_path, exist_ok=True)
    archive_filename = f"ARCExport_{current_run_date}.csv"
    archive_full_path = os.path.join(export_base_path, archive_filename)

    # New logic to handle visualization file
    visualization_base_path = os.path.join(script_dir, 'visualization')
    os.makedirs(visualization_base_path, exist_ok=True)
    visualization_filename = "VisualizationData.csv"
    visualization_full_path = os.path.join(visualization_base_path, visualization_filename)


    endpoints_file_path = os.path.join(script_dir, 'Endpoints.txt')
    with open(endpoints_file_path, 'r') as file, \
         open(archive_full_path, 'w', newline='', encoding='utf-8') as archive_csv_file, \
         open(visualization_full_path, 'w', newline='', encoding='utf-8') as visualization_csv_file:  # Open in 'w' mode to overwrite
        
        archive_csv_writer = csv.writer(archive_csv_file)
        visualization_csv_writer = csv.writer(visualization_csv_file)

        archive_csv_writer.writerow(['Current Run Date', current_run_date, 'Last Run Date', last_run_date])
        archive_csv_writer.writerow(['Endpoint', 'Status', 'Record Count', 'Message'])

        visualization_csv_writer.writerow(['Endpoint', 'Status', 'Record Count', 'Message'])  # Adjust headers as needed for visualization


        for line in file:
            base_path_identifier, endpoint_name = ('tpdm' if ':tpdm' in line else 'ed-fi', line.strip().split(':')[0])
            api_endpoint = f"{base_url}{base_path_identifier}/{endpoint_name}"
            
            response, error_details = make_authenticated_request(api_endpoint)
            if response and response.ok:
                data = response.json()
                object_count = len(data)  # Assuming data is a list of objects
                status = 'Success'
                message = "Data fetched successfully"
            elif error_details:  # Check if there are error details
                object_count = 0
                status = 'Failed'
                # Include more detailed message in case of error
                message = f"Error {error_details['status_code']}: {error_details['response_body']}"
            else:  # Response is None, all retries failed or other non-403 errors
                object_count = 0
                status = 'Failed'
                message = "Failed to fetch data or all retries failed"

            # Write to CSV once per endpoint with the updated or failed status
            archive_csv_writer.writerow([api_endpoint, status, object_count, message])

            visualization_csv_writer.writerow([api_endpoint, status, object_count, message])

            print(f"Endpoint: {api_endpoint}\nStatus: {status}\nObject Count: {object_count}\nMessage: {message}\n")

            # Time delay to mitigate potential rate limiting issues
            time.sleep(3)
    
    # Update the last run date file with the current run date
    with open(last_run_file, 'w') as file:
        file.write(current_run_date)

if __name__ == "__main__":
    main()