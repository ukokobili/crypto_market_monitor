import sys  
import logging  
import requests  
from typing import *  
sys.path.insert(0, './logs/')
# Import the log_config function from the logs.logger module
from config import log_config

# Call the log_config function to configure the logger
log_config()

# Get the logger for the current module (__name__)
logger = logging.getLogger(__name__)

# Define a function to retrieve data from a cryptocurrency exchange API
def get_exchange_data() -> List[Dict[str, str]]:
    url = 'https://api.coincap.io/v2/exchanges'  # Define the URL of the API
    try:
        # Make a GET request to the API
        response = requests.get(url) 
        logger.info(f'API request status: {response.status_code}')
    # Handle connection errors
    except requests.ConnectionError as ce:  
        # Log an error message and exit the program with status code 1
        logging.error(f"There was an error with the request, {ce}")
        sys.exit(1)
    return response.json().get('data', [])  # Return the 'data' field from the API response as a list, or an empty list if 'data' is not present
