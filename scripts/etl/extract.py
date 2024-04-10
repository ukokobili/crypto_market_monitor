import sys
import logging
import requests
from typing import Dict, List
sys.path.insert(0, './logs/')
from config import log_config

# Call the log_config function
log_config()

# Get logger for the current module (__name__)
logger = logging.getLogger(__name__)


# Retrieve data from a cryptocurrency exchange API
def get_exchange_data() -> List[Dict[str, str]]:
    url = 'https://api.coincap.io/v2/exchanges'  
    try:
        # Make a GET request to the API
        response = requests.get(url)
        logger.info(f'API request status: {response.status_code}')
    # Handle connection errors
    except requests.ConnectionError as ce:
        # Log an error message and exit the program with status code 1
        logger.error(f"There was an error with the request, {ce}")
        sys.exit(1)
    return response.json().get(
        'data', []
    )  