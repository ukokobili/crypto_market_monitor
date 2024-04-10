import sys
import datetime
import logging
import pandas as pd
from typing import Any, Dict, List, Optional
sys.path.insert(0, './logs/')
# Import the log_config function from the logs.logger module
from config import log_config

# Call the log_config function to configure the logger
log_config()

# Get the logger for the current module (__name__)
logger = logging.getLogger(__name__)

# Function unix to datatime
def get_utc_from_unix_time(
    unix_ts: Optional[Any], second: int = 1000
) -> Optional[datetime.datetime]:
    return (
        datetime.datetime.utcfromtimestamp(int(unix_ts) / second)
        if unix_ts
        else None
    )

# Function for cleaning and transforming the data
def transform_exchange_data(results) -> pd.DataFrame:
    try:
        results = results
        coincap = []

        for result in results:
            exchange = {
                'exchangeId': str(result['exchangeId']),
                'name': str(result['name']),
                'rank': int(result['rank']) if result['rank'] is not None else None,
                'percentTotalVolume': float(result['percentTotalVolume']) if result['percentTotalVolume'] is not None else None,
                'volumeUsd': float(result['volumeUsd']) if result['volumeUsd'] is not None else None,
                'tradingPairs': int(result['tradingPairs']) if result['tradingPairs'] is not None else None,
                'socket': bool(result['socket']),
                'exchangeUrl': str(result['exchangeUrl']),
                'updated': int(result['updated']) if result['updated'] is not None else None,
                'updated_at': get_utc_from_unix_time(result.get('updated'))
            }
            coincap.append(exchange)
        # Convert to dataframe
        data_ = pd.DataFrame(coincap)
        logger.info(f'There are {data_.shape[0]} rows and {data_.shape[1]} columns in the Dataframe.')
        
    except Exception as e:
        logger.exception( f'{data_ } : - exception {e} encountered while transforming data from the API source')
    
    return data_