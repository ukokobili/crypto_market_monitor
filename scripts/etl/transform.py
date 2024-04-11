import datetime
import logging
import sys
from typing import Any, Optional

import pandas as pd

sys.path.insert(0, './logs/')
from config import log_config  # noqa

log_config()

# logger for the current module (__name__)
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
                'rank': (
                    int(result['rank']) if result['rank'] is not None else None
                ),
                'percentTotalVolume': (
                    float(result['percentTotalVolume'])
                    if result['percentTotalVolume'] is not None
                    else None
                ),
                'volumeUsd': (
                    float(result['volumeUsd'])
                    if result['volumeUsd'] is not None
                    else None
                ),
                'tradingPairs': (
                    int(result['tradingPairs'])
                    if result['tradingPairs'] is not None
                    else None
                ),
                'socket': bool(result['socket']),
                'exchangeUrl': str(result['exchangeUrl']),
                'updated': (
                    int(result['updated'])
                    if result['updated'] is not None
                    else None
                ),
                'updated_at': get_utc_from_unix_time(result.get('updated')),
            }
            coincap.append(exchange)
        # Convert to dataframe
        data_ = pd.DataFrame(coincap)
        logger.info(
            f'There are {data_.shape[0]} rows & {data_.shape[1]} columns.'
        )

    except Exception as e:
        logger.exception(
            f'{data_ } : -exception {e} encountered during transformation'
        )

    return data_
