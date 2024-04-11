import logging
import os
import sys

import duckdb

sys.path.insert(0, './logs/')
from config import log_config  # noqa

# Call the log_config function
log_config()
# logger for the current module (__name__)
logger = logging.getLogger(__name__)

# import datawarehouse variables
database_name = os.getenv('DATABASE_NAME')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')


def write_to_motherduck_from_data_frame(data_frame):
    with duckdb.connect(f'md:{database_name}?motherduck_token={motherduck_token}'
    ) as con:
        logger.info('MotherDuck connection successfully initiated.')
        try:
            con.execute(
                f"""
                INSERT INTO tokens
                SELECT * FROM {'data_frame'}
                ON CONFLICT (exchangeId) DO UPDATE SET 
                name = EXCLUDED.name,
                rank = EXCLUDED.rank,
                percentTotalVolume = EXCLUDED.percentTotalVolume,
                volumeUsd = EXCLUDED.volumeUsd,
                tradingPairs = EXCLUDED.tradingPairs,
                socket = EXCLUDED.socket,
                exchangeUrl = EXCLUDED.exchangeUrl,
                updated = EXCLUDED.updated,
                updated_at = EXCLUDED.updated_at;      
                """
            )
            logger.info('Successfully wrote to MotherDuck')
        except Exception as e:
            logger.error(f"Error writing to MotherDuck: {e}")
