import pytest
import pandas as pd
from scripts.etl.extract import get_exchange_data
from scripts.etl.transform import transform_exchange_data

def test_transform_exchange_data_types():
    # Sample data
    results = [
        {'exchangeId': 'binance', 'name': 'Binance', 'rank': 1, 'percentTotalVolume': 41.42649,
         'volumeUsd': 11692961313.19589, 'tradingPairs': 745, 'socket': True,
         'exchangeUrl': 'https://www.binance.com/', 'updated': 1712729250635, 'updated_at': '2024-04-10 06:07:30.635'},
        {'exchangeId': 'gdax', 'name': 'Coinbase Pro', 'rank': 2, 'percentTotalVolume': 9.03357,
         'volumeUsd': 2549797968.95156, 'tradingPairs': 215, 'socket': True,
         'exchangeUrl': 'https://pro.coinbase.com/', 'updated': 1712729249598, 'updated_at': '2024-04-10 06:07:29.598'}
    ]

    # Transform the data
    data_frame = transform_exchange_data(get_exchange_data())

    # Define expected data types
    expected_data_types = {
        'exchangeId': 'object',
        'name': 'object',
        'rank': 'Int64',
        'percentTotalVolume': 'float64',
        'volumeUsd': 'float64',
        'tradingPairs': 'Int64',
        'socket': 'bool',
        'exchangeUrl': 'object',
        'updated': 'Int64',
        'updated_at': 'datetime64[ns]'
    }

    # Assert data types
    for column, expected_type in expected_data_types.items():
        assert data_frame[column].dtype == expected_type
