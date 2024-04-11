import os
import sys

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, './scripts/etl/')
from extract import get_exchange_data  # noqa
from load import write_to_motherduck_from_data_frame  # noqa
from transform import transform_exchange_data  # noqa

database_name = os.getenv('DATABASE_NAME')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
data_frame = transform_exchange_data(get_exchange_data())


# Mock data for testing
@pytest.fixture
def mock_data_frame():
    data = {
        'exchangeId': ['binance', 'gdax'],
        'name': ['Binance', 'Coinbase Pro'],
        'rank': [1, 2],
        'percentTotalVolume': [41.42649, 9.03357],
        'volumeUsd': [11692961313.19589, 2549797968.95156],
        'tradingPairs': [745, 215],
        'socket': [True, True],
        'exchangeUrl': [
            'https://www.binance.com/',
            'https://pro.coinbase.com/',
        ],
        'updated': [1712729250635, 1712729249598],
        'updated_at': ['2024-04-10 06:07:30.635', '2024-04-10 06:07:29.598'],
    }
    return pd.DataFrame(data)


def test_write_to_motherduck(mock_data_frame, mocker):
    # Mocking the duckdb.connect function
    mocker.patch('duckdb.connect')

    # Call the function
    write_to_motherduck_from_data_frame(mock_data_frame)

    # Assertions
    duckdb.connect.assert_called_once_with(
        f'md:{database_name}?motherduck_token={motherduck_token}'
    )
    # You can add more assertions based on the behavior of your function
