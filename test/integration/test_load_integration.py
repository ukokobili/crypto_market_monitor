import os
import pytest
import duckdb as db
import pandas as pd

from scripts.etl.extract import get_exchange_data
from scripts.etl.transform import transform_exchange_data
from scripts.etl.load import write_to_motherduck_from_data_frame

database_name = os.getenv('DATABASE_NAME')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
data_frame = transform_exchange_data(get_exchange_data)

@pytest.fixture
def mock_duckdb(mocker):
    mock_con = mocker.Mock()
    mock_con.execute.return_value = None
    mocker.patch('duckdb.connect', return_value=mock_con)

def test_write_to_motherduck(mock_duckdb, mocker):
    # Create a pandas DataFrame from the sample data
    data = {
        'exchangeId': ['binance', 'gdax'],
        'name': ['Binance', 'Coinbase Pro'],
        'rank': [1, 2],
        'percentTotalVolume': [41.42649, 9.03357],
        'volumeUsd': [11692961313.19589, 2549797968.95156],
        'tradingPairs': [745, 215],
        'socket': [True, True],
        'exchangeUrl': ['https://www.binance.com/', 'https://pro.coinbase.com/'],
        'updated': [1712729250635, 1712729249598],
        'updated_at': ['2024-04-10 06:07:30.635', '2024-04-10 06:07:29.598']
    }
        
    # Call the function
    write_to_motherduck_from_data_frame(data_frame)
    
    # Assertions
    mock_con = mocker.patch('duckdb.connect', return_value=mock_duckdb)
    
    # Assert expected SQL statement was executed
    expected_sql = f"""
    INSERT INTO tokens
    SELECT * FROM data_frame
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
    mock_con.execute.assert_called_once_with(expected_sql)
    # Assert that the number of rows inserted matches the number of rows in the DataFrame
    assert mock_con.execute.call_count == 1

    
