import os
import sys

import pytest

sys.path.insert(0, './scripts/etl/')
from extract import get_exchange_data  # noqa
from load import write_to_motherduck_from_data_frame  # noqa
from transform import transform_exchange_data  # noqa

database_name = os.getenv('DATABASE_NAME')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
data_frame = transform_exchange_data(get_exchange_data())


@pytest.fixture
def mock_duckdb(mocker):
    mock_con = mocker.Mock()
    mock_con.execute.return_value = None
    mocker.patch('duckdb.connect', return_value=mock_con)


def test_write_to_motherduck(mock_duckdb, mocker):
    # Call the function
    write_to_motherduck_from_data_frame(data_frame)

    # Assertions
    mock_con = mocker.patch('duckdb.connect', return_value=mock_duckdb)

    # Assert expected SQL statement was executed
    expected_sql = """
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
    # Assert that the number of rows inserted is same as in the DataFrame
    assert mock_con.execute.call_count == 1
