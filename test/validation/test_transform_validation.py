import pytest
from scripts.etl.extract import get_exchange_data
from scripts.etl.transform import transform_exchange_data


@pytest.fixture
def transformed_data():
    # Assuming transform_exchange_data returns the transformed data as a DataFrame
    return transform_exchange_data(get_exchange_data())

def test_transform_exchange_data_types(transformed_data):
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
        assert transformed_data[column].dtype == expected_type