import sys

import pytest

sys.path.insert(0, "./scripts/etl/")
from extract import get_exchange_data  # noqa
from transform import transform_exchange_data  # noqa


@pytest.fixture
def _source_data():
    return transform_exchange_data(get_exchange_data())


# Expected output DataFrame
expected_columns = [
    "exchangeId",
    "name",
    "rank",
    "percentTotalVolume",
    "volumeUsd",
    "tradingPairs",
    "socket",
    "exchangeUrl",
    "updated",
    "updated_at",
]


# Test function
def test_transform_exchange_data():

    transformed_data = _source_data()

    # Assert column names
    assert list(transformed_data.columns) == expected_columns

    # Assert data types
    assert transformed_data["exchangeId"].dtype == "object"
    assert transformed_data["name"].dtype == "object"
    assert transformed_data["rank"].dtype == "int64"
    assert transformed_data["percentTotalVolume"].dtype == "float64"
    assert transformed_data["volumeUsd"].dtype == "float64"
    assert transformed_data["tradingPairs"].dtype == "float64"
    assert transformed_data["socket"].dtype == "bool"
    assert transformed_data["exchangeUrl"].dtype == "object"
    assert transformed_data["updated"].dtype == "float64"
    assert transformed_data["updated_at"].dtype == "datetime64[ns]"
