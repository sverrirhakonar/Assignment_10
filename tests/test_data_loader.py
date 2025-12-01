import pytest
import pandas as pd
import numpy as np
from data_loader import load_validate_data

# This fixture creates a valid tickers.csv file in a temporary directory
# for our tests to use.
@pytest.fixture
def tickers_csv(tmp_path):
    """Create a dummy tickers.csv file in a temporary directory."""
    tickers_content = "ticker_id,symbol,name,exchange\n1,AAPL,Apple Inc.,NASDAQ\n2,MSFT,Microsoft Corp.,NASDAQ"
    file_path = tmp_path / "tickers.csv"
    file_path.write_text(tickers_content)
    return file_path

# This fixture creates a valid market_data_multi.csv file
@pytest.fixture
def market_data_csv(tmp_path):
    """Create a dummy, valid market_data_multi.csv file."""
    market_data_content = (
        "Timestamp,Ticker,Open,High,Low,Close,Volume\n"
        "2025-11-17T09:30:00Z,AAPL,271.45,272.07,270.77,270.88,1416\n"
        "2025-11-17T09:31:00Z,AAPL,270.88,271.50,270.88,271.12,1500\n"
        "2025-11-17T09:30:00Z,MSFT,184.21,184.51,183.63,183.89,4997\n"
        "2025-11-17T09:31:00Z,MSFT,183.89,184.00,183.50,183.95,5000\n"
    )
    file_path = tmp_path / "market_data.csv"
    file_path.write_text(market_data_content)
    return file_path

# --- Test Cases ---

def test_load_happy_path(market_data_csv, tickers_csv):
    """
    Tests that the data loads correctly when all inputs are valid.
    """
    # This should run without raising any errors
    df = load_validate_data(market_data_csv, tickers_csv)
    
    # Check that the data is loaded
    assert not df.empty
    # Check that the index is correct (timestamp, symbol)
    assert 'timestamp' in df.index.names
    assert 'symbol' in df.index.names
    # Check that it correctly found our 2 tickers
    assert len(df.index.get_level_values('symbol').unique()) == 2

def test_load_renames_ticker_column(tmp_path, tickers_csv):
    """
    Tests that the loader correctly renames a 'ticker' column to 'symbol'.
    This test is based on the bug we found earlier.
    """
    # Note the column name "Ticker"
    market_data_content = (
        "Timestamp,Ticker,Open,High,Low,Close,Volume\n"
        "2025-11-17T09:30:00Z,AAPL,271.45,272.07,270.77,270.88,1416\n"
    )
    market_data_csv = tmp_path / "market_data.csv"
    market_data_csv.write_text(market_data_content)
    
    df = load_validate_data(market_data_csv, tickers_csv)
    
    # The final DataFrame should have 'symbol' in its index, not 'ticker'
    assert 'symbol' in df.index.names
    assert 'ticker' not in df.index.names

def test_load_fails_on_missing_data(tmp_path, tickers_csv):
    """
    Tests that validation fails if 'close' price is missing (NaN).
    """
    # Note the blank 'Close' value
    market_data_content = (
        "Timestamp,Ticker,Open,High,Low,Close,Volume\n"
        "2025-11-17T09:30:00Z,AAPL,271.45,272.07,270.77,,1416\n"
    )
    market_data_csv = tmp_path / "market_data.csv"
    market_data_csv.write_text(market_data_content)
    
    # We assert that a ValueError IS raised, and check the error message
    with pytest.raises(ValueError, match="Missing critical OHLCV or timestamp data"):
        load_validate_data(market_data_csv, tickers_csv)

def test_load_fails_on_duplicate_rows(tmp_path, tickers_csv):
    """
    Tests that validation fails if there are duplicate (timestamp, symbol) rows.
    """
    # Note the two identical rows for AAPL at 09:30:00
    market_data_content = (
        "Timestamp,Ticker,Open,High,Low,Close,Volume\n"
        "2025-11-17T09:30:00Z,AAPL,271.45,272.07,270.77,270.88,1416\n"
        "2025-11-17T09:30:00Z,AAPL,271.45,272.07,270.77,270.88,1416\n"
    )
    market_data_csv = tmp_path / "market_data.csv"
    market_data_csv.write_text(market_data_content)
    
    with pytest.raises(ValueError, match="Duplicate data found"):
        load_validate_data(market_data_csv, tickers_csv)

def test_load_fails_on_missing_ticker(tmp_path):
    """
    Tests that validation fails if tickers.csv expects a ticker
    that is not in the market data.
    """
    # tickers.csv expects 'AAPL' and 'MSFT'
    tickers_content = "ticker_id,symbol,name,exchange\n1,AAPL,Apple Inc.,NASDAQ\n2,MSFT,Microsoft Corp.,NASDAQ"
    tickers_csv = tmp_path / "tickers.csv"
    tickers_csv.write_text(tickers_content)
    
    # market_data.csv ONLY contains 'AAPL'
    market_data_content = (
        "Timestamp,Ticker,Open,High,Low,Close,Volume\n"
        "2025-11-17T09:30:00Z,AAPL,271.45,272.07,270.77,270.88,1416\n"
    )
    market_data_csv = tmp_path / "market_data.csv"
    market_data_csv.write_text(market_data_content)
    
    # The function should fail because 'MSFT' is missing
    with pytest.raises(ValueError, match="Missing required tickers"):
        load_validate_data(market_data_csv, tickers_csv)