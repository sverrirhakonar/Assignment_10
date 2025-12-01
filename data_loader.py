import pandas as pd
from typing import Set

def load_validate_data(market_data_path: str, tickers_path: str) -> pd.DataFrame:
    """
    Loads multi-ticker market data from a CSV, validates it, and cleans it.

    Args:
        market_data_path: Path to the market_data_multi.csv file.
        tickers_path: Path to the tickers.csv file.

    Returns:
        A cleaned and validated pandas DataFrame.
    
    Raises:
        ValueError: If any validation check fails.
    """
    
    print("--- 1. Data Ingestion and Validation ---")

    # 1. Load Required Tickers
    try:
        required_tickers_df = pd.read_csv(tickers_path)
        required_tickers: Set[str] = set(required_tickers_df['symbol'].unique())
        print(f"Successfully loaded {len(required_tickers)} required tickers from {tickers_path}")
    except FileNotFoundError:
        print(f"Error: Ticker file not found at {tickers_path}")
        raise

    # 2. Load Market Data
    try:
        df = pd.read_csv(market_data_path)
        print(f"Successfully loaded market data from {market_data_path}. Shape: {df.shape}")
    except FileNotFoundError:
        print(f"Error: Market data file not found at {market_data_path}")
        raise

    # 3. Normalize & Clean Data
    
    # Standardize column names (e.g., 'Timestamp' -> 'timestamp')
    df.columns = [col.lower() for col in df.columns]
    
    # --- FIX: Standardize the ticker/symbol column to be 'symbol' ---
    if 'ticker' in df.columns and 'symbol' not in df.columns:
        df.rename(columns={'ticker': 'symbol'}, inplace=True)
        print("Normalized 'ticker' column to 'symbol'.")
    # --- END FIX ---
    
    # Convert timestamp column to datetime objects.
    # We assume UTC, as is standard for financial data.
    # We'll use format='ISO8601' for robustness
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', utc=True)

    # Ensure OHLCV columns are numeric
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce') # 'coerce' turns bad data into NaN

    print("Data normalized: Columns lowercased, timestamp set to datetime (UTC).")

    # 4. Run Validations

    # Check for missing data
    critical_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    
    # Check if all critical columns exist *before* trying to access them
    missing_cols = set(critical_cols) - set(df.columns)
    if missing_cols:
        print(f"Error: The market data file is missing critical columns: {missing_cols}")
        raise ValueError(f"Missing required columns in {market_data_path}")
        
    missing_data = df[critical_cols].isna().sum()
    
    if missing_data.sum() > 0:
        print(f"Error: Missing data found!\n{missing_data}")
        raise ValueError("Missing critical OHLCV or timestamp data.")
    print("Validation passed: No missing data.")

    # Check for duplicate timestamp/ticker pairs
    duplicates = df.duplicated(subset=['timestamp', 'symbol']).sum()
    if duplicates > 0:
        print(f"Error: Found {duplicates} duplicate rows (same ticker at same time).")
        raise ValueError("Duplicate data found.")
    print("Validation passed: No duplicate timestamp/ticker entries.")

    # Check for ticker completeness
    loaded_tickers: Set[str] = set(df['symbol'].unique())
    
    missing_tickers = required_tickers - loaded_tickers
    if missing_tickers:
        print(f"Error: Market data is missing required tickers: {missing_tickers}")
        raise ValueError("Missing required tickers in market data.")
    print("Validation passed: All required tickers are present.")
    
    print("--- Data Ingestion Complete ---")
    
    # Set a clean index for time-series operations
    df = df.set_index(['timestamp', 'symbol']).sort_index()
    
    return df

if __name__ == '__main__':
    # Define file paths
    MARKET_DATA_FILE = 'market_data_multi.csv'
    TICKERS_FILE = 'tickers.csv'

    try:
        # Run the loading and validation
        cleaned_df = load_validate_data(MARKET_DATA_FILE, TICKERS_FILE)
        
        print("\n--- Cleaned Data Sample (MultiIndex) ---")
        print(cleaned_df.head())
        
        print("\n--- Data Info ---")
        cleaned_df.info()

    except (FileNotFoundError, ValueError) as e:
        print(f"\nProcess failed: {e}")