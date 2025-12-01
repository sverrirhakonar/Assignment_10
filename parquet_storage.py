import pandas as pd
import pyarrow as pa  # We'll use the pyarrow engine
import pyarrow.parquet as pq
import os
import shutil
import timeit
import sqlite3
from data_loader import load_validate_data # Import from our first script

# --- Configuration ---
MARKET_DATA_FILE = 'market_data_multi.csv'
TICKERS_FILE = 'tickers.csv'
PARQUET_DIR = 'market_data_parquet/' # Directory to store partitioned data
DB_FILE = 'market_data.db' # For comparison

def save_to_parquet(data_df: pd.DataFrame, path: str):
    """
    Saves the DataFrame to a partitioned Parquet dataset.

    Args:
        data_df: The cleaned DataFrame from data_loader.
        path: The directory to save the Parquet files into.
    """
    print(f"\n--- 3. Parquet Storage ---")
    
    # Parquet partitioning needs regular columns, not an index.
    df_to_save = data_df.reset_index()

    # If the directory already exists, remove it to start fresh.
    # This makes our script idempotent (re-runnable).
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Removed existing directory: {path}")

    # Save to Parquet, partitioned by 'symbol'
    # This automatically creates the directory structure.
    df_to_save.to_parquet(
        path,
        engine='pyarrow',
        partition_cols=['symbol'],
        index=False  # We don't save the pandas index
    )
    print(f"Successfully saved data to partitioned Parquet at: {path}")

def run_parquet_queries(path: str):
    """
    Runs the specified Parquet queries from query_tasks.md.

    Args:
        path: The directory of the Parquet dataset.
    """
    print(f"\n--- Running Parquet Queries ---")

    # Query 1: Load all data for AAPL and compute 5-minute rolling average
    print("\n[Query 1: AAPL 5-minute (5-period) rolling average]")
    
    # We use 'filters' to only read the 'AAPL' partition. This is the magic!
    # The query engine never even looks at the other tickers' files.
    df_aapl = pd.read_parquet(
        path,
        engine='pyarrow',
        filters=[('symbol', '==', 'AAPL')]
    )
    
    # Set index for time-series operations
    df_aapl = df_aapl.set_index('timestamp').sort_index()
    
    # The data is 1-minute, so a 5-minute rolling average is a window of 5
    df_aapl['rolling_avg_5min'] = df_aapl['close'].rolling(5).mean()
    print(df_aapl[['close', 'rolling_avg_5min']].tail()) # Show the last 5 rows

    # Query 2: Compute 5-day rolling volatility (std dev) of returns for each ticker
    print("\n[Query 2: 5-day rolling volatility of daily returns]")
    
    # This is a key benefit of columnar storage: we only read the columns we need.
    df_all = pd.read_parquet(
        path,
        engine='pyarrow',
        columns=['timestamp', 'symbol', 'close']
    )
    
    # Ensure timestamp is a datetime object
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
    
    # 1. Get daily closing prices (last price of the day) for each symbol
    df_daily_close = df_all.groupby('symbol').resample('D', on='timestamp')['close'].last()
    
    # 2. Calculate daily percentage returns for each symbol
    df_daily_returns = df_daily_close.groupby('symbol').pct_change()
    
    # 3. Calculate 5-day rolling standard deviation (volatility)
    rolling_vol = df_daily_returns.groupby('symbol').rolling(5).std()
    
    # Rename for clarity
    rolling_vol.name = "5_day_rolling_vol"
    
    # Show the last few entries for each ticker
    print(rolling_vol.groupby('symbol').tail(3))

def run_comparison(db_path: str, parquet_path: str):
    """
    Compares file size and query speed of SQLite vs. Parquet.

    Args:
        db_path: Path to the SQLite DB file.
        parquet_path: Path to the Parquet directory.
    """
    print(f"\n--- 4. Format Comparison ---")
    
    # 1. Compare File Size
    print("\n[Comparison: File Size]")
    db_size_bytes = os.path.getsize(db_path)
    
    parquet_size_bytes = 0
    for dirpath, dirnames, filenames in os.walk(parquet_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            parquet_size_bytes += os.path.getsize(fp)
            
    print(f"SQLite DB Size:    {db_size_bytes / (1024*1024):.4f} MB")
    print(f"Parquet Dir Size:  {parquet_size_bytes / (1024*1024):.4f} MB")
    
    # 2. Compare Query Speed (Task 1: Retrieve all data for one ticker)
    # We will use 'TSLA' as in the SQLite query
    print("\n[Comparison: Query Speed (Avg. of 10 runs)]")
    
    # --- SQLite Test ---
    def query_sqlite_tsla():
        with sqlite3.connect(db_path) as conn:
            pd.read_sql_query(
                "SELECT * FROM prices p JOIN tickers t ON p.ticker_id = t.ticker_id WHERE t.symbol = 'TSLA'",
                conn
            )
    
    # Run 10 times and get the average
    sqlite_time = timeit.timeit(query_sqlite_tsla, number=10) / 10
    print(f"SQLite Query Time (TSLA):    {sqlite_time * 1000:.2f} ms")

    # --- Parquet Test ---
    def query_parquet_tsla():
        pd.read_parquet(
            parquet_path,
            engine='pyarrow',
            filters=[('symbol', '==', 'TSLA')]
        )
        
    parquet_time = timeit.timeit(query_parquet_tsla, number=10) / 10
    print(f"Parquet Query Time (TSLA):   {parquet_time * 1000:.2f} ms")
    print(f"Parquet was {sqlite_time / parquet_time:.2f}x faster.")


if __name__ == '__main__':
    # 1. Load and validate the data first
    try:
        data_df = load_validate_data(MARKET_DATA_FILE, TICKERS_FILE)
        
        # 2. Save to Parquet
        save_to_parquet(data_df, PARQUET_DIR)
        
        # 3. Run the Parquet-specific queries
        run_parquet_queries(PARQUET_DIR)
        
        # 4. Run the comparison (Task 4)
        run_comparison(DB_FILE, PARQUET_DIR)
        
    except (FileNotFoundError, ValueError, pa.ArrowInvalid) as e:
        print(f"\nProcess failed: {e}")