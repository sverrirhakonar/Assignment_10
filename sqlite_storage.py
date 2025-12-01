import sqlite3
import pandas as pd
from data_loader import load_validate_data  # Import from our previous script
from typing import Dict, Set

# --- Configuration ---
DB_FILE = 'market_data.db'
SCHEMA_FILE = 'schema.sql'
MARKET_DATA_FILE = 'market_data_multi.csv'
TICKERS_FILE = 'tickers.csv'

def create_and_populate_db(db_path: str, schema_path: str, data_df: pd.DataFrame, tickers_path: str):
    """
    Creates and populates the SQLite database from the schema and data.
    
    Args:
        db_path: The file path for the SQLite database.
        schema_path: The file path for the schema.sql file.
        data_df: The cleaned market data DataFrame (from data_loader).
        tickers_path: The file path for the tickers.csv file.
    """
    print(f"\n--- 2. SQLite3 Storage ---")
    
    # Load the tickers.csv data
    try:
        tickers_df = pd.read_csv(tickers_path)
    except FileNotFoundError:
        print(f"Error: Ticker file not found at {tickers_path}")
        raise
    
    # Connect to the SQLite database (it will be created if it doesn't exist)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # 1. Create Tables from Schema
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            cursor.executescript(schema_sql)
            print(f"Successfully executed schema from {schema_path}")
        except FileNotFoundError:
            print(f"Error: Schema file not found at {schema_path}")
            raise
        except sqlite3.Error as e:
            print(f"Error executing schema: {e}")
            raise

        # 2. Populate 'tickers' Table
        # Use pandas to_sql. 'replace' drops the table first and recreates it.
        # This makes the script easy to re-run.
        try:
            tickers_df.to_sql('tickers', conn, if_exists='replace', index=False)
            print(f"Successfully populated 'tickers' table with {len(tickers_df)} entries.")
        except Exception as e:
            print(f"Error populating 'tickers' table: {e}")
            raise

        # 3. Prepare and Populate 'prices' Table
        
        # First, create the symbol -> ticker_id map from our newly populated table
        ticker_map_df = pd.read_sql_query("SELECT ticker_id, symbol FROM tickers", conn)
        ticker_map: Dict[str, int] = dict(zip(ticker_map_df['symbol'], ticker_map_df['ticker_id']))
        
        # Reset the index of our data_df to get 'timestamp' and 'symbol' as columns
        prices_df = data_df.reset_index()
        
        # Map the 'symbol' column to 'ticker_id'
        prices_df['ticker_id'] = prices_df['symbol'].map(ticker_map)
        
        # Convert timestamp back to string for SQLite compatibility if it's not already
        if not pd.api.types.is_string_dtype(prices_df['timestamp']):
             prices_df['timestamp'] = prices_df['timestamp'].astype(str)
        
        # Prepare the final DataFrame to match the 'prices' table schema
        prices_to_insert = prices_df[['timestamp', 'ticker_id', 'open', 'high', 'low', 'close', 'volume']]
        
        # Use to_sql to append the data
        try:
            prices_to_insert.to_sql('prices', conn, if_exists='append', index=False)
            print(f"Successfully populated 'prices' table with {len(prices_to_insert)} entries.")
        except Exception as e:
            print(f"Error populating 'prices' table: {e}")
            raise
            
    print(f"Database '{db_path}' created and populated successfully.")

def run_sqlite_queries(db_path: str):
    """
    Runs the specified SQL queries from query_tasks.md against the database.
    
    Args:
        db_path: The file path for the SQLite database.
    """
    print(f"\n--- Running SQLite Queries ---")
    
    try:
        with sqlite3.connect(db_path) as conn:
            
            # Query 1: Retrieve all data for TSLA between 2025-11-17 and 2025-11-18.
            print("\n[Query 1: TSLA data for 2025-11-17 to 2025-11-18]")
            q1 = """
            SELECT p.timestamp, t.symbol, p.open, p.high, p.low, p.close, p.volume
            FROM prices p
            JOIN tickers t ON p.ticker_id = t.ticker_id
            WHERE t.symbol = 'TSLA'
              AND p.timestamp >= '2025-11-17 00:00:00+00:00'
              AND p.timestamp <= '2025-11-18 23:59:59+00:00'
            ORDER BY p.timestamp;
            """
            df_q1 = pd.read_sql_query(q1, conn)
            print(df_q1)

            # Query 2: Calculate average daily volume per ticker.
            # This query calculates the average of the *total* daily volume for each ticker.
            print("\n[Query 2: Average of *total* daily volume per ticker]")
            q2 = """
            WITH DailyVolume AS (
                SELECT
                    t.symbol,
                    DATE(p.timestamp) AS trade_date,
                    SUM(p.volume) AS total_daily_volume
                FROM prices p
                JOIN tickers t ON p.ticker_id = t.ticker_id
                GROUP BY t.symbol, trade_date
            )
            SELECT
                symbol,
                AVG(total_daily_volume) AS average_of_total_daily_volume
            FROM DailyVolume
            GROUP BY symbol
            ORDER BY symbol;
            """
            df_q2 = pd.read_sql_query(q2, conn)
            print(df_q2)

            # Query 3: Identify top 3 tickers by return over the full period.
            print("\n[Query 3: Top 3 tickers by return (full period)]")
            q3 = """
            WITH FirstPrices AS (
                SELECT
                    ticker_id,
                    open AS first_price
                FROM prices
                WHERE (ticker_id, timestamp) IN (
                    SELECT ticker_id, MIN(timestamp)
                    FROM prices
                    GROUP BY ticker_id
                )
            ),
            LastPrices AS (
                SELECT
                    ticker_id,
                    close AS last_price
                FROM prices
                WHERE (ticker_id, timestamp) IN (
                    SELECT ticker_id, MAX(timestamp)
                    FROM prices
                    GROUP BY ticker_id
                )
            )
            SELECT
                t.symbol,
                fp.first_price,
                lp.last_price,
                ((lp.last_price / fp.first_price) - 1.0) * 100.0 AS percentage_return
            FROM tickers t
            JOIN FirstPrices fp ON t.ticker_id = fp.ticker_id
            JOIN LastPrices lp ON t.ticker_id = lp.ticker_id
            ORDER BY percentage_return DESC
            LIMIT 3;
            """
            df_q3 = pd.read_sql_query(q3, conn)
            print(df_q3)

            # Query 4: Find first and last trade price for each ticker per day.
            print("\n[Query 4: First (open) and Last (close) trade price per ticker per day]")
            q4 = """
            WITH DailyData AS (
                SELECT
                    ticker_id,
                    timestamp,
                    DATE(timestamp) AS trade_date,
                    open,
                    close,
                    -- Find the first row for each day/ticker partition
                    ROW_NUMBER() OVER(
                        PARTITION BY ticker_id, DATE(timestamp) 
                        ORDER BY timestamp ASC
                    ) as rn_first,
                    -- Find the last row for each day/ticker partition
                    ROW_NUMBER() OVER(
                        PARTITION BY ticker_id, DATE(timestamp) 
                        ORDER BY timestamp DESC
                    ) as rn_last
                FROM prices
            ),
            FirstPrice AS (
                SELECT ticker_id, trade_date, open AS first_price
                FROM DailyData
                WHERE rn_first = 1
            ),
            LastPrice AS (
                SELECT ticker_id, trade_date, close AS last_price
                FROM DailyData
                WHERE rn_last = 1
            )
            SELECT
                t.symbol,
                f.trade_date,
                f.first_price,
                l.last_price
            FROM tickers t
            JOIN FirstPrice f ON t.ticker_id = f.ticker_id
            JOIN LastPrice l ON t.ticker_id = l.ticker_id AND f.trade_date = l.trade_date
            ORDER BY t.symbol, f.trade_date;
            """
            df_q4 = pd.read_sql_query(q4, conn)
            print(df_q4)
            
    except sqlite3.Error as e:
        print(f"Error running queries: {e}")
        raise

if __name__ == '__main__':
    # 1. Load and validate the data first
    try:
        data_df = load_validate_data(MARKET_DATA_FILE, TICKERS_FILE)
        
        # 2. Create and populate the database
        create_and_populate_db(DB_FILE, SCHEMA_FILE, data_df, TICKERS_FILE)
        
        # 3. Run the queries
        run_sqlite_queries(DB_FILE)
        
    except (FileNotFoundError, ValueError, sqlite3.Error) as e:
        print(f"\nProcess failed: {e}")