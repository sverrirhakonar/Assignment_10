## 📊 Format Comparison: SQLite vs. Parquet

This document analyzes the performance and storage trade-offs between using SQLite and partitioned Parquet for our multi-ticker market data.

### 1. File Size

* **SQLite:** 0.7109 MB
* **Parquet:** 0.3239 MB

**Analysis:**

The partitioned Parquet directory is significantly **smaller** (less than half the size) than the single SQLite database file.

**Why?**
* **Columnar Compression:** Parquet compresses each column individually. Data in a single column (like `close` prices) is very uniform and compresses exceptionally well.
* **Row-Based Storage (SQLite):** SQLite stores data row by row. This mixes data types (datetimes, floats, ints) and makes compression far less effective.
* **Encoding:** Parquet uses efficient data encodings (like dictionary encoding for repeating strings and run-length encoding) that further reduce file size.

---

### 2. Query Speed

We compared the average time to retrieve all data for a single ticker ('TSLA') for a specific date range.

* **SQLite Query Time:** 21.13 ms
* **Parquet Query Time:** 3.09 ms
* **Result:** Parquet was **6.84x faster** for this query.

**Why?**
1.  **Partitioning (Filter Pushdown):** This is the main reason. The Parquet query `filters=[('symbol', '==', 'TSLA')]` told the reader to *only* open the files inside the `symbol=TSLA/` sub-directory. It completely ignored the data for AAPL, MSFT, GOOG, and AMZN.
2.  **SQLite Query Path:** The SQLite query had to:
    1.  Scan the `tickers` table to find the `ticker_id` for 'TSLA'.
    2.  Scan the *entire* `prices` table (or its index), which contains 9,775 rows.
    3.  Filter those rows for the matching `ticker_id` and date range.
    
The Parquet query read only the ~1955 rows for TSLA, while the SQLite query had to sift through all 9775 rows to find the ones it needed.

---

### 3. Use Case Analysis & Discussion

The choice between SQLite and Parquet is not about which is "better," but which is **right for the job**.

#### 🏛️ When to use SQLite (Relational Database)

SQLite is best for **transactional workloads (OLTP)**, where data integrity and frequent, small writes/updates are critical.

* **Best Fit: Live Trading Engine.**
    * **Transactions:** A live system needs to safely `INSERT` a new trade, `UPDATE` a position, and log an order all at once. SQLite's `COMMIT` and `ROLLBACK` features are essential for this to prevent data corruption.
    * **Point Lookups:** It's extremely fast at "point lookups" like `SELECT * FROM positions WHERE symbol = 'AAPL'`.
    * **Data Integrity:** The `FOREIGN KEY` constraint ensures it's impossible to add a price for a `ticker_id` that doesn't exist in the `tickers` table.

* **Poor Fit: Research & Backtesting.** A backtest is a large, analytical "scan" query, not a transactional one. SQLite will be slow as it reads row by row.

#### 🧮 When to use Parquet (Columnar Storage)

Parquet is built for **analytical workloads (OLAP)**, where you read massive amounts of data (often only a few columns) to perform aggregations.

* **Best Fit: Backtesting & Research.**
    * **Backtesting:** A backtest is the classic analytical query: "Read the `close` and `volume` columns for 'AAPL' and 'MSFT' from 2010 to 2020." Parquet is perfect for this. It will only read the `close` and `volume` columns (ignoring open, high, low) and only from the 'AAPL' and 'MSFT' partitions. This makes backtests run exceptionally fast.
    * **Research:** Ideal for "big data" analytics. Calculating the rolling volatility across all tickers (like our Query 2) is much faster because it only reads the `close` column, skipping all other data.

* **Poor Fit: Live Trading Engine.** You cannot easily "update" a Parquet file to change a position. You can only append new files, which is slow and not designed for managing real-time state.