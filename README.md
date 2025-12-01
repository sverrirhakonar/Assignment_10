# Assignment 10

## 🧠 Overview

This project implements a Python-based system for ingesting, storing, and querying multi-ticker (AAPL, MSFT, GOOG, TSLA, AMZN) OHLCV market data.


It directly compares two different storage solutions for financial data:
1.  **SQLite3:** A relational (row-based) database, excellent for transactions and data integrity.
2.  **Parquet:** A columnar (column-based) storage format, optimized for large-scale analytical queries.

The goal is to demonstrate the tradeoffs in file size, query speed, and ideal use cases for each technology in a financial engineering context.

---

## 📦 Project Structure
```
ASSIGNMENT_10/
├── market_data_parquet/        <-- Output: Parquet data, partitioned by symbol
│   ├── symbol=AAPL/
│   │   └── ... .parquet
│   ...
├── tests/                      <-- Unit tests
│   └── test_data_loader.py
├── data_loader.py              <-- Task 1: Loads and validates CSV data
├── sqlite_storage.py           <-- Task 2: Populates SQLite DB and runs SQL queries
├── parquet_storage.py          <-- Task 3 & 4: Creates Parquet files, runs queries, and runs comparison
├── market_data_multi.csv       <-- Input: Raw 1-minute OHLCV data
├── tickers.csv                 <-- Input: Master list of tickers
├── schema.sql                  <-- Input: Schema for the SQLite database
├── market_data.db              <-- Output: SQLite database file
├── query_tasks.md              <-- Assignment: The query requirements
├── comparison.md               <-- Output: Written analysis from Task 4
└── README.md                   <-- This file
```

---

## ⚙️ Setup & Installation

This project uses Python 3.x. It is highly recommended to use a virtual environment.

1.  **Clone the repository (or set up the folder):**
```bash
    git clone ...
    cd ASSIGNMENT_10
```

2.  **Create a virtual environment:**
```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3.  **Create a `requirements.txt` file:**
    Create a file named `requirements.txt` in the `ASSIGNMENT_10` folder and paste the following content into it:
```txt
    pandas
    pyarrow
    pytest
    numpy
```

4.  **Install the required libraries:**
```bash
    pip install -r requirements.txt
```

---

## 🚀 How to Run

The scripts are designed to be run in order.

1.  **Run the SQLite Pipeline:**
    This script will (1) load and validate the data, (2) create `market_data.db`, (3) populate the tables, and (4) run all four SQL queries from `query_tasks.md`.
```bash
    python sqlite_storage.py
```

2.  **Run the Parquet Pipeline & Comparison:**
    This script will (1) load and validate the data, (2) create the `market_data_parquet/` directory, (3) run all Parquet-specific queries, and (4) run the final file size and query speed comparison.
```bash
    python parquet_storage.py
```

3.  **Run Unit Tests:**
    To ensure the data loader's validation logic is working correctly, run `pytest` from the main `ASSIGNMENT_10` directory:
```bash
    pytest
```
    This will automatically discover and run the tests in the `tests/` folder.

## Acknowledgments

This project was developed with assistance from Google Gemini AI for code generation, debugging, and implementation guidance.