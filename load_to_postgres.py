import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
from io import StringIO

# Load .env variables
load_dotenv()

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Constants
TICKERS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'JPM', 'WMT', 'V']
DATA_DIR = 'cleaned_data'
TABLE_NAME = 'raw_stock_data'

# SQL to create and clear table
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date DATE,
    ticker VARCHAR(10),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
);
TRUNCATE TABLE {TABLE_NAME};
"""

def main():
    """Wrapper for Airflow: loads cleaned data to Postgres using the script's logic."""
    # Initialize connection
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create and clear table
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print(f"Table '{TABLE_NAME}' ready.")

    # Load each CSV
    for ticker in TICKERS:
        print(f"Loading {ticker}...")
        df = pd.read_csv(f'{DATA_DIR}/{ticker}_daily_cleaned.csv')
        df = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, TABLE_NAME, sep='\t', columns=df.columns)
        conn.commit()
        print(f"Loaded {ticker} ({len(df)} rows)")

    # Cleanup
    cur.close()
    conn.close()
    print("Data loading complete!")