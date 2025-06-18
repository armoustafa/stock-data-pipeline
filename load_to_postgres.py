import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Create database connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# List of tickers
tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'JPM', 'WMT', 'V']
cleaned_dir = 'cleaned_data'

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS raw_stock_data (
    date DATE,
    ticker VARCHAR(10),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
);
"""
with engine.connect() as connection:
    connection.execute(create_table_query)
    print("Table raw_stock_data created or verified.")

# Load each CSV into the table
for ticker in tickers:
    input_path = f'{cleaned_dir}/{ticker}_daily_cleaned.csv'
    print(f"Loading data for {ticker}...")
    try:
        df = pd.read_csv(input_path)
        # Ensure column names match table schema
        df = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.to_sql('raw_stock_data', engine, if_exists='append', index=False)
        print(f"Loaded {ticker} data into raw_stock_data.")
    except Exception as e:
        print(f"Error loading {ticker} data: {e}")

print("Data loading complete!")