import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# Define top 10 tickers
tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'JPM', 'WMT', 'V']
end_date = datetime.today()
start_date = end_date - timedelta(days=365 * 5)  

# Create directory for raw data (already created, but ensure it exists)
os.makedirs('raw_data', exist_ok=True)

# Download and save data for each ticker
for ticker in tickers:
    print(f"Downloading data for {ticker}...")
    stock = yf.Ticker(ticker)
    df = stock.history(start=start_date, end=end_date, interval='1d')
    
    # Reset index to make Date a column
    df = df.reset_index()
    
    # Save to CSV
    output_path = f'raw_data/{ticker}_daily.csv'
    df.to_csv(output_path, index=False)
    print(f"Saved {ticker} data to {output_path}")

print("Data ingestion complete!")