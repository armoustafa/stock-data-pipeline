import pandas as pd
import os

# Directory paths
raw_dir = 'raw_data'
cleaned_dir = 'cleaned_data'
os.makedirs(cleaned_dir, exist_ok=True)

# List of tickers
tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'JPM', 'WMT', 'V']

# Clean each file
for ticker in tickers:
    input_path = f'{raw_dir}/{ticker}_daily.csv'
    print(f"Cleaning data for {ticker}...")
    try:
        df = pd.read_csv(input_path)
        
        # Standardize date format to YYYY-MM-DD, handling time zones
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.strftime('%Y-%m-%d')
        
        # Handle missing values: forward fill for prices, 0 for volume
        df['Open'] = df['Open'].ffill()
        df['High'] = df['High'].ffill()
        df['Low'] = df['Low'].ffill()
        df['Close'] = df['Close'].ffill()
        df['Volume'] = df['Volume'].fillna(0)
        
        # Add ticker column for later merge
        df['Ticker'] = ticker
        
        # Save cleaned data
        output_path = f'{cleaned_dir}/{ticker}_daily_cleaned.csv'
        df.to_csv(output_path, index=False)
        print(f"Saved cleaned {ticker} data to {output_path}")
    except Exception as e:
        print(f"Error cleaning {ticker} data: {e}")

print("Data cleaning complete!")