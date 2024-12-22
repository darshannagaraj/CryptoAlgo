import requests
import pandas as pd
def extrembearish(df):
    df['Body'] = abs(df['Open'] - df['Close'])
    df['UpperShadow'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['LowerShadow'] = df[['Open', 'Close']].min(axis=1) - df['Low']

    # Define conditions for extreme bearish candles
    extreme_bearish_candles = df[
        (df['Open'] > df['Close']) &
        (df['LowerShadow'] > df['Body']) &
        (df['UpperShadow'] < df['Body'] * 0.1)
        ]
    return  extreme_bearish_candles
# Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
api_key = 'MMZ52UKQA1LETQZB'

# Define the currency pair you want to fetch data for (e.g., EUR/USD)
symbol = 'EURUSD'

# Define the time interval (e.g., '1min' for 1-minute data, '1h' for 1-hour data)
interval = '5min'

# Define the number of data points you want to retrieve
num_points = 10

# API endpoint URL
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}'

# Fetch data from the API
response = requests.get(url)
data = response.json()


# Parse the data into a DataFrame
time_series = data['Time Series ({})'.format(interval)]
df = pd.DataFrame.from_dict(time_series, orient='index')
df.index = pd.to_datetime(df.index)
df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

# Print the DataFrame
# print(extrembearish(df))
print(df.head(num_points))
