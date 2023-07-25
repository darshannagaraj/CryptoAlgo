import ccxt
import pandas as pd
import matplotlib.pyplot as plt

def get_historical_data(exchange, symbol, timeframe, limit):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def calculate_vwap(df):
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    df['pv'] = df['tp'] * df['volume']
    df['cum_volume'] = df['volume'].cumsum()
    df['cum_pv'] = df['pv'].cumsum()
    df['vwap'] = df['cum_pv'] / df['cum_volume']
    return df

def scan_stocks_making_higher_highs(df):
    return df['high'].idxmax()

def main():
    # Set up the Binance exchange instance
    exchange = ccxt.binance()

    # Symbol and parameters for Bitcoin
    symbol = 'BTC/USDT'
    timeframe = '1h'  # 1-hour timeframe
    limit = 1000      # Number of candles to fetch

    # Get historical data and calculate VWAP
    data = get_historical_data(exchange, symbol, timeframe, limit)
    data_with_vwap = calculate_vwap(data)

    # Plot the Bitcoin price chart and VWAP
    plt.figure(figsize=(12, 6))
    plt.plot(data_with_vwap['timestamp'], data_with_vwap['close'], label='Bitcoin Price', color='blue')
    plt.plot(data_with_vwap['timestamp'], data_with_vwap['vwap'], label='VWAP', color='orange')
    plt.xlabel('Timestamp')
    plt.ylabel('Price')
    plt.title('Bitcoin Price and VWAP')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    main()
