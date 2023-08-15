import numpy as np
import pandas as pd
from binance.enums import HistoricalKlinesType
def calculateRSI(df1):
    df = df1
    df['Price Change'] = df['Close'].diff()

    # Calculate gains and losses
    df['Gain'] = df['Price Change'].apply(lambda x: x if x > 0 else 0)
    df['Loss'] = df['Price Change'].apply(lambda x: -x if x < 0 else 0)

    # Calculate average gains and losses (you can adjust the period here)
    rsi_period = 14  # RSI period
    df['Avg Gain'] = df['Gain'].rolling(window=rsi_period).mean()
    df['Avg Loss'] = df['Loss'].rolling(window=rsi_period).mean()

    # Calculate relative strength (RS)
    df['RS'] = df['Avg Gain'] / df['Avg Loss']

    # Calculate RSI
    df1['RSI'] = 100 - (100 / (1 + df['RS']))

def vwapslope(df):
    from sklearn.linear_model import LinearRegression

    # Sample historical price data (date, high, low, close prices, and volume)
    # ... (your data here)

    # Assume you have already calculated VWAP and stored it in the 'VWAP' column

    # Calculate linear regression of VWAP
    vwap_values = df['VWAP'].values.reshape(-1, 1)
    timestamps = np.arange(len(df)).reshape(-1, 1)
    regressor = LinearRegression()
    regressor.fit(timestamps, vwap_values)
    vwap_slope = regressor.coef_[0][0]

    # Check if VWAP is increasing linearly
    is_vwap_increasing_linearly = vwap_slope > 0

    # Print the result
    print(f"Is VWAP increasing linearly? {is_vwap_increasing_linearly}")


def TrainTrack(df, higherema, lowerema, threshold = 2):
    df['5EMA'] = df['Close'].ewm(span=higherema, adjust=False).mean()
    df['9EMA'] = df['Close'].ewm(span=lowerema, adjust=False).mean()

    df['TrainTrack'] = ( df['5EMA'] < df['9EMA'])\
                       | (df['5EMA'] > df['9EMA']) & (abs(df['5EMA'] - df['9EMA']) >= 0.12 * df['Close']) & \
                       (((df['Upper_VWAP_Band'] - df['Lower_VWAP_Band']) / df['VWAP']) >= 0.03)


def lonelyCandle(df, emaSize):
    df['lonelyEMA'] = df['Close'].ewm(span=emaSize, adjust=False).mean()
    df['lonelyEma'] = ((df['High'] < df['lonelyEMA']) & (df['Low'] < df['lonelyEMA']) |
                       (df['High'] > df['lonelyEMA']) & (df['Low'] > df['lonelyEMA'] ))

def is_volume_less_than_average(df, average_period=3):
    df['AvgVolume3'] = df['Volume'].rolling(window=average_period).mean()
    # Check if candles are not touching the EMAs and volume is less than the average of last three candles
    df['VolumeBelowAverage3'] = (df['Volume'] < df['AvgVolume3'])

def calculateVwap(df, vwap_period):
    # Calculate VWAP
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    # Calculate standard deviation of VWAP
    vwap_period = 100  # You can adjust this period
    df['VWAP_STD'] = df['VWAP'].rolling(window=vwap_period).std()

    # Calculate upper and lower VWAP bands
    vwap_band_multiplier = 2  # You can adjust this multiplier
    df['Upper_VWAP_Band'] = df['VWAP'] + vwap_band_multiplier * df['VWAP_STD']
    df['Lower_VWAP_Band'] = df['VWAP'] - vwap_band_multiplier * df['VWAP_STD']

def getminutedata(symbol, interval, lookback, MarketType, client):
    # timestamp = client._get_earliest_valid_timestamp(symbol, '1')
    # print(timestamp)
    if MarketType == "future":
        frame = pd.DataFrame(client.get_historical_klines(symbol, interval, str(lookback) + ' day ago UTC',
                                                          klines_type=HistoricalKlinesType.FUTURES))
    else:
        frame = pd.DataFrame(client.get_historical_klines(symbol, interval, str(lookback) + ' day ago UTC',
                                                          klines_type=HistoricalKlinesType.SPOT))

    frame = frame.iloc[:, :6]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame

def volumeGainers(df, volumeAvgperiod):
    df['AvgVolume'] = df['Volume'].rolling(window=volumeAvgperiod).mean()
    df['AvgVolumelast3'] = df['Volume'].rolling(window=3).mean()
    ema_period_90 = 90
    ema_period_85 = 85
    df['PriceChange'] = df['High'] - df['Low']
    df['AvgPriceChange4'] = df['PriceChange'].rolling(window=4).mean()
    df['AvgPriceChange50'] = df['PriceChange'].rolling(window=50).mean()
    df['CandleRange'] = df['High'] - df['Low']
    df['CloseOpenDifference'] = df['Close'] - df['Open']

    # Calculate percentage of candle that is green
    df['GreenPercentage'] = (df['CloseOpenDifference'] / df['CandleRange']) * 100
    #
    # # Identify if 80% of the candle is green
    # df['Is80PercentGreen'] = df['GreenPercentage'] >= 80
    df['rolling_max'] = df['High'].rolling(window=10, min_periods=1).max()

    # Compare the current 'high' value with the corresponding value in the 'rolling_max' column
    # Create a new column 'is_highest' to store the result of the comparison



    df['allBuy'] = (df['Volume'] > (df['AvgVolume'] * 20)) &\
                        (df['GreenPercentage'] >= 75) &\
                       (df['PriceChange'] > (df['AvgPriceChange50'] * 3.35))


    df['allSell'] = (df['Volume'] > (df['AvgVolume'] * 20)) &  (df['High'] >= df['rolling_max']) &\
                        ((df['GreenPercentage'] <= 40) & (df['GreenPercentage'] >= -15)) & \
                       (df['PriceChange'] > (df['AvgPriceChange50'] * 3.35))

    # df['90EMA'] = df['Close'].ewm(span=ema_period_90, adjust=False).mean()
    # df['85EMA'] = df['Close'].ewm(span=ema_period_85, adjust=False).mean()
    # # Check if candles are not touching the EMAs and volume is less than the average of last three candles
    # df['freshMove'] = (df['Volume'] > ( df['AvgVolume'] * 3 ) ) & \
    #   (df['Close'] > df['90EMA']) & (df['Close'] > df['85EMA']) & (df['AvgPriceChange4'] < df['AvgPriceChange50'])
    #

def panicBuy(df, ema_period):
    df['100EMA'] = df['Close'].ewm(span=ema_period, adjust=False).mean()

    # Sample conditions
    df['Above100EMA'] = df['Close'] > df['100EMA']
    df['PanicCandle'] = (df['High'] - df['Low']) > 2 * df['100EMA'].shift()  # Adjust this condition as needed
    df['BuySignal'] = df['Above100EMA'] & df['PanicCandle'] & (df['High'] > df['High'].shift())

    # Print rows where the BuySignal is True
    selected_rows = df[df['BuySignal']]
    print(selected_rows)

# def VwapStrategy(df):
#     #price is lower then vwap lower band and cross over on lower band
#     #rsi below 20 and crsses above
#     #volume of last 4 candles is less then avergae volume * 2
#     #EMA below 70 Above 90
