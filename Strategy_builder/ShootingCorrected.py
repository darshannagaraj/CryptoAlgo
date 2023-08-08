import datetime
import math
import os
import pandas as pd
import numpy as np
import sys
import logging

logging.basicConfig(
    level=logging.ERROR,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',   # Set the name of the log file
    filemode='a'          # Set the file mode (w: write, a: append)
)


current_directory = os.path.abspath(__file__)
two_levels_before = os.path.dirname(os.path.dirname(current_directory))
three = os.path.dirname(os.path.dirname(os.path.dirname(current_directory)))
sys.path.append(three)
sys.path.append(two_levels_before)
sys.path.append(two_levels_before+'\\Data_loader\\')
sys.path.append(two_levels_before+'\\lib\\')
sys.path.append(two_levels_before+'\\OrderManagement\\')

from binance.client import Client
from Data_loader.Binance_api import Binance_Api_wrapper_generic
from Data_loader.Binance_api_helper import Binance_api_helper
from Data_loader.postgres_data_handler import postges_push_trade
import btalib
from binance.enums import FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, HistoricalKlinesType
import time
import pytz
from lib.database import insert_scanned_data


def gmt_to_ist(gmt_time_str):
    # Define time zones for GMT and IST
    gmt_tz = pytz.timezone('GMT')
    ist_tz = pytz.timezone('Asia/Kolkata')

    # Convert the input string to a datetime object with GMT timezone
    gmt_time = datetime.strptime(gmt_time_str, '%Y-%m-%d %H:%M:%S')
    gmt_time = gmt_tz.localize(gmt_time)

    # Convert GMT time to IST time
    ist_time = gmt_time.astimezone(ist_tz)

    return ist_time.strftime('%Y-%m-%d %H:%M:%S')

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

def avg_candle_size_calculate(dt):
    sum= 0
    k=0
    for index, row in dt.iterrows():
        k=k+1
        sum = sum + (row.High - row.Low)
    sum = sum/k
    return sum


def is_hammer(candle, average_candle_size):
    body_size = abs(candle['Open'] - candle['Close'])
    lower_shadow = min(candle['Open'], candle['Close']) - candle['Low']

    return body_size <= average_candle_size * 0.5 and lower_shadow >= body_size * 2

def is_shooting_star_buy(candle,candlestick_data) :
    #, average_candle_size):
    average_candle_size = candlestick_data['High'].mean() - candlestick_data['Low'].mean()

    body_size = abs(candle['Open'] - candle['Close'])
    upper_shadow = candle['High'] - max(candle['Open'], candle['Close'])
    lower_shadow = min(candle['Open'], candle['Close']) - candle['Low']

    return lower_shadow >= 2 * body_size and body_size >= upper_shadow * 2 and body_size > average_candle_size

def longwick(df):
    df['Body'] = abs(df['Open'] - df['Close'])
    df['UpperShadow'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['LowerShadow'] = df[['Open', 'Close']].min(axis=1) - df['Low']

    # Define the maximum body size percentage for small body
    max_body_percentage = 20  # Adjust this value as needed

    # Identify long wick candles with a small body
    long_wick_small_body_candles = df[
        (df['Body'] <= df['Range'] * max_body_percentage / 100) &
        ((df['UpperShadow'] > df['Body'] * 1.8) | (df['LowerShadow'] > df['Body'] * 2))
        ]
def extremeBullish(df):
    df['Body'] = abs(df['Open'] - df['Close'])
    df['UpperShadow'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['LowerShadow'] = df[['Open', 'Close']].min(axis=1) - df['Low']

    # Define conditions for extreme bullish candles
    extreme_bullish_candles = df[
        (df['Close'] > df['Open']) &
        (df['UpperShadow'] > df['Body']) &
        (df['LowerShadow'] < df['Body'] * 0.1)
        ]
    return extreme_bullish_candles


def is_extreme_bearish_candle(df):
    open_price = df['Open']
    high_price= df['High']
    low_price = df['Low']
    close_price = df['Close']
    body_size = abs(open_price - close_price)
    upper_shadow = high_price - max(open_price, close_price)
    lower_shadow = min(open_price, close_price) - low_price

    if open_price > close_price and lower_shadow > body_size and upper_shadow < body_size * 0.1:
        return True
    else:
        return False

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

def shootingstar(df):
    df['Range'] = df['High'] - df['Low']
    df['Body'] = abs(df['Open'] - df['Close'])
    min_body_percentage = 0.1
    shooting_stars = df[
        (df['Body'] >= df['Range'] * min_body_percentage) &
        (df['Body'] < df['Range'] * 0.3) &
        (df['Close'] < df['Open']) &
        (df['Close'] < df['Low'] + df['Range'] * 0.3)
        ]

    return shooting_stars


def special_candle(candle):
    highToclose = abs(candle['High'] - candle['Close'])
    lowtoclose = abs(candle['Low'] - candle['Close'])
    if highToclose > lowtoclose:
        return True

def is_shooting_star(candle,candlestick_data) :
    #, average_candle_size):
    average_candle_size = candlestick_data['High'].mean() - candlestick_data['Low'].mean()

    body_size = abs(candle['Open'] - candle['Close'])
    upper_shadow = candle['High'] - max(candle['Open'], candle['Close'])
    lower_shadow = min(candle['Open'], candle['Close']) - candle['Low']

    return upper_shadow >= 2 * body_size and body_size >= lower_shadow * 0.1 and body_size > average_candle_size


def find_shooting_stars(candlestick_data, lookback_period=50):
    shooting_stars = []
    average_candle_size =  average_candle_size = candlestick_data['High'].rolling(lookback_period).mean() - candlestick_data['Low'].rolling(lookback_period).mean()

    for i in range(1, len(candlestick_data)):
        current_candle = candlestick_data.iloc[i]
        previous_candle = candlestick_data.iloc[i - 1]

        if is_shooting_star(previous_candle, average_candle_size):
            shooting_stars.append(previous_candle)

    return shooting_stars


def calculate_vwap_bands(df, num_std_dev=1):
    # Calculate VWAP for each symbol
    df['vwap'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    # Calculate VWAP for each symbol
    df['vwap'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    # Calculate standard deviation of price around VWAP
    df['price_deviation'] = df['Close'] - df['vwap']
    price_std_dev = df['price_deviation'].std()

    # Calculate upper and lower bands based on standard deviation
    df['lower_band'] = df['vwap'] - num_std_dev * price_std_dev
    df['higher_band'] = df['vwap'] + num_std_dev * price_std_dev

    return df
def count_decimal_places(num):
    decimal_part = str(num).split('.')[-1]
    return len(decimal_part)


def calculate_stop_lossForSell(candle):
    slprice = candle['Close'].iloc[0] * 0.98
    closing_prices = candle['Close'].iloc[0] + (candle['Close'].iloc[0] * 0.993)
    decimal_count = count_decimal_places(slprice)
    sl_price = round(float(slprice), decimal_count)
    sl = np.maximum(candle['High'].iloc[0], sl_price)  # 2% below the close price
    return sl

def calculate_stop_lossForBuy(candle):
    slprice = candle['Close'].iloc[0] * 1.02
    closing_prices = candle['Close'].iloc[0]
    decimal_count = count_decimal_places(slprice)
    sl_price = round(float(slprice), decimal_count)
    sl = np.minimum(candle['Low'].iloc[0], sl_price)  # 2% below the close price
    return sl


def find_symbols_close_to_vwap(df, threshold_lower=0.8, threshold_higher=1.2):
    # Calculate VWAP bands using 1 standard deviation
    df = calculate_vwap_bands(df, num_std_dev=1)

    # Filter symbols where the price is within the specified threshold of VWAP
    # Check if the price is within the specified threshold of VWAP
    price = df['Close'].iloc[-1]
    lower_band = df['lower_band'].iloc[-1]
    higher_band = df['higher_band'].iloc[-1]
    return lower_band <= price <= higher_band
def find_dictionary_by_key(list_of_dicts, key, value):
    for dictionary in list_of_dicts:
        if dictionary.get(key) == value:
            return dictionary
    return 0

def getLatestPrice(client, symbol):
    dt1 = client.get_all_tickers()
    latestPrice = (find_dictionary_by_key(dt1, 'symbol', symbol))

def find_movement_based_on_time_frame(s,client,market_type, Scanned_all,wrapper_obj, drop_rows=0 ):

    atr_period = 10
    atr_multiplier = 3.0
    scanned_data = {}
    five_minute = getminutedata(s['symbol'],client.KLINE_INTERVAL_5MINUTE,1, market_type,client)
    five_minute.drop(five_minute.tail(drop_rows).index,
            inplace=True)

    five_minute.reset_index(inplace=True)
    # five_minute = five_minute.loc[five_minute.Time <= specifictime]
    df = five_minute
    # Find Sell signals
    # df = df.drop(df.index[-95])


    vwapStatus = find_symbols_close_to_vwap(five_minute)
    VWAP = calculate_vwap_bands(five_minute, num_std_dev=1)
    df['SMA'] = df['Close'].rolling(window=20).mean()
    df['STD'] = df['Close'].rolling(window=20).std()
    df['BollingerUpper'] = df['SMA'] + 2 * df['STD']
    df['BollingerLower'] = df['SMA'] - 2 * df['STD']

    # timeFrame =  is_candle_completed(df, 5)
    sell =""
    buy =""
    # Filter data for faster access
    previous_candles = df.iloc[-8:-3]
    pcandle = df.iloc[-3: -2].to_dict(orient='records')[0]
    JustCandle = df.iloc[-2: -1].to_dict(orient='records')[0]
    boldifSati, diff = is_bollinger_difference_sufficient(pcandle['BollingerUpper'], pcandle['BollingerLower'])

    if is_volume_greater_than_average(pcandle, df):
        print("volume above avergae", s['symbol'])
        if is_volume_greater_than_previous(pcandle, previous_candles):
            print("volume is greter than previous ", s['symbol'])
            if boldifSati and is_outside_bollinger_upper(pcandle, pcandle['BollingerUpper']):
                print("bolinger diff i sufficent  ", s['symbol'])
                if (special_candle(pcandle) or is_shooting_star(pcandle, previous_candles) or is_extreme_bearish_candle(pcandle)):
                    print("substabtial red candle", s['symbol'], datetime.datetime.now())
                    sell = "yes"
            elif boldifSati and is_outside_bollinger_lower(pcandle, pcandle['BollingerLower']):
                if is_exhaustive_volume(pcandle, df) and is_shooting_star_buy(pcandle, previous_candles):
                    print("substabtial gree candle", s['symbol'], datetime.datetime.now())
                    print("Buy condition matched", s['symbol'], pcandle)
                    buy = "yes"

    # or (is_candle_red(pcandle, previous_candles)

    if sell == "yes":
        sl = calculate_stop_lossForSell(df.iloc[-3: -2])
        sl1 = calculate_stop_lossForSell(df.iloc[-2:])
        sl2 = calculate_stop_lossForSell(df.iloc[-3:])
        sl = np.maximum(np.maximum(sl, sl1), sl2)  # 2% below the close price
        if abs(sl - JustCandle['Close']) / JustCandle['Close'] <= 1:
            sl = sl + (sl * 0.005)
            decimal_count = count_decimal_places(sl2)
            sl = round(float(sl2), decimal_count)
        print(sl)
        PlaceOrder("SELL", JustCandle, sl, s)

    elif buy == "yes":
        sl = calculate_stop_lossForBuy(df.iloc[-3: -2])
        sl1 = calculate_stop_lossForBuy(df.iloc[-2:])
        sl2 = calculate_stop_lossForBuy(df.iloc[-3:])
        sl = np.maximum(np.maximum(sl, sl1), sl2)  # 2% below the close price
        dt1 = client.get_all_tickers()
        latestPrice= find_dictionary_by_key(dt1, 'symbol', s['symbol'])
        if abs(sl - JustCandle['Close']) / JustCandle['Close'] <= 1 or latestPrice > sl:
            sl = sl - (sl * 0.005)
            decimal_count = count_decimal_places(sl2)
            sl = round(float(sl2), decimal_count)
        print(sl)
        PlaceOrder("BUY", JustCandle, sl, s)

    # if is_shooting_star(candle,previous_candles) :
    #     print("shooting star", s['symbol'])
    # if is_volume_greater_than_previous(candle, previous_candles):
    #     print("volume is greater than previous 5 candles", s['symbol'])
    #
    # if  is_outside_bollinger_upper(candle, candle['BollingerUpper']):
    #     print("candle is greater than upper bolinger ", s['symbol'])

    # if  is_volume_greater_than_average(candle, df):
    #     print("volume is greater than average r ", s['symbol'])


    # if boldifSati :
    #     print("bolinger diff i sufficent  ", s['symbol'], diff/candle['Close'] * 100)
    #
    # if (is_candle_red(candle, previous_candles)) :
    #     print("deep red candle ", s['symbol'])
    #
    # if  extreme_bullishCandle(candle) and\
    #             is_volume_greater_than_previous(candle, previous_candles) and \
    #             is_volume_greater_than_average(candle, candlestick_data) and \
    #             is_inside_bollinger_upper(candle, candle['BollingerUpper'],candle['BollingerLower']) and \
    #             is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):
    #             # candle['oi_change_last2_pc'] > 2:

def is_candle_red(candle,candlestick_data):
    average_candle_size = candlestick_data['High'].mean() - candlestick_data['Low'].mean()
    return (candle['Open'] > candle['Close'] and ( candle['High']) - candle['Low'] > average_candle_size)

def get_postion_details(script_code, pos_info):
    for pos in pos_info:
        if pos['symbol'] == script_code:
            return pos
    return None

def PlaceOrder(type, candle, sl,obj):
    logging.info('placing order')
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    pos_info = client.futures_position_information()
    pos = get_postion_details(obj['symbol'], pos_info)
    Entry_usdt = 150
    latestPrice = getLatestPrice(client, obj['symbol'])
    decimal_count = count_decimal_places(sl)
    if type == "BUY" and sl < latestPrice:
        newsl = latestPrice - (sl * 0.005)
        newsl = round(float(sl), decimal_count)
    elif type == "SELL" and sl > latestPrice:
        newsl = latestPrice + (sl * 0.005)
        newsl = round(float(sl), decimal_count)

    if float(pos['positionAmt']) == 0:
        print("here placing order")
        logging.info('placing order posiiton is 0')
        qty = (Entry_usdt / candle['Close'])
        qty = math.floor(qty * (10 ** obj['quantityPrecision'])) / (10 ** obj['quantityPrecision'])

        order = Wrapper_obj.create_market_order(obj['symbol'], type, qty, client)

        sltype = "SELL" if type == "BUY" else "BUY"
        xrp_positions = [pos for pos in client.futures_account()['positions'] if pos['symbol'] == obj['symbol']][0]

        sl_order =Wrapper_obj.create_stop_loss_market_order(pos['symbol'], sltype, abs(float(xrp_positions['positionAmt'])), sl, client)
        print("order -", order, "sl order " , sl_order)
    #
    # elif(float(pos['positionAmt']) <= 0):
    #     sltype = "SELL" if type == "BUY" else "BUY"
    #     xrp_positions = [pos for pos in client.futures_account()['positions'] if pos['symbol'] == obj['symbol']][0]
    #
    #     sl_order = Wrapper_obj.create_stop_loss_market_order(pos['symbol'], sltype, xrp_positions['positionAmt'], sl,
    #                                                          client)
# Function to check if the current candle is completed
def is_candle_completed(candle_dataframe,candleTime):
    current_time = pd.Timestamp.now()  # Get the current time
    last_data_point_time = candle_dataframe.index[-1]  # Get the timestamp of the last data point
    return current_time >= last_data_point_time + pd.Timedelta(minutes=candleTime)  # Check if current time is >= last data point + 1 minute


def is_candle_green(candle):
    return candle['Close'] > candle['Open']

def extreme_bullishCandle(candle):
    perce = abs(candle['Close'] - candle['High']) / candle['High']

    # Set the percentage-based threshold
    percentage_threshold = 0.004

    # Filter the rows where the percentage variance is small
    filtered_candles = perce <= percentage_threshold


def is_outside_bollinger_upper(candle, bollinger_upper):
    return candle['High'] > bollinger_upper and candle['Close'] < bollinger_upper

def is_outside_bollinger_lower(candle, bollinger_lower):
    return candle['Low'] < bollinger_lower

def is_inside_bollinger_upper(candle, bollinger_upper, bollinger_lower):
    return candle['Close'] < bollinger_upper and candle['Close'] > bollinger_lower

def is_volume_greater_than_previous(candle, previous_candles):
    current_volume = candle['Volume']
    previous_volumes = previous_candles['Volume'].tolist()

    return all(current_volume > volume for volume in previous_volumes)

def is_exhaustive_volume(candle, previous_candles):
    return candle['Volume'] > (previous_candles['Volume'].mean() *3)

def is_volume_greater_than_previousAvg(candle, previous_candles):
    return candle['Volume'] > previous_candles['Volume'].mean()


def is_volume_greater_than_average(candle, candles, average_period=40):
    return candle['Volume'] > (candles['Volume'].rolling(average_period).mean().iloc[-1] * 4.25)


def is_bollinger_difference_sufficient(bollinger_upper, bollinger_lower, threshold=0.023):
    return (bollinger_upper - bollinger_lower) >= bollinger_upper * threshold, (bollinger_upper - bollinger_lower)


def find_buy_signals(candlestick_data):
    buy_signals = []

    # Calculate Bollinger Bands
    candlestick_data['SMA'] = candlestick_data['Close'].rolling(window=20).mean()
    candlestick_data['STD'] = candlestick_data['Close'].rolling(window=20).std()
    candlestick_data['BollingerUpper'] = candlestick_data['SMA'] + 2 * candlestick_data['STD']
    candlestick_data['BollingerLower'] = candlestick_data['SMA'] - 2 * candlestick_data['STD']

    # Filter data for faster access
    previous_candles = candlestick_data.iloc[-5:]

    for idx, candle in candlestick_data.iterrows():
        if  extreme_bullishCandle(candle) and\
                is_volume_greater_than_previous(candle, previous_candles) and \
                is_volume_greater_than_average(candle, candlestick_data) and \
                is_inside_bollinger_upper(candle, candle['BollingerUpper'],candle['BollingerLower']) and \
                is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):
                # candle['oi_change_last2_pc'] > 2:
            if idx == len(candlestick_data) - 1 or  idx == len(candlestick_data) - 2:
                return "yes"
            buy_signals.append(candle)

    return buy_signals


def find_sell_signals(candlestick_data):
    sell_signals = []

    # Calculate Bollinger Bands
    candlestick_data['SMA'] = candlestick_data['Close'].rolling(window=20).mean()
    candlestick_data['STD'] = candlestick_data['Close'].rolling(window=20).std()
    candlestick_data['BollingerUpper'] = candlestick_data['SMA'] + 2 * candlestick_data['STD']
    candlestick_data['BollingerLower'] = candlestick_data['SMA'] - 2 * candlestick_data['STD']

    # Filter data for faster access
    previous_candles = candlestick_data.iloc[-5:]
    candle = candlestick_data.iloc[-1:]
    if is_shooting_star(candle,previous_candles) :
        print("shooting star", s['symbol'])


    # if ( is_shooting_star(candle,previous_candles) ) and\
    #         is_volume_greater_than_previous(candle, previous_candles) and \
    #         is_volume_greater_than_average(candle, candlestick_data) and \
    #         is_outside_bollinger_upper(candle, candle['BollingerUpper']) and \
    #         is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):
    #     print(idx, "----", len(candlestick_data))
    #     if idx == len(candlestick_data) - 1 or idx == len(candlestick_data) - 2 or idx == len(candlestick_data) - 3:
    #         return "yes"
    #
    #     sell_signals.append(candle)
    # elif (is_candle_red(candle, previous_candles)) and \
    #      is_volume_greater_than_previous(candle, previous_candles) and \
    #      is_volume_greater_than_average(candle, candlestick_data) and \
    #      is_outside_bollinger_upper(candle, candle['BollingerUpper']) and \
    #      is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):
    #     print(idx,"----", len(candlestick_data))
    #     if(idx == len(candlestick_data) - 1 or idx == len(candlestick_data) - 2) or idx == len(candlestick_data) - 3:
    #         return "yes"
    #
    #     sell_signals.append(candle)
    #
    #
    return sell_signals


def wait_for_5_minutes_since_last_run(last_run_time):
    while datetime.datetime.now() - last_run_time < datetime.timedelta(minutes=5):
        time.sleep(1)


def Scanner():
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    while True:
        try:
            dictionary_index = 0
            Wrapper_obj = Binance_Api_wrapper_generic()
            client = Wrapper_obj.get_client()

            current_datetime = datetime.datetime.now()
            # Update the time of the last run
            last_run_time = datetime.datetime.now()

            Scanned_all = []
            print("scanning date and time ",current_datetime)
            symbol_list = (Wrapper_obj.get_all_symbols_binance(client))
            for s in symbol_list['symbols']:
                try:
                    # Future_scan =  find_movement_based_on_time_frame(s, client, "future", Scanned_all,Wrapper_obj, '2023-07-29 17:25:00')
                    find_movement_based_on_time_frame(s, client, "future", Scanned_all, Wrapper_obj)

                except Exception as ex1:
                    print(s['symbol'])
                    print('Error creating batch: %s' % str(ex1),    print(s['symbol']))
                    import traceback
                    traceback.print_exc()

            analysis_time = datetime.datetime.now() - last_run_time
            # Wait for the remaining time (at least 2 minutes) before starting the next analysis
            remaining_time = datetime.timedelta(minutes=3) - analysis_time
            if remaining_time.total_seconds() > 0:
                time.sleep(remaining_time.total_seconds())

        except Exception as ex1:
            print('Error creating batch: %s' % str(ex1))
            time.sleep(100)

Scanner()

#         is_volume_greater_than_average(candle, candlestick_data) and \
#         is_outside_bollinger_upper(candle, candle['BollingerUpper']) and \
#         is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):

#
# if (sell_signals == "yes"):
#     sl = calculate_stop_lossForSell(df.iloc[-1:])
#     sl1 = calculate_stop_lossForSell(df.iloc[-2:])
#     sl2 = calculate_stop_lossForSell(df.iloc[-2:])
#     sl = np.maximum(np.maximum(sl,sl1) ,sl2) # 2% below the close price
#     # insert_scanned_data(datetime.datetime.now(), s['symbol'], "SELL", "SELL Signal bb", "CRYPTO",
#     #
#     #                     VWAP['higher_band'].iloc[-1],  sl[0], VWAP['vwap'].iloc[-1])
#     PlaceOrder("SELL", df.iloc[-1:], sl, s)
#
# if (buy_signals == "yes"):
#     sl = calculate_stop_lossForBuy(df.iloc[-1:])
#     sl1 = calculate_stop_lossForSell(df.iloc[-2:])
#     sl2 = calculate_stop_lossForSell(df.iloc[-2:])
#     sl = np.minimum(np.minimum(sl,sl1) ,sl2)
#     # insert_scanned_data(datetime.datetime.now(), s['symbol'], "BUY", "BUY Signal bb", "CRYPTO",
#     #                     VWAP['higher_band'].iloc[-1],  sl[0], VWAP['vwap'].iloc[-1])
#     PlaceOrder("BUY", df.iloc[-1:], sl, s)

# for idx, (signal_candle, signal_type) in enumerate(buy_signals):
#     print(f"Signal {idx + 1} ({signal_type}):")
#     print(signal_candle)
#     print("\n")
#     insert_scanned_data(datetime.now(), s['symbol'], "SELL", "BUY Signal", "CRYPTO",
#                         VWAP['higher_band'].iloc[-1], VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])
#
# # Output the Sell signals
# print("Sell signals:")
# for idx, (signal_candle, signal_type) in enumerate(sell_signals):
#     print(f"Signal {idx + 1} ({signal_type}):")
#     print(signal_candle)
#     print("\n")
#     insert_scanned_data(datetime.now(), s['symbol'], "SELL", "SELL Signal", "CRYPTO",
#                         VWAP['higher_band'].iloc[-1], VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])

# current_cl_index = len(five_minute.Close) - 1
# if five_minute['oi_change_last2_pc'].values[current_cl_index] > 2  :
#     insert_scanned_data(datetime.datetime.now(), s['symbol'], "TBD", "OI Change "+ five_minute['oi_change_last2_pc'].values[current_cl_index], "CRYPTO",
#                         VWAP['higher_band'].iloc[-1], VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])
#
# for idx, shooting_star in enumerate(sell_signals):
#     print(f"Pattern {idx + 1}:")
#     print(shooting_star)
#     print("\n")