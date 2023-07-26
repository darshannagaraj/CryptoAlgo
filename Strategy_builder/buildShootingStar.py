import datetime
import os
import pandas as pd
import numpy as np
import sys
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


def is_shooting_star(candle,candlestick_data) :
    #, average_candle_size):
    average_candle_size = candlestick_data['High'].mean() - candlestick_data['Low'].mean()

    body_size = abs(candle['Open'] - candle['Close'])
    upper_shadow = candle['High'] - max(candle['Open'], candle['Close'])
    lower_shadow = min(candle['Open'], candle['Close']) - candle['Low']

    return upper_shadow >= 2 * body_size and body_size >= lower_shadow * 2 and body_size > average_candle_size


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
    slprice =  candle['Close'] * 0.98
    closing_prices = candle['Close']
    decimal_count = [count_decimal_places(price) for price in closing_prices]
    sl_price = round(float(slprice), decimal_count[0])
    sl = np.minimum(candle['High'], sl_price)  # 2% below the close price
    return sl

def calculate_stop_lossForBuy(candle):
    slprice = candle['Close'] * 1.02
    closing_prices = candle['Close']
    decimal_count = [count_decimal_places(price) for price in closing_prices]
    sl_price = round(float(slprice), decimal_count[0])
    sl = np.minimum(candle['Low'], sl_price)  # 2% below the close price
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
def find_movement_based_on_time_frame(s,client,market_type,Scanned_all,wrapper_obj,drop_rows=0 ):
    atr_period = 10
    atr_multiplier = 3.0
    scanned_data = {}
    print(s['symbol'])
    five_minute = getminutedata(s['symbol'],client.KLINE_INTERVAL_5MINUTE,1, market_type,client)
    five_minute.drop(five_minute.tail(drop_rows).index,
            inplace=True)
    hist_data = client.futures_open_interest_hist(symbol=s['symbol'], period=client.KLINE_INTERVAL_5MINUTE,

    limit=25)
    # print("Shooting star patterns:")
    oi_df= Binance_api_helper.convert_hash_to_data_frame(hist_data)
    Binance_api_helper.oi_change(oi_df)
    Binance_api_helper.oi_change_candles(oi_df, 1)
    Binance_api_helper.oi_change_candles(oi_df, 2)
    # five_minute['oi_change_last2'] = oi_df['oi_change_last2']
    five_minute['oi_change_last2_pc'] = oi_df['oi_change_last2_pc']
    # shooting_stars = find_shooting_stars(five_minute)


    df = five_minute

    # Find Sell signals
    sell_signals = find_sell_signals(df)
    buy_signals = find_buy_signals(df)

    vwapStatus = find_symbols_close_to_vwap(five_minute)
    VWAP = calculate_vwap_bands(five_minute, num_std_dev=1)

    for idx, shooting_star in enumerate(sell_signals):
        print(f"Pattern {idx + 1}:")
        print(shooting_star)
        print("\n")
    print("Buy signals:")
    sl = calculate_stop_lossForSell(df.iloc[-1:])
    insert_scanned_data(datetime.datetime.now(), s['symbol'], "SELL", "SELL Signal bb", "CRYPTO",
                        VWAP['higher_band'].iloc[-1], sl[0], VWAP['vwap'].iloc[-1])
    PlaceOrder("SELL", df.iloc[-1:], sl[0], s)

    print ("I am here")
    if (sell_signals == "yes"):
        sl = calculate_stop_lossForSell(df.iloc[-1:])
        insert_scanned_data(datetime.datetime.now(), s['symbol'], "SELL", "SELL Signal bb", "CRYPTO",
                            VWAP['higher_band'].iloc[-1],  sl[0], VWAP['vwap'].iloc[-1])
        PlaceOrder("SELL", df.iloc[-1:], sl[0], s)

    if (buy_signals == "yes"):
        sl = calculate_stop_lossForBuy(df.iloc[-1:])
        insert_scanned_data(datetime.datetime.now(), s['symbol'], "BUY", "BUY Signal bb", "CRYPTO",
                            VWAP['higher_band'].iloc[-1],  sl[0], VWAP['vwap'].iloc[-1])
        PlaceOrder("BUY", df.iloc[-1:], sl[0], s)

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

    current_cl_index = len(five_minute.Close) - 1
    if five_minute['oi_change_last2_pc'].values[current_cl_index] > 2  :
        insert_scanned_data(datetime.datetime.now(), s['symbol'], "TBD", "OI Change "+ five_minute['oi_change_last2_pc'].values[current_cl_index], "CRYPTO",
                            VWAP['higher_band'].iloc[-1], VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])

    for idx, shooting_star in enumerate(sell_signals):
        print(f"Pattern {idx + 1}:")
        print(shooting_star)
        print("\n")

def is_candle_red(candle,candlestick_data):
    average_candle_size = candlestick_data['High'].mean() - candlestick_data['Low'].mean()
    return (candle['Open'] > candle['Close'] and ( candle['High']) - candle['Low'] > average_candle_size)

def get_postion_details(script_code, pos_info):
    for pos in pos_info:
        if pos['symbol'] == script_code:
            return pos
    return None

def PlaceOrder(type, candle, sl,obj):
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    pos_info = client.futures_position_information()
    pos = get_postion_details(obj['symbol'], pos_info)
    Entry_usdt = 150

    if float(pos['positionAmt']) == 0:
        qty = (Entry_usdt / candle['Close'])
        qty = (round(float((qty)), int( obj['quantityPrecision'])))
        order = Wrapper_obj.create_market_order(obj['symbol'], type, qty, client)
        sl_order =Wrapper_obj.create_stop_loss_market_order(pos['symbol'], type, pos['positionAmt'], sl, client)
        print("order -", order, "sl order " , sl_order)

def is_candle_green(candle):
    return candle['Close'] > candle['Open']

def extreme_bullishCandle(candle):
    perce = abs(candle['Close'] - candle['High']) / candle['High']

    # Set the percentage-based threshold
    percentage_threshold = 0.004

    # Filter the rows where the percentage variance is small
    filtered_candles = perce <= percentage_threshold


def is_outside_bollinger_upper(candle, bollinger_upper):
    return candle['Close'] > bollinger_upper


def is_inside_bollinger_upper(candle, bollinger_upper, bollinger_lower):
    return candle['Close'] < bollinger_upper and candle['Close'] > bollinger_lower

def is_volume_greater_than_previous(candle, previous_candles):
    current_volume = candle['Volume']
    previous_volumes = previous_candles['Volume'].tolist()

    return all(current_volume > volume for volume in previous_volumes)

def is_volume_greater_than_previousAvg(candle, previous_candles):
    return candle['Volume'] > previous_candles['Volume'].mean()


def is_volume_greater_than_average(candle, candles, average_period=40):
    return candle['Volume'] > candles['Volume'].rolling(average_period).mean().iloc[-1]


def is_bollinger_difference_sufficient(bollinger_upper, bollinger_lower, threshold=0.023):
    return (bollinger_upper - bollinger_lower) >= bollinger_upper * threshold


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

    for idx, candle in candlestick_data.iterrows():
        if ( is_shooting_star(candle,previous_candles) ) and\
                is_volume_greater_than_previous(candle, previous_candles) and \
                is_volume_greater_than_average(candle, candlestick_data) and \
                is_outside_bollinger_upper(candle, candle['BollingerUpper']) and \
                is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):
            if idx == len(candlestick_data) - 1 or  idx == len(candlestick_data) - 2:
                return "yes"

            sell_signals.append(candle)
        elif (is_candle_red(candle, previous_candles)) and \
             is_volume_greater_than_previous(candle, previous_candles) and \
             is_volume_greater_than_average(candle, candlestick_data) and \
             is_outside_bollinger_upper(candle, candle['BollingerUpper']) and \
             is_bollinger_difference_sufficient(candle['BollingerUpper'], candle['BollingerLower']):
            if(idx == len(candlestick_data) - 1 or idx == len(candlestick_data) - 2):
                return "yes"

            sell_signals.append(candle)
    return sell_signals



def Scanner():
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    while True:
        try:
            dictionary_index = 0
            Wrapper_obj = Binance_Api_wrapper_generic()
            client = Wrapper_obj.get_client()

            starttime = time.time()
            Scanned_all = []
            print(starttime)

            print("started scanning ")
            symbol_list = (Wrapper_obj.get_all_symbols_binance(client))
            for s in symbol_list['symbols']:
                try:
                    Future_scan =  find_movement_based_on_time_frame(s, client, "future", Scanned_all,Wrapper_obj)
                    if Future_scan != None:
                            db_string = Future_scan.split("||")[1].split("|")
                            postges_push_trade(s['symbol'], db_string[1], db_string[2],
                                               db_string[3], db_string[4], db_string[5], db_string[6], db_string[7], db_string[8],
                                           db_string[9], "OPEN_INREST")


                except Exception as ex1:
                    print('Error creating batch: %s' % str(ex1))

            print(" #{starttime} ended scanning ")
            print(starttime)
            time.sleep(300.0 - ((time.time() - starttime) % 60.0))
        except Exception as ex1:
            print('Error creating batch: %s' % str(ex1))
            time.sleep(100)

Scanner()