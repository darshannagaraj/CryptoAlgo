import datetime
import math
import time
import pandas as pd
import numpy as np

from binane_api import Binance_Api_wrapper_generic
from lib.genericCode import getminutedata, lonelyCandle, is_volume_less_than_average, TrainTrack, calculateVwap, \
    volumeGainers, panicBuy, vwapslope
from Data_loader.Binance_api import Binance_Api_wrapper_generic
from Data_loader.Binance_api_helper import Binance_api_helper

def get_postion_details(script_code, pos_info):
    for pos in pos_info:
        if pos['symbol'] == script_code:
            return pos
    return None
def getLatestPrice(client, symbol):
    dt1 = client.get_all_tickers()
    latestPrice = (find_dictionary_by_key(dt1, 'symbol', symbol))
    return float(latestPrice)

def find_dictionary_by_key(list_of_dicts, key, value):
    for dictionary in list_of_dicts:
        if dictionary.get(key) == value:
            return dictionary['price']
    return 0


def count_decimal_places(num):
    decimal_part = str(num).split('.')[-1]
    return len(decimal_part)



def PlaceOrder(type, candle, sl,obj):
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    pos_info = client.futures_position_information()
    pos = get_postion_details(obj['symbol'], pos_info)
    Entry_usdt = 150
    latestPrice = getLatestPrice(client, obj['symbol'])
    decimal_count = count_decimal_places(sl)
    if type == "BUY" and sl < latestPrice:
        newsl = latestPrice - (sl * 0.0225)
        newsl = round(float(sl), decimal_count)
    elif type == "SELL" and sl > latestPrice:
        newsl = latestPrice + (sl * 0.0225)
        newsl = round(float(sl), decimal_count)

    if float(pos['positionAmt']) == 0:
        print("here placing order")
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
    # elif(float(pos['positionAmt']) <= 0):
def classify_candle(row):
    if row['Close'] >= row['High'] - (row['High'] - row['Low']) * 0.1:  # within top 10% of the range
        return 'bullish'
    elif row['Close'] <= row['Low'] + (row['High'] - row['Low']) * 0.1:  # within bottom 10% of the range
        return 'bearish'
    else:
        return 'neutral'


def find_movement_based_on_time_frame(s,client,market_type, Scanned_all,wrapper_obj, drop_rows=0 ):


    df = getminutedata(s['symbol'],client.KLINE_INTERVAL_15MINUTE,10, market_type,client)
    df.drop(df.tail(drop_rows).index,
            inplace=True)
    find_InsideBarConditon(df)
#conditions 15mns candle, Volume Twice as Average
#and its the only candle with avergae volume greater then the volume for last 20 candles
# @next bar is inside bar and
def find_InsideBarConditon(df):
    # Calculate the spread for each candle
    import pandas as pd

    # Sample DataFrame creation
    # data = {
    #     'Time': ['2023-05-01 12:00', '2023-05-01 12:01', '2023-05-01 12:02', ...],
    #     'Open': [100, 105, 103, ...],
    #     'High': [110, 108, 107, ...],
    #     'Low': [95, 100, 101, ...],
    #     'Close': [105, 104, 103, ...],
    #     'Volume': [300, 600, 450, ...]
    # }
    # df = pd.DataFrame(data)

    # Assuming you already have a DataFrame named `df`

    # Define the conditions
    df['Spread'] = df['High'] - df['Low']
    average_30_spread = df['Spread'].rolling(window=30).mean()
    roll_max_volume = df['Volume'].rolling(window=12).max()
    roll_min_low = df['Low'].rolling(window=12).min()

    # Conditions for n-2 candle
    df['n2_High_Volume'] = (df['Volume'] == roll_max_volume) & (df['Volume'] > 2 * df['Volume'].mean())
    df['n2_Lowest_Low'] = df['Low'] == roll_min_low
    df['n2_Spread_Above_Avg'] = df['Spread'] > average_30_spread

    # Inside bar condition for n-1 candle
    df['n1_Is_Inside_Bar'] = (df['High'].shift(1) < df['High'].shift(2)) & (df['Low'].shift(1) > df['Low'].shift(2))

    # Combine conditions for n-2
    df['Conditions_n2'] = df['n2_High_Volume'] & df['n2_Lowest_Low'] & df['n2_Spread_Above_Avg']

    # Trade Initiation (nth candle crossing above n-1 high)
    df['Initiate_Trade'] = (df['Close'] > df['High'].shift(1)) & df['n1_Is_Inside_Bar'] & df['Conditions_n2'].shift(1)
    df['Is_Mother_Bar'] = (df['High'] > df['High'].shift(1)) & (df['Low'] < df['Low'].shift(1))

    # Determine if the mother bar is negative or positive
    df['Candle_Type'] = 'Not a Mother Bar'  # Default value
    df.loc[df['Is_Mother_Bar'] & (df['Close'] < df['Open']), 'Candle_Type'] = 'Negative Mother Bar'
    df.loc[df['Is_Mother_Bar'] & (df['Close'] >= df['Open']), 'Candle_Type'] = 'Positive Mother Bar'

    N = 12  # Example: Look back over the last 12 candles

    # Calculate if the current high is the highest of the last N candles
    df['Highest_High_Last_N'] = df['High'] == df['High'].rolling(window=N).max()

    # You can also print out or analyze only those times when the current high is the highest of the last N candles
    highest_high_times = df[df['Highest_High_Last_N']].index
    print(highest_high_times)


    # Filter rows where trade can be initiated
    trade_times = df[df['Initiate_Trade']].index
    print(trade_times)


import pandas as pd

import pandas as pd


def update_btc_trend(df, high_col='high_btc', low_col='low_btc', window=30):
    """
    Update the DataFrame to include a rolling 30-minute SMA of the average of high and low BTC prices and determine the trend.

    Args:
    df (DataFrame): DataFrame containing the BTC price data.
    high_col (str): The name of the column in df that contains BTC high prices.
    low_col (str): The name of the column in df that contains BTC low prices.
    window (int): The window size in minutes for the rolling average.

    Returns:
    DataFrame: The original DataFrame with added columns for 'btc_avg_price', 'btc_sma', and 'btc_trend'.
    """
    # Calculate the average of high and low prices
    df['btc_avg_price'] = (df[high_col] + df[low_col]) / 2

    # Calculate the rolling average of the composite price
    df['btc_sma'] = df['btc_avg_price'].rolling(window=window, min_periods=1).mean()

    # Determine the trend based on the average price relative to the SMA
    df['btc_trend'] = 'neutral'  # Default to neutral
    df.loc[df['btc_avg_price'] > df['btc_sma'], 'btc_trend'] = 'up'
    df.loc[df['btc_avg_price'] < df['btc_sma'], 'btc_trend'] = 'down'

    return df

def find_trade_signals(df):
    # Calculate the percent change and the rolling volume average
    df['percent_change'] = (df['Close'] - df['Open']) / df['Open'] * 100
    df['rolling_volume'] = df['Volume'].rolling(window=12).mean()  # 8 periods of 15 mins each cover 2 hours
    df['candle_range'] = df['High'] - df['Low']
    df['close_to_high'] = (df['High'] - df['Close']) / df['High']
    df['close_to_low'] = (df['Close'] - df['Low']) / df['Low']
    previousandle = df.iloc[-2: -1]
    if (abs(previousandle['percent_change'][0]) >= 1.7) & (abs(previousandle['percent_change'][0]) <= 2.6) :
        if previousandle['Volume'][0] >= 5.5 * previousandle['rolling_volume'][0]:
            if previousandle['close_to_high'][0] < previousandle['close_to_low'][0]:
                print("sell")
            else:
                print("buy")

            #below code is for backtesting
    # Filtering criteria for candles
    # qualifying_candles = df[
    #     (abs(df['percent_change']) >= 1.7) & (abs(df['percent_change']) <= 2.6) &
    #     (df['Volume'] >= 5.5 * df['rolling_volume'])]
    #
    # # Apply buy/sell logic to the filtered DataFrame
    # qualifying_candles['trade_signal'] = qualifying_candles.apply(
    #     lambda row: 'buy' if row['close_to_high'] < row['close_to_low'] else 'sell', axis=1
    # )
    #
    # candle_size = qualifying_candles['candle_range']
    # qualifying_candles['stop_loss'] = np.where(
    #     qualifying_candles['trade_signal'] == 'buy', qualifying_candles['Low'] - 0.8 * candle_size,
    #     np.where(qualifying_candles['trade_signal'] == 'sell', qualifying_candles['High'] + 0.8 * candle_size, None)
    # )

    # return qualifying_candles


def backtest_trades(df):
    initial_capital = 10000  # Starting capital, for example, $10,000
    capital = initial_capital
    results = []

    for index, row in df.iterrows():
        if row['trade_signal'] != 'None':
            entry_price = row['Close']
            stop_loss = row['stop_loss']
            trade_risk = abs(entry_price - stop_loss)
            trade_size = capital / trade_risk  # Adjust this calculation based on your risk management

            if row['trade_signal'] == 'Buy':
                # Example: exit at a fixed profit target or stop loss
                profit_target = entry_price + trade_risk * 2  # Example target
                exit_price = min(profit_target, stop_loss)  # Simplified exit logic
            elif row['trade_signal'] == 'Sell':
                profit_target = entry_price - trade_risk * 2
                exit_price = max(profit_target, stop_loss)

            profit_loss = (entry_price - exit_price) * trade_size
            capital += profit_loss
            results.append((index, row['trade_signal'], entry_price, exit_price, profit_loss, capital))

    result_df = pd.DataFrame(results, columns=['DateTime', 'Trade Signal', 'Entry Price', 'Exit Price', 'Profit/Loss', 'Capital'])
    return result_df




def find_movement_based_on_time_framefinal(s,client,market_type, Scanned_all,wrapper_obj, drop_rows=0 ):
    if (s['symbol'] == "BTCUSDT"):
            return
    if (s['symbol'] == "ETHUSDT"):
        return
    df_15m = getminutedata(s['symbol'],client.KLINE_INTERVAL_15MINUTE,10, market_type,client)
    df_15m.drop(df_15m.tail(drop_rows).index)
    filtered_trades = find_trade_signals(df_15m)
    # print(filtered_trades[['percent_change', 'Volume', 'rolling_volume', 'trade_signal', 'stop_loss']])
    # backtest_results = backtest_trades(filtered_trades)
    # print(backtest_results)


def find_movement_based_on_time_frame1(s,client,market_type, Scanned_all,wrapper_obj, drop_rows=0 ):

    if (s['symbol'] == "BTCUSDT" ) :
        BTC_df =getminutedata(s['symbol'],client.KLINE_INTERVAL_3MINUTE,28, market_type,client)
        BTC_df = update_btc_trend(BTC_df, "High", "Low")
        return

    if (s['symbol'] == "ETHUSDT"):
        ETH_df = getminutedata(s['symbol'], client.KLINE_INTERVAL_3MINUTE, 28, market_type, client)
        ETH_df = update_btc_trend(ETH_df, "High", "Low")
        return


    df_3m = getminutedata(s['symbol'],client.KLINE_INTERVAL_3MINUTE,50, market_type,client)
    df_3m.drop(df_3m.tail(drop_rows).index,
            inplace=True)
    df_1h = getminutedata(s['symbol'],client.KLINE_INTERVAL_1HOUR,50, market_type,client)
    # Calculate the rolling mean of the last 10 hours' volume and use it as the threshold

    df_1h['10_hour_volume_avg'] = df_1h['Volume'].rolling(window=10).mean()
    # volume_thresholds = df_1h['10_hour_volume_avg'] * 0.12 \
    #     if s['symbol'] in ['BTCUSDT', 'ETHUSDT', 'BCHUSDT']\
    #     else df_1h['10_hour_volume_avg'] * 0.2
    hist_data = client.futures_open_interest_hist(symbol=s['symbol'], period=client.KLINE_INTERVAL_1DAY,
                                                  limit=50)
    oi_df = Binance_api_helper.convert_hash_to_data_frame(hist_data)
    Binance_api_helper.oi_change(oi_df)
    Binance_api_helper.oi_change_candles(oi_df, 5)
    oi_df['Prev_oiChangePc'] = oi_df['oi_change_last5_pc'].shift(1)
    oi_df['Prev_Prev_oiChangePc'] = oi_df['oi_change_last5_pc'].shift(2)

    oi_df['Date'] = oi_df.index.date
    df_3m['Date'] = df_3m.index.date

    # Convert 'Date' to datetime if it's not already
    df_3m['Date'] = pd.to_datetime(df_3m['Date'])
    oi_df['Date'] = pd.to_datetime(oi_df['Date'])
    df_3m = df_3m.join(oi_df, how='left', on='Date', rsuffix='_new')

    if s['symbol'] in ['BTCUSDT', 'ETHUSDT', 'BCHUSDT']:

        volume_thresholds =df_1h['10_hour_volume_avg'] * float(0.13)
        initalvol = 3500
    else:
        volume_thresholds = df_1h['10_hour_volume_avg'] * float(0.2)
        initalvol = 10000


    # if s['symbol'] in ['BTCUSDT', 'ETHUSDT', 'BCHUSDT']:
    #     volume_thresholds = df_1h['Volume'] * float(0.12)
    #     initalvol = 3500
    # else:
    #     volume_thresholds = df_1h['Volume'] * float(0.2)
    #     initalvol = 10000
    # Reindex and align 3-minute data with hourly volume thresholds
    df_3m['hourly_volume_threshold'] = volume_thresholds.reindex(df_3m.index, method='ffill')

    # Identify high volume candles
    df_3m['is_high_volume'] = (df_3m['Volume'] >= df_3m['hourly_volume_threshold']) & (df_3m['Volume'] > initalvol)
    df_3m['percentage_change'] = abs((df_3m['High'] - df_3m['Low']) / df_3m['Low']) * 100
    # Define the trading conditions based on OI percentage changes
    df_3m['ConsiderTrade'] = (df_3m['Prev_oiChangePc'] > 12) | (df_3m['Prev_Prev_oiChangePc'] > 14 | (df_3m['Prev_Prev_oiChangePc'] + df_3m['Prev_oiChangePc'] > 22))
    print()
    df_3m['classification'] = df_3m.apply(classify_candle, axis=1)
    # Applying target and stop-loss function
    targets_and_stops = df_3m.apply(set_target_and_sl, axis=1)
    df_3m = df_3m.join(targets_and_stops)  # Joining results back to the main DataFrame ensures index alignment



    # Filter for significant high volume and non-neutral classification candles

    significant_candles = df_3m[df_3m['is_high_volume'] & df_3m['ConsiderTrade'] & (df_3m['classification'] != 'neutral')]
    # significant_candles['btc_sma'] = BTC_df['price_btc'].rolling(window=40, min_periods=1).mean()
    # significant_candles['eth_sma'] = ETH_df['price_eth'].rolling(window=40, min_periods=1).mean()
    #
    # # You might also want to calculate the slope of these averages to understand the direction of the trend
    # significant_candles['btc_trend'] = BTC_df['btc_sma'].diff().fillna(0)
    # significant_candles['eth_trend'] = ETH_df['eth_sma'].diff().fillna(0)
    # # Define trend conditions, for example, a positive trend slope indicating upward momentum
    # significant_candles['btc_positive_trend'] = BTC_df['btc_trend'] > 0
    # significant_candles['eth_positive_trend'] = ETH_df['eth_trend'] > 0
    #
    # # Filter candles where both BTC and ETH have a positive trend
    # positive_trend_candles = significant_candles[(significant_candles['btc_positive_trend']) &
    #                                              (significant_candles['eth_positive_trend'])]


    if significant_candles.empty:
        print("No significant candles found. Exiting the function.")
        return  # Or handle this case appropriately depending on your application logic

    # Backtesting each significant candle
    results = []
    for idx, row in significant_candles.iterrows():
        future_data = df_3m.loc[idx + pd.Timedelta(minutes=3): idx + pd.Timedelta(hours=10)]
        result = backtest(row, future_data)
        results.append(result)



    if results:
        results_df = pd.DataFrame(results, columns=['hit_target', 'hit_sl', 'outcome'], index=significant_candles.index)
        significant_candles = pd.concat([significant_candles, results_df], axis=1)
    if not significant_candles.empty:
        header = " | ".join([f"{col}" for col in significant_candles.columns])
        print(header)
        print("-" * len(header))  # print a divider
        for index, row in significant_candles.iterrows():
            row_str = " | ".join([f"{str(item)}" for item in row])
            print(s['symbol'], (index), row_str)
    else:
        print("No significant candles to display.")

    # if 'target' in significant_candles.columns and 'stop_loss' in significant_candles.columns:
    #     print((s['symbol']))
    #     for index, row in significant_candles.iterrows():
    #         # Printing each row's desired columns
    #         print(f"Classification: {row['classification']}, Target: {row['target']}, Stop Loss: {row['stop_loss']}, "
    #               f"Hit Target: {row['hit_target']}, Hit SL: {row['hit_sl']}, Outcome: {row['outcome']}")
    #
    #         # You can also add conditional logic
    #         if row['outcome'] == 'profit':
    #             print("This candle resulted in a profit.")
    #         elif row['outcome'] == 'loss':
    #             print("This candle resulted in a loss.")
    #         else:
    #             print("No significant outcome.")
    # else:
    #     print("Error: Target and/or Stop Loss columns missing.")


def PlaceOrder(type, candle, sl,obj):
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    pos_info = client.futures_position_information()
    pos = get_postion_details(obj['symbol'], pos_info)
    Entry_usdt = 150
    latestPrice = getLatestPrice(client, obj['symbol'])
    decimal_count = count_decimal_places(sl)
    latestPrice1 = latestPrice
    if type == "BUY" and sl < latestPrice and sl < (latestPrice - (sl * 0.0225)):
        newsl = latestPrice - (sl * 0.0225)
        newsl = round(float(sl), decimal_count)
    elif type == "SELL" and sl > latestPrice and sl > (latestPrice + (sl * 0.0225)):
        newsl = latestPrice + (sl * 0.0225)
        newsl = round(float(sl), decimal_count)

    if float(pos['positionAmt']) == 0:
        print("here placing order")
        logging.info('placing order posiiton is 0')
        qty = (Entry_usdt / candle['Close'])
        qty = math.floor(qty * (10 ** obj['quantityPrecision'])) / (10 ** obj['quantityPrecision'])

        order = Wrapper_obj.create_market_order(obj['symbol'], type, qty, client)

        sltype = "SELL" if type == "BUY" else "BUY"
        xrp_positions = [pos for pos in client.futures_account()['positions'] if pos['symbol'] == obj['symbol']][0]
        try:
            sl_order = Wrapper_obj.create_stop_loss_market_order(pos['symbol'], sltype,
                                                                 abs(float(xrp_positions['positionAmt'])), sl, client)
        except Exception as ex1:
            if "Order would immediately trigger" in ex1:
                if type == "BUY" and sl < latestPrice and sl < (latestPrice - (sl * 0.0225)):
                    newsl = latestPrice - (sl * 0.0225)
                    newsl = round(float(sl), decimal_count)
                elif type == "SELL" and sl > latestPrice and sl > (latestPrice + (sl * 0.0225)):
                    newsl = latestPrice + (sl * 0.0225)
                    newsl = round(float(sl), decimal_count)

                sl_order = Wrapper_obj.create_stop_loss_market_order(pos['symbol'], sltype,
                                                                 abs(float(xrp_positions['positionAmt'])), sl, client)


# Backtesting to check if target or SL is hit
def backtest(row, future_data):
    if row['classification'] == 'bullish':
        hit_target = (future_data['High'] >= row['target']).any()
        hit_sl = (future_data['Low'] <= row['stop_loss']).any()
    else:
        hit_target = (future_data['Low'] <= row['target']).any()
        hit_sl = (future_data['High'] >= row['stop_loss']).any()
    outcome = 'profit' if hit_target and not hit_sl else 'loss' if hit_sl else 'no result'
    return pd.Series([hit_target, hit_sl, outcome])


def set_target_and_sl(row):
    candle_size = row['High'] - row['Low']
    # Calculating target based on classification
    target = row['Close'] + 4 * candle_size if row['classification'] == 'bullish' else row['Close'] - 4 * candle_size
    # Setting stop loss based on classification for completeness (assuming bearish SL should also be set)
    sl = row['High'] if row['classification'] == 'bearish' else row['Low']
    # Return a Series with index names that clearly define target and stop_loss
    return pd.Series({'target': target, 'stop_loss': sl})


BTC_df=None
ETH_df =None
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
                    print(s['symbol'])
                    # Future_scan =  find_movement_based_on_time_frame(s, client, "future", Scanned_all,Wrapper_obj, '2023-07-29 17:25:00')
                    # find_movement_based_on_time_frame1(s, client, "future", Scanned_all, Wrapper_obj)
                    find_movement_based_on_time_framefinal(s, client, "future", Scanned_all, Wrapper_obj)
                except Exception as ex1:
                    print(s['symbol'])
                    print('Error creating batch: %s' % str(ex1),    print(s['symbol']))
                    import traceback
                    traceback.print_exc()

            analysis_time = datetime.datetime.now() - last_run_time
            # Wait for the remaining time (at least 2 minutes) before starting the next analysis
            remaining_time = datetime.timedelta(minutes=14) - analysis_time
            if remaining_time.total_seconds() > 0:
                time.sleep(remaining_time.total_seconds())

        except Exception as ex1:
            print('Error creating batch: %s' % str(ex1))
            time.sleep(100)

Scanner()