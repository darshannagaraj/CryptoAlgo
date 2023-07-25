
import os
import pandas as pd
from binance.client import Client
from Data_loader.Binance_api import Binance_Api_wrapper_generic
from Data_loader.Binance_api_helper import Binance_api_helper
from Data_loader.postgres_data_handler import postges_push_trade
import btalib
from binance.enums import FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, HistoricalKlinesType
import time
import datetime

import psycopg2
from datetime import datetime


def find_symbols_close_to_vwap(df, threshold_lower=0.8, threshold_higher=1.2):
    # Calculate VWAP bands using 1 standard deviation
    df = calculate_vwap_bands(df, num_std_dev=1)

    # Filter symbols where the price is within the specified threshold of VWAP
    # Check if the price is within the specified threshold of VWAP
    price = df['Close'].iloc[-1]
    lower_band = df['lower_band'].iloc[-1]
    higher_band = df['higher_band'].iloc[-1]
    return lower_band <= price <= higher_band

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

def CurrentDateTime():
    # Get the current date and time
    current_datetime = datetime.datetime.now()
    # Format the date and time as a string
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    print(formatted_datetime)

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

def get_plus_minus_volume_count_previous_candles(candle_count, df):
    positive_vol=0
    negative_vol=0
    i = len(df.Close.values)-1
    i=i-candle_count
    count =0
    while (count < candle_count):
        count = count + 1
        i=i+1
        if df.Close.values[i] > df.Open.values[i] :
           positive_vol = positive_vol + df.Volume.values[i]
        else:
           negative_vol =  negative_vol + df.Volume.values[i]
    if positive_vol > negative_vol:
        an = "Postivie_vol" + str((positive_vol/negative_vol)) + "positive vol" +str(positive_vol) +  "negative vol" +str(negative_vol)
    else:
        an = "Negative_vol" + str(negative_vol/positive_vol)+ "positive vol" +str(positive_vol) +  "positive vol" +str(negative_vol)

    return an


def find_OIChange_based_on_time_frame(s,client,market_type,Scanned_all,wrapper_obj,drop_rows=0 ):
    atr_period = 10
    atr_multiplier = 3.0
    scanned_data = {}
    print("scanning - ", s['symbol'])
    five_minute = getminutedata(s['symbol'],client.KLINE_INTERVAL_5MINUTE,3, market_type,client)
    five_minute.drop(five_minute.tail(drop_rows).index,
            inplace=True)
    hist_data = client.futures_open_interest_hist(symbol=s['symbol'], period=client.KLINE_INTERVAL_5MINUTE,
                                                  limit=63)
    hist_data_15minutes = client.futures_open_interest_hist(symbol=s['symbol'], period=client.KLINE_INTERVAL_15MINUTE,
                                                  limit=50)

    oi_df= Binance_api_helper.convert_hash_to_data_frame(hist_data)

    oi_df_15= Binance_api_helper.convert_hash_to_data_frame(hist_data_15minutes)

    Binance_api_helper.oi_change(oi_df)
    vwapStatus = find_symbols_close_to_vwap(five_minute)
    VWAP = calculate_vwap_bands(five_minute, num_std_dev=1)
    Binance_api_helper.oi_change_candles(oi_df, 12)
    Binance_api_helper.oi_change_candles(oi_df, 1)
    Binance_api_helper.oi_change_candles(oi_df, 6)
    Binance_api_helper.oi_change_candles(oi_df, 24)
    Binance_api_helper.oi_change_candles(oi_df, 61)
    Binance_api_helper.oi_change_candles(oi_df, 61)
    Binance_api_helper.oi_change(oi_df_15)
    Binance_api_helper.oi_change_candles(oi_df_15, 4) # 2 hours
    Binance_api_helper.oi_change_candles(oi_df_15, 8)  # 4 hours
    Binance_api_helper.oi_change_candles(oi_df_15, 16)  # 8 hours
    #
    # five_minute['oi_change_last2Hours'] = oi_df_15['oi_change_last4']
    # five_minute['oi_change_last2Hours_pc'] = oi_df_15['oi_change_last4_pc']
    # five_minute['oi_change_last4Hours'] = oi_df_15['oi_change_last8']
    # five_minute['oi_change_last4Hours_pc'] = oi_df_15['oi_change_last8_pc']
    # five_minute['oi_change_last8Hours'] = oi_df_15['oi_change_last16']
    # five_minute['oi_change_last8Hours_pc'] = oi_df_15['oi_change_last16_pc']
    #
    #
    # five_minute['oi_change_last24'] = oi_df['oi_change_last24']
    # five_minute['oi_change_last24_pc'] = oi_df['oi_change_last24_pc']
    #
    # five_minute['oi_change_last12'] =  oi_df['oi_change_last12']
    # five_minute['oi_change_last12_pc'] = oi_df['oi_change_last12_pc']


    five_minute['oi_change_last6'] = oi_df['oi_change_last6']
    five_minute['oi_change_last6_pc'] = oi_df['oi_change_last6_pc']
    #
    # five_minute['oi_change_last61'] = oi_df['oi_change_last61']
    # five_minute['oi_change_last61_pc'] = oi_df['oi_change_last61_pc']
    five_minute['oi_change_last1'] = oi_df['oi_change_last1']
    five_minute['oi_change_last1_pc'] = oi_df['oi_change_last1_pc']
    # insert_scanned_data( datetime.now() , s['symbol'], "Buy", "VWAP above OI 10Pc", "CRYPTO",
    #                     VWAP['higher_band'].iloc[-1], VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])
    #
    current_cl_index = len(five_minute.Close) - 1
    if five_minute['oi_change_last1_pc'].values[current_cl_index ] > 4 and vwapStatus == True :
        print("OI and Vwap change matched ")
    if five_minute['oi_change_last6_pc'].values[current_cl_index] > 8 and vwapStatus == True:
        print("OI and Vwap change matched retracement  ")
        insert_scanned_data(datetime.now(),s['symbol'], "Buy", "VWAP above OI 10Pc", "CRYPTO", VWAP['higher_band'].iloc[-1],  VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])
    elif five_minute['oi_change_last6_pc'].values[current_cl_index] > 8 and vwapStatus == False:
        insert_scanned_data(datetime.now(), s['symbol'], "SELL", "VWAP BELOW OI 10Pc", "CRYPTO",
                            VWAP['higher_band'].iloc[-1], VWAP['lower_band'].iloc[-1], VWAP['vwap'].iloc[-1])

    #
    if five_minute['oi_change_last12_pc'].values[current_cl_index] > 10 or five_minute['oi_change_last24_pc'].values[current_cl_index] > 10 :
        print("OI change in Symbol------------------1 hour 5 minute candels, symbol,60,24,12,6  " + s['symbol'] ,  five_minute['oi_change_last61_pc'].values[current_cl_index],  five_minute['oi_change_last24_pc'].values[current_cl_index], five_minute['oi_change_last12_pc'].values[current_cl_index] , five_minute['oi_change_last6_pc'].values[current_cl_index])

    # size1 = len(five_minute['oi_change_last2Hours_pc'].dropna())
    #
    # # Iterate over rows in reverse order
    # for row in range(five_minute.shape[0] - 1, -1, -1):
    #     # Iterate over columns in reverse order
    #     for col in range(five_minute.shape[1] - 1, -1, -1):
    #         if pd.notna(five_minute.iloc[row, col]):
    #             # print(f"Non-NaN value found at row {row}, column {col}")
    #             if five_minute['oi_change_last2Hours_pc'].values[row-1] > 20 or five_minute['oi_change_last8Hours_pc'].values[row-1] > 20:
    #                 print("OI change in Symbol------------------ hours, symbol,2,4,8  " + s['symbol'],
    #                      five_minute['oi_change_last2Hours_pc'].values[row-1],
    #                       five_minute['oi_change_last4Hours_pc'].values[row-1],
    #                       five_minute['oi_change_last8Hours_pc'].values[row-1])
    #                 break



    #
    # size1= (five_minute['oi_change_last2Hours_pc'].close) -1
    # if five_minute['oi_change_last2Hours_pc'].values[size1] > 10 or \
    #         five_minute['oi_change_last24_pc'].values[
    #             size1] > 10:
    #     print("OI change in Symbol------------------ hours, symbol,2,4,8  " + s['symbol'],
    #           five_minute['oi_change_last2Hours_pc'].values[size1],
    #           five_minute['oi_change_last4Hours_pc'].values[size1],
    #           five_minute['oi_change_last8Hours_pc'].values[size1])
        # five_minute['oi_change_last3'] = oi_df['oi_change_last3']
    # five_minute['oi_change_last3_pc'] = oi_df['oi_change_last3_pc']
    # five_minute['ema_5'] = btalib.ema(five_minute.Close, period=5).df
    # five_minute['sma_20'] = btalib.sma(five_minute.Close, period=20).df
    # five_minute['sma_40'] = btalib.sma((five_minute.High + five_minute.Low)/2 , period=40).df
    # five_minute['sma-volume_20'] = btalib.sma(five_minute.Volume, period=20).df
    # five_minute['sma-volume_3'] = btalib.sma(five_minute.Volume, period=3).df
    # current_cl_index = len(five_minute.Close)-1
    # five_minute['sma-volume_40'] = btalib.sma(five_minute.Volume, period=40).df
    # diff_sma_pre_pc  = (abs(five_minute['sma_20'].values[current_cl_index-2] - five_minute['sma_40'].values[current_cl_index-2])/five_minute['sma_20'].values[current_cl_index-2] ) * 100
    # diff_sma_price_pc = (abs(five_minute['sma_20'].values[current_cl_index-2] - five_minute.Close.values[current_cl_index-2])/ five_minute.Close.values[current_cl_index-2]) * 100


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
            # print(CurrentDateTime())
            print("started scanning ")
            symbol_list = (Wrapper_obj.get_all_symbols_binance(client))
            for s in symbol_list['symbols']:
                try:
                    Future_scan =  find_OIChange_based_on_time_frame(s, client, "future", Scanned_all,Wrapper_obj)

                except Exception as ex1:
                    print('Error creating batch: %s' % str(ex1))

            print(" #{starttime} ended scanning ")
            print(starttime)
            time.sleep(60.0 - ((time.time() - starttime) % 60.0))
        except Exception as ex1:
            print('Error creating batch: %s' % str(ex1))
            time.sleep(100)

Scanner()