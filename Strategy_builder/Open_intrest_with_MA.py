import os
import pandas as pd
from binance.client import Client
from Data_loader.Binance_api import Binance_Api_wrapper_generic
from Data_loader.Binance_api_helper import Binance_api_helper
from Data_loader.postgres_data_handler import postges_push_trade
import btalib
from binance.enums import FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, HistoricalKlinesType
import time


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

def sell_text(client, market_type, s,five_minute,current_cl_index):
    avg = five_minute.High.values[current_cl_index] - five_minute.Low.values[current_cl_index]
    minutedata = getminutedata(s['symbol'], client.KLINE_INTERVAL_1MINUTE, 1, market_type)
    previous_index = current_cl_index - 2
    times_candle_size = five_minute['avg_pips'].values[current_cl_index] / avg
    low_clse = five_minute.Low.values[current_cl_index] - five_minute.High.values[current_cl_index]
    res = get_plus_minus_volume_count_previous_candles(6, minutedata)
    change_dif = (five_minute['sma_20'][previous_index] - five_minute['sma_40'][previous_index]) / \
                 five_minute['sma_20'][
                     previous_index] * 100
    send_text = "SELL  --" + s['symbol'] + "\n price change is > " + str(
        times_candle_size) + "\n low to close diff value " + str(
        low_clse) + "\n Change_dif % (sma )" + str(change_dif)
    send_text = send_text + "volume_condion" + res + "0"
    sl = (five_minute.Low.values[current_cl_index]) + (avg * 0.8)
    dec = len(str(five_minute.Low.values[current_cl_index]).split(".")[1])
    sl_price = round(float(sl), dec)
    send_text = send_text + "\n Sell Price " + str(
        five_minute.Low.values[current_cl_index]) + "\nSL price" + str(sl_price)
    print(" sell " + s['symbol'])
    ret = s['symbol'] + "|" + "SELL" + "|" + str(five_minute.Low.values[current_cl_index]) + "|" + str(
        five_minute.Low.values[current_cl_index])
    ret = ret + "|" + str(five_minute.High.values[current_cl_index]) + "|" + str(sl_price) + "|" + str(
        five_minute.index[current_cl_index]) + "|" + str(s['quantityPrecision'])
    ret = ret + "|" + str(five_minute.Open.values[current_cl_index]) + "|" + str(
        five_minute.Close.values[current_cl_index])
    return (send_text + "||" + ret)

def buy_text(client, market_type, s,five_minute,current_cl_index):
    avg = five_minute.High.values[current_cl_index] - five_minute.Low.values[current_cl_index]
    five_minute['avg_candle_size'] = avg_candle_size_calculate(five_minute)
    five_minute['avg_pips'] = btalib.sma(five_minute['avg_candle_size'], period=100).df
    times_candle_size = five_minute['avg_pips'].values[current_cl_index] / avg
    high_clse = five_minute.High.values[current_cl_index] - five_minute.Close.values[current_cl_index]
    low_clse = five_minute.Low.values[current_cl_index] - five_minute.High.values[current_cl_index]
    previous_index = current_cl_index - 2
    change_dif = (five_minute['sma_20'][previous_index] - five_minute['sma_40'][previous_index]) / \
                 five_minute['sma_20'][
                     previous_index] * 100
    minutedata = getminutedata(s['symbol'], client.KLINE_INTERVAL_1MINUTE, 1, market_type, client)
    res = get_plus_minus_volume_count_previous_candles(6, minutedata)
    send_text = "BUY -- " + s['symbol'] + "\nprice change is > " + str(
        times_candle_size) + "\n High to close diff value " + str(
        high_clse) + "\n Change_dif % (sma)" + str(change_dif)
    send_text = send_text + "\nvolume_condion" + "0"
    sl = (five_minute.High.values[current_cl_index]) - (avg * 0.8)
    dec = len(str(five_minute.High.values[current_cl_index]).split(".")[1])
    sl_price = round(float(sl), dec)
    ret = s['symbol'] + "|" + "BUY" + "|" + str(five_minute.High.values[current_cl_index]) + "|" + str(
        five_minute.Low.values[current_cl_index])
    ret = ret + "|" + str(five_minute.High.values[current_cl_index]) + "|" + str(sl_price) + "|" + str(
        five_minute.index[current_cl_index]) + "|" + str(s['quantityPrecision'])
    ret = ret + "|" + str(five_minute.Open.values[current_cl_index]) + "|" + str(
        five_minute.Close.values[current_cl_index])
    return (send_text + "||" + ret)

def cross_price_over_ema(df):
    if (df.tail(1)['Close'].values[0] > df.tail(1)['ema_5'].values[0] and  df.tail(1)['ema_5'].values[0] >  df.tail(1)['sma_20'].values[0] ):
        return "buy"
    elif  (df.tail(1)['Close'].values[0] < df.tail(1)['ema_5'].values[0] and  df.tail(1)['ema_5'].values[0] <  df.tail(1)['sma_20'].values[0]):
        return "sell"


def cross_over(df):

    if ( df.tail(1)['ema_5'].values[0] >  df.tail(1)['sma_20'].values[0] and df.tail(2)['ema_5'].values[0] <  df.tail(3)['sma_20'].values[0]  ):
        return "buy"
    elif  ( df.tail(1)['ema_5'].values[0] <  df.tail(1)['sma_20'].values[0] and df.tail(2)['ema_5'].values[0] > df.tail(3)['sma_20'].values[0] ):
        return "sell"


def find_movement_based_on_time_frame(s,client,market_type,Scanned_all,wrapper_obj,drop_rows=0 ):
    atr_period = 10
    atr_multiplier = 3.0
    scanned_data = {}

    five_minute = getminutedata(s['symbol'],client.KLINE_INTERVAL_5MINUTE,3, market_type,client)
    five_minute.drop(five_minute.tail(drop_rows).index,
            inplace=True)
    hist_data = client.futures_open_interest_hist(symbol=s['symbol'], period=client.KLINE_INTERVAL_1DAY,
                                                  limit=50)
    oi_df= Binance_api_helper.convert_hash_to_data_frame(hist_data)
    Binance_api_helper.oi_change(oi_df)
    Binance_api_helper.oi_change_candles(oi_df, 10)
    five_minute['oi_change_last10'] =  oi_df['oi_change_last10']
    five_minute['oi_change_last10_pc'] = oi_df['oi_change_last10_pc']
    Binance_api_helper.oi_change_candles(oi_df, 3)
    five_minute['oi_change_last3'] = oi_df['oi_change_last3']
    five_minute['oi_change_last3_pc'] = oi_df['oi_change_last3_pc']
    five_minute['ema_5'] = btalib.ema(five_minute.Close, period=5).df
    five_minute['sma_20'] = btalib.sma(five_minute.Close, period=20).df
    five_minute['sma_40'] = btalib.sma((five_minute.High + five_minute.Low)/2 , period=40).df
    five_minute['sma-volume_20'] = btalib.sma(five_minute.Volume, period=20).df
    five_minute['sma-volume_3'] = btalib.sma(five_minute.Volume, period=3).df
    current_cl_index = len(five_minute.Close)-1
    five_minute['sma-volume_40'] = btalib.sma(five_minute.Volume, period=40).df
    diff_sma_pre_pc  = (abs(five_minute['sma_20'].values[current_cl_index-2] - five_minute['sma_40'].values[current_cl_index-2])/five_minute['sma_20'].values[current_cl_index-2] ) * 100
    diff_sma_price_pc = (abs(five_minute['sma_20'].values[current_cl_index-2] - five_minute.Close.values[current_cl_index-2])/ five_minute.Close.values[current_cl_index-2]) * 100


    if (five_minute['sma-volume_20'].values[current_cl_index] * 3) < five_minute['sma-volume_3'].values[current_cl_index] :
        if (five_minute['sma-volume_40'].values[current_cl_index] * 3) < five_minute['sma-volume_3'].values[current_cl_index]:
            if five_minute['oi_change_last3_pc'].values[current_cl_index] > 6 :
               print("oi matched"+s['symbol'] )

               if ( cross_price_over_ema(five_minute) == "buy" and  diff_sma_pre_pc < 1.3  ):
                   print("buy signla Symbol------------------" + s['symbol'])
                   send_text = buy_text(client, market_type, s, five_minute, current_cl_index)
                   return (send_text)
               elif(cross_price_over_ema(five_minute) == "sell"
                      and diff_sma_pre_pc < 1.3):
                   print("sell signla Symbol------------------" + s['symbol'])
                   send_text = sell_text(client, market_type, s, five_minute, current_cl_index)
                   return (send_text)

    elif (five_minute['sma-volume_20'].values[current_cl_index] * 3) < five_minute['sma-volume_3'].values[current_cl_index]:
        if (five_minute['sma-volume_40'].values[current_cl_index] * 3) < five_minute['sma-volume_3'].values[current_cl_index]:
            if five_minute['oi_change_last3_pc'].values[current_cl_index] > 4 and five_minute['oi_change_last10_pc'] > 15:
                print("oi matched" + s['symbol'])
                if (cross_price_over_ema(five_minute) == "buy"
                     and  diff_sma_pre_pc < 1.3 ):
                    print("buy signla Symbol------------------" + s['symbol'])
                    send_text=buy_text(client, market_type, s, five_minute, current_cl_index)
                    return (send_text)
                elif( cross_price_over_ema(five_minute) == "sell"
                                and   diff_sma_pre_pc < 1.3        ):
                               print("sell signla Symbol------------------" + s['symbol'])
                               send_text = sell_text(client, market_type, s, five_minute, current_cl_index)
                               return (send_text)

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
            time.sleep(60.0 - ((time.time() - starttime) % 60.0))
        except Exception as ex1:
            print('Error creating batch: %s' % str(ex1))
            time.sleep(100)

Scanner()