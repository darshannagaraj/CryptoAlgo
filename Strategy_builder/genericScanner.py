import datetime
import math
import time
import pandas as pd

from binane_api import Binance_Api_wrapper_generic
from lib.genericCode import getminutedata, lonelyCandle, is_volume_less_than_average, TrainTrack, calculateVwap, \
    volumeGainers, panicBuy, vwapslope


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

def find_movement_based_on_time_frame(s,client,market_type, Scanned_all,wrapper_obj, drop_rows=0 ):

    atr_period = 10
    atr_multiplier = 3.0
    scanned_data = {}
    five_minute = getminutedata(s['symbol'],client.KLINE_INTERVAL_5MINUTE,3, market_type,client)
    five_minute.drop(five_minute.tail(drop_rows).index,
            inplace=True)

    five_minute.reset_index(inplace=True)
    # five_minute = five_minute.loc[five_minute.Time <= specifictime]
    df = five_minute

    calculateVwap(df,30)
    volumeGainers(df, 40)
    # vwapslope(df)
    selected_rows = df[df['allSell'] == True]
    if len(selected_rows) > 0:
        print(s['symbol'])
        if (selected_rows['Volume'].iloc[0] * selected_rows['Close'].iloc[0]) > 200000:
            if df['Time'].iloc[-1] == selected_rows['Time'].iloc[0]:
                print("currentsell")
            print("sell")
            print(selected_rows)

    selected_rows = df[df['allBuy'] == True]
    if len(selected_rows) > 0:
        print(s['symbol'])
        if (selected_rows['Volume'].iloc[0] * selected_rows['Close'].iloc[0] ) > 200000 :
            print("buy")
            print(selected_rows)

  #highest or lowest point of the last 5 candles high or low)
  #bolinger body should be ouside the upper or lower band
  #candle high should be greater then the bolinger


    # df = df.drop(df.index[-95])
    # lonelyCandle(df, 5)
    # is_volume_less_than_average(df)
    # TrainTrack(df, 5, 9)
    #
    # # selected_rows = df[(df['TrainTrack'] == True)]
    # print(s['symbol'])
    # selected_rows = df[(df['lonelyEma'] == True) & (df['VolumeBelowAverage3'] == True) & (df['TrainTrack'] == True)]
    # print(selected_rows)
    #


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
            remaining_time = datetime.timedelta(minutes=4) - analysis_time
            if remaining_time.total_seconds() > 0:
                time.sleep(remaining_time.total_seconds())

        except Exception as ex1:
            print('Error creating batch: %s' % str(ex1))
            time.sleep(100)

Scanner()