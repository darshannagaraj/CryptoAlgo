import datetime
import time
import pandas as pd

from binane_api import Binance_Api_wrapper_generic
from lib.genericCode import getminutedata, lonelyCandle, is_volume_less_than_average, TrainTrack, calculateVwap


def find_movement_based_on_time_frame(s,client,market_type, Scanned_all,wrapper_obj, drop_rows=0 ):

    atr_period = 10
    atr_multiplier = 3.0
    scanned_data = {}
    five_minute = getminutedata(s['symbol'],client.KLINE_INTERVAL_1HOUR,3, market_type,client)
    five_minute.drop(five_minute.tail(drop_rows).index,
            inplace=True)

    five_minute.reset_index(inplace=True)
    # five_minute = five_minute.loc[five_minute.Time <= specifictime]
    df = five_minute
    calculateVwap(df,10)
    # Find Sell signals
    # df = df.drop(df.index[-95])
    lonelyCandle(df, 5)
    is_volume_less_than_average(df)
    TrainTrack(df, 5, 9)

    # selected_rows = df[(df['TrainTrack'] == True)]
    print(s['symbol'])
    selected_rows = df[(df['lonelyEma'] == True) & (df['VolumeBelowAverage3'] == True) & (df['TrainTrack'] == True)]
    print(selected_rows)



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