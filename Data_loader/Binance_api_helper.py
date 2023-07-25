import pandas as pd
from binance.client import Client
import btalib
from binance.enums import FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET
import config as cfg


class Binance_api_helper():
    def __init__(self):
        print ("class")
        self.client =Client

    def oi_change(df):
      for i in range(2, len(df['sumOpenInterest'].values)):
            idx = df.index[i]
            df.loc[idx, 'oi_diff'] = (float(df.iloc[i, :]['sumOpenInterest']) - float(df.iloc[i-1, :]['sumOpenInterest']))
      return df

    def oi_change_candles(df, period):
        sum=0
        name = 'oi_change_last' + str(period)
        for i in range(period, len(df['oi_diff'].values)):
            idx = df.index[i]
            sum = 0
            for j in  range(period):
                sum = sum + float( df.iloc[i-j, :]['oi_diff'])
            df.loc[idx, name]= sum
            df.loc[idx, name+"_pc"] = (sum/ float(df.iloc[i-j, :]['sumOpenInterest'])) *100
        return df

    def convert_hash_to_data_frame(data):
        oi_da_frame= pd.DataFrame(data)
        oi_da_frame = oi_da_frame.iloc[:, :4]
        oi_da_frame.columns = ['symbol', 'sumOpenInterest', 'sumOpenInterestValue', 'timeStamp']
        oi_da_frame = oi_da_frame.set_index('timeStamp')
        oi_da_frame.index = pd.to_datetime(oi_da_frame.index, unit='ms')
        # oi_da_frame = oi_da_frame.astype(float)
        return oi_da_frame