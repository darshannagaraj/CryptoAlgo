# binance_api.py
import os
import pandas as pd
from binance.client import Client
import btalib
from binance.enums import FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, FUTURE_ORDER_TYPE_STOP_MARKET
import config as cfg
from binance.enums import HistoricalKlinesType

class Binance_Api_wrapper_generic():

    def __init__(self):
        print ("class")
        self.client =Client


    def get_client(self):
        api_key =  cfg.getPublicKey()
        api_secret = cfg.getPrivateKey()
        client = Client(api_key, api_secret)
        return client

    def get_future_Asset_balance(self, client, assset):
        f_balance = client.futures_account_balance()
        balance = ([i for i in f_balance if i['asset'] == assset][0])
        return balance

    def create_market_order(self, symbol, side, qty, client):
        ab = client.futures_create_order(symbol=symbol, side=side, type='MARKET', quantity=qty
                                         )
        return ab

    def create_take_profit_market_order(self, symbol, side, qty, stop_loss_price, client):
        ab = client.futures_create_order(symbol=symbol, side=side, type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, quantity=qty,
                                         workingType='MARK_PRICE', stopPrice=stop_loss_price, reduceOnly=True)
        return ab

    def create_stop_loss_market_order(self, symbol, side, qty, stop_loss_price, client):
        ab = client.futures_create_order(symbol=symbol, side=side, type='STOP_MARKET', quantity=qty,
                                         workingType='MARK_PRICE', stopPrice=stop_loss_price, reduceOnly=True)
        return ab

    # get timestamp of earliest date data is available

    def get_all_symbols_binance(self, client):
        exchange_info = client.futures_exchange_info()
        # for s in exchange_info['symbols']:
        #     # print(s['symbol'])
        return exchange_info

    def oi_change(df):
        for i in range(2, len(df['sumOpenInterest'].values)):
            idx = df.index[i]
            df.loc[idx, 'oi_diff'] = (
                        float(df.iloc[i, :]['sumOpenInterest']) - float(df.iloc[i - 1, :]['sumOpenInterest']))
        return df

    def oi_change_candles(df, period):
        sum = 0
        name = 'oi_change_last' + str(period)
        for i in range(period, len(df['oi_diff'].values)):
            idx = df.index[i]
            sum = 0
            for j in range(period):
                sum = sum + float(df.iloc[i - j, :]['oi_diff'])
            df.loc[idx, name] = sum
            df.loc[idx, name + "_pc"] = (sum / float(df.iloc[i - j, :]['sumOpenInterest'])) * 100
        return df

    def convert_hash_to_data_frame(data):
        oi_da_frame = pd.DataFrame(data)
        oi_da_frame = oi_da_frame.iloc[:, :4]
        oi_da_frame.columns = ['symbol', 'sumOpenInterest', 'sumOpenInterestValue', 'timeStamp']
        oi_da_frame = oi_da_frame.set_index('timeStamp')
        oi_da_frame.index = pd.to_datetime(oi_da_frame.index, unit='ms')
        # oi_da_frame = oi_da_frame.astype(float)
        return oi_da_frame



    