# binance_api.py
import os
import pandas as pd
from binance.client import Client
import btalib
from binance.enums import FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET
import config as cfg

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

    def create_stop_loss_market_order(self, symbol, side, qty, stop_loss_price , client):
       ab = client.futures_create_order(symbol=symbol, side=side, type='STOP_MARKET', quantity=qty,
                                    workingType='MARK_PRICE', stopPrice=stop_loss_price, reduceOnly=True)
       return ab

    # get timestamp of earliest date data is available

    def get_all_symbols_binance(self, client):
        exchange_info = client.futures_exchange_info()
        # for s in exchange_info['symbols']:
        #     # print(s['symbol'])
        return exchange_info


    def get_signal(self,clientreal):
        candles = clientreal.get_klines(symbol=self.DATASET[0],
                                        interval=clientreal.KLINE_INTERVAL_1MINUTE,
                                        limit=40)
        data = pd.DataFrame({'close': np.asarray(candles)[:, 4]})
        data['close'] = data['close'].astype(float)
        data['wma7'] = btalib.WMA(data['close'], timeperiod=7)
        data['wma14'] = btalib.WMA(data['close'], timeperiod=14)
        data['signal'] = np.where(data['wma14'] > data['wma7'], 1, 0)
        data['action'] = data['signal'].diff()

        if list(data['action'])[-1] == 1:
            self.action = 1
        elif list(data['action'])[-1] == -1:
            self.action = -1
        else:
            self.action = 0

        self.action_open()