from binane_api import *

Wrapper_obj  = Binance_Api_wrapper_generic()
client = Wrapper_obj.get_client()
print(client.get_account())
print(client.get_asset_balance(asset='BTC'))

Future_balance= Wrapper_obj.get_future_Asset_balance(client,"USDT")

print (client.futures_position_information())
# print (client.futures_get_position_mode())
# timestamp = client._get_earliest_valid_timestamp('1000SHIBUSDT Perpetual', '1d')
client.futures_position_information()

print (client.futures_get_open_orders())

print (Wrapper_obj.get_all_symbols_binance(client))
# client.futures_cancel_order(symbol = "NKNUSDT", orderId = "314603745")
# print(timestamp)

# order = client.futures_create_order(symbol='ETHUSDT', side='SELL', type='MARKET', quantity=100)

candles = client.get_klines(symbol="NKNUSDT",
                                        interval=client.KLINE_INTERVAL_5MINUTE,
                                        limit=500)

frame = pd.dat
print ("chekcing")
#
# 1499040000000,  # Open time
# "0.01634790",  # Open
# "0.80000000",  # High
# "0.01575800",  # Low
# "0.01577100",  # Close
# "148976.11427815",  # Volume
# 1499644799999,  # Close time
# "2434.19055334",  # Quote asset volume
# 308,  # Number of trades
# "1756.87402397",  # Taker buy base asset volume
# "28.46694368",  # Taker buy quote asset volume
# "17928899.62484339"  # Can be ignored
# ]