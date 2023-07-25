from datetime import datetime, timedelta, time
import multiprocessing
from datetime import time

from binance.enums import TIME_IN_FORCE_GTC, SIDE_SELL

from Master.postgres_insert import postgres_fetch_trades, postgres_update_last_trade
# important conditions
# - Move the sl once we have the 30% of the profit to cost of entry
# - once the sl moved to entry price then every time the stock appritiates from new sl to current market by 38
#       pc then 5pc sl moves up
# if no open sl orders present then the sl price would be 0.25 pc of the entry
#  if the sl order is palced for the partial qty then in that case sl order gets cancled and recreates the
#  sl order with the ope position qty

from binane_api import Binance_Api_wrapper_generic
import time

default_sl=0.12
first_trail_profit_con=30
subsequent_trail_con_pc=38
increase_sl__by_pc=0.07

def position_count(pos_info):
    position_dict = dict()
    neagtive = 0
    positive = 0
    for pos in pos_info:
        if float(pos['positionAmt']) < 0:
            neagtive = neagtive + 1
        elif float(pos['positionAmt']) > 0:
            positive = positive + 1
    position_dict["positive"] = positive
    position_dict['negative'] = neagtive
    return position_dict


# round the position size we can open to the precision of the market
def round_to_precision(_qty, _precision):
    new_qty = "{:0.0{}f}".format(_qty , _precision)
    return float(new_qty)

# get the precision of the market, this is needed to avoid errors when creating orders
def get_market_precision(client, _market):

    market_data = client.get_exchange_information()
    precision = 3
    for market in market_data.symbols:
        if market.symbol == _market:
            precision = market.quantityPrecision
            break
    return precision

def get_market_precision_price(client, _market):

    market_data = client.futures_exchange_info()
    precision = 3
    for market in market_data['symbols']:
        if market['symbol'] == _market:
            precision = market['pricePrecision']
            break
    return precision

def get_postion_details(script_code, pos_info):
    for pos in pos_info:
        if pos['symbol'] == script_code:
            return pos
    return None
def get_open_stop_loss_order_details(client,symbol):
    open_orders = client.futures_get_open_orders(symbol=symbol)
    for open_order in open_orders:
        if open_order['type'] == "STOP_MARKET" :
            return open_order

def round_price_based_on_sample(sample_price, actual_price):
    dec = len(str(sample_price).split(".")[1])
    price = round(float(actual_price), dec)
    return price


def create_default_sl_order(Wrapper_obj, client, pos, trade_side,sl_pr ):
    if trade_side=="BUY":
        sl_pr_pre = get_market_precision_price(client, pos['symbol'])
        sl_pr = round_to_precision(sl_pr, sl_pr_pre)
        sl_order = Wrapper_obj.create_stop_loss_market_order(pos['symbol'], "SELL", pos['positionAmt'], sl_pr, client)
    else:
        sl_pr_pre = get_market_precision_price(client, pos['symbol'])
        sl_pr = round_to_precision(sl_pr, sl_pr_pre)
        sl_order = Wrapper_obj.create_stop_loss_market_order(pos['symbol'], "BUY", abs(float(pos['positionAmt'])), sl_pr, client)

def trail_stop_loss_based_on_pc(Wrapper_obj, client, pos, trade_side,sl_order ):
    if trade_side == "BUY":
        total_investment = (float(pos['positionAmt']) * float(pos['entryPrice']) / float(pos['leverage']))
        sl_pr_pre = get_market_precision_price(client, pos['symbol'])
        er_pr = round_to_precision(float(pos['entryPrice']), sl_pr_pre)
        if (float(pos['unRealizedProfit']) > 0) and (float(sl_order['stopPrice']) < er_pr):
            # first time move the sl to entry price
            if ((float(pos['unRealizedProfit']) / total_investment) * 100) > first_trail_profit_con:
                client.futures_cancel_order(symbol=pos['symbol'], orderID=sl_order['orderId'])
                sl_pr = float(pos['entryPrice'])
                create_default_sl_order(Wrapper_obj, client, pos, "BUY", sl_pr)
        elif (float(pos['unRealizedProfit']) > 0) and (float(sl_order['stopPrice']) >= float(er_pr)):
            fetch_unreliased_profit = (float(pos['markPrice']) - float(sl_order['stopPrice'])) / float(
                pos['entryPrice']) * 100 * float(pos['leverage'])
            if fetch_unreliased_profit > subsequent_trail_con_pc:
                new_slprice = (float(sl_order['stopPrice']) * increase_sl__by_pc) / float(pos['leverage']) + float(
                    sl_order['stopPrice'])
                client.futures_cancel_order(symbol=pos['symbol'], orderID=sl_order['orderId'])
                create_default_sl_order(Wrapper_obj, client, pos, "BUY", new_slprice)
    else:
        total_investment = (float(pos['positionAmt']) * float(pos['entryPrice']) / float(pos['leverage']))
        sl_pr_pre = get_market_precision_price(client, pos['symbol'])
        er_pr = round_to_precision(float(pos['entryPrice']), sl_pr_pre)
        if (float(pos['unRealizedProfit']) > 0) and (float(sl_order['stopPrice']) > er_pr):
            # first time move the sl to entry price
            if ((float(pos['unRealizedProfit']) / abs(total_investment)) * 100) > first_trail_profit_con:
                client.futures_cancel_order(symbol=pos['symbol'], orderID=sl_order['orderId'])
                sl_pr = float(pos['entryPrice'])
                create_default_sl_order(Wrapper_obj, client, pos, "SELL", sl_pr)
        elif (float(pos['unRealizedProfit']) > 0) and (float(sl_order['stopPrice']) <= float(er_pr)):
            fetch_unreliased_profit = (float(sl_order['stopPrice']) -float(pos['markPrice'])) / float(
               pos['entryPrice']) * 100 * float(pos['leverage'])
            if fetch_unreliased_profit > subsequent_trail_con_pc:
                new_slprice = (float(sl_order['stopPrice']) * increase_sl__by_pc) / float(pos['leverage']) + float(
                    sl_order['stopPrice'])
                client.futures_cancel_order(symbol=pos['symbol'], orderID=sl_order['orderId'])
                create_default_sl_order(Wrapper_obj, client, pos, "SELL", new_slprice)

def Manage_existing_orders():
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    pos_info = client.futures_position_information()
    pos_hash = position_count(pos_info)
    for pos in pos_info:
        if float(pos['positionAmt'] )> 0:
            sl_order= get_open_stop_loss_order_details(client,pos['symbol'])
            if sl_order == None : #place new sl order if no sl present
                sl_pr = float(pos['entryPrice']) - (float(pos['entryPrice']) * default_sl) / float(pos['leverage'])
                create_default_sl_order(Wrapper_obj,client, pos, "BUY",sl_pr )
            elif float(pos['positionAmt'] ) != float(sl_order['origQty'] ) :
                sl_pr = float(pos['entryPrice']) - (float(pos['entryPrice']) * default_sl) / float(pos['leverage'])
                client.futures_cancel_order(symbol=pos['symbol'], orderID=sl_order['orderId'])
                create_default_sl_order(Wrapper_obj, client, pos, "BUY",sl_pr)
            # stopPriceclientOrderId
            trail_stop_loss_based_on_pc(Wrapper_obj, client, pos, "BUY", sl_order)

        elif float(pos['positionAmt'] ) < 0:
            sl_order = get_open_stop_loss_order_details(client, pos['symbol'])
            if sl_order == None:  # place new sl order if no sl present
                sl_pr = float(pos['entryPrice']) + (float(pos['entryPrice']) * default_sl) / float(pos['leverage'])
                create_default_sl_order(Wrapper_obj, client, pos, "SELL",sl_pr)
            elif abs(float(pos['positionAmt'] )) != float(sl_order['origQty'] ) :
                client.futures_cancel_order(symbol=pos['symbol'], orderID=sl_order['orderId'])
                sl_pr = float(pos['entryPrice']) + (float(pos['entryPrice']) * default_sl) / float(pos['leverage'])
                create_default_sl_order(Wrapper_obj, client, pos, "SELL",sl_pr)
            trail_stop_loss_based_on_pc(Wrapper_obj, client, pos, "SELL", sl_order)

while True:
    try:
        starttime = time.time()
        print(starttime)
        print("started scanning ")
        Manage_existing_orders()

        print(" #{starttime} ended scanning ")
        print(starttime)
        time.sleep(12.0 - ((time.time() - starttime) % 12.0))
    except Exception as ex1:
        print('Error creating batch: %s' % str(ex1))



