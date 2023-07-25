from datetime import datetime, timedelta, time
import multiprocessing
from datetime import time

from Master.postgres_insert import postgres_fetch_trades, postgres_update_last_trade
from binane_api import Binance_Api_wrapper_generic
import time


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


def get_postion_details(script_code, pos_info):
    for pos in pos_info:
        if pos['symbol'] == script_code:
            return pos
    return None


def manage_buy_position(script_code, other_data, client, pos_info, Wrapper_obj, qty, sl_price, trigger_price,indication):
    sl_price = other_data[5] if sl_price is None else sl_price
    trigger_price = other_data[2] if trigger_price is None else trigger_price
    indication = "BUY" if indication is None else indication
    pos = get_postion_details(script_code, pos_info)
    open_order_present = check_open_orders(script_code, client)
    if float(pos['positionAmt']) == 0:  # cosnider its fresh order
        current_price = client.futures_symbol_ticker(symbol=other_data[1])
        if float(current_price['price']) > trigger_price:  # check if the price is with in the range else ignore it
            order = Wrapper_obj.create_market_order(script_code, "BUY", qty, client)
            sl_order = Wrapper_obj.create_stop_loss_market_order(script_code, "SELL", qty, sl_price, client)
            tp_order= Wrapper_obj.create_take_profit_market_order(script_code, "SELL", qty, sl_price, client)
            postgres_update_last_trade(indication, script_code)
            print("created order id " + order)
    else:
        if float(pos['positionAmt']) > 0:
            print("already same is position is present no action required ")
        else:
            # write to function to close all open orders for that script
            # place sl order
            current_price = client.futures_symbol_ticker(symbol=other_data[1])
            if float(current_price['price']) > trigger_price:  # check if the price is with in the range else ignore it
                order = Wrapper_obj.create_market_order(script_code, "BUY", qty, client)
                cancel_order(script_code, client)
                # sl_order = Wrapper_obj.create_stop_loss_market_order(script_code, "SELL", qty, other_data[5], client)


def manage_sell_position(script_code, other_data, client, pos_info, Wrapper_obj, qty, sl_price, trigger_price, indication):
    sl_price = other_data[5] if sl_price is None else sl_price
    indication = "SELL" if indication is None else indication
    open_order_present = check_open_orders(script_code, client)
    pos = get_postion_details(script_code, pos_info)
    trigger_price = other_data[2] if trigger_price is None else trigger_price
    if float(pos['positionAmt']) == 0:
        current_price = client.futures_symbol_ticker(symbol=other_data[1])
        if float(current_price['price']) < trigger_price:  # check if the price is with in the range else ignore it
            order = Wrapper_obj.create_market_order(script_code, "SELL", qty, client)
            sl_order = Wrapper_obj.create_stop_loss_market_order(script_code, "BUY", qty, sl_price, client)
            postgres_update_last_trade(indication, script_code)
            print("created order id " + order)
    else:
        if float(pos['positionAmt']) < 0:
            print("already same is position is present no action required ")
        else:
            # write to function to close all open orders for that script
            current_price = client.futures_symbol_ticker(symbol=other_data[1])
            if float(current_price['price']) < trigger_price:  # check if the price is with in the range else ignore it
                # replace with closing the position
                order = Wrapper_obj.create_market_order(script_code, "SELL", qty, client)
                cancel_order(script_code, client)
                # sl_order = Wrapper_obj.create_stop_loss_market_order(script_code, "BUY", qty, other_data[5], client)


def check_open_orders(symbol, client):
    open_orders = client.futures_get_open_orders()
    client.futures_position_information()

    for order in open_orders:
        if order['symbol'] == symbol:
            return True
    return False


def check_if_open_position_present():
    print("open position present")


def cancel_order(symbol, client):
    open_orders = client.futures_get_open_orders()
    for order in open_orders:
        if order['symbol'] == symbol:
            client.futures_cancel_order(symbol=symbol, orderID=order['orderId'])
    return False


def exit_position(position_details):
    print("exited position")


def create_stop_market_order():
    Entry_usdt = 300
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    trade_list = postgres_fetch_trades()
    pos_info = client.futures_position_information()

    # client.fu
    for row in trade_list:
        mydate = row[0]
        current_price = client.futures_symbol_ticker(symbol=row[1])
        pos_hash = position_count(pos_info)
        qty = (Entry_usdt / row[3])
        qty = (round(float((qty)), int(row[8])))
        if not ((datetime.now() - row[0]).total_seconds() > timedelta(minutes=60).total_seconds()):
            candle_change =  (abs(float(row[3]) - float(row[4]))/ float(row[4]) ) *100
            if row[7] == "SELL" and candle_change <=6: # and pos_hash['negative'] <= 3
                manage_sell_position(row[1], row, client, pos_info, Wrapper_obj, qty, None, None,None)
            elif row[7] == "BUY"  and candle_change <=6: # and pos_hash['positive'] <= 3
                manage_buy_position(row[1], row, client, pos_info, Wrapper_obj, qty,None, None, None)


def round_price_based_on_sample(sample_price, actual_price):
    dec = len(str(sample_price).split(".")[1])
    price = round(float(actual_price), dec)
    return price


def create_reversal_orders():
    Entry_usdt = 120
    Wrapper_obj = Binance_Api_wrapper_generic()
    client = Wrapper_obj.get_client()
    trade_list = postgres_fetch_trades()
    pos_info = client.futures_position_information()
    sl_price = None
    # client.fu
    for row in trade_list:
        mydate = row[0]
        current_price = client.futures_symbol_ticker(symbol=row[1])
        pos_hash = position_count(pos_info)
        qty = (Entry_usdt / float(row[3]))
        qty = (round(float((qty)), int(row[8])))
        if not ((datetime.now() - row[0]).total_seconds() > timedelta(minutes=220).total_seconds()):
            total_size = abs(float(row[3]) - float(row[4]))
            if row[7] == "BUY" and pos_hash['negative'] <= 10:
                sl_price_ind = abs(float(row[3]) - float(row[11]))
                sl_price = float(row[3]) + sl_price_ind  # hgh - price
                sl_price = round_price_based_on_sample(float(row[5]), float(sl_price))
                if (total_size / sl_price_ind) > 2.1:
                    manage_sell_position(row[1], row, client, pos_info, Wrapper_obj, qty, sl_price, float(row[3]),row[7])
            elif row[7] == "SELL" and pos_hash['negative'] <= 10:
                sl_price_ind = abs(float(row[4]) - float(row[11]))
                sl_price =  float(row[4]) - sl_price_ind #hgh - price

                sl_price = round_price_based_on_sample(row[5], sl_price)
                if (total_size / sl_price_ind) > 2.1:
                    manage_buy_position(row[1], row, client, pos_info, Wrapper_obj, qty, sl_price, float(row[4]),row[7])
            # sl_price = abs(row[11] - row[10])
            # total_size = High - low

            # if row[7] == "SELL" and pos_hash['negative'] <= 3:
            #      manage_buy_position(row[1], row, client, pos_info, Wrapper_obj, qty, sl_price)
            # elif row[7] == "BUY" and pos_hash['positive'] <= 3:
            #      manage_sell_position(row[1], row, client, pos_info, Wrapper_obj, qty, sl_price)


while True:
    try:
        starttime = time.time()
        print(starttime)
        print("started scanning ")
        # create_reversal_orders()
        create_stop_market_order()

        print(" #{starttime} ended scanning ")
        print(starttime)
        time.sleep(12.0 - ((time.time() - starttime) % 12.0))
    except Exception as ex1:
        time.sleep(100)
        print('Error creating batch: %s' % str(ex1) % ex1.print_exc())

