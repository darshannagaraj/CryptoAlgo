from datetime import time, datetime

import psycopg2
#
#
# CREATE TABLE public.trades (
# 	scanned_date_time timestamp NOT NULL,
# 	script_code varchar NOT NULL,
# 	trigger_price float8 NOT NULL,
# 	low_price float8 NOT NULL,
# 	high_price float8 NOT NULL,
# 	sl_price float8 NOT NULL,
# 	candle_date_time timestamp NOT NULL,
# 	indication varchar NOT NULL,
# 	qty_precesion varchar NULL,
# 	trade_taken varchar NULL,
# 	open_price varchar NULL,
# 	close_price varchar NULL,
# 	algo_name varchar NULL
# );


def connection():
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='admin', host='localhost', port='5432'
    )

def postgres_update_last_trade(indication, script_code):
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='admin', host='localhost', port='5432'
    )
    cursor = conn.cursor()
    postgreSQL_select_Query = """UPDATE public.trades SET trade_taken = 'TRUE'  where script_code =  %s and 
     indication = %s """
    cursor.execute(postgreSQL_select_Query, (script_code, indication) )
    conn.commit()
    # Close communication with the PostgreSQL database
    cursor.close()

def postgres_fetch_trades():
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='admin', host='localhost', port='5432'
    )

    cursor = conn.cursor()
    postgreSQL_select_Query = """SELECT scanned_date_time, script_code, trigger_price, low_price, high_price, sl_price, candle_date_time,  indication,qty_precesion, trade_taken, open_price,close_price
FROM public.trades  where trade_taken is null and algo_name like 'OPEN_INREST' order by scanned_date_time desc ;"""
    cursor.execute(postgreSQL_select_Query)
    print("Selecting rows from mobile table using cursor.fetchall")
    mobile_records = cursor.fetchall()
    print("Print each row and it's columns values")
    return mobile_records


def postges_push_trade(script_code, indication, trigger_price, low_price, high_price, sl_price, candle_date_time, qty_precesion, open_price, close_price, algo):
    # Establishing the connection
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='admin', host='localhost', port='5432'
    )
    # Setting auto commit false
    conn.autocommit = True
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    postgres_insert_query = """ INSERT INTO public.trades (scanned_date_time, script_code, indication, trigger_price, low_price, high_price, sl_price,qty_precesion, candle_date_time, open_price, close_price,algo_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
    record_to_insert = (
    datetime.now(), script_code, indication, trigger_price, low_price, high_price, sl_price,qty_precesion, candle_date_time,open_price, close_price,algo)
    cursor.execute(postgres_insert_query, record_to_insert)
    # Commit your changes in the database
    conn.commit()
    print("Records inserted........")

    # Closing the connection
    conn.close()
