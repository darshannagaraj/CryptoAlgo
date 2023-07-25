# database.py

import psycopg2
import bcrypt
# config.py

DB_PARAMS = {
    'dbname': 'Option_decoder',
    'user': 'postgres',
    'password': 'postgrespw',
    'host': 'localhost',
    'port': '49153'
}

# Database connection function
def connect_db():
    return psycopg2.connect(**DB_PARAMS)

# Hash a password using bcrypt
def hash_password(password):
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed_password

# Check if the entered password matches the hashed password
def check_password(hashed_password, input_password):
    return bcrypt.checkpw(input_password.encode('utf-8'), hashed_password)


# Function to connect to the database
def connect_db():
    return psycopg2.connect(**DB_PARAMS)

# Function to insert scanned data into the database
def insert_scanned_data( date, script,  signal, reason, script_category, expected_target, expected_sl, good_price):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute('INSERT INTO scanned_data (date,symbol, signal, reason, script_category, expected_target, expected_sl, good_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                    (date, script, signal, reason, script_category, expected_target, expected_sl, good_price))

    conn.commit()

# def fetch_option_data(symbol, date1):
#     with connect_db().cursor() as cur:
#         cur.execute(
#         'select date,time,volume,open,high,low,ltp as "close" from future_data fd  where date = '2022-09-30' and script_name  like 'NIFTY'
#
#             '        )
#         scanned_data = [{'symbol': row[1], 'date': row[2], 'signal': row[3]
#                             , 'reason': row[4], 'script_category': row[5], 'sl': row[0], 'target': row[6]
#                          }
#                         for row in cur.fetchall()]
#     return scanned_data
