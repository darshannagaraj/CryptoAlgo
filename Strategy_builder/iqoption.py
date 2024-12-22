import requests
import json

# Replace with your actual API credentials
api_key = 'your_api_key'
api_secret = 'your_api_secret'

# Base URL for Iqoption API
base_url = 'https://api.iqoption.com/'

# Function to get the authorization token
def get_token(api_key, api_secret):
    data = {
        "api_key": api_key,
        "api_secret": api_secret
    }
    response = requests.post(base_url + 'api/v1.0/login', data=data)
    return response.json()['token']

# Function to place a new binary options trade
def place_binary_trade(token, instrument_id, side, amount, expiration_time):
    data = {
        "instrument_id": instrument_id,  # Replace with the instrument ID for the desired asset
        "side": side,  # 'call' (for "up" prediction) or 'put' (for "down" prediction)
        "amount": amount,  # The amount you want to trade
        "expiration_time": expiration_time  # Expiration time in seconds
    }
    headers = {
        "Authorization": "Bearer " + token
    }
    response = requests.post(base_url + 'api/v1.0/order', headers=headers, data=json.dumps(data))
    return response.json()

if __name__ == '__main__':
    # Get the authentication token
    token = get_token(api_key, api_secret)

    # Example: Place a binary options trade to predict "up" for 1 BTC with 5-minute expiration
    instrument_id = 1  # Replace with the correct instrument ID for BTC/USD or other pairs
    side = 'call'  # 'call' for predicting "up", 'put' for predicting "down"
    amount = 1
    expiration_time = 300  # 5 minutes (expiration time is in seconds)
    trade_response = place_binary_trade(token, instrument_id, side, amount, expiration_time)

    print(trade_response)
