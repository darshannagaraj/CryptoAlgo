import yfinance as yf
import pandas as pd
from flask import Flask, render_template_string

app = Flask(__name__)


# Function to calculate VWAP
def calculate_vwap(data):
    tp = (data['High'] + data['Low'] + data['Close']) / 3
    vwap = ((tp * data['Volume']).cumsum()) / data['Volume'].cumsum()
    return vwap


# Function to scan for VWAP crossover
def scan_vwap_crossover(ticker):
    stock_data = yf.download(ticker, period='1y')
    stock_data['VWAP'] = calculate_vwap(stock_data)

    # VWAP crossover logic (simple crossover here)
    crossover_signals = []
    for i in range(1, len(stock_data)):
        if stock_data['Close'][i] > stock_data['VWAP'][i] and stock_data['Close'][i - 1] <= stock_data['VWAP'][i - 1]:
            crossover_signals.append((stock_data.index[i], "Bullish"))
        elif stock_data['Close'][i] < stock_data['VWAP'][i] and stock_data['Close'][i - 1] >= stock_data['VWAP'][i - 1]:
            crossover_signals.append((stock_data.index[i], "Bearish"))

    return crossover_signals


# HTML template to display the VWAP crossover scanner results
html_template = '''
<!DOCTYPE html>
<html>
<head>
    <title>VWAP Crossover Scanner</title>
</head>
<body>
    <h1>VWAP Crossover Scanner</h1>
    <table border="1">
        <tr>
            <th>Ticker</th>
            <th>Crossover Date</th>
            <th>Signal</th>
        </tr>
        {% for result in scanner_results %}
        <tr>
            <td>{{ result["ticker"] }}</td>
            <td>{{ result["date"] }}</td>
            <td>{{ result["signal"] }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
'''


@app.route('/')
def display_vwap_crossover_scanner():
    # Define a list of tickers to scan (add more if needed)
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'FB']

    scanner_results = []
    for ticker in tickers:
        signals = scan_vwap_crossover(ticker)
        for signal in signals:
            scanner_results.append({"ticker": ticker, "date": signal[0].strftime("%Y-%m-%d"), "signal": signal[1]})

    return render_template_string(html_template, scanner_results=scanner_results)


if __name__ == '__main__':
    app.run()
