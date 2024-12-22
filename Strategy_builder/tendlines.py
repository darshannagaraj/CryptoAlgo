import pandas as pd

from Data_loader.Binance_api import Binance_Api_wrapper_generic

Wrapper_obj = Binance_Api_wrapper_generic()
client = Wrapper_obj.get_client()
Future_scan = find_movement_based_on_time_frame(s, client, "future", Scanned_all, Wrapper_obj)



df = pd.read_csv("EURUSD_Candlestick_4_Hour_ASK_05.05.2003-16.10.2021.csv")
df.columns=['time', 'open', 'high', 'low', 'close', 'volume']
#Check if NA values are in data
df=df[df['volume']!=0]
df.reset_index(drop=True, inplace=True)
df.isna().sum()
df.head(10)

dfpl = df[7000:7200]
import plotly.graph_objects as go
from datetime import datetime

fig = go.Figure(data=[go.Candlestick(x=dfpl.index,
                open=dfpl['open'],
                high=dfpl['high'],
                low=dfpl['low'],
                close=dfpl['close'])])

fig.show()

import numpy as np
from matplotlib import pyplot
backcandles= 50
wind = 5

candleid = 8800

maxim = np.array([])
minim = np.array([])
xxmin = np.array([])
xxmax = np.array([])
for i in range(candleid-backcandles, candleid+1, wind):
    minim = np.append(minim, df.low.iloc[i:i+wind].min())
    xxmin = np.append(xxmin, df.low.iloc[i:i+wind].idxmin())
for i in range(candleid-backcandles, candleid+1, wind):
    maxim = np.append(maxim, df.high.loc[i:i+wind].max())
    xxmax = np.append(xxmax, df.high.iloc[i:i+wind].idxmax())
slmin, intercmin = np.polyfit(xxmin, minim,1)
slmax, intercmax = np.polyfit(xxmax, maxim,1)

dfpl = df[candleid-backcandles:candleid+backcandles]
fig = go.Figure(data=[go.Candlestick(x=dfpl.index,
                open=dfpl['open'],
                high=dfpl['high'],
                low=dfpl['low'],
                close=dfpl['close'])])
fig.add_trace(go.Scatter(x=xxmin, y=slmin*xxmin + intercmin, mode='lines', name='min slope'))
fig.add_trace(go.Scatter(x=xxmax, y=slmax*xxmax + intercmax, mode='lines', name='max slope'))