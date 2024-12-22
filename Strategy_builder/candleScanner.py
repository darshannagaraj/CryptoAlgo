# Identify "5smalowisolated candle":
#
# This is a candle whose low is the lowest among the last 6 candles.
# This candle's low does not touch the 5SMA.
# Within the next 3 candles after this isolated candle, there must be at least one candle that closes above the 5SMA.
# The lowest low of these candles remains the low of the "5smalowisolated candle".
# Identify "5smahighisolated candle":
#
# This is a candle whose high is the highest among the last 6 candles.
# This candle's high does not touch the 5SMA.
# Within the next 3 candles after this isolated candle, there must be at least one candle that closes below the 5SMA.
# The highest high of these candles remains the high of the "5smahighisolated candle".
# Output:
#
# From the filtered data, list the top 10 "gainers" and top 10 "losers" based on the criteria mentioned.

def identify_isolated_candles(df):
    # Identify potential 5smalowisolated candles
    for i in range(5, len(df) - 3):  # Adjusted index for lookback and lookahead
        is_low_isolated = (df['low'][i] == min(df['low'][i-5:i+1]) and
                           df['low'][i] < df['5SMA'][i] and
                           df['high'][i] < df['5SMA'][i])
        if is_low_isolated:
            if any(df['close'][i+1:i+4] > df['5SMA'][i+1:i+4]) and min(df['low'][i+1:i+4]) == df['low'][i]:
                df.loc[i, '5smalowisolated'] = True
            else:
                df.loc[i, '5smalowisolated'] = False

    # Identify potential 5smahighisolated candles
    for i in range(5, len(df) - 3):
        is_high_isolated = (df['high'][i] == max(df['high'][i-5:i+1]) and
                            df['low'][i] > df['5SMA'][i] and
                            df['high'][i] > df['5SMA'][i])
        if is_high_isolated:
            if any(df['close'][i+1:i+4] < df['5SMA'][i+1:i+4]) and max(df['high'][i+1:i+4]) == df['high'][i]:
                df.loc[i, '5smahighisolated'] = True
            else:
                df.loc[i, '5smahighisolated'] = False

    return df
