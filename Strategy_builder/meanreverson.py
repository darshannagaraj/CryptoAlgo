window = 3  # Number of periods for rolling mean and standard deviation
df['rolling_mean'] = df['close'].rolling(window=window).mean()
df['rolling_std'] = df['close'].rolling(window=window).std()

# Define mean reversion threshold (in terms of standard deviations)
threshold = 1.0  # You can adjust this threshold based on your strategy

# Initialize trade signals
df['long_signal'] = 0
df['short_signal'] = 0

# Generate trade signals
for i in range(window, len(df)):
    if df['close'][i] > df['rolling_mean'][i] + threshold * df['rolling_std'][i]:
        df['short_signal'][i] = -1  # Short signal (sell)
    elif df['close'][i] < df['rolling_mean'][i] - threshold * df['rolling_std'][i]:
        df['long_signal'][i] = 1  # Long signal (buy)

# Print the signals
print(df[['close', 'rolling_mean', 'rolling_std', 'long_signal', 'short_signal']])