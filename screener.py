import requests
import pandas as pd
from datetime import date, timedelta
import pandas_ta as ta

def load_access_token(filename="dhan_credentials.txt"):
    with open(filename, "r") as file:
        for line in file:
            if line.startswith("ACCESS_TOKEN="):
                return line.strip().split("=", 1)[1]
    raise Exception("ACCESS_TOKEN not found in credentials file.")

ACCESS_TOKEN = load_access_token()
#print("Access token loaded successfully!")

# Load the CSV
df = pd.read_csv("api-scrip-master-detailed.csv")

# Filter for NSE stock futures
futures_df = df[
    (df['EXCH_ID'] == 'NSE') &
    (df['SEGMENT'] == 'D') &
    (df['INSTRUMENT'] == 'FUTSTK')
]

#print(f"Found {len(futures_df)} NSE stock futures contracts.")
#print(futures_df[['SYMBOL_NAME', 'SECURITY_ID']].head())

# Group by symbol and pick the row with the minimum expiry date
futures_df['EXPIRY_DATE'] = pd.to_datetime(futures_df['SM_EXPIRY_DATE'])
nearest_expiry_df = futures_df.sort_values('EXPIRY_DATE').groupby('SYMBOL_NAME').first().reset_index()

# Build contracts list as before, but from nearest_expiry_df
futures_contracts = []
for _, row in nearest_expiry_df.iterrows():
    contract = {
        "name": row['SYMBOL_NAME'],
        "securityId": str(row['SECURITY_ID']),
        "exchangeSegment": "NSE_FNO",
        "instrument": "FUTSTK",
    }
    futures_contracts.append(contract)

#print(f"Prepared {len(futures_contracts)} contracts for screening.")

#print("Futures symbols to screen:", futures_contracts)

def fetch_daily_ohlcv(security_id, exchange_segment, instrument, access_token, from_date, to_date):
    url = "https://api.dhan.co/v2/charts/historical"
    headers = {
        "Content-Type": "application/json",
        "access-token": access_token
    }
    body = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "fromDate": from_date,
        "toDate": to_date
    }
    response = requests.post(url, headers=headers, json=body)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch daily data: {response.text}")
        return None

def fetch_intraday_ohlcv(security_id, exchange_segment, instrument, access_token, from_date, to_date, interval="60"):
    url = "https://api.dhan.co/v2/charts/intraday"
    headers = {
        "Content-Type": "application/json",
        "access-token": access_token
    }
    body = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "fromDate": "2025-07-22 09:30:00",
        "toDate": "2025-07-22 13:00:00",
        "interval": interval
    }
    response = requests.post(url, headers=headers, json=body)
    # Print the response details
    print("Status Code:", response.status_code)
    if response.status_code == 200:
        data = response.json()
        #print("Response:", data)
        return data
    else:
        print(f"Failed to fetch intraday data: {response.text}")
        return None

def parse_ohlcv_to_df(api_data, tz='Asia/Kolkata'):
    if not api_data or not all(k in api_data for k in ["open", "high", "low", "close", "volume", "timestamp"]):
        print("API data missing required fields.")
        return None
    df = pd.DataFrame({
        "open": api_data["open"],
        "high": api_data["high"],
        "low": api_data["low"],
        "close": api_data["close"],
        "volume": api_data["volume"],
        "timestamp": api_data["timestamp"]
    })
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(tz)
    df.set_index("datetime", inplace=True)
    today = pd.Timestamp(date.today(), tz=tz)
    #df = df[df.index.date == today.date()]
    return df

def resample_to_weekly_include_incomplete(df):
    # This will group by week starting on Monday, so the current week is always included
    weekly = df.resample('W-MON', label='left', closed='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    return weekly

def add_supertrend(df, period=1, multiplier=1.2):
    st = ta.supertrend(
        high=df['high'],
        low=df['low'],
        close=df['close'],
        length=period,
        multiplier=multiplier
    )
    df['supertrend'] = st[f'SUPERT_{period}_{multiplier}']
    return df

def is_supertrend_touch(df):
    if len(df) == 0:
        return False
    last_row = df.iloc[-1]
    supertrend_value = last_row['supertrend']
    return last_row['low'] <= supertrend_value <= last_row['high']

def get_matching_contracts():
    from_date = "2024-01-01"
    # to_date is non-inclusive, so set to tomorrow to include today if available
    to_date = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    today_str = date.today().strftime("%Y-%m-%d")
    tomorrow_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    matching_contracts = []

    for contract in futures_contracts:
        print(f"\nProcessing contract: {contract['name']}")
        # 1. Fetch daily data (up to yesterday)
        daily_data = fetch_daily_ohlcv(
            contract["securityId"],
            contract["exchangeSegment"],
            contract["instrument"],
            ACCESS_TOKEN,
            from_date,
            to_date  # up to today, non-inclusive, so up to yesterday
        )
        daily_df = parse_ohlcv_to_df(daily_data)
        if daily_df is not None:
            print("Raw daily data (last 10 candles):")
            #print(daily_df.tail(10))
        else:
            print(f"Could not parse daily data for {contract['name']}")
            continue

        # 2. Fetch today's intraday data (all 60-min candles)
        intraday_data = fetch_intraday_ohlcv(
            contract["securityId"],
            contract["exchangeSegment"],
            contract["instrument"],
            ACCESS_TOKEN,
            today_str,
            tomorrow_str,
            interval="60"
        )
        intraday_df = parse_ohlcv_to_df(intraday_data)
        #print("\nIntraday 60-min data for today:")
        #print("data for intradat",intraday_df)
        # 3. Aggregate intraday to daily and append if available
        if intraday_df is not None and not intraday_df.empty:
            today_open = intraday_df['open'].iloc[0]
            today_high = intraday_df['high'].max()
            today_low = intraday_df['low'].min()
            today_close = intraday_df['close'].iloc[-1]
            today_volume = intraday_df['volume'].sum()
            today_index = pd.Timestamp(date.today(), tz='Asia/Kolkata')
            today_row = pd.DataFrame({
                'open': [today_open],
                'high': [today_high],
                'low': [today_low],
                'close': [today_close],
                'volume': [today_volume]
            }, index=[today_index])
            #print("Today's aggregated daily candle from intraday data:")
            #print(today_row)
            daily_df = pd.concat([daily_df, today_row])

        # 4. Resample to weekly, filter out future weeks
        weekly_df = resample_to_weekly_include_incomplete(daily_df)
        weekly_df = weekly_df[weekly_df.index <= pd.Timestamp(date.today(), tz='Asia/Kolkata')]
        weekly_df = add_supertrend(weekly_df, period=1, multiplier=1.2)
        #print(f"\nAll weekly OHLCV + Supertrend for {contract['name']}:")
        #print(weekly_df)
        #print("\nLatest weekly candle:")
        #print("Last 5 weekly candles:")
        #print(weekly_df.tail(5))
        # 5. Screener logic
        if is_supertrend_touch(weekly_df):
            matching_contracts.append(contract['name'])
            print(f"✅ {contract['name']}: TOUCHES Supertrend!")
            last_row = weekly_df.iloc[-1]
            print(f"{contract['name']} | Open: {last_row['open']} | High: {last_row['high']} | Low: {last_row['low']} | Close: {last_row['close']} | Supertrend: {last_row['supertrend']}")
        else:
            print(f"❌ {contract['name']}: Does NOT touch Supertrend")

    # Final Results Summary
    print("\n" + "="*50)
    print("SCREENER RESULTS")
    print("="*50)
    if matching_contracts:
        print(f"Found {len(matching_contracts)} contract(s) touching Supertrend:")
        for contract in matching_contracts:
            print(f"  • {contract}")
    else:
        print("No contracts found touching Supertrend.")
    print("="*50)
    return matching_contracts

# Remove the main script execution block
# (No code should run on import)
