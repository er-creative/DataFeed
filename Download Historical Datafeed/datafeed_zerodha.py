import requests
import pandas as pd
import time 
from datetime import date, timedelta

start_date = date(2021, 3, 15)
end_date = date(2024, 3, 14)

userid = 'USERID'
timeframe = 'minute'

auth_token = 'YOUR_AUTH_KEY'
headers = {'Authorization': auth_token}

token_df = pd.read_excel(r'E:\ListOne\token_symbol.xlsx') #File path name of all symbols
token_column_name = 'TOKEN' 

for i in range(0, len(token_df)):
    start1 = time.time()
    token = token_df.loc[i]['TOKEN']
    columns = ['timestamp', 'Open', 'High', 'Low', 'Close', 'V', 'OI']  # Define columns before the loop
    final_df = pd.DataFrame(columns=columns)  # Initialize DataFrame with columns

    from_date = start_date

    while from_date < end_date:
        to_date = from_date + timedelta(days=30)
        url = f'https://kite.zerodha.com/oms/instruments/historical/{token}/{timeframe}?user_id={userid}&oi=1&from={from_date}&to={to_date}'
        resJson = requests.get(url, headers=headers).json()
        candelinfo = resJson['data']['candles']
        df = pd.DataFrame(candelinfo, columns=columns)
        final_df = pd.concat([final_df, df], ignore_index=True)
        from_date = from_date + timedelta(days=31)

    # Rest of your code...
    final_df['timestamp'] = pd.to_datetime(final_df['timestamp'], format='%Y-%m-%dT%H:%M:%S%z')

    final_df['date'] = pd.to_datetime(final_df['timestamp']).dt.date
    final_df['time'] = pd.to_datetime(final_df['timestamp']).dt.strftime('%H:%M')
    final_df['ticker'] = token_df.loc[i]['SYMBOL']
    
    # Drop the 'timestamp' and OI' column
    final_df.drop('timestamp', axis=1, inplace=True)
    final_df.drop('OI', axis=1, inplace=True)
    
    #final_df = final_df[['ticker', 'date', 'Open', 'High', 'Low', 'Close', 'V', 'OI', 'time']]
    final_df = final_df[['ticker', 'date', 'Open', 'High', 'Low', 'Close', 'V', 'time']]  # Removed 'OI' from here

    filename = str(token_df.loc[i]['SYMBOL']) + '.txt'
    final_df.to_csv('E:/ListOne/'+ filename, header=False, index=False) #save txt file
    print(time.time() - start1)
    print(final_df)


