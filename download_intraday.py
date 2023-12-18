import sekrets
import requests
import yfinance 

import pandas as pd
import datetime as dt
import os

# This script downloads the last 2 years of historical data from Yahoo Finance
# and then downloads the intraday data from Marketstack for each day in the
# historical data. The intraday data is saved in a separate CSV file for each
# day in the historical data.

# list of stocks to download

STOCKS = ["NVDA", "SQ", "SHOP", "TSLA", "AMD", "MSFT"]

# create 2 year ago date in YYYY-MM-DD format
start_date_string = (dt.datetime.now() - dt.timedelta(days=365*2)).strftime("%Y-%m-%d")
end_date_string = dt.datetime.now().strftime("%Y-%m-%d")

def download_yf_data():
    # for each stock, download the last 1 year of historical data from Yahoo Finance
    for stock in STOCKS:
        df = yfinance.download(stock, start=start_date_string, end=end_date_string)
        df.to_csv(f'input_data/{stock}_yf.csv')

def download_marketstack_data(stock):

    #open the respective csv file
    df = pd.read_csv(f'input_data/{stock}_yf.csv')

    # create "input_data/{stock}" folder if it doesn't exist
    if not os.path.exists(f'input_data/{stock}'):
        os.makedirs(f'input_data/{stock}')

    # for each date in the csv file, download the intraday data from Marketstack
    for index, row in df.iterrows():
        date_string = row['Date']
        print(stock, date_string)

        params = {
            'access_key': sekrets.MARKETSTACK_API_TOKEN,
            'symbols': stock,
            'interval': '1min',
            'sort' : 'ASC',
            'limit': 1000
        }

        api_result = requests.get(f'https://api.marketstack.com/v1/intraday/{date_string}', params)

        api_response = api_result.json()

        if 'error' in api_response.keys():
            print(f"API error: {api_response['error']['message']}")
        else:
            #print(api_response)
            df = pd.DataFrame(api_response['data'])
            df.to_csv(f'input_data/{stock}/{date_string}.csv')

if __name__ == "__main__":
    #download_yf_data()

    for stock in STOCKS:
        download_marketstack_data(stock)