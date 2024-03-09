import argparse
import pandas as pd
import requests
import json
import socket
import sys
from influxdb import InfluxDBClient
import numpy as np
import time
import threading
from datetime import datetime, timedelta
import sqlite3
import asyncio
from asyncio import StreamReader, StreamWriter


from datetime import datetime, timedelta




class DatabaseManager:
    def __init__(self, db_path='stock_prices.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS stock_data
                               (datetime TEXT, ticker TEXT, price REAL, signal INTEGER, pnl REAL)''')
        self.conn.commit()

    def write_dataframe_to_sqlite(self, df):
        df.to_sql('stock_data', self.conn, if_exists='append', index=False)

    def close(self):
        self.conn.close()


class TradingServer:
    def __init__(self, port, tickers, host, interval='5min'):
        self.port = port
        self.host = host
        self.tickers = tickers
        self.interval = interval
        self.api_key = None
        self.api_key_2 = None

        self.db = DatabaseManager()
        self.start_data_fetch_threads()

    def start_data_fetch_threads(self):
        self.data_fetch_thread = threading.Thread(target=self.fetch_and_update_data_loop)
        self.data_fetch_thread.daemon = True
        self.data_fetch_thread.start()

        self.quote_fetch_thread = threading.Thread(target=self.fetch_and_update_quotes_loop)
        self.quote_fetch_thread.daemon = True
        self.quote_fetch_thread.start()

        self.quote_fetch_thread = threading.Thread(target=self.start_asyncio_server)
        self.quote_fetch_thread.daemon = True
        self.quote_fetch_thread.start()

    def fetch_and_update_data_loop(self):
        month_offsets = {ticker: 0 for ticker in self.tickers}
        while True:
            for ticker in list(self.tickers):
                target_month = (datetime.now() - timedelta(days=30 * month_offsets[ticker])).strftime('%Y-%m')
                try:
                    self.initialize_server_data(self.interval, ticker, target_month)
                    month_offsets[ticker] += 1
                except Exception as e:
                    print(f"Error fetching data for {ticker}: {e}")
                time.sleep(12)

    def fetch_and_update_quotes_loop(self):
        quote_update_interval = 60  # TODO: update interval
        while True:
            for ticker in self.tickers:
                self.fetch_and_update_quote(ticker)
                time.sleep(1)
            time.sleep(quote_update_interval)

    def fetch_and_update_quote(self, ticker):
        url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.api_key_2}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Implement function to write quote to database or another storage
            print(f"Quote for {ticker}: {data}")
        else:
            print(f"Failed to fetch quote for {ticker}")

    def add_ticker(self, ticker):
        if ticker not in self.tickers:
            self.tickers.append(ticker)

    def initialize_server_data(self, interval, ticker, month):
        json_data = self.fetch_intraday_series_for_month(ticker, interval, month)
        if not json_data:
            print(f"No data for {ticker} in month {month}")
            return
        time_series = json_data.get(f"Time Series ({interval})", {})
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index.name = 'datetime'
        df.reset_index(inplace=True)
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df['price'] = (df['open'].astype(float) + df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 4
        df['ticker'] = ticker
        df = self.calculate_signal_and_pnl(df)
        self.db.write_dataframe_to_sqlite(df)

    def calculate_signal_and_pnl(self,df):
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        df.sort_values('datetime', inplace=True)

        # Calculate moving average and standard deviation over a 24H rolling window
        df['S_avg'] = df['price'].rolling('24H').mean()
        df['Sigma'] = df['price'].rolling('24H').std()

        df['Pos'] = np.nan

        df.loc[df['price'] > (df['S_avg'] + df['Sigma']), 'Pos'] = 1  # Buy signal
        df.loc[df['price'] < (df['S_avg'] - df['Sigma']), 'Pos'] = -1  # Sell signal

        df['Pos'] = df['Pos'].shift(1)

        df['Pos'].fillna(method='ffill', inplace=True)
        df['Pos'].fillna(0, inplace=True)

        df['pnl'] = df['Pos'].shift(1) * (df['price'] - df['price'].shift(1))

        df['pnl'].fillna(0, inplace=True)
        df = df.rename(columns={"Pos":"signal"})
        return df
    def fetch_intraday_series_for_month(self, ticker, interval, month=None):
        '''uncomment when ready, until then just use data.json'''

        #url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval={interval}&{"&month="+month if month!= None else ""}&outputsize=full&apikey={self.api_key}'
        #response = requests.get(url)
        #data = response.json()
        # Check for an error message in API response
        with open('data.json', 'r') as file:
            data = json.load(file)

        if "Error Message" in data:
            raise ValueError(f"API Error for {ticker}: {data['Error Message']}")

        time_series_key = f"Time Series ({interval})"
        if time_series_key in data:
            return data
        else:
            print(f"No data found for {ticker} for {month}.")
            return pd.DataFrame()

    async def start_asyncio_server(self):
        # Simplified to use the modern asyncio server start method
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        # Example handler logic
        data = await reader.read(100)  # Adjust based on your protocol
        print(f"Received: {data.decode()}")

        response = "Hello, client!"
        writer.write(response.encode())
        await writer.drain()

        writer.close()
        await writer.wait_closed()
        print("Connection closed.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trading Server")
    parser.add_argument("--tickers", nargs='+', default=[],help="List of tickers")
    parser.add_argument("--port", type=int, default=8000, help="Network port for the server")
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to listen on')

    args = parser.parse_args()

    trading_Server = TradingServer(args.port,args.tickers,args.host,'5min')
    try:
        while True:
            time.sleep(1)  # Keep the main thread alive with minimal CPU usage.
    except KeyboardInterrupt:
        print("Exiting the trading server...")