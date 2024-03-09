import argparse
import pandas as pd
import requests
import json
import os
import numpy as np
import time
import threading
import sqlite3
import asyncio


from datetime import datetime, timedelta




class DatabaseManager:
    def __init__(self, db_path='stock_prices.db',blank_db = False):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        if blank_db and os.path.exists(self.db_path):
            self.cursor.execute('''DELETE FROM stock_data''')
            self.conn.commit()

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS stock_data
                               (datetime TEXT, ticker TEXT, price REAL, signal INTEGER, pnl REAL)''')
        self.conn.commit()

    def write_dataframe_to_sqlite(self, df):
        df = df[['datetime','ticker','price','signal','pnl']]
        df.to_sql('stock_data', self.conn, if_exists='append', index=False)

    def close(self):
        self.conn.close()


class TradingServer:
    def __init__(self, port, tickers, host, interval='5min',reset_db=False):
        self.port = port
        self.host = host
        self.tickers = tickers
        self.interval = interval
        self.api_key = 1
        self.api_key_2 = 1
        self.month_offsets = {ticker: 0 for ticker in self.tickers}

        self.db = DatabaseManager(blank_db=reset_db)
        self.start_data_fetch_threads()

    def start_data_fetch_threads(self):
        self.data_fetch_thread = threading.Thread(target=self.fetch_and_update_data_loop)
        self.data_fetch_thread.daemon = True
        self.data_fetch_thread.start()

        self.quote_fetch_thread = threading.Thread(target=self.fetch_and_update_quotes_loop)
        self.quote_fetch_thread.daemon = True
        self.quote_fetch_thread.start()

        self.quote_fetch_thread = threading.Thread(target=self.run_asyncio_server)
        self.quote_fetch_thread.daemon = True
        self.quote_fetch_thread.start()

    def fetch_and_update_data_loop(self):
        while True:
            for ticker in list(self.tickers):
                target_month = (datetime.now() - timedelta(days=30 * self.month_offsets[ticker])).strftime('%Y-%m')
                try:
                    self.initialize_server_data(self.interval, ticker, target_month)
                    self.month_offsets[ticker] += 1
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
            current_price = (data['o'] + data['h'] + data['l'] + data['c']) / 4  # Example average calculation

            query = f"""SELECT * FROM stock_data WHERE ticker = ? AND datetime > ? ORDER BY datetime ASC"""
            one_day_ago = datetime.now() - timedelta(days=1)
            self.db.cursor.execute(query, (ticker, one_day_ago.strftime('%Y-%m-%d %H:%M:%S')))
            rows = self.db.cursor.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=['datetime', 'ticker', 'price', 'signal', 'pnl'])
            else:
                df = pd.DataFrame(columns=['datetime', 'ticker', 'price', 'signal', 'pnl'])

            # Append the new quote
            new_row = {'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'ticker': ticker,
                       'price': current_price, 'signal': np.nan, 'pnl': np.nan}
            df = df.append(new_row, ignore_index=True)
            df = self.calculate_signal_and_pnl(df)

            # Update the database with the new data point
            self.db.write_dataframe_to_sqlite(df.tail(1))  # Only write the latest row

            print(f"Updated quote for {ticker}: {data}")
            return True
        else:
            print(f"Failed to fetch quote for {ticker}")
            return False
    def check_ticker_validity(self,ticker):
        url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.api_key_2}'
        response = requests.get(url)
        data = response.json()

        #Previous close is 0 and change since previous close is None, meaning there is no data available.
        #This means that the server does not support this ticker, or it does not exist.
        if response.status_code != 200 or (data['pc'] != 0 and data['dp'] != None):
            return True
        else:
            return False
    def add_ticker(self, ticker):
        if ticker not in self.tickers:
            ticker_exists = self.check_ticker_validity(ticker)
            if not ticker_exists:
                return f"Error, {ticker} does not exist or is not recognized by the API."
            self.month_offsets[ticker] = 0
            self.tickers.append(ticker)

            #check if valid ticker
            return f"{ticker} successfully added to the server"
        else:
            return f"{ticker} already being monitored by the server"


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
        df.reset_index(inplace=True)
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

    def run_asyncio_server(self):
        '''
        Sets up a new event loop for the thread, starts the asyncio server,
        and runs the event loop until the server is closed.
        '''
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Now that the new loop is set, run the async server setup and serve_forever
        loop.run_until_complete(self.async_server_setup())

        # Keep the server running
        try:
            loop.run_forever()
        finally:
            loop.close()
            print("Event loop closed.")

    async def async_server_setup(self):
        '''
        Starts the asyncio server and prints the serving address. This method
        should be called within an asyncio event loop.
        '''
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}', flush=True)

        async with server:
            await server.serve_forever()

    def delete_ticker(self, ticker):
        if ticker in self.tickers:
            self.tickers.remove(ticker)
            query = "DELETE FROM stock_data WHERE ticker = ?"
            self.db.cursor.execute(query, (ticker,))
            self.db.conn.commit()
            print(f"Deleted all entries for {ticker} and removed from tickers list.")

    def generate_report(self):
        df = pd.read_sql_query("SELECT * FROM stock_data", self.db.conn)
        report_path = "server_report.csv"
        df.to_csv(report_path, index=False)
        print(f"Report generated and saved to {report_path}")

    async def stream_report(self, writer):
        report_path = "server_report.csv"
        # Calculate and send file size first
        file_size = os.path.getsize(report_path)
        header = f"Content-Length: {file_size}\n"
        writer.write(header.encode())
        await writer.drain()

        # Now send the file content
        with open(report_path, "rb") as file:
            while chunk := file.read(4096):  # Read in chunks of 4 KB
                writer.write(chunk)
                await writer.drain()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        print('Client connected', flush=True)
        try:
            while True:
                data = await reader.read(1024)  # You may adjust buffer size as needed
                if not data:
                    break

                message = data.decode().strip()
                print(f"Received: {message}")

                if message.lower() == 'quit':
                    response = "Closing connection to: Trading Server"
                    writer.write(response.encode())
                    await writer.drain()
                    break
                elif message.startswith('add '):
                    ticker = message.split(' ')[1]
                    response = self.add_ticker(ticker)

                elif message.startswith('delete '):
                    ticker = message.split(' ')[1]
                    self.delete_ticker(ticker)
                    response = f"Deleted ticker: {ticker}"
                elif message == 'report':
                    self.generate_report()
                    await self.stream_report(writer)
                    continue
                elif message == "sql":
                    query = "SELECT * FROM stock_data"
                    self.db.cursor.execute(query)
                    rows = self.db.cursor.fetchall()
                    print(rows,flush=True)
                    response = rows
                else:
                    response = "Unknown command or message"

                writer.write(response.encode())
                await writer.drain()
                print("Sent response",flush=True)

            print("Closing the connection",flush=True)
            writer.close()
            await writer.wait_closed()
            print("Connection closed.",flush=True)
        except Exception as e:
            print(f"An error occurred: {e}",flush=True)
            writer.close()
            await writer.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trading Server")
    parser.add_argument("--tickers", nargs='+', default=[],help="List of tickers")
    parser.add_argument("--port", type=int, default=8000, help="Network port for the server")
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to listen on')
    parser.add_argument('--reset_db', type=bool, default=False, help='Delete existing database on startup')

    args = parser.parse_args()
    print("HJERE ")
    trading_Server = TradingServer(port=args.port,tickers=args.tickers,host=args.host,interval='5min',reset_db=args.reset_db)
    try:
        while True:
            time.sleep(1)  # Keep the main thread alive with minimal CPU usage.
    except KeyboardInterrupt:
        print("Exiting the trading server...")