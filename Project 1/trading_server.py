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
        """
        Initializes the database manager with a given database path and option to start with a blank database.

        Parameters:
        - db_path (str): Path to the SQLite database file.
        - blank_db (bool): If True, clears the existing data in the database. Default is False.
        """
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
        """
        Writes a given DataFrame to the SQLite database.

        Parameters:
        - df (DataFrame): The DataFrame to be written into the 'stock_data' table.
        """
        df = df[['datetime','ticker','price','signal','pnl']]
        df.to_sql('stock_data', self.conn, if_exists='append', index=False)

    def close(self):
        """Closes the database connection."""

        self.conn.close()


class TradingServer:
    def __init__(self, port, tickers, host, interval='5min',reset_db=False,retrieve_historic=False):
        """
        Initializes the trading server with specified settings.

        Parameters:
        - port (int): The port number for the server.
        - tickers (str): A comma-separated string of ticker symbols to monitor.
        - host (str): The hostname or IP address to listen on.
        - interval (str): The interval for fetching data. One of: ['1min', '5min', '15min', '30min', '60min'].
        - reset_db (bool): Whether to reset the database on startup. Default is False.
        """
        self.port = port
        self.host = host
        self.retrieve_historic=retrieve_historic
        self.tickers = tickers.split(',')
        if interval.strip() not in ['1min', '5min', '15min', '30min', '60min']:
            raise ValueError("Interval must be one of: ['1min', '5min', '15min', '30min', '60min']",flush=True)
        self.interval = interval.strip()
        self.interval_int = int(self.interval.replace("min",""))
        self.alphavantage = 1

        #Thread will sleep for 12*number of tickers with missing data for the "data" function
        self.alphavantage_sleep = 0

        self.finnhub = 1
        self.month_offsets = {ticker: 0 for ticker in self.tickers}

        self.db = DatabaseManager(blank_db=reset_db)
        self.start_data_fetch_threads()

    def start_data_fetch_threads(self):
        """
        Starts background threads for fetching data and handling server requests.
        """
        if self.retreive_historic:
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
        """
        Continuously fetches and updates historical data for monitored tickers.
        """
        while True:
            for ticker in list(self.tickers):
                try:
                    target_Y_m = (datetime.now() - timedelta(days=30 * self.month_offsets[ticker])).strftime('%Y-%m')
                    #API does not support earlier dates
                    if target_Y_m <datetime.strptime('2000-01-01','%Y-%m-%d'):
                        continue
                    self.initialize_server_data(interval=self.interval, ticker=ticker,target_Y_m= target_Y_m)
                    self.month_offsets[ticker] += 1
                except Exception as e:
                    print(f"Error fetching data for {ticker}: {e}")

                time.sleep(12)

    def fetch_and_update_quotes_loop(self):
        """
        Continuously fetches and updates the latest quotes for monitored tickers.
        """
        quote_update_interval = 60  #update every minute
        while True:
            for ticker in self.tickers:
                self.fetch_and_update_quote(ticker)
                time.sleep(1)
            time.sleep(quote_update_interval)

    def fetch_and_update_quote(self, ticker):
        """
        Fetches and updates the database with the latest quote for a given ticker.

        Parameters:
        - ticker (str): The ticker symbol to fetch the quote for.
        """
        url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.finnhub}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            current_price = data['c']

            query = f"""SELECT * FROM stock_data WHERE ticker = ? AND datetime > ? ORDER BY datetime ASC"""
            one_day_ago = datetime.now() - timedelta(days=1)
            self.db.cursor.execute(query, (ticker, one_day_ago.strftime('%Y-%m-%d %H:%M:%S')))
            rows = self.db.cursor.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=['datetime', 'ticker', 'price', 'signal', 'pnl'])
            else:
                df = pd.DataFrame(columns=['datetime', 'ticker', 'price', 'signal', 'pnl'])

            new_row_df = pd.DataFrame([{'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'ticker': ticker,
                                        'price': current_price, 'signal': np.nan, 'pnl': np.nan}])
            df = pd.concat([df, new_row_df], ignore_index=True)
            df = self.calculate_signal_and_pnl(df)
            df.reset_index(inplace=True)
            # Update the database
            self.db.write_dataframe_to_sqlite(df.tail(1))  # Only write the LATEST row

            return True
        else:
            print(f"Failed to fetch quote for {ticker}")
            return False
    def check_ticker_validity(self,ticker):
        """
        Checks if a given ticker is valid and recognized by the Finnhub API.

        Parameters:
        - ticker (str): The ticker symbol to validate.

        Returns:
        - bool: True if the ticker is valid, False otherwise.
        """
        url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.finnhub}'
        response = requests.get(url)
        data = response.json()

        #Previous close is 0 and change since previous close is None, meaning there is no data available.
        #This means that the server does not support this ticker, or it does not exist.
        if response.status_code != 200 or (data['pc'] != 0 and data['dp'] != None):
            return True
        else:
            return False
    def add_ticker(self, ticker):
        """
        Adds a new ticker to the list of tickers to monitor.

        Parameters:
        - ticker (str): The ticker symbol to add.

        Returns:
        - str: A message indicating the result of the operation.
        """
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

    def initialize_server_data(self, interval, ticker, target_Y_m):
        """
        Initializes the server data for a given ticker, interval, and target year-month.

        Parameters:
        - interval (str): The data interval.
        - ticker (str): The ticker symbol.
        - target_Y_m (str): The target year and month in 'YYYY-MM' format.
        """
        json_data = self.fetch_intraday_series_for_month(ticker, interval, target_Y_m)
        if not json_data:
            print(f"No data for {ticker} in month {target_Y_m}")
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
        """
        Calculates trading signals and profit/loss based on a given DataFrame.

        Parameters:
        - df (DataFrame): The DataFrame containing price data.

        Returns:
        - DataFrame: The updated DataFrame with signal and PnL columns.
        """
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        df.sort_values('datetime', inplace=True)

        # Calculate moving average and standard deviation over a rolling window
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

    def fetch_intraday_series_for_month(self, ticker, interval, target_Y_m=None):
        """
        Fetches intraday time series data for a given ticker, interval, and target month.

        Parameters:
        - ticker (str): The ticker symbol.
        - interval (str): The data interval.
        - target_Y_m (str, optional): The target year and month in 'YYYY-MM' format.

        Returns:
        - dict/DataFrame: The fetched data as a dictionary or an empty DataFrame if no data found.
        """

        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&{"&month="+target_Y_m if target_Y_m!= None else ""}&outputsize=full&apikey={self.alphavantage}'
        response = requests.get(url)
        data = response.json()

        if "Error Message" in data:
            raise ValueError(f"API Error for {ticker}: {data['Error Message']}")

        time_series_key = f"Time Series ({interval})"
        if time_series_key in data:
            return data
        else:
            print(f"No data found for {ticker} for {target_Y_m}.")
            return pd.DataFrame()

    def run_asyncio_server(self):
        '''
        Sets up a new event loop for the thread, starts the asyncio server,
        and runs the event loop until the server is closed.
        '''
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_server_setup())

        # Keep the server running
        try:
            loop.run_forever()
        finally:
            loop.close()
            print("Event loop closed.")

    async def async_server_setup(self):
        """
        Configures and starts the asyncio server, printing the serving address.
        """

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}', flush=True)

        async with server:
            await server.serve_forever()

    def delete_ticker(self, ticker):
        """
        Deletes a ticker from the monitoring list and its data from the database.

        Parameters:
        - ticker (str): The ticker symbol to delete.
        """
        if ticker in self.tickers:
            self.tickers.remove(ticker)
            query = "DELETE FROM stock_data WHERE ticker = ?"
            self.db.cursor.execute(query, (ticker,))
            self.db.conn.commit()
            print(f"Deleted all entries for {ticker} and removed from tickers list.")

    def generate_report(self):
        """
        Generates a CSV report of the data stored in the database.
        """
        df = pd.read_sql_query("SELECT * FROM stock_data", self.db.conn)
        report_path = "server_report.csv"
        df.to_csv(report_path, index=False)
        print(f"Report generated and saved to {report_path}")

    async def stream_report(self, writer):
        """"
        Streams the generated report to a client.

        Parameters:
        - writer (StreamWriter): The StreamWriter object to write data to the client.
        """

        report_path = "server_report.csv"
        # Calculate and send file size first
        file_size = os.path.getsize(report_path)
        header = f"Content-Length: {file_size}\n"
        writer.write(header.encode())
        await writer.drain()

        # Now send the file content
        with open(report_path, "rb") as file:
            while chunk := file.read(4096):  # chunks of 4 KB
                writer.write(chunk)
                await writer.drain()

    async def query_data_as_of(self, datetime_str):
        # Convert the datetime string to a datetime object
        query_datetime = datetime.strptime(datetime_str, "%Y-%m-%d-%H:%M")

        # Check if the data is available in the database
        response = ""
        missing = 0
        tickers_missing = []
        for ticker in self.tickers:
            data_available, data = self.check_data_availability(query_datetime,ticker)
            if not data_available:
                #In case it is more recent
                self.fetch_and_update_quote(ticker)
            data_available, data = self.check_data_availability(query_datetime,ticker)

            if not data_available:
                missing+=1
                tickers_missing.append(ticker)
            else:
                response += data
        if missing!=0:
            self.alphavantage_sleep = 12*(missing+1)
            time.sleep(12) #Wait in case there was a query that was initiated immediately when this line was run.
            for ticker in tickers_missing:
                self.initialize_server_data(ticker=ticker,interval=self.interval,target_Y_m=query_datetime.strftime("%Y-%m"))
                data_available, data = self.check_data_availability(query_datetime,ticker)
                if not data_available:
                    response+=f"Unable to retreive data or signal for {ticker}"
                else:
                    response += data
        # Format and return the data response
        return data

    def check_data_availability(self, query_datetime,ticker):
        query_datetime_str = query_datetime.strftime('%Y-%m-%d %H:%M:%S')
        start_datetime = query_datetime - timedelta(minutes=self.interval_int)
        start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') # Most recent interval would be contained in this

        query = "SELECT ticker, price, signal FROM stock_data WHERE datetime <= ? and datetime >= ? and datetime and ticker= ? ORDER BY datetime DESC LIMIT 1;"
        self.db.cursor.execute(query, (query_datetime_str,start_datetime_str,ticker))  # Use self.db.cursor
        result = self.db.cursor.fetchall()  # Use self.db.cursor

        if result:
            data = f"Data available for {query_datetime_str}: " + ", ".join(
                [f"{row[0]}: Price={row[1]}, Signal={row[2]}" for row in result])
            return True, data
        else:
            return False, "No data available."

    def calculate_missing_data_points(self, query_datetime):
        fixed_interval_minutes = 5  # Assuming 5-minute intervals for simplicity
        self.db.cursor.execute("SELECT datetime FROM stock_data ORDER BY datetime DESC LIMIT 1;")  # Use self.db.cursor
        last_data_point = self.db.cursor.fetchone()  # Use self.db.cursor

        if last_data_point:
            last_data_datetime = datetime.strptime(last_data_point[0], '%Y-%m-%d %H:%M:%S')
            delta = query_datetime - last_data_datetime
            missing_intervals = delta.total_seconds() / (fixed_interval_minutes * 60)
            return int(missing_intervals)
        else:
            return 0

    def fetch_and_update_data_as_of(self, query_datetime):
        try:
            for ticker in self.tickers:
                print(ticker)
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker.strip()}&interval={self.interval}&{"&month="+query_datetime.strftime("%Y-%m") if query_datetime!= None else ""}&outputsize=full&apikey={self.alphavantage}'
                response = requests.get(url)
                data = response.json()
        except Exception as e:
            print(f"Failed to fetch data for {ticker} as of {query_datetime}: {str(e)}")

        # After fetching, you might need to adjust `alphavantage_sleep` back or perform other cleanup actions
        self.alphavantage_sleep = max(0, self.alphavantage_sleep - 12)  # Example adjustment


    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handles client connections,  commands, and responses.

        Commands supported: add, delete, report, data, quit

        Parameters:
        - reader: The object to read data from the client.
        - writer: The object to send data to the client.
        """
        print('Client connected', flush=True)
        try:
            while True:
                try:
                    data = await reader.read(1024)
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
                    elif message.startswith('data '):
                        datetime_str = message[5:]
                        response = await self.query_data_as_of(datetime_str)
                        writer.write(response.encode() + b'\n')
                        await writer.drain()

                    elif message.startswith('delete '):
                        ticker = message.split(' ')[1]
                        self.delete_ticker(ticker)
                        response = f"Deleted ticker: {ticker}"
                    elif message == 'report':
                        self.generate_report()
                        await self.stream_report(writer)
                        continue
                    else:
                        response = "Unknown command or message"

                    writer.write(response.encode())
                    await writer.drain()
                    print("Sent response",flush=True)
                except Exception as e:
                    response = "There was an error with your input: \n"+str(e)
                    writer.write(response.encode())
                    await writer.drain()
                    print("Sent response", flush=True)


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
    parser.add_argument("--tickers", type=str, help="List of tickers, as strings in the form: ticker1,ticker2,ticker3...")
    parser.add_argument("--port", type=int, default=8000, help="Network port for the server")
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to listen on')
    parser.add_argument('--reset_db', type=bool, default=False, help='Delete existing database on startup')
    parser.add_argument('--retrieve_historic', type=bool, default=False, help='Continuously retrieve historic data from alphavantage (not recommended given API limitations).')
    parser.add_argument('--interval', type=str, default='5min', help='Interval we want to obtain our historic data at. \
                                                                    Can be one of: 1min, 5min, 15min, 30min, 60min.')

    args = parser.parse_args()
    trading_Server = TradingServer(port=args.port,tickers=args.tickers,host=args.host,interval='5min',reset_db=args.reset_db)
    try:
        while True:
            time.sleep(1)  # Keep the main thread alive with minimal CPU usage.
    except KeyboardInterrupt:
        print("Exiting the trading server...")