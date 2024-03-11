import pandas as pd
import sqlite3
import os
from dateutil.relativedelta import relativedelta

class DatabaseManager:
    def __init__(self, db_path='stock_prices.db', blank_db=False):
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
                               (datetime DATETIME, ticker TEXT, price REAL, signal INTEGER, pnl REAL)''')
        self.conn.commit()

    def write_dataframe_to_sqlite(self, df):
        """
        Writes a given DataFrame to the SQLite database.

        Parameters:
        - df (DataFrame): The DataFrame to be written into the 'stock_data' table.
        """
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[['datetime','ticker','price','signal','pnl']].dropna()
        df.to_sql('stock_data', self.conn, if_exists='append', index=False)

    def is_data_available_for_month(self, query_datetime, ticker):
        """
        Checks if there's data for the specific month and ticker.
        """
        start_of_month = query_datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_of_month = (start_of_month + relativedelta(months=1)).strftime('%Y-%m-%d %H:%M:%S')
        start_of_month = start_of_month.strftime('%Y-%m-%d %H:%M:%S')
        query = '''SELECT COUNT(*) FROM stock_data
                   WHERE ticker = ? AND datetime >= ? AND datetime < ?;'''

        self.cursor.execute(query, (ticker, start_of_month, end_of_month))
        result = self.cursor.fetchone()
        return result[0] > 0

    def close(self):
        """Closes the database connection."""
        self.conn.close()
