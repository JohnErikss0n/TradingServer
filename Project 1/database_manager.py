import numpy as np
import pandas as pd
import sqlite3
import os

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

    def close(self):
        """Closes the database connection."""
        self.conn.close()
