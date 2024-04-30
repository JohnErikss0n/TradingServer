# Trading Server README

## Overview

This program initializes a trading server designed to monitor stock tickers, fetch real-time and historical data, and calculate trading signals and profit/loss based on price movements. It leverages data from Alphavantage and Finnhub APIs.

### Note: the free version of the alphavantage API is now limited to 25 calls per day. Currently, the server will start crawling backwards to build a time series of monthly data every 12 seconds (i.e. the server will attempt to make one call to the API every 12 seconds).   

## Features

- Fetch and update real-time quotes for specified tickers.
- Retrieve historical data for tickers (optional, based on settings).
- Calculate trading signals and profit/loss.
- Add or remove tickers dynamically.
- Generate a report of the data stored in the database.
- Serve data to clients over a network connection using asyncio.

## Environment Set Up
Using conda, run:

` conda env create -f environment.yml `

Activate the environment using:

` conda activate project_1_env `

## Running the program
Below is a sample command line input for the server:

`
python trading_server.py --tickers AAPL,MSFT,GOOGL --port 8000 --host 0.0.0.0 --reset_db False --interval 5min --finnhub_key YOUR_KEY_HERE --alphavantage_key YOUR_KEY_HERE
`

Below is a sample command line input for the client:

`
python trading_client.py --server 127.0.0.1:8000
`
