# src/ingest_prices.py
import yfinance as yf
import pandas as pd

def fetch(ticker: str, start: str, end: str):
    df = yf.download(ticker, start=start, end=end)
    df.to_csv(f"data/raw/{ticker}.csv")
    print(f"Saved {len(df)} rows for {ticker}")

if __name__ == "__main__":
    import os
    os.makedirs("data/raw", exist_ok=True)
    fetch("AAPL", "2020-01-01", "2020-12-31")
