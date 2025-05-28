"""
ingest_news.py: Fetch company-specific news from Finnhub in 30-day chunks.
"""
import os
import sys
import argparse
import logging
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

def make_30d_windows(start_date, end_date):
    """Split the overall date range into 30-day windows."""
    current = start_date
    windows = []
    while current <= end_date:
        window_end = min(current + timedelta(days=29), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows

def fetch_news_window(symbol, start_dt, end_dt, api_key, max_retries=3, backoff=5):
    """Fetch a single window of news for a symbol, with retry logic."""
    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": symbol,
        "from": start_dt.strftime("%Y-%m-%d"),
        "to": end_dt.strftime("%Y-%m-%d"),
        "token": api_key
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                df = pd.json_normalize(data)
                if not df.empty:
                    df['symbol'] = symbol
                    df['datetime'] = pd.to_datetime(
                        df['datetime'], unit='s', errors='coerce'
                    )
                return df
            elif resp.status_code == 429:
                logging.warning(
                    f"Rate limit hit for {symbol} {start_dt}→{end_dt}. Sleeping {backoff}s."
                )
                time.sleep(backoff)
            else:
                resp.raise_for_status()
        except Exception as e:
            logging.error(
                f"Error fetching {symbol} {start_dt}→{end_dt} (attempt {attempt}): {e}"
            )
            time.sleep(backoff)
    logging.error(
        f"Failed to fetch after {max_retries} attempts: {symbol} {start_dt}→{end_dt}"
    )
    return pd.DataFrame()

def parse_symbols_file(path):
    """Read tickers from a file, ignoring empty lines and comments."""
    symbols = []
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            symbols.append(line)
    return symbols

def main():
    parser = argparse.ArgumentParser(
        description="Ingest Finnhub company-news in 30-day windows."
    )
    parser.add_argument(
        "--symbols", required=True,
        help="Path to symbols.txt (one ticker per line)."
    )
    parser.add_argument(
        "--start", required=True,
        help="Start date YYYY-MM-DD"
    )
    parser.add_argument(
        "--end", required=True,
        help="End date YYYY-MM-DD"
    )
    parser.add_argument(
        "--output-dir", default="data/raw/news/finnhub",
        help="Base output directory"
    )
    parser.add_argument(
        "--format", choices=["csv", "parquet"], default="csv",
        help="Output file format"
    )
    args = parser.parse_args()

    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        logging.error("FINNHUB_API_KEY not set in environment.")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").date()
    symbols = parse_symbols_file(args.symbols)

    for symbol in symbols:
        logging.info(f"Processing symbol: {symbol}")
        out_dir = os.path.join(args.output_dir, symbol)
        os.makedirs(out_dir, exist_ok=True)
        windows = make_30d_windows(start_dt, end_dt)

        for window_start, window_end in windows:
            df_win = fetch_news_window(symbol, window_start, window_end, api_key)
            if df_win.empty:
                logging.info(
                    f"No articles for {symbol} between {window_start} and {window_end}"
                )
                continue
            
            df_win.drop_duplicates(subset=['url'], inplace=True)
            for col in ['category', 'image', 'related','source','url']:
                if col in df_win.columns:
                    df_win.drop(columns=[col], inplace=True)
            
            file_name = (
                f"{symbol}_{window_start.strftime('%Y-%m-%d')}_{window_end.strftime('%Y-%m-%d')}.{args.format}"
            )
            file_path = os.path.join(out_dir, file_name)

            if args.format == 'csv':
                df_win.to_csv(file_path, index=False)
            else:
                df_win.to_parquet(file_path, index=False)

            logging.info(f"Wrote {len(df_win)} rows to {file_path}")

if __name__ == "__main__":
    main()
