#!/usr/bin/env python3
"""
Live bid/ask data for CME Gold Futures via Databento.

Requires:
    pip install databento python-dotenv

API key is loaded automatically from .env file in the same directory.
"""

import os
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from databento import Live

# Load .env from script directory
load_dotenv(Path(__file__).parent / ".env")

# CME Globex dataset
DATASET = "GLBX.MDP3"

# Gold futures root symbol on COMEX
# GC = Gold Futures
GOLD_FUTURES_ROOT = "GC"

# Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
#              N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
}


def get_expiry_code(year: int, month: int) -> str:
    """Convert year/month to CME expiry code (e.g., H6 for March 2026)."""
    month_code = MONTH_CODES[month]
    year_code = str(year)[-1:]  # Last digit
    return f"{month_code}{year_code}"


def build_futures_symbol(expiry_code: str) -> str:
    """
    Build symbol for gold futures with given expiry.

    Example: GCH6 for March 2026 gold futures.
    """
    return f"{GOLD_FUTURES_ROOT}{expiry_code}"


def run_live_feed(
    expiry_year: int,
    expiry_month: int,
    api_key: Optional[str] = None,
):
    """
    Stream live bid/ask data for gold futures.

    Args:
        expiry_year: Futures expiry year (e.g., 2026)
        expiry_month: Futures expiry month (1-12)
        api_key: Databento API key (or set DATABENTO_API_KEY env var)
    """
    api_key = api_key or os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        raise ValueError("API key required. Set DATABENTO_API_KEY or pass api_key param.")

    expiry_code = get_expiry_code(expiry_year, expiry_month)
    symbol = build_futures_symbol(expiry_code)

    print(f"Connecting to Databento live feed...")
    print(f"Dataset: {DATASET}")
    print(f"Symbol: {symbol}")
    print(f"Expiry: {MONTH_CODES[expiry_month]} {expiry_year} ({expiry_code})")
    print("-" * 60)

    client = Live(key=api_key)

    # Subscribe to best bid/offer (BBO) data
    client.subscribe(
        dataset=DATASET,
        schema="mbp-1",  # Top of book bid/ask
        symbols=[symbol],
    )

    print("Streaming bid/ask data (Ctrl+C to stop)...\n")

    for record in client:
        symbol_str = str(record.symbol) if hasattr(record, 'symbol') else symbol

        # Extract bid/ask from MBP-1 record
        if hasattr(record, 'levels') and record.levels:
            level = record.levels[0]
            bid_price = level.bid_px / 1e9 if level.bid_px else None
            ask_price = level.ask_px / 1e9 if level.ask_px else None
            bid_size = level.bid_sz
            ask_size = level.ask_sz

            ts = datetime.fromtimestamp(record.ts_event / 1e9)

            print(
                f"{ts.strftime('%H:%M:%S.%f')[:-3]} | {symbol_str:12} | "
                f"Bid: {bid_price:>10.2f} x {bid_size:<4} | "
                f"Ask: {ask_price:>10.2f} x {ask_size:<4}"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Stream live CME Gold Futures bid/ask data via Databento"
    )
    parser.add_argument(
        "--year", "-y",
        type=int,
        default=2026,
        help="Expiry year (default: 2026)"
    )
    parser.add_argument(
        "--month", "-m",
        type=int,
        default=3,
        help="Expiry month 1-12 (default: 3 for March)"
    )
    parser.add_argument(
        "--api-key", "-k",
        type=str,
        help="Databento API key (or set DATABENTO_API_KEY env var)"
    )

    args = parser.parse_args()

    try:
        run_live_feed(
            expiry_year=args.year,
            expiry_month=args.month,
            api_key=args.api_key,
        )
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
