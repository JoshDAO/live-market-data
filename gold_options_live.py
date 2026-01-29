#!/usr/bin/env python3
"""
Live bid/ask data for CME Gold Options via Databento.

Requires:
    pip install databento python-dotenv

API key is loaded automatically from .env file in the same directory.
"""

import os
import argparse
import calendar
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from databento import Live

# Load .env from script directory
load_dotenv(Path(__file__).parent / ".env")

# CME Globex dataset
DATASET = "GLBX.MDP3"

# Gold options root symbol on COMEX
# OG = Gold Options (American-style on GC futures)
GOLD_OPTIONS_ROOT = "OG"

# Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
#              N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
}


def get_expiry_code(year: int, month: int) -> str:
    """Convert year/month to CME expiry code (e.g., H26 for March 2026)."""
    month_code = MONTH_CODES[month]
    year_code = str(year)[-1:]  # Last digit
    return f"{month_code}{year_code}"


def get_expiration_date(year: int, month: int, week: Optional[int] = None) -> date:
    """
    Calculate expiration date for gold options.

    Monthly options: 4th business day before end of month prior to contract month
    Weekly options: Friday of specified week in the contract month
    """
    if week:
        # Weekly options expire on Friday of that week
        # Find first day of month, then find the Friday of the specified week
        first_day = date(year, month, 1)
        # Find first Friday
        days_until_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_until_friday)
        # Add weeks
        expiry = first_friday + timedelta(weeks=week - 1)
        return expiry
    else:
        # Monthly options: ~4 business days before end of prior month
        # Simplified: last Friday of the month before contract month
        prior_month = month - 1 if month > 1 else 12
        prior_year = year if month > 1 else year - 1

        # Get last day of prior month
        last_day = calendar.monthrange(prior_year, prior_month)[1]
        last_date = date(prior_year, prior_month, last_day)

        # Find last Friday
        days_since_friday = (last_date.weekday() - 4) % 7
        last_friday = last_date - timedelta(days=days_since_friday)
        return last_friday


def build_parent_symbol(week: Optional[int] = None) -> str:
    """
    Build parent symbol for gold options.

    Args:
        week: Week number (1-5) for weekly options, None for monthly

    Returns:
        OG.OPT for monthly, OG1.OPT/OG2.OPT/etc for weekly
    """
    if week:
        return f"{GOLD_OPTIONS_ROOT}{week}.OPT"
    return f"{GOLD_OPTIONS_ROOT}.OPT"




def run_live_feed(
    expiry_year: int,
    expiry_month: int,
    api_key: Optional[str] = None,
    strikes: Optional[List[int]] = None,
    option_type: Optional[str] = None,  # "C" for calls, "P" for puts, None for both
    week: Optional[int] = None,  # 1-5 for weekly options, None for monthly
    debug: bool = False,
):
    """
    Stream live bid/ask data for gold options.

    Args:
        expiry_year: Option expiry year (e.g., 2026)
        expiry_month: Option expiry month (1-12)
        api_key: Databento API key (or set DATABENTO_API_KEY env var)
        strikes: Optional list of strikes to filter (e.g., [2700, 2750, 2800])
        option_type: "C" for calls only, "P" for puts only, None for both
        week: Week number (1-5) for weekly options, None for monthly
    """
    api_key = api_key or os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        raise ValueError("API key required. Set DATABENTO_API_KEY or pass api_key param.")

    expiry_code = get_expiry_code(expiry_year, expiry_month)
    parent_symbol = build_parent_symbol(week)

    # Build expected symbol prefix
    # Monthly: OGH6, Weekly: OG1H6, OG2H6, etc.
    expiration_date = get_expiration_date(expiry_year, expiry_month, week)
    if week:
        symbol_prefix = f"{GOLD_OPTIONS_ROOT}{week}{expiry_code}"
        expiry_desc = f"Week {week} of {MONTH_CODES[expiry_month]} {expiry_year}"
    else:
        symbol_prefix = f"{GOLD_OPTIONS_ROOT}{expiry_code}"
        expiry_desc = f"{MONTH_CODES[expiry_month]} {expiry_year} (monthly)"

    print(f"Dataset: {DATASET}")
    print(f"Parent symbol: {parent_symbol}")
    print(f"Filtering for: {expiry_desc} ({symbol_prefix})")
    print("-" * 60)

    client = Live(key=api_key)

    # Subscribe to BBO-1s - samples best bid/offer every second
    # This gives immediate values without waiting for market activity
    client.subscribe(
        dataset=DATASET,
        schema="bbo-1s",
        symbols=[parent_symbol],
        stype_in="parent",
    )

    print("Streaming bid/ask data (Ctrl+C to stop)...\n")

    # Store latest quotes: {symbol: {strike, type, bid, ask, bid_sz, ask_sz, ts}}
    quotes = {}
    last_display = None

    for record in client:
        # Skip metadata messages (SystemMsg, SymbolMappingMsg, etc.)
        record_type = type(record).__name__
        if record_type in ('SystemMsg', 'SymbolMappingMsg', 'ErrorMsg'):
            continue

        # Look up symbol from instrument_id using client's symbology map
        instrument_id = record.instrument_id if hasattr(record, 'instrument_id') else None
        symbol_str = client.symbology_map.get(instrument_id, "") if instrument_id else ""

        # Debug mode: print record type and fields
        if debug:
            print(f"DEBUG: type={record_type}, symbol={symbol_str}")
            print(f"  {record}")
            continue

        # Filter by symbol prefix
        # Monthly format: "OGH6 C5700", Weekly format: "OG1H6 C5700"
        if not symbol_str.startswith(symbol_prefix):
            continue

        # For monthly options, exclude weekly symbols (e.g., OGH6 shouldn't match OG1H6)
        # Weekly symbols have a digit after OG, monthly don't
        if not week:
            # Check that character after "OG" is the month code, not a digit
            after_root = symbol_str[len(GOLD_OPTIONS_ROOT):]
            if after_root and after_root[0].isdigit():
                continue

        # Parse option type and strike from symbol (e.g., "OGH6 C5700" -> C, 5700)
        parts = symbol_str.split()
        if len(parts) != 2:
            continue
        opt_info = parts[1]  # e.g., "C5700"
        sym_option_type = opt_info[0]  # C or P
        sym_strike = int(opt_info[1:])  # e.g., 5700

        if strikes:
            if sym_strike not in strikes:
                continue

        if option_type:
            if sym_option_type != option_type:
                continue

        # Extract bid/ask from BBO record (uses levels array like mbp-1)
        if not hasattr(record, 'levels') or not record.levels:
            continue

        level = record.levels[0]
        # Prices are fixed-point, divide by 1e9 to get decimal
        bid_price = level.bid_px / 1e9 if level.bid_px and level.bid_px > 0 else None
        ask_price = level.ask_px / 1e9 if level.ask_px and level.ask_px > 0 else None

        if bid_price is None and ask_price is None:
            continue

        # Use ts_recv if ts_event is undefined (max uint64)
        ts_ns = record.ts_recv if record.ts_event > 1e18 else record.ts_event
        ts = datetime.fromtimestamp(ts_ns / 1e9)

        # Store quote
        quotes[symbol_str] = {
            'strike': sym_strike,
            'type': sym_option_type,
            'bid': bid_price,
            'ask': ask_price,
            'bid_sz': level.bid_sz,
            'ask_sz': level.ask_sz,
            'ts': ts,
        }

        # Refresh display every second
        now = datetime.now()
        if last_display is None or (now - last_display).total_seconds() >= 1:
            last_display = now
            # Clear screen and print sorted quotes
            print("\033[2J\033[H", end="")  # ANSI clear screen
            print(f"Gold Options - {expiry_desc}")
            print(f"Expiration: {expiration_date.strftime('%b %d, %Y')} | Updated: {now.strftime('%H:%M:%S')} | {len(quotes)} instruments")
            print("-" * 70)
            print(f"{'Symbol':20} | {'Strike':>6} | {'Bid':>15} | {'Ask':>15}")
            print("-" * 70)

            # Sort by strike price, then by type (C before P)
            for sym, q in sorted(quotes.items(), key=lambda x: (x[1]['strike'], x[1]['type'])):
                bid_str = f"${q['bid']:>7.2f} x {q['bid_sz']:<4}" if q['bid'] else "      N/A"
                ask_str = f"${q['ask']:>7.2f} x {q['ask_sz']:<4}" if q['ask'] else "      N/A"
                print(f"{sym:20} | {q['strike']:>6} | {bid_str} | {ask_str}")


def main():
    parser = argparse.ArgumentParser(
        description="Stream live CME Gold Options bid/ask data via Databento"
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
        "--strikes", "-s",
        type=int,
        nargs="+",
        help="Filter to specific strikes (e.g., --strikes 2700 2750 2800)"
    )
    parser.add_argument(
        "--calls", "-c",
        action="store_true",
        help="Show calls only"
    )
    parser.add_argument(
        "--puts", "-p",
        action="store_true",
        help="Show puts only"
    )
    parser.add_argument(
        "--week", "-w",
        type=int,
        choices=[1, 2, 3, 4, 5],
        help="Week number (1-5) for weekly options. Omit for monthly options."
    )
    parser.add_argument(
        "--api-key", "-k",
        type=str,
        help="Databento API key (or set DATABENTO_API_KEY env var)"
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Print all incoming symbols without filtering (for debugging)"
    )

    args = parser.parse_args()

    option_type = None
    if args.calls and not args.puts:
        option_type = "C"
    elif args.puts and not args.calls:
        option_type = "P"

    try:
        run_live_feed(
            expiry_year=args.year,
            expiry_month=args.month,
            api_key=args.api_key,
            strikes=args.strikes,
            option_type=option_type,
            week=args.week,
            debug=args.debug,
        )
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
