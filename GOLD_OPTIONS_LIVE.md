# Gold Options Live Feed

Live streaming of CME Gold Options (OG) bid/ask data with implied volatility and delta, powered by Databento.

## Prerequisites

- Go 1.21+
- Databento API key (set `DATABENTO_API_KEY` in a `.env` file or environment variable)

```bash
go mod init gold-options
go get github.com/NimbleMarkets/dbn-go
go get github.com/joho/godotenv
```

## Usage

```bash
go run gold_options_live.go [options]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-year` | `2026` | Expiry year |
| `-month` | `3` | Expiry month (1-12) |
| `-week` | `0` | Week number for weekly options (1-5). Use 0 for monthly options. |
| `-strikes` | (all) | Comma-separated list of strikes to filter (e.g., `-strikes 2700,2800`) |
| `-calls` | `false` | Show calls only |
| `-puts` | `false` | Show puts only |
| `-rate` | `0.045` | Risk-free rate used for IV and delta calculation |
| `-api-key` | | Databento API key (overrides env var) |
| `-debug` | `false` | Print raw incoming data for debugging |

### Examples

```bash
# March 2026 monthly options (default)
go run gold_options_live.go

# Week 1 weekly options for February 2026
go run gold_options_live.go -year 2026 -month 2 -week 1

# Only puts at specific strikes
go run gold_options_live.go -puts -strikes 4100,4200,4300,4400,4500

# Custom risk-free rate
go run gold_options_live.go -rate 0.05
```

## Data Sources

The script subscribes to two live Databento feeds on the CME Globex dataset (`GLBX.MDP3`), using the `bbo-1s` schema (best bid/offer sampled every second):

1. **Options feed** - Subscribes via parent symbol (e.g., `OG.OPT` for monthly, `OG1.OPT` for week 1 weeklies). The script filters the stream to the specific contract month.
2. **Futures feed** - Subscribes to all gold futures cycle months (GCG, GCJ, GCM, GCQ, GCV, GCZ) for the current and next year.

On the first option that passes all filters (correct series, strike, and option type), the script calls the Databento Historical API (`/v0/timeseries.get_range` with `schema=definition` and `stype_in=raw_symbol`) using that option's raw symbol to fetch the **underlying futures symbol** and **expiration date** for the series. These are displayed in the table header.

## Output Format

The script displays a live-updating table:

```
Gold Options - Week 1 of G 2026
Futures (GCJ6): $2845.50 | Expiration: Feb 07, 2026 | Updated: 14:30:05 | 12 instruments
-------------------------------------------------------------------------------------------------------------------
Symbol                    | Strike |  Underly |             Bid |             Ask |      IV |  Delta
-------------------------------------------------------------------------------------------------------------------
OG1G6 C2700               |   2700 |     GCJ6 | $ 148.20 x 5    | $ 162.50 x 10   |   22.5% |  0.820
OG1G6 P2700               |   2700 |     GCJ6 | $   3.10 x 20   | $   5.40 x 15   |   23.1% | -0.180
```

### Header Fields

| Field | Description |
|-------|-------------|
| Futures (GCJ6) | Mid-price of the underlying futures contract |
| Expiration | Option expiration date (from Databento API) |
| Updated | Local time of last display refresh |
| instruments | Number of option instruments currently tracked |

### Per-Option Fields

| Field | Description |
|-------|-------------|
| Symbol | CME raw symbol (e.g., `OG1G6 C2700` = Week 1, Feb 2026, Call, Strike 2700) |
| Strike | Strike price in USD |
| Underly | Underlying futures contract symbol |
| Bid | Best bid price and size |
| Ask | Best ask price and size |
| IV | Implied volatility (annualized, Black-76 model) |
| Delta | Option delta (Black-76 model) |

## Key Data Structures

### `Quote` struct

Each option instrument is stored as a `Quote` (defined at line 83):

```go
type Quote struct {
    Strike     int       // Strike price (e.g., 2700)
    Type       string    // "C" for call, "P" for put
    Underlying string    // Underlying futures symbol (e.g., "GCJ6")
    Bid        *float64  // Best bid price (nil if no bid)
    Ask        *float64  // Best ask price (nil if no ask)
    BidSz      uint32    // Bid size (number of contracts)
    AskSz      uint32    // Ask size (number of contracts)
    Ts         time.Time // Timestamp of last update
    IV         *float64  // Implied volatility (nil if not calculable)
    Delta      *float64  // Option delta (nil if not calculable)
}
```

All quotes are stored in `RecordVisitor.quotes`, a `map[string]*Quote` keyed by the CME raw symbol (e.g., `"OG1G6 C2700"`).

### Futures Price

The underlying futures mid-price is stored in `RecordVisitor.futuresPrices`, a `map[string]*float64` keyed by futures symbol (e.g., `"GCJ6"`). The mid-price is calculated as `(bid + ask) / 2`, falling back to whichever side is available.

### Underlying and Expiration

Determined once at startup via the Databento Historical API and stored in:

- `RecordVisitor.underlying` (`string`) - e.g., `"GCJ6"`
- `RecordVisitor.expirationDate` (`time.Time`) - the option expiration date

All options in a given series share the same underlying futures contract.

## IV and Delta Calculation

Implied volatility and delta are calculated using the **Black-76 model** (the standard model for options on futures).

### Inputs

| Input | Source |
|-------|--------|
| Futures price (F) | Live mid-price from the underlying futures feed |
| Strike (K) | Parsed from the option symbol |
| Time to expiry (T) | `expirationDate - now`, in years |
| Risk-free rate (r) | `-rate` flag (default 4.5%) |
| Option price | Mid-price of the option bid/ask |

### Method

IV is solved numerically using Newton-Raphson iteration with step clamping:

1. Start with initial guess of 50% volatility
2. Compute theoretical price using Black-76
3. Compare to market mid-price
4. Adjust volatility using vega (with step size clamped to +/-0.5 per iteration)
5. Repeat until convergence (tolerance: 1e-6) or 200 iterations

Once IV is found, delta is computed directly from Black-76.

### When IV/Delta Show N/A

- **No futures price yet** - The underlying futures feed hasn't delivered data
- **No bid or ask** - The option has no market quotes
- **Expiration passed** - Time to expiry is zero or negative
- **Non-convergence** - The solver could not find IV within bounds (0.1% to 1000%)
- **API failure** - The underlying could not be determined from the Databento API

## CME Gold Options Symbology

| Component | Format | Example |
|-----------|--------|---------|
| Monthly option | `OG{month_code}{year_digit}` | `OGG6` = Feb 2026 monthly |
| Weekly option | `OG{week}{month_code}{year_digit}` | `OG1G6` = Week 1, Feb 2026 |
| Full symbol | `{base} {C\|P}{strike}` | `OG1G6 C2700` = Week 1, Feb 2026, Call 2700 |

### Month Codes

| Code | Month | Code | Month |
|------|-------|------|-------|
| F | January | N | July |
| G | February | Q | August |
| H | March | U | September |
| J | April | V | October |
| K | May | X | November |
| M | June | Z | December |

### Gold Futures Cycle Months

Gold futures trade on even-month cycles: **G** (Feb), **J** (Apr), **M** (Jun), **Q** (Aug), **V** (Oct), **Z** (Dec). Options on gold are settled into the nearest cycle-month futures contract.

### Monthly Option Expiration

Monthly options are named after their underlying futures contract month, but they expire in the **prior calendar month**. For example, `OGG6` (February 2026 monthly options) expire in **January 2026**, not February. Specifically, monthly options expire on the 4th last business day of the month prior to the contract month (moved to Thursday if that day is a Friday). Weekly options expire on the Friday of their numbered week within the named month.

## Integration Notes

To integrate this into a pricing system, the key integration points are:

1. **`RecordVisitor.quotes`** (`map[string]*Quote`) - Access all live option data. Keyed by raw symbol. Each `Quote` contains bid, ask, IV, delta, and the underlying.

2. **`RecordVisitor.futuresPrices`** (`map[string]*float64`) - Access the current underlying futures price.

3. **`RecordVisitor.underlying`** (`string`) - The underlying futures symbol shared by all options in the series.

4. **`RecordVisitor.expirationDate`** (`time.Time`) - The option expiration timestamp.

The `processBidAsk` method (line 472) is the central handler where all incoming data is parsed, stored, and where IV/delta are calculated. The `recalculateGreeks` method (line 672) re-runs IV/delta for all stored quotes when the futures price updates.
