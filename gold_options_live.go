// Live bid/ask data for CME Gold Options via Databento with IV and Delta.
//
// Requires:
//
//	go mod init gold-options
//	go get github.com/NimbleMarkets/dbn-go
//	go get github.com/joho/godotenv
//
// API key is loaded automatically from .env file or DATABENTO_API_KEY env var.
//
// Usage:
//
//	go run gold_options_live.go [options]
//
// Options:
//
//	-year YEAR       Expiry year (default: 2026)
//	-month MONTH     Expiry month 1-12 (default: 3 for March)
//	-week WEEK       Week number 1-5 for weekly options (0 for monthly)
//	-strikes N,N     Filter to specific strikes comma-separated (e.g., -strikes 2700,2750)
//	-calls           Show calls only
//	-puts            Show puts only
//	-rate RATE       Risk-free rate for IV/delta calculation (default: 0.045)
//	-api-key KEY     Databento API key (or set DATABENTO_API_KEY env var)
//	-debug           Print raw incoming data for debugging
//
// Examples:
//
//	# March 2026 monthly options (default)
//	go run gold_options_live.go
//
//	# June 2026 monthly options
//	go run gold_options_live.go -year 2026 -month 6
//
//	# Week 1 weekly options for March 2026
//	go run gold_options_live.go -week 1
//
//	# Only calls at strikes 2700 and 2800
//	go run gold_options_live.go -calls -strikes 2700,2800
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/NimbleMarkets/dbn-go"
	dbn_live "github.com/NimbleMarkets/dbn-go/live"
	"github.com/joho/godotenv"
)

const (
	// CME Globex dataset
	Dataset = "GLBX.MDP3"
	// Gold options root symbol on COMEX
	GoldOptionsRoot = "OG"
	// Gold futures root symbol on COMEX
	GoldFuturesRoot = "GC"
)

// Month codes for CME futures/options
var MonthCodes = map[int]string{
	1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
	7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

// Quote represents a single option quote
type Quote struct {
	Strike int
	Type   string // "C" or "P"
	Bid    *float64
	Ask    *float64
	BidSz  uint32
	AskSz  uint32
	Ts     time.Time
	IV     *float64 // Implied volatility
	Delta  *float64 // Option delta
}

// Config holds command-line configuration
type Config struct {
	Year       int
	Month      int
	Week       int
	Strikes    []int
	OptionType string // "C", "P", or "" for both
	APIKey     string
	Debug      bool
	Rate       float64 // Risk-free rate
}

func getExpiryCode(year, month int) string {
	monthCode := MonthCodes[month]
	yearCode := strconv.Itoa(year % 10) // Last digit
	return monthCode + yearCode
}

func getExpirationDate(year, month, week int) time.Time {
	if week > 0 {
		// Weekly options expire on Friday of that week
		firstDay := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		// Find first Friday (weekday 5)
		daysUntilFriday := (5 - int(firstDay.Weekday()) + 7) % 7
		if daysUntilFriday == 0 && firstDay.Weekday() != time.Friday {
			daysUntilFriday = 7
		}
		firstFriday := firstDay.AddDate(0, 0, daysUntilFriday)
		return firstFriday.AddDate(0, 0, (week-1)*7)
	}

	// Monthly options: last Friday of the month before contract month
	priorMonth := month - 1
	priorYear := year
	if priorMonth < 1 {
		priorMonth = 12
		priorYear--
	}

	// Get last day of prior month
	lastDay := time.Date(priorYear, time.Month(priorMonth)+1, 0, 0, 0, 0, 0, time.UTC)
	// Find last Friday
	daysSinceFriday := (int(lastDay.Weekday()) - 5 + 7) % 7
	return lastDay.AddDate(0, 0, -daysSinceFriday)
}

func buildParentSymbol(week int) string {
	if week > 0 {
		return fmt.Sprintf("%s%d.OPT", GoldOptionsRoot, week)
	}
	return GoldOptionsRoot + ".OPT"
}

func parseStrikes(s string) []int {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	strikes := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if v, err := strconv.Atoi(p); err == nil {
			strikes = append(strikes, v)
		}
	}
	return strikes
}

func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// Black-76 model for options on futures

// normalCDF computes the cumulative distribution function of the standard normal distribution
func normalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}

// normalPDF computes the probability density function of the standard normal distribution
func normalPDF(x float64) float64 {
	return math.Exp(-0.5*x*x) / math.Sqrt(2*math.Pi)
}

// black76Price calculates option price using Black-76 model
// F = futures price, K = strike, T = time to expiry (years), r = risk-free rate, sigma = volatility
// optionType = "C" for call, "P" for put
func black76Price(F, K, T, r, sigma float64, optionType string) float64 {
	if T <= 0 || sigma <= 0 {
		return 0
	}

	sqrtT := math.Sqrt(T)
	d1 := (math.Log(F/K) + 0.5*sigma*sigma*T) / (sigma * sqrtT)
	d2 := d1 - sigma*sqrtT

	discount := math.Exp(-r * T)

	if optionType == "C" {
		return discount * (F*normalCDF(d1) - K*normalCDF(d2))
	}
	// Put
	return discount * (K*normalCDF(-d2) - F*normalCDF(-d1))
}

// black76Vega calculates vega (sensitivity to volatility) using Black-76 model
func black76Vega(F, K, T, r, sigma float64) float64 {
	if T <= 0 || sigma <= 0 {
		return 0
	}

	sqrtT := math.Sqrt(T)
	d1 := (math.Log(F/K) + 0.5*sigma*sigma*T) / (sigma * sqrtT)
	discount := math.Exp(-r * T)

	return F * discount * sqrtT * normalPDF(d1)
}

// black76Delta calculates delta using Black-76 model
func black76Delta(F, K, T, r, sigma float64, optionType string) float64 {
	if T <= 0 || sigma <= 0 {
		return 0
	}

	sqrtT := math.Sqrt(T)
	d1 := (math.Log(F/K) + 0.5*sigma*sigma*T) / (sigma * sqrtT)
	discount := math.Exp(-r * T)

	if optionType == "C" {
		return discount * normalCDF(d1)
	}
	// Put
	return -discount * normalCDF(-d1)
}

// impliedVolatility calculates IV using Newton-Raphson method
func impliedVolatility(price, F, K, T, r float64, optionType string) *float64 {
	if T <= 0 || price <= 0 {
		return nil
	}

	// Initial guess
	sigma := 0.3
	const maxIter = 100
	const tolerance = 1e-6

	for i := 0; i < maxIter; i++ {
		calcPrice := black76Price(F, K, T, r, sigma, optionType)
		vega := black76Vega(F, K, T, r, sigma)

		if vega < 1e-10 {
			// Vega too small, can't converge
			return nil
		}

		diff := calcPrice - price
		if math.Abs(diff) < tolerance {
			return &sigma
		}

		sigma = sigma - diff/vega

		// Bounds check
		if sigma <= 0.001 {
			sigma = 0.001
		}
		if sigma > 5.0 {
			return nil // IV unreasonably high
		}
	}

	return nil // Did not converge
}

func clearScreen() {
	fmt.Print("\033[2J\033[H")
}

func printTable(quotes map[string]*Quote, expiryDesc string, expirationDate time.Time, futuresPrice *float64) {
	clearScreen()
	now := time.Now()

	fmt.Printf("Gold Options - %s\n", expiryDesc)
	if futuresPrice != nil {
		fmt.Printf("Futures: $%.2f | ", *futuresPrice)
	} else {
		fmt.Print("Futures: N/A | ")
	}
	fmt.Printf("Expiration: %s | Updated: %s | %d instruments\n",
		expirationDate.Format("Jan 02, 2006"),
		now.Format("15:04:05"),
		len(quotes))
	fmt.Println(strings.Repeat("-", 105))
	fmt.Printf("%-25s | %6s | %15s | %15s | %7s | %6s\n", "Symbol", "Strike", "Bid", "Ask", "IV", "Delta")
	fmt.Println(strings.Repeat("-", 105))

	// Sort by strike, then by type
	type kv struct {
		Symbol string
		Quote  *Quote
	}
	sorted := make([]kv, 0, len(quotes))
	for k, v := range quotes {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Quote.Strike != sorted[j].Quote.Strike {
			return sorted[i].Quote.Strike < sorted[j].Quote.Strike
		}
		return sorted[i].Quote.Type < sorted[j].Quote.Type
	})

	for _, item := range sorted {
		q := item.Quote
		var bidStr, askStr, ivStr, deltaStr string
		if q.Bid != nil {
			bidStr = fmt.Sprintf("$%7.2f x %-4d", *q.Bid, q.BidSz)
		} else {
			bidStr = "      N/A"
		}
		if q.Ask != nil {
			askStr = fmt.Sprintf("$%7.2f x %-4d", *q.Ask, q.AskSz)
		} else {
			askStr = "      N/A"
		}
		if q.IV != nil {
			ivStr = fmt.Sprintf("%6.1f%%", *q.IV*100)
		} else {
			ivStr = "   N/A"
		}
		if q.Delta != nil {
			deltaStr = fmt.Sprintf("%6.3f", *q.Delta)
		} else {
			deltaStr = "   N/A"
		}
		fmt.Printf("%-25s | %6d | %s | %s | %s | %s\n", item.Symbol, q.Strike, bidStr, askStr, ivStr, deltaStr)
	}
}

// RecordVisitor handles incoming DBN records
type RecordVisitor struct {
	cfg            Config
	symbolPrefix   string
	futuresSymbol  string
	expiryDesc     string
	expirationDate time.Time
	quotes         map[string]*Quote
	symbolMap      map[uint32]string
	futuresPrice   *float64
	lastDisplay    time.Time
}

// Implement the dbn.Visitor interface

func (v *RecordVisitor) OnBbo(record *dbn.BboMsg) error {
	return v.processBidAsk(record.Header.InstrumentID, record.Level, record.TsRecv)
}

func (v *RecordVisitor) OnMbp1(record *dbn.Mbp1Msg) error {
	return v.processBidAsk(record.Header.InstrumentID, record.Level, record.TsRecv)
}

func (v *RecordVisitor) OnSymbolMappingMsg(record *dbn.SymbolMappingMsg) error {
	v.symbolMap[record.Header.InstrumentID] = record.StypeOutSymbol
	return nil
}

// No-op implementations for other required methods
func (v *RecordVisitor) OnMbp0(record *dbn.Mbp0Msg) error             { return nil }
func (v *RecordVisitor) OnMbp10(record *dbn.Mbp10Msg) error           { return nil }
func (v *RecordVisitor) OnMbo(record *dbn.MboMsg) error               { return nil }
func (v *RecordVisitor) OnOhlcv(record *dbn.OhlcvMsg) error           { return nil }
func (v *RecordVisitor) OnCmbp1(record *dbn.Cmbp1Msg) error           { return nil }
func (v *RecordVisitor) OnImbalance(record *dbn.ImbalanceMsg) error   { return nil }
func (v *RecordVisitor) OnStatMsg(record *dbn.StatMsg) error          { return nil }
func (v *RecordVisitor) OnStatusMsg(record *dbn.StatusMsg) error      { return nil }
func (v *RecordVisitor) OnInstrumentDefMsg(record *dbn.InstrumentDefMsg) error { return nil }
func (v *RecordVisitor) OnErrorMsg(record *dbn.ErrorMsg) error        { return nil }
func (v *RecordVisitor) OnSystemMsg(record *dbn.SystemMsg) error      { return nil }
func (v *RecordVisitor) OnStreamEnd() error                           { return nil }

func (v *RecordVisitor) processBidAsk(instrumentID uint32, level dbn.BidAskPair, tsRecv uint64) error {
	symbol, ok := v.symbolMap[instrumentID]
	if !ok || symbol == "" {
		return nil
	}

	if v.cfg.Debug {
		fmt.Printf("DEBUG: symbol=%s, bid_px=%d, ask_px=%d\n", symbol, level.BidPx, level.AskPx)
		return nil
	}

	// Check if this is the futures symbol
	if symbol == v.futuresSymbol {
		// Update futures price (use mid price)
		var bid, ask float64
		if level.BidPx > 0 {
			bid = float64(level.BidPx) / 1e9
		}
		if level.AskPx > 0 {
			ask = float64(level.AskPx) / 1e9
		}
		if bid > 0 && ask > 0 {
			mid := (bid + ask) / 2
			v.futuresPrice = &mid
		} else if bid > 0 {
			v.futuresPrice = &bid
		} else if ask > 0 {
			v.futuresPrice = &ask
		}
		// Recalculate IV/delta for all quotes when futures price updates
		v.recalculateGreeks()
		// Refresh display
		now := time.Now()
		if v.lastDisplay.IsZero() || now.Sub(v.lastDisplay) >= time.Second {
			v.lastDisplay = now
			printTable(v.quotes, v.expiryDesc, v.expirationDate, v.futuresPrice)
		}
		return nil
	}

	// Filter by symbol prefix (options)
	if !strings.HasPrefix(symbol, v.symbolPrefix) {
		return nil
	}

	// For monthly, exclude weekly symbols
	if v.cfg.Week == 0 {
		afterRoot := symbol[len(GoldOptionsRoot):]
		if len(afterRoot) > 0 && afterRoot[0] >= '0' && afterRoot[0] <= '9' {
			return nil
		}
	}

	// Parse option type and strike
	parts := strings.Split(symbol, " ")
	if len(parts) != 2 {
		return nil
	}
	optInfo := parts[1]
	if len(optInfo) < 2 {
		return nil
	}
	symOptionType := string(optInfo[0])
	symStrike, err := strconv.Atoi(optInfo[1:])
	if err != nil {
		return nil
	}

	// Apply filters
	if len(v.cfg.Strikes) > 0 && !containsInt(v.cfg.Strikes, symStrike) {
		return nil
	}
	if v.cfg.OptionType != "" && symOptionType != v.cfg.OptionType {
		return nil
	}

	// Convert prices (fixed-point, divide by 1e9)
	var bid, ask *float64
	if level.BidPx > 0 {
		b := float64(level.BidPx) / 1e9
		bid = &b
	}
	if level.AskPx > 0 {
		a := float64(level.AskPx) / 1e9
		ask = &a
	}

	if bid == nil && ask == nil {
		return nil
	}

	// Calculate time to expiration in years
	T := v.expirationDate.Sub(time.Now()).Hours() / (24 * 365.25)

	// Calculate IV and delta if we have futures price
	var iv, delta *float64
	if v.futuresPrice != nil && T > 0 {
		F := *v.futuresPrice
		K := float64(symStrike)
		r := v.cfg.Rate

		// Use mid price for IV calculation
		var optionPrice float64
		if bid != nil && ask != nil {
			optionPrice = (*bid + *ask) / 2
		} else if bid != nil {
			optionPrice = *bid
		} else if ask != nil {
			optionPrice = *ask
		}

		if optionPrice > 0 {
			iv = impliedVolatility(optionPrice, F, K, T, r, symOptionType)
			if iv != nil {
				delta = new(float64)
				*delta = black76Delta(F, K, T, r, *iv, symOptionType)
			}
		}
	}

	// Store quote
	v.quotes[symbol] = &Quote{
		Strike: symStrike,
		Type:   symOptionType,
		Bid:    bid,
		Ask:    ask,
		BidSz:  level.BidSz,
		AskSz:  level.AskSz,
		Ts:     time.Unix(0, int64(tsRecv)),
		IV:     iv,
		Delta:  delta,
	}

	// Refresh display every second
	now := time.Now()
	if v.lastDisplay.IsZero() || now.Sub(v.lastDisplay) >= time.Second {
		v.lastDisplay = now
		printTable(v.quotes, v.expiryDesc, v.expirationDate, v.futuresPrice)
	}

	return nil
}

// recalculateGreeks updates IV and delta for all quotes when futures price changes
func (v *RecordVisitor) recalculateGreeks() {
	if v.futuresPrice == nil {
		return
	}

	T := v.expirationDate.Sub(time.Now()).Hours() / (24 * 365.25)
	if T <= 0 {
		return
	}

	F := *v.futuresPrice
	r := v.cfg.Rate

	for _, q := range v.quotes {
		K := float64(q.Strike)

		var optionPrice float64
		if q.Bid != nil && q.Ask != nil {
			optionPrice = (*q.Bid + *q.Ask) / 2
		} else if q.Bid != nil {
			optionPrice = *q.Bid
		} else if q.Ask != nil {
			optionPrice = *q.Ask
		}

		if optionPrice > 0 {
			q.IV = impliedVolatility(optionPrice, F, K, T, r, q.Type)
			if q.IV != nil {
				q.Delta = new(float64)
				*q.Delta = black76Delta(F, K, T, r, *q.IV, q.Type)
			}
		}
	}
}

func runLiveFeed(cfg Config) error {
	expiryCode := getExpiryCode(cfg.Year, cfg.Month)
	parentSymbol := buildParentSymbol(cfg.Week)
	expirationDate := getExpirationDate(cfg.Year, cfg.Month, cfg.Week)

	// Build futures symbol (e.g., GCH6 for March 2026)
	futuresSymbol := GoldFuturesRoot + expiryCode

	var symbolPrefix, expiryDesc string
	if cfg.Week > 0 {
		symbolPrefix = fmt.Sprintf("%s%d%s", GoldOptionsRoot, cfg.Week, expiryCode)
		expiryDesc = fmt.Sprintf("Week %d of %s %d", cfg.Week, MonthCodes[cfg.Month], cfg.Year)
	} else {
		symbolPrefix = GoldOptionsRoot + expiryCode
		expiryDesc = fmt.Sprintf("%s %d (monthly)", MonthCodes[cfg.Month], cfg.Year)
	}

	fmt.Printf("Dataset: %s\n", Dataset)
	fmt.Printf("Options parent symbol: %s\n", parentSymbol)
	fmt.Printf("Futures symbol: %s\n", futuresSymbol)
	fmt.Printf("Filtering for: %s (%s)\n", expiryDesc, symbolPrefix)
	fmt.Printf("Risk-free rate: %.2f%%\n", cfg.Rate*100)
	fmt.Println(strings.Repeat("-", 60))

	// Create live client config
	liveConfig := dbn_live.LiveConfig{
		ApiKey:  cfg.APIKey,
		Dataset: Dataset,
	}

	// Create client
	client, err := dbn_live.NewLiveClient(liveConfig)
	if err != nil {
		return fmt.Errorf("failed to create live client: %w", err)
	}

	// Authenticate
	fmt.Println("Authenticating...")
	_, err = client.Authenticate(cfg.APIKey)
	if err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Subscribe to BBO-1s data for options
	fmt.Println("Subscribing to options data...")
	optionsSub := dbn_live.SubscriptionRequestMsg{
		Schema:  "bbo-1s",
		StypeIn: dbn.SType_Parent,
		Symbols: []string{parentSymbol},
	}
	if err := client.Subscribe(optionsSub); err != nil {
		return fmt.Errorf("options subscription failed: %w", err)
	}

	// Subscribe to BBO-1s data for underlying futures
	fmt.Println("Subscribing to futures data...")
	futuresSub := dbn_live.SubscriptionRequestMsg{
		Schema:  "bbo-1s",
		StypeIn: dbn.SType_RawSymbol,
		Symbols: []string{futuresSymbol},
	}
	if err := client.Subscribe(futuresSub); err != nil {
		return fmt.Errorf("futures subscription failed: %w", err)
	}

	// Start streaming
	fmt.Println("Starting stream...")
	if err := client.Start(); err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	fmt.Println("Streaming bid/ask data (Ctrl+C to stop)...\n")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create visitor for processing records
	visitor := &RecordVisitor{
		cfg:            cfg,
		symbolPrefix:   symbolPrefix,
		futuresSymbol:  futuresSymbol,
		expiryDesc:     expiryDesc,
		expirationDate: expirationDate,
		quotes:         make(map[string]*Quote),
		symbolMap:      make(map[uint32]string),
	}

	// Process records in goroutine
	done := make(chan error, 1)
	go func() {
		scanner := client.GetDbnScanner()
		for scanner.Next() {
			if err := scanner.Visit(visitor); err != nil {
				done <- err
				return
			}
		}
		if err := scanner.Error(); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	// Wait for signal or completion
	select {
	case <-sigChan:
		fmt.Println("\nStopped.")
	case err := <-done:
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// Load .env file
	execPath, _ := os.Executable()
	envPath := filepath.Join(filepath.Dir(execPath), ".env")
	godotenv.Load(envPath)
	godotenv.Load(".env") // Also try current directory

	// Parse command-line flags
	year := flag.Int("year", 2026, "Expiry year")
	month := flag.Int("month", 3, "Expiry month 1-12")
	week := flag.Int("week", 0, "Week number 1-5 for weekly options (0 for monthly)")
	strikesStr := flag.String("strikes", "", "Filter to specific strikes, comma-separated")
	calls := flag.Bool("calls", false, "Show calls only")
	puts := flag.Bool("puts", false, "Show puts only")
	rate := flag.Float64("rate", 0.045, "Risk-free rate for IV/delta calculation")
	apiKey := flag.String("api-key", "", "Databento API key")
	debug := flag.Bool("debug", false, "Print raw incoming data for debugging")

	flag.Parse()

	// Determine option type filter
	var optionType string
	if *calls && !*puts {
		optionType = "C"
	} else if *puts && !*calls {
		optionType = "P"
	}

	// Get API key
	key := *apiKey
	if key == "" {
		key = os.Getenv("DATABENTO_API_KEY")
	}
	if key == "" {
		log.Fatal("API key required. Set DATABENTO_API_KEY or use -api-key flag.")
	}

	cfg := Config{
		Year:       *year,
		Month:      *month,
		Week:       *week,
		Strikes:    parseStrikes(*strikesStr),
		OptionType: optionType,
		APIKey:     key,
		Debug:      *debug,
		Rate:       *rate,
	}

	if err := runLiveFeed(cfg); err != nil {
		log.Fatal(err)
	}
}
