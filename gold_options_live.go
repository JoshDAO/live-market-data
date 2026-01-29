// Live bid/ask data for CME Gold Options via Databento.
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

func clearScreen() {
	fmt.Print("\033[2J\033[H")
}

func printTable(quotes map[string]*Quote, expiryDesc string, expirationDate time.Time) {
	clearScreen()
	now := time.Now()

	fmt.Printf("Gold Options - %s\n", expiryDesc)
	fmt.Printf("Expiration: %s | Updated: %s | %d instruments\n",
		expirationDate.Format("Jan 02, 2006"),
		now.Format("15:04:05"),
		len(quotes))
	fmt.Println(strings.Repeat("-", 70))
	fmt.Printf("%-20s | %6s | %15s | %15s\n", "Symbol", "Strike", "Bid", "Ask")
	fmt.Println(strings.Repeat("-", 70))

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
		var bidStr, askStr string
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
		fmt.Printf("%-20s | %6d | %s | %s\n", item.Symbol, q.Strike, bidStr, askStr)
	}
}

// RecordVisitor handles incoming DBN records
type RecordVisitor struct {
	cfg            Config
	symbolPrefix   string
	expiryDesc     string
	expirationDate time.Time
	quotes         map[string]*Quote
	symbolMap      map[uint32]string
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

	// Filter by symbol prefix
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

	// Store quote
	v.quotes[symbol] = &Quote{
		Strike: symStrike,
		Type:   symOptionType,
		Bid:    bid,
		Ask:    ask,
		BidSz:  level.BidSz,
		AskSz:  level.AskSz,
		Ts:     time.Unix(0, int64(tsRecv)),
	}

	// Refresh display every second
	now := time.Now()
	if v.lastDisplay.IsZero() || now.Sub(v.lastDisplay) >= time.Second {
		v.lastDisplay = now
		printTable(v.quotes, v.expiryDesc, v.expirationDate)
	}

	return nil
}

func runLiveFeed(cfg Config) error {
	expiryCode := getExpiryCode(cfg.Year, cfg.Month)
	parentSymbol := buildParentSymbol(cfg.Week)
	expirationDate := getExpirationDate(cfg.Year, cfg.Month, cfg.Week)

	var symbolPrefix, expiryDesc string
	if cfg.Week > 0 {
		symbolPrefix = fmt.Sprintf("%s%d%s", GoldOptionsRoot, cfg.Week, expiryCode)
		expiryDesc = fmt.Sprintf("Week %d of %s %d", cfg.Week, MonthCodes[cfg.Month], cfg.Year)
	} else {
		symbolPrefix = GoldOptionsRoot + expiryCode
		expiryDesc = fmt.Sprintf("%s %d (monthly)", MonthCodes[cfg.Month], cfg.Year)
	}

	fmt.Printf("Dataset: %s\n", Dataset)
	fmt.Printf("Parent symbol: %s\n", parentSymbol)
	fmt.Printf("Filtering for: %s (%s)\n", expiryDesc, symbolPrefix)
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

	// Subscribe to BBO-1s data
	fmt.Println("Subscribing to data...")
	sub := dbn_live.SubscriptionRequestMsg{
		Schema:  "bbo-1s",
		StypeIn: dbn.SType_Parent,
		Symbols: []string{parentSymbol},
	}
	if err := client.Subscribe(sub); err != nil {
		return fmt.Errorf("subscription failed: %w", err)
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
	}

	if err := runLiveFeed(cfg); err != nil {
		log.Fatal(err)
	}
}
