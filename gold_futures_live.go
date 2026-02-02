// Live bid/ask data for CME Gold Futures via Databento.
//
// Requires:
//
//	go mod init gold-futures
//	go get github.com/NimbleMarkets/dbn-go
//	go get github.com/joho/godotenv
//
// API key is loaded automatically from .env file or DATABENTO_API_KEY env var.
//
// Usage:
//
//	go run gold_futures_live.go [options]
//
// Options:
//
//	-year YEAR       Expiry year (default: 2026)
//	-month MONTH     Expiry month 1-12 (default: 3 for March)
//	-api-key KEY     Databento API key (or set DATABENTO_API_KEY env var)
//	-debug           Print raw incoming data for debugging
//
// Examples:
//
//	# March 2026 futures (default)
//	go run gold_futures_live.go
//
//	# June 2026 futures
//	go run gold_futures_live.go -year 2026 -month 6
//
//	# December 2025 futures
//	go run gold_futures_live.go -year 2025 -month 12
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	// Gold futures root symbol on COMEX
	GoldFuturesRoot = "GC"
)

// Month codes for CME futures
// F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
// N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
var MonthCodes = map[int]string{
	1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
	7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

// Config holds command-line configuration
type Config struct {
	Year   int
	Month  int
	APIKey string
	Debug  bool
}

func getExpiryCode(year, month int) string {
	monthCode := MonthCodes[month]
	yearCode := fmt.Sprintf("%d", year%10) // Last digit
	return monthCode + yearCode
}

func buildFuturesSymbol(expiryCode string) string {
	return GoldFuturesRoot + expiryCode
}

// RecordVisitor handles incoming DBN records
type RecordVisitor struct {
	cfg        Config
	symbol     string
	expiryDesc string
}

func (v *RecordVisitor) OnMbp1(record *dbn.Mbp1Msg) error {
	return v.processBidAsk(record.Level, record.TsRecv)
}

func (v *RecordVisitor) OnBbo(record *dbn.BboMsg) error {
	return v.processBidAsk(record.Level, record.TsRecv)
}

func (v *RecordVisitor) processBidAsk(level dbn.BidAskPair, tsEvent uint64) error {
	if v.cfg.Debug {
		fmt.Printf("DEBUG: bid_px=%d, ask_px=%d, bid_sz=%d, ask_sz=%d\n",
			level.BidPx, level.AskPx, level.BidSz, level.AskSz)
		return nil
	}

	// Convert prices (fixed-point, divide by 1e9)
	var bidPrice, askPrice float64
	if level.BidPx > 0 {
		bidPrice = float64(level.BidPx) / 1e9
	}
	if level.AskPx > 0 {
		askPrice = float64(level.AskPx) / 1e9
	}

	ts := time.Unix(0, int64(tsEvent))

	fmt.Printf("%s | %-12s | Bid: %10.2f x %-4d | Ask: %10.2f x %-4d\n",
		ts.Format("15:04:05.000"),
		v.symbol,
		bidPrice, level.BidSz,
		askPrice, level.AskSz)

	return nil
}

// No-op implementations for other required Visitor methods
func (v *RecordVisitor) OnMbp0(record *dbn.Mbp0Msg) error                       { return nil }
func (v *RecordVisitor) OnMbp10(record *dbn.Mbp10Msg) error                     { return nil }
func (v *RecordVisitor) OnMbo(record *dbn.MboMsg) error                         { return nil }
func (v *RecordVisitor) OnOhlcv(record *dbn.OhlcvMsg) error                     { return nil }
func (v *RecordVisitor) OnCmbp1(record *dbn.Cmbp1Msg) error                     { return nil }
func (v *RecordVisitor) OnImbalance(record *dbn.ImbalanceMsg) error             { return nil }
func (v *RecordVisitor) OnStatMsg(record *dbn.StatMsg) error                    { return nil }
func (v *RecordVisitor) OnStatusMsg(record *dbn.StatusMsg) error                { return nil }
func (v *RecordVisitor) OnInstrumentDefMsg(record *dbn.InstrumentDefMsg) error  { return nil }
func (v *RecordVisitor) OnSymbolMappingMsg(record *dbn.SymbolMappingMsg) error  { return nil }
func (v *RecordVisitor) OnErrorMsg(record *dbn.ErrorMsg) error                  { return nil }
func (v *RecordVisitor) OnSystemMsg(record *dbn.SystemMsg) error                { return nil }
func (v *RecordVisitor) OnStreamEnd() error                                     { return nil }

func runLiveFeed(cfg Config) error {
	expiryCode := getExpiryCode(cfg.Year, cfg.Month)
	symbol := buildFuturesSymbol(expiryCode)
	expiryDesc := fmt.Sprintf("%s %d (%s)", MonthCodes[cfg.Month], cfg.Year, expiryCode)

	fmt.Println("Connecting to Databento live feed...")
	fmt.Printf("Dataset: %s\n", Dataset)
	fmt.Printf("Symbol: %s\n", symbol)
	fmt.Printf("Expiry: %s\n", expiryDesc)
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

	// Subscribe to BBO-1s data (best bid/offer sampled every second)
	fmt.Println("Subscribing to data...")
	sub := dbn_live.SubscriptionRequestMsg{
		Schema:  "bbo-1s",
		StypeIn: dbn.SType_RawSymbol,
		Symbols: []string{symbol},
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
		cfg:        cfg,
		symbol:     symbol,
		expiryDesc: expiryDesc,
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
	apiKey := flag.String("api-key", "", "Databento API key")
	debug := flag.Bool("debug", false, "Print raw incoming data for debugging")

	flag.Parse()

	// Get API key
	key := *apiKey
	if key == "" {
		key = os.Getenv("DATABENTO_API_KEY")
	}
	if key == "" {
		log.Fatal("API key required. Set DATABENTO_API_KEY or use -api-key flag.")
	}

	cfg := Config{
		Year:   *year,
		Month:  *month,
		APIKey: key,
		Debug:  *debug,
	}

	if err := runLiveFeed(cfg); err != nil {
		log.Fatal(err)
	}
}
