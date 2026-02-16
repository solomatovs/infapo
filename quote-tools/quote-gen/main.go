package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Symbol holds the current state of a trading instrument.
type Symbol struct {
	Name     string
	Price    float64 // current bid
	Spread   float64 // ask = bid + spread
	Decimals int     // formatting precision
	Step     float64 // max price change per tick
}

func (s *Symbol) Tick() {
	delta := (rand.Float64()*2 - 1) * s.Step
	s.Price += delta
	if s.Price < s.Step {
		s.Price = s.Step
	}
}

func (s *Symbol) Format() string {
	ask := s.Price + s.Spread
	return fmt.Sprintf(`{"symbol":"%s","bid":%.*f,"ask":%.*f}`,
		s.Name, s.Decimals, s.Price, s.Decimals, ask)
}

func (s *Symbol) FormatHistory() string {
	ask := s.Price + s.Spread
	ts := time.Now().UnixMilli()
	return fmt.Sprintf(`{"symbol":"%s","bid":%.*f,"ask":%.*f,"ts_ms":%d}`,
		s.Name, s.Decimals, s.Price, s.Decimals, ask, ts)
}

func newSymbols() []*Symbol {
	return []*Symbol{
		{"XAUUSD", 2650.00, 0.70, 2, 2.00},
		{"XAGUSD", 31.50, 0.03, 2, 0.05},
		{"EURUSD", 1.08500, 0.00020, 5, 0.00050},
		{"GBPUSD", 1.26500, 0.00020, 5, 0.00050},
		{"USDJPY", 150.000, 0.020, 3, 0.050},
		{"AUDUSD", 0.65500, 0.00020, 5, 0.00050},
		{"USDCHF", 0.88000, 0.00020, 5, 0.00050},
		{"USDCAD", 1.36000, 0.00020, 5, 0.00050},
		{"NZDUSD", 0.61500, 0.00020, 5, 0.00050},
	}
}

// FileReader reads lines from a CSV or space-separated file.
type FileReader struct {
	lines []string
	pos   int
	path  string
}

func NewFileReader(path string) (*FileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// Convert CSV (comma/semicolon) to space-separated format
		line = strings.ReplaceAll(line, ",", " ")
		line = strings.ReplaceAll(line, ";", " ")
		line = strings.Join(strings.Fields(line), " ")
		lines = append(lines, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("file is empty: %s", path)
	}
	return &FileReader{lines: lines, pos: 0, path: path}, nil
}

func (fr *FileReader) Remaining() int { return len(fr.lines) - fr.pos }
func (fr *FileReader) Total() int     { return len(fr.lines) }
func (fr *FileReader) Done() bool     { return fr.pos >= len(fr.lines) }

func (fr *FileReader) Next() string {
	if fr.Done() {
		return ""
	}
	line := fr.lines[fr.pos]
	fr.pos++
	return line
}

func (fr *FileReader) NextN(n int) []string {
	if fr.Done() {
		return nil
	}
	end := fr.pos + n
	if end > len(fr.lines) {
		end = len(fr.lines)
	}
	result := fr.lines[fr.pos:end]
	fr.pos = end
	return result
}

func parseTime(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q (expected: 2006-01-02 15:04:05)", s)
}

func filterSymbols(symbols []*Symbol, name string) []*Symbol {
	if name == "" {
		return symbols
	}
	var filtered []*Symbol
	for _, s := range symbols {
		if strings.EqualFold(s.Name, name) {
			filtered = append(filtered, s)
		}
	}
	if len(filtered) == 0 {
		fmt.Fprintf(os.Stderr, "error: unknown symbol %q\navailable:", name)
		for _, s := range symbols {
			fmt.Fprintf(os.Stderr, " %s", s.Name)
		}
		fmt.Fprintln(os.Stderr)
		os.Exit(1)
	}
	return filtered
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: quote-gen [options]

Quote generator: interactive sending, auto generation, file loading.

Modes:
  Interactive (default)  Send random quotes one-by-one (Enter/N/q)
  Auto (--rate N)        Send N quotes per second continuously
  Generate (--gen)       Generate quotes_history data to stdout / Kafka
  File (--file PATH)     Send lines from a file

Connection:
  --broker       Kafka broker address (default: kafka:9092)
  --user         SASL PLAIN username (default: admin)
  --password     SASL PLAIN password (required for Kafka modes)
  --tls          Use TLS (default: false)
  --topic        Kafka topic name (default: quotes)

Common:
  --symbol       Symbol filter, e.g. EURUSD (default: all)
  --rate         Messages per second, 0 = interactive (default: 0)
  --history      Add timestamp (ms) to quotes (quotes_history format)
  --file         CSV/space-separated file to send

Generate (--gen):
  --from         Start time (required), e.g. '2026-02-15 20:00:00'
  --to           End time (required), e.g. '2026-02-15 20:30:00'
  --interval     Tick interval in ms (default: 1000)
  --price        Starting bid price, 0 = symbol default (default: 0)
  --seed         Random seed, 0 = current time (default: 0)

Examples:
  # --- Interactive mode ---

  # All symbols, one tick per Enter
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>'

  # One symbol only
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' --symbol EURUSD

  # History format (with timestamp) to quotes_history topic
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' --topic quotes_history --history

  # --- Auto mode ---

  # 10 ticks/s, all symbols
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' --rate 10

  # 100 ticks/s, one symbol
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' --symbol EURUSD --rate 100

  # --- Generate mode (--gen) ---

  # Generate to stdout (one symbol, 5 sec)
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 20:00:05" \
    --symbol EURUSD --seed 42

  # Generate all symbols (3 sec)
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 20:00:03" \
    --seed 42

  # Generate one symbol, custom interval 500ms
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --interval 500

  # Generate and send to Kafka (batch)
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --broker 95.217.61.39:9094 --tls --user admin --password '<KAFKA_PASSWORD>' \
    --topic quotes_history

  # Generate and send with rate limit (1000 msg/s)
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --broker 95.217.61.39:9094 --tls --user admin --password '<KAFKA_PASSWORD>' \
    --topic quotes_history --rate 1000

  # Generate with fixed seed (reproducible data)
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --seed 42

  # Generate with custom starting price
  quote-gen --gen --from "2026-02-15 20:00:00" --to "2026-02-15 20:10:00" \
    --symbol EURUSD --price 1.12000

  # --- File mode ---

  # Interactive file sending
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' --topic quotes_history \
    --file data/sample.tsv

  # Auto file sending (500 msg/s)
  quote-gen --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' --topic quotes_history \
    --file data/sample.tsv --rate 500
`)
}

func main() {
	fs := flag.NewFlagSet("quote-gen", flag.ExitOnError)
	fs.Usage = usage

	// Common flags
	symbol := fs.String("symbol", "", "Symbol filter (e.g. EURUSD)")
	rate := fs.Float64("rate", 0, "Messages per second (0 = interactive)")

	// Generate mode flags
	gen := fs.Bool("gen", false, "Generate quotes_history data to stdout (no Kafka needed)")
	from := fs.String("from", "", "Start time for --gen (e.g. '2026-02-15 20:00:00')")
	to := fs.String("to", "", "End time for --gen (e.g. '2026-02-15 20:30:00')")
	interval := fs.Int("interval", 1000, "Tick interval in ms for --gen (default 1000)")
	price := fs.Float64("price", 0, "Starting bid price for --gen (0 = use symbol default)")
	seed := fs.Int64("seed", 0, "Random seed for --gen (0 = current time)")

	// Kafka mode flags
	broker := fs.String("broker", "kafka:9092", "Kafka broker address")
	topic := fs.String("topic", "quotes", "Kafka topic name")
	username := fs.String("user", "admin", "SASL username")
	password := fs.String("password", "", "SASL password (required for Kafka modes)")
	file := fs.String("file", "", "CSV/space-separated file to send")
	history := fs.Bool("history", false, "Add timestamp (ms) to generated quotes (quotes_history format)")
	useTLS := fs.Bool("tls", false, "Use TLS (certificate verification disabled)")

	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "-help" || os.Args[1] == "help") {
		usage()
		return
	}
	fs.Parse(os.Args[1:])

	// Helper: create Kafka writer (returns nil if password is empty)
	makeWriter := func() *kafka.Writer {
		if *password == "" {
			return nil
		}
		brokerHost, brokerPort, _ := net.SplitHostPort(*broker)
		if brokerPort == "" {
			brokerPort = "9092"
		}
		transport := &kafka.Transport{
			SASL: plain.Mechanism{
				Username: *username,
				Password: *password,
			},
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				_, port, err := net.SplitHostPort(address)
				if err != nil {
					port = brokerPort
				}
				target := net.JoinHostPort(brokerHost, port)
				return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, target)
			},
		}
		if *useTLS {
			transport.TLS = &tls.Config{InsecureSkipVerify: true}
		}
		return &kafka.Writer{
			Addr:         kafka.TCP(*broker),
			Topic:        *topic,
			Transport:    transport,
			BatchTimeout: 10 * time.Millisecond,
		}
	}

	// --- Generate mode: stdout + optional Kafka ---
	if *gen {
		if *from == "" || *to == "" {
			fmt.Fprintln(os.Stderr, "error: --from and --to are required with --gen")
			os.Exit(1)
		}
		fromTime, err := parseTime(*from)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --from: %v\n", err)
			os.Exit(1)
		}
		toTime, err := parseTime(*to)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --to: %v\n", err)
			os.Exit(1)
		}
		if !toTime.After(fromTime) {
			fmt.Fprintln(os.Stderr, "error: --to must be after --from")
			os.Exit(1)
		}
		if *seed != 0 {
			rand.Seed(*seed)
		} else {
			rand.Seed(time.Now().UnixNano())
		}
		symbols := filterSymbols(newSymbols(), *symbol)
		if *price > 0 && len(symbols) == 1 {
			symbols[0].Price = *price
		}
		writer := makeWriter()
		if writer != nil {
			defer writer.Close()
		}
		runGenerate(symbols, fromTime, toTime, *interval, writer, *rate)
		return
	}

	// --- Kafka modes: require --password ---
	if *password == "" {
		fmt.Fprintln(os.Stderr, "error: --password is required")
		usage()
		os.Exit(1)
	}

	// File mode: load file
	var fr *FileReader
	if *file != "" {
		var err error
		fr, err = NewFileReader(*file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}

	writer := makeWriter()
	defer writer.Close()

	// Print header
	fmt.Println("Quote Generator")
	fmt.Printf("  broker  : %s\n", *broker)
	fmt.Printf("  topic   : %s\n", *topic)
	if *useTLS {
		fmt.Printf("  tls     : on (insecure)\n")
	}

	if fr != nil {
		fmt.Printf("  mode    : file (%s, %d lines)\n", fr.path, fr.Total())
		if *rate > 0 {
			fmt.Printf("  rate    : %.1f msg/s\n", *rate)
			fmt.Println()
			runFileAuto(writer, fr, *rate)
		} else {
			fmt.Println()
			fmt.Println("Commands: Enter — send next | N — send N lines | q — quit")
			fmt.Println()
			runFileInteractive(writer, fr)
		}
	} else {
		symbols := filterSymbols(newSymbols(), *symbol)
		if *symbol != "" {
			fmt.Printf("  symbol  : %s\n", *symbol)
		}
		if *history {
			fmt.Printf("  mode    : random JSON {symbol, bid, ask, ts_ms}\n")
		} else {
			fmt.Printf("  mode    : random JSON {symbol, bid, ask}\n")
		}
		if *rate > 0 {
			fmt.Printf("  rate    : %.1f msg/s\n", *rate)
			fmt.Println()
			runRandomAuto(writer, symbols, *history, *rate)
		} else {
			fmt.Println()
			fmt.Println("Commands: Enter — send 1 random quote | N — send N quotes | q — quit")
			fmt.Println()
			runRandomInteractive(writer, symbols, *history)
		}
	}
}

// --- Generate mode: write quotes_history lines to stdout ---

func runGenerate(symbols []*Symbol, from, to time.Time, intervalMs int, kw *kafka.Writer, rate float64) {
	out := bufio.NewWriter(os.Stdout)

	step := time.Duration(intervalMs) * time.Millisecond
	var lines []string
	count := 0

	for t := from; t.Before(to); t = t.Add(step) {
		for _, s := range symbols {
			s.Tick()
			jitter := int64(rand.Intn(intervalMs))
			ts := t.UnixMilli() + jitter
			ask := s.Price + s.Spread
			line := fmt.Sprintf(`{"symbol":"%s","bid":%.*f,"ask":%.*f,"ts_ms":%d}`,
				s.Name, s.Decimals, s.Price, s.Decimals, ask, ts)
			fmt.Fprintln(out, line)
			lines = append(lines, line)
			count++
		}
	}
	out.Flush()

	fmt.Fprintf(os.Stderr, "Generated %d ticks (%d symbols × %s, interval %dms)\n",
		count, len(symbols), to.Sub(from), intervalMs)

	// Send to Kafka if writer provided
	if kw == nil || len(lines) == 0 {
		return
	}

	fmt.Fprintf(os.Stderr, "Sending %d messages to Kafka...\n", len(lines))
	start := time.Now()

	if rate <= 0 {
		// Batch send: chunks of 1000
		for i := 0; i < len(lines); i += 1000 {
			end := i + 1000
			if end > len(lines) {
				end = len(lines)
			}
			if err := send(kw, lines[i:end]); err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
				return
			}
		}
	} else {
		// Rate-limited send
		interval := time.Duration(float64(time.Second) / rate)
		for i, line := range lines {
			if err := send(kw, []string{line}); err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
				return
			}
			if (i+1)%500 == 0 {
				elapsed := time.Since(start)
				fmt.Fprintf(os.Stderr, "\r  %d/%d sent (%.1f msg/s)", i+1, len(lines), float64(i+1)/elapsed.Seconds())
			}
			if i < len(lines)-1 {
				time.Sleep(interval)
			}
		}
	}

	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "\r  done: %d sent in %s (%.1f msg/s)\n",
		len(lines), elapsed.Round(time.Millisecond), float64(len(lines))/elapsed.Seconds())
}

func send(w *kafka.Writer, lines []string) error {
	msgs := make([]kafka.Message, len(lines))
	for i, line := range lines {
		msgs[i] = kafka.Message{Value: []byte(line)}
	}
	return w.WriteMessages(context.Background(), msgs...)
}

// --- File interactive mode ---

func runFileInteractive(w *kafka.Writer, fr *FileReader) {
	scanner := bufio.NewScanner(os.Stdin)

	for !fr.Done() {
		fmt.Printf("[%d/%d] > ", fr.Total()-fr.Remaining(), fr.Total())
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())

		switch {
		case input == "q" || input == "quit":
			fmt.Println("Bye!")
			return

		case input == "":
			line := fr.Next()
			if line != "" {
				fmt.Printf("  → %s\n", line)
				if err := send(w, []string{line}); err != nil {
					fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
				}
			}

		default:
			n, err := strconv.Atoi(input)
			if err != nil || n <= 0 {
				fmt.Println("  unknown command (Enter, N, q)")
				continue
			}
			lines := fr.NextN(n)
			if len(lines) == 0 {
				fmt.Println("  EOF")
				continue
			}
			for _, l := range lines {
				fmt.Printf("  → %s\n", l)
			}
			if err := send(w, lines); err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
			} else {
				fmt.Printf("  sent %d lines\n", len(lines))
			}
		}
	}
	fmt.Println("  EOF — all lines sent")
}

// --- File auto mode (with rate control) ---

func runFileAuto(w *kafka.Writer, fr *FileReader, rate float64) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent := 0
	start := time.Now()

	fmt.Println("Sending... (Ctrl+C to stop)")

	for !fr.Done() {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			fmt.Printf("\n  stopped: %d/%d sent in %s (%.1f msg/s)\n", sent, fr.Total(), elapsed.Round(time.Millisecond), float64(sent)/elapsed.Seconds())
			return
		case <-ticker.C:
			line := fr.Next()
			if line == "" {
				break
			}
			if err := send(w, []string{line}); err != nil {
				fmt.Fprintf(os.Stderr, "\n  ERROR: %v\n", err)
				return
			}
			sent++
			if sent%100 == 0 {
				elapsed := time.Since(start)
				fmt.Printf("\r  %d/%d sent (%.1f msg/s)", sent, fr.Total(), float64(sent)/elapsed.Seconds())
			}
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\r  done: %d/%d sent in %s (%.1f msg/s)\n", sent, fr.Total(), elapsed.Round(time.Millisecond), float64(sent)/elapsed.Seconds())
}

// --- Random interactive mode ---

func runRandomInteractive(w *kafka.Writer, symbols []*Symbol, history bool) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())

		switch {
		case input == "q" || input == "quit":
			fmt.Println("Bye!")
			return

		case input == "":
			s := symbols[rand.Intn(len(symbols))]
			s.Tick()
			var val string
			if history {
				val = s.FormatHistory()
			} else {
				val = s.Format()
			}
			if err := send(w, []string{val}); err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
			} else {
				fmt.Printf("  → %s\n", val)
			}

		default:
			n, err := strconv.Atoi(input)
			if err != nil || n <= 0 {
				fmt.Println("  unknown command (Enter, N, q)")
				continue
			}
			lines := make([]string, n)
			for i := range lines {
				s := symbols[rand.Intn(len(symbols))]
				s.Tick()
				if history {
					lines[i] = s.FormatHistory()
				} else {
					lines[i] = s.Format()
				}
				fmt.Printf("  → %s\n", lines[i])
			}
			if err := send(w, lines); err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
			}
		}
	}
}

// --- Random auto mode (with rate control) ---

func runRandomAuto(w *kafka.Writer, symbols []*Symbol, history bool, rate float64) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent := 0
	start := time.Now()

	fmt.Println("Sending... (Ctrl+C to stop)")

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			fmt.Printf("\n  stopped: %d sent in %s (%.1f msg/s)\n", sent, elapsed.Round(time.Millisecond), float64(sent)/elapsed.Seconds())
			return
		case <-ticker.C:
			s := symbols[rand.Intn(len(symbols))]
			s.Tick()
			var val string
			if history {
				val = s.FormatHistory()
			} else {
				val = s.Format()
			}
			if err := send(w, []string{val}); err != nil {
				fmt.Fprintf(os.Stderr, "\n  ERROR: %v\n", err)
				return
			}
			sent++
			if sent%100 == 0 {
				elapsed := time.Since(start)
				fmt.Printf("\r  %d sent (%.1f msg/s)", sent, float64(sent)/elapsed.Seconds())
			}
		}
	}
}
