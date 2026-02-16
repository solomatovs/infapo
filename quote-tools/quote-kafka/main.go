package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// ---------------------------------------------------------------------------
// Quotes pipeline constants
// ---------------------------------------------------------------------------

var defaultTopics = []string{"quotes", "quotes_history"}

var defaultGroups = []string{
	"clickhouse_quotes",
	"clickhouse_quotes_history",
}

const defaultPartitions = 3

// ---------------------------------------------------------------------------
// Config & connections
// ---------------------------------------------------------------------------

type Config struct {
	Broker   string
	User     string
	Password string
	UseTLS   bool
}

func (c *Config) dialer() *kafka.Dialer {
	d := &kafka.Dialer{Timeout: 10 * time.Second}
	if c.User != "" {
		d.SASLMechanism = plain.Mechanism{
			Username: c.User,
			Password: c.Password,
		}
	}
	if c.UseTLS {
		d.TLS = &tls.Config{InsecureSkipVerify: true}
	}
	return d
}

func (c *Config) transport() *kafka.Transport {
	t := &kafka.Transport{}
	if c.User != "" {
		t.SASL = plain.Mechanism{
			Username: c.User,
			Password: c.Password,
		}
	}
	if c.UseTLS {
		t.TLS = &tls.Config{InsecureSkipVerify: true}
	}
	return t
}

func (c *Config) connect() (*kafka.Conn, error) {
	return c.dialer().DialContext(context.Background(), "tcp", c.Broker)
}

func (c *Config) controllerConn() (*kafka.Conn, error) {
	conn, err := c.connect()
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	controller, err := conn.Controller()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("find controller: %w", err)
	}
	conn.Close()

	addr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	adminConn, err := c.dialer().DialContext(context.Background(), "tcp", addr)
	if err != nil {
		adminConn, err = c.connect()
		if err != nil {
			return nil, fmt.Errorf("connect to controller: %w", err)
		}
	}
	return adminConn, nil
}

func (c *Config) client() *kafka.Client {
	return &kafka.Client{
		Addr:      kafka.TCP(c.Broker),
		Transport: c.transport(),
	}
}

// topicExists returns true if a topic exists on the broker.
func (c *Config) topicExists(topic string) bool {
	conn, err := c.connect()
	if err != nil {
		return false
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(topic)
	return err == nil && len(parts) > 0
}

// topicMeta returns partition count and replication factor per topic.
func (c *Config) topicMeta(topics ...string) (map[string][2]int, error) {
	conn, err := c.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(topics...)
	if err != nil {
		return nil, err
	}

	meta := map[string][2]int{}
	for _, p := range parts {
		m := meta[p.Topic]
		m[0]++
		if rf := len(p.Replicas); rf > m[1] {
			m[1] = rf
		}
		meta[p.Topic] = m
	}
	return meta, nil
}

// ---------------------------------------------------------------------------
// Consumer group operations
// ---------------------------------------------------------------------------

func deleteGroups(cfg *Config, groups []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := cfg.client()

	for _, group := range groups {
		fmt.Printf("  DELETE group %s ... ", group)

		resp, err := client.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{
			GroupIDs: []string{group},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			continue
		}

		if errEntry, ok := resp.Errors[group]; ok && errEntry != nil {
			msg := errEntry.Error()
			if strings.Contains(msg, "UNKNOWN") ||
				strings.Contains(msg, "GROUP_ID_NOT_FOUND") ||
				strings.Contains(msg, "not found") {
				fmt.Println("NOT FOUND (skip)")
			} else {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", errEntry)
			}
		} else {
			fmt.Println("OK")
		}
	}
}

// ---------------------------------------------------------------------------
// create — create topics if they don't exist
// ---------------------------------------------------------------------------

func runCreate(cfg *Config, topics []string, partitions, replication int) {
	fmt.Println("  --- Create topics (if not exist) ---")

	conn, err := cfg.controllerConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	hasError := false
	for _, topic := range topics {
		if cfg.topicExists(topic) {
			fmt.Printf("  CREATE %s ... ALREADY EXISTS (skip)\n", topic)
			continue
		}
		fmt.Printf("  CREATE %s (partitions=%d, rf=%d) ... ", topic, partitions, replication)
		if err := conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replication,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			hasError = true
		} else {
			fmt.Println("OK")
		}
	}

	fmt.Println()
	if hasError {
		fmt.Println("  Completed with errors.")
		os.Exit(1)
	}
	fmt.Println("  Done.")
}

// ---------------------------------------------------------------------------
// delete — delete topics + consumer groups
// ---------------------------------------------------------------------------

func runDelete(cfg *Config, topics, groups []string) {
	fmt.Println("  --- Delete topics ---")

	conn, err := cfg.controllerConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	hasError := false
	for _, topic := range topics {
		fmt.Printf("  DELETE %s ... ", topic)
		if err := conn.DeleteTopics(topic); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "UNKNOWN_TOPIC") || strings.Contains(msg, "does not exist") {
				fmt.Println("NOT FOUND (skip)")
			} else {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
				hasError = true
			}
		} else {
			fmt.Println("OK")
		}
	}
	conn.Close()

	fmt.Println()
	fmt.Println("  --- Delete consumer groups ---")
	deleteGroups(cfg, groups)

	fmt.Println()
	if hasError {
		fmt.Println("  Completed with errors.")
		os.Exit(1)
	}
	fmt.Println("  Done.")
}

// ---------------------------------------------------------------------------
// recreate — delete + create topics, delete consumer groups
// ---------------------------------------------------------------------------

func runRecreate(cfg *Config, topics, groups []string, partitions, replication int) {
	// Read current metadata so we can preserve config when needed
	meta, _ := cfg.topicMeta(topics...)

	type tc struct{ p, r int }
	configs := make(map[string]tc, len(topics))
	for _, topic := range topics {
		p, r := partitions, replication
		if m, ok := meta[topic]; ok {
			if p < 0 {
				p = m[0]
			}
			if r < 0 {
				r = m[1]
			}
		}
		if p <= 0 {
			p = defaultPartitions
		}
		if r <= 0 {
			r = 1
		}
		configs[topic] = tc{p, r}
	}

	// Delete consumer groups first
	fmt.Println("  --- Delete consumer groups ---")
	deleteGroups(cfg, groups)

	// Delete topics
	fmt.Println()
	fmt.Println("  --- Delete topics ---")

	conn, err := cfg.controllerConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	for _, topic := range topics {
		fmt.Printf("  DELETE %s ... ", topic)
		if err := conn.DeleteTopics(topic); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "UNKNOWN_TOPIC") || strings.Contains(msg, "does not exist") {
				fmt.Println("NOT FOUND (skip)")
			} else {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			}
		} else {
			fmt.Println("OK")
		}
	}
	conn.Close()

	// Wait for deletion to propagate
	fmt.Println()
	fmt.Print("  Waiting for deletion to propagate ... ")
	time.Sleep(3 * time.Second)
	fmt.Println("OK")

	// Reconnect and recreate
	fmt.Println()
	fmt.Println("  --- Create topics ---")

	conn, err = cfg.controllerConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reconnecting: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	hasError := false
	for _, topic := range topics {
		c := configs[topic]
		fmt.Printf("  CREATE %s (partitions=%d, rf=%d) ... ", topic, c.p, c.r)
		if err := conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     c.p,
			ReplicationFactor: c.r,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			hasError = true
		} else {
			fmt.Println("OK")
		}
	}

	fmt.Println()
	if hasError {
		fmt.Println("  Completed with errors.")
		os.Exit(1)
	}
	fmt.Println("  Done.")
}

// ---------------------------------------------------------------------------
// purge — clear all messages (delete + recreate, preserve config)
// ---------------------------------------------------------------------------

func runPurge(cfg *Config, topics, groups []string) {
	// Preserve original partition count and replication factor
	runRecreate(cfg, topics, groups, -1, -1)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: quote-kafka <command> [options]

Manage Kafka topics and consumer groups for the quotes pipeline.

Topics:   %s
Groups:   %s

Commands:
  create      Create topics if they don't exist
  recreate    Delete and recreate topics + consumer groups
  delete      Delete topics + consumer groups
  purge       Clear all messages (delete + recreate, preserving config)

Options:
  --broker       Kafka broker address (default: kafka:9092)
  --user         SASL PLAIN username (required)
  --password     SASL PLAIN password (required)
  --tls          Use TLS (default: false)

Create/Recreate options:
  --partitions   Number of partitions (default: %d)
  --replication  Replication factor  (default: 1)

Examples:
  # --- create ---

  # Create topics (idempotent, 3 partitions by default)
  quote-kafka create --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>'

  # Create with custom partition count
  quote-kafka create --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' \
    --partitions 6

  # --- recreate ---

  # Recreate topics + delete consumer groups
  quote-kafka recreate --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>'

  # Recreate with different partition count
  quote-kafka recreate --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>' \
    --partitions 6

  # --- delete ---

  # Delete topics + consumer groups
  quote-kafka delete --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>'

  # --- purge ---

  # Clear all messages (delete + recreate, preserving config)
  quote-kafka purge --broker 95.217.61.39:9094 --tls \
    --user admin --password '<KAFKA_PASSWORD>'
`, strings.Join(defaultTopics, ", "), strings.Join(defaultGroups, ", "), defaultPartitions)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	if cmd == "-h" || cmd == "--help" || cmd == "-help" || cmd == "help" {
		usage()
		return
	}

	fs := flag.NewFlagSet("quote-kafka", flag.ExitOnError)
	broker := fs.String("broker", "kafka:9092", "Kafka broker address")
	user := fs.String("user", "", "SASL PLAIN username")
	password := fs.String("password", "", "SASL PLAIN password")
	useTLS := fs.Bool("tls", false, "Use TLS")
	partitions := fs.Int("partitions", -1, "Number of partitions")
	replication := fs.Int("replication", -1, "Replication factor")
	fs.Parse(os.Args[2:])

	if (*user != "" || *password != "") && (*user == "" || *password == "") {
		fmt.Fprintln(os.Stderr, "error: --user and --password must both be specified")
		os.Exit(1)
	}

	cfg := &Config{
		Broker:   *broker,
		User:     *user,
		Password: *password,
		UseTLS:   *useTLS,
	}

	fmt.Println("Quote Kafka")
	fmt.Printf("  broker    : %s\n", cfg.Broker)
	fmt.Printf("  command   : %s\n", cmd)
	fmt.Printf("  topics    : %s\n", strings.Join(defaultTopics, ", "))
	fmt.Printf("  groups    : %s\n", strings.Join(defaultGroups, ", "))
	fmt.Println()

	switch cmd {
	case "create":
		p := *partitions
		if p < 0 {
			p = defaultPartitions
		}
		r := *replication
		if r < 0 {
			r = 1
		}
		runCreate(cfg, defaultTopics, p, r)

	case "delete":
		runDelete(cfg, defaultTopics, defaultGroups)

	case "recreate":
		runRecreate(cfg, defaultTopics, defaultGroups, *partitions, *replication)

	case "purge":
		runPurge(cfg, defaultTopics, defaultGroups)

	default:
		fmt.Fprintf(os.Stderr, "error: unknown command %q\n\n", cmd)
		usage()
		os.Exit(1)
	}
}
