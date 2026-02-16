use std::collections::BTreeMap;
use std::io::Write;

use crate::config::Config;
use crate::kafka_admin::KafkaAdmin;

use clap::Args;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::record::Record;

// ═══════════════════════════════════════════════════════════════
//  CLI args
// ═══════════════════════════════════════════════════════════════

#[derive(Args, Clone, Debug)]
pub struct GenArgs {
    /// Symbol filter (e.g. EURUSD). If omitted — all 9 symbols
    #[arg(long)]
    pub symbol: Option<String>,

    /// Messages per second (0 = interactive)
    #[arg(long, default_value_t = 0.0)]
    pub rate: f64,

    /// Add ts_ms timestamp to output
    #[arg(long)]
    pub history: bool,

    /// CSV/space-separated file to send
    #[arg(long)]
    pub file: Option<String>,

    /// Generate historical data (to stdout + optional Kafka)
    #[arg(long)]
    pub generate: bool,

    /// Start time for --generate (UTC), e.g. "2026-02-15 20:00:00"
    #[arg(long)]
    pub from: Option<String>,

    /// End time for --generate (UTC), e.g. "2026-02-15 21:00:00"
    #[arg(long)]
    pub to: Option<String>,

    /// Tick interval in ms for --generate (default 1000)
    #[arg(long, default_value_t = 1000)]
    pub interval: u64,

    /// Starting bid price (0 = symbol default)
    #[arg(long, default_value_t = 0.0)]
    pub price: f64,

    /// Random seed (0 = current time)
    #[arg(long, default_value_t = 0)]
    pub seed: i64,
}

// ═══════════════════════════════════════════════════════════════
//  Symbol
// ═══════════════════════════════════════════════════════════════

struct Symbol {
    name: &'static str,
    price: f64,
    spread: f64,
    decimals: usize,
    step: f64,
}

impl Symbol {
    fn tick(&mut self, rng: &mut Rng) {
        let delta = (rng.next_f64() * 2.0 - 1.0) * self.step;
        self.price += delta;
        if self.price < self.step {
            self.price = self.step;
        }
    }

    fn format_json(&self) -> String {
        let ask = self.price + self.spread;
        format!(
            r#"{{"symbol":"{}","bid":{:.prec$},"ask":{:.prec$}}}"#,
            self.name, self.price, ask, prec = self.decimals
        )
    }

    fn format_json_ts(&self, ts_ms: i64) -> String {
        let ask = self.price + self.spread;
        format!(
            r#"{{"symbol":"{}","bid":{:.prec$},"ask":{:.prec$},"ts_ms":{}}}"#,
            self.name, self.price, ask, ts_ms, prec = self.decimals
        )
    }
}

fn new_symbols() -> Vec<Symbol> {
    vec![
        Symbol { name: "XAUUSD", price: 2650.00, spread: 0.70, decimals: 2, step: 2.00 },
        Symbol { name: "XAGUSD", price: 31.50, spread: 0.03, decimals: 2, step: 0.05 },
        Symbol { name: "EURUSD", price: 1.08500, spread: 0.00020, decimals: 5, step: 0.00050 },
        Symbol { name: "GBPUSD", price: 1.26500, spread: 0.00020, decimals: 5, step: 0.00050 },
        Symbol { name: "USDJPY", price: 150.000, spread: 0.020, decimals: 3, step: 0.050 },
        Symbol { name: "AUDUSD", price: 0.65500, spread: 0.00020, decimals: 5, step: 0.00050 },
        Symbol { name: "USDCHF", price: 0.88000, spread: 0.00020, decimals: 5, step: 0.00050 },
        Symbol { name: "USDCAD", price: 1.36000, spread: 0.00020, decimals: 5, step: 0.00050 },
        Symbol { name: "NZDUSD", price: 0.61500, spread: 0.00020, decimals: 5, step: 0.00050 },
    ]
}

// ═══════════════════════════════════════════════════════════════
//  RNG (xorshift64)
// ═══════════════════════════════════════════════════════════════

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: i64) -> Self {
        let state = if seed == 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
                | 1 // ensure non-zero
        } else {
            seed as u64
        };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    /// Returns f64 in [0, 1)
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    fn next_intn(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ═══════════════════════════════════════════════════════════════
//  File reader
// ═══════════════════════════════════════════════════════════════

struct FileReader {
    lines: Vec<String>,
    pos: usize,
    path: String,
}

impl FileReader {
    fn open(path: &str) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("cannot open {path}: {e}"))?;
        let lines: Vec<String> = content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(|l| {
                // Normalize separators: comma/semicolon → space, collapse whitespace
                let l = l.replace(',', " ").replace(';', " ");
                l.split_whitespace().collect::<Vec<_>>().join(" ")
            })
            .collect();
        if lines.is_empty() {
            return Err(format!("file is empty: {path}"));
        }
        Ok(Self {
            lines,
            pos: 0,
            path: path.to_string(),
        })
    }

    fn total(&self) -> usize {
        self.lines.len()
    }
    fn remaining(&self) -> usize {
        self.lines.len() - self.pos
    }
    fn done(&self) -> bool {
        self.pos >= self.lines.len()
    }

    fn next_line(&mut self) -> Option<&str> {
        if self.done() {
            return None;
        }
        let line = &self.lines[self.pos];
        self.pos += 1;
        Some(line)
    }

    fn next_n(&mut self, n: usize) -> &[String] {
        if self.done() {
            return &[];
        }
        let end = (self.pos + n).min(self.lines.len());
        let slice = &self.lines[self.pos..end];
        self.pos = end;
        slice
    }
}

// ═══════════════════════════════════════════════════════════════
//  Kafka helpers
// ═══════════════════════════════════════════════════════════════

struct Producer {
    _client: rskafka::client::Client,
    partition: PartitionClient,
}

async fn make_producer(cfg: &Config) -> Result<Option<Producer>, String> {
    if cfg.kafka_password.is_empty() {
        return Ok(None);
    }
    let kafka = KafkaAdmin::new(cfg);
    let client = kafka.build_client().await?;
    let partition = client
        .partition_client(&cfg.topic, 0, UnknownTopicHandling::Retry)
        .await
        .map_err(|e| format!("Kafka partition client: {e}"))?;
    Ok(Some(Producer {
        _client: client,
        partition,
    }))
}

async fn kafka_send(producer: &PartitionClient, lines: &[String]) -> Result<(), String> {
    if lines.is_empty() {
        return Ok(());
    }
    let now = chrono::Utc::now();
    let records: Vec<Record> = lines
        .iter()
        .map(|line| Record {
            key: None,
            value: Some(line.as_bytes().to_vec()),
            headers: BTreeMap::new(),
            timestamp: now,
        })
        .collect();
    producer
        .produce(records, Compression::NoCompression)
        .await
        .map_err(|e| format!("Kafka send: {e}"))?;
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════
//  Time parsing (multiple formats)
// ═══════════════════════════════════════════════════════════════

fn parse_time(s: &str) -> Result<i64, String> {
    let s = s.trim();
    // Try formats: "YYYY-MM-DD HH:MM:SS", "YYYY-MM-DDTHH:MM:SS",
    //              "YYYY-MM-DD HH:MM", "YYYY-MM-DDTHH:MM", "YYYY-MM-DD"
    let (date_part, time_part) = if s.len() >= 11 && (s.as_bytes()[10] == b'T' || s.as_bytes()[10] == b' ')
    {
        (&s[..10], Some(&s[11..]))
    } else {
        (s, None)
    };

    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("bad date: {s}"));
    }
    let year: i64 = parts[0].parse().map_err(|_| format!("bad year: {s}"))?;
    let month: u32 = parts[1].parse().map_err(|_| format!("bad month: {s}"))?;
    let day: u32 = parts[2].parse().map_err(|_| format!("bad day: {s}"))?;

    let (hour, min, sec) = if let Some(t) = time_part {
        let tp: Vec<&str> = t.split(':').collect();
        let h: u32 = tp.first().and_then(|v| v.parse().ok()).unwrap_or(0);
        let m: u32 = tp.get(1).and_then(|v| v.parse().ok()).unwrap_or(0);
        let sc: u32 = tp.get(2).and_then(|v| v.parse().ok()).unwrap_or(0);
        (h, m, sc)
    } else {
        (0, 0, 0)
    };

    // civil_from_days (Howard Hinnant algorithm)
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (m as u64) + 2) / 5 + (day as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era as i64 * 146097 + doe as i64 - 719468;

    Ok(days * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64)
}

// ═══════════════════════════════════════════════════════════════
//  Main dispatch
// ═══════════════════════════════════════════════════════════════

pub async fn run(cfg: &Config, args: &GenArgs) -> Result<(), String> {
    let mut rng = Rng::new(args.seed);
    let mut symbols = new_symbols();

    if let Some(ref sym) = args.symbol {
        symbols.retain(|s| s.name.eq_ignore_ascii_case(sym));
        if symbols.is_empty() {
            let names: Vec<_> = new_symbols().iter().map(|s| s.name).collect();
            return Err(format!(
                "unknown symbol: {sym}\navailable: {}",
                names.join(" ")
            ));
        }
    }

    if args.price > 0.0 && symbols.len() == 1 {
        symbols[0].price = args.price;
    }

    if args.generate {
        let from = args
            .from
            .as_deref()
            .ok_or("--from is required with --generate")?;
        let to = args
            .to
            .as_deref()
            .ok_or("--to is required with --generate")?;
        let from_sec = parse_time(from)?;
        let to_sec = parse_time(to)?;
        if to_sec <= from_sec {
            return Err("--to must be after --from".to_string());
        }
        run_generate(cfg, args, &mut symbols, &mut rng, from_sec, to_sec).await
    } else if let Some(ref path) = args.file {
        let path = path.clone();
        run_file(cfg, args, &path).await
    } else {
        run_stream(cfg, args, &mut symbols, &mut rng).await
    }
}

// ═══════════════════════════════════════════════════════════════
//  Generate mode: stdout + optional Kafka
// ═══════════════════════════════════════════════════════════════

async fn run_generate(
    cfg: &Config,
    args: &GenArgs,
    symbols: &mut [Symbol],
    rng: &mut Rng,
    from_sec: i64,
    to_sec: i64,
) -> Result<(), String> {
    let interval_ms = args.interval as i64;
    let from_ms = from_sec * 1000;
    let to_ms = to_sec * 1000;

    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    let mut lines: Vec<String> = Vec::new();

    let mut t_ms = from_ms;
    while t_ms < to_ms {
        for sym in symbols.iter_mut() {
            sym.tick(rng);
            let jitter = rng.next_intn(interval_ms as usize) as i64;
            let ts = t_ms + jitter;
            let line = sym.format_json_ts(ts);
            let _ = writeln!(out, "{line}");
            lines.push(line);
        }
        t_ms += interval_ms;
    }
    out.flush().ok();

    let duration = to_sec - from_sec;
    eprintln!(
        "Generated {} ticks ({} symbols × {}s, interval {}ms)",
        lines.len(),
        symbols.len(),
        duration,
        interval_ms
    );

    // Send to Kafka if credentials provided
    let producer = make_producer(cfg).await?;
    let producer = match producer {
        Some(p) => p,
        None => return Ok(()),
    };

    eprintln!("Sending {} messages to Kafka...", lines.len());
    let start = std::time::Instant::now();

    if args.rate <= 0.0 {
        // Batch send: chunks of 1000
        for chunk in lines.chunks(1000) {
            kafka_send(&producer.partition, chunk).await?;
        }
    } else {
        // Rate-limited send
        let interval = std::time::Duration::from_secs_f64(1.0 / args.rate);
        for (i, line) in lines.iter().enumerate() {
            kafka_send(&producer.partition, std::slice::from_ref(line)).await?;
            if (i + 1) % 500 == 0 {
                let elapsed = start.elapsed();
                eprint!(
                    "\r  {}/{} sent ({:.1} msg/s)",
                    i + 1,
                    lines.len(),
                    (i + 1) as f64 / elapsed.as_secs_f64()
                );
            }
            if i < lines.len() - 1 {
                tokio::time::sleep(interval).await;
            }
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "\r  done: {} sent in {:.1}s ({:.1} msg/s)",
        lines.len(),
        elapsed.as_secs_f64(),
        lines.len() as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

// ═══════════════════════════════════════════════════════════════
//  Stream mode: interactive / auto
// ═══════════════════════════════════════════════════════════════

async fn run_stream(
    cfg: &Config,
    args: &GenArgs,
    symbols: &mut Vec<Symbol>,
    rng: &mut Rng,
) -> Result<(), String> {
    let producer = make_producer(cfg)
        .await?
        .ok_or("--kafka-password is required for streaming mode")?;

    println!("Quote Generator");
    println!("  broker  : {}", cfg.kafka_broker);
    println!("  topic   : {}", cfg.topic);
    if cfg.kafka_tls {
        println!("  tls     : on (insecure)");
    }
    if let Some(ref sym) = args.symbol {
        println!("  symbol  : {sym}");
    }
    if args.history {
        println!("  format  : {{symbol, bid, ask, ts_ms}}");
    } else {
        println!("  format  : {{symbol, bid, ask}}");
    }

    if args.rate > 0.0 {
        println!("  rate    : {:.1} msg/s", args.rate);
        println!();
        run_random_auto(&producer.partition, symbols, rng, args.history, args.rate).await
    } else {
        println!();
        println!("Commands: Enter — send 1 random quote | N — send N quotes | q — quit");
        println!();
        run_random_interactive(&producer.partition, symbols, rng, args.history).await
    }
}

async fn run_random_interactive(
    producer: &PartitionClient,
    symbols: &mut [Symbol],
    rng: &mut Rng,
    history: bool,
) -> Result<(), String> {
    let stdin = std::io::stdin();

    loop {
        print!("> ");
        std::io::stdout().flush().ok();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap_or(0) == 0 {
            break;
        }
        let input = input.trim();

        if input == "q" || input == "quit" {
            println!("Bye!");
            break;
        }

        if input.is_empty() {
            let idx = rng.next_intn(symbols.len());
            symbols[idx].tick(rng);
            let val = if history {
                symbols[idx].format_json_ts(now_ms())
            } else {
                symbols[idx].format_json()
            };
            match kafka_send(producer, &[val.clone()]).await {
                Ok(_) => println!("  → {val}"),
                Err(e) => eprintln!("  ERROR: {e}"),
            }
        } else {
            match input.parse::<usize>() {
                Ok(n) if n > 0 => {
                    let mut batch = Vec::with_capacity(n);
                    for _ in 0..n {
                        let idx = rng.next_intn(symbols.len());
                        symbols[idx].tick(rng);
                        let val = if history {
                            symbols[idx].format_json_ts(now_ms())
                        } else {
                            symbols[idx].format_json()
                        };
                        println!("  → {val}");
                        batch.push(val);
                    }
                    if let Err(e) = kafka_send(producer, &batch).await {
                        eprintln!("  ERROR: {e}");
                    }
                }
                _ => {
                    println!("  unknown command (Enter, N, q)");
                }
            }
        }
    }

    Ok(())
}

async fn run_random_auto(
    producer: &PartitionClient,
    symbols: &mut [Symbol],
    rng: &mut Rng,
    history: bool,
    rate: f64,
) -> Result<(), String> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs_f64(1.0 / rate));
    let mut sent = 0u64;
    let start = std::time::Instant::now();

    println!("Sending... (Ctrl+C to stop)");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let elapsed = start.elapsed();
                println!(
                    "\n  stopped: {sent} sent in {:.1}s ({:.1} msg/s)",
                    elapsed.as_secs_f64(),
                    sent as f64 / elapsed.as_secs_f64()
                );
                break;
            }
            _ = interval.tick() => {
                let idx = rng.next_intn(symbols.len());
                symbols[idx].tick(rng);
                let val = if history {
                    symbols[idx].format_json_ts(now_ms())
                } else {
                    symbols[idx].format_json()
                };
                if let Err(e) = kafka_send(producer, &[val]).await {
                    eprintln!("\n  ERROR: {e}");
                    return Err(e);
                }
                sent += 1;
                if sent % 100 == 0 {
                    let elapsed = start.elapsed();
                    eprint!("\r  {sent} sent ({:.1} msg/s)", sent as f64 / elapsed.as_secs_f64());
                }
            }
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════
//  File mode: interactive / auto
// ═══════════════════════════════════════════════════════════════

async fn run_file(cfg: &Config, args: &GenArgs, path: &str) -> Result<(), String> {
    let mut fr = FileReader::open(path)?;

    let producer = make_producer(cfg)
        .await?
        .ok_or("--kafka-password is required for file mode")?;

    println!("Quote Generator");
    println!("  broker  : {}", cfg.kafka_broker);
    println!("  topic   : {}", cfg.topic);
    if cfg.kafka_tls {
        println!("  tls     : on (insecure)");
    }
    println!("  mode    : file ({}, {} lines)", fr.path, fr.total());

    if args.rate > 0.0 {
        println!("  rate    : {:.1} msg/s", args.rate);
        println!();
        run_file_auto(&producer.partition, &mut fr, args.rate).await
    } else {
        println!();
        println!("Commands: Enter — send next | N — send N lines | q — quit");
        println!();
        run_file_interactive(&producer.partition, &mut fr).await
    }
}

async fn run_file_interactive(
    producer: &PartitionClient,
    fr: &mut FileReader,
) -> Result<(), String> {
    let stdin = std::io::stdin();

    while !fr.done() {
        print!("[{}/{}] > ", fr.total() - fr.remaining(), fr.total());
        std::io::stdout().flush().ok();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap_or(0) == 0 {
            break;
        }
        let input = input.trim();

        if input == "q" || input == "quit" {
            println!("Bye!");
            return Ok(());
        }

        if input.is_empty() {
            if let Some(line) = fr.next_line() {
                let line = line.to_string();
                println!("  → {line}");
                if let Err(e) = kafka_send(producer, &[line]).await {
                    eprintln!("  ERROR: {e}");
                }
            }
        } else {
            match input.parse::<usize>() {
                Ok(n) if n > 0 => {
                    let lines: Vec<String> = fr.next_n(n).to_vec();
                    if lines.is_empty() {
                        println!("  EOF");
                        continue;
                    }
                    for l in &lines {
                        println!("  → {l}");
                    }
                    match kafka_send(producer, &lines).await {
                        Ok(_) => println!("  sent {} lines", lines.len()),
                        Err(e) => eprintln!("  ERROR: {e}"),
                    }
                }
                _ => {
                    println!("  unknown command (Enter, N, q)");
                }
            }
        }
    }

    println!("  EOF — all lines sent");
    Ok(())
}

async fn run_file_auto(
    producer: &PartitionClient,
    fr: &mut FileReader,
    rate: f64,
) -> Result<(), String> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs_f64(1.0 / rate));
    let mut sent = 0u64;
    let total = fr.total();
    let start = std::time::Instant::now();

    println!("Sending... (Ctrl+C to stop)");

    while !fr.done() {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let elapsed = start.elapsed();
                println!(
                    "\n  stopped: {sent}/{total} sent in {:.1}s ({:.1} msg/s)",
                    elapsed.as_secs_f64(),
                    sent as f64 / elapsed.as_secs_f64()
                );
                return Ok(());
            }
            _ = interval.tick() => {
                let line = match fr.next_line() {
                    Some(l) => l.to_string(),
                    None => break,
                };
                if let Err(e) = kafka_send(producer, &[line]).await {
                    eprintln!("\n  ERROR: {e}");
                    return Err(e);
                }
                sent += 1;
                if sent % 100 == 0 {
                    let elapsed = start.elapsed();
                    eprint!("\r  {sent}/{total} sent ({:.1} msg/s)", sent as f64 / elapsed.as_secs_f64());
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "\r  done: {sent}/{total} sent in {:.1}s ({:.1} msg/s)",
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}
