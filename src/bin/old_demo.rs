use anyhow::Result;
use bureau::client::Client;
use bureau::protocol::{Request, Response};
use bytes::Bytes;
use clap::Parser;
use num_traits::cast;
use parking_lot::RwLock;
use rand::{distr::Uniform, prelude::*, rngs::StdRng};
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode};
use ratatui::crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::crossterm::ExecutableCommand;
use ratatui::widgets::Paragraph;
use ratatui::Terminal;
use std::collections::{HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{task, time::Instant};

// Latencies of the requests are saved into the array of atomics. The index of the entry
// is effectively a latency in milliseconds here and the value is amount of requests
// finished in the given milliseconds.
static LATENCY_CHART_SIZE: usize = 128;
static SET_LATENCIES: [AtomicU64; LATENCY_CHART_SIZE] = unsafe { std::mem::zeroed() };
static GET_LATENCIES: [AtomicU64; LATENCY_CHART_SIZE] = unsafe { std::mem::zeroed() };

const HIGH_DEMAND_KEYS_CNT: usize = 300;

const BAR_HEIGHT: u64 = 10;

#[derive(Parser)]
struct Args {
    #[clap(short, long, default_value = "30")]
    clients: usize,

    #[clap(short, long, default_value = "127.0.0.1:12650")]
    address: String,
}

struct Metrics {
    read_requests: AtomicU64,
    write_requests: AtomicU64,
    read_success: AtomicU64,
    write_success: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            read_requests: AtomicU64::new(0),
            write_requests: AtomicU64::new(0),
            read_success: AtomicU64::new(0),
            write_success: AtomicU64::new(0),
        }
    }

    fn read_requests(&self) -> u64 {
        self.read_requests.load(Ordering::Acquire)
    }

    fn write_requests(&self) -> u64 {
        self.write_requests.load(Ordering::Acquire)
    }

    fn read_success(&self) -> u64 {
        self.read_success.load(Ordering::Acquire)
    }

    fn write_success(&self) -> u64 {
        self.write_success.load(Ordering::Acquire)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let metrics = Arc::new(Metrics::new());
    // Save the keys set to request them again.
    let keys_set = Arc::new(RwLock::new(HashSet::new()));
    // A window of most demand keys so cache have some work to do here.
    let high_demand_keys_window =
        Arc::new(RwLock::new(VecDeque::with_capacity(HIGH_DEMAND_KEYS_CNT)));

    for _ in 0..args.clients {
        let addr = args.address.clone();
        let metrics = Arc::clone(&metrics);
        let keys_set = Arc::clone(&keys_set);
        let high_demand_keys_window = Arc::clone(&high_demand_keys_window);

        task::spawn(async move {
            let mut client = match Client::connect(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect: {}", e);
                    return;
                }
            };

            let key_dist = Uniform::new_inclusive(1, 200).unwrap();
            let val_dist = Uniform::new_inclusive(1, 500).unwrap();
            let mut rng = StdRng::from_os_rng();

            loop {
                let is_write = rng.random_bool(0.7); // Make it write heavy since we test LSM.
                let reuse_key = rng.random_bool(0.25); // Quarter of the keys will be reused not fresh generated.
                let request_high_demand_key = rng.random_bool(0.2); // Every fifth key to GET will be from the limited set of high demand keys so that cache is being useful.
                let push_to_hdkw = rng.random_bool(0.005);

                let request = if is_write {
                    let key = if reuse_key {
                        let keys = keys_set.read();
                        if keys.is_empty() {
                            continue;
                        }
                        keys.iter().choose(&mut rng).cloned().unwrap()
                    } else {
                        Bytes::from(
                            (0..key_dist.sample(&mut rng))
                                .map(|_| rng.random())
                                .collect::<Vec<u8>>(),
                        )
                    };

                    let value = Bytes::from(
                        (0..val_dist.sample(&mut rng))
                            .map(|_| rng.random())
                            .collect::<Vec<u8>>(),
                    );

                    Request::Set {
                        key: key.clone(),
                        value,
                    }
                } else if request_high_demand_key {
                    let keys = high_demand_keys_window.read();
                    if keys.is_empty() {
                        continue;
                    }
                    let random_key = keys.iter().choose(&mut rng).cloned();
                    drop(keys);
                    match random_key {
                        Some(key) => Request::Get { key },
                        None => continue,
                    }
                } else {
                    let keys = keys_set.read();
                    if keys.is_empty() {
                        continue;
                    }
                    let random_key = keys.iter().choose(&mut rng).cloned();
                    drop(keys);
                    match random_key {
                        Some(key) => Request::Get { key },
                        None => continue,
                    }
                };

                let start_time = Instant::now();
                let response = client.send(request.clone()).await;
                let elapsed = start_time.elapsed().as_millis() as usize;

                update_latencies(&request, elapsed);

                match &request {
                    Request::Set { key, .. } => {
                        metrics.write_requests.fetch_add(1, Ordering::Release);
                        if response.is_ok() {
                            keys_set.write().insert(key.clone());
                            if push_to_hdkw {
                                let mut hdkw_lock = high_demand_keys_window.write();
                                if hdkw_lock.len() == HIGH_DEMAND_KEYS_CNT {
                                    hdkw_lock.pop_back();
                                }
                                hdkw_lock.push_front(key.clone());
                                drop(hdkw_lock);
                            }
                            metrics.write_success.fetch_add(1, Ordering::Release);
                        }
                    }
                    Request::Get { .. } => {
                        metrics.read_requests.fetch_add(1, Ordering::Release);
                        if let Ok(Response::OkValue { .. }) = response {
                            metrics.read_success.fetch_add(1, Ordering::Release);
                        }
                    }
                }
            }
        });
    }

    start_tui(metrics).await?;

    Ok(())
}

fn update_latencies(req: &Request, latency: usize) {
    if latency >= LATENCY_CHART_SIZE {
        return;
    }

    match req {
        Request::Set { .. } => {
            SET_LATENCIES[latency].fetch_add(1, Ordering::Release);
        }
        Request::Get { .. } => {
            GET_LATENCIES[latency].fetch_add(1, Ordering::Release);
        }
    }
}

async fn start_tui(metrics: Arc<Metrics>) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let start_time = Instant::now();

    loop {
        terminal.draw(|frame| {
            let area = frame.area();
            let chunks = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints([
                    ratatui::layout::Constraint::Percentage(40), // Histogram occupies 40%
                    ratatui::layout::Constraint::Percentage(40), // Histogram occupies 40%
                    ratatui::layout::Constraint::Percentage(20), // Text summary below
                ])
                .split(area);

            let mut max_set_latency: u64 = 0;
            let mut max_get_latency: u64 = 0;
            let mut set_hist_data = Vec::new();
            let mut get_hist_data = Vec::new();
            for i in 0..69 {
                let set_latency = SET_LATENCIES[i].load(Ordering::Acquire);
                let get_latency = GET_LATENCIES[i].load(Ordering::Acquire);
                if set_latency > max_set_latency {
                    max_set_latency = set_latency;
                }
                if get_latency > max_get_latency {
                    max_get_latency = get_latency;
                }
                set_hist_data.push((format!("{}", i), set_latency));
                get_hist_data.push((format!("{}", i), get_latency));
            }

            let set_hist_labels: Vec<String> = set_hist_data
                .iter()
                .map(|(label, _)| label.clone())
                .collect();

            let set_hist_display: Vec<(&str, u64)> = set_hist_labels
                .iter()
                .zip(
                    set_hist_data
                        .iter()
                        .map(|(_, value)| bar_height(max_set_latency, *value)),
                )
                .map(|(label, value)| (label.as_str(), value))
                .collect();

            let set_barchart = ratatui::widgets::BarChart::default()
                .block(ratatui::widgets::Block::default().title("SET Latency Histogram"))
                .bar_width(2)
                .data(&set_hist_display)
                .max(10);

            let get_hist_labels: Vec<String> = get_hist_data
                .iter()
                .map(|(label, _)| label.clone())
                .collect();

            let get_hist_display: Vec<(&str, u64)> = get_hist_labels
                .iter()
                .zip(
                    get_hist_data
                        .iter()
                        .map(|(_, value)| bar_height(max_get_latency, *value)),
                )
                .map(|(label, value)| (label.as_str(), value))
                .collect();

            let get_barchart = ratatui::widgets::BarChart::default()
                .block(ratatui::widgets::Block::default().title("GET Latency Histogram"))
                .bar_width(2)
                .data(&get_hist_display)
                .max(BAR_HEIGHT);

            let text = format!(
                r#"Database Load Test
Read Requests: {} 
Write Requests: {} 
Successful Reads: {} 
Successful Writes: {} 
Press 'q' to stop..."#,
                metrics.read_requests(),
                metrics.write_requests(),
                metrics.read_success(),
                metrics.write_success()
            );
            let paragraph = Paragraph::new(text);

            frame.render_widget(set_barchart, chunks[0]);
            frame.render_widget(get_barchart, chunks[1]);
            frame.render_widget(paragraph, chunks[2]);
        })?;

        // Check for 'q' press to exit
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    let total_elapsed = start_time.elapsed();

    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;

    println!("Final Stats:");
    println!("Read Requests: {}", metrics.read_requests());
    println!("Write Requests: {}", metrics.write_requests());
    println!("Successful Reads: {}", metrics.read_success());
    println!("Successful Writes: {}", metrics.write_success());

    let total_sets = metrics.write_success();
    let total_gets = metrics.read_success();
    let avg_set_time = if total_sets > 0 {
        (0..LATENCY_CHART_SIZE)
            .map(|i| SET_LATENCIES[i].load(Ordering::Relaxed) * i as u64)
            .sum::<u64>()
            / total_sets
    } else {
        0
    };
    let avg_get_time = if total_gets > 0 {
        (0..LATENCY_CHART_SIZE)
            .map(|i| GET_LATENCIES[i].load(Ordering::Relaxed) * i as u64)
            .sum::<u64>()
            / total_gets
    } else {
        0
    };

    println!("Average Set Request Time: {} ms", avg_set_time);
    println!("Average Get Request Time: {} ms", avg_get_time);
    println!("Total Elapsed Time: {:.2?}", total_elapsed);

    Ok(())
}

fn bar_height(max: u64, cur: u64) -> u64 {
    if max == 0 {
        return 0;
    }

    cast(f64::from(cur as u32) / f64::from(max as u32) * f64::from(BAR_HEIGHT as u32)).unwrap()
}
