use anyhow::Result;
use bureau::client::Client;
use bureau::protocol::{Request, Response};
use bytes::Bytes;
use clap::Parser;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use rand::{distr::Uniform, prelude::*, rngs::StdRng};
use ratatui::backend::CrosstermBackend;
use ratatui::widgets::Paragraph;
use ratatui::Terminal;
use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{task, time::Instant};

// Durations of the requests are saved into the array of atomics. The index of the entry
// is effectively a duration in milliseconds here and the value is amount of requests
// finished in the given milliseconds.
static DURATIONS_SIZE: usize = 1024;
static SET_DURATIONS: [AtomicU64; DURATIONS_SIZE] = unsafe { std::mem::zeroed() };
static GET_DURATIONS: [AtomicU64; DURATIONS_SIZE] = unsafe { std::mem::zeroed() };

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
    let keys_set = Arc::new(parking_lot::RwLock::new(HashSet::new()));

    for _ in 0..args.clients {
        let addr = args.address.clone();
        let metrics = Arc::clone(&metrics);
        let keys_set = Arc::clone(&keys_set);

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
                let is_write = rng.random_bool(0.9); // Make it write heavy since we test LSM.
                let reuse_key = rng.random_bool(0.5); // Half of the keys will be reused not fresh generated.

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

                update_duration(&request, elapsed);

                match &request {
                    Request::Set { key, .. } => {
                        metrics.write_requests.fetch_add(1, Ordering::Release);
                        if response.is_ok() {
                            keys_set.write().insert(key.clone());
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

fn update_duration(req: &Request, dur: usize) {
    if dur >= DURATIONS_SIZE {
        return;
    }

    match req {
        Request::Set { .. } => {
            SET_DURATIONS[dur].fetch_add(1, Ordering::Relaxed);
        }
        Request::Get { .. } => {
            GET_DURATIONS[dur].fetch_add(1, Ordering::Relaxed);
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
            frame.render_widget(paragraph, area);
        })?;

        // Check for 'q' press to exit
        if event::poll(Duration::from_millis(500))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    let total_elapsed = start_time.elapsed(); // Compute total elapsed time

    // Clean up terminal state
    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;

    // Print final stats
    println!("Final Stats:");
    println!("Read Requests: {}", metrics.read_requests());
    println!("Write Requests: {}", metrics.write_requests());
    println!("Successful Reads: {}", metrics.read_success());
    println!("Successful Writes: {}", metrics.write_success());

    // Compute average elapsed time for set/get requests
    let total_sets = metrics.write_success();
    let total_gets = metrics.read_success();
    let avg_set_time = if total_sets > 0 {
        (0..DURATIONS_SIZE)
            .map(|i| SET_DURATIONS[i].load(Ordering::Relaxed) * i as u64)
            .sum::<u64>()
            / total_sets
    } else {
        0
    };
    let avg_get_time = if total_gets > 0 {
        (0..DURATIONS_SIZE)
            .map(|i| GET_DURATIONS[i].load(Ordering::Relaxed) * i as u64)
            .sum::<u64>()
            / total_gets
    } else {
        0
    };

    println!("Average Set Request Time: {} ms", avg_set_time);
    println!("Average Get Request Time: {} ms", avg_get_time);
    println!("Total Elapsed Time: {:.2?}", total_elapsed); // Print total elapsed time

    Ok(())
}
