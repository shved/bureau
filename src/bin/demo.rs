use bureau::{
    client::Client,
    protocol::{Request, Response},
};
use bytes::Bytes;
use clap::Parser;
use parking_lot::RwLock;
use rand::{
    distr::{Distribution, Uniform},
    prelude::IteratorRandom,
    rngs::StdRng,
    Rng, SeedableRng,
};
use ratatui::{
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Direction, Layout, Rect},
    text::Line,
    widgets::{Bar, BarChart, BarGroup, Block, Paragraph},
    DefaultTerminal, Frame,
};
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::result::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::task;

static LATENCY_CHART_LEN: usize = 128;

const HIGH_DEMAND_KEYS_LEN: usize = 300;

#[derive(Parser)]
struct Args {
    #[clap(short, long, default_value = "30")]
    clients: usize,

    #[clap(short, long, default_value = "127.0.0.1:12650")]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let metrics = Arc::new(AtomicMetrics::new());

    color_eyre::install()?;
    let terminal = ratatui::init();
    let app = App::new(&metrics);
    spawn_clients(args.clients, args.address, Arc::clone(&metrics));
    let app_result = app.run(terminal)?;
    ratatui::restore();

    println!(
        "Final stats after running for {} seconds:",
        app_result.run_seconds
    );
    println!("Write Requests: {}", app_result.writes_sum);
    println!("Successful Writes: {}", app_result.writes_suc_sum);
    println!("Read Requests: {}", app_result.reads_sum);
    println!("Successful Reads: {}", app_result.reads_suc_sum);
    println!("SSTables Writen: {}", app_result.sstables_written);
    println!("Data Writen: {}Mb", app_result.data_writen);

    Ok(())
}

struct FrameData {
    reads: u64,
    reads_suc: u64,
    writes: u64,
    writes_suc: u64,
    set_latencies: Vec<u64>,
    get_latencies: Vec<u64>,
}

impl FrameData {
    fn new() -> Self {
        Self {
            reads: u64::default(),
            reads_suc: u64::default(),
            writes: u64::default(),
            writes_suc: u64::default(),
            set_latencies: [0; LATENCY_CHART_LEN].to_vec(),
            get_latencies: [0; LATENCY_CHART_LEN].to_vec(),
        }
    }
}

struct AtomicMetrics {
    read_requests: AtomicU64,
    read_success: AtomicU64,
    write_requests: AtomicU64,
    write_success: AtomicU64,
    set_latencies: [AtomicU64; LATENCY_CHART_LEN],
    get_latencies: [AtomicU64; LATENCY_CHART_LEN],
}

impl AtomicMetrics {
    fn new() -> Self {
        Self {
            read_requests: AtomicU64::new(0),
            read_success: AtomicU64::new(0),
            write_requests: AtomicU64::new(0),
            write_success: AtomicU64::new(0),
            set_latencies: std::array::from_fn(|_| AtomicU64::new(0)),
            get_latencies: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Very sloppy and chill function that flaws guaranties, but since it is stats Im good with it.
    fn reset(&self) -> FrameData {
        let reads = self.read_requests.swap(0, Ordering::Release);
        let writes = self.write_requests.swap(0, Ordering::Release);
        let reads_suc = self.read_success.swap(0, Ordering::Release);
        let writes_suc = self.write_success.swap(0, Ordering::Release);
        let mut set_latencies = [0; LATENCY_CHART_LEN].to_vec();
        let mut get_latencies = [0; LATENCY_CHART_LEN].to_vec();
        for i in 0..LATENCY_CHART_LEN {
            let set = self.set_latencies[i].swap(0, Ordering::Release);
            set_latencies[i] = set;
            let get = self.get_latencies[i].swap(0, Ordering::Release);
            get_latencies[i] = get;
        }

        FrameData {
            reads,
            reads_suc,
            writes,
            writes_suc,
            set_latencies,
            get_latencies,
        }
    }

    fn update_latencies(&self, req: &Request, latency: usize) {
        if latency >= LATENCY_CHART_LEN {
            return;
        }

        match req {
            Request::Set { .. } => {
                self.set_latencies[latency].fetch_add(1, Ordering::Release);
            }
            Request::Get { .. } => {
                self.get_latencies[latency].fetch_add(1, Ordering::Release);
            }
        }
    }
}

struct AppResult {
    run_seconds: usize,
    writes_sum: u64,
    writes_suc_sum: u64,
    reads_sum: u64,
    reads_suc_sum: u64,
    sstables_written: usize,
    data_writen: f64,
}

struct App<'a> {
    metrics: &'a AtomicMetrics,
    run_seconds: usize,
    writes_sum: u64,
    writes_suc_sum: u64,
    reads_sum: u64,
    reads_suc_sum: u64,
}

impl<'a> App<'a> {
    pub fn new(metrics: &'a AtomicMetrics) -> Self {
        Self {
            metrics,
            run_seconds: 0,
            writes_sum: 0,
            writes_suc_sum: 0,
            reads_sum: 0,
            reads_suc_sum: 0,
        }
    }

    fn run(mut self, mut terminal: DefaultTerminal) -> Result<AppResult, Box<dyn Error>> {
        let tick_rate = Duration::from_secs(1);
        let mut frame_data = FrameData::new();
        let mut last_tick = Instant::now();
        loop {
            terminal.draw(|frame| self.draw(frame, &frame_data))?;

            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            if self.handle_exit(timeout)? {
                return Ok(self.app_result());
            }
            if last_tick.elapsed() >= tick_rate {
                frame_data = self.on_tick();
                last_tick = Instant::now();
            }
        }
    }

    fn app_result(&self) -> AppResult {
        let (files, size) = data_stat();

        AppResult {
            run_seconds: self.run_seconds,
            writes_sum: self.writes_sum,
            writes_suc_sum: self.writes_suc_sum,
            reads_sum: self.reads_sum,
            reads_suc_sum: self.reads_suc_sum,
            sstables_written: files,
            data_writen: size,
        }
    }

    fn on_tick(&mut self) -> FrameData {
        let frame_data = self.metrics.reset();
        self.writes_sum += frame_data.writes;
        self.writes_suc_sum += frame_data.writes_suc;
        self.reads_sum += frame_data.reads;
        self.reads_suc_sum += frame_data.reads_suc;
        self.run_seconds += 1;

        frame_data
    }

    fn draw(&self, frame: &mut Frame, frame_data: &FrameData) {
        let layout = layout(frame.area());

        self.render_req_rates(frame, layout[0], frame_data);
        self.render_set_latency_histogram(frame, layout[1], frame_data);
        self.render_get_latency_histogram(frame, layout[2], frame_data);
        self.render_stats(frame, layout[3]);
    }

    fn render_req_rates(&self, frame: &mut Frame, area: Rect, data: &FrameData) {
        let bars = BarGroup::default().bars(&[
            Bar::default().label(Line::from("SET")).value(data.writes),
            Bar::default().label(Line::from("GET")).value(data.reads),
        ]);

        let chart = BarChart::default()
            .block(Block::bordered().title("Requests / Second"))
            .direction(Direction::Horizontal)
            .data(bars)
            .bar_gap(0)
            .bar_width(3);

        frame.render_widget(chart, area);
    }

    fn render_set_latency_histogram(&self, frame: &mut Frame, area: Rect, data: &FrameData) {
        let bars: Vec<Bar> = data
            .set_latencies
            .iter()
            .enumerate()
            .map(|(i, l)| Bar::default().value(*l).label(Line::from(format!("{i}ms"))))
            .collect();

        let chart = BarChart::default()
            .data(BarGroup::default().bars(&bars))
            .block(Block::bordered().title("SET Requests Latency Distribution"))
            .direction(Direction::Vertical)
            .bar_gap(1)
            .bar_width(3);

        frame.render_widget(chart, area);
    }

    fn render_get_latency_histogram(&self, frame: &mut Frame, area: Rect, data: &FrameData) {
        let bars: Vec<Bar> = data
            .get_latencies
            .iter()
            .enumerate()
            .map(|(i, l)| Bar::default().value(*l).label(Line::from(format!("{i}ms"))))
            .collect();

        let chart = BarChart::default()
            .data(BarGroup::default().bars(&bars))
            .block(Block::bordered().title("GET Requests Latency Distribution"))
            .direction(Direction::Vertical)
            .bar_gap(1)
            .bar_width(3);

        frame.render_widget(chart, area);
    }

    fn render_stats(&self, frame: &mut Frame, area: Rect) {
        let (tables_count, total_size) = data_stat();

        let text = format!(
            r#"Other Stats
Seconds Run: {}
Read Requests: {} 
Successful Reads: {} 
Write Requests: {} 
Successful Writes: {} 
Tables written: {} ({}Mb of data)
Press 'q' to stop..."#,
            self.run_seconds,
            self.reads_sum,
            self.reads_suc_sum,
            self.writes_sum,
            self.writes_suc_sum,
            tables_count,
            total_size,
        );

        let paragraph = Paragraph::new(text);
        frame.render_widget(paragraph, area);
    }

    fn handle_exit(&mut self, timeout: Duration) -> Result<bool, Box<dyn Error>> {
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

fn spawn_clients(clients_cnt: usize, addr: String, metrics: Arc<AtomicMetrics>) {
    let keys_set = Arc::new(RwLock::new(HashSet::new()));
    // Slow sliding window of most demand keys so cache have some work to do here.
    let high_demand_keys_window =
        Arc::new(RwLock::new(VecDeque::with_capacity(HIGH_DEMAND_KEYS_LEN)));

    for _ in 0..clients_cnt {
        let addr = addr.clone();
        let keys_set = Arc::clone(&keys_set);
        let high_demand_keys_window = Arc::clone(&high_demand_keys_window);
        spawn_client(
            Arc::clone(&keys_set),
            Arc::clone(&high_demand_keys_window),
            addr,
            Arc::clone(&metrics),
        );
    }
}

fn spawn_client(
    keys_set: Arc<RwLock<HashSet<Bytes>>>,
    high_demand_keys_window: Arc<RwLock<VecDeque<Bytes>>>,
    addr: String,
    metrics: Arc<AtomicMetrics>,
) {
    task::spawn(async move {
        let mut client = match Client::connect(&addr).await {
            Ok(c) => c,
            Err(e) => {
                panic!("Failed to connect: {}", e);
            }
        };

        let key_dist = Uniform::new_inclusive(1, 200).unwrap();
        let val_dist = Uniform::new_inclusive(1, 500).unwrap();
        let mut rng = StdRng::from_os_rng();

        loop {
            let is_write = rng.random_bool(0.7); // Make it write heavy since we test (kind of) LSM.
            let reuse_key = rng.random_bool(0.25); // Quarter of the keys will be reused not fresh generated.
            let request_high_demand_key = rng.random_bool(0.2); // Every fifth key to GET will be from the limited set of high demand keys so that cache is being useful.
            let push_to_hdkw = rng.random_bool(0.005); // Probability that a new key will go into a high demand team.

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

            metrics.update_latencies(&request, elapsed);

            match &request {
                Request::Set { key, .. } => {
                    metrics.write_requests.fetch_add(1, Ordering::Release);
                    match response {
                        Ok(Response::Ok | Response::OkValue { .. }) => {
                            keys_set.write().insert(key.clone());
                            if push_to_hdkw {
                                let mut hdkw_lock = high_demand_keys_window.write();
                                if hdkw_lock.len() == HIGH_DEMAND_KEYS_LEN {
                                    hdkw_lock.pop_back();
                                }
                                hdkw_lock.push_front(key.clone());
                                drop(hdkw_lock);
                            }
                            metrics.write_success.fetch_add(1, Ordering::Release);
                        }
                        Ok(Response::Error { message }) => {
                            panic!("response error: {:?}", message);
                        }
                        Err(e) => panic!("request failed: {}", e),
                    }
                }
                Request::Get { .. } => {
                    metrics.read_requests.fetch_add(1, Ordering::Release);
                    match response {
                        Ok(Response::OkValue { .. }) => {
                            metrics.read_success.fetch_add(1, Ordering::Release);
                        }
                        Ok(Response::Ok) => (),
                        Ok(Response::Error { message }) => {
                            panic!("response error: {:?}", message);
                        }
                        Err(e) => panic!("request failed: {}", e),
                    }
                }
            }
        }
    });
}

fn data_stat() -> (usize, f64) {
    let mut total_size = 0;
    let mut count = 0;

    if let Ok(entries) = std::fs::read_dir("var/lib/bureau") {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total_size += metadata.len();
                count += 1;
            }
        }
    }

    (count, bytes_to_mb(total_size))
}

fn bytes_to_mb(bytes: u64) -> f64 {
    (bytes as f64 / 1_048_576.0 * 100.0).round() / 100.0
}

fn layout(area: Rect) -> std::rc::Rc<[Rect]> {
    // TODO: Wrap it into another rect so that window has some padding with a set of constraints.
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(20), // Req/Sec Rates
            Constraint::Percentage(30), // Set Latency Histogram
            Constraint::Percentage(30), // Get Latency Histogram
            Constraint::Percentage(20), // Legend
        ])
        .split(area)
}
