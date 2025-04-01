use ratatui::{
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Direction, Layout, Rect},
    text::Line,
    widgets::{Bar, BarChart, BarGroup, Block, Paragraph},
    DefaultTerminal, Frame,
};
use std::error::Error;
use std::result::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

// Latencies of the requests are saved into the array of atomics. The index of the entry
// is effectively a latency in milliseconds here and the value is amount of requests
// finished in the given ms bucket.
static LATENCY_CHART_SIZE: usize = 128;
static SET_LATENCIES: [AtomicU64; LATENCY_CHART_SIZE] = unsafe { std::mem::zeroed() };
static GET_LATENCIES: [AtomicU64; LATENCY_CHART_SIZE] = unsafe { std::mem::zeroed() };

const HIGH_DEMAND_KEYS_CNT: usize = 300;

const BAR_HEIGHT: u64 = 10;

fn main() -> Result<(), Box<dyn Error>> {
    color_eyre::install()?;
    let terminal = ratatui::init();
    let metrics = Metrics::new();
    let app = App::new(&metrics);
    let app_result = app.run(terminal);
    ratatui::restore();
    app_result
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

    fn reset(&self) {
        self.read_requests.store(0, Ordering::Release);
        self.write_requests.store(0, Ordering::Release);
        self.read_success.store(0, Ordering::Release);
        self.write_success.store(0, Ordering::Release);
    }
}

struct App<'a> {
    metrics: &'a Metrics,
    run_seconds: usize,
}

impl<'a> App<'a> {
    pub fn new(metrics: &'a Metrics) -> Self {
        Self {
            metrics,
            run_seconds: 0,
        }
    }

    fn run(mut self, mut terminal: DefaultTerminal) -> Result<(), Box<dyn Error>> {
        let tick_rate = Duration::from_secs(1);
        let mut last_tick = Instant::now();
        loop {
            terminal.draw(|frame| self.draw(frame))?;

            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            if self.handle_exit(timeout)? {
                return Ok(());
            }
            if last_tick.elapsed() >= tick_rate {
                self.on_tick();
                last_tick = Instant::now();
            }
        }
    }

    fn on_tick(&mut self) {
        self.metrics.reset();
        self.run_seconds += 1;
    }

    fn draw(&self, frame: &mut Frame) {
        let layout = layout(frame.area());

        self.render_req_rates(frame, layout[0]);
        self.render_set_latency_histogram(frame, layout[1]);
        self.render_get_latency_histogram(frame, layout[2]);
        self.render_legend(frame, layout[3]);

        // let mut max_set_latency: u64 = 0;
        // let mut max_get_latency: u64 = 0;
        // let mut set_hist_data = Vec::new();
        // let mut get_hist_data = Vec::new();

        //         for i in 0..69 {
        //             let set_latency = SET_LATENCIES[i].load(Ordering::Acquire);
        //             let get_latency = GET_LATENCIES[i].load(Ordering::Acquire);
        //             if set_latency > max_set_latency {
        //                 max_set_latency = set_latency;
        //             }
        //             if get_latency > max_get_latency {
        //                 max_get_latency = get_latency;
        //             }
        //             set_hist_data.push((format!("{}ms", i), set_latency));
        //             get_hist_data.push((format!("{}ms", i), get_latency));
        //         }

        //         let set_hist_labels: Vec<String> = set_hist_data
        //             .iter()
        //             .map(|(label, _)| label.clone())
        //             .collect();

        //         let set_hist_display: Vec<(&str, u64)> = set_hist_labels
        //             .iter()
        //             .zip(
        //                 set_hist_data
        //                     .iter()
        //                     .map(|(_, value)| bar_height(max_set_latency, *value)),
        //             )
        //             .map(|(label, value)| (label.as_str(), value))
        //             .collect();

        //         let set_barchart = ratatui::widgets::BarChart::default()
        //             .block(ratatui::widgets::Block::default().title("SET Latency Histogram"))
        //             .bar_width(3)
        //             .data(&set_hist_display)
        //             .max(10);

        //         let get_hist_labels: Vec<String> = get_hist_data
        //             .iter()
        //             .map(|(label, _)| label.clone())
        //             .collect();

        //         let get_hist_display: Vec<(&str, u64)> = get_hist_labels
        //             .iter()
        //             .zip(
        //                 get_hist_data
        //                     .iter()
        //                     .map(|(_, value)| bar_height(max_get_latency, *value)),
        //             )
        //             .map(|(label, value)| (label.as_str(), value))
        //             .collect();

        //         let get_barchart = ratatui::widgets::BarChart::default()
        //             .block(ratatui::widgets::Block::default().title("GET Latency Histogram"))
        //             .bar_width(2)
        //             .data(&get_hist_display)
        //             .max(BAR_HEIGHT);
    }

    fn render_req_rates(&self, frame: &mut Frame, area: Rect) {
        let bars = BarGroup::default().bars(&[
            Bar::default()
                .label(Line::from("SET"))
                .value(self.run_seconds as u64 * 2),
            Bar::default()
                .label(Line::from("GET"))
                .value(self.run_seconds as u64),
        ]);

        let chart = BarChart::default()
            .block(Block::bordered().title("Requests / Second"))
            .direction(Direction::Horizontal)
            .data(bars)
            .bar_gap(0)
            .bar_width(3);

        frame.render_widget(chart, area);
    }

    fn render_set_latency_histogram(&self, frame: &mut Frame, area: Rect) {
        let bars: Vec<Bar> = (0..69)
            .map(|i| {
                Bar::default()
                    .value(self.run_seconds.saturating_sub(i) as u64)
                    .label(Line::from(format!("{i}")))
            })
            .collect();

        let chart = BarChart::default()
            .data(BarGroup::default().bars(&bars))
            .block(Block::bordered().title("SET Requests Latency Distribution"))
            .direction(Direction::Vertical)
            .bar_gap(1)
            .bar_width(3);

        frame.render_widget(chart, area);
    }

    fn render_get_latency_histogram(&self, frame: &mut Frame, area: Rect) {
        let bars: Vec<Bar> = (0..69)
            .map(|i| {
                Bar::default()
                    .value(self.run_seconds.saturating_sub(i) as u64)
                    .label(Line::from(format!("{i}")))
            })
            .collect();

        let chart = BarChart::default()
            .data(BarGroup::default().bars(&bars))
            .block(Block::bordered().title("GET Requests Latency Distribution"))
            .direction(Direction::Vertical)
            .bar_gap(1)
            .bar_width(3);

        frame.render_widget(chart, area);
    }

    fn render_legend(&self, frame: &mut Frame, area: Rect) {
        let paragraph = Paragraph::new("Press 'q' to stop...");
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
