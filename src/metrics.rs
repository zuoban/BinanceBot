use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const LATENCY_BUCKETS_SECONDS: [f64; 11] = [
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.25, 1.0,
];

pub(crate) struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_SECONDS.len()],
    count: AtomicU64,
    sum_nanos: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; LATENCY_BUCKETS_SECONDS.len()],
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        if let Some(index) = LATENCY_BUCKETS_SECONDS
            .iter()
            .position(|bound| seconds <= *bound)
        {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0;
        for (bound, bucket) in LATENCY_BUCKETS_SECONDS.iter().zip(&self.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let sum = self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

pub(crate) struct Counter(AtomicU64);

impl Counter {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub(crate) fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

pub(crate) static SQLITE_CONNECTION_LOCK_WAIT: Histogram = Histogram::new();
pub(crate) static SQLITE_BLOCKING_QUEUE_WAIT: Histogram = Histogram::new();
pub(crate) static MARKET_WS_RECONNECTS: Counter = Counter::new();
pub(crate) static USER_DATA_WS_RECONNECTS: Counter = Counter::new();
pub(crate) static ORDER_RECONCILIATION_FAILURES: Counter = Counter::new();
pub(crate) static POSITION_SYNC_FAILURES: Counter = Counter::new();
pub(crate) static ACCOUNT_SYNC_FAILURES: Counter = Counter::new();
pub(crate) static TICKER_BROADCAST_LAGGED: Counter = Counter::new();
pub(crate) static WS_CLIENT_LAGGED: Counter = Counter::new();
pub(crate) static HTTP_REQUESTS_2XX: Counter = Counter::new();
pub(crate) static HTTP_REQUESTS_4XX: Counter = Counter::new();
pub(crate) static HTTP_REQUESTS_5XX: Counter = Counter::new();

pub(crate) fn record_http_status(status: u16) {
    match status {
        200..=299 => HTTP_REQUESTS_2XX.inc(),
        400..=499 => HTTP_REQUESTS_4XX.inc(),
        500..=599 => HTTP_REQUESTS_5XX.inc(),
        _ => {}
    }
}

fn render_counter(out: &mut String, name: &str, help: &str, series: &[(&str, &Counter)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    for (label, counter) in series {
        if label.is_empty() {
            let _ = writeln!(out, "{name} {}", counter.get());
        } else {
            let _ = writeln!(out, "{name}{{{label}}} {}", counter.get());
        }
    }
}

pub(crate) fn render() -> String {
    let mut out = String::new();
    SQLITE_CONNECTION_LOCK_WAIT.render(
        &mut out,
        "binance_bot_sqlite_connection_lock_wait_seconds",
        "Time spent waiting for the shared SQLite connection mutex.",
    );
    SQLITE_BLOCKING_QUEUE_WAIT.render(
        &mut out,
        "binance_bot_sqlite_blocking_queue_wait_seconds",
        "Time between submitting a SQLite job and a blocking worker starting it.",
    );
    render_counter(
        &mut out,
        "binance_bot_websocket_reconnects_total",
        "Binance WebSocket sessions that ended and are being re-established.",
        &[
            ("stream=\"market\"", &MARKET_WS_RECONNECTS),
            ("stream=\"user_data\"", &USER_DATA_WS_RECONNECTS),
        ],
    );
    render_counter(
        &mut out,
        "binance_bot_sync_failures_total",
        "Failed exchange synchronization attempts.",
        &[
            ("kind=\"open_orders\"", &ORDER_RECONCILIATION_FAILURES),
            ("kind=\"position\"", &POSITION_SYNC_FAILURES),
            ("kind=\"account\"", &ACCOUNT_SYNC_FAILURES),
        ],
    );
    render_counter(
        &mut out,
        "binance_bot_broadcast_lagged_total",
        "Broadcast receivers that fell behind and skipped messages.",
        &[
            ("channel=\"ticker\"", &TICKER_BROADCAST_LAGGED),
            ("channel=\"websocket_client\"", &WS_CLIENT_LAGGED),
        ],
    );
    render_counter(
        &mut out,
        "binance_bot_http_requests_total",
        "HTTP API responses by status class.",
        &[
            ("status=\"2xx\"", &HTTP_REQUESTS_2XX),
            ("status=\"4xx\"", &HTTP_REQUESTS_4XX),
            ("status=\"5xx\"", &HTTP_REQUESTS_5XX),
        ],
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative_and_overflow_only_counts_in_inf() {
        let histogram = Histogram::new();
        histogram.observe(Duration::from_micros(40));
        histogram.observe(Duration::from_micros(900));
        histogram.observe(Duration::from_secs(5));
        let mut out = String::new();
        histogram.render(&mut out, "test_wait_seconds", "help");
        assert!(out.contains("test_wait_seconds_bucket{le=\"0.00005\"} 1\n"));
        assert!(out.contains("test_wait_seconds_bucket{le=\"0.001\"} 2\n"));
        assert!(out.contains("test_wait_seconds_bucket{le=\"1\"} 2\n"));
        assert!(out.contains("test_wait_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(out.contains("test_wait_seconds_count 3\n"));
    }

    #[test]
    fn http_status_classes_map_to_their_own_counters() {
        let before = (
            HTTP_REQUESTS_2XX.get(),
            HTTP_REQUESTS_4XX.get(),
            HTTP_REQUESTS_5XX.get(),
        );
        record_http_status(204);
        record_http_status(401);
        record_http_status(503);
        record_http_status(101);
        assert!(HTTP_REQUESTS_2XX.get() > before.0);
        assert!(HTTP_REQUESTS_4XX.get() > before.1);
        assert!(HTTP_REQUESTS_5XX.get() > before.2);
    }
}
