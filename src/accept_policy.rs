//! Accept-error policy shared by every listener.
//!
//! An accept loop must never end on an accept error, must not spin when the error is resource
//! exhaustion, and must not write one log line per failed accept. This module owns the
//! classification (`accept_error_backoff`), the pause (`ACCEPT_ERROR_BACKOFF`) and the
//! rate-limited logger (`AcceptErrorLog`) so the health, metrics, dashboard, HTTP, TCP
//! passthrough and TLS listeners all behave the same way.

use std::time::{Duration, Instant};
use tracing::warn;

/// At most one accept-error `warn!` per listener per this interval; the line carries how many
/// errors were suppressed since the previous one.
pub const ACCEPT_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Pause after an accept error that is not about one connection.
///
/// An accept that fails for lack of descriptors or memory (EMFILE, ENFILE, ENOBUFS, ENOMEM)
/// leaves the pending connection in the backlog, so the next `accept` fails at once and an
/// unpaused loop spins a core. 100 ms caps that at ten attempts a second and delays recovery by
/// at most 100 ms once a descriptor frees.
pub const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// How long to wait before the next accept after `e`. `None` means retry immediately.
///
/// Errors about the one connection being accepted (the peer aborted or reset it before accept
/// returned, a protocol error on it, or an interrupted call) retry at once: the next connection
/// is unaffected. Everything else, resource exhaustion included, backs off. No accept error is
/// treated as fatal: the listener keeps its port for as long as the process runs.
pub(crate) fn accept_error_backoff(e: &std::io::Error) -> Option<Duration> {
    use std::io::ErrorKind;
    match e.kind() {
        ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset | ErrorKind::Interrupted => {
            return None
        }
        _ => {}
    }
    #[cfg(unix)]
    if e.raw_os_error() == Some(libc::EPROTO) {
        return None;
    }
    Some(ACCEPT_ERROR_BACKOFF)
}

/// Per-listener rate limiter for accept-error log lines.
#[derive(Debug, Default)]
pub struct AcceptErrorLog {
    last_logged: Option<Instant>,
    suppressed: u64,
}

impl AcceptErrorLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one accept error at `now`. Returns `Some(suppressed)` when this error should be
    /// logged, where `suppressed` is how many errors were swallowed since the previous line, and
    /// `None` when it falls inside `ACCEPT_ERROR_LOG_INTERVAL` of the previous line.
    pub fn record(&mut self, now: Instant) -> Option<u64> {
        if self
            .last_logged
            .is_none_or(|t| now.duration_since(t) >= ACCEPT_ERROR_LOG_INTERVAL)
        {
            self.last_logged = Some(now);
            Some(std::mem::take(&mut self.suppressed))
        } else {
            self.suppressed += 1;
            None
        }
    }

    /// Log `e` for `listener_name` (rate-limited) and apply the backoff it calls for. Call this
    /// from an accept loop's error arm, then continue the loop.
    pub async fn on_accept_error(&mut self, listener_name: &str, e: &std::io::Error) {
        let backoff = accept_error_backoff(e);
        if let Some(suppressed) = self.record(Instant::now()) {
            warn!(
                listener = listener_name,
                error = %e,
                suppressed_since_last_log = suppressed,
                backoff_ms = backoff.map_or(0, |d| d.as_millis() as u64),
                "Accept failed; the listener keeps running"
            );
        }
        if let Some(delay) = backoff {
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Resource exhaustion backs off; an error about one connection retries at once.
    #[test]
    fn accept_errors_back_off_only_when_not_per_connection() {
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            assert_eq!(
                accept_error_backoff(&std::io::Error::from_raw_os_error(errno)),
                Some(ACCEPT_ERROR_BACKOFF),
                "errno {} must back off",
                errno
            );
        }
        for errno in [
            libc::ECONNABORTED,
            libc::ECONNRESET,
            libc::EINTR,
            libc::EPROTO,
        ] {
            assert_eq!(
                accept_error_backoff(&std::io::Error::from_raw_os_error(errno)),
                None,
                "errno {} concerns one connection and must retry at once",
                errno
            );
        }
    }

    /// One line per interval: the first error logs, errors inside the interval are counted, and
    /// the first error after it logs again carrying that count.
    #[test]
    fn accept_error_log_is_rate_limited_to_one_line_per_interval() {
        let mut log = AcceptErrorLog::new();
        let t0 = Instant::now();
        assert_eq!(log.record(t0), Some(0), "the first error must log");
        for i in 1..=5u64 {
            assert_eq!(
                log.record(t0 + Duration::from_secs(i)),
                None,
                "error {} inside the interval must be suppressed",
                i
            );
        }
        assert_eq!(
            log.record(t0 + ACCEPT_ERROR_LOG_INTERVAL - Duration::from_millis(1)),
            None,
            "an error just inside the interval must be suppressed"
        );
        assert_eq!(
            log.record(t0 + ACCEPT_ERROR_LOG_INTERVAL),
            Some(6),
            "the first error at the interval must log with the suppressed count"
        );
        assert_eq!(
            log.record(t0 + ACCEPT_ERROR_LOG_INTERVAL + Duration::from_secs(1)),
            None,
            "the interval restarts from the line just logged"
        );
    }
}
