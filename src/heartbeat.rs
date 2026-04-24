//! Heartbeat infrastructure for `ZeroMQ` connections using PING/PONG commands.
//!
//! This module provides heartbeat capability as defined in ZMTP spec (RFC 37):
//! <https://rfc.zeromq.org/spec/37>
//!
//! ## Implementation
//!
//! Heartbeat is implemented in two parts:
//! 1. **`ActivityTracker`** - Tracks peer state
//! 2. **`HeartbeatTask`** - Background task that sends PINGs periodically

use crate::async_rt;
use crate::async_rt::task::timeout;
use crate::util::PeerIdentity;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct HeartbeatConfig {
    /// Interval to send PING commands
    pub ping_interval: Duration,
    /// Maximum time to wait for activity (traffic or PONG) before considering connection dead
    pub timeout: Duration,
    /// TTL (time-to-live) to include in PING commands, in tenths of seconds
    /// If None, TTL will be 0 (no specific timeout hint)
    pub ttl: Option<u16>,
    /// Maximum number of unanswered PING commands before considering peer dead (PONG storm prevention)
    pub max_unanswered_pings: u32,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(15),
            timeout: Duration::from_secs(60),
            ttl: Some(600),
            max_unanswered_pings: 3,
        }
    }
}

impl HeartbeatConfig {
    pub fn send_ping_timeout(&self) -> Duration {
        self.ping_interval
    }
}

/// Activity tracker for heartbeat monitoring
///
/// This struct tracks:
/// - Last activity time (any traffic)
/// - Unanswered PING count (to prevent PONG storms)
/// - Active timeout duration (from received PING TTL, if any)
///
/// **IMPORTANT**: Socket implementations MUST:
/// 1. Call `record_activity()` when ANY message is received
/// 2. Call `should_send_ping()` to check if PING should be sent
/// 3. If true, create PING and send it, then call `sent_ping()`
/// 4. Call `received_ping(ttl)` when PING is received, `received_pong()` when PONG is received
/// 5. Call `is_dead()` to detect heartbeat timeout and close connection
#[derive(Debug, Clone)]
pub struct ActivityTracker {
    config: HeartbeatConfig,
    /// The active timeout duration from the last received PING command's TTL (in milliseconds).
    peer_timeout_ms: Arc<AtomicU64>,
    /// Number of PING commands sent without receiving PONG (PONG storm prevention)
    unanswered_pings: Arc<AtomicU32>,
    /// Last time a PING was sent (in milliseconds). Used to enforce minimum interval between PINGs.
    last_ping_ms: Arc<AtomicU64>,
    last_activity_ms: Arc<AtomicU64>,
}

impl ActivityTracker {
    pub fn new(config: HeartbeatConfig) -> Self {
        let now = current_time_ms();
        Self {
            config,
            unanswered_pings: Arc::new(AtomicU32::new(0)),
            last_ping_ms: Arc::new(AtomicU64::new(0)),
            peer_timeout_ms: Arc::new(AtomicU64::new(0)),
            last_activity_ms: Arc::new(AtomicU64::new(now)),
        }
    }

    /// Get the active timeout duration
    ///
    /// Returns the minimum of config.timeout and any peer-specified timeout (from received PING TTL).
    pub fn active_timeout(&self) -> Duration {
        let peer_timeout = self.peer_timeout_ms.load(Ordering::Relaxed);
        if peer_timeout > 0 {
            let peer_dur = Duration::from_millis(peer_timeout);
            if peer_dur < self.config.timeout {
                return peer_dur;
            }
        }
        self.config.timeout
    }

    /// Check if the connection should be considered dead due to timeout
    pub fn is_dead(&self) -> bool {
        let now = current_time_ms();
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_activity));

        elapsed > self.active_timeout()
    }

    /// Check if it's time to send a PING command
    ///
    /// Returns true if:
    /// 1. Time since last activity >= `ping_interval`, AND
    /// 2. Time since last PING send >= `ping_interval` (enforces minimum interval), AND
    /// 3. Unanswered PING count < `max_unanswered_pings` (PONG storm prevention)
    pub fn should_send_ping(&self) -> bool {
        let unanswered = self.unanswered_pings.load(Ordering::Relaxed);
        if unanswered >= self.config.max_unanswered_pings {
            return false; // Don't send more PINGs while waiting for replies
        }

        let now = current_time_ms();

        // Enforce minimum interval since last PING was sent
        let last_ping_ms = self.last_ping_ms.load(Ordering::Relaxed);
        if last_ping_ms > 0 {
            let elapsed_since_last_ping = Duration::from_millis(now.saturating_sub(last_ping_ms));
            if elapsed_since_last_ping < self.config.ping_interval {
                return false;
            }
        }

        // Check if time since last activity >= ping_interval
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_activity));
        elapsed >= self.config.ping_interval
    }

    /// Record that activity occurred on the connection.
    pub fn record_activity(&self) {
        let now = current_time_ms();
        self.last_activity_ms.store(now, Ordering::Relaxed);
    }

    /// Call this after successfully sending a PING command
    pub fn sent_ping(&self) {
        self.unanswered_pings.fetch_add(1, Ordering::Relaxed);
        self.last_ping_ms
            .store(current_time_ms(), Ordering::Relaxed);
    }

    /// Call when a PING command is received
    /// If TTL is provided and non-zero, stores the peer-specified timeout duration
    pub fn received_ping(&self, ttl: Option<u16>) {
        self.record_activity();

        if let Some(ttl_tenths_of_secs) = ttl {
            if ttl_tenths_of_secs > 0 {
                // TTL is in tenths of seconds, convert to milliseconds
                let ttl_ms = (ttl_tenths_of_secs as u64) * 100;
                self.peer_timeout_ms.store(ttl_ms, Ordering::Relaxed);
            }
        }
    }

    /// Call this when PONG is received
    pub fn received_pong(&self) {
        // Atomically decrement if > 0, using compare_and_swap to avoid race conditions
        loop {
            let current = self.unanswered_pings.load(Ordering::Relaxed);
            if current == 0 {
                break;
            }

            // Try to decrement atomically
            if self
                .unanswered_pings
                .compare_exchange(current, current - 1, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        self.record_activity();
    }

    /// Get the time remaining before timeout
    ///
    /// Returns None if the connection is already dead.
    #[allow(dead_code)]
    pub fn time_until_timeout(&self) -> Option<Duration> {
        let now = current_time_ms();
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_activity));
        let timeout = self.active_timeout();

        if elapsed > timeout {
            None
        } else {
            Some(timeout.saturating_sub(elapsed))
        }
    }

    /// Get number of unanswered PINGs
    #[allow(dead_code)]
    pub fn unanswered_ping_count(&self) -> u32 {
        self.unanswered_pings.load(Ordering::Relaxed)
    }
}

/// Handle to a running heartbeat task
///
/// When dropped, signals the heartbeat task to shut down.
pub struct HeartbeatHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl HeartbeatHandle {
    /// Request graceful shutdown of the heartbeat task.
    #[expect(unused)]
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            log::debug!("Heartbeat handle shutdown signal sent");
        }
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        // Signal shutdown when dropped (non-blocking)
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            log::debug!("Heartbeat handle dropped, shutdown signal sent");
        }
    }
}

/// Spawns a heartbeat task for a single peer connection
///
/// This task runs independently from the message loop and periodically checks if a PING command should be sent.
///
/// The task:
/// - Wakes up every ~500ms to check if PING should be sent
/// - Calls the `should_send_ping_callback` to check if PING is needed
/// - If callback returns true, calls `send_ping_callback` to send the PING
/// - Detects dead connections when `is_dead()` returns true
/// - Calls the `on_timeout` callback if the connection is dead
/// - Continues until shutdown signal is received
///
/// # Arguments
/// * `activity_tracker` - The `ActivityTracker` for this peer
/// * `peer_id` - The peer identity
/// * `send_ping` - Async callback to send a PING when needed
/// * `on_timeout` - Callback invoked when heartbeat timeout is detected; returns true to break loop, false to continue checking
///
/// # Returns
/// A `HeartbeatHandle` to control the task
pub fn spawn_heartbeat_task<SendFn, TimeoutFn>(
    activity_tracker: ActivityTracker,
    peer_id: PeerIdentity,
    mut send_ping: SendFn,
    on_timeout: TimeoutFn,
) -> HeartbeatHandle
where
    SendFn: FnMut() -> BoxFuture<'static, Result<(), std::io::Error>> + Send + 'static,
    TimeoutFn: Fn() -> bool + Send + Sync + 'static,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let on_timeout = Arc::new(on_timeout);
    let peer_id_clone = peer_id.clone();
    let on_timeout_clone = on_timeout.clone();

    async_rt::task::spawn(async move {
        log::debug!("Heartbeat task started for peer {:?}", peer_id_clone);

        const CHECK_INTERVAL_MS: u64 = 500;

        let mut shutdown_rx = shutdown_rx.fuse();

        loop {
            let sleep_future = async_rt::task::sleep(Duration::from_millis(CHECK_INTERVAL_MS));

            futures::select! {
                _ = shutdown_rx => {
                    log::debug!("Heartbeat ping sender for peer {:?} shutting down", peer_id_clone);
                    break;
                }
                _ = sleep_future.fuse() => {
                    if activity_tracker.is_dead() {
                        log::warn!("Heartbeat timeout detected for peer {:?}", peer_id_clone);
                        if on_timeout_clone() {
                            break;
                        }
                    }

                    if activity_tracker.should_send_ping() {
                        match timeout(activity_tracker.config.send_ping_timeout(), send_ping()).await
                                .map_err(|e| std::io::Error::new(ErrorKind::TimedOut, e.to_string()))
                                .and_then(|x| x) {
                            Ok(()) => {
                                activity_tracker.sent_ping();
                                log::debug!("Sent PING to peer {:?}", peer_id_clone);
                            }
                            Err(e) => {
                                log::debug!("Failed to send PING to peer {:?}: {:?}", peer_id_clone, e);
                                if on_timeout_clone() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    HeartbeatHandle {
        shutdown_tx: Some(shutdown_tx),
    }
}

/// Get the current time in milliseconds since `UNIX_EPOCH`
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_timeout_ms_from_received_ping() {
        let tracker = ActivityTracker::new(HeartbeatConfig::default());

        // Initially no peer timeout
        assert_eq!(tracker.active_timeout(), HeartbeatConfig::default().timeout);

        // After receiving PING with TTL=100 (10 seconds)
        tracker.received_ping(Some(100));
        assert_eq!(tracker.active_timeout(), Duration::from_secs(10));

        // Receiving TTL=0 doesn't change peer timeout
        tracker.received_ping(Some(0));
        assert_eq!(tracker.active_timeout(), Duration::from_secs(10));

        // When peer_timeout > config.timeout, active_timeout returns config.timeout
        // TTL=10000 → 1000 seconds (much larger than config.timeout)
        tracker.received_ping(Some(10000));
        assert_eq!(tracker.active_timeout(), HeartbeatConfig::default().timeout);
    }

    #[test]
    fn test_activity_tracker_creation() {
        let tracker = ActivityTracker::new(HeartbeatConfig::default());
        assert!(!tracker.is_dead());
        // Just created, ping interval not reached yet
        assert!(!tracker.should_send_ping());
    }
}
