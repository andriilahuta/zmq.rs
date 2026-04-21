//! Heartbeat infrastructure for `ZeroMQ` connections using PING/PONG commands.
//!
//! This module provides heartbeat capability as defined in ZMTP spec (RFC 37):
//! <https://rfc.zeromq.org/spec/37>
//!
//! ## Implementation
//!
//! Heartbeat is implemented in two parts:
//! 1. **ActivityTracker** - Tracks peer state
//! 2. **HeartbeatTask** - Separate background task that sends PINGs periodically

use crate::async_rt::task::timeout;
use crate::util::PeerIdentity;
use crate::async_rt;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::FutureExt;

/// Configuration for heartbeat behavior
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
            // Send heartbeat every 15 seconds
            ping_interval: Duration::from_secs(15),
            // Wait 2x the ping interval for response
            timeout: Duration::from_secs(60),
            // TTL = 2x the ping interval = 600 tenths of seconds = 60 seconds
            ttl: Some(600),
            // Limit unanswered PINGs to 3 to prevent PONG storms
            max_unanswered_pings: 3,
        }
    }
}

impl HeartbeatConfig {
    pub fn send_ping_timeout(&self) -> Duration {
        self.ping_interval
    }
}

/// Simple activity tracker for heartbeat monitoring
///
/// This struct tracks:
/// - Last activity time (any traffic)
/// - Unanswered PING count (to prevent PONG storms)
/// - TTL-based timeout from received PING
///
/// **IMPORTANT**: Socket implementations MUST:
/// 1. Call `record_activity()` when ANY message is received
/// 2. Call `decrement_unanswered_ping()` when PONG is received & context matches
/// 3. Call `should_send_ping()` to check if PING should be sent
/// 4. If true, create PING and send it, then call `sent_ping()`
/// 5. Call `is_dead()` to detect heartbeat timeout and close connection
///
/// See src/pub.rs for example integration.
#[derive(Debug, Clone)]
pub struct ActivityTracker {
    config: HeartbeatConfig,
    /// Number of PING commands sent without receiving PONG (PONG storm prevention)
    unanswered_pings: Arc<AtomicU32>,
    last_activity_ms: Arc<AtomicU64>,
    /// TTL received from PING command (in milliseconds). Separate timeout from activity timeout.
    ttl_deadline_ms: Arc<AtomicU64>,
    /// Last time a PING was sent (in milliseconds). Used to enforce minimum interval between PINGs.
    last_ping_ms: Arc<AtomicU64>,
}

impl ActivityTracker {
    pub fn new(config: HeartbeatConfig) -> Self {
        let now = current_time_ms();
        Self {
            last_activity_ms: Arc::new(AtomicU64::new(now)),
            config,
            unanswered_pings: Arc::new(AtomicU32::new(0)),
            ttl_deadline_ms: Arc::new(AtomicU64::new(0)),
            last_ping_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record that activity occurred on the connection
    pub fn record_activity(&self) {
        self.last_activity_ms.store(current_time_ms(), Ordering::Relaxed);
    }

    /// Check if the connection should be considered dead due to timeout
    /// Checks both activity timeout and TTL-based timeout
    pub fn is_dead(&self) -> bool {
        // Check activity timeout
        let now = current_time_ms();
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_activity));

        if elapsed > self.config.timeout {
            return true;
        }

        // Check TTL-based timeout (RFC 37: if PING received with non-zero TTL, disconnect if no traffic within that TTL)
        let ttl_deadline = self.ttl_deadline_ms.load(Ordering::Relaxed);
        if ttl_deadline > 0 && now > ttl_deadline {
            return true;
        }

        false
    }

    /// Check if it's time to send a PING command
    ///
    /// Returns true if:
    /// 1. Time since last activity >= ping_interval, AND
    /// 2. Time since last PING send >= ping_interval (enforces minimum interval), AND
    /// 3. Unanswered PING count < max_unanswered_pings (PONG storm prevention)
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

    /// Call this after successfully sending a PING command
    pub fn sent_ping(&self) {
        self.unanswered_pings.fetch_add(1, Ordering::Relaxed);
        self.last_ping_ms.store(current_time_ms(), Ordering::Relaxed);
    }

    /// Call this when PONG is received
    /// Any traffic counts as sign of life; PONG is acknowledged
    ///
    /// Uses atomic operations to safely decrement unanswered_pings counter
    pub fn received_pong(&self) {
        // Atomically decrement if > 0, using compare_and_swap to avoid race conditions
        loop {
            let current = self.unanswered_pings.load(Ordering::Relaxed);
            if current == 0 {
                break; // Already at 0, don't go negative
            }
            // Try to decrement atomically
            match self.unanswered_pings.compare_exchange(
                current,
                current - 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break, // Successfully decremented
                Err(_) => continue, // Retry due to concurrent modification
            }
        }
        self.record_activity();
    }

    /// Call when a PING command is received
    /// If TTL is provided and non-zero, sets a TTL-based deadline
    pub fn received_ping(&self, ttl: Option<u16>) {
        self.record_activity();

        if let Some(ttl_tenths_of_secs) = ttl {
            if ttl_tenths_of_secs > 0 {
                // TTL is in tenths of seconds, convert to milliseconds
                let ttl_ms = (ttl_tenths_of_secs as u64) * 100;
                let deadline = current_time_ms() + ttl_ms;
                self.ttl_deadline_ms.store(deadline, Ordering::Relaxed);
            }
        }
    }

    /// Get the time remaining before timeout
    ///
    /// Returns None if the connection is already dead.
    #[allow(dead_code)]
    pub fn time_until_timeout(&self) -> Option<Duration> {
        let now = current_time_ms();
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_activity));

        if elapsed > self.config.timeout {
            None
        } else {
            Some(self.config.timeout.saturating_sub(elapsed))
        }
    }

    /// Get time until next PING should be sent
    #[allow(dead_code)]
    pub fn time_until_ping(&self) -> Duration {
        let now = current_time_ms();
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_activity));

        self.config.ping_interval.saturating_sub(elapsed)
    }

    /// Get the TTL value for PING commands
    #[allow(dead_code)]
    pub fn ping_ttl(&self) -> u16 {
        self.config.ttl.unwrap_or(0)
    }

    /// Get number of unanswered PINGs
    #[allow(dead_code)]
    pub fn unanswered_ping_count(&self) -> u32 {
        self.unanswered_pings.load(Ordering::Relaxed)
    }
}

/// Handle to a running heartbeat task
///
/// When dropped, signals the heartbeat task to shut down. Call `shutdown()` to
/// cleanly wait for the task to exit, or `into_inner()` to get the join handle.
pub struct HeartbeatHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl HeartbeatHandle {
    /// Request graceful shutdown of the heartbeat task and wait for it to exit
    pub async fn shutdown(mut self) {
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
/// - Calls the `on_timeout` callback if the connection is dead; breaks loop if it returns true
/// - Continues until shutdown signal is received
///
/// # Arguments
/// * `heartbeat` - The ActivityTracker for this peer
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
        let mut shutdown_rx = shutdown_rx.fuse();
        const CHECK_INTERVAL_MS: u64 = 500; // Check every 500ms if PING should be sent

        loop {
            let sleep_future = async_rt::task::sleep(Duration::from_millis(CHECK_INTERVAL_MS));

            futures::select! {
                _ = shutdown_rx => {
                    log::debug!("Heartbeat ping sender for peer {:?} shutting down", peer_id_clone);
                    break;
                }
                _ = sleep_future.fuse() => {
                    // Check if connection is dead
                    if activity_tracker.is_dead() {
                        log::warn!("Heartbeat timeout detected for peer {:?}", peer_id_clone);
                        // Call on_timeout callback; if it returns true, break the loop
                        if on_timeout_clone() {
                            break;
                        }
                        // Otherwise continue checking
                    }

                    // Check if we should send a PING
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
                                // Send callback error to on_timeout; break if it returns true
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
    fn test_heartbeat_config_default() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.ping_interval, Duration::from_secs(60));
        assert_eq!(config.timeout, Duration::from_secs(120));
        assert_eq!(config.ttl, Some(1200));
    }

    #[test]
    fn test_activity_tracker_creation() {
        let tracker = ActivityTracker::new(HeartbeatConfig::default());
        assert!(!tracker.is_dead());
        // Just created, ping interval not reached yet
        assert!(!tracker.should_send_ping());
    }

    #[test]
    fn test_current_time_ms_is_reasonable() {
        let time = current_time_ms();
        // Test that current time is roughly in the 2020s (ms since epoch)
        // 1577836800000 = 2020-01-01
        // 1893456000000 = 2030-01-01
        assert!(time > 1577836800000, "time should be after 2020");
        assert!(time < 1893456000000, "time should be before 2030");
    }
}
