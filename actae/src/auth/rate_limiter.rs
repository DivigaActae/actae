// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Rate Limiting System for Authentication
//!
//! Provides comprehensive rate limiting for authentication endpoints:
//! - IP-based rate limiting (10 attempts/minute default)
//! - User-based rate limiting (prevent credential stuffing)
//! - Sliding window algorithm for accurate rate limiting
//! - Configurable limits and time windows
//! - Audit logging for security monitoring
//! - Automatic cleanup of expired entries

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum attempts per time window
    pub max_attempts: u32,
    /// Time window duration in seconds
    pub window_duration_secs: u64,
    /// Block duration in seconds after limit exceeded
    pub block_duration_secs: u64,
    /// Enable progressive delays (exponential backoff)
    pub enable_progressive_delays: bool,
    /// Maximum delay in seconds
    pub max_delay_secs: u64,
    /// Cleanup interval in seconds
    pub cleanup_interval_secs: u64,
    /// Maximum entries to keep in memory
    pub max_entries: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_attempts: 100,
            window_duration_secs: 60, // 1 minute
            block_duration_secs: 300, // 5 minutes
            enable_progressive_delays: true,
            max_delay_secs: 60,         // 1 minute max delay
            cleanup_interval_secs: 300, // 5 minutes cleanup
            max_entries: 100000,        // 100k entries max
        }
    }
}

/// Authentication attempt record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthAttempt {
    pub timestamp: DateTime<Utc>,
    pub ip_address: Option<IpAddr>,
    pub user_id: Option<String>,
    pub success: bool,
    pub failure_reason: Option<String>,
    pub user_agent: Option<String>,
    pub request_id: String,
}

impl AuthAttempt {
    pub fn new(ip_address: Option<IpAddr>, user_id: Option<String>, success: bool) -> Self {
        Self {
            timestamp: Utc::now(),
            ip_address,
            user_id,
            success,
            failure_reason: None,
            user_agent: None,
            request_id: Uuid::new_v4().to_string(),
        }
    }

    pub fn with_failure_reason(mut self, reason: String) -> Self {
        self.failure_reason = Some(reason);
        self
    }

    pub fn with_user_agent(mut self, user_agent: String) -> Self {
        self.user_agent = Some(user_agent);
        self
    }
}

/// Rate limit bucket using sliding window algorithm
#[derive(Debug, Clone)]
struct RateLimitBucket {
    /// Queue of attempt timestamps
    attempts: VecDeque<DateTime<Utc>>,
    /// Total failed attempts count
    failed_attempts: u32,
    /// Last attempt timestamp
    last_attempt: DateTime<Utc>,
    /// Current block end time (if blocked)
    blocked_until: Option<DateTime<Utc>>,
    /// Progressive delay multiplier
    delay_multiplier: u32,
}

impl RateLimitBucket {
    fn new() -> Self {
        Self {
            attempts: VecDeque::new(),
            failed_attempts: 0,
            last_attempt: Utc::now(),
            blocked_until: None,
            delay_multiplier: 1,
        }
    }

    /// Add an attempt and check if rate limit is exceeded
    fn add_attempt(&mut self, config: &RateLimitConfig, success: bool) -> RateLimitResult {
        let now = Utc::now();
        self.last_attempt = now;

        // Clean old attempts outside the window
        let window_start = now - Duration::seconds(config.window_duration_secs as i64);
        while let Some(&front_time) = self.attempts.front() {
            if front_time < window_start {
                self.attempts.pop_front();
            } else {
                break;
            }
        }

        // Add current attempt
        self.attempts.push_back(now);

        // Update failure tracking
        if !success {
            self.failed_attempts += 1;
            if config.enable_progressive_delays {
                self.delay_multiplier = (self.delay_multiplier * 2).min(16); // Cap at 16x
            }
        } else {
            // Reset on successful auth
            self.failed_attempts = 0;
            self.delay_multiplier = 1;
            self.blocked_until = None;
        }

        // Check if currently blocked
        if let Some(blocked_until) = self.blocked_until {
            if now < blocked_until {
                return RateLimitResult::Blocked {
                    retry_after: blocked_until,
                    attempts_in_window: self.attempts.len() as u32,
                    total_failed_attempts: self.failed_attempts,
                };
            } else {
                // Block expired
                self.blocked_until = None;
            }
        }

        // Check rate limit
        if self.attempts.len() as u32 > config.max_attempts {
            // Rate limit exceeded - impose block
            self.blocked_until = Some(now + Duration::seconds(config.block_duration_secs as i64));

            RateLimitResult::Blocked {
                retry_after: self.blocked_until.unwrap(),
                attempts_in_window: self.attempts.len() as u32,
                total_failed_attempts: self.failed_attempts,
            }
        } else {
            // Calculate progressive delay if enabled
            let delay_secs = if config.enable_progressive_delays && !success {
                (self.delay_multiplier as u64).min(config.max_delay_secs)
            } else {
                0
            };

            RateLimitResult::Allowed {
                attempts_remaining: config.max_attempts - self.attempts.len() as u32,
                window_reset: window_start + Duration::seconds(config.window_duration_secs as i64),
                delay_secs,
                total_failed_attempts: self.failed_attempts,
            }
        }
    }

    /// Check if bucket is expired and can be cleaned up
    fn is_expired(&self, config: &RateLimitConfig) -> bool {
        let now = Utc::now();
        let expiry_threshold = Duration::seconds(config.window_duration_secs as i64 * 2);
        now - self.last_attempt > expiry_threshold
    }
}

/// Rate limiting result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RateLimitResult {
    Allowed {
        attempts_remaining: u32,
        window_reset: DateTime<Utc>,
        delay_secs: u64,
        total_failed_attempts: u32,
    },
    Blocked {
        retry_after: DateTime<Utc>,
        attempts_in_window: u32,
        total_failed_attempts: u32,
    },
}

impl RateLimitResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, RateLimitResult::Allowed { .. })
    }

    pub fn is_blocked(&self) -> bool {
        matches!(self, RateLimitResult::Blocked { .. })
    }

    pub fn get_delay_secs(&self) -> u64 {
        match self {
            RateLimitResult::Allowed { delay_secs, .. } => *delay_secs,
            RateLimitResult::Blocked { .. } => 0,
        }
    }

    pub fn to_json(&self) -> Result<Value> {
        sonic_rs::to_value(self).context("Failed to serialize rate limit result")
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiterStats {
    pub total_ip_buckets: usize,
    pub total_user_buckets: usize,
    pub total_requests_allowed: u64,
    pub total_requests_blocked: u64,
    pub blocked_ips: usize,
    pub blocked_users: usize,
    pub average_requests_per_minute: f64,
    pub last_cleanup: DateTime<Utc>,
}

/// High-performance rate limiter for authentication
pub struct AuthRateLimiter {
    config: RateLimitConfig,

    /// IP-based rate limiting buckets
    ip_buckets: Arc<DashMap<IpAddr, RateLimitBucket>>,

    /// User-based rate limiting buckets
    user_buckets: Arc<DashMap<String, RateLimitBucket>>,

    /// Global statistics
    stats: Arc<RwLock<RateLimiterStats>>,

    /// Cleanup task handle
    cleanup_task: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal
    shutdown_sender: Arc<tokio::sync::broadcast::Sender<()>>,
}

impl AuthRateLimiter {
    /// Create new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        let (shutdown_sender, _) = tokio::sync::broadcast::channel(1);

        Self {
            config,
            ip_buckets: Arc::new(DashMap::new()),
            user_buckets: Arc::new(DashMap::new()),
            stats: Arc::new(RwLock::new(RateLimiterStats {
                total_ip_buckets: 0,
                total_user_buckets: 0,
                total_requests_allowed: 0,
                total_requests_blocked: 0,
                blocked_ips: 0,
                blocked_users: 0,
                average_requests_per_minute: 0.0,
                last_cleanup: Utc::now(),
            })),
            cleanup_task: None,
            shutdown_sender: Arc::new(shutdown_sender),
        }
    }

    /// Start the rate limiter with background cleanup
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting authentication rate limiter");

        // Start cleanup task
        self.start_cleanup_task().await;

        info!("Authentication rate limiter started successfully");
        Ok(())
    }

    /// Check rate limit for authentication attempt
    pub async fn check_auth_attempt(
        &self,
        ip_address: Option<IpAddr>,
        user_id: Option<String>,
        success: bool,
        user_agent: Option<String>,
    ) -> Result<RateLimitResult> {
        let attempt = AuthAttempt::new(ip_address, user_id.clone(), success)
            .with_user_agent(user_agent.unwrap_or_default());

        // Check IP-based rate limit
        let ip_result = if let Some(ip) = ip_address {
            Some(self.check_ip_rate_limit(ip, success).await?)
        } else {
            None
        };

        // Check user-based rate limit
        let user_result = if let Some(ref user) = user_id {
            Some(self.check_user_rate_limit(user, success).await?)
        } else {
            None
        };

        // Apply the most restrictive result
        let final_result = match (ip_result, user_result) {
            (Some(ip_res), Some(user_res)) => {
                if ip_res.is_blocked() || user_res.is_blocked() {
                    if ip_res.is_blocked() {
                        ip_res
                    } else {
                        user_res
                    }
                } else {
                    // Both allowed - return the one with higher delay
                    if ip_res.get_delay_secs() >= user_res.get_delay_secs() {
                        ip_res
                    } else {
                        user_res
                    }
                }
            }
            (Some(result), None) | (None, Some(result)) => result,
            (None, None) => {
                // No rate limiting applied
                RateLimitResult::Allowed {
                    attempts_remaining: self.config.max_attempts,
                    window_reset: Utc::now()
                        + Duration::seconds(self.config.window_duration_secs as i64),
                    delay_secs: 0,
                    total_failed_attempts: 0,
                }
            }
        };

        // Update statistics
        self.update_stats(&final_result).await;

        // Audit log the attempt
        self.audit_log_attempt(&attempt, &final_result).await;

        Ok(final_result)
    }

    /// Check IP-based rate limit
    async fn check_ip_rate_limit(&self, ip: IpAddr, success: bool) -> Result<RateLimitResult> {
        let mut bucket = self
            .ip_buckets
            .entry(ip)
            .or_insert_with(RateLimitBucket::new);

        let result = bucket.add_attempt(&self.config, success);

        debug!("IP rate limit check for {}: {:?}", ip, result);
        Ok(result)
    }

    /// Check user-based rate limit
    async fn check_user_rate_limit(&self, user_id: &str, success: bool) -> Result<RateLimitResult> {
        let mut bucket = self
            .user_buckets
            .entry(user_id.to_string())
            .or_insert_with(RateLimitBucket::new);

        let result = bucket.add_attempt(&self.config, success);

        debug!("User rate limit check for {}: {:?}", user_id, result);
        Ok(result)
    }

    /// Update global statistics
    async fn update_stats(&self, result: &RateLimitResult) {
        let mut stats = self.stats.write().await;

        match result {
            RateLimitResult::Allowed { .. } => {
                stats.total_requests_allowed += 1;
            }
            RateLimitResult::Blocked { .. } => {
                stats.total_requests_blocked += 1;
            }
        }

        // Update bucket counts
        stats.total_ip_buckets = self.ip_buckets.len();
        stats.total_user_buckets = self.user_buckets.len();

        // Count currently blocked entries
        stats.blocked_ips = self
            .ip_buckets
            .iter()
            .filter(|entry| entry.blocked_until.is_some_and(|until| until > Utc::now()))
            .count();

        stats.blocked_users = self
            .user_buckets
            .iter()
            .filter(|entry| entry.blocked_until.is_some_and(|until| until > Utc::now()))
            .count();
    }

    /// Get rate limiter statistics
    pub async fn get_stats(&self) -> RateLimiterStats {
        self.stats.read().await.clone()
    }

    /// Get blocked IPs
    pub async fn get_blocked_ips(&self) -> Vec<IpAddr> {
        let now = Utc::now();
        self.ip_buckets
            .iter()
            .filter_map(|entry| {
                if entry.blocked_until.is_some_and(|until| until > now) {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get blocked users
    pub async fn get_blocked_users(&self) -> Vec<String> {
        let now = Utc::now();
        self.user_buckets
            .iter()
            .filter_map(|entry| {
                if entry.blocked_until.is_some_and(|until| until > now) {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Manually unblock an IP address
    pub async fn unblock_ip(&self, ip: IpAddr) -> Result<bool> {
        if let Some(mut bucket) = self.ip_buckets.get_mut(&ip) {
            bucket.blocked_until = None;
            bucket.delay_multiplier = 1;
            bucket.attempts.clear();
            bucket.failed_attempts = 0;
            info!("Manually unblocked IP: {}", ip);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Manually unblock a user
    pub async fn unblock_user(&self, user_id: &str) -> Result<bool> {
        if let Some(mut bucket) = self.user_buckets.get_mut(user_id) {
            bucket.blocked_until = None;
            bucket.delay_multiplier = 1;
            bucket.attempts.clear();
            bucket.failed_attempts = 0;
            info!("Manually unblocked user: {}", user_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Clear all rate limit data
    pub async fn clear_all(&self) -> Result<()> {
        self.ip_buckets.clear();
        self.user_buckets.clear();

        let mut stats = self.stats.write().await;
        *stats = RateLimiterStats {
            total_ip_buckets: 0,
            total_user_buckets: 0,
            total_requests_allowed: 0,
            total_requests_blocked: 0,
            blocked_ips: 0,
            blocked_users: 0,
            average_requests_per_minute: 0.0,
            last_cleanup: Utc::now(),
        };

        info!("Cleared all rate limit data");
        Ok(())
    }

    /// Start background cleanup task
    async fn start_cleanup_task(&mut self) {
        let ip_buckets = Arc::clone(&self.ip_buckets);
        let user_buckets = Arc::clone(&self.user_buckets);
        let stats = Arc::clone(&self.stats);
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_sender.subscribe();

        let task = tokio::spawn(async move {
            let mut cleanup_interval =
                interval(std::time::Duration::from_secs(config.cleanup_interval_secs));

            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        Self::cleanup_expired_buckets(&ip_buckets, &user_buckets, &stats, &config).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Rate limiter cleanup task shutting down");
                        break;
                    }
                }
            }
        });

        self.cleanup_task = Some(task);
    }

    /// Cleanup expired rate limit buckets
    async fn cleanup_expired_buckets(
        ip_buckets: &Arc<DashMap<IpAddr, RateLimitBucket>>,
        user_buckets: &Arc<DashMap<String, RateLimitBucket>>,
        stats: &Arc<RwLock<RateLimiterStats>>,
        config: &RateLimitConfig,
    ) {
        let start_time = Instant::now();
        let mut ip_removed = 0;
        let mut user_removed = 0;

        // Cleanup IP buckets
        ip_buckets.retain(|_, bucket| {
            if bucket.is_expired(config) {
                ip_removed += 1;
                false
            } else {
                true
            }
        });

        // Cleanup user buckets
        user_buckets.retain(|_, bucket| {
            if bucket.is_expired(config) {
                user_removed += 1;
                false
            } else {
                true
            }
        });

        // Enforce maximum entries limit
        while ip_buckets.len() > config.max_entries / 2 {
            if let Some(entry) = ip_buckets.iter().next() {
                let key = *entry.key();
                drop(entry);
                ip_buckets.remove(&key);
                ip_removed += 1;
            } else {
                break;
            }
        }

        while user_buckets.len() > config.max_entries / 2 {
            if let Some(entry) = user_buckets.iter().next() {
                let key = entry.key().clone();
                drop(entry);
                user_buckets.remove(&key);
                user_removed += 1;
            } else {
                break;
            }
        }

        // Update cleanup stats
        {
            let mut stats = stats.write().await;
            stats.last_cleanup = Utc::now();
        }

        let cleanup_duration = start_time.elapsed();
        debug!(
            "Rate limiter cleanup completed: removed {} IP buckets, {} user buckets in {:?}",
            ip_removed, user_removed, cleanup_duration
        );
    }

    /// Audit log for authentication attempts
    async fn audit_log_attempt(&self, attempt: &AuthAttempt, result: &RateLimitResult) {
        match result {
            RateLimitResult::Allowed {
                delay_secs,
                total_failed_attempts,
                ..
            } => {
                if *delay_secs > 0 {
                    warn!(
                        "Auth attempt with delay: ip={:?}, user={:?}, success={}, delay={}s, failed_attempts={}",
                        attempt.ip_address,
                        attempt.user_id,
                        attempt.success,
                        delay_secs,
                        total_failed_attempts
                    );
                } else {
                    debug!(
                        "Auth attempt allowed: ip={:?}, user={:?}, success={}",
                        attempt.ip_address, attempt.user_id, attempt.success
                    );
                }
            }
            RateLimitResult::Blocked {
                retry_after,
                total_failed_attempts,
                ..
            } => {
                error!(
                    "Auth attempt blocked: ip={:?}, user={:?}, retry_after={}, failed_attempts={}, reason=rate_limit_exceeded",
                    attempt.ip_address, attempt.user_id, retry_after, total_failed_attempts
                );
            }
        }
    }

    /// Shutdown rate limiter
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down authentication rate limiter");

        // Send shutdown signal
        let _ = self.shutdown_sender.send(());

        // Wait for cleanup task to complete
        if let Some(task) = self.cleanup_task.take() {
            let _ = task.await;
        }

        info!("Authentication rate limiter shutdown complete");
        Ok(())
    }
}

/// Create a global rate limiter instance
pub fn create_auth_rate_limiter(config: RateLimitConfig) -> AuthRateLimiter {
    AuthRateLimiter::new(config)
}

/// Convenience function for quick rate limit check
pub async fn quick_rate_limit_check(
    rate_limiter: &AuthRateLimiter,
    ip: IpAddr,
    success: bool,
) -> Result<bool> {
    let result = rate_limiter
        .check_auth_attempt(Some(ip), None, success, None)
        .await?;

    Ok(result.is_allowed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn config(
        max_attempts: u32,
        window: u64,
        block: u64,
        progressive: bool,
        max_delay: u64,
    ) -> RateLimitConfig {
        RateLimitConfig {
            max_attempts,
            window_duration_secs: window,
            block_duration_secs: block,
            enable_progressive_delays: progressive,
            max_delay_secs: max_delay,
            cleanup_interval_secs: 300,
            max_entries: 100,
        }
    }

    // ── RateLimitBucket sliding-window unit tests ───────────────────

    #[test]
    fn bucket_allows_up_to_max_attempts_then_blocks() {
        let cfg = config(3, 60, 60, false, 60);
        let mut bucket = RateLimitBucket::new();
        for i in 0..3 {
            let result = bucket.add_attempt(&cfg, true);
            assert!(result.is_allowed(), "attempt {i} must be allowed");
            assert_eq!(result.get_delay_secs(), 0);
            assert!(bucket.blocked_until.is_none());
        }
        let blocked = bucket.add_attempt(&cfg, true);
        assert!(blocked.is_blocked());
        assert!(bucket.blocked_until.is_some());
        assert_eq!(blocked.get_delay_secs(), 0);
    }

    #[test]
    fn bucket_window_prunes_old_attempts() {
        let cfg = config(2, 1, 1, false, 60);
        let mut bucket = RateLimitBucket::new();
        bucket.add_attempt(&cfg, true);
        bucket.add_attempt(&cfg, true);
        assert!(bucket.add_attempt(&cfg, true).is_blocked());
        // Past both the 1s window and the 1s block: old attempts are pruned
        // and the fresh attempt is allowed again.
        std::thread::sleep(std::time::Duration::from_millis(2300));
        assert!(bucket.add_attempt(&cfg, true).is_allowed());
    }

    #[test]
    fn bucket_progressive_delay_doubles_then_caps_at_16() {
        let cfg = config(100, 60, 60, true, 60);
        let mut bucket = RateLimitBucket::new();
        assert_eq!(bucket.add_attempt(&cfg, false).get_delay_secs(), 2);
        assert_eq!(bucket.add_attempt(&cfg, false).get_delay_secs(), 4);
        assert_eq!(bucket.add_attempt(&cfg, false).get_delay_secs(), 8);
        assert_eq!(bucket.add_attempt(&cfg, false).get_delay_secs(), 16);
        for _ in 0..6 {
            bucket.add_attempt(&cfg, false);
        }
        assert_eq!(bucket.add_attempt(&cfg, false).get_delay_secs(), 16);
    }

    #[test]
    fn bucket_delay_respects_max_delay_secs() {
        let cfg = config(100, 60, 60, true, 5);
        let mut bucket = RateLimitBucket::new();
        for _ in 0..6 {
            bucket.add_attempt(&cfg, false);
        }
        let delay = bucket.add_attempt(&cfg, false).get_delay_secs();
        assert!(delay <= 5, "delay {delay} must be capped by max_delay_secs");
    }

    #[test]
    fn bucket_success_resets_failure_state() {
        let cfg = config(100, 60, 60, true, 60);
        let mut bucket = RateLimitBucket::new();
        bucket.add_attempt(&cfg, false);
        bucket.add_attempt(&cfg, false);
        assert_eq!(bucket.failed_attempts, 2);
        assert!(bucket.delay_multiplier > 1);
        let result = bucket.add_attempt(&cfg, true);
        assert!(result.is_allowed());
        assert_eq!(result.get_delay_secs(), 0);
        assert_eq!(bucket.failed_attempts, 0);
        assert_eq!(bucket.delay_multiplier, 1);
        assert!(bucket.blocked_until.is_none());
    }

    #[test]
    fn bucket_fresh_bucket_is_not_expired() {
        let cfg = config(10, 60, 60, false, 60);
        let bucket = RateLimitBucket::new();
        assert!(!bucket.is_expired(&cfg));
    }

    #[test]
    fn bucket_known_block_until_is_reported() {
        let cfg = config(1, 60, 60, false, 60);
        let mut bucket = RateLimitBucket::new();
        bucket.add_attempt(&cfg, true);
        let blocked = bucket.add_attempt(&cfg, true);
        let RateLimitResult::Blocked {
            attempts_in_window,
            total_failed_attempts,
            ..
        } = blocked
        else {
            panic!("second attempt must block with max_attempts=1");
        };
        assert!(attempts_in_window >= 2);
        assert_eq!(total_failed_attempts, 0);
    }

    #[test]
    fn rate_limit_result_to_json_shape() {
        let result = RateLimitResult::Allowed {
            attempts_remaining: 7,
            window_reset: Utc::now(),
            delay_secs: 2,
            total_failed_attempts: 1,
        };
        let json = result.to_json().unwrap();
        // serde externally-tags enum variants.
        assert_eq!(json["Allowed"]["attempts_remaining"], 7);
        assert_eq!(json["Allowed"]["delay_secs"], 2);
        let blocked = RateLimitResult::Blocked {
            retry_after: Utc::now(),
            attempts_in_window: 9,
            total_failed_attempts: 3,
        };
        assert!(blocked.is_blocked());
        assert_eq!(
            blocked.to_json().unwrap()["Blocked"]["attempts_in_window"],
            9
        );
    }

    #[tokio::test]
    async fn test_ip_rate_limiting() {
        let config = RateLimitConfig {
            max_attempts: 3,
            window_duration_secs: 60,
            block_duration_secs: 300,
            enable_progressive_delays: false,
            ..Default::default()
        };

        let rate_limiter = AuthRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));

        // First 3 attempts should be allowed
        for i in 1..=3 {
            let result = rate_limiter
                .check_auth_attempt(Some(ip), None, false, None)
                .await
                .unwrap();

            assert!(result.is_allowed(), "Attempt {i} should be allowed");
        }

        // 4th attempt should be blocked
        let result = rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();

        assert!(result.is_blocked(), "4th attempt should be blocked");
    }

    #[tokio::test]
    async fn test_progressive_delays() {
        let config = RateLimitConfig {
            max_attempts: 10,
            window_duration_secs: 60,
            enable_progressive_delays: true,
            max_delay_secs: 60,
            ..Default::default()
        };

        let rate_limiter = AuthRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 101));

        // First failure - should have some delay
        let result = rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();

        if let RateLimitResult::Allowed { delay_secs, .. } = result {
            assert!(delay_secs > 0, "First failure should have delay");
        }

        // Second failure - should have increased delay
        let result = rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();

        if let RateLimitResult::Allowed { delay_secs, .. } = result {
            assert!(
                delay_secs >= 2,
                "Second failure should have increased delay"
            );
        }
    }

    #[tokio::test]
    async fn test_successful_auth_resets_delay() {
        let config = RateLimitConfig {
            max_attempts: 10,
            window_duration_secs: 60,
            enable_progressive_delays: true,
            ..Default::default()
        };

        let rate_limiter = AuthRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 102));

        // Failed attempt
        rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();

        // Successful attempt should reset
        let result = rate_limiter
            .check_auth_attempt(Some(ip), None, true, None)
            .await
            .unwrap();

        if let RateLimitResult::Allowed { delay_secs, .. } = result {
            assert_eq!(delay_secs, 0, "Successful auth should reset delay");
        }
    }

    #[tokio::test]
    async fn test_manual_unblock() {
        let config = RateLimitConfig {
            max_attempts: 1,
            window_duration_secs: 60,
            block_duration_secs: 300,
            ..Default::default()
        };

        let rate_limiter = AuthRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 103));

        // Exceed rate limit
        rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();

        rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();

        // Should be blocked
        let result = rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();
        assert!(result.is_blocked());

        // Unblock manually
        rate_limiter.unblock_ip(ip).await.unwrap();

        // Should be allowed now
        let result = rate_limiter
            .check_auth_attempt(Some(ip), None, false, None)
            .await
            .unwrap();
        assert!(result.is_allowed());
    }
}
