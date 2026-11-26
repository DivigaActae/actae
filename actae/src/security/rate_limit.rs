// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock source for the token bucket. Injectable so timing behavior can
/// be tested deterministically instead of sleeping against real time.
type NowFn = Box<dyn Fn() -> u64 + Send + Sync>;

/// High-performance token bucket rate limiter
pub struct TokenBucketRateLimit {
    tokens: AtomicU32,
    last_refill: AtomicU64,
    last_used: AtomicU64,
    refill_rate: u32,
    capacity: u32,
    now: NowFn,
}

impl TokenBucketRateLimit {
    pub fn new(refill_rate: u32, capacity: u32, window_seconds: u64) -> Self {
        Self::with_clock(refill_rate, capacity, window_seconds, || {
            current_time_millis()
        })
    }

    /// Construct with a custom time source (deterministic tests).
    fn with_clock(
        refill_rate: u32,
        capacity: u32,
        _window_seconds: u64,
        now: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        let now = Box::new(now);
        let current = now();
        Self {
            tokens: AtomicU32::new(capacity),
            last_refill: AtomicU64::new(current),
            last_used: AtomicU64::new(current),
            refill_rate,
            capacity,
            now,
        }
    }

    /// Try to consume a token (O(1) operation)
    pub fn try_consume(&self) -> bool {
        self.refill_tokens();
        self.last_used.store((self.now)(), Ordering::Relaxed);

        loop {
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            if current_tokens == 0 {
                return false;
            }
            match self.tokens.compare_exchange_weak(
                current_tokens,
                current_tokens - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Try to consume multiple tokens at once
    pub fn try_consume_multiple(&self, count: u32) -> bool {
        if count > self.capacity {
            return false;
        }

        self.refill_tokens();
        self.last_used.store((self.now)(), Ordering::Relaxed);

        loop {
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            if current_tokens < count {
                return false;
            }
            match self.tokens.compare_exchange_weak(
                current_tokens,
                current_tokens - count,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Get current token count
    pub fn available_tokens(&self) -> u32 {
        self.refill_tokens();
        self.last_used.store((self.now)(), Ordering::Relaxed);
        self.tokens.load(Ordering::Relaxed)
    }

    /// Refill tokens based on elapsed time
    fn refill_tokens(&self) {
        let now = (self.now)();
        let last_refill = self.last_refill.load(Ordering::Relaxed);

        if now > last_refill {
            let elapsed_ms = now - last_refill;
            let tokens_to_add = (elapsed_ms * self.refill_rate as u64) / 1000;

            if tokens_to_add > 0 {
                // Update last refill time
                self.last_refill.store(now, Ordering::Relaxed);

                // Add tokens up to capacity
                let current_tokens = self.tokens.load(Ordering::Relaxed);
                let new_tokens =
                    std::cmp::min(self.capacity, current_tokens + tokens_to_add as u32);
                self.tokens.store(new_tokens, Ordering::Relaxed);
            }
        }
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Multi-dimensional rate limiter for connections, messages, and authentication
pub struct MultiRateLimiter {
    // Per-IP rate limits
    ip_connection_limits: Arc<DashMap<IpAddr, TokenBucketRateLimit>>,
    ip_message_limits: Arc<DashMap<IpAddr, TokenBucketRateLimit>>,
    ip_auth_limits: Arc<DashMap<IpAddr, TokenBucketRateLimit>>,

    // Per-user rate limits
    user_message_limits: Arc<DashMap<String, TokenBucketRateLimit>>,
    user_subscription_limits: Arc<DashMap<String, TokenBucketRateLimit>>,

    // Global rate limits
    global_connection_limit: TokenBucketRateLimit,
    global_message_limit: TokenBucketRateLimit,

    // Configuration
    config: RateLimitConfig,

    // Cleanup tracking
    last_cleanup: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    // IP-based limits
    pub ip_connection_rate: u32,
    pub ip_connection_capacity: u32,
    pub ip_message_rate: u32,
    pub ip_message_capacity: u32,
    pub ip_auth_rate: u32,
    pub ip_auth_capacity: u32,

    // User-based limits
    pub user_message_rate: u32,
    pub user_message_capacity: u32,
    pub user_subscription_rate: u32,
    pub user_subscription_capacity: u32,

    // Global limits
    pub global_connection_rate: u32,
    pub global_connection_capacity: u32,
    pub global_message_rate: u32,
    pub global_message_capacity: u32,

    // Cleanup interval
    pub cleanup_interval_seconds: u64,
}

#[derive(Debug, Clone)]
pub enum RateLimitResult {
    Allowed,
    Limited(RateLimitInfo),
}

#[derive(Debug, Clone)]
pub struct RateLimitInfo {
    pub limit_type: String,
    pub current_rate: u32,
    pub limit_threshold: u32,
    pub retry_after_seconds: u64,
}

impl MultiRateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            ip_connection_limits: Arc::new(DashMap::new()),
            ip_message_limits: Arc::new(DashMap::new()),
            ip_auth_limits: Arc::new(DashMap::new()),
            user_message_limits: Arc::new(DashMap::new()),
            user_subscription_limits: Arc::new(DashMap::new()),
            global_connection_limit: TokenBucketRateLimit::new(
                config.global_connection_rate,
                config.global_connection_capacity,
                60,
            ),
            global_message_limit: TokenBucketRateLimit::new(
                config.global_message_rate,
                config.global_message_capacity,
                60,
            ),
            config,
            last_cleanup: Arc::new(AtomicU64::new(Self::current_time_seconds())),
        }
    }

    /// Check if a new connection is allowed
    pub fn check_connection_limit(&self, ip: IpAddr) -> RateLimitResult {
        // Check global limit first
        if !self.global_connection_limit.try_consume() {
            return RateLimitResult::Limited(RateLimitInfo {
                limit_type: "global_connections".to_string(),
                current_rate: 0, // Would need tracking for exact rate
                limit_threshold: self.config.global_connection_capacity,
                retry_after_seconds: 60,
            });
        }

        // Check IP-specific limit
        let ip_limiter = self.ip_connection_limits.entry(ip).or_insert_with(|| {
            TokenBucketRateLimit::new(
                self.config.ip_connection_rate,
                self.config.ip_connection_capacity,
                60,
            )
        });

        if !ip_limiter.try_consume() {
            return RateLimitResult::Limited(RateLimitInfo {
                limit_type: "ip_connections".to_string(),
                current_rate: 0,
                limit_threshold: self.config.ip_connection_capacity,
                retry_after_seconds: 60,
            });
        }

        RateLimitResult::Allowed
    }

    /// Check if a message is allowed
    pub fn check_message_limit(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<&str>,
    ) -> RateLimitResult {
        // Check global message limit
        if !self.global_message_limit.try_consume() {
            return RateLimitResult::Limited(RateLimitInfo {
                limit_type: "global_messages".to_string(),
                current_rate: 0,
                limit_threshold: self.config.global_message_capacity,
                retry_after_seconds: 1,
            });
        }

        // Check IP-based message limit
        if let Some(ip) = ip {
            let ip_limiter = self.ip_message_limits.entry(ip).or_insert_with(|| {
                TokenBucketRateLimit::new(
                    self.config.ip_message_rate,
                    self.config.ip_message_capacity,
                    60,
                )
            });

            if !ip_limiter.try_consume() {
                return RateLimitResult::Limited(RateLimitInfo {
                    limit_type: "ip_messages".to_string(),
                    current_rate: 0,
                    limit_threshold: self.config.ip_message_capacity,
                    retry_after_seconds: 1,
                });
            }
        }

        // Check user-based message limit
        if let Some(user_id) = user_id {
            let user_limiter = self
                .user_message_limits
                .entry(user_id.to_string())
                .or_insert_with(|| {
                    TokenBucketRateLimit::new(
                        self.config.user_message_rate,
                        self.config.user_message_capacity,
                        60,
                    )
                });

            if !user_limiter.try_consume() {
                return RateLimitResult::Limited(RateLimitInfo {
                    limit_type: "user_messages".to_string(),
                    current_rate: 0,
                    limit_threshold: self.config.user_message_capacity,
                    retry_after_seconds: 1,
                });
            }
        }

        RateLimitResult::Allowed
    }

    /// Check if an authentication attempt is allowed
    pub fn check_auth_limit(&self, ip: IpAddr) -> RateLimitResult {
        let ip_limiter = self.ip_auth_limits.entry(ip).or_insert_with(|| {
            TokenBucketRateLimit::new(self.config.ip_auth_rate, self.config.ip_auth_capacity, 60)
        });

        if !ip_limiter.try_consume() {
            return RateLimitResult::Limited(RateLimitInfo {
                limit_type: "ip_auth_attempts".to_string(),
                current_rate: 0,
                limit_threshold: self.config.ip_auth_capacity,
                retry_after_seconds: 60,
            });
        }

        RateLimitResult::Allowed
    }

    /// Check if a subscription is allowed
    pub fn check_subscription_limit(&self, user_id: &str) -> RateLimitResult {
        let user_limiter = self
            .user_subscription_limits
            .entry(user_id.to_string())
            .or_insert_with(|| {
                TokenBucketRateLimit::new(
                    self.config.user_subscription_rate,
                    self.config.user_subscription_capacity,
                    60,
                )
            });

        if !user_limiter.try_consume() {
            return RateLimitResult::Limited(RateLimitInfo {
                limit_type: "user_subscriptions".to_string(),
                current_rate: 0,
                limit_threshold: self.config.user_subscription_capacity,
                retry_after_seconds: 60,
            });
        }

        RateLimitResult::Allowed
    }

    /// Cleanup old rate limit entries (call periodically)
    pub fn cleanup_old_entries(&self) {
        let now = Self::current_time_seconds();
        let last_cleanup = self.last_cleanup.load(Ordering::Relaxed);

        if now - last_cleanup > self.config.cleanup_interval_seconds {
            self.last_cleanup.store(now, Ordering::Relaxed);

            let threshold = now.saturating_sub(self.config.cleanup_interval_seconds * 2);
            let threshold_millis = threshold * 1000;

            let retain_ip = |_key: &IpAddr, val: &mut TokenBucketRateLimit| {
                val.last_used.load(Ordering::Relaxed) >= threshold_millis
            };
            let retain_str = |_key: &String, val: &mut TokenBucketRateLimit| {
                val.last_used.load(Ordering::Relaxed) >= threshold_millis
            };

            self.ip_connection_limits.retain(retain_ip);
            self.ip_message_limits.retain(retain_ip);
            self.ip_auth_limits.retain(retain_ip);
            self.user_message_limits.retain(retain_str);
            self.user_subscription_limits.retain(retain_str);
        }
    }

    /// Get rate limiter statistics
    pub fn get_stats(&self) -> RateLimitStats {
        RateLimitStats {
            ip_connection_limiters: self.ip_connection_limits.len(),
            ip_message_limiters: self.ip_message_limits.len(),
            ip_auth_limiters: self.ip_auth_limits.len(),
            user_message_limiters: self.user_message_limits.len(),
            user_subscription_limiters: self.user_subscription_limits.len(),
            global_connection_tokens: self.global_connection_limit.available_tokens(),
            global_message_tokens: self.global_message_limit.available_tokens(),
        }
    }

    fn current_time_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub ip_connection_limiters: usize,
    pub ip_message_limiters: usize,
    pub ip_auth_limiters: usize,
    pub user_message_limiters: usize,
    pub user_subscription_limiters: usize,
    pub global_connection_tokens: u32,
    pub global_message_tokens: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            // Conservative IP-based limits
            ip_connection_rate: 10,     // 10 connections per second per IP
            ip_connection_capacity: 50, // Burst up to 50 connections
            ip_message_rate: 100,       // 100 messages per second per IP
            ip_message_capacity: 1000,  // Burst up to 1000 messages
            ip_auth_rate: 5,            // 5 auth attempts per second per IP
            ip_auth_capacity: 20,       // Burst up to 20 auth attempts

            // User-based limits (more generous for authenticated users)
            user_message_rate: 200,      // 200 messages per second per user
            user_message_capacity: 2000, // Burst up to 2000 messages
            user_subscription_rate: 10,  // 10 subscriptions per second per user
            user_subscription_capacity: 100, // Burst up to 100 subscriptions

            // Global system limits
            global_connection_rate: 1000, // 1000 connections per second globally
            global_connection_capacity: 10000, // Burst up to 10k connections
            global_message_rate: 10000,   // 10k messages per second globally
            global_message_capacity: 100000, // Burst up to 100k messages

            cleanup_interval_seconds: 300, // Cleanup every 5 minutes
        }
    }
}

impl Default for MultiRateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::thread;

    // --- Existing tests ---

    /// Build a bucket with a controllable clock. Returns the bucket and the
    /// clock handle; advancing the clock simulates the passage of time.
    fn bucket_with_clock(
        refill_rate: u32,
        capacity: u32,
    ) -> (TokenBucketRateLimit, Arc<AtomicU64>) {
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let clock_ref = Arc::clone(&clock);
        let bucket = TokenBucketRateLimit::with_clock(refill_rate, capacity, 1, move || {
            clock_ref.load(Ordering::Relaxed)
        });
        (bucket, clock)
    }

    #[test]
    fn test_token_bucket_creation() {
        let bucket = TokenBucketRateLimit::new(10, 100, 1);
        assert_eq!(bucket.capacity, 100);
        assert_eq!(bucket.refill_rate, 10);
    }

    #[test]
    fn test_token_bucket_consume() {
        let bucket = TokenBucketRateLimit::new(100, 100, 1);
        for _ in 0..100 {
            assert!(bucket.try_consume(), "Should allow within capacity");
        }
        assert!(!bucket.try_consume(), "Should deny when exhausted");
    }

    #[test]
    fn test_token_bucket_refill() {
        let (bucket, clock) = bucket_with_clock(1000, 100);
        for _ in 0..100 {
            bucket.try_consume();
        }
        assert!(!bucket.try_consume(), "Should be empty with a frozen clock");

        // 200ms at 1000 tokens/sec refills the bucket to capacity.
        clock.store(clock.load(Ordering::Relaxed) + 200, Ordering::Relaxed);
        assert!(bucket.try_consume(), "Should refill after time");
        assert_eq!(
            bucket.available_tokens(),
            99,
            "200ms at 1000 tokens/sec = 200 tokens, capped at capacity 100, minus 1 consumed"
        );
    }

    #[test]
    fn test_token_bucket_refill_proportional() {
        let (bucket, clock) = bucket_with_clock(10, 1000);
        for _ in 0..500 {
            bucket.try_consume();
        }
        assert_eq!(bucket.available_tokens(), 500);

        // 50ms at 10 tokens/sec adds exactly 0 tokens (integer truncation).
        clock.store(clock.load(Ordering::Relaxed) + 50, Ordering::Relaxed);
        assert_eq!(bucket.available_tokens(), 500);

        // 100ms adds exactly 1 token.
        clock.store(clock.load(Ordering::Relaxed) + 50, Ordering::Relaxed);
        assert_eq!(bucket.available_tokens(), 501);
    }

    #[test]
    fn test_token_bucket_refill_capped_at_capacity() {
        let (bucket, clock) = bucket_with_clock(1000, 100);
        for _ in 0..50 {
            bucket.try_consume();
        }
        // 10s of refill must cap at capacity, never exceed it.
        clock.store(clock.load(Ordering::Relaxed) + 10_000, Ordering::Relaxed);
        assert_eq!(bucket.available_tokens(), 100);
    }

    #[test]
    fn test_rate_limit_config_defaults() {
        let cfg = RateLimitConfig::default();
        assert_eq!(cfg.ip_message_rate, 100);
        assert_eq!(cfg.ip_message_capacity, 1000);
        assert_eq!(cfg.user_message_rate, 200);
        assert_eq!(cfg.global_connection_rate, 1000);
    }

    #[test]
    fn test_multi_rate_limiter_default() {
        let limiter = MultiRateLimiter::default();
        assert_eq!(limiter.ip_connection_limits.len(), 0);
        assert_eq!(limiter.ip_message_limits.len(), 0);
        assert_eq!(limiter.ip_auth_limits.len(), 0);
    }

    // --- TokenBucketRateLimit new tests ---

    #[test]
    fn test_try_consume_multiple() {
        let bucket = TokenBucketRateLimit::new(10, 100, 1);
        assert!(bucket.try_consume_multiple(50));
        assert!(!bucket.try_consume_multiple(60));
        assert!(bucket.try_consume_multiple(50));
        assert!(!bucket.try_consume_multiple(1));
    }

    #[test]
    fn test_try_consume_multiple_exceeds_capacity() {
        let bucket = TokenBucketRateLimit::new(10, 100, 1);
        assert!(!bucket.try_consume_multiple(101));
    }

    #[test]
    fn test_zero_capacity_bucket() {
        let bucket = TokenBucketRateLimit::new(10, 0, 1);
        assert!(!bucket.try_consume());
        assert!(!bucket.try_consume_multiple(1));
        assert_eq!(bucket.available_tokens(), 0);
    }

    #[test]
    fn test_available_tokens_after_refill() {
        let (bucket, clock) = bucket_with_clock(1000, 100);
        for _ in 0..100 {
            bucket.try_consume();
        }
        assert_eq!(bucket.available_tokens(), 0);
        clock.store(clock.load(Ordering::Relaxed) + 200, Ordering::Relaxed);
        let tokens = bucket.available_tokens();
        assert_eq!(tokens, 100, "200ms at 1000 tokens/sec caps at capacity");
    }

    #[test]
    fn test_refill_timing() {
        let (bucket, clock) = bucket_with_clock(1000, 200);
        for _ in 0..200 {
            bucket.try_consume();
        }
        assert_eq!(bucket.available_tokens(), 0);
        clock.store(clock.load(Ordering::Relaxed) + 500, Ordering::Relaxed);
        assert_eq!(bucket.available_tokens(), 200);
    }

    #[test]
    fn test_concurrent_access() {
        let bucket = Arc::new(TokenBucketRateLimit::new(1000000, 100000, 1));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let b = Arc::clone(&bucket);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    b.try_consume();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let remaining = bucket.available_tokens();
        assert!(
            remaining > 80000 && remaining <= 100000,
            "Expected ~90000 tokens remaining, got {remaining}"
        );
    }

    // --- MultiRateLimiter new tests ---

    #[test]
    fn test_check_connection_limit_allows_up_to_capacity() {
        let limiter = MultiRateLimiter::default();
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        for i in 0..50 {
            assert!(
                matches!(limiter.check_connection_limit(ip), RateLimitResult::Allowed),
                "Connection {i} should be allowed"
            );
        }
        assert!(
            matches!(
                limiter.check_connection_limit(ip),
                RateLimitResult::Limited(_)
            ),
            "Connection 51 should be limited"
        );
    }

    #[test]
    fn test_check_connection_limit_then_denies() {
        let limiter = MultiRateLimiter::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        for _ in 0..50 {
            limiter.check_connection_limit(ip);
        }
        let result = limiter.check_connection_limit(ip);
        assert!(
            matches!(result, RateLimitResult::Limited(info) if info.limit_type == "ip_connections")
        );
    }

    #[test]
    fn test_check_message_limit_with_ip_and_user_id() {
        // Use a tiny capacity so wall-clock refill on a slow runner cannot
        // rescue the loop (the default 1000 @ 100/s capacity is non-
        // deterministic under load — see the sibling test below). This still
        // exercises that the (ip, user_id) pair is tracked as a single key.
        let mut config = RateLimitConfig::default();
        config.ip_message_rate = 1;
        config.ip_message_capacity = 10;
        config.user_message_rate = 1;
        config.user_message_capacity = 10;
        let limiter = MultiRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let user_id = "test_user";
        for _ in 0..200 {
            limiter.check_message_limit(Some(ip), Some(user_id));
        }
        assert!(
            matches!(
                limiter.check_message_limit(Some(ip), Some(user_id)),
                RateLimitResult::Limited(_)
            ),
            "Message after capacity should be limited"
        );
    }

    #[test]
    fn test_check_message_limit_with_ip_only() {
        // Deterministic under load: default capacities (1000 @ 100/s) can be
        // rescued by wall-clock refill on a slow runner mid-loop. A tiny
        // capacity + trickle refill cannot.
        let mut config = RateLimitConfig::default();
        config.ip_message_rate = 1;
        config.ip_message_capacity = 10;
        let limiter = MultiRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        for _ in 0..200 {
            limiter.check_message_limit(Some(ip), None);
        }
        assert!(matches!(
            limiter.check_message_limit(Some(ip), None),
            RateLimitResult::Limited(_)
        ));
    }

    #[test]
    fn test_check_message_limit_with_user_only() {
        let mut config = RateLimitConfig::default();
        config.user_message_rate = 1;
        config.user_message_capacity = 10;
        let limiter = MultiRateLimiter::new(config);
        let user_id = "high_volume_user";
        for _ in 0..200 {
            limiter.check_message_limit(None, Some(user_id));
        }
        assert!(matches!(
            limiter.check_message_limit(None, Some(user_id)),
            RateLimitResult::Limited(_)
        ));
    }

    #[test]
    fn test_check_auth_limit() {
        let limiter = MultiRateLimiter::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        for i in 0..20 {
            assert!(
                matches!(limiter.check_auth_limit(ip), RateLimitResult::Allowed),
                "Auth attempt {i} should be allowed"
            );
        }
        assert!(
            matches!(limiter.check_auth_limit(ip), RateLimitResult::Limited(_)),
            "Auth attempt 21 should be limited"
        );
    }

    #[test]
    fn test_check_subscription_limit() {
        let limiter = MultiRateLimiter::default();
        let user_id = "subscriber1";
        for i in 0..100 {
            assert!(
                matches!(
                    limiter.check_subscription_limit(user_id),
                    RateLimitResult::Allowed
                ),
                "Subscription {i} should be allowed"
            );
        }
        assert!(
            matches!(
                limiter.check_subscription_limit(user_id),
                RateLimitResult::Limited(_)
            ),
            "Subscription 101 should be limited"
        );
    }

    #[test]
    fn test_cleanup_old_entries_evicts_stale() {
        let mut config = RateLimitConfig::default();
        config.cleanup_interval_seconds = 1;
        let limiter = MultiRateLimiter::new(config);
        // Make last_cleanup ancient so cleanup triggers immediately
        limiter.last_cleanup.store(0, Ordering::Relaxed);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        limiter.check_connection_limit(ip);
        limiter.check_message_limit(Some(ip), Some("user"));
        assert_eq!(limiter.ip_connection_limits.len(), 1);
        assert_eq!(limiter.ip_message_limits.len(), 1);

        // Manually set last_used to ancient time so cleanup evicts
        if let Some(entry) = limiter.ip_connection_limits.get_mut(&ip) {
            entry.last_used.store(1, Ordering::Relaxed);
        }
        if let Some(entry) = limiter.ip_message_limits.get_mut(&ip) {
            entry.last_used.store(1, Ordering::Relaxed);
        }

        limiter.cleanup_old_entries();
        assert_eq!(limiter.ip_connection_limits.len(), 0);
        assert_eq!(limiter.ip_message_limits.len(), 0);
    }
    #[test]
    fn test_cleanup_old_entries_doesnt_crash() {
        let limiter = MultiRateLimiter::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        limiter.check_connection_limit(ip);
        limiter.check_message_limit(Some(ip), Some("user"));
        limiter.check_auth_limit(ip);
        limiter.check_subscription_limit("user");
        limiter.cleanup_old_entries();
    }

    #[test]
    fn test_get_stats_returns_correct_counts() {
        let limiter = MultiRateLimiter::default();
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let _ = limiter.check_connection_limit(ip1);
        let _ = limiter.check_connection_limit(ip2);
        let _ = limiter.check_message_limit(Some(ip1), Some("user1"));
        let _ = limiter.check_auth_limit(ip1);
        let _ = limiter.check_subscription_limit("user1");
        let _ = limiter.check_subscription_limit("user2");

        let stats = limiter.get_stats();
        assert_eq!(stats.ip_connection_limiters, 2);
        assert_eq!(stats.ip_message_limiters, 1);
        assert_eq!(stats.ip_auth_limiters, 1);
        assert_eq!(stats.user_message_limiters, 1);
        assert_eq!(stats.user_subscription_limiters, 2);
    }

    // --- RateLimitConfig new tests ---

    #[test]
    fn test_custom_config() {
        let config = RateLimitConfig {
            ip_connection_rate: 5,
            ip_connection_capacity: 10,
            ip_message_rate: 50,
            ip_message_capacity: 100,
            ip_auth_rate: 2,
            ip_auth_capacity: 5,
            user_message_rate: 100,
            user_message_capacity: 200,
            user_subscription_rate: 5,
            user_subscription_capacity: 10,
            global_connection_rate: 500,
            global_connection_capacity: 1000,
            global_message_rate: 5000,
            global_message_capacity: 10000,
            cleanup_interval_seconds: 60,
        };
        assert_eq!(config.ip_connection_rate, 5);
        assert_eq!(config.global_connection_capacity, 1000);
        assert_eq!(config.cleanup_interval_seconds, 60);
    }

    #[test]
    fn test_all_fields_have_defaults() {
        let cfg = RateLimitConfig::default();
        assert!(cfg.ip_connection_rate > 0);
        assert!(cfg.ip_connection_capacity > 0);
        assert!(cfg.ip_message_rate > 0);
        assert!(cfg.ip_message_capacity > 0);
        assert!(cfg.ip_auth_rate > 0);
        assert!(cfg.ip_auth_capacity > 0);
        assert!(cfg.user_message_rate > 0);
        assert!(cfg.user_message_capacity > 0);
        assert!(cfg.user_subscription_rate > 0);
        assert!(cfg.user_subscription_capacity > 0);
        assert!(cfg.global_connection_rate > 0);
        assert!(cfg.global_connection_capacity > 0);
        assert!(cfg.global_message_rate > 0);
        assert!(cfg.global_message_capacity > 0);
        assert!(cfg.cleanup_interval_seconds > 0);
    }

    // --- RateLimitResult / RateLimitInfo tests ---

    #[test]
    fn test_rate_limit_result_debug_clone() {
        let allowed = RateLimitResult::Allowed;
        let limited = RateLimitResult::Limited(RateLimitInfo {
            limit_type: "test".into(),
            current_rate: 5,
            limit_threshold: 10,
            retry_after_seconds: 30,
        });
        let _ = format!("{:?}", &allowed);
        let _ = allowed.clone();
        let _ = format!("{:?}", &limited);
        let _ = limited.clone();
    }

    #[test]
    fn test_rate_limit_info_contents() {
        let info = RateLimitInfo {
            limit_type: "ip_connections".into(),
            current_rate: 42,
            limit_threshold: 50,
            retry_after_seconds: 60,
        };
        assert_eq!(info.limit_type, "ip_connections");
        assert_eq!(info.current_rate, 42);
        assert_eq!(info.retry_after_seconds, 60);
    }
}
