// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use lru::LruCache;
use parking_lot::RwLock;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

use crate::auth::websocket_auth::AuthResult as WebSocketAuthResult;

/// High-performance security cache with proper invalidation
pub struct SecurityCache {
    // LRU caches for frequently accessed data
    auth_cache: Arc<RwLock<LruCache<String, CachedAuthResult>>>,
    permission_cache: Arc<RwLock<LruCache<String, CachedPermission>>>,

    // Fast lockout tracking
    auth_failures: Arc<DashMap<String, AuthFailureTracker>>,

    // IP allowlist/blocklist (very fast lookups)
    ip_allowlist: Arc<DashMap<IpAddr, bool>>,
    ip_blocklist: Arc<DashMap<IpAddr, BlocklistEntry>>,

    // Configuration
    cache_ttl_seconds: u64,
}

#[derive(Clone, Debug)]
pub struct CachedAuthResult {
    pub auth_result: WebSocketAuthResult,
    pub cached_at: Instant,
    pub expires_at: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub struct CachedPermission {
    pub allowed: bool,
    pub cached_at: Instant,
    pub cache_key: String,
}

#[derive(Clone, Debug)]
pub struct AuthFailureTracker {
    pub failures: u32,
    pub first_failure: Instant,
    pub last_failure: Instant,
    pub lockout_until: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct BlocklistEntry {
    pub blocked_at: Instant,
    pub reason: String,
    pub expires_at: Option<Instant>,
}

impl SecurityCache {
    pub fn new(
        auth_cache_size: usize,
        permission_cache_size: usize,
        cache_ttl_seconds: u64,
    ) -> Self {
        Self {
            auth_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(auth_cache_size).unwrap(),
            ))),
            permission_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(permission_cache_size).unwrap(),
            ))),
            auth_failures: Arc::new(DashMap::new()),
            ip_allowlist: Arc::new(DashMap::new()),
            ip_blocklist: Arc::new(DashMap::new()),
            cache_ttl_seconds,
        }
    }

    /// Generate secure cache key using HMAC
    pub fn generate_cache_key(&self, prefix: &str, data: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        // Use a secret derived from system time for cache key generation
        let secret = format!(
            "actae_cache_secret_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                / 3600
        ); // Rotate hourly

        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("Invalid cache key secret");
        mac.update(data.as_bytes());
        let result = hex::encode(mac.finalize().into_bytes());

        format!("{}:{}", prefix, &result[..16]) // Use first 16 chars for performance
    }

    /// Check cached authentication result
    pub fn get_cached_auth(&self, token: &str) -> Option<WebSocketAuthResult> {
        let cache_key = self.generate_cache_key("auth", token);

        let cache = self.auth_cache.read();
        if let Some(cached) = cache.peek(&cache_key) {
            // Check if cache entry is still valid
            if cached.cached_at.elapsed().as_secs() < self.cache_ttl_seconds {
                // Check if auth result hasn't expired
                if let Some(expires_at) = cached.expires_at {
                    if SystemTime::now() > expires_at {
                        return None; // Expired
                    }
                }

                debug!("Cache hit for auth token");
                return Some(cached.auth_result.clone());
            }
        }

        None
    }

    /// Cache authentication result
    pub fn cache_auth(&self, token: &str, auth_result: WebSocketAuthResult) {
        let cache_key = self.generate_cache_key("auth", token);
        let cached_result = CachedAuthResult {
            expires_at: auth_result.expires_at.map(|dt| {
                std::time::SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(dt.timestamp() as u64)
            }),
            auth_result,
            cached_at: Instant::now(),
        };

        let mut cache = self.auth_cache.write();
        cache.put(cache_key, cached_result);

        debug!("Cached auth result");
    }

    /// Check cached permission
    pub fn get_cached_permission(
        &self,
        user_id: &str,
        resource: &str,
        action: &str,
    ) -> Option<bool> {
        let permission_key = format!("{user_id}:{resource}:{action}");
        let cache_key = self.generate_cache_key("perm", &permission_key);

        let cache = self.permission_cache.read();
        if let Some(cached) = cache.peek(&cache_key) {
            if cached.cached_at.elapsed().as_secs() < self.cache_ttl_seconds {
                debug!("Cache hit for permission check");
                return Some(cached.allowed);
            }
        }

        None
    }

    /// Cache permission result
    pub fn cache_permission(&self, user_id: &str, resource: &str, action: &str, allowed: bool) {
        let permission_key = format!("{user_id}:{resource}:{action}");
        let cache_key = self.generate_cache_key("perm", &permission_key);

        let cached_permission = CachedPermission {
            allowed,
            cached_at: Instant::now(),
            cache_key: format!("perm:{permission_key}"), // raw key for invalidation lookup
        };

        let mut cache = self.permission_cache.write();
        cache.put(cache_key, cached_permission);

        debug!("Cached permission result");
    }

    /// Track authentication failure
    pub fn record_auth_failure(
        &self,
        identifier: &str,
        lockout_threshold: u32,
        lockout_duration: Duration,
    ) -> bool {
        let now = Instant::now();

        let is_locked_out = self
            .auth_failures
            .entry(identifier.to_string())
            .and_modify(|tracker| {
                tracker.failures += 1;
                tracker.last_failure = now;

                if tracker.failures >= lockout_threshold {
                    tracker.lockout_until = Some(now + lockout_duration);
                }
            })
            .or_insert_with(|| AuthFailureTracker {
                failures: 1,
                first_failure: now,
                last_failure: now,
                lockout_until: if lockout_threshold <= 1 {
                    Some(now + lockout_duration)
                } else {
                    None
                },
            })
            .lockout_until
            .map(|lockout_until| now < lockout_until)
            .unwrap_or(false);

        if is_locked_out {
            warn!(
                "Account locked out due to repeated failures: {}",
                identifier
            );
        }

        is_locked_out
    }

    /// Check if identifier is locked out
    pub fn is_locked_out(&self, identifier: &str) -> bool {
        if let Some(tracker) = self.auth_failures.get(identifier) {
            if let Some(lockout_until) = tracker.lockout_until {
                return Instant::now() < lockout_until;
            }
        }
        false
    }

    /// Clear auth failures (on successful auth)
    pub fn clear_auth_failures(&self, identifier: &str) {
        self.auth_failures.remove(identifier);
    }

    /// Check if IP is in allowlist
    pub fn is_ip_allowed(&self, ip: IpAddr) -> bool {
        // If allowlist is empty, allow all IPs
        if self.ip_allowlist.is_empty() {
            return true;
        }

        self.ip_allowlist.contains_key(&ip)
    }

    /// Check if IP is blocked
    pub fn is_ip_blocked(&self, ip: IpAddr) -> bool {
        if let Some(entry) = self.ip_blocklist.get(&ip) {
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    // Remove expired entry
                    drop(entry);
                    self.ip_blocklist.remove(&ip);
                    return false;
                }
            }
            return true;
        }
        false
    }

    /// Add IP to allowlist
    pub fn add_ip_to_allowlist(&self, ip: IpAddr) {
        self.ip_allowlist.insert(ip, true);
    }

    /// Block IP address
    pub fn block_ip(&self, ip: IpAddr, reason: String, duration: Option<Duration>) {
        let entry = BlocklistEntry {
            blocked_at: Instant::now(),
            reason: reason.clone(),
            expires_at: duration.map(|d| Instant::now() + d),
        };

        self.ip_blocklist.insert(ip, entry);
        warn!("Blocked IP address: {} for {}", ip, reason);
    }

    /// Invalidate all caches for a user
    pub fn invalidate_user_cache(&self, user_id: &str) {
        // Remove permission cache entries matching this user_id.
        // The cache_key field stores the raw permission string ("perm:user:res:action")
        // separately from the HMAC-based LRU key, enabling substring-based invalidation.
        let mut permission_cache = self.permission_cache.write();
        let keys_to_remove: Vec<_> = permission_cache
            .iter()
            .filter(|(_, cached)| cached.cache_key.contains(user_id))
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_remove {
            permission_cache.pop(&key);
        }

        debug!("Invalidated cache for user: {}", user_id);
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> SecurityCacheStats {
        let auth_cache = self.auth_cache.read();
        let permission_cache = self.permission_cache.read();

        SecurityCacheStats {
            auth_cache_size: auth_cache.len(),
            auth_cache_capacity: auth_cache.cap().get(),
            permission_cache_size: permission_cache.len(),
            permission_cache_capacity: permission_cache.cap().get(),
            auth_failures_tracked: self.auth_failures.len(),
            ip_allowlist_size: self.ip_allowlist.len(),
            ip_blocklist_size: self.ip_blocklist.len(),
        }
    }

    /// Cleanup expired entries (should be called periodically)
    pub fn cleanup_expired(&self) {
        let now = Instant::now();

        // Clean up auth failures
        self.auth_failures.retain(|_, tracker| {
            if let Some(lockout_until) = tracker.lockout_until {
                now < lockout_until
            } else {
                // Keep failures for some time even after lockout expires
                tracker.last_failure.elapsed() < Duration::from_secs(3600) // 1 hour
            }
        });

        // Clean up expired IP blocks
        self.ip_blocklist.retain(|_, entry| {
            if let Some(expires_at) = entry.expires_at {
                now < expires_at
            } else {
                true // Permanent blocks
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct SecurityCacheStats {
    pub auth_cache_size: usize,
    pub auth_cache_capacity: usize,
    pub permission_cache_size: usize,
    pub permission_cache_capacity: usize,
    pub auth_failures_tracked: usize,
    pub ip_allowlist_size: usize,
    pub ip_blocklist_size: usize,
}

impl Default for SecurityCache {
    fn default() -> Self {
        Self::new(
            1000, // auth cache size
            5000, // permission cache size
            300,  // 5 minute TTL
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::websocket_auth::{AuthMethod, AuthResult};
    use chrono::Utc;
    use std::net::{IpAddr, Ipv4Addr};

    // --- Existing tests ---

    #[test]
    fn test_security_cache_blocklist() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(!cache.is_ip_blocked(ip));
        cache.block_ip(ip, "test".into(), None);
        assert!(cache.is_ip_blocked(ip));
    }

    #[test]
    fn test_security_cache_auth_failures() {
        let cache = SecurityCache::default();
        let id = "10.0.0.3";
        assert!(!cache.is_locked_out(id));
        cache.record_auth_failure(id, 5, Duration::from_secs(300));
        assert!(!cache.is_locked_out(id));
    }

    #[test]
    fn test_ip_allowed_by_default() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(cache.is_ip_allowed(ip));
    }

    #[test]
    fn test_blocked_ip_is_blocked() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        cache.block_ip(ip, "blocked".into(), None);
        assert!(cache.is_ip_blocked(ip));
    }

    #[test]
    fn test_blocked_ip_not_affected_by_is_ip_allowed() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6));
        cache.block_ip(ip, "blocked".into(), None);
        // is_ip_allowed only checks allowlist, not blocklist
        // Empty allowlist means all IPs are allowed
        assert!(cache.is_ip_allowed(ip));
        assert!(cache.is_ip_blocked(ip));
    }

    // --- SecurityCache new tests ---

    #[test]
    fn test_new_creates_cache_with_correct_capacities() {
        let cache = SecurityCache::new(100, 200, 300);
        let stats = cache.get_stats();
        assert_eq!(stats.auth_cache_capacity, 100);
        assert_eq!(stats.permission_cache_capacity, 200);
    }

    #[test]
    fn test_auth_cache_stores_and_retrieves() {
        let cache = SecurityCache::default();
        let auth_result = AuthResult {
            federation_operation_id: None,
            federation_action: None,
            federation_instance_id: None,
            user_id: "user1".to_string(),
            auth_method: AuthMethod::JWT,
            permissions: None,
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            key_source: None,
            key_id: None,
        };
        cache.cache_auth("token123", auth_result.clone());
        let cached = cache.get_cached_auth("token123");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().user_id, "user1");
    }

    #[test]
    fn test_auth_cache_returns_none_for_unknown_token() {
        let cache = SecurityCache::default();
        assert!(cache.get_cached_auth("nonexistent_token").is_none());
    }

    #[test]
    fn test_auth_cache_evicts_after_size_limit() {
        let cache = SecurityCache::new(1, 1, 300);
        let auth1 = AuthResult {
            federation_operation_id: None,
            federation_action: None,
            federation_instance_id: None,
            user_id: "user1".to_string(),
            auth_method: AuthMethod::JWT,
            permissions: None,
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            key_source: None,
            key_id: None,
        };
        let auth2 = AuthResult {
            federation_operation_id: None,
            federation_action: None,
            federation_instance_id: None,
            user_id: "user2".to_string(),
            auth_method: AuthMethod::ApiKey,
            permissions: None,
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            key_source: Some("local".to_string()),
            key_id: None,
        };
        cache.cache_auth("token1", auth1);
        cache.cache_auth("token2", auth2);
        assert!(cache.get_cached_auth("token1").is_none());
        assert!(cache.get_cached_auth("token2").is_some());
    }

    #[test]
    fn test_permission_cache_operations() {
        let cache = SecurityCache::default();
        assert!(cache
            .get_cached_permission("user1", "resource1", "read")
            .is_none());
        cache.cache_permission("user1", "resource1", "read", true);
        assert_eq!(
            cache.get_cached_permission("user1", "resource1", "read"),
            Some(true)
        );
        cache.cache_permission("user1", "resource1", "read", false);
        assert_eq!(
            cache.get_cached_permission("user1", "resource1", "read"),
            Some(false)
        );
    }

    #[test]
    fn test_permission_cache_different_keys_independent() {
        let cache = SecurityCache::default();
        cache.cache_permission("user1", "resource1", "read", true);
        cache.cache_permission("user1", "resource1", "write", true);
        cache.cache_permission("user2", "resource1", "read", false);
        assert_eq!(
            cache.get_cached_permission("user1", "resource1", "read"),
            Some(true)
        );
        assert_eq!(
            cache.get_cached_permission("user1", "resource1", "write"),
            Some(true)
        );
        assert_eq!(
            cache.get_cached_permission("user2", "resource1", "read"),
            Some(false)
        );
    }

    #[test]
    fn test_auth_failures_tracking() {
        let cache = SecurityCache::default();
        let id = "brute_forcer";
        assert!(!cache.is_locked_out(id));
        assert!(!cache.record_auth_failure(id, 3, Duration::from_secs(300)));
        assert!(!cache.record_auth_failure(id, 3, Duration::from_secs(300)));
        let is_locked = cache.record_auth_failure(id, 3, Duration::from_secs(300));
        assert!(is_locked);
        assert!(cache.is_locked_out(id));
        cache.clear_auth_failures(id);
        assert!(!cache.is_locked_out(id));
    }

    #[test]
    fn test_lockout_tracking() {
        let cache = SecurityCache::default();
        let id = "lockout_user";
        assert!(!cache.record_auth_failure(id, 2, Duration::from_secs(60)));
        let is_locked = cache.record_auth_failure(id, 2, Duration::from_secs(60));
        assert!(is_locked);
        assert!(cache.is_locked_out(id));
        cache.clear_auth_failures(id);
        assert!(!cache.is_locked_out(id));
        assert!(!cache.is_locked_out("nonexistent"));
    }

    #[test]
    fn test_auth_failure_threshold_one_locks_immediately() {
        let cache = SecurityCache::default();
        let id = "threshold_one";
        let is_locked = cache.record_auth_failure(id, 1, Duration::from_secs(300));
        assert!(is_locked);
        assert!(cache.is_locked_out(id));
    }

    #[test]
    fn test_ip_allowlist_lookup() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(cache.is_ip_allowed(ip));
        cache.add_ip_to_allowlist(ip);
        assert!(cache.is_ip_allowed(ip));
        let other_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(!cache.is_ip_allowed(other_ip));
    }

    #[test]
    fn test_ip_blocklist_lookup() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(!cache.is_ip_blocked(ip));
        cache.block_ip(ip, "malicious".into(), Some(Duration::from_secs(3600)));
        assert!(cache.is_ip_blocked(ip));
    }

    #[test]
    fn test_ip_blocklist_expired_removed() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        cache.block_ip(ip, "temporary".into(), Some(Duration::from_secs(0)));
        std::thread::sleep(Duration::from_millis(10));
        assert!(!cache.is_ip_blocked(ip));
    }

    #[test]
    fn test_ip_blocklist_permanent() {
        let cache = SecurityCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        cache.block_ip(ip, "permanent".into(), None);
        assert!(cache.is_ip_blocked(ip));
    }

    #[test]
    fn test_get_stats() {
        let cache = SecurityCache::new(10, 20, 300);
        let stats = cache.get_stats();
        assert_eq!(stats.auth_cache_capacity, 10);
        assert_eq!(stats.permission_cache_capacity, 20);
        assert_eq!(stats.auth_cache_size, 0);
        assert_eq!(stats.permission_cache_size, 0);
        assert_eq!(stats.auth_failures_tracked, 0);
        assert_eq!(stats.ip_allowlist_size, 0);
        assert_eq!(stats.ip_blocklist_size, 0);
    }

    #[test]
    fn test_cleanup_expired_auth_failures() {
        let cache = SecurityCache::default();
        // Trigger lockout: record enough failures to reach threshold=2
        cache.record_auth_failure("old_entry", 2, Duration::from_millis(10));
        assert!(!cache.is_locked_out("old_entry"));
        cache.record_auth_failure("old_entry", 2, Duration::from_millis(10));
        assert!(cache.is_locked_out("old_entry"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !cache.is_locked_out("old_entry"),
            "Lockout should have expired"
        );
    }

    #[test]
    fn test_cleanup_expired_ip_blocks() {
        let cache = SecurityCache::default();
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        cache.block_ip(ip1, "temp".into(), Some(Duration::from_millis(10)));
        cache.block_ip(ip2, "permanent".into(), None);
        std::thread::sleep(Duration::from_millis(20));
        cache.cleanup_expired();
        assert!(!cache.is_ip_blocked(ip1));
        assert!(cache.is_ip_blocked(ip2));
    }

    #[test]
    fn test_invalidate_user_cache() {
        let cache = SecurityCache::default();
        cache.cache_permission("user1", "res1", "read", true);
        cache.cache_permission("user2", "res1", "read", true);
        let stats_before = cache.get_stats();
        assert_eq!(
            stats_before.permission_cache_size, 2,
            "Should have 2 cached permissions"
        );

        cache.invalidate_user_cache("user1");
        let stats_after = cache.get_stats();
        assert_eq!(
            stats_after.permission_cache_size, 1,
            "user1's permissions should be removed"
        );
        // user2's permission should still be cached
        assert_eq!(
            cache.get_cached_permission("user2", "res1", "read"),
            Some(true)
        );
    }

    // --- AuthFailureTracker tests ---

    #[test]
    fn test_auth_failure_tracker_creation() {
        let now = Instant::now();
        let tracker = AuthFailureTracker {
            failures: 3,
            first_failure: now,
            last_failure: now,
            lockout_until: Some(now + Duration::from_secs(300)),
        };
        assert_eq!(tracker.failures, 3);
        assert!(tracker.lockout_until.is_some());
    }

    // --- BlocklistEntry tests ---

    #[test]
    fn test_blocklist_entry_creation() {
        let now = Instant::now();
        let entry = BlocklistEntry {
            blocked_at: now,
            reason: "DoS attack detected".to_string(),
            expires_at: Some(now + Duration::from_secs(3600)),
        };
        assert_eq!(entry.reason, "DoS attack detected");
        assert!(entry.expires_at.is_some());
        let entry_permanent = BlocklistEntry {
            blocked_at: now,
            reason: "permanent ban".to_string(),
            expires_at: None,
        };
        assert!(entry_permanent.expires_at.is_none());
    }

    // --- CachedAuthResult tests ---

    #[test]
    fn test_cached_auth_result_timing() {
        let cached_at = Instant::now();
        let result = CachedAuthResult {
            auth_result: AuthResult {
                federation_operation_id: None,
                federation_action: None,
                federation_instance_id: None,
                user_id: "user1".to_string(),
                auth_method: AuthMethod::JWT,
                permissions: None,
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
                key_source: None,
                key_id: None,
            },
            cached_at,
            expires_at: Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
        };
        assert!(result.cached_at.elapsed().as_secs() < 1);
        assert!(result.expires_at.unwrap() > std::time::SystemTime::now());
    }

    // --- SecurityCacheStats tests ---

    #[test]
    fn test_security_cache_stats_debug_clone() {
        let stats = SecurityCacheStats {
            auth_cache_size: 10,
            auth_cache_capacity: 100,
            permission_cache_size: 20,
            permission_cache_capacity: 200,
            auth_failures_tracked: 5,
            ip_allowlist_size: 2,
            ip_blocklist_size: 3,
        };
        let _ = format!("{:?}", &stats);
        let _ = stats.clone();
    }

    // --- CachedPermission tests ---

    #[test]
    fn test_cached_permission_debug_clone() {
        let perm = CachedPermission {
            allowed: true,
            cached_at: Instant::now(),
            cache_key: "test_key".to_string(),
        };
        let _ = format!("{:?}", &perm);
        let _ = perm.clone();
    }
}
