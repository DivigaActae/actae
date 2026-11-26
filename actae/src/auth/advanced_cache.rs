// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Advanced authentication caching with frequency-based LRU eviction
//!
//! This module implements a high-performance authentication cache that combines
//! frequency-based access patterns with LRU eviction for optimal cache hit rates.

use parking_lot::RwLock;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, HistogramVec,
    IntCounterVec, IntGaugeVec,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, info};

/// Configuration for the advanced auth cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedCacheConfig {
    /// Maximum number of entries in the cache
    pub max_entries: usize,
    /// Default TTL for cache entries
    pub default_ttl: Duration,
    /// Frequency decay factor (0.0 to 1.0)
    pub frequency_decay: f64,
    /// Frequency update interval
    pub frequency_update_interval: Duration,
    /// Minimum access frequency to avoid eviction
    pub min_frequency_threshold: f64,
    /// Enable adaptive TTL based on access patterns
    pub adaptive_ttl: bool,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for AdvancedCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            default_ttl: Duration::from_secs(300), // 5 minutes
            frequency_decay: 0.9,
            frequency_update_interval: Duration::from_secs(60), // 1 minute
            min_frequency_threshold: 0.1,
            adaptive_ttl: true,
            enable_metrics: true,
        }
    }
}

/// Cache entry with frequency tracking and TTL
#[derive(Debug, Clone)]
struct CacheEntry<V> {
    /// Cached value
    value: V,
    /// Access frequency score
    frequency: f64,
    /// Last access timestamp
    last_access: Instant,
    /// Entry creation timestamp
    created_at: Instant,
    /// Entry expiration time
    expires_at: Instant,
    /// Total access count
    access_count: u64,
    /// Average access interval
    avg_access_interval: Duration,
    freq_key: OrderedFrequency,
}

impl<V> CacheEntry<V> {
    fn new(value: V, ttl: Duration, seq: u64) -> Self {
        let now = Instant::now();
        Self {
            value,
            frequency: 1.0,
            last_access: now,
            created_at: now,
            expires_at: now + ttl,
            access_count: 1,
            avg_access_interval: Duration::from_secs(0),
            freq_key: OrderedFrequency {
                frequency: 1.0,
                seq,
            },
        }
    }

    fn access(&mut self, decay_factor: f64) {
        let now = Instant::now();
        let _time_since_last = now.duration_since(self.last_access);

        // Update access count and average interval
        self.access_count += 1;
        if self.access_count > 1 {
            let total_time = now.duration_since(self.created_at);
            self.avg_access_interval = total_time / (self.access_count - 1) as u32;
        }

        // Apply frequency decay and add current access
        self.frequency = self.frequency * decay_factor + 1.0;
        self.last_access = now;
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    fn refresh_ttl(&mut self, ttl: Duration) {
        self.expires_at = Instant::now() + ttl;
    }

    fn calculate_adaptive_ttl(&self, base_ttl: Duration) -> Duration {
        if self.access_count < 2 {
            return base_ttl;
        }

        // Higher frequency = longer TTL
        let frequency_multiplier = (self.frequency / 10.0).clamp(0.5, 3.0);

        // More frequent access = longer TTL
        let access_multiplier = if self.avg_access_interval < Duration::from_secs(60) {
            2.0 // Very frequent access
        } else if self.avg_access_interval < Duration::from_secs(300) {
            1.5 // Frequent access
        } else {
            1.0 // Normal access
        };

        let multiplier = frequency_multiplier * access_multiplier;
        Duration::from_secs((base_ttl.as_secs() as f64 * multiplier) as u64)
    }
}

/// Frequency-based LRU cache metrics
#[derive(Debug)]
pub struct CacheMetrics {
    /// Cache hit counter
    hits: IntCounterVec,
    /// Cache miss counter
    misses: IntCounterVec,
    /// Cache size gauge
    size: IntGaugeVec,
    /// Eviction counter
    evictions: IntCounterVec,
    /// Access latency histogram
    access_latency: HistogramVec,
    /// Frequency distribution histogram
    frequency_distribution: HistogramVec,
}

impl CacheMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let hits = register_int_counter_vec!(
            "actae_auth_cache_hits_total",
            "Total number of cache hits",
            &["cache_type"]
        )?;

        let misses = register_int_counter_vec!(
            "actae_auth_cache_misses_total",
            "Total number of cache misses",
            &["cache_type"]
        )?;

        let size = register_int_gauge_vec!(
            "actae_auth_cache_size",
            "Current cache size",
            &["cache_type"]
        )?;

        let evictions = register_int_counter_vec!(
            "actae_auth_cache_evictions_total",
            "Total number of cache evictions",
            &["cache_type", "reason"]
        )?;

        let access_latency = register_histogram_vec!(
            "actae_auth_cache_access_latency_seconds",
            "Cache access latency",
            &["cache_type", "result"],
            vec![
                0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001,
                0.0025, 0.005
            ]
        )?;

        let frequency_distribution = register_histogram_vec!(
            "actae_auth_cache_frequency_distribution",
            "Distribution of entry frequencies",
            &["cache_type"],
            vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]
        )?;

        Ok(Self {
            hits,
            misses,
            size,
            evictions,
            access_latency,
            frequency_distribution,
        })
    }

    pub fn record_hit(&self, cache_type: &str, latency: Duration) {
        self.hits.with_label_values(&[cache_type]).inc();
        self.access_latency
            .with_label_values(&[cache_type, "hit"])
            .observe(latency.as_secs_f64());
    }

    pub fn record_miss(&self, cache_type: &str, latency: Duration) {
        self.misses.with_label_values(&[cache_type]).inc();
        self.access_latency
            .with_label_values(&[cache_type, "miss"])
            .observe(latency.as_secs_f64());
    }

    pub fn record_eviction(&self, cache_type: &str, reason: &str) {
        self.evictions
            .with_label_values(&[cache_type, reason])
            .inc();
    }

    pub fn update_size(&self, cache_type: &str, size: usize) {
        self.size.with_label_values(&[cache_type]).set(size as i64);
    }

    pub fn record_frequency(&self, cache_type: &str, frequency: f64) {
        self.frequency_distribution
            .with_label_values(&[cache_type])
            .observe(frequency);
    }
}

/// Advanced frequency-based LRU cache
pub struct FrequencyLRUCache<K, V> {
    /// Cache entries
    entries: HashMap<K, CacheEntry<V>>,
    /// Frequency-based ordering for eviction
    frequency_order: BTreeMap<OrderedFrequency, K>,
    /// Configuration
    config: AdvancedCacheConfig,
    /// Metrics collector
    metrics: Option<Arc<CacheMetrics>>,
    cache_type: String,
    seq: u64,
}

/// Wrapper for frequency that implements ordering
///
/// Ordering: frequency ASC (evict least frequent first), then access order
/// ASC (the entry accessed longest ago has the smallest `seq`). The access
/// sequence number is the deterministic, preemption-proof recency
/// discriminator — wall-clock elapsed time between accesses is microsecond
/// noise that made eviction order non-deterministic under load.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedFrequency {
    frequency: f64,
    seq: u64,
}

impl Eq for OrderedFrequency {}

impl PartialOrd for OrderedFrequency {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFrequency {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.frequency
            .partial_cmp(&other.frequency)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl<K, V> FrequencyLRUCache<K, V>
where
    K: Clone + Eq + Hash + std::fmt::Debug,
    V: Clone,
{
    /// Create a new frequency-based LRU cache
    pub fn new(config: AdvancedCacheConfig, cache_type: String) -> Result<Self, prometheus::Error> {
        let metrics = if config.enable_metrics {
            Some(Arc::new(CacheMetrics::new()?))
        } else {
            None
        };

        Ok(Self {
            entries: HashMap::with_capacity(config.max_entries),
            frequency_order: BTreeMap::new(),
            config,
            metrics,
            cache_type,
            seq: 0,
        })
    }

    /// Get a value from the cache
    pub fn get(&mut self, key: &K) -> Option<V> {
        let start = Instant::now();

        if let Some(entry) = self.entries.get_mut(key) {
            // Check if entry is expired
            if entry.is_expired() {
                self.remove_internal(key);
                if let Some(ref metrics) = self.metrics {
                    metrics.record_miss(&self.cache_type, start.elapsed());
                }
                return None;
            }

            // Update frequency and access time
            let old_frequency = entry.freq_key;

            entry.access(self.config.frequency_decay);

            // Update frequency ordering
            self.frequency_order.remove(&old_frequency);
            self.seq += 1;
            entry.freq_key = OrderedFrequency {
                frequency: entry.frequency,
                seq: self.seq,
            };
            self.frequency_order.insert(entry.freq_key, key.clone());

            // Refresh TTL if adaptive TTL is enabled
            if self.config.adaptive_ttl {
                let new_ttl = entry.calculate_adaptive_ttl(self.config.default_ttl);
                entry.refresh_ttl(new_ttl);
            }

            if let Some(ref metrics) = self.metrics {
                metrics.record_hit(&self.cache_type, start.elapsed());
                metrics.record_frequency(&self.cache_type, entry.frequency);
            }

            Some(entry.value.clone())
        } else {
            if let Some(ref metrics) = self.metrics {
                metrics.record_miss(&self.cache_type, start.elapsed());
            }
            None
        }
    }

    /// Insert a value into the cache
    pub fn insert(&mut self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.config.default_ttl);
    }

    /// Insert a value with custom TTL
    pub fn insert_with_ttl(&mut self, key: K, value: V, ttl: Duration) {
        // Remove existing entry if present
        if self.entries.contains_key(&key) {
            self.remove_internal(&key);
        }

        // Evict entries if necessary
        self.evict_if_necessary();

        // Create new entry
        self.seq += 1;
        let entry = CacheEntry::new(value, ttl, self.seq);
        let key_ord = entry.freq_key;

        self.entries.insert(key.clone(), entry);
        self.frequency_order.insert(key_ord, key);

        self.update_metrics();
    }

    /// Remove a value from the cache
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.remove_internal(key).map(|entry| entry.value)
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let mut frequency_sum = 0.0;
        let mut expired_count = 0;
        let mut avg_age = Duration::from_secs(0);
        let now = Instant::now();

        for entry in self.entries.values() {
            frequency_sum += entry.frequency;
            if entry.is_expired() {
                expired_count += 1;
            }
            avg_age += now.duration_since(entry.created_at);
        }

        let size = self.entries.len();
        CacheStats {
            size,
            max_size: self.config.max_entries,
            avg_frequency: if size > 0 {
                frequency_sum / size as f64
            } else {
                0.0
            },
            expired_count,
            avg_age: if size > 0 {
                avg_age / size as u32
            } else {
                Duration::from_secs(0)
            },
            hit_rate: 0.0, // This would need to be calculated from metrics
        }
    }

    /// Clean up expired entries
    pub fn cleanup_expired(&mut self) -> usize {
        let _now = Instant::now();
        let mut expired_keys = Vec::new();

        for (key, entry) in &self.entries {
            if entry.is_expired() {
                expired_keys.push(key.clone());
            }
        }

        let count = expired_keys.len();
        for key in expired_keys {
            self.remove_internal(&key);
            if let Some(ref metrics) = self.metrics {
                metrics.record_eviction(&self.cache_type, "expired");
            }
        }

        if count > 0 {
            self.update_metrics();
            debug!("Cleaned up {} expired cache entries", count);
        }

        count
    }

    /// Apply frequency decay to all entries
    pub fn apply_frequency_decay(&mut self) {
        let decay_factor = self.config.frequency_decay;
        let mut updates = Vec::new();

        for (key, entry) in &mut self.entries {
            let old_frequency = entry.freq_key;

            entry.frequency *= decay_factor;

            self.seq += 1;
            entry.freq_key = OrderedFrequency {
                frequency: entry.frequency,
                seq: self.seq,
            };

            updates.push((old_frequency, entry.freq_key, key.clone()));
        }

        // Update frequency ordering
        for (old_freq, new_freq, key) in updates {
            self.frequency_order.remove(&old_freq);
            self.frequency_order.insert(new_freq, key);
        }

        debug!(
            "Applied frequency decay to {} cache entries",
            self.entries.len()
        );
    }

    /// Internal method to remove an entry
    fn remove_internal(&mut self, key: &K) -> Option<CacheEntry<V>> {
        if let Some(entry) = self.entries.remove(key) {
            self.frequency_order.remove(&entry.freq_key);
            Some(entry)
        } else {
            None
        }
    }

    /// Evict entries if cache is full
    fn evict_if_necessary(&mut self) {
        while self.entries.len() >= self.config.max_entries {
            self.evict_one();
        }
    }

    /// Evict one entry using frequency-based LRU
    fn evict_one(&mut self) {
        // Find the entry with lowest frequency (and oldest access time as tiebreaker)
        if let Some((frequency, key)) = self.frequency_order.iter().next() {
            let frequency = *frequency;
            let key = key.clone();

            // Check if frequency is above minimum threshold
            if frequency.frequency >= self.config.min_frequency_threshold {
                // If all entries have high frequency, evict the oldest
                if let Some((_, oldest_key)) = self.frequency_order.iter().next() {
                    let oldest_key = oldest_key.clone();
                    self.remove_internal(&oldest_key);
                    if let Some(ref metrics) = self.metrics {
                        metrics.record_eviction(&self.cache_type, "lru");
                    }
                }
            } else {
                // Evict low-frequency entry
                self.remove_internal(&key);
                if let Some(ref metrics) = self.metrics {
                    metrics.record_eviction(&self.cache_type, "low_frequency");
                }
            }
        }
    }

    /// Update metrics
    fn update_metrics(&self) {
        if let Some(ref metrics) = self.metrics {
            metrics.update_size(&self.cache_type, self.entries.len());
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries
    pub size: usize,
    /// Maximum allowed entries
    pub max_size: usize,
    /// Average frequency of entries
    pub avg_frequency: f64,
    /// Number of expired entries
    pub expired_count: usize,
    /// Average age of entries
    pub avg_age: Duration,
    /// Cache hit rate
    pub hit_rate: f64,
}

/// Cache manager that handles multiple cache instances
pub struct AdvancedCacheManager {
    /// Configuration
    config: AdvancedCacheConfig,
    /// JWT token cache
    jwt_cache: Arc<RwLock<FrequencyLRUCache<String, JwtCacheEntry>>>,
    /// API key cache
    api_key_cache: Arc<RwLock<FrequencyLRUCache<String, ApiKeyCacheEntry>>>,
    /// User session cache
    session_cache: Arc<RwLock<FrequencyLRUCache<String, SessionCacheEntry>>>,
    /// Permission cache
    permission_cache: Arc<RwLock<FrequencyLRUCache<String, PermissionCacheEntry>>>,
    /// Cleanup task handle
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

/// JWT cache entry
#[derive(Debug, Clone)]
pub struct JwtCacheEntry {
    pub user_id: String,
    pub permissions: Vec<String>,
    pub expires_at: i64,
}

/// API key cache entry
#[derive(Debug, Clone)]
pub struct ApiKeyCacheEntry {
    pub key_id: String,
    pub user_id: String,
    pub scopes: Vec<String>,
    pub rate_limit: u32,
    pub is_active: bool,
}

/// Session cache entry
#[derive(Debug, Clone)]
pub struct SessionCacheEntry {
    pub session_id: String,
    pub user_id: String,
    pub last_activity: i64,
    pub expires_at: i64,
    pub ip_address: Option<String>,
}

/// Permission cache entry
#[derive(Debug, Clone)]
pub struct PermissionCacheEntry {
    pub allowed: bool,
    pub permissions: Vec<String>,
    pub resource_patterns: Vec<String>,
    pub conditions: Vec<String>,
}

impl AdvancedCacheManager {
    /// Create new advanced cache manager
    pub fn new(config: AdvancedCacheConfig) -> Result<Self, prometheus::Error> {
        let jwt_cache = Arc::new(RwLock::new(FrequencyLRUCache::new(
            config.clone(),
            "jwt".to_string(),
        )?));
        let api_key_cache = Arc::new(RwLock::new(FrequencyLRUCache::new(
            config.clone(),
            "api_key".to_string(),
        )?));
        let session_cache = Arc::new(RwLock::new(FrequencyLRUCache::new(
            config.clone(),
            "session".to_string(),
        )?));
        let permission_cache = Arc::new(RwLock::new(FrequencyLRUCache::new(
            config.clone(),
            "permission".to_string(),
        )?));

        Ok(Self {
            config,
            jwt_cache,
            api_key_cache,
            session_cache,
            permission_cache,
            cleanup_handle: None,
        })
    }

    /// Start background cleanup and maintenance tasks
    pub fn start(&mut self) {
        let jwt_cache = Arc::clone(&self.jwt_cache);
        let api_key_cache = Arc::clone(&self.api_key_cache);
        let session_cache = Arc::clone(&self.session_cache);
        let permission_cache = Arc::clone(&self.permission_cache);
        let cleanup_interval = self.config.frequency_update_interval;

        let handle = tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                // Clean up expired entries
                let mut total_cleaned = 0;
                total_cleaned += jwt_cache.write().cleanup_expired();
                total_cleaned += api_key_cache.write().cleanup_expired();
                total_cleaned += session_cache.write().cleanup_expired();
                total_cleaned += permission_cache.write().cleanup_expired();

                // Apply frequency decay
                jwt_cache.write().apply_frequency_decay();
                api_key_cache.write().apply_frequency_decay();
                session_cache.write().apply_frequency_decay();
                permission_cache.write().apply_frequency_decay();

                if total_cleaned > 0 {
                    info!(
                        "Cache maintenance: cleaned {} expired entries",
                        total_cleaned
                    );
                }
            }
        });

        self.cleanup_handle = Some(handle);
        info!(
            "Advanced cache manager started with {} max entries per cache",
            self.config.max_entries
        );
    }

    /// Stop the cache manager
    pub async fn stop(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
            info!("Advanced cache manager stopped");
        }
    }

    /// Get JWT cache entry
    pub fn get_jwt(&self, token: &str) -> Option<JwtCacheEntry> {
        self.jwt_cache.write().get(&token.to_string())
    }

    /// Cache JWT entry
    pub fn cache_jwt(&self, token: String, entry: JwtCacheEntry) {
        self.jwt_cache.write().insert(token, entry);
    }

    /// Get API key cache entry
    pub fn get_api_key(&self, key: &str) -> Option<ApiKeyCacheEntry> {
        self.api_key_cache.write().get(&key.to_string())
    }

    /// Cache API key entry
    pub fn cache_api_key(&self, key: String, entry: ApiKeyCacheEntry) {
        self.api_key_cache.write().insert(key, entry);
    }

    /// Get session cache entry
    pub fn get_session(&self, session_id: &str) -> Option<SessionCacheEntry> {
        self.session_cache.write().get(&session_id.to_string())
    }

    /// Cache session entry
    pub fn cache_session(&self, session_id: String, entry: SessionCacheEntry) {
        self.session_cache.write().insert(session_id, entry);
    }

    /// Get permission cache entry
    pub fn get_permission(&self, key: &str) -> Option<PermissionCacheEntry> {
        self.permission_cache.write().get(&key.to_string())
    }

    /// Cache permission entry
    pub fn cache_permission(&self, key: String, entry: PermissionCacheEntry) {
        self.permission_cache.write().insert(key, entry);
    }

    /// Get comprehensive cache statistics
    pub fn get_stats(&self) -> HashMap<String, CacheStats> {
        let mut stats = HashMap::new();
        stats.insert("jwt".to_string(), self.jwt_cache.read().stats());
        stats.insert("api_key".to_string(), self.api_key_cache.read().stats());
        stats.insert("session".to_string(), self.session_cache.read().stats());
        stats.insert(
            "permission".to_string(),
            self.permission_cache.read().stats(),
        );
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_frequency_lru_cache_basic() {
        let config = AdvancedCacheConfig {
            max_entries: 3,
            enable_metrics: false,
            ..Default::default()
        };
        let mut cache = FrequencyLRUCache::new(config, "test".to_string()).unwrap();

        // Insert items
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());
        cache.insert("key3".to_string(), "value3".to_string());

        // All should be present
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
        assert_eq!(cache.get(&"key3".to_string()), Some("value3".to_string()));

        // Insert fourth item should evict least frequent
        cache.insert("key4".to_string(), "value4".to_string());

        // key1 should be evicted (lowest frequency)
        assert_eq!(cache.get(&"key1".to_string()), None);
        assert_eq!(cache.get(&"key4".to_string()), Some("value4".to_string()));
    }

    #[test]
    fn test_frequency_based_eviction() {
        let config = AdvancedCacheConfig {
            max_entries: 2,
            frequency_decay: 0.5,
            enable_metrics: false,
            ..Default::default()
        };
        let mut cache = FrequencyLRUCache::new(config, "test".to_string()).unwrap();

        // Insert two items
        cache.insert("frequent".to_string(), "value1".to_string());
        cache.insert("infrequent".to_string(), "value2".to_string());

        // Access frequent item multiple times
        for _ in 0..5 {
            cache.get(&"frequent".to_string());
        }

        // Insert third item should evict infrequent
        cache.insert("new".to_string(), "value3".to_string());

        assert_eq!(
            cache.get(&"frequent".to_string()),
            Some("value1".to_string())
        );
        assert_eq!(cache.get(&"infrequent".to_string()), None);
        assert_eq!(cache.get(&"new".to_string()), Some("value3".to_string()));
    }

    #[tokio::test]
    async fn test_cache_manager() {
        let config = AdvancedCacheConfig {
            max_entries: 100,
            enable_metrics: false,
            ..Default::default()
        };
        let manager = AdvancedCacheManager::new(config).unwrap();

        // Test JWT cache
        let jwt_entry = JwtCacheEntry {
            user_id: "user123".to_string(),
            permissions: vec!["read".to_string(), "write".to_string()],
            expires_at: 1234567890,
        };
        manager.cache_jwt("token123".to_string(), jwt_entry.clone());

        let retrieved = manager.get_jwt("token123").unwrap();
        assert_eq!(retrieved.user_id, jwt_entry.user_id);

        // Test API key cache
        let api_key_entry = ApiKeyCacheEntry {
            key_id: "key123".to_string(),
            user_id: "user123".to_string(),
            scopes: vec!["read".to_string()],
            rate_limit: 1000,
            is_active: true,
        };
        manager.cache_api_key("apikey123".to_string(), api_key_entry.clone());

        let retrieved = manager.get_api_key("apikey123").unwrap();
        assert_eq!(retrieved.key_id, api_key_entry.key_id);
        assert_eq!(retrieved.rate_limit, api_key_entry.rate_limit);

        // Test stats
        let stats = manager.get_stats();
        assert!(stats.contains_key("jwt"));
        assert!(stats.contains_key("api_key"));
        assert_eq!(stats["jwt"].size, 1);
        assert_eq!(stats["api_key"].size, 1);
    }

    #[tokio::test]
    async fn test_adaptive_ttl() {
        let config = AdvancedCacheConfig {
            max_entries: 10,
            adaptive_ttl: true,
            default_ttl: Duration::from_secs(60),
            enable_metrics: false,
            ..Default::default()
        };
        let mut cache = FrequencyLRUCache::new(config, "test".to_string()).unwrap();

        cache.insert("key1".to_string(), "value1".to_string());

        // Access multiple times to increase frequency
        for _ in 0..10 {
            cache.get(&"key1".to_string());
            sleep(Duration::from_millis(10)).await;
        }

        // High frequency should result in longer TTL
        if let Some(entry) = cache.entries.get(&"key1".to_string()) {
            let adaptive_ttl = entry.calculate_adaptive_ttl(Duration::from_secs(60));
            assert!(adaptive_ttl > Duration::from_secs(60));
        }
    }

    // ── CacheEntry pure logic ───────────────────────────────────────

    #[test]
    fn cache_entry_new_and_access_frequency() {
        let mut entry = CacheEntry::new("v", Duration::from_secs(60), 0);
        assert_eq!(entry.access_count, 1);
        assert_eq!(entry.frequency, 1.0);
        assert!(!entry.is_expired());

        // Each access applies decay_factor and adds 1: 1*0.5+1 = 1.5.
        entry.access(0.5);
        assert_eq!(entry.access_count, 2);
        assert!((entry.frequency - 1.5).abs() < 1e-9);
        entry.access(0.5);
        assert!((entry.frequency - 1.75).abs() < 1e-9);
    }

    #[test]
    fn cache_entry_adaptive_ttl_uses_base_below_two_accesses() {
        let entry = CacheEntry::new("v", Duration::from_secs(60), 0);
        assert_eq!(
            entry.calculate_adaptive_ttl(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn cache_entry_refresh_ttl_extends_expiry() {
        let mut entry = CacheEntry::new("v", Duration::from_millis(5), 0);
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(entry.is_expired(), "5ms TTL must lapse");
        entry.refresh_ttl(Duration::from_secs(60));
        assert!(!entry.is_expired(), "refreshed TTL must not be expired");
    }

    #[test]
    fn ordered_frequency_orders_by_frequency_then_seq() {
        use std::cmp::Ordering;
        let low_high = OrderedFrequency {
            frequency: 1.0,
            seq: 5,
        };
        let high = OrderedFrequency {
            frequency: 9.0,
            seq: 1,
        };
        assert_eq!(low_high.cmp(&high), Ordering::Less);

        // Equal frequency → lower seq (older recency) sorts first.
        let older = OrderedFrequency {
            frequency: 2.0,
            seq: 1,
        };
        let newer = OrderedFrequency {
            frequency: 2.0,
            seq: 2,
        };
        assert_eq!(older.cmp(&newer), Ordering::Less);
        assert_eq!(newer.cmp(&older), Ordering::Greater);
        assert_eq!(newer.cmp(&newer), Ordering::Equal);
    }
}
