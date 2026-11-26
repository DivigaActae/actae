// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use lru::LruCache;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use crate::auth::websocket_auth::AuthResult as WebSocketAuthResult;

/// Advanced security cache with predictive prefetching and intelligent eviction
pub struct AdvancedSecurityCache {
    // Multi-tier caching strategy
    l1_cache: Arc<RwLock<LruCache<String, CachedEntry>>>, // In-memory hot cache
    l2_cache: Arc<DashMap<String, CachedEntry>>,          // Larger in-memory cache

    // Predictive caching
    access_patterns: Arc<RwLock<HashMap<String, AccessPattern>>>,
    prefetch_queue: Arc<RwLock<VecDeque<PrefetchTask>>>,

    // Intelligent eviction
    usage_stats: Arc<DashMap<String, UsageStats>>,
    eviction_scorer: Arc<EvictionScorer>,

    // Configuration
    config: AdvancedCacheConfig,

    // Metrics
    metrics: Arc<CacheMetrics>,
}

#[derive(Debug, Clone)]
pub struct AdvancedCacheConfig {
    pub l1_cache_size: usize,
    pub l2_cache_size: usize,
    pub prefetch_threshold: f64, // Access frequency threshold for prefetching
    pub prefetch_window_seconds: u64, // Time window for pattern analysis
    pub cleanup_interval_seconds: u64,
    pub enable_predictive_prefetch: bool,
    pub enable_intelligent_eviction: bool,
    pub compression_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct CachedEntry {
    pub data: CachedData,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub size_bytes: usize,
    pub prediction_score: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CachedData {
    Auth(WebSocketAuthResult),
    Permission(bool),
    RateLimit(RateLimitData),
    Custom(sonic_rs::Value),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RateLimitData {
    pub current_count: u32,
    pub window_start_timestamp: u64,          // Unix timestamp
    pub blocked_until_timestamp: Option<u64>, // Unix timestamp
}

#[derive(Debug, Clone)]
pub struct AccessPattern {
    pub key_prefix: String,
    pub access_times: VecDeque<Instant>,
    pub frequency: f64,
    pub trend: Trend,
    pub next_predicted_access: Option<Instant>,
}

#[derive(Debug, Clone)]
pub enum Trend {
    Increasing,
    Decreasing,
    Stable,
    Seasonal,
}

#[derive(Debug, Clone)]
pub struct PrefetchTask {
    pub key: String,
    pub priority: f64,
    pub estimated_size: usize,
    pub created_at: Instant,
}

#[derive(Debug)]
pub struct UsageStats {
    pub total_accesses: AtomicU64,
    pub recent_accesses: AtomicU64,
    pub last_access: AtomicU64, // Timestamp
    pub avg_response_time_ns: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
}

pub struct EvictionScorer {
    // Weights for different factors in eviction scoring
    pub age_weight: f64,
    pub frequency_weight: f64,
    pub size_weight: f64,
    pub prediction_weight: f64,
}

#[derive(Debug, Default)]
pub struct CacheMetrics {
    pub l1_hits: AtomicU64,
    pub l1_misses: AtomicU64,
    pub l2_hits: AtomicU64,
    pub l2_misses: AtomicU64,
    pub prefetch_hits: AtomicU64,
    pub prefetch_misses: AtomicU64,
    pub evictions: AtomicU64,
    pub prefetch_tasks_queued: AtomicU64,
    pub prefetch_tasks_completed: AtomicU64,
    pub total_memory_bytes: AtomicU64,
}

impl Clone for CacheMetrics {
    fn clone(&self) -> Self {
        Self {
            l1_hits: AtomicU64::new(self.l1_hits.load(Ordering::Relaxed)),
            l1_misses: AtomicU64::new(self.l1_misses.load(Ordering::Relaxed)),
            l2_hits: AtomicU64::new(self.l2_hits.load(Ordering::Relaxed)),
            l2_misses: AtomicU64::new(self.l2_misses.load(Ordering::Relaxed)),
            prefetch_hits: AtomicU64::new(self.prefetch_hits.load(Ordering::Relaxed)),
            prefetch_misses: AtomicU64::new(self.prefetch_misses.load(Ordering::Relaxed)),
            evictions: AtomicU64::new(self.evictions.load(Ordering::Relaxed)),
            prefetch_tasks_queued: AtomicU64::new(
                self.prefetch_tasks_queued.load(Ordering::Relaxed),
            ),
            prefetch_tasks_completed: AtomicU64::new(
                self.prefetch_tasks_completed.load(Ordering::Relaxed),
            ),
            total_memory_bytes: AtomicU64::new(self.total_memory_bytes.load(Ordering::Relaxed)),
        }
    }
}

impl Default for AdvancedCacheConfig {
    fn default() -> Self {
        Self {
            l1_cache_size: 1000,
            l2_cache_size: 10000,
            prefetch_threshold: 0.7,
            prefetch_window_seconds: 300,
            cleanup_interval_seconds: 60,
            enable_predictive_prefetch: true,
            enable_intelligent_eviction: true,
            compression_enabled: true,
        }
    }
}

impl Default for EvictionScorer {
    fn default() -> Self {
        Self {
            age_weight: 0.3,
            frequency_weight: 0.4,
            size_weight: 0.2,
            prediction_weight: 0.1,
        }
    }
}

impl AdvancedSecurityCache {
    pub fn new(config: AdvancedCacheConfig) -> Self {
        let cache = Self {
            l1_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(config.l1_cache_size).unwrap(),
            ))),
            l2_cache: Arc::new(DashMap::new()),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            prefetch_queue: Arc::new(RwLock::new(VecDeque::new())),
            usage_stats: Arc::new(DashMap::new()),
            eviction_scorer: Arc::new(EvictionScorer::default()),
            config,
            metrics: Arc::new(CacheMetrics::default()),
        };

        cache.start_background_tasks();

        cache
    }

    fn start_background_tasks(&self) {
        // Prefetch worker
        if self.config.enable_predictive_prefetch {
            let cache_clone = self.clone_for_worker();
            tokio::spawn(async move {
                cache_clone.prefetch_worker().await;
            });
        }

        // Cleanup worker
        let cache_clone = self.clone_for_worker();
        tokio::spawn(async move {
            cache_clone.cleanup_worker().await;
        });

        // Pattern analysis worker
        let cache_clone = self.clone_for_worker();
        tokio::spawn(async move {
            cache_clone.pattern_analysis_worker().await;
        });
    }

    fn clone_for_worker(&self) -> AdvancedSecurityCache {
        Self {
            l1_cache: Arc::clone(&self.l1_cache),
            l2_cache: Arc::clone(&self.l2_cache),
            access_patterns: Arc::clone(&self.access_patterns),
            prefetch_queue: Arc::clone(&self.prefetch_queue),
            usage_stats: Arc::clone(&self.usage_stats),
            eviction_scorer: Arc::clone(&self.eviction_scorer),
            config: self.config.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Get cached entry with intelligent tier checking
    pub async fn get(&self, key: &str) -> Option<CachedData> {
        let start_time = Instant::now();

        // Record access pattern
        self.record_access_pattern(key);

        // L1 cache check (hot cache)
        if let Some(entry) = self.l1_cache.write().get_mut(key) {
            entry.last_accessed = Instant::now();
            entry.access_count += 1;
            self.metrics.l1_hits.fetch_add(1, Ordering::Relaxed);
            self.update_usage_stats(key, start_time, true);
            return Some(entry.data.clone());
        }
        self.metrics.l1_misses.fetch_add(1, Ordering::Relaxed);

        // L2 cache check (larger in-memory)
        if let Some(mut entry) = self.l2_cache.get_mut(key) {
            entry.last_accessed = Instant::now();
            entry.access_count += 1;

            // Promote to L1 if hot
            if self.should_promote_to_l1(&entry) {
                self.l1_cache.write().put(key.to_string(), entry.clone());
            }

            self.metrics.l2_hits.fetch_add(1, Ordering::Relaxed);
            self.update_usage_stats(key, start_time, true);
            return Some(entry.data.clone());
        }
        self.metrics.l2_misses.fetch_add(1, Ordering::Relaxed);

        // Schedule prefetch if pattern suggests future access
        if self.config.enable_predictive_prefetch {
            self.consider_prefetch(key);
        }

        self.update_usage_stats(key, start_time, false);
        None
    }

    /// Cache data with intelligent placement
    pub async fn set(&self, key: &str, data: CachedData, _ttl: Option<Duration>) {
        let entry = CachedEntry {
            data: data.clone(),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
            size_bytes: self.estimate_size(&data),
            prediction_score: self.calculate_prediction_score(key),
        };

        // Always cache in L2
        self.l2_cache.insert(key.to_string(), entry.clone());

        let entry_size = entry.size_bytes;

        // Cache in L1 if it meets promotion criteria
        if self.should_promote_to_l1(&entry) {
            // Check if L1 needs eviction
            if self.l1_cache.read().len() >= self.config.l1_cache_size {
                self.intelligent_evict_l1();
            }
            self.l1_cache.write().put(key.to_string(), entry);
        }

        // Update memory usage
        self.metrics
            .total_memory_bytes
            .fetch_add(entry_size as u64, Ordering::Relaxed);
    }

    fn should_promote_to_l1(&self, entry: &CachedEntry) -> bool {
        // Promote based on access frequency and prediction score
        entry.access_count > 2
            || entry.prediction_score > 0.7
            || entry.last_accessed.elapsed() < Duration::from_secs(60)
    }

    fn intelligent_evict_l1(&self) {
        if !self.config.enable_intelligent_eviction {
            return;
        }

        let mut l1_cache = self.l1_cache.write();
        if l1_cache.len() < l1_cache.cap().get() {
            return;
        }

        // Find best candidate for eviction based on scoring
        let mut best_score = f64::MAX;
        let mut best_key = None;

        for (key, entry) in l1_cache.iter() {
            let score = self.calculate_eviction_score(entry);
            if score < best_score {
                best_score = score;
                best_key = Some(key.clone());
            }
        }

        if let Some(key) = best_key {
            l1_cache.pop(&key);
            self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            debug!(
                "Intelligently evicted key '{}' with score {:.2}",
                key, best_score
            );
        }
    }

    fn calculate_eviction_score(&self, entry: &CachedEntry) -> f64 {
        let age_score = entry.created_at.elapsed().as_secs_f64() / 3600.0; // Hours
        let frequency_score = 1.0 / (entry.access_count as f64 + 1.0);
        let size_score = entry.size_bytes as f64 / 1024.0; // KB
        let prediction_score = 1.0 - entry.prediction_score;

        (age_score * self.eviction_scorer.age_weight)
            + (frequency_score * self.eviction_scorer.frequency_weight)
            + (size_score * self.eviction_scorer.size_weight)
            + (prediction_score * self.eviction_scorer.prediction_weight)
    }

    fn record_access_pattern(&self, key: &str) {
        let key_prefix = self.extract_key_prefix(key);
        let now = Instant::now();

        let mut patterns = self.access_patterns.write();
        let pattern = patterns
            .entry(key_prefix.clone())
            .or_insert_with(|| AccessPattern {
                key_prefix,
                access_times: VecDeque::new(),
                frequency: 0.0,
                trend: Trend::Stable,
                next_predicted_access: None,
            });

        pattern.access_times.push_back(now);

        // Keep only recent accesses
        let cutoff = now - Duration::from_secs(self.config.prefetch_window_seconds);
        while pattern.access_times.front().is_some_and(|&t| t < cutoff) {
            pattern.access_times.pop_front();
        }

        // Calculate frequency
        pattern.frequency =
            pattern.access_times.len() as f64 / self.config.prefetch_window_seconds as f64;

        // Analyze trend
        pattern.trend = self.analyze_trend(&pattern.access_times);

        // Predict next access
        pattern.next_predicted_access =
            self.predict_next_access(&pattern.access_times, &pattern.trend);
    }

    fn extract_key_prefix(&self, key: &str) -> String {
        // Extract meaningful prefix for pattern analysis
        if let Some(pos) = key.find(':') {
            key[..pos].to_string()
        } else {
            key.to_string()
        }
    }

    fn analyze_trend(&self, access_times: &VecDeque<Instant>) -> Trend {
        if access_times.len() < 3 {
            return Trend::Stable;
        }

        let intervals: Vec<Duration> = access_times
            .iter()
            .zip(access_times.iter().skip(1))
            .map(|(a, b)| b.duration_since(*a))
            .collect();

        if intervals.is_empty() {
            return Trend::Stable;
        }

        let avg_interval =
            intervals.iter().map(|d| d.as_secs_f64()).sum::<f64>() / intervals.len() as f64;
        let recent_interval = intervals.last().unwrap().as_secs_f64();

        if recent_interval < avg_interval * 0.8 {
            Trend::Increasing
        } else if recent_interval > avg_interval * 1.2 {
            Trend::Decreasing
        } else {
            Trend::Stable
        }
    }

    fn predict_next_access(
        &self,
        access_times: &VecDeque<Instant>,
        trend: &Trend,
    ) -> Option<Instant> {
        if access_times.len() < 2 {
            return None;
        }

        let last_access = *access_times.back()?;
        let intervals: Vec<Duration> = access_times
            .iter()
            .zip(access_times.iter().skip(1))
            .map(|(a, b)| b.duration_since(*a))
            .collect();

        if intervals.is_empty() {
            return None;
        }

        let avg_interval =
            intervals.iter().map(|d| d.as_secs_f64()).sum::<f64>() / intervals.len() as f64;

        let predicted_interval = match trend {
            Trend::Increasing => avg_interval * 0.8, // Faster access
            Trend::Decreasing => avg_interval * 1.2, // Slower access
            Trend::Stable => avg_interval,
            Trend::Seasonal => {
                // Implement seasonal prediction based on time of day patterns
                self.calculate_seasonal_prediction(access_times, avg_interval)
            }
        };

        Some(last_access + Duration::from_secs_f64(predicted_interval))
    }

    fn calculate_seasonal_prediction(
        &self,
        access_times: &VecDeque<Instant>,
        avg_interval: f64,
    ) -> f64 {
        if access_times.len() < 4 {
            return avg_interval;
        }

        // Analyze hourly patterns
        let now = Instant::now();
        let mut hourly_intervals: [Vec<f64>; 24] = Default::default();

        // Group intervals by hour of day (approximated)
        let intervals: Vec<Duration> = access_times
            .iter()
            .zip(access_times.iter().skip(1))
            .map(|(a, b)| b.duration_since(*a))
            .collect();

        for (i, &interval) in intervals.iter().enumerate() {
            if let Some(access_time) = access_times.get(i + 1) {
                // Approximate hour by taking modulo of elapsed milliseconds
                let elapsed_ms = now.duration_since(*access_time).as_millis() as u64;
                let approx_hour = ((elapsed_ms / (1000 * 60 * 60)) % 24) as usize;
                hourly_intervals[approx_hour].push(interval.as_secs_f64());
            }
        }

        // Calculate current hour and predict based on historical pattern
        let current_hour = {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            ((now_ms / (1000 * 60 * 60)) % 24) as usize
        };

        // Use historical data for current hour, or fall back to average
        if !hourly_intervals[current_hour].is_empty() {
            let hour_avg = hourly_intervals[current_hour].iter().sum::<f64>()
                / hourly_intervals[current_hour].len() as f64;

            // Apply seasonal factor (shorter intervals during business hours)
            match current_hour {
                9..=17 => hour_avg * 0.8,          // Business hours - more frequent access
                0..=6 | 22..=23 => hour_avg * 1.5, // Night hours - less frequent access
                _ => hour_avg * 1.0,               // Other hours - normal access
            }
        } else {
            // No historical data for this hour, use time-of-day heuristics
            match current_hour {
                9..=17 => avg_interval * 0.8,          // Business hours
                0..=6 | 22..=23 => avg_interval * 1.5, // Night hours
                _ => avg_interval,                     // Other hours
            }
        }
    }

    fn consider_prefetch(&self, key: &str) {
        let key_prefix = self.extract_key_prefix(key);
        let patterns = self.access_patterns.read();

        if let Some(pattern) = patterns.get(&key_prefix) {
            if pattern.frequency > self.config.prefetch_threshold {
                let task = PrefetchTask {
                    key: key.to_string(),
                    priority: pattern.frequency,
                    estimated_size: 1024, // Estimate
                    created_at: Instant::now(),
                };

                self.prefetch_queue.write().push_back(task);
                self.metrics
                    .prefetch_tasks_queued
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn calculate_prediction_score(&self, key: &str) -> f64 {
        let key_prefix = self.extract_key_prefix(key);
        let patterns = self.access_patterns.read();

        patterns
            .get(&key_prefix)
            .map(|p| p.frequency.min(1.0))
            .unwrap_or(0.1)
    }

    fn estimate_size(&self, data: &CachedData) -> usize {
        match data {
            CachedData::Auth(_) => 512,      // Estimate for auth data
            CachedData::Permission(_) => 64, // Small boolean data
            CachedData::RateLimit(_) => 128, // Rate limit data
            CachedData::Custom(value) => {
                // Rough JSON size estimation
                sonic_rs::to_string(value).map(|s| s.len()).unwrap_or(256)
            }
        }
    }

    fn update_usage_stats(&self, key: &str, start_time: Instant, hit: bool) {
        let stats = self
            .usage_stats
            .entry(key.to_string())
            .or_insert_with(|| UsageStats {
                total_accesses: AtomicU64::new(0),
                recent_accesses: AtomicU64::new(0),
                last_access: AtomicU64::new(0),
                avg_response_time_ns: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
            });

        let response_time_ns = start_time.elapsed().as_nanos() as u64;

        stats.total_accesses.fetch_add(1, Ordering::Relaxed);
        stats.recent_accesses.fetch_add(1, Ordering::Relaxed);
        stats.last_access.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );

        // Update average response time (exponential moving average)
        let current_avg = stats.avg_response_time_ns.load(Ordering::Relaxed);
        let new_avg = if current_avg == 0 {
            response_time_ns
        } else {
            (current_avg * 9 + response_time_ns) / 10
        };
        stats.avg_response_time_ns.store(new_avg, Ordering::Relaxed);

        if hit {
            stats.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn prefetch_worker(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            // Process prefetch queue
            let task = {
                let mut queue = self.prefetch_queue.write();
                queue.pop_front()
            };

            if let Some(task) = task {
                // Execute prefetch
                if let Some(_data) = self.get(&task.key).await {
                    self.metrics.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.metrics.prefetch_misses.fetch_add(1, Ordering::Relaxed);
                }

                self.metrics
                    .prefetch_tasks_completed
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    "Completed prefetch for key '{}' with priority {:.2}",
                    task.key, task.priority
                );
            }
        }
    }

    async fn cleanup_worker(&self) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.cleanup_interval_seconds));

        loop {
            interval.tick().await;

            // Clean up expired L2 entries
            let now = Instant::now();
            let expiry_threshold = Duration::from_secs(3600); // 1 hour

            let initial_l2_size = self.l2_cache.len();
            self.l2_cache
                .retain(|_key, entry| now.duration_since(entry.last_accessed) < expiry_threshold);
            let l2_removed = initial_l2_size - self.l2_cache.len();

            // Clean up old usage stats
            let initial_stats_size = self.usage_stats.len();
            self.usage_stats.retain(|_key, stats| {
                let last_access = stats.last_access.load(Ordering::Relaxed);
                let last_access_time = UNIX_EPOCH + Duration::from_secs(last_access);
                SystemTime::now()
                    .duration_since(last_access_time)
                    .unwrap_or_default()
                    < Duration::from_secs(3600)
            });
            let stats_removed = initial_stats_size - self.usage_stats.len();

            // Clean up old access patterns
            let patterns_removed = {
                let mut patterns = self.access_patterns.write();
                let initial_patterns_size = patterns.len();
                patterns.retain(|_prefix, pattern| {
                    !pattern.access_times.is_empty()
                        && now.duration_since(*pattern.access_times.back().unwrap())
                            < Duration::from_secs(self.config.prefetch_window_seconds * 2)
                });
                initial_patterns_size - patterns.len()
            };

            // Only log when there's actual cleanup activity to reduce log spam
            if l2_removed > 0 || stats_removed > 0 || patterns_removed > 0 {
                info!(
                    "Cache cleanup cycle completed - L2 entries removed: {}, stats removed: {}, patterns removed: {}",
                    l2_removed, stats_removed, patterns_removed
                );
            } else {
                debug!("Cache cleanup cycle completed - no items removed");
            }
        }
    }

    async fn pattern_analysis_worker(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(60)); // Analyze every minute

        loop {
            interval.tick().await;

            // Analyze patterns and update predictions
            let mut patterns = self.access_patterns.write();
            for pattern in patterns.values_mut() {
                pattern.trend = self.analyze_trend(&pattern.access_times);
                pattern.next_predicted_access =
                    self.predict_next_access(&pattern.access_times, &pattern.trend);
            }

            debug!(
                "Completed pattern analysis cycle for {} patterns",
                patterns.len()
            );
        }
    }

    /// Get comprehensive cache metrics
    pub fn get_metrics(&self) -> CacheMetrics {
        // Clone the metrics (atomic operations)
        CacheMetrics {
            l1_hits: AtomicU64::new(self.metrics.l1_hits.load(Ordering::Relaxed)),
            l1_misses: AtomicU64::new(self.metrics.l1_misses.load(Ordering::Relaxed)),
            l2_hits: AtomicU64::new(self.metrics.l2_hits.load(Ordering::Relaxed)),
            l2_misses: AtomicU64::new(self.metrics.l2_misses.load(Ordering::Relaxed)),
            prefetch_hits: AtomicU64::new(self.metrics.prefetch_hits.load(Ordering::Relaxed)),
            prefetch_misses: AtomicU64::new(self.metrics.prefetch_misses.load(Ordering::Relaxed)),
            evictions: AtomicU64::new(self.metrics.evictions.load(Ordering::Relaxed)),
            prefetch_tasks_queued: AtomicU64::new(
                self.metrics.prefetch_tasks_queued.load(Ordering::Relaxed),
            ),
            prefetch_tasks_completed: AtomicU64::new(
                self.metrics
                    .prefetch_tasks_completed
                    .load(Ordering::Relaxed),
            ),
            total_memory_bytes: AtomicU64::new(
                self.metrics.total_memory_bytes.load(Ordering::Relaxed),
            ),
        }
    }

    /// Get cache hit ratios
    pub fn get_hit_ratios(&self) -> (f64, f64) {
        let l1_hits = self.metrics.l1_hits.load(Ordering::Relaxed) as f64;
        let l1_misses = self.metrics.l1_misses.load(Ordering::Relaxed) as f64;
        let l2_hits = self.metrics.l2_hits.load(Ordering::Relaxed) as f64;
        let l2_misses = self.metrics.l2_misses.load(Ordering::Relaxed) as f64;

        let l1_ratio = if l1_hits + l1_misses > 0.0 {
            l1_hits / (l1_hits + l1_misses)
        } else {
            0.0
        };
        let l2_ratio = if l2_hits + l2_misses > 0.0 {
            l2_hits / (l2_hits + l2_misses)
        } else {
            0.0
        };

        (l1_ratio, l2_ratio)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_data() {
        let data = RateLimitData {
            current_count: 50,
            window_start_timestamp: 1000,
            blocked_until_timestamp: None,
        };
        assert_eq!(data.current_count, 50);
    }

    #[test]
    fn test_rate_limit_data_blocked() {
        let data = RateLimitData {
            current_count: 100,
            window_start_timestamp: 1000,
            blocked_until_timestamp: Some(2000),
        };
        assert!(data.blocked_until_timestamp.is_some());
    }

    #[test]
    fn test_cached_entry_permission() {
        let entry = CachedEntry {
            data: CachedData::Permission(true),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
            size_bytes: 10,
            prediction_score: 0.0,
        };
        assert!(matches!(entry.data, CachedData::Permission(true)));
    }

    #[test]
    fn test_cached_entry_with_custom_data() {
        let entry = CachedEntry {
            data: CachedData::Custom(sonic_rs::json!({"key": "value"})),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 5,
            size_bytes: 100,
            prediction_score: 0.8,
        };
        assert_eq!(entry.access_count, 5);
    }

    #[test]
    fn test_cache_metrics_default() {
        let m = CacheMetrics::default();
        assert_eq!(m.l1_hits.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_trend_variants() {
        assert!(matches!(Trend::Increasing, Trend::Increasing));
        assert!(matches!(Trend::Stable, Trend::Stable));
        assert!(matches!(Trend::Decreasing, Trend::Decreasing));
        assert!(matches!(Trend::Seasonal, Trend::Seasonal));
    }

    // ── pure key/size/eviction helpers ─────────────────────────────

    fn cache() -> AdvancedSecurityCache {
        AdvancedSecurityCache::new(AdvancedCacheConfig::default())
    }

    #[tokio::test]
    async fn extract_key_prefix_splits_on_colon() {
        let c = cache();
        assert_eq!(c.extract_key_prefix("read:ch-1"), "read");
        assert_eq!(c.extract_key_prefix("rate:127.0.0.1"), "rate");
        assert_eq!(c.extract_key_prefix("no-colon-key"), "no-colon-key");
        assert_eq!(c.extract_key_prefix(""), "");
    }

    #[tokio::test]
    async fn estimate_size_accounts_for_data() {
        let c = cache();
        let small = c.estimate_size(&CachedData::Permission(true));
        assert!(small > 0);
        let bigger = c.estimate_size(&CachedData::Custom(sonic_rs::json!({
            "blob": "x".repeat(10_000)
        })));
        assert!(bigger > small, "larger payload must estimate larger");
    }

    #[tokio::test]
    async fn eviction_score_prefers_older_larger_less_accessed() {
        let c = cache();
        let old = CachedEntry {
            data: CachedData::Permission(true),
            created_at: Instant::now() - std::time::Duration::from_secs(7200), // 2h old
            last_accessed: Instant::now(),
            access_count: 1,
            size_bytes: 4096,
            prediction_score: 0.0,
        };
        let fresh = CachedEntry {
            data: CachedData::Permission(true),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 10,
            size_bytes: 128,
            prediction_score: 1.0,
        };
        let old_score = c.calculate_eviction_score(&old);
        let fresh_score = c.calculate_eviction_score(&fresh);
        assert!(
            old_score > fresh_score,
            "old/large/infrequent entry must score higher for eviction"
        );
    }

    #[tokio::test]
    async fn trend_detection_requires_three_samples() {
        let c = cache();
        let few = VecDeque::from(vec![Instant::now(), Instant::now()]);
        assert!(matches!(c.analyze_trend(&few), Trend::Stable));
        let empty = VecDeque::new();
        assert!(matches!(c.analyze_trend(&empty), Trend::Stable));
    }
}
