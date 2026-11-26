// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::interval;
use tracing::error;

use super::SecurityEvent;
use crate::auth::websocket_auth::AuthMethod;

/// Advanced threat detection and automated mitigation system
pub struct ThreatDetectionSystem {
    // Core components
    anomaly_detector: Arc<AnomalyDetector>,
    behavioral_analyzer: Arc<BehavioralAnalyzer>,
    attack_pattern_detector: Arc<AttackPatternDetector>,
    mitigation_engine: Arc<MitigationEngine>,

    // Configuration
    config: ThreatDetectionConfig,

    // Monitoring and metrics
    metrics: Arc<ThreatDetectionMetrics>,
}

#[derive(Debug, Clone)]
pub struct ThreatDetectionConfig {
    pub enable_anomaly_detection: bool,
    pub enable_behavioral_analysis: bool,
    pub enable_attack_pattern_detection: bool,
    pub enable_automated_mitigation: bool,

    // Thresholds
    pub anomaly_threshold: f64,
    pub behavioral_deviation_threshold: f64,
    pub attack_confidence_threshold: f64,

    // Time windows
    pub analysis_window_seconds: u64,
    pub baseline_learning_hours: u64,
    pub threat_cooldown_minutes: u64,

    // Mitigation settings
    pub auto_block_duration_minutes: u64,
    pub escalation_threshold: u32,
    pub max_concurrent_threats: u32,
}

/// Detects statistical anomalies in connection and message patterns
pub struct AnomalyDetector {}

#[derive(Debug, Clone)]
pub struct AnomalyDetectionConfig {
    pub learning_period_hours: u64,
    pub sensitivity: f64,
    pub min_samples: u32,
    pub metrics_retention_hours: u64,
}

#[derive(Debug)]
pub struct BaselineStats {
    pub connections_per_hour: StatisticalBaseline,
    pub messages_per_minute: StatisticalBaseline,
    pub auth_failures_per_hour: StatisticalBaseline,
    pub unique_ips_per_hour: StatisticalBaseline,
    pub last_updated: SystemTime,
}

impl Default for BaselineStats {
    fn default() -> Self {
        Self {
            connections_per_hour: StatisticalBaseline::default(),
            messages_per_minute: StatisticalBaseline::default(),
            auth_failures_per_hour: StatisticalBaseline::default(),
            unique_ips_per_hour: StatisticalBaseline::default(),
            last_updated: SystemTime::now(),
        }
    }
}

#[derive(Debug, Default)]
pub struct StatisticalBaseline {
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub sample_count: u64,
}

#[derive(Debug)]
pub struct MetricWindow {
    pub values: VecDeque<(Instant, f64)>,
    pub window_duration: Duration,
}

/// Analyzes user and IP behavioral patterns
pub struct BehavioralAnalyzer {}

#[derive(Debug, Clone)]
pub struct BehavioralAnalysisConfig {
    pub profile_learning_days: u64,
    pub behavior_deviation_threshold: f64,
    pub min_activity_threshold: u32,
    pub profile_retention_days: u64,
}

#[derive(Debug)]
pub struct UserBehaviorProfile {
    pub user_id: String,
    pub typical_activity_hours: Vec<f64>, // 24-hour activity pattern
    pub avg_session_duration: Duration,
    pub typical_message_rate: f64,
    pub common_topics: HashMap<String, f64>,
    pub auth_methods: HashMap<AuthMethod, u32>,
    pub last_seen: Instant,
    pub anomaly_score: f64,
}

#[derive(Debug)]
pub struct IpBehaviorProfile {
    pub ip_address: IpAddr,
    pub connection_patterns: ConnectionPattern,
    pub geographic_consistency: GeographicData,
    pub user_agent_patterns: Vec<String>,
    pub threat_score: f64,
    pub last_activity: Instant,
}

#[derive(Debug)]
pub struct ConnectionPattern {
    pub avg_connections_per_hour: f64,
    pub typical_session_lengths: Vec<Duration>,
    pub connection_timing_pattern: Vec<f64>, // 24-hour pattern
    pub concurrent_connection_max: u32,
}

#[derive(Debug)]
pub struct GeographicData {
    pub typical_locations: Vec<String>,
    pub location_changes: u32,
    pub suspicious_location_changes: u32,
}

/// Detects known attack patterns and signatures
pub struct AttackPatternDetector {
    active_attacks: Arc<DashMap<String, OngoingAttack>>,
}

#[derive(Debug, Clone)]
pub struct AttackDetectionConfig {
    pub signature_update_interval_hours: u64,
    pub attack_correlation_window_minutes: u64,
    pub min_attack_confidence: f64,
    pub max_active_attacks: u32,
}

#[derive(Debug, Clone)]
pub struct AttackSignature {
    pub signature_id: String,
    pub name: String,
    pub description: String,
    pub pattern: AttackPattern,
    pub severity: ThreatSeverity,
    pub confidence_threshold: f64,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub enum AttackPattern {
    BruteForce {
        min_attempts: u32,
        time_window: Duration,
        failure_rate: f64,
    },
    DdosDetection {
        connection_rate: u32,
        time_window: Duration,
        unique_ip_threshold: u32,
    },
    CredentialStuffing {
        user_enumeration: bool,
        password_spraying: bool,
        success_rate_threshold: f64,
    },
    SqlInjection {
        patterns: Vec<String>,
        payload_analysis: bool,
    },
    XssAttempt {
        script_patterns: Vec<String>,
        encoding_detection: bool,
    },
    AnomalousAccess {
        deviation_threshold: f64,
        baseline_comparison: bool,
    },
}

#[derive(Debug, Clone)]
pub enum ThreatSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug)]
pub struct OngoingAttack {
    pub attack_id: String,
    pub signature_id: String,
    pub started_at: Instant,
    pub last_activity: Instant,
    pub confidence_score: f64,
    pub affected_targets: Vec<String>,
    pub source_ips: Vec<IpAddr>,
    pub event_count: u32,
    pub mitigation_applied: bool,
}

/// Automated threat mitigation and response
pub struct MitigationEngine {}

#[derive(Debug, Clone)]
pub struct MitigationConfig {
    pub enable_auto_blocking: bool,
    pub enable_rate_limiting: bool,
    pub enable_quarantine: bool,
    pub enable_notification: bool,

    pub block_duration_minutes: u64,
    pub rate_limit_factor: f64,
    pub quarantine_duration_hours: u64,
    pub escalation_levels: u32,
}

#[derive(Debug)]
pub struct ActiveMitigation {
    pub mitigation_id: String,
    pub threat_id: String,
    pub mitigation_type: MitigationType,
    pub targets: Vec<String>,
    pub applied_at: Instant,
    pub expires_at: Option<Instant>,
    pub effectiveness_score: f64,
}

#[derive(Debug, Clone)]
pub enum MitigationType {
    IpBlocking { duration: Duration },
    RateLimiting { factor: f64, duration: Duration },
    UserQuarantine { duration: Duration },
    ConnectionThrottling { max_connections: u32 },
    ContentFiltering { patterns: Vec<String> },
    GeoBlocking { blocked_regions: Vec<String> },
}

#[derive(Debug)]
pub struct MitigationEvent {
    pub event_id: String,
    pub timestamp: SystemTime,
    pub mitigation_type: MitigationType,
    pub trigger_threat: String,
    pub targets_affected: Vec<String>,
    pub success: bool,
    pub details: String,
}

#[derive(Debug, Default)]
pub struct ThreatDetectionMetrics {
    pub anomalies_detected: AtomicU64,
    pub behavioral_violations: AtomicU64,
    pub attacks_detected: AtomicU64,
    pub attacks_mitigated: AtomicU64,
    pub false_positives: AtomicU64,
    pub mitigation_success_rate: AtomicU64,
    pub avg_detection_time_ms: AtomicU64,
    pub avg_mitigation_time_ms: AtomicU64,
}

impl Clone for ThreatDetectionMetrics {
    fn clone(&self) -> Self {
        Self {
            anomalies_detected: AtomicU64::new(self.anomalies_detected.load(Ordering::Relaxed)),
            behavioral_violations: AtomicU64::new(
                self.behavioral_violations.load(Ordering::Relaxed),
            ),
            attacks_detected: AtomicU64::new(self.attacks_detected.load(Ordering::Relaxed)),
            attacks_mitigated: AtomicU64::new(self.attacks_mitigated.load(Ordering::Relaxed)),
            false_positives: AtomicU64::new(self.false_positives.load(Ordering::Relaxed)),
            mitigation_success_rate: AtomicU64::new(
                self.mitigation_success_rate.load(Ordering::Relaxed),
            ),
            avg_detection_time_ms: AtomicU64::new(
                self.avg_detection_time_ms.load(Ordering::Relaxed),
            ),
            avg_mitigation_time_ms: AtomicU64::new(
                self.avg_mitigation_time_ms.load(Ordering::Relaxed),
            ),
        }
    }
}

impl Default for ThreatDetectionConfig {
    fn default() -> Self {
        Self {
            enable_anomaly_detection: true,
            enable_behavioral_analysis: true,
            enable_attack_pattern_detection: true,
            enable_automated_mitigation: true,
            anomaly_threshold: 3.0, // 3 standard deviations
            behavioral_deviation_threshold: 2.5,
            attack_confidence_threshold: 0.8,
            analysis_window_seconds: 300, // 5 minutes
            baseline_learning_hours: 168, // 1 week
            threat_cooldown_minutes: 30,
            auto_block_duration_minutes: 60,
            escalation_threshold: 3,
            max_concurrent_threats: 100,
        }
    }
}

impl ThreatDetectionSystem {
    pub fn new(config: ThreatDetectionConfig) -> Result<Self> {
        let anomaly_detector = Arc::new(AnomalyDetector::new(AnomalyDetectionConfig::default()));
        let behavioral_analyzer =
            Arc::new(BehavioralAnalyzer::new(BehavioralAnalysisConfig::default()));
        let attack_pattern_detector =
            Arc::new(AttackPatternDetector::new(AttackDetectionConfig::default()));
        let mitigation_engine = Arc::new(MitigationEngine::new(MitigationConfig::default()));

        let system = Self {
            anomaly_detector,
            behavioral_analyzer,
            attack_pattern_detector,
            mitigation_engine,
            config,
            metrics: Arc::new(ThreatDetectionMetrics::default()),
        };

        system.start_background_tasks();

        Ok(system)
    }

    fn start_background_tasks(&self) {
        // Anomaly detection worker
        if self.config.enable_anomaly_detection {
            let detector = Arc::clone(&self.anomaly_detector);
            let metrics = Arc::clone(&self.metrics);
            tokio::spawn(async move {
                Self::anomaly_detection_worker(detector, metrics).await;
            });
        }

        // Behavioral analysis worker
        if self.config.enable_behavioral_analysis {
            let analyzer = Arc::clone(&self.behavioral_analyzer);
            let metrics = Arc::clone(&self.metrics);
            tokio::spawn(async move {
                Self::behavioral_analysis_worker(analyzer, metrics).await;
            });
        }

        // Attack pattern detection worker
        if self.config.enable_attack_pattern_detection {
            let detector = Arc::clone(&self.attack_pattern_detector);
            let metrics = Arc::clone(&self.metrics);
            tokio::spawn(async move {
                Self::attack_pattern_worker(detector, metrics).await;
            });
        }

        // Mitigation engine worker
        if self.config.enable_automated_mitigation {
            let engine = Arc::clone(&self.mitigation_engine);
            tokio::spawn(async move {
                Self::mitigation_worker(engine).await;
            });
        }
    }

    /// Analyze a security event for threats
    pub async fn analyze_security_event(
        &self,
        event: &SecurityEvent,
    ) -> Result<ThreatAnalysisResult> {
        let start_time = Instant::now();
        let mut result = ThreatAnalysisResult::default();

        // Anomaly detection
        if self.config.enable_anomaly_detection {
            if let Some(anomaly) = self.anomaly_detector.analyze_event(event).await? {
                result.anomalies.push(anomaly);
            }
        }

        // Behavioral analysis
        if self.config.enable_behavioral_analysis {
            if let Some(violation) = self.behavioral_analyzer.analyze_event(event).await? {
                result.behavioral_violations.push(violation);
            }
        }

        // Attack pattern detection
        if self.config.enable_attack_pattern_detection {
            if let Some(attack) = self.attack_pattern_detector.analyze_event(event).await? {
                result.detected_attacks.push(attack);
            }
        }

        // Calculate overall threat score
        result.threat_score = self.calculate_threat_score(&result);
        result.analysis_duration = start_time.elapsed();

        // Trigger mitigation if necessary
        if result.threat_score > self.config.attack_confidence_threshold
            && self.config.enable_automated_mitigation
        {
            result.mitigation_applied = self.mitigation_engine.apply_mitigation(&result).await?;
        }

        // Update metrics
        self.update_threat_metrics(&result);

        Ok(result)
    }

    fn calculate_threat_score(&self, result: &ThreatAnalysisResult) -> f64 {
        let mut score = 0.0;

        // Anomaly contribution
        for anomaly in &result.anomalies {
            score += anomaly.confidence * 0.3;
        }

        // Behavioral violation contribution
        for violation in &result.behavioral_violations {
            score += violation.deviation_score * 0.4;
        }

        // Attack detection contribution
        for attack in &result.detected_attacks {
            score += attack.confidence * 0.5;
        }

        score.min(1.0) // Cap at 1.0
    }

    fn update_threat_metrics(&self, result: &ThreatAnalysisResult) {
        if !result.anomalies.is_empty() {
            self.metrics
                .anomalies_detected
                .fetch_add(1, Ordering::Relaxed);
        }

        if !result.behavioral_violations.is_empty() {
            self.metrics
                .behavioral_violations
                .fetch_add(1, Ordering::Relaxed);
        }

        if !result.detected_attacks.is_empty() {
            self.metrics
                .attacks_detected
                .fetch_add(1, Ordering::Relaxed);
        }

        if result.mitigation_applied {
            self.metrics
                .attacks_mitigated
                .fetch_add(1, Ordering::Relaxed);
        }

        let detection_time_ms = result.analysis_duration.as_millis() as u64;
        self.metrics
            .avg_detection_time_ms
            .store(detection_time_ms, Ordering::Relaxed);
    }

    async fn anomaly_detection_worker(
        detector: Arc<AnomalyDetector>,
        _metrics: Arc<ThreatDetectionMetrics>,
    ) {
        let mut interval = interval(Duration::from_secs(60)); // Run every minute

        loop {
            interval.tick().await;

            if let Err(e) = detector.update_baselines().await {
                error!("Failed to update anomaly detection baselines: {}", e);
            }

            if let Err(e) = detector.cleanup_old_metrics().await {
                error!("Failed to cleanup old metrics: {}", e);
            }
        }
    }

    async fn behavioral_analysis_worker(
        analyzer: Arc<BehavioralAnalyzer>,
        _metrics: Arc<ThreatDetectionMetrics>,
    ) {
        let mut interval = interval(Duration::from_secs(300)); // Run every 5 minutes

        loop {
            interval.tick().await;

            if let Err(e) = analyzer.update_behavioral_profiles().await {
                error!("Failed to update behavioral profiles: {}", e);
            }

            if let Err(e) = analyzer.cleanup_stale_profiles().await {
                error!("Failed to cleanup stale profiles: {}", e);
            }
        }
    }

    async fn attack_pattern_worker(
        detector: Arc<AttackPatternDetector>,
        _metrics: Arc<ThreatDetectionMetrics>,
    ) {
        let mut interval = interval(Duration::from_secs(30)); // Run every 30 seconds

        loop {
            interval.tick().await;

            if let Err(e) = detector.correlate_attack_events().await {
                error!("Failed to correlate attack events: {}", e);
            }

            if let Err(e) = detector.cleanup_expired_attacks().await {
                error!("Failed to cleanup expired attacks: {}", e);
            }
        }
    }

    async fn mitigation_worker(engine: Arc<MitigationEngine>) {
        let mut interval = interval(Duration::from_secs(10)); // Run every 10 seconds

        loop {
            interval.tick().await;

            if let Err(e) = engine.monitor_mitigation_effectiveness().await {
                error!("Failed to monitor mitigation effectiveness: {}", e);
            }

            if let Err(e) = engine.cleanup_expired_mitigations().await {
                error!("Failed to cleanup expired mitigations: {}", e);
            }
        }
    }

    /// Get current threat detection metrics
    pub fn get_metrics(&self) -> ThreatDetectionMetrics {
        ThreatDetectionMetrics {
            anomalies_detected: AtomicU64::new(
                self.metrics.anomalies_detected.load(Ordering::Relaxed),
            ),
            behavioral_violations: AtomicU64::new(
                self.metrics.behavioral_violations.load(Ordering::Relaxed),
            ),
            attacks_detected: AtomicU64::new(self.metrics.attacks_detected.load(Ordering::Relaxed)),
            attacks_mitigated: AtomicU64::new(
                self.metrics.attacks_mitigated.load(Ordering::Relaxed),
            ),
            false_positives: AtomicU64::new(self.metrics.false_positives.load(Ordering::Relaxed)),
            mitigation_success_rate: AtomicU64::new(
                self.metrics.mitigation_success_rate.load(Ordering::Relaxed),
            ),
            avg_detection_time_ms: AtomicU64::new(
                self.metrics.avg_detection_time_ms.load(Ordering::Relaxed),
            ),
            avg_mitigation_time_ms: AtomicU64::new(
                self.metrics.avg_mitigation_time_ms.load(Ordering::Relaxed),
            ),
        }
    }

    /// Get active threats summary
    pub async fn get_active_threats(&self) -> Vec<ThreatSummary> {
        let mut threats = Vec::new();

        // Get active attacks
        for attack in self.attack_pattern_detector.active_attacks.iter() {
            threats.push(ThreatSummary {
                threat_id: attack.attack_id.clone(),
                threat_type: ThreatType::Attack,
                severity: match attack.confidence_score {
                    s if s > 0.9 => ThreatSeverity::Critical,
                    s if s > 0.7 => ThreatSeverity::High,
                    s if s > 0.5 => ThreatSeverity::Medium,
                    _ => ThreatSeverity::Low,
                },
                confidence: attack.confidence_score,
                started_at: attack.started_at,
                last_activity: attack.last_activity,
                targets: attack.affected_targets.clone(),
                mitigation_applied: attack.mitigation_applied,
            });
        }

        threats
    }
}

#[derive(Debug, Default)]
pub struct ThreatAnalysisResult {
    pub anomalies: Vec<DetectedAnomaly>,
    pub behavioral_violations: Vec<BehavioralViolation>,
    pub detected_attacks: Vec<DetectedAttack>,
    pub threat_score: f64,
    pub mitigation_applied: bool,
    pub analysis_duration: Duration,
}

#[derive(Debug)]
pub struct DetectedAnomaly {
    pub anomaly_type: String,
    pub metric_name: String,
    pub current_value: f64,
    pub baseline_mean: f64,
    pub deviation: f64,
    pub confidence: f64,
}

#[derive(Debug)]
pub struct BehavioralViolation {
    pub violation_type: String,
    pub entity_id: String,
    pub entity_type: String, // "user" or "ip"
    pub deviation_score: f64,
    pub details: String,
}

#[derive(Debug)]
pub struct DetectedAttack {
    pub attack_id: String,
    pub signature_id: String,
    pub attack_name: String,
    pub confidence: f64,
    pub severity: ThreatSeverity,
    pub evidence: Vec<String>,
}

#[derive(Debug)]
pub struct ThreatSummary {
    pub threat_id: String,
    pub threat_type: ThreatType,
    pub severity: ThreatSeverity,
    pub confidence: f64,
    pub started_at: Instant,
    pub last_activity: Instant,
    pub targets: Vec<String>,
    pub mitigation_applied: bool,
}

#[derive(Debug)]
pub enum ThreatType {
    Anomaly,
    BehavioralViolation,
    Attack,
}

// Implementation stubs for the component traits
impl AnomalyDetector {
    pub fn new(_config: AnomalyDetectionConfig) -> Self {
        Self {}
    }

    pub async fn analyze_event(&self, _event: &SecurityEvent) -> Result<Option<DetectedAnomaly>> {
        // Implementation would analyze the event against baselines
        Ok(None)
    }

    pub async fn update_baselines(&self) -> Result<()> {
        // Implementation would update statistical baselines
        Ok(())
    }

    pub async fn cleanup_old_metrics(&self) -> Result<()> {
        // Implementation would remove old metric data
        Ok(())
    }
}

impl BehavioralAnalyzer {
    pub fn new(_config: BehavioralAnalysisConfig) -> Self {
        Self {}
    }

    pub async fn analyze_event(
        &self,
        _event: &SecurityEvent,
    ) -> Result<Option<BehavioralViolation>> {
        // Implementation would analyze behavioral patterns
        Ok(None)
    }

    pub async fn update_behavioral_profiles(&self) -> Result<()> {
        // Implementation would update user/IP profiles
        Ok(())
    }

    pub async fn cleanup_stale_profiles(&self) -> Result<()> {
        // Implementation would remove old profiles
        Ok(())
    }
}

impl AttackPatternDetector {
    pub fn new(_config: AttackDetectionConfig) -> Self {
        Self {
            active_attacks: Arc::new(DashMap::new()),
        }
    }

    pub async fn analyze_event(&self, _event: &SecurityEvent) -> Result<Option<DetectedAttack>> {
        // Implementation would match against attack signatures
        Ok(None)
    }

    pub async fn correlate_attack_events(&self) -> Result<()> {
        // Implementation would correlate related attack events
        Ok(())
    }

    pub async fn cleanup_expired_attacks(&self) -> Result<()> {
        // Implementation would remove expired attack records
        Ok(())
    }
}

impl MitigationEngine {
    pub fn new(_config: MitigationConfig) -> Self {
        Self {}
    }

    pub async fn apply_mitigation(&self, _result: &ThreatAnalysisResult) -> Result<bool> {
        // Implementation would apply appropriate mitigations
        Ok(true)
    }

    pub async fn monitor_mitigation_effectiveness(&self) -> Result<()> {
        // Implementation would monitor if mitigations are working
        Ok(())
    }

    pub async fn cleanup_expired_mitigations(&self) -> Result<()> {
        // Implementation would remove expired mitigations
        Ok(())
    }
}

impl Default for AnomalyDetectionConfig {
    fn default() -> Self {
        Self {
            learning_period_hours: 168, // 1 week
            sensitivity: 2.5,
            min_samples: 100,
            metrics_retention_hours: 720, // 30 days
        }
    }
}

impl Default for BehavioralAnalysisConfig {
    fn default() -> Self {
        Self {
            profile_learning_days: 7,
            behavior_deviation_threshold: 2.0,
            min_activity_threshold: 10,
            profile_retention_days: 90,
        }
    }
}

impl Default for AttackDetectionConfig {
    fn default() -> Self {
        Self {
            signature_update_interval_hours: 24,
            attack_correlation_window_minutes: 15,
            min_attack_confidence: 0.7,
            max_active_attacks: 1000,
        }
    }
}

impl Default for MitigationConfig {
    fn default() -> Self {
        Self {
            enable_auto_blocking: true,
            enable_rate_limiting: true,
            enable_quarantine: true,
            enable_notification: true,
            block_duration_minutes: 60,
            rate_limit_factor: 0.1,
            quarantine_duration_hours: 24,
            escalation_levels: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::websocket_auth::AuthMethod;
    use crate::security::monitor::AuthFailureEvent;
    use std::sync::atomic::Ordering;

    // --- ThreatDetectionConfig tests ---

    #[test]
    fn test_threat_detection_config_defaults() {
        let config = ThreatDetectionConfig::default();
        assert!(config.enable_anomaly_detection);
        assert!(config.enable_behavioral_analysis);
        assert!(config.enable_attack_pattern_detection);
        assert!(config.enable_automated_mitigation);
        assert_eq!(config.anomaly_threshold, 3.0);
        assert_eq!(config.behavioral_deviation_threshold, 2.5);
        assert_eq!(config.attack_confidence_threshold, 0.8);
        assert_eq!(config.analysis_window_seconds, 300);
        assert_eq!(config.baseline_learning_hours, 168);
        assert_eq!(config.threat_cooldown_minutes, 30);
        assert_eq!(config.auto_block_duration_minutes, 60);
        assert_eq!(config.escalation_threshold, 3);
        assert_eq!(config.max_concurrent_threats, 100);
    }

    #[test]
    fn test_threat_detection_config_custom() {
        let config = ThreatDetectionConfig {
            enable_anomaly_detection: false,
            enable_behavioral_analysis: false,
            anomaly_threshold: 5.0,
            analysis_window_seconds: 600,
            baseline_learning_hours: 24,
            auto_block_duration_minutes: 30,
            ..Default::default()
        };
        assert!(!config.enable_anomaly_detection);
        assert!(!config.enable_behavioral_analysis);
        assert_eq!(config.anomaly_threshold, 5.0);
        assert_eq!(config.analysis_window_seconds, 600);
        assert_eq!(config.baseline_learning_hours, 24);
        assert_eq!(config.auto_block_duration_minutes, 30);
        assert!(config.enable_attack_pattern_detection);
        assert!(config.enable_automated_mitigation);
    }

    // --- AnomalyDetectionConfig tests ---

    #[test]
    fn test_anomaly_detection_config_default() {
        let config = AnomalyDetectionConfig::default();
        assert_eq!(config.learning_period_hours, 168);
        assert_eq!(config.sensitivity, 2.5);
        assert_eq!(config.min_samples, 100);
        assert_eq!(config.metrics_retention_hours, 720);
    }

    // --- AnomalyDetector tests ---

    #[test]
    fn test_anomaly_detector_creation() {
        let _detector = AnomalyDetector::new(AnomalyDetectionConfig::default());
    }

    #[test]
    fn test_anomaly_detector_analyze_event_returns_none() {
        let detector = AnomalyDetector::new(AnomalyDetectionConfig::default());
        let event = SecurityEvent::AuthFailure(AuthFailureEvent {
            timestamp: 0,
            ip: None,
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::JWT,
            reason: "test".into(),
            user_agent: None,
        });
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(detector.analyze_event(&event));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_anomaly_detector_update_baselines_succeeds() {
        let detector = AnomalyDetector::new(AnomalyDetectionConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(detector.update_baselines());
        assert!(result.is_ok());
    }

    #[test]
    fn test_anomaly_detector_cleanup_old_metrics_succeeds() {
        let detector = AnomalyDetector::new(AnomalyDetectionConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(detector.cleanup_old_metrics());
        assert!(result.is_ok());
    }

    // --- BaselineStats tests ---

    #[test]
    fn test_baseline_stats_default_values() {
        let stats = BaselineStats::default();
        assert_eq!(stats.connections_per_hour.mean, 0.0);
        assert_eq!(stats.connections_per_hour.std_dev, 0.0);
        assert_eq!(stats.connections_per_hour.min, 0.0);
        assert_eq!(stats.connections_per_hour.max, 0.0);
        assert_eq!(stats.connections_per_hour.sample_count, 0);
        assert_eq!(stats.messages_per_minute.mean, 0.0);
        assert_eq!(stats.auth_failures_per_hour.mean, 0.0);
        assert_eq!(stats.unique_ips_per_hour.mean, 0.0);
        assert!(stats.last_updated > SystemTime::UNIX_EPOCH);
    }

    // --- StatisticalBaseline tests ---

    #[test]
    fn test_statistical_baseline_default() {
        let baseline = StatisticalBaseline::default();
        assert_eq!(baseline.mean, 0.0);
        assert_eq!(baseline.std_dev, 0.0);
        assert_eq!(baseline.min, 0.0);
        assert_eq!(baseline.max, 0.0);
        assert_eq!(baseline.sample_count, 0);
    }

    // --- ThreatDetectionMetrics tests ---

    #[test]
    fn test_threat_detection_metrics_construction() {
        let metrics = ThreatDetectionMetrics::default();
        assert_eq!(metrics.anomalies_detected.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.behavioral_violations.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.attacks_detected.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.attacks_mitigated.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.false_positives.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.mitigation_success_rate.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.avg_detection_time_ms.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.avg_mitigation_time_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_threat_detection_metrics_clone() {
        let metrics = ThreatDetectionMetrics::default();
        metrics.anomalies_detected.fetch_add(5, Ordering::Relaxed);
        metrics.attacks_detected.fetch_add(3, Ordering::Relaxed);
        let cloned = metrics.clone();

        // Verify original values
        assert_eq!(cloned.anomalies_detected.load(Ordering::Relaxed), 5);
        assert_eq!(cloned.attacks_detected.load(Ordering::Relaxed), 3);
        assert_eq!(cloned.behavioral_violations.load(Ordering::Relaxed), 0);
    }

    // --- BehavioralAnalysisConfig tests ---

    #[test]
    fn test_behavioral_analysis_config_default() {
        let config = BehavioralAnalysisConfig::default();
        assert_eq!(config.profile_learning_days, 7);
        assert_eq!(config.behavior_deviation_threshold, 2.0);
        assert_eq!(config.min_activity_threshold, 10);
        assert_eq!(config.profile_retention_days, 90);
    }

    // --- BehavioralAnalyzer tests ---

    #[test]
    fn test_behavioral_analyzer_creation() {
        let _analyzer = BehavioralAnalyzer::new(BehavioralAnalysisConfig::default());
    }

    #[test]
    fn test_behavioral_analyzer_analyze_event_returns_none() {
        let analyzer = BehavioralAnalyzer::new(BehavioralAnalysisConfig::default());
        let event = SecurityEvent::AuthFailure(AuthFailureEvent {
            timestamp: 0,
            ip: None,
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::JWT,
            reason: "test".into(),
            user_agent: None,
        });
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(analyzer.analyze_event(&event));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_behavioral_analyzer_update_profiles_succeeds() {
        let analyzer = BehavioralAnalyzer::new(BehavioralAnalysisConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(analyzer.update_behavioral_profiles());
        assert!(result.is_ok());
    }

    #[test]
    fn test_behavioral_analyzer_cleanup_profiles_succeeds() {
        let analyzer = BehavioralAnalyzer::new(BehavioralAnalysisConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(analyzer.cleanup_stale_profiles());
        assert!(result.is_ok());
    }

    // --- AttackDetectionConfig tests ---

    #[test]
    fn test_attack_detection_config_default() {
        let config = AttackDetectionConfig::default();
        assert_eq!(config.signature_update_interval_hours, 24);
        assert_eq!(config.attack_correlation_window_minutes, 15);
        assert_eq!(config.min_attack_confidence, 0.7);
        assert_eq!(config.max_active_attacks, 1000);
    }

    // --- AttackPatternDetector tests ---

    #[test]
    fn test_attack_pattern_detector_creation() {
        let detector = AttackPatternDetector::new(AttackDetectionConfig::default());
        assert!(detector.active_attacks.is_empty());
    }

    #[test]
    fn test_attack_pattern_detector_analyze_event_returns_none() {
        let detector = AttackPatternDetector::new(AttackDetectionConfig::default());
        let event = SecurityEvent::AuthFailure(AuthFailureEvent {
            timestamp: 0,
            ip: None,
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::JWT,
            reason: "test".into(),
            user_agent: None,
        });
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(detector.analyze_event(&event));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_attack_pattern_detector_correlate_succeeds() {
        let detector = AttackPatternDetector::new(AttackDetectionConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(detector.correlate_attack_events());
        assert!(result.is_ok());
    }

    #[test]
    fn test_attack_pattern_detector_cleanup_succeeds() {
        let detector = AttackPatternDetector::new(AttackDetectionConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(detector.cleanup_expired_attacks());
        assert!(result.is_ok());
    }

    // --- AttackSignature tests ---

    #[test]
    fn test_attack_signature_creation() {
        let signature = AttackSignature {
            signature_id: "SIG-001".to_string(),
            name: "Brute Force".to_string(),
            description: "Detects brute force login attempts".to_string(),
            pattern: AttackPattern::BruteForce {
                min_attempts: 5,
                time_window: Duration::from_secs(60),
                failure_rate: 0.9,
            },
            severity: ThreatSeverity::High,
            confidence_threshold: 0.8,
            created_at: SystemTime::now(),
        };
        assert_eq!(signature.signature_id, "SIG-001");
        assert_eq!(signature.name, "Brute Force");
    }

    // --- MitigationConfig tests ---

    #[test]
    fn test_mitigation_config_default() {
        let config = MitigationConfig::default();
        assert!(config.enable_auto_blocking);
        assert!(config.enable_rate_limiting);
        assert!(config.enable_quarantine);
        assert!(config.enable_notification);
        assert_eq!(config.block_duration_minutes, 60);
        assert_eq!(config.rate_limit_factor, 0.1);
        assert_eq!(config.quarantine_duration_hours, 24);
        assert_eq!(config.escalation_levels, 3);
    }

    // --- MitigationEngine tests ---

    #[test]
    fn test_mitigation_engine_creation() {
        let _engine = MitigationEngine::new(MitigationConfig::default());
    }

    #[test]
    fn test_mitigation_engine_apply_mitigation_returns_true() {
        let engine = MitigationEngine::new(MitigationConfig::default());
        let result = ThreatAnalysisResult::default();
        let applied = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.apply_mitigation(&result));
        assert!(applied.unwrap());
    }

    #[test]
    fn test_mitigation_engine_monitor_succeeds() {
        let engine = MitigationEngine::new(MitigationConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.monitor_mitigation_effectiveness());
        assert!(result.is_ok());
    }

    #[test]
    fn test_mitigation_engine_cleanup_succeeds() {
        let engine = MitigationEngine::new(MitigationConfig::default());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.cleanup_expired_mitigations());
        assert!(result.is_ok());
    }

    // --- ThreatAnalysisResult tests ---

    #[test]
    fn test_threat_analysis_result_default() {
        let result = ThreatAnalysisResult::default();
        assert!(result.anomalies.is_empty());
        assert!(result.behavioral_violations.is_empty());
        assert!(result.detected_attacks.is_empty());
        assert_eq!(result.threat_score, 0.0);
        assert!(!result.mitigation_applied);
        assert_eq!(result.analysis_duration, Duration::from_secs(0));
    }

    // --- DetectedAnomaly tests ---

    #[test]
    fn test_detected_anomaly_debug() {
        let anomaly = DetectedAnomaly {
            anomaly_type: "traffic_spike".to_string(),
            metric_name: "connections_per_hour".to_string(),
            current_value: 1500.0,
            baseline_mean: 500.0,
            deviation: 3.5,
            confidence: 0.95,
        };
        let _ = format!("{:?}", &anomaly);
        assert_eq!(anomaly.anomaly_type, "traffic_spike");
        assert_eq!(anomaly.deviation, 3.5);
    }

    // --- BehavioralViolation tests ---

    #[test]
    fn test_behavioral_violation_debug() {
        let violation = BehavioralViolation {
            violation_type: "unusual_location".to_string(),
            entity_id: "user-123".to_string(),
            entity_type: "user".to_string(),
            deviation_score: 4.2,
            details: "Login from unusual geographic location".to_string(),
        };
        let _ = format!("{:?}", &violation);
        assert_eq!(violation.deviation_score, 4.2);
    }

    // --- DetectedAttack tests ---

    #[test]
    fn test_detected_attack_debug() {
        let attack = DetectedAttack {
            attack_id: "ATK-001".to_string(),
            signature_id: "SIG-BRUTE-01".to_string(),
            attack_name: "Brute Force".to_string(),
            confidence: 0.92,
            severity: ThreatSeverity::Critical,
            evidence: vec!["5 failed logins in 10 seconds".to_string()],
        };
        let _ = format!("{:?}", &attack);
        assert_eq!(attack.attack_id, "ATK-001");
        assert_eq!(attack.confidence, 0.92);
    }

    // --- ThreatSummary tests ---

    #[test]
    fn test_threat_summary_debug() {
        let summary = ThreatSummary {
            threat_id: "THR-001".to_string(),
            threat_type: ThreatType::Attack,
            severity: ThreatSeverity::High,
            confidence: 0.85,
            started_at: Instant::now(),
            last_activity: Instant::now(),
            targets: vec!["10.0.0.1".to_string()],
            mitigation_applied: false,
        };
        let _ = format!("{:?}", &summary);
    }

    // --- ThreatType tests ---

    #[test]
    fn test_threat_type_variants() {
        match ThreatType::Anomaly {
            ThreatType::Anomaly => {}
            _ => panic!("Expected Anomaly"),
        }
        match ThreatType::BehavioralViolation {
            ThreatType::BehavioralViolation => {}
            _ => panic!("Expected BehavioralViolation"),
        }
        match ThreatType::Attack {
            ThreatType::Attack => {}
            _ => panic!("Expected Attack"),
        }
    }

    // --- AttackPattern enum tests ---

    #[test]
    fn test_attack_pattern_variants_debug() {
        let patterns: Vec<AttackPattern> = vec![
            AttackPattern::BruteForce {
                min_attempts: 5,
                time_window: Duration::from_secs(60),
                failure_rate: 0.9,
            },
            AttackPattern::DdosDetection {
                connection_rate: 1000,
                time_window: Duration::from_secs(10),
                unique_ip_threshold: 50,
            },
            AttackPattern::CredentialStuffing {
                user_enumeration: true,
                password_spraying: false,
                success_rate_threshold: 0.01,
            },
            AttackPattern::SqlInjection {
                patterns: vec!["' OR".to_string()],
                payload_analysis: true,
            },
            AttackPattern::XssAttempt {
                script_patterns: vec!["<script>".to_string()],
                encoding_detection: true,
            },
            AttackPattern::AnomalousAccess {
                deviation_threshold: 3.0,
                baseline_comparison: true,
            },
        ];
        for p in &patterns {
            let _ = format!("{p:?}");
        }
    }

    // --- ThreatSeverity enum tests ---

    #[test]
    fn test_threat_severity_variants() {
        assert_ne!(ThreatSeverity::Low as u8, ThreatSeverity::Critical as u8);
        assert_eq!(ThreatSeverity::Low as u8, 0);
        assert_eq!(ThreatSeverity::Critical as u8, 3);
    }

    // --- OngoingAttack tests ---

    #[test]
    fn test_ongoing_attack_debug() {
        let attack = OngoingAttack {
            attack_id: "ATK-ONGOING-01".to_string(),
            signature_id: "SIG-DDOS-01".to_string(),
            started_at: Instant::now(),
            last_activity: Instant::now(),
            confidence_score: 0.75,
            affected_targets: vec!["10.0.0.1".to_string()],
            source_ips: vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                192, 168, 1, 1,
            ))],
            event_count: 100,
            mitigation_applied: false,
        };
        let _ = format!("{:?}", &attack);
        assert_eq!(attack.event_count, 100);
    }

    // --- ActiveMitigation tests ---

    #[test]
    fn test_active_mitigation_debug() {
        let mitigation = ActiveMitigation {
            mitigation_id: "MIT-001".to_string(),
            threat_id: "THR-001".to_string(),
            mitigation_type: MitigationType::IpBlocking {
                duration: Duration::from_secs(3600),
            },
            targets: vec!["10.0.0.1".to_string()],
            applied_at: Instant::now(),
            expires_at: Some(Instant::now() + Duration::from_secs(3600)),
            effectiveness_score: 0.9,
        };
        let _ = format!("{:?}", &mitigation);
    }

    // --- MitigationType enum tests ---

    #[test]
    fn test_mitigation_type_variants() {
        let types: Vec<MitigationType> = vec![
            MitigationType::IpBlocking {
                duration: Duration::from_secs(3600),
            },
            MitigationType::RateLimiting {
                factor: 0.5,
                duration: Duration::from_secs(300),
            },
            MitigationType::UserQuarantine {
                duration: Duration::from_secs(86400),
            },
            MitigationType::ConnectionThrottling { max_connections: 5 },
            MitigationType::ContentFiltering {
                patterns: vec!["script".to_string()],
            },
            MitigationType::GeoBlocking {
                blocked_regions: vec!["XX".to_string()],
            },
        ];
        for t in &types {
            let _ = format!("{t:?}");
        }
    }

    // --- MitigationEvent tests ---

    #[test]
    fn test_mitigation_event_debug() {
        let event = MitigationEvent {
            event_id: "ME-001".to_string(),
            timestamp: SystemTime::now(),
            mitigation_type: MitigationType::IpBlocking {
                duration: Duration::from_secs(3600),
            },
            trigger_threat: "THR-001".to_string(),
            targets_affected: vec!["10.0.0.1".to_string()],
            success: true,
            details: "IP blocked successfully".to_string(),
        };
        let _ = format!("{:?}", &event);
    }

    // --- ThreatDetectionSystem constructor test ---
    // We can't easily instantiate ThreatDetectionSystem in tests
    // because it requires Arc<SecurityMonitor> and spawns background tasks,
    // but we can test the calculation logic.

    #[tokio::test]
    async fn test_calculate_threat_score_no_events() {
        let config = ThreatDetectionConfig::default();
        let system = ThreatDetectionSystem::new(config).unwrap();
        let result = ThreatAnalysisResult::default();
        let score = system.calculate_threat_score(&result);
        assert_eq!(score, 0.0);
    }
}
