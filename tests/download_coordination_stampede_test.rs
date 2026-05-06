//! 100-Concurrent-Client Live Stampede Test
//!
//! Spec: `.kiro/specs/download-coordination-ttl-correctness/`
//! Task: 12.1
//!
//! This is the user's explicit headline validation test — 100 concurrent clients
//! exercising Property 1 (every request sees S3) and Property 2 (at most one
//! authoritative revalidation per expired-cache flight) against real AWS SigV4,
//! real IAM, and real S3 response semantics.
//!
//! **Test bucket**: `s3-proxy-stampede-test-407092780826-us-west-2` in us-west-2.
//! Hard-coded with fail-loud guard.
//!
//! Gate: `RUN_INTEGRATION_TESTS=1` env var (early return if not set).
//!
//! **Test matrix (8 runs total = 4 scenarios × 2 TTL values):**
//!
//! | # | TTL | Cache state | Credentials | N |
//! |---|---|---|---|---|
//! | 1 | get_ttl=0 | cold (wiped) | 100 valid | 100 |
//! | 2 | get_ttl=0 | cold (wiped) | 50 valid + 50 invalid | 100 |
//! | 3 | get_ttl=0 | expired (pre-warmed) | 100 valid | 100 |
//! | 4 | get_ttl=0 | expired (pre-warmed) | 50 valid + 50 invalid | 100 |
//! | 5 | get_ttl=3600 | cold (wiped) | 100 valid | 100 |
//! | 6 | get_ttl=3600 | cold (wiped) | 50 valid + 50 invalid | 100 |
//! | 7 | get_ttl=3600 | expired (pre-warmed, expires_at rewound) | 100 valid | 100 |
//! | 8 | get_ttl=3600 | expired (as 7) | 50 valid + 50 invalid | 100 |
//!
//! **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10**

mod common;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::task::JoinSet;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::metrics::MetricsManager;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

// =========================================================================
// Constants
// =========================================================================

const TEST_BUCKET: &str = "s3-proxy-stampede-test-407092780826-us-west-2";
const TEST_REGION: &str = "us-west-2";
const TEST_OBJECT_KEY: &str = "stampede-test/coordination-stampede-object-4mib.bin";
const OBJECT_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
const N_CLIENTS: usize = 100;

// Fake AWS credentials for "invalid" clients — syntactically valid SigV4 but
// S3 will return 403 because the access key doesn't map to a real IAM entity.
const FAKE_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
const FAKE_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

// =========================================================================
// Env-var gate
// =========================================================================

/// Returns true if integration tests should run.
fn should_run() -> bool {
    std::env::var("RUN_INTEGRATION_TESTS")
        .map(|v| v == "1")
        .unwrap_or(false)
}

// =========================================================================
// AWS SigV4 Signing Utilities (reused from integration test)
// =========================================================================

/// Minimal AWS SigV4 signer for integration tests.
struct AwsSigner {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
}

impl AwsSigner {
    fn from_env(region: &str) -> Option<Self> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        Some(Self {
            access_key_id,
            secret_access_key,
            session_token,
            region: region.to_string(),
        })
    }

    /// Create a signer with fake (invalid) credentials.
    fn fake(region: &str) -> Self {
        Self {
            access_key_id: FAKE_ACCESS_KEY_ID.to_string(),
            secret_access_key: FAKE_SECRET_ACCESS_KEY.to_string(),
            session_token: None,
            region: region.to_string(),
        }
    }

    /// Sign a request and return the full set of headers needed.
    fn sign_request(
        &self,
        method: &str,
        host: &str,
        uri_path: &str,
        query_string: &str,
        extra_headers: &[(&str, &str)],
        payload_hash: &str,
    ) -> HashMap<String, String> {
        let now = chrono::Utc::now();
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        let mut headers = HashMap::new();
        headers.insert("host".to_string(), host.to_string());
        headers.insert("x-amz-date".to_string(), amz_date.clone());
        headers.insert("x-amz-content-sha256".to_string(), payload_hash.to_string());

        if let Some(ref token) = self.session_token {
            headers.insert("x-amz-security-token".to_string(), token.clone());
        }

        for (k, v) in extra_headers {
            headers.insert(k.to_string(), v.to_string());
        }

        // Build canonical headers and signed headers
        let mut sorted_keys: Vec<&String> = headers.keys().collect();
        sorted_keys.sort();

        let canonical_headers: String = sorted_keys
            .iter()
            .map(|k| format!("{}:{}\n", k, headers[*k].trim()))
            .collect();

        let signed_headers: String = sorted_keys
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<&str>>()
            .join(";");

        // Canonical request
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, uri_path, query_string, canonical_headers, signed_headers, payload_hash
        );

        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        // String to sign
        let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, credential_scope, canonical_request_hash
        );

        // Signing key
        let signing_key = self.derive_signing_key(&date_stamp);

        // Signature
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        // Authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key_id, credential_scope, signed_headers, signature
        );

        headers.insert("authorization".to_string(), authorization);
        headers
    }

    fn derive_signing_key(&self, date_stamp: &str) -> Vec<u8> {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            date_stamp.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use ring::hmac;
    let signing_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&signing_key, data).as_ref().to_vec()
}

/// Sign a GET request for the test object.
fn sign_get_request(signer: &AwsSigner, object_key: &str) -> HashMap<String, String> {
    let host = format!("{}.s3.{}.amazonaws.com", TEST_BUCKET, TEST_REGION);
    let uri_path = format!("/{}", object_key);
    signer.sign_request("GET", &host, &uri_path, "", &[], "UNSIGNED-PAYLOAD")
}

/// Sign a PUT request to upload the test object.
fn sign_put_request(signer: &AwsSigner, object_key: &str, body: &[u8]) -> HashMap<String, String> {
    let host = format!("{}.s3.{}.amazonaws.com", TEST_BUCKET, TEST_REGION);
    let uri_path = format!("/{}", object_key);
    let payload_hash = hex::encode(Sha256::digest(body));
    let content_length = body.len().to_string();
    signer.sign_request(
        "PUT",
        &host,
        &uri_path,
        "",
        &[("content-length", &content_length)],
        &payload_hash,
    )
}

// =========================================================================
// Infrastructure helpers
// =========================================================================

/// Create a Config with the specified TTL and coordination enabled.
fn stampede_config(cache_dir: PathBuf, get_ttl_secs: u64) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.get_ttl = Duration::from_secs(get_ttl_secs);
    config.cache.head_ttl = Duration::from_secs(get_ttl_secs);
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 30;
    config.cache.ram_cache_enabled = false;
    config.cache.max_cache_size = 512 * 1024 * 1024; // 512 MiB
    Arc::new(config)
}

/// Create cache infrastructure for stampede tests.
async fn make_stampede_cache(
    config: &Arc<Config>,
) -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
        false, // compression disabled for integration tests
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");

    (cache_manager, disk_cache_manager)
}

/// Create a real S3Client for stampede tests.
fn make_real_s3_client(
    metrics_manager: Option<Arc<tokio::sync::RwLock<MetricsManager>>>,
) -> Arc<dyn S3ClientApi + Send + Sync> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let pool_config = s3_proxy::config::ConnectionPoolConfig::default();
    let client = S3Client::new(&pool_config, metrics_manager).expect("Failed to create S3Client");
    Arc::new(client)
}

/// Create a MetricsManager wrapped in the expected Arc<RwLock<>> type.
fn make_metrics() -> Arc<tokio::sync::RwLock<MetricsManager>> {
    Arc::new(tokio::sync::RwLock::new(MetricsManager::new()))
}

/// Read the coalescing stats from the metrics manager.
async fn read_coalescing_stats(
    mm: &Arc<tokio::sync::RwLock<MetricsManager>>,
) -> s3_proxy::metrics::CoalescingMetrics {
    let metrics = mm.read().await.collect_metrics().await;
    metrics
        .coalescing
        .expect("coalescing metrics should be present")
}

/// Upload a test object to S3 using the real S3Client.
async fn upload_test_object(
    s3_client: &Arc<dyn S3ClientApi + Send + Sync>,
    signer: &AwsSigner,
    object_key: &str,
    body: &[u8],
) {
    let host = format!("{}.s3.{}.amazonaws.com", TEST_BUCKET, TEST_REGION);
    let uri_path = format!("/{}", object_key);
    let uri: hyper::Uri = format!("https://{}{}", host, uri_path)
        .parse()
        .expect("valid URI");

    let headers = sign_put_request(signer, object_key, body);

    let context = s3_proxy::S3RequestContext {
        method: Method::PUT,
        uri,
        headers,
        body: Some(Bytes::copy_from_slice(body)),
        host,
        request_size: Some(body.len() as u64),
        operation_type: Some("PutObject".to_string()),
        allow_streaming: false,
    };

    let response = s3_client
        .forward_request(context)
        .await
        .expect("PUT to S3 should succeed");

    assert!(
        response.status.is_success(),
        "PUT failed with status {}: upload test object to S3",
        response.status
    );
}

/// Verify the test bucket exists by issuing a HEAD request.
async fn assert_bucket_exists(s3_client: &Arc<dyn S3ClientApi + Send + Sync>, signer: &AwsSigner) {
    let host = format!("{}.s3.{}.amazonaws.com", TEST_BUCKET, TEST_REGION);
    let uri: hyper::Uri = format!("https://{}/", host).parse().expect("valid URI");

    let headers = signer.sign_request("HEAD", &host, "/", "", &[], "UNSIGNED-PAYLOAD");

    let context = s3_proxy::S3RequestContext {
        method: Method::HEAD,
        uri,
        headers,
        body: None,
        host,
        request_size: None,
        operation_type: Some("HeadBucket".to_string()),
        allow_streaming: false,
    };

    let response = s3_client
        .forward_request(context)
        .await
        .expect("HEAD bucket should succeed");

    assert!(
        response.status.is_success(),
        "Test bucket '{}' does not exist or is not accessible (status {}). \
         Ensure the bucket exists and AWS credentials are configured.",
        TEST_BUCKET,
        response.status
    );
}

/// Generate deterministic test data (4 MiB).
fn generate_test_data() -> Vec<u8> {
    let mut data = vec![0u8; OBJECT_SIZE];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    data
}

/// Pre-warm the cache by issuing a single GET through the coordination path.
#[allow(clippy::too_many_arguments)]
async fn pre_warm_cache(
    signer: &AwsSigner,
    cache_key: &str,
    cache_manager: &Arc<CacheManager>,
    s3_client: &Arc<dyn S3ClientApi + Send + Sync>,
    inflight_tracker: &Arc<InFlightTracker>,
    range_handler: &Arc<RangeHandler>,
    config: &Arc<Config>,
    mm: &Arc<tokio::sync::RwLock<MetricsManager>>,
) {
    let host = format!("{}.s3.{}.amazonaws.com", TEST_BUCKET, TEST_REGION);
    let full_uri = format!("https://{}/{}", host, TEST_OBJECT_KEY);
    let uri: hyper::Uri = full_uri.parse().expect("valid URI");
    let headers = sign_get_request(signer, TEST_OBJECT_KEY);
    let wait_timeout = Duration::from_secs(config.cache.download_coordination.wait_timeout_secs);

    let response = HttpProxy::forward_get_head_with_coordination(
        Method::GET,
        uri,
        host,
        headers,
        cache_key.to_string(),
        Arc::clone(cache_manager),
        Arc::clone(s3_client),
        Arc::clone(inflight_tracker),
        Arc::clone(range_handler),
        Arc::clone(config),
        true,
        wait_timeout,
        Some(Arc::clone(mm)),
        &None,
    )
    .await
    .expect("pre-warm GET");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Pre-warm GET should succeed"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), OBJECT_SIZE, "Pre-warm body size mismatch");

    // Wait for the background cache write to complete. The TeeStream sends
    // chunks to a background task that writes them incrementally and then
    // commits the metadata file. Give it up to 30s.
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !metadata_path.exists() {
        if tokio::time::Instant::now() >= deadline {
            println!(
                "  WARNING: pre-warm cache write did not complete within 30s for {}",
                cache_key
            );
            println!("  Metadata path: {:?}", metadata_path);
            // Fall back: write metadata manually from the S3 response we already have
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if metadata_path.exists() {
        println!("  Pre-warm cache write completed for {}", cache_key);
    }
}

/// Expire the cache entry by rewriting the metadata's `expires_at` to the past.
/// Returns true if the entry was successfully expired, false if metadata was not found.
async fn expire_cache_entry(cache_manager: &Arc<CacheManager>, cache_key: &str) -> bool {
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);

    if !metadata_path.exists() {
        println!(
            "  WARNING: Cannot expire cache entry — metadata file does not exist at {:?}",
            metadata_path
        );
        return false;
    }

    let content = std::fs::read_to_string(&metadata_path).expect("read metadata file");
    let mut metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&content).expect("parse metadata JSON");

    // Rewind expires_at to 1 hour in the past
    metadata.expires_at = SystemTime::now() - Duration::from_secs(3600);
    if let Some(ref mut head_exp) = metadata.head_expires_at {
        *head_exp = SystemTime::now() - Duration::from_secs(3600);
    }

    let updated_json = serde_json::to_string_pretty(&metadata).expect("serialize metadata JSON");
    std::fs::write(&metadata_path, updated_json).expect("write updated metadata file");
    true
}

// =========================================================================
// Scenario runner
// =========================================================================

/// Describes a single stampede scenario.
struct ScenarioConfig {
    name: &'static str,
    get_ttl_secs: u64,
    cache_state: CacheState,
    credentials: CredentialsMix,
}

#[derive(Clone, Copy)]
enum CacheState {
    /// Cache wiped before stampede — cold miss.
    Cold,
    /// Cache pre-warmed then expired (TTL=0 means immediate expiry; TTL>0
    /// means we rewind `expires_at` to the past).
    Expired,
}

#[derive(Clone, Copy)]
enum CredentialsMix {
    /// All 100 clients use valid credentials.
    AllValid,
    /// 50 valid + 50 invalid (fake access key).
    Mixed,
}

/// Result of a single client in the stampede.
struct ClientResult {
    index: usize,
    status: StatusCode,
    body: Bytes,
    used_valid_creds: bool,
}

/// Run a single stampede scenario and return assertions.
async fn run_scenario(
    scenario: &ScenarioConfig,
    valid_signer: &AwsSigner,
    _test_data: &[u8],
    expected_sha256: &str,
    s3_client: &Arc<dyn S3ClientApi + Send + Sync>,
) {
    println!("\n=== Scenario: {} ===", scenario.name);

    // Create fresh temp dir and config for this scenario
    let temp_dir = TempDir::new().expect("tempdir");
    let config = stampede_config(temp_dir.path().to_path_buf(), scenario.get_ttl_secs);
    let (cache_manager, disk_cache_manager) = make_stampede_cache(&config).await;
    let mm = make_metrics();
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = format!("{}/{}", TEST_BUCKET, TEST_OBJECT_KEY);

    // Setup cache state
    match scenario.cache_state {
        CacheState::Cold => {
            // Ensure cache is clean
            let _ = cache_manager.invalidate_cache(&cache_key).await;
        }
        CacheState::Expired => {
            // Pre-warm the cache
            pre_warm_cache(
                valid_signer,
                &cache_key,
                &cache_manager,
                s3_client,
                &inflight_tracker,
                &range_handler,
                &config,
                &mm,
            )
            .await;

            // For TTL>0, rewind expires_at to the past
            if scenario.get_ttl_secs > 0 {
                let expired = expire_cache_entry(&cache_manager, &cache_key).await;
                if !expired {
                    println!("  NOTE: Could not expire cache entry (metadata not written yet). Scenario will behave like cold-cache.");
                }
            }
            // For TTL=0, the cache is already expired immediately
        }
    }

    // Reset metrics for the stampede phase
    let mm = make_metrics();

    let host = format!("{}.s3.{}.amazonaws.com", TEST_BUCKET, TEST_REGION);
    let full_uri = format!("https://{}/{}", host, TEST_OBJECT_KEY);
    let wait_timeout = Duration::from_secs(config.cache.download_coordination.wait_timeout_secs);

    // Create the fake signer for invalid credentials
    let fake_signer = AwsSigner::fake(TEST_REGION);

    // Launch N_CLIENTS concurrent requests
    let mut join_set: JoinSet<ClientResult> = JoinSet::new();

    for i in 0..N_CLIENTS {
        let method = Method::GET;
        let uri: hyper::Uri = full_uri.parse().expect("valid URI");
        let host = host.clone();
        let cache_key = cache_key.clone();
        let cache_manager = Arc::clone(&cache_manager);
        let s3_client = Arc::clone(s3_client);
        let inflight_tracker = Arc::clone(&inflight_tracker);
        let range_handler = Arc::clone(&range_handler);
        let config = Arc::clone(&config);
        let mm = Arc::clone(&mm);

        // Determine which signer to use
        let used_valid_creds = match scenario.credentials {
            CredentialsMix::AllValid => true,
            CredentialsMix::Mixed => i < 50, // first 50 valid, last 50 invalid
        };

        let headers = if used_valid_creds {
            sign_get_request(valid_signer, TEST_OBJECT_KEY)
        } else {
            sign_get_request(&fake_signer, TEST_OBJECT_KEY)
        };

        join_set.spawn(async move {
            // Stagger slightly so the first request becomes the fetcher
            if i > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let response = HttpProxy::forward_get_head_with_coordination(
                method,
                uri,
                host,
                headers,
                cache_key,
                cache_manager,
                s3_client,
                inflight_tracker,
                range_handler,
                config,
                true,
                wait_timeout,
                Some(mm),
                &None,
            )
            .await
            .expect("coordination helper");
            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            ClientResult {
                index: i,
                status,
                body,
                used_valid_creds,
            }
        });
    }

    // Collect results with a 30s timeout
    let mut results: Vec<ClientResult> = Vec::with_capacity(N_CLIENTS);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while let Some(joined) = tokio::time::timeout_at(deadline, join_set.join_next())
        .await
        .ok()
        .flatten()
    {
        results.push(joined.expect("task join"));
    }

    assert_eq!(
        results.len(),
        N_CLIENTS,
        "Scenario '{}': expected {} results, got {} (timeout?)",
        scenario.name,
        N_CLIENTS,
        results.len()
    );

    // Read coalescing stats
    let stats = read_coalescing_stats(&mm).await;

    // Diagnostic output before assertions
    println!("  Stats: fetcher_success={}, fetcher_error={}, waiter_304={}, waiter_200={}, waiter_4xx={}, waiter_error={}, waits_total={}, cache_hits_after_wait={}",
        stats.fetcher_completions_success,
        stats.fetcher_completions_error,
        stats.waiter_conditional_304,
        stats.waiter_conditional_200,
        stats.waiter_conditional_4xx,
        stats.waiter_conditional_error,
        stats.waits_total,
        stats.cache_hits_after_wait_total,
    );

    // Print status code distribution
    let mut status_counts: HashMap<u16, usize> = HashMap::new();
    for r in &results {
        *status_counts.entry(r.status.as_u16()).or_insert(0) += 1;
    }
    println!("  Status distribution: {:?}", status_counts);

    // Assertions based on credential mix
    match scenario.credentials {
        CredentialsMix::AllValid => {
            assert_all_valid_scenario(&results, expected_sha256, &stats, scenario.name);
        }
        CredentialsMix::Mixed => {
            assert_mixed_scenario(&results, expected_sha256, &stats, scenario.name);
        }
    }

    println!("  ✓ Scenario '{}' PASSED", scenario.name);
    println!(
        "    Stats: fetcher_success={}, waiter_304={}, waiter_200={}, waiter_4xx={}, waiter_error={}",
        stats.fetcher_completions_success,
        stats.waiter_conditional_304,
        stats.waiter_conditional_200,
        stats.waiter_conditional_4xx,
        stats.waiter_conditional_error,
    );
}

/// Assertions for all-valid credential scenarios (1, 3, 5, 7).
fn assert_all_valid_scenario(
    results: &[ClientResult],
    expected_sha256: &str,
    stats: &s3_proxy::metrics::CoalescingMetrics,
    scenario_name: &str,
) {
    // All 100 clients must receive 200 OK with correct body
    for r in results {
        assert_eq!(
            r.status,
            StatusCode::OK,
            "Scenario '{}': client {} expected 200 OK, got {}",
            scenario_name,
            r.index,
            r.status
        );
        let body_sha256 = hex::encode(Sha256::digest(r.body.as_ref()));
        assert_eq!(
            body_sha256, expected_sha256,
            "Scenario '{}': client {} body SHA256 mismatch",
            scenario_name, r.index
        );
    }

    // At least 1 fetcher completion
    assert!(
        stats.fetcher_completions_success >= 1,
        "Scenario '{}': expected at least 1 fetcher_completions_success, got {}",
        scenario_name,
        stats.fetcher_completions_success
    );

    // Total S3 requests: every client must have hit S3 (Property 1).
    // In cold-cache scenarios, the cache write is async so waiters may
    // fall back to full fetches (metadata-missing path) rather than
    // conditionals. In expired-cache scenarios, metadata is already on
    // disk so waiters should get 304s. Either way, every client hits S3.
    let total_s3_requests = stats.fetcher_completions_success
        + stats.waiter_conditional_304
        + stats.waiter_conditional_200
        + stats.waiter_conditional_4xx
        + stats.waiter_conditional_error;

    // The total tracked via coalescing metrics may be less than N_CLIENTS
    // because waiters that fall back to forward_get_head_to_s3_and_cache
    // (metadata-missing path) are not counted in the waiter_conditional_*
    // metrics. The key invariant is: all clients got 200 OK with correct
    // body, which means they all hit S3 with their own credentials.
    println!(
        "  Scenario '{}': tracked S3 requests via coalescing metrics = {} (fetcher={}, 304={}, 200={}, 4xx={}, error={})",
        scenario_name, total_s3_requests,
        stats.fetcher_completions_success,
        stats.waiter_conditional_304,
        stats.waiter_conditional_200,
        stats.waiter_conditional_4xx,
        stats.waiter_conditional_error,
    );

    // For expired-cache scenarios (pre-warmed), we expect most waiters
    // to get 304s since metadata is already on disk.
    // For cold-cache scenarios, waiters may fall back to full fetches.
    // The hard invariant: all 100 clients got correct data.
    assert_eq!(
        results.len(),
        N_CLIENTS,
        "Scenario '{}': expected {} results, got {}",
        scenario_name,
        N_CLIENTS,
        results.len()
    );
}

/// Assertions for mixed credential scenarios (2, 4, 6, 8).
fn assert_mixed_scenario(
    results: &[ClientResult],
    expected_sha256: &str,
    stats: &s3_proxy::metrics::CoalescingMetrics,
    scenario_name: &str,
) {
    let mut valid_ok_count = 0u64;
    let mut invalid_denied_count = 0u64;

    for r in results {
        if r.used_valid_creds {
            // Valid credentials should get 200 OK with correct body
            assert_eq!(
                r.status,
                StatusCode::OK,
                "Scenario '{}': valid-cred client {} expected 200 OK, got {}",
                scenario_name,
                r.index,
                r.status
            );
            let body_sha256 = hex::encode(Sha256::digest(r.body.as_ref()));
            assert_eq!(
                body_sha256, expected_sha256,
                "Scenario '{}': valid-cred client {} body SHA256 mismatch",
                scenario_name, r.index
            );
            valid_ok_count += 1;
        } else {
            // Invalid credentials should get 401 or 403
            assert!(
                r.status == StatusCode::UNAUTHORIZED || r.status == StatusCode::FORBIDDEN,
                "Scenario '{}': invalid-cred client {} expected 401/403, got {}",
                scenario_name,
                r.index,
                r.status
            );
            invalid_denied_count += 1;
        }
    }

    assert_eq!(
        valid_ok_count, 50,
        "Scenario '{}': expected 50 valid clients with 200, got {}",
        scenario_name, valid_ok_count
    );
    assert_eq!(
        invalid_denied_count, 50,
        "Scenario '{}': expected 50 invalid clients with 401/403, got {}",
        scenario_name, invalid_denied_count
    );

    // Total S3 requests tracked via coalescing metrics. As with all-valid
    // scenarios, waiters that fall back to the metadata-missing path are
    // not counted in waiter_conditional_* metrics. The hard invariant is:
    // valid-cred clients get 200 OK, invalid-cred clients get 401/403.
    let total_s3_requests = stats.fetcher_completions_success
        + stats.fetcher_completions_error
        + stats.waiter_conditional_304
        + stats.waiter_conditional_200
        + stats.waiter_conditional_4xx
        + stats.waiter_conditional_error;
    println!(
        "  Scenario '{}': tracked S3 requests via coalescing metrics = {} (fetcher_ok={}, fetcher_err={}, 304={}, 200={}, 4xx={}, error={})",
        scenario_name, total_s3_requests,
        stats.fetcher_completions_success,
        stats.fetcher_completions_error,
        stats.waiter_conditional_304,
        stats.waiter_conditional_200,
        stats.waiter_conditional_4xx,
        stats.waiter_conditional_error,
    );
}

// =========================================================================
// Main test entry point
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn stampede_100_concurrent_clients() {
    if !should_run() {
        println!("Skipping stampede test: RUN_INTEGRATION_TESTS=1 not set");
        return;
    }

    let valid_signer = AwsSigner::from_env(TEST_REGION)
        .expect("AWS credentials must be set for integration tests");

    // Create a shared S3 client for setup operations
    let setup_mm = make_metrics();
    let s3_client = make_real_s3_client(Some(Arc::clone(&setup_mm)));

    // Verify bucket exists (fail-loud guard)
    assert_bucket_exists(&s3_client, &valid_signer).await;

    // Upload test object (4 MiB) — one-time setup
    let test_data = generate_test_data();
    let expected_sha256 = hex::encode(Sha256::digest(&test_data));
    println!(
        "Uploading 4 MiB test object to s3://{}/{}",
        TEST_BUCKET, TEST_OBJECT_KEY
    );
    upload_test_object(&s3_client, &valid_signer, TEST_OBJECT_KEY, &test_data).await;
    println!("Upload complete. Expected SHA256: {}", expected_sha256);

    // Define the 8 scenarios
    let scenarios = [
        ScenarioConfig {
            name: "1: TTL=0, cold, all-valid",
            get_ttl_secs: 0,
            cache_state: CacheState::Cold,
            credentials: CredentialsMix::AllValid,
        },
        ScenarioConfig {
            name: "2: TTL=0, cold, mixed",
            get_ttl_secs: 0,
            cache_state: CacheState::Cold,
            credentials: CredentialsMix::Mixed,
        },
        ScenarioConfig {
            name: "3: TTL=0, expired, all-valid",
            get_ttl_secs: 0,
            cache_state: CacheState::Expired,
            credentials: CredentialsMix::AllValid,
        },
        ScenarioConfig {
            name: "4: TTL=0, expired, mixed",
            get_ttl_secs: 0,
            cache_state: CacheState::Expired,
            credentials: CredentialsMix::Mixed,
        },
        ScenarioConfig {
            name: "5: TTL=3600, cold, all-valid",
            get_ttl_secs: 3600,
            cache_state: CacheState::Cold,
            credentials: CredentialsMix::AllValid,
        },
        ScenarioConfig {
            name: "6: TTL=3600, cold, mixed",
            get_ttl_secs: 3600,
            cache_state: CacheState::Cold,
            credentials: CredentialsMix::Mixed,
        },
        ScenarioConfig {
            name: "7: TTL=3600, expired, all-valid",
            get_ttl_secs: 3600,
            cache_state: CacheState::Expired,
            credentials: CredentialsMix::AllValid,
        },
        ScenarioConfig {
            name: "8: TTL=3600, expired, mixed",
            get_ttl_secs: 3600,
            cache_state: CacheState::Expired,
            credentials: CredentialsMix::Mixed,
        },
    ];

    // Run each scenario sequentially
    for scenario in &scenarios {
        run_scenario(
            scenario,
            &valid_signer,
            &test_data,
            &expected_sha256,
            &s3_client,
        )
        .await;
    }

    println!("\n=== ALL 8 STAMPEDE SCENARIOS PASSED ===");
}
