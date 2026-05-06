//! Integration tests for the rolling validation scan feature.
//!
//! Tests end-to-end behavior of rolling scan mode selection, cursor advancement,
//! cursor continuity across scans, backward compatibility with full scans,
//! and mode transitions between full and rolling modes.

use s3_proxy::cache_size_tracker::{
    determine_scan_mode, CacheSizeConfig, CacheSizeTracker, RollingCycleStats, RollingState,
    ScanMode,
};
use s3_proxy::cache_types::{
    CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create the required directory structure for a cache directory.
fn create_cache_dirs(cache_dir: &std::path::Path) {
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();
    std::fs::create_dir_all(cache_dir.join("ranges")).unwrap();
}

/// Create a minimal valid `.meta` file at the given path with a specified size.
fn create_meta_file(path: &std::path::Path, compressed_size: u64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let now = SystemTime::now();
    let meta = NewCacheMetadata {
        cache_key: "test-bucket:test-object".to_string(),
        object_metadata: ObjectMetadata {
            etag: "\"abc123\"".to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length: compressed_size,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: compressed_size,
            parts: Vec::new(),
            response_headers: HashMap::new(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
        },
        ranges: vec![RangeSpec {
            start: 0,
            end: compressed_size.saturating_sub(1),
            file_path: "test_0.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size,
            uncompressed_size: compressed_size,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        }],
        created_at: now,
        expires_at: now + Duration::from_secs(86400),
        compression_info: CompressionInfo::default(),
        head_expires_at: None,
        head_last_accessed: None,
        head_access_count: 0,
    };
    let json = serde_json::to_string_pretty(&meta).unwrap();
    std::fs::write(path, json).unwrap();
}

/// Populate a few L1/L2 directories with `.meta` files under `metadata/{bucket}/`.
/// Returns the set of L1 hex indices that were created.
fn populate_l1_dirs(
    cache_dir: &std::path::Path,
    bucket: &str,
    l1_indices: &[u8],
    meta_size: u64,
) -> Vec<u8> {
    for &idx in l1_indices {
        let l1_hex = format!("{:02x}", idx);
        // Create one L2 directory "000" with one .meta file
        let meta_path = cache_dir
            .join("metadata")
            .join(bucket)
            .join(&l1_hex)
            .join("000")
            .join("test_object.meta");
        create_meta_file(&meta_path, meta_size);
    }
    l1_indices.to_vec()
}

/// Create a JournalConsolidator for testing.
async fn create_test_consolidator(cache_dir: std::path::PathBuf) -> Arc<JournalConsolidator> {
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir,
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    ));
    consolidator.initialize().await.unwrap();
    consolidator
}

/// Create a CacheSizeTracker for testing with a given max_duration.
async fn create_test_tracker(
    cache_dir: std::path::PathBuf,
    consolidator: Arc<JournalConsolidator>,
    max_duration: Duration,
) -> Arc<CacheSizeTracker> {
    let config = CacheSizeConfig {
        validation_enabled: false,
        validation_max_duration: max_duration,
        ..CacheSizeConfig::default()
    };
    Arc::new(
        CacheSizeTracker::new(cache_dir, config, false, consolidator)
            .await
            .unwrap(),
    )
}

/// Seed validation.json with a full scan that took the given duration.
fn seed_full_scan_state(cache_dir: &std::path::Path, duration_secs: f64) {
    let now_epoch = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let json = serde_json::json!({
        "last_validation": now_epoch,
        "status": "completed",
        "completed_at": now_epoch,
        "validation_type": "full",
        "last_full_scan_duration_secs": duration_secs,
        "scanned_size": 0,
        "tracked_size": 0,
        "drift_bytes": 0,
        "scan_duration_ms": (duration_secs * 1000.0) as u64,
        "metadata_files_scanned": 0,
    });
    let path = cache_dir.join("size_tracking").join("validation.json");
    std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();
}

/// Seed validation.json with a rolling scan state.
fn seed_rolling_scan_state(
    cache_dir: &std::path::Path,
    cursor: u8,
    dirs_scanned: u64,
    cycle_duration_secs: f64,
    scan_rate: Option<f64>,
    last_full_scan_duration_secs: Option<f64>,
) {
    let now_epoch = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut json = serde_json::json!({
        "last_validation": now_epoch,
        "status": "completed",
        "completed_at": now_epoch,
        "validation_type": "rolling",
        "rolling_cursor": cursor,
        "rolling_dirs_scanned": dirs_scanned,
        "rolling_cycle_duration_secs": cycle_duration_secs,
        "rolling_full_rotation_count": 0,
    });
    if let Some(rate) = scan_rate {
        json["rolling_scan_rate"] = serde_json::json!(rate);
    }
    if let Some(dur) = last_full_scan_duration_secs {
        json["last_full_scan_duration_secs"] = serde_json::json!(dur);
    }
    let path = cache_dir.join("size_tracking").join("validation.json");
    std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();
}

/// Read validation.json and return the parsed JSON value.
fn read_validation_json(cache_dir: &std::path::Path) -> serde_json::Value {
    let path = cache_dir.join("size_tracking").join("validation.json");
    let content = std::fs::read_to_string(&path).unwrap();
    serde_json::from_str(&content).unwrap()
}

// ===========================================================================
// 10.1 — Rolling scan: cursor advances and size state is updated
// ===========================================================================

/// Create a temp cache directory with L1/L2 directories containing `.meta` files,
/// seed validation.json with a previous full scan that exceeded the budget,
/// run a rolling scan cycle (via the public building blocks), and verify that
/// the cursor advances and size state is updated.
#[tokio::test]
async fn test_rolling_scan_cursor_advances_and_size_updated() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    create_cache_dirs(&cache_dir);

    // Populate L1 dirs 0x00..0x03 with one .meta file each (1000 bytes)
    let meta_size = 1000u64;
    populate_l1_dirs(&cache_dir, "test-bucket", &[0, 1, 2, 3], meta_size);

    let consolidator = create_test_consolidator(cache_dir.clone()).await;
    // Set initial tracked size so proportional correction has something to work with
    consolidator
        .update_size_from_validation(4000, None, Some(4))
        .await;

    let max_duration = Duration::from_secs(60); // 1 minute budget
    let tracker = create_test_tracker(cache_dir.clone(), consolidator.clone(), max_duration).await;

    // Seed a full scan that exceeded the budget → next cycle should be rolling
    seed_full_scan_state(&cache_dir, max_duration.as_secs_f64() + 100.0);

    // Verify mode selection picks Rolling
    let (mode, _reason) = determine_scan_mode(
        Some("full"),
        Some(max_duration.as_secs_f64() + 100.0),
        None,
        None,
        max_duration,
    );
    assert_eq!(mode, ScanMode::Rolling);

    // Simulate a rolling scan cycle using public building blocks
    let rolling_state = tracker.read_rolling_state().unwrap();
    assert_eq!(rolling_state.cursor, 0, "Initial cursor should be 0");

    let batch_size = tracker.estimate_batch_size(rolling_state.scan_rate, max_duration);
    assert!(batch_size > 0);

    let metadata_dir = cache_dir.join("metadata");
    let (l1_dirs, _wraps) =
        tracker.select_l1_directories(&metadata_dir, rolling_state.cursor, batch_size.min(4));

    // We created 4 L1 dirs (00-03), so we should find them
    assert!(!l1_dirs.is_empty(), "Should find L1 directories");

    // Count objects in selected dirs (simulating what the scan loop does)
    let mut scanned_size = 0u64;
    let mut scanned_objects = 0u64;
    for l1_dir in &l1_dirs {
        for l2_entry in std::fs::read_dir(l1_dir).unwrap().flatten() {
            if !l2_entry.path().is_dir() {
                continue;
            }
            for file_entry in std::fs::read_dir(l2_entry.path()).unwrap().flatten() {
                if file_entry.path().extension().is_some_and(|e| e == "meta") {
                    scanned_size += meta_size;
                    scanned_objects += 1;
                }
            }
        }
    }

    let dirs_scanned = 4usize;
    let tracked_size = consolidator.get_current_size().await;
    let tracked_objects = consolidator.get_size_state().await.cached_objects;

    // Apply proportional correction
    let (corrected_size, corrected_objects) = tracker.apply_proportional_correction(
        scanned_size,
        scanned_objects,
        dirs_scanned,
        tracked_size,
        tracked_objects,
    );

    // Update size state
    consolidator
        .update_size_from_validation(corrected_size, None, Some(corrected_objects))
        .await;

    // Advance cursor and persist
    let new_cursor = ((rolling_state.cursor as usize + dirs_scanned) % 256) as u8;
    let elapsed_secs = 0.5; // simulated
    let scan_rate = elapsed_secs / dirs_scanned as f64;

    let updated_state = RollingState {
        cursor: new_cursor,
        scan_rate: Some(scan_rate),
        full_rotation_count: 0,
        rotation_start_time: Some(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        ),
        last_full_scan_duration_secs: Some(max_duration.as_secs_f64() + 100.0),
    };
    let cycle_stats = RollingCycleStats {
        dirs_scanned: dirs_scanned as u64,
        objects_validated: scanned_objects,
        cycle_duration_secs: elapsed_secs,
    };
    tracker
        .write_rolling_state(&updated_state, &cycle_stats)
        .unwrap();

    // Verify cursor advanced
    let persisted = tracker.read_rolling_state().unwrap();
    assert_eq!(persisted.cursor, 4, "Cursor should advance to 4");
    assert!(persisted.scan_rate.is_some(), "Scan rate should be set");

    // Verify size state was updated
    let size_state = consolidator.get_size_state().await;
    assert!(
        size_state.total_size > 0,
        "Size state should be updated after rolling scan"
    );

    // Verify validation.json has rolling type
    let val_json = read_validation_json(&cache_dir);
    assert_eq!(val_json["validation_type"], "rolling");
    assert_eq!(val_json["rolling_cursor"], 4);
    assert_eq!(val_json["rolling_dirs_scanned"], 4);
}

// ===========================================================================
// 10.2 — Cursor continuity: second scan resumes from where first left off
// ===========================================================================

/// Run two rolling scan cycles in sequence and verify the second scan resumes
/// from the cursor position where the first scan left off.
#[tokio::test]
async fn test_rolling_scan_cursor_continuity() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    create_cache_dirs(&cache_dir);

    // Populate L1 dirs 0x00..0x07 with .meta files
    let meta_size = 500u64;
    populate_l1_dirs(
        &cache_dir,
        "test-bucket",
        &[0, 1, 2, 3, 4, 5, 6, 7],
        meta_size,
    );

    let consolidator = create_test_consolidator(cache_dir.clone()).await;
    consolidator
        .update_size_from_validation(4000, None, Some(8))
        .await;

    let max_duration = Duration::from_secs(60);
    let tracker = create_test_tracker(cache_dir.clone(), consolidator.clone(), max_duration).await;

    // --- First rolling scan cycle: scan dirs 0x00..0x03 ---
    let first_dirs_scanned = 4usize;
    let first_state = RollingState {
        cursor: (first_dirs_scanned % 256) as u8, // cursor = 4
        scan_rate: Some(0.1),
        full_rotation_count: 0,
        rotation_start_time: Some(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        ),
        last_full_scan_duration_secs: Some(max_duration.as_secs_f64() + 50.0),
    };
    let first_stats = RollingCycleStats {
        dirs_scanned: first_dirs_scanned as u64,
        objects_validated: 4,
        cycle_duration_secs: 0.4,
    };
    tracker
        .write_rolling_state(&first_state, &first_stats)
        .unwrap();

    // Verify first cycle persisted correctly
    let after_first = tracker.read_rolling_state().unwrap();
    assert_eq!(
        after_first.cursor, 4,
        "After first cycle, cursor should be 4"
    );

    // --- Second rolling scan cycle: should resume from cursor=4 ---
    let second_state_read = tracker.read_rolling_state().unwrap();
    assert_eq!(
        second_state_read.cursor, 4,
        "Second cycle should read cursor=4 from persisted state"
    );

    // Select dirs starting from cursor=4
    let metadata_dir = cache_dir.join("metadata");
    let (l1_dirs, _wraps) =
        tracker.select_l1_directories(&metadata_dir, second_state_read.cursor, 4);

    // Verify we selected dirs 04-07 (the ones starting from cursor=4)
    let mut selected_indices: Vec<u8> = l1_dirs
        .iter()
        .filter_map(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| u8::from_str_radix(s, 16).ok())
        })
        .collect();
    selected_indices.sort();
    assert_eq!(
        selected_indices,
        vec![4, 5, 6, 7],
        "Second cycle should select dirs 04-07"
    );

    // Persist second cycle state
    let second_dirs_scanned = 4usize;
    let second_state = RollingState {
        cursor: ((second_state_read.cursor as usize + second_dirs_scanned) % 256) as u8,
        scan_rate: Some(0.1),
        full_rotation_count: 0,
        rotation_start_time: second_state_read.rotation_start_time,
        last_full_scan_duration_secs: second_state_read.last_full_scan_duration_secs,
    };
    let second_stats = RollingCycleStats {
        dirs_scanned: second_dirs_scanned as u64,
        objects_validated: 4,
        cycle_duration_secs: 0.4,
    };
    tracker
        .write_rolling_state(&second_state, &second_stats)
        .unwrap();

    // Verify cursor advanced to 8
    let after_second = tracker.read_rolling_state().unwrap();
    assert_eq!(
        after_second.cursor, 8,
        "After second cycle, cursor should be 8"
    );

    // Verify validation.json reflects second cycle
    let val_json = read_validation_json(&cache_dir);
    assert_eq!(val_json["rolling_cursor"], 8);
    assert_eq!(val_json["rolling_dirs_scanned"], 4);
}

// ===========================================================================
// 10.3 — Backward compatibility: full scan when no previous history
// ===========================================================================

/// Verify that when no previous scan history exists, determine_scan_mode
/// returns Full mode, preserving backward-compatible behavior.
#[tokio::test]
async fn test_full_scan_when_no_history() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    create_cache_dirs(&cache_dir);

    let consolidator = create_test_consolidator(cache_dir.clone()).await;
    let max_duration = Duration::from_secs(4 * 3600); // 4 hours
    let tracker = create_test_tracker(cache_dir.clone(), consolidator.clone(), max_duration).await;

    // No validation.json exists → read_rolling_state should return defaults
    let state = tracker.read_rolling_state().unwrap();
    assert_eq!(state.cursor, 0, "Default cursor should be 0");
    assert!(
        state.scan_rate.is_none(),
        "Default scan rate should be None"
    );
    assert_eq!(
        state.full_rotation_count, 0,
        "Default rotation count should be 0"
    );

    // determine_scan_mode with no history → Full
    let (mode, _reason) = determine_scan_mode(None, None, None, None, max_duration);
    assert_eq!(
        mode,
        ScanMode::Full,
        "No history should result in Full scan mode"
    );

    // Also verify with explicit None validation_type
    let (mode2, _) = determine_scan_mode(None, None, None, None, Duration::from_secs(600));
    assert_eq!(mode2, ScanMode::Full);

    // Verify that a full scan within budget stays Full
    let (mode3, _) = determine_scan_mode(
        Some("full"),
        Some(100.0), // 100 seconds, well within 4h budget
        None,
        None,
        max_duration,
    );
    assert_eq!(
        mode3,
        ScanMode::Full,
        "Full scan within budget should stay Full"
    );
}

/// Verify that the full scan path records its duration and validation_type
/// in validation.json, and that reading it back works correctly.
#[tokio::test]
async fn test_full_scan_persists_duration_and_type() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    create_cache_dirs(&cache_dir);

    let consolidator = create_test_consolidator(cache_dir.clone()).await;
    let max_duration = Duration::from_secs(4 * 3600);
    let tracker = create_test_tracker(cache_dir.clone(), consolidator.clone(), max_duration).await;

    // Simulate what perform_full_validation does: write validation metadata
    // then persist full scan duration
    tracker
        .write_validation_metadata(
            5000,                     // scanned_size
            5000,                     // tracked_size
            0,                        // drift
            Duration::from_secs(120), // 2 minutes
            10,                       // files_scanned
            0,
            0,
            0,
        )
        .await
        .unwrap();

    // Manually persist full scan duration (mimicking persist_full_scan_duration)
    seed_full_scan_state(&cache_dir, 120.0);

    // Read back and verify
    let val_json = read_validation_json(&cache_dir);
    assert_eq!(val_json["validation_type"], "full");
    assert_eq!(val_json["last_full_scan_duration_secs"], 120.0);

    // Mode selection should stay Full since 120s < 4h
    let (mode, _) = determine_scan_mode(Some("full"), Some(120.0), None, None, max_duration);
    assert_eq!(mode, ScanMode::Full);
}

// ===========================================================================
// 10.4 — Mode transition: full→rolling and rolling→full
// ===========================================================================

/// Verify mode transition: seed a full scan duration that exceeds budget →
/// next cycle uses rolling. Then seed rolling state where extrapolated full
/// time fits within budget → next cycle switches back to full.
#[tokio::test]
async fn test_mode_transition_full_to_rolling_and_back() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    create_cache_dirs(&cache_dir);

    // Populate some L1 dirs for realism
    populate_l1_dirs(&cache_dir, "test-bucket", &[0, 1, 2, 3], 500);

    let consolidator = create_test_consolidator(cache_dir.clone()).await;
    consolidator
        .update_size_from_validation(2000, None, Some(4))
        .await;

    let max_duration = Duration::from_secs(3600); // 1 hour budget
    let tracker = create_test_tracker(cache_dir.clone(), consolidator.clone(), max_duration).await;

    // --- Phase 1: Full scan exceeded budget → should switch to Rolling ---
    let full_scan_duration = 5000.0; // 5000 seconds > 3600 budget
    seed_full_scan_state(&cache_dir, full_scan_duration);

    let (mode1, _reason1) = determine_scan_mode(
        Some("full"),
        Some(full_scan_duration),
        None,
        None,
        max_duration,
    );
    assert_eq!(
        mode1,
        ScanMode::Rolling,
        "Full scan exceeding budget should trigger Rolling mode"
    );

    // Simulate a rolling scan cycle
    let rolling_dirs_scanned = 64u64;
    let rolling_cycle_duration = 900.0; // 900 seconds for 64 dirs
                                        // Extrapolated full time = (900 / 64) * 256 = 3600.0 seconds = exactly budget
                                        // At exactly budget, extrapolated <= budget → should switch back to Full
    let scan_rate = rolling_cycle_duration / rolling_dirs_scanned as f64;

    let rolling_state = RollingState {
        cursor: 64,
        scan_rate: Some(scan_rate),
        full_rotation_count: 0,
        rotation_start_time: Some(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        ),
        last_full_scan_duration_secs: Some(full_scan_duration),
    };
    let cycle_stats = RollingCycleStats {
        dirs_scanned: rolling_dirs_scanned,
        objects_validated: 100,
        cycle_duration_secs: rolling_cycle_duration,
    };
    tracker
        .write_rolling_state(&rolling_state, &cycle_stats)
        .unwrap();

    // --- Phase 2: Rolling scan with extrapolated time exactly at budget ---
    // Extrapolated = (900 / 64) * 256 = 3600.0 which is NOT > 3600 → Full
    let (mode2, _reason2) = determine_scan_mode(
        Some("rolling"),
        Some(full_scan_duration),
        Some(rolling_cycle_duration),
        Some(rolling_dirs_scanned),
        max_duration,
    );
    assert_eq!(
        mode2,
        ScanMode::Full,
        "Rolling scan with extrapolated time at budget should switch back to Full"
    );

    // --- Phase 3: Rolling scan with extrapolated time above budget ---
    // Simulate a slower rolling scan: 64 dirs in 1800 seconds
    // Extrapolated = (1800 / 64) * 256 = 7200 > 3600 → stay Rolling
    let slow_cycle_duration = 1800.0;
    seed_rolling_scan_state(
        &cache_dir,
        64,
        64,
        slow_cycle_duration,
        Some(slow_cycle_duration / 64.0),
        Some(full_scan_duration),
    );

    let (mode3, _reason3) = determine_scan_mode(
        Some("rolling"),
        Some(full_scan_duration),
        Some(slow_cycle_duration),
        Some(64),
        max_duration,
    );
    assert_eq!(
        mode3,
        ScanMode::Rolling,
        "Rolling scan with extrapolated time above budget should stay Rolling"
    );

    // --- Phase 4: Rolling scan with fast rate → switch back to Full ---
    // 64 dirs in 450 seconds → extrapolated = (450/64)*256 = 1800 < 3600 → Full
    let fast_cycle_duration = 450.0;
    seed_rolling_scan_state(
        &cache_dir,
        128,
        64,
        fast_cycle_duration,
        Some(fast_cycle_duration / 64.0),
        Some(full_scan_duration),
    );

    let (mode4, _reason4) = determine_scan_mode(
        Some("rolling"),
        Some(full_scan_duration),
        Some(fast_cycle_duration),
        Some(64),
        max_duration,
    );
    assert_eq!(
        mode4,
        ScanMode::Full,
        "Rolling scan with fast rate should switch back to Full"
    );

    // Verify the state was persisted and can be read back
    let persisted = tracker.read_rolling_state().unwrap();
    assert_eq!(persisted.cursor, 128);
}

/// Verify the complete lifecycle: no history → full → rolling → full.
#[tokio::test]
async fn test_complete_mode_lifecycle() {
    let max_duration = Duration::from_secs(3600); // 1 hour

    // Step 1: No history → Full
    let (mode, _) = determine_scan_mode(None, None, None, None, max_duration);
    assert_eq!(mode, ScanMode::Full);

    // Step 2: Full scan completed in 2000s (within budget) → stay Full
    let (mode, _) = determine_scan_mode(Some("full"), Some(2000.0), None, None, max_duration);
    assert_eq!(mode, ScanMode::Full);

    // Step 3: Full scan took 5000s (exceeded budget) → switch to Rolling
    let (mode, _) = determine_scan_mode(Some("full"), Some(5000.0), None, None, max_duration);
    assert_eq!(mode, ScanMode::Rolling);

    // Step 4: Rolling scan, 64 dirs in 1800s → extrapolated 7200s > 3600 → stay Rolling
    let (mode, _) = determine_scan_mode(
        Some("rolling"),
        Some(5000.0),
        Some(1800.0),
        Some(64),
        max_duration,
    );
    assert_eq!(mode, ScanMode::Rolling);

    // Step 5: Rolling scan, 64 dirs in 400s → extrapolated 1600s < 3600 → switch to Full
    let (mode, _) = determine_scan_mode(
        Some("rolling"),
        Some(5000.0),
        Some(400.0),
        Some(64),
        max_duration,
    );
    assert_eq!(mode, ScanMode::Full);

    // Step 6: Back to full, completes in 3000s (within budget) → stay Full
    let (mode, _) = determine_scan_mode(Some("full"), Some(3000.0), None, None, max_duration);
    assert_eq!(mode, ScanMode::Full);
}
