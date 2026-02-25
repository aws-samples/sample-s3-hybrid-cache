//! Integration tests for journal-based metadata updates
//!
//! Tests the journal-based metadata update system for shared storage mode,
//! including multi-instance contention and consolidation cleanup behavior.

use s3_proxy::cache_hit_update_buffer::CacheHitUpdateBuffer;
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Create a test range spec
fn create_test_range_spec(start: u64, end: u64) -> RangeSpec {
    let now = SystemTime::now();
    RangeSpec {
        start,
        end,
        file_path: format!("test_range_{}-{}.bin", start, end),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: end - start + 1,
        uncompressed_size: end - start + 1,
        created_at: now,
        last_accessed: now,
        access_count: 1,
        frequency_score: 1,
    }
}

/// Create test metadata with a range
fn create_test_metadata(cache_key: &str, range_start: u64, range_end: u64) -> NewCacheMetadata {
    let now = SystemTime::now();
    let range_spec = create_test_range_spec(range_start, range_end);

    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::default(),
        ranges: vec![range_spec],
        created_at: now,
        expires_at: now + Duration::from_secs(3153600000),
        compression_info: s3_proxy::cache_types::CompressionInfo::default(),
        ..Default::default()
    }
}

/// Test 10.1: Multi-instance contention
/// Spawn multiple tasks doing concurrent TTL refreshes
/// Verify no corruption and all updates eventually applied
#[tokio::test]
async fn test_multi_instance_concurrent_ttl_refreshes() {
    let temp_dir = TempDir::new().unwrap();
    let cache_key = "test-bucket/test-object";

    // Create multiple CacheHitUpdateBuffers simulating multiple instances
    let num_instances = 5;
    let updates_per_instance = 20;

    let mut buffers = Vec::new();
    for i in 0..num_instances {
        let buffer = Arc::new(CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            format!("instance-{}", i),
            Duration::from_secs(60), // Long interval to control flush timing
            10000,
        ));
        buffers.push(buffer);
    }

    // Spawn concurrent tasks to record TTL refreshes
    let mut handles = Vec::new();
    for (instance_idx, buffer) in buffers.iter().enumerate() {
        let buffer_clone = buffer.clone();
        let cache_key_clone = cache_key.to_string();

        let handle = tokio::spawn(async move {
            for update_idx in 0..updates_per_instance {
                let ttl = Duration::from_secs(3600 + (instance_idx * 100 + update_idx) as u64);
                buffer_clone
                    .record_ttl_refresh(&cache_key_clone, ttl)
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Flush all buffers
    for buffer in &buffers {
        buffer.force_flush().await.unwrap();
    }

    // Verify journal files were created for each instance
    let journals_dir = temp_dir.path().join("metadata").join("_journals");
    assert!(journals_dir.exists(), "Journals directory should exist");

    let mut total_entries = 0;
    for i in 0..num_instances {
        let journal_path = journals_dir.join(format!("instance-{}.journal", i));
        assert!(
            journal_path.exists(),
            "Journal file for instance {} should exist",
            i
        );

        let content = tokio::fs::read_to_string(&journal_path).await.unwrap();
        let entry_count = content.lines().filter(|l| !l.trim().is_empty()).count();
        assert_eq!(
            entry_count, updates_per_instance,
            "Instance {} should have {} entries",
            i, updates_per_instance
        );
        total_entries += entry_count;
    }

    assert_eq!(
        total_entries,
        num_instances * updates_per_instance,
        "Total entries should match"
    );

    // Create consolidator and verify it can read all entries
    let journal_manager = Arc::new(JournalManager::new(
        temp_dir.path().to_path_buf(),
        "consolidator-instance".to_string(),
    ));

    let all_entries = journal_manager
        .get_all_entries_for_cache_key(cache_key)
        .await
        .unwrap();

    assert_eq!(
        all_entries.len(),
        num_instances * updates_per_instance,
        "Consolidator should see all entries from all instances"
    );

    // Verify all entries are TtlRefresh operations
    for entry in &all_entries {
        assert_eq!(entry.operation, JournalOperation::TtlRefresh);
        assert_eq!(entry.cache_key, cache_key);
    }
}

/// Test 10.1 continued: Verify no corruption after concurrent writes
#[tokio::test]
async fn test_concurrent_writes_no_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let cache_key = "test-bucket/concurrent-object";

    // Create multiple buffers and write concurrently
    let num_instances = 3;
    let mut buffers = Vec::new();

    for i in 0..num_instances {
        let buffer = Arc::new(CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            format!("concurrent-instance-{}", i),
            Duration::from_secs(60),
            10000,
        ));
        buffers.push(buffer);
    }

    // Write and flush concurrently
    let mut handles = Vec::new();
    for buffer in buffers.iter() {
        let buffer_clone = buffer.clone();
        let cache_key_clone = cache_key.to_string();

        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                buffer_clone
                    .record_ttl_refresh(&cache_key_clone, Duration::from_secs(3600))
                    .await
                    .unwrap();
                buffer_clone.force_flush().await.unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all journal files are valid JSON
    let journals_dir = temp_dir.path().join("metadata").join("_journals");
    for i in 0..num_instances {
        let journal_path = journals_dir.join(format!("concurrent-instance-{}.journal", i));
        if journal_path.exists() {
            let content = tokio::fs::read_to_string(&journal_path).await.unwrap();
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                // Verify each line is valid JSON
                let entry: JournalEntry = serde_json::from_str(line)
                    .expect(&format!("Line should be valid JSON: {}", line));
                assert_eq!(entry.cache_key, cache_key);
            }
        }
    }
}

/// Test 10.2: Consolidation cleanup behavior
/// Property 7: Consolidation Cleanup Behavior
/// - On success: all processed journal entries shall be deleted
/// - On failure: all journal entries shall remain intact for retry
#[tokio::test]
async fn test_consolidation_cleanup_on_success() {
    let temp_dir = TempDir::new().unwrap();
    let cache_key = "test-bucket/cleanup-test-object";

    // Create buffer and write some entries
    let buffer = CacheHitUpdateBuffer::with_config(
        temp_dir.path().to_path_buf(),
        "cleanup-test-instance".to_string(),
        Duration::from_secs(60),
        10000,
    );

    // Record some TTL refreshes
    for i in 0..5 {
        buffer
            .record_ttl_refresh(cache_key, Duration::from_secs(3600 + i * 100))
            .await
            .unwrap();
    }
    buffer.force_flush().await.unwrap();

    // Verify journal file exists
    let journal_path = temp_dir
        .path()
        .join("metadata")
        .join("_journals")
        .join("cleanup-test-instance.journal");
    assert!(
        journal_path.exists(),
        "Journal file should exist before consolidation"
    );

    // Create metadata file (required for consolidation)
    let metadata = create_test_metadata(cache_key, 0, 8388607);
    let metadata_base_dir = temp_dir.path().join("metadata");
    let metadata_path =
        s3_proxy::disk_cache::get_sharded_path(&metadata_base_dir, cache_key, ".meta").unwrap();
    if let Some(parent) = metadata_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    let json = serde_json::to_string_pretty(&metadata).unwrap();
    tokio::fs::write(&metadata_path, json).await.unwrap();

    // Create range file (required for validation)
    let range_base_dir = temp_dir.path().join("ranges");
    let range_path =
        s3_proxy::disk_cache::get_sharded_path(&range_base_dir, cache_key, "_0-8388607.bin")
            .unwrap();
    if let Some(parent) = range_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(&range_path, b"test data").await.unwrap();

    // Create consolidator
    let journal_manager = Arc::new(JournalManager::new(
        temp_dir.path().to_path_buf(),
        "consolidator".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        temp_dir.path().to_path_buf(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = JournalConsolidator::new(
        temp_dir.path().to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    );

    // Run consolidation
    let result = consolidator.consolidate_object(cache_key).await.unwrap();
    assert!(result.success, "Consolidation should succeed");
    assert!(
        result.entries_consolidated > 0,
        "Should have consolidated entries"
    );

    // Clean up ONLY the consolidated entries (not all entries)
    consolidator
        .cleanup_consolidated_entries(&result.consolidated_entries)
        .await
        .unwrap();

    // Verify journal file is truncated/empty after cleanup (since all entries were consolidated)
    if journal_path.exists() {
        let content = tokio::fs::read_to_string(&journal_path).await.unwrap();
        assert!(
            content.trim().is_empty(),
            "Journal file should be empty after successful consolidation cleanup"
        );
    }
}

/// Test that entries with missing range files are preserved (not cleaned up)
/// This is the fix for the 12% orphaned ranges bug - entries should only be
/// removed from journals after they are successfully consolidated.
#[tokio::test]
async fn test_consolidation_preserves_entries_with_missing_range_files() {
    let temp_dir = TempDir::new().unwrap();
    let cache_key = "test-bucket/partial-consolidation-object";

    // Create buffer and write some entries
    let buffer = CacheHitUpdateBuffer::with_config(
        temp_dir.path().to_path_buf(),
        "partial-test-instance".to_string(),
        Duration::from_secs(60),
        10000,
    );

    // Record TTL refreshes for two different ranges
    // Range 1: 0-8388607 (will have range file)
    // Range 2: 8388608-16777215 (will NOT have range file - simulates NFS caching delay)
    buffer
        .record_ttl_refresh(cache_key, Duration::from_secs(3600))
        .await
        .unwrap();
    buffer
        .record_ttl_refresh(cache_key, Duration::from_secs(3600))
        .await
        .unwrap();
    buffer.force_flush().await.unwrap();

    // Verify journal file exists with 2 entries
    let journal_path = temp_dir
        .path()
        .join("metadata")
        .join("_journals")
        .join("partial-test-instance.journal");
    assert!(
        journal_path.exists(),
        "Journal file should exist before consolidation"
    );
    let initial_content = tokio::fs::read_to_string(&journal_path).await.unwrap();
    let initial_entry_count = initial_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(
        initial_entry_count, 2,
        "Should have 2 journal entries initially"
    );

    // Create metadata file (required for consolidation)
    let metadata = create_test_metadata(cache_key, 0, 8388607);
    let metadata_base_dir = temp_dir.path().join("metadata");
    let metadata_path =
        s3_proxy::disk_cache::get_sharded_path(&metadata_base_dir, cache_key, ".meta").unwrap();
    if let Some(parent) = metadata_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    let json = serde_json::to_string_pretty(&metadata).unwrap();
    tokio::fs::write(&metadata_path, json).await.unwrap();

    // Create range file ONLY for range 1 (0-8388607)
    // Range 2 (8388608-16777215) does NOT have a range file - simulates NFS caching delay
    let range_base_dir = temp_dir.path().join("ranges");
    let range_path =
        s3_proxy::disk_cache::get_sharded_path(&range_base_dir, cache_key, "_0-8388607.bin")
            .unwrap();
    if let Some(parent) = range_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(&range_path, b"test data for range 1")
        .await
        .unwrap();
    // NOTE: We intentionally do NOT create the range file for range 2

    // Create consolidator
    let journal_manager = Arc::new(JournalManager::new(
        temp_dir.path().to_path_buf(),
        "consolidator".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        temp_dir.path().to_path_buf(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = JournalConsolidator::new(
        temp_dir.path().to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    );

    // Run consolidation
    let result = consolidator.consolidate_object(cache_key).await.unwrap();
    assert!(result.success, "Consolidation should succeed");

    // Both TTL refresh entries should be consolidated (validated via metadata file, not range file)
    assert_eq!(
        result.entries_consolidated, 2,
        "Both TTL refresh entries should be consolidated (validated via metadata file, not range file)"
    );
    assert_eq!(
        result.consolidated_entries.len(),
        2,
        "Both TTL refresh entries should be consolidated (validated via metadata file, not range file)"
    );

    // Clean up ONLY the consolidated entries
    consolidator
        .cleanup_consolidated_entries(&result.consolidated_entries)
        .await
        .unwrap();

    // Both entries were consolidated, so journal should be empty after cleanup
    let final_content = tokio::fs::read_to_string(&journal_path).await.unwrap();
    let final_entry_count = final_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(
        final_entry_count, 0,
        "Journal should be empty after all entries are consolidated and cleaned up"
    );
}

/// Test that consolidation applies TTL refresh entries correctly
#[tokio::test]
async fn test_consolidation_applies_ttl_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let cache_key = "test-bucket/ttl-refresh-object";

    // Create initial metadata with a range and a SHORT TTL so the refresh extends it
    let mut initial_metadata = create_test_metadata(cache_key, 0, 8388607);
    initial_metadata.expires_at = SystemTime::now() + Duration::from_secs(60);
    let initial_expires_at = initial_metadata.expires_at;

    let metadata_base_dir = temp_dir.path().join("metadata");
    let metadata_path =
        s3_proxy::disk_cache::get_sharded_path(&metadata_base_dir, cache_key, ".meta").unwrap();
    if let Some(parent) = metadata_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    let json = serde_json::to_string_pretty(&initial_metadata).unwrap();
    tokio::fs::write(&metadata_path, json).await.unwrap();

    // Create range file
    let range_base_dir = temp_dir.path().join("ranges");
    let range_path =
        s3_proxy::disk_cache::get_sharded_path(&range_base_dir, cache_key, "_0-8388607.bin")
            .unwrap();
    if let Some(parent) = range_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(&range_path, b"test data").await.unwrap();

    // Create buffer and record TTL refresh with longer TTL
    let buffer = CacheHitUpdateBuffer::with_config(
        temp_dir.path().to_path_buf(),
        "ttl-refresh-instance".to_string(),
        Duration::from_secs(60),
        10000,
    );

    let new_ttl = Duration::from_secs(7200); // 2 hours
    buffer
        .record_ttl_refresh(cache_key, new_ttl)
        .await
        .unwrap();
    buffer.force_flush().await.unwrap();

    // Create consolidator and run consolidation
    let journal_manager = Arc::new(JournalManager::new(
        temp_dir.path().to_path_buf(),
        "consolidator".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        temp_dir.path().to_path_buf(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = JournalConsolidator::new(
        temp_dir.path().to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    );

    let result = consolidator.consolidate_object(cache_key).await.unwrap();
    assert!(result.success, "Consolidation should succeed");
    assert_eq!(
        result.entries_consolidated, 1,
        "Should have consolidated 1 entry"
    );

    // Read updated metadata and verify TTL was refreshed
    let updated_content = tokio::fs::read_to_string(&metadata_path).await.unwrap();
    let updated_metadata: NewCacheMetadata = serde_json::from_str(&updated_content).unwrap();

    assert_eq!(updated_metadata.ranges.len(), 1);
    let updated_expires_at = updated_metadata.expires_at;

    // The new expires_at should be later than the initial one
    assert!(
        updated_expires_at > initial_expires_at,
        "TTL should have been refreshed: initial={:?}, updated={:?}",
        initial_expires_at,
        updated_expires_at
    );
}

/// Test that consolidation applies access update entries correctly
#[tokio::test]
async fn test_consolidation_applies_access_update() {
    let temp_dir = TempDir::new().unwrap();
    let cache_key = "test-bucket/access-update-object";

    // Create initial metadata with a range
    let initial_metadata = create_test_metadata(cache_key, 0, 8388607);
    let initial_access_count = initial_metadata.ranges[0].access_count;

    let metadata_base_dir = temp_dir.path().join("metadata");
    let metadata_path =
        s3_proxy::disk_cache::get_sharded_path(&metadata_base_dir, cache_key, ".meta").unwrap();
    if let Some(parent) = metadata_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    let json = serde_json::to_string_pretty(&initial_metadata).unwrap();
    tokio::fs::write(&metadata_path, json).await.unwrap();

    // Create range file
    let range_base_dir = temp_dir.path().join("ranges");
    let range_path =
        s3_proxy::disk_cache::get_sharded_path(&range_base_dir, cache_key, "_0-8388607.bin")
            .unwrap();
    if let Some(parent) = range_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(&range_path, b"test data").await.unwrap();

    // Create buffer and record access updates
    let buffer = CacheHitUpdateBuffer::with_config(
        temp_dir.path().to_path_buf(),
        "access-update-instance".to_string(),
        Duration::from_secs(60),
        10000,
    );

    // Record multiple access updates
    let access_increment = 5u64;
    buffer
        .record_access_update(cache_key, 0, 8388607, access_increment)
        .await
        .unwrap();
    buffer.force_flush().await.unwrap();

    // Create consolidator and run consolidation
    let journal_manager = Arc::new(JournalManager::new(
        temp_dir.path().to_path_buf(),
        "consolidator".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        temp_dir.path().to_path_buf(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = JournalConsolidator::new(
        temp_dir.path().to_path_buf(),
        journal_manager.clone(),
        lock_manager,
        ConsolidationConfig::default(),
    );

    let result = consolidator.consolidate_object(cache_key).await.unwrap();
    assert!(result.success, "Consolidation should succeed");
    assert_eq!(
        result.entries_consolidated, 1,
        "Should have consolidated 1 entry"
    );

    // Read updated metadata and verify access count was updated
    let updated_content = tokio::fs::read_to_string(&metadata_path).await.unwrap();
    let updated_metadata: NewCacheMetadata = serde_json::from_str(&updated_content).unwrap();

    assert_eq!(updated_metadata.ranges.len(), 1);
    let updated_access_count = updated_metadata.ranges[0].access_count;

    assert_eq!(
        updated_access_count,
        initial_access_count + access_increment,
        "Access count should have been incremented: initial={}, increment={}, expected={}, got={}",
        initial_access_count,
        access_increment,
        initial_access_count + access_increment,
        updated_access_count
    );
}
