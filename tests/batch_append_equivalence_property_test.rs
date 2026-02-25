//! Property-Based Test for Batch Append Equivalence
//!
//! **Feature: logging-and-consolidation-perf, Property 1: Batch append equivalence**
//!
//! *For any* list of valid `JournalEntry` objects for a given cache key, calling
//! `append_range_entries_batch(cache_key, entries)` on an empty journal SHALL produce
//! identical file content to calling `append_range_entry(cache_key, entry)` sequentially
//! for each entry on a separate empty journal.
//!
//! **Validates: Requirements 7.2, 7.4**

use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use std::time::SystemTime;
use tempfile::TempDir;

// ============================================================================
// Test Data Generators
// ============================================================================

/// Wrapper for generating arbitrary JournalEntry values via quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryJournalEntry(JournalEntry);

impl Arbitrary for ArbitraryJournalEntry {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate operation type (0-4)
        let op_seed = u8::arbitrary(g) % 5;
        let operation = match op_seed {
            0 => JournalOperation::Add,
            1 => JournalOperation::Update,
            2 => JournalOperation::Remove,
            3 => JournalOperation::TtlRefresh,
            4 => JournalOperation::AccessUpdate,
            _ => unreachable!(),
        };

        // Generate range parameters
        let range_start = u64::arbitrary(g) % 1_000_000;
        let range_size = (u16::arbitrary(g) as u64).max(1);
        let range_end = range_start + range_size - 1;
        let compressed_size = range_size;

        // Generate optional fields based on operation type
        let (new_ttl_secs, access_increment) = match operation {
            JournalOperation::TtlRefresh => (Some(u64::arbitrary(g) % 86400 + 1), None),
            JournalOperation::AccessUpdate => (None, Some(u64::arbitrary(g) % 100 + 1)),
            _ => (None, None),
        };

        let instance_seed = u8::arbitrary(g) % 4;
        let cache_key_seed = u8::arbitrary(g) % 8;
        let metadata_version = (u8::arbitrary(g) % 10 + 1) as u64;
        let metadata_written = bool::arbitrary(g);

        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: range_start,
            end: range_end,
            file_path: format!("test_range_{}-{}.bin", range_start, range_end),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size,
            uncompressed_size: compressed_size,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        };

        let entry = JournalEntry {
            timestamp: now,
            instance_id: format!("test-instance-{}", instance_seed),
            cache_key: format!("bucket-{}/object-{}", cache_key_seed, cache_key_seed),
            range_spec,
            operation,
            range_file_path: format!("ranges/test_range_{}-{}.bin", range_start, range_end),
            metadata_version,
            new_ttl_secs,
            object_ttl_secs: None,
            access_increment,
            object_metadata: None,
            metadata_written,
        };

        ArbitraryJournalEntry(entry)
    }
}

// ============================================================================
// Property 1: Batch Append Equivalence
// ============================================================================

/// **Feature: logging-and-consolidation-perf, Property 1: Batch append equivalence**
///
/// *For any* list of valid `JournalEntry` objects for a given cache key, calling
/// `append_range_entries_batch(cache_key, entries)` on an empty journal SHALL produce
/// identical file content to calling `append_range_entry(cache_key, entry)` sequentially
/// for each entry on a separate empty journal.
///
/// **Validates: Requirements 7.2, 7.4**
#[quickcheck]
fn prop_batch_append_equivalence(raw_entries: Vec<ArbitraryJournalEntry>) -> TestResult {
    // Filter: need at least 1 entry, cap at 50 to keep tests fast
    if raw_entries.is_empty() || raw_entries.len() > 50 {
        return TestResult::discard();
    }

    let entries: Vec<JournalEntry> = raw_entries.into_iter().map(|a| a.0).collect();

    // Use a fixed cache key for all entries (the property is about a single cache key)
    let cache_key = "test-bucket/batch-equivalence-object";
    let instance_id = "batch-test-instance";

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Create two separate temp directories
        let batch_dir = TempDir::new().unwrap();
        let sequential_dir = TempDir::new().unwrap();

        let batch_cache_dir = batch_dir.path().to_path_buf();
        let sequential_cache_dir = sequential_dir.path().to_path_buf();

        // Create journal directories
        std::fs::create_dir_all(batch_cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(sequential_cache_dir.join("metadata/_journals")).unwrap();

        // Create two JournalManager instances with the same instance_id but different cache_dirs
        let batch_manager = JournalManager::new(batch_cache_dir.clone(), instance_id.to_string());
        let sequential_manager =
            JournalManager::new(sequential_cache_dir.clone(), instance_id.to_string());

        // Normalize entries: set consistent cache_key and instance_id
        let normalized_entries: Vec<JournalEntry> = entries
            .into_iter()
            .map(|mut e| {
                e.cache_key = cache_key.to_string();
                e.instance_id = instance_id.to_string();
                e
            })
            .collect();

        // Batch path: call append_range_entries_batch once
        batch_manager
            .append_range_entries_batch(cache_key, normalized_entries.clone())
            .await
            .expect("batch append should succeed");

        // Sequential path: call append_range_entry for each entry
        for entry in &normalized_entries {
            sequential_manager
                .append_range_entry(cache_key, entry.clone())
                .await
                .expect("sequential append should succeed");
        }

        // Read resulting journal files
        let journal_filename = format!("{}.journal", instance_id);
        let batch_journal_path = batch_cache_dir
            .join("metadata/_journals")
            .join(&journal_filename);
        let sequential_journal_path = sequential_cache_dir
            .join("metadata/_journals")
            .join(&journal_filename);

        let batch_content = tokio::fs::read_to_string(&batch_journal_path)
            .await
            .expect("batch journal file should exist");
        let sequential_content = tokio::fs::read_to_string(&sequential_journal_path)
            .await
            .expect("sequential journal file should exist");

        // Assert byte-identical content
        if batch_content != sequential_content {
            // Provide diagnostic info on failure
            let batch_lines: Vec<&str> = batch_content.lines().collect();
            let sequential_lines: Vec<&str> = sequential_content.lines().collect();

            return TestResult::error(format!(
                "Journal file contents differ!\n\
                 Batch lines: {}, Sequential lines: {}\n\
                 Batch bytes: {}, Sequential bytes: {}",
                batch_lines.len(),
                sequential_lines.len(),
                batch_content.len(),
                sequential_content.len(),
            ));
        }

        TestResult::passed()
    })
}
