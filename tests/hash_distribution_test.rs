// Hash Distribution Quality Tests
//
// These property-based tests verify that the BLAKE3-based hash sharding
// provides uniform distribution across directories and handles edge cases correctly.
//
// Requirements tested:
// - 2.5: Uniform hash distribution across 100K keys
// - 2.3: Same object key in different buckets uses same hash directories
// - 12.3: Multi-directory distribution for stored objects

use s3_proxy::disk_cache::get_sharded_path;
use std::collections::HashMap;
use std::path::PathBuf;

/// Property 5: Uniform hash distribution
///
/// **Feature: cache-directory-sharding, Property 5: Uniform hash distribution**
///
/// For any set of 100,000 random object keys, the distribution across hash directories
/// should have standard deviation within 10% of the expected uniform distribution.
///
/// This test:
/// 1. Generates 100,000 random object keys
/// 2. Computes the sharded path for each key
/// 3. Counts how many keys map to each level-1 directory (XX)
/// 4. Calculates the standard deviation of the distribution
/// 5. Verifies the standard deviation is within 10% of expected value
///
/// **Validates: Requirements 2.5**
#[test]
fn test_uniform_hash_distribution() {
    const NUM_KEYS: usize = 100_000;
    const NUM_LEVEL1_DIRS: usize = 256; // 2 hex digits = 256 directories
    const EXPECTED_PER_DIR: f64 = NUM_KEYS as f64 / NUM_LEVEL1_DIRS as f64; // ~390.625
    const TOLERANCE_PERCENT: f64 = 10.0; // 10% tolerance

    let base_dir = PathBuf::from("/cache/objects");
    let bucket = "test-bucket";

    // Track distribution across level-1 directories (XX)
    let mut level1_distribution: HashMap<String, usize> = HashMap::new();

    // Generate 100K random object keys and compute their sharded paths
    for i in 0..NUM_KEYS {
        // Create diverse object keys with different patterns
        let object_key = match i % 5 {
            0 => format!("path/to/object-{}", i),
            1 => format!("deeply/nested/path/file-{}.txt", i),
            2 => format!("short-{}", i),
            3 => format!("with-special-chars-{}!@#$%", i),
            _ => format!("random-{}-{}-{}", i, i * 2, i * 3),
        };

        let cache_key = format!("{}/{}", bucket, object_key);

        // Get sharded path
        let path =
            get_sharded_path(&base_dir, &cache_key, ".meta").expect("Should generate valid path");

        // Extract level-1 directory (XX) from path
        // Path format: /cache/metadata/bucket/XX/YYY/filename
        let path_str = path.to_string_lossy();
        let components: Vec<&str> = path_str.split('/').collect();

        // Find the bucket in the path and get the next component (XX)
        if let Some(bucket_idx) = components.iter().position(|&c| c == bucket) {
            if bucket_idx + 1 < components.len() {
                let level1 = components[bucket_idx + 1].to_string();

                // Verify level1 is 2 hex digits
                assert_eq!(level1.len(), 2, "Level 1 should be 2 characters");
                assert!(
                    level1.chars().all(|c| c.is_ascii_hexdigit()),
                    "Level 1 should be hex digits: {}",
                    level1
                );

                *level1_distribution.entry(level1).or_insert(0) += 1;
            }
        }
    }

    // Verify we have entries in all or most level-1 directories
    println!(
        "Distribution across {} level-1 directories:",
        level1_distribution.len()
    );
    println!("Expected per directory: {:.2}", EXPECTED_PER_DIR);

    // Calculate statistics
    let counts: Vec<usize> = level1_distribution.values().copied().collect();
    let sum: usize = counts.iter().sum();
    let mean = sum as f64 / counts.len() as f64;

    // Calculate standard deviation
    let variance: f64 = counts
        .iter()
        .map(|&count| {
            let diff = count as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / counts.len() as f64;
    let std_dev = variance.sqrt();

    // Expected standard deviation for uniform distribution
    // For a uniform distribution with n items in k bins, std dev ≈ sqrt(n/k)
    let expected_std_dev = (EXPECTED_PER_DIR).sqrt();
    let tolerance = expected_std_dev * (TOLERANCE_PERCENT / 100.0);

    println!("Mean: {:.2}", mean);
    println!("Standard deviation: {:.2}", std_dev);
    println!("Expected std dev: {:.2}", expected_std_dev);
    println!("Tolerance ({}%): {:.2}", TOLERANCE_PERCENT, tolerance);
    println!("Min count: {}", counts.iter().min().unwrap());
    println!("Max count: {}", counts.iter().max().unwrap());

    // Verify distribution is uniform within tolerance
    assert!(
        std_dev <= expected_std_dev + tolerance,
        "Standard deviation ({:.2}) exceeds expected ({:.2}) + tolerance ({:.2})",
        std_dev,
        expected_std_dev,
        tolerance
    );

    // Verify we're using a significant portion of available directories
    // With 100K keys and 256 directories, we should use most directories
    assert!(
        level1_distribution.len() >= 250,
        "Should use at least 250 out of 256 directories, got {}",
        level1_distribution.len()
    );

    println!(
        "✓ Hash distribution is uniform within {}% tolerance",
        TOLERANCE_PERCENT
    );
}

/// Property 3: Same object key, same hash directories
///
/// **Feature: cache-directory-sharding, Property 3: Same object key, same hash directories**
///
/// For any two cache keys with different buckets but the same object key,
/// they should resolve to the same XX/YYY hash directories but different bucket directories.
///
/// This test:
/// 1. Generates pairs of cache keys with different buckets but same object key
/// 2. Computes sharded paths for each pair
/// 3. Verifies the hash directories (XX/YYY) are identical
/// 4. Verifies the bucket directories are different
///
/// **Validates: Requirements 2.3**
#[test]
fn test_same_object_key_same_hash_directories() {
    let base_dir = PathBuf::from("/cache/objects");

    // Test with various object keys
    let long_key = "very-long-key-".to_string() + &"a".repeat(100);
    let test_cases = vec![
        "path/to/file.txt",
        "simple.txt",
        "deeply/nested/path/to/object.bin",
        "with-special-chars!@#$.dat",
        &long_key,
    ];

    for object_key in test_cases {
        // Create cache keys with different buckets but same object key
        let bucket1 = "bucket-alpha";
        let bucket2 = "bucket-beta";
        let bucket3 = "my.bucket.with.dots";

        let cache_key1 = format!("{}/{}", bucket1, object_key);
        let cache_key2 = format!("{}/{}", bucket2, object_key);
        let cache_key3 = format!("{}/{}", bucket3, object_key);

        // Get sharded paths
        let path1 =
            get_sharded_path(&base_dir, &cache_key1, ".meta").expect("Should generate valid path");
        let path2 =
            get_sharded_path(&base_dir, &cache_key2, ".meta").expect("Should generate valid path");
        let path3 =
            get_sharded_path(&base_dir, &cache_key3, ".meta").expect("Should generate valid path");

        // Extract components from paths
        let extract_components = |path: &PathBuf| -> (String, String, String, String) {
            let path_str = path.to_string_lossy();
            let components: Vec<&str> = path_str.split('/').collect();

            // Find "objects" in path and extract: bucket, XX, YYY, filename
            if let Some(objects_idx) = components.iter().position(|&c| c == "objects") {
                let bucket = components[objects_idx + 1].to_string();
                let level1 = components[objects_idx + 2].to_string();
                let level2 = components[objects_idx + 3].to_string();
                let filename = components[objects_idx + 4].to_string();
                (bucket, level1, level2, filename)
            } else {
                panic!("Could not find 'objects' in path: {}", path_str);
            }
        };

        let (b1, l1_1, l2_1, f1) = extract_components(&path1);
        let (b2, l1_2, l2_2, f2) = extract_components(&path2);
        let (b3, l1_3, l2_3, f3) = extract_components(&path3);

        // Verify buckets are different
        assert_eq!(b1, bucket1, "Bucket 1 should match");
        assert_eq!(b2, bucket2, "Bucket 2 should match");
        assert_eq!(b3, bucket3, "Bucket 3 should match");
        assert_ne!(b1, b2, "Buckets should be different");
        assert_ne!(b1, b3, "Buckets should be different");

        // Verify hash directories (XX/YYY) are IDENTICAL
        assert_eq!(
            l1_1, l1_2,
            "Level 1 hash directories should be identical for same object key: {} vs {}",
            l1_1, l1_2
        );
        assert_eq!(
            l1_1, l1_3,
            "Level 1 hash directories should be identical for same object key: {} vs {}",
            l1_1, l1_3
        );

        assert_eq!(
            l2_1, l2_2,
            "Level 2 hash directories should be identical for same object key: {} vs {}",
            l2_1, l2_2
        );
        assert_eq!(
            l2_1, l2_3,
            "Level 2 hash directories should be identical for same object key: {} vs {}",
            l2_1, l2_3
        );

        // Verify filenames are identical (since they're based on object key only)
        assert_eq!(f1, f2, "Filenames should be identical for same object key");
        assert_eq!(f1, f3, "Filenames should be identical for same object key");

        println!(
            "✓ Object key '{}' maps to same hash dirs ({}/{}) across buckets",
            object_key, l1_1, l2_1
        );
    }

    println!("✓ All object keys map to same hash directories across different buckets");
}

/// Property 29: Multi-directory distribution
///
/// **Feature: cache-directory-sharding, Property 29: Multi-directory distribution**
///
/// For any set of 1000 stored objects, they should be distributed across multiple
/// hash directories (not all in one directory).
///
/// This test:
/// 1. Generates 1000 diverse object keys
/// 2. Computes sharded paths for each
/// 3. Counts unique level-1 (XX) and level-2 (YYY) directories used
/// 4. Verifies objects are spread across multiple directories
///
/// **Validates: Requirements 12.3**
#[test]
fn test_multi_directory_distribution() {
    const NUM_OBJECTS: usize = 1000;

    let base_dir = PathBuf::from("/cache/ranges");
    let bucket = "test-bucket";

    // Track unique directories used
    let mut level1_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut level2_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut full_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Generate 1000 diverse object keys
    for i in 0..NUM_OBJECTS {
        // Create diverse object keys with different patterns
        let object_key = match i % 10 {
            0 => format!("images/photo-{}.jpg", i),
            1 => format!("videos/clip-{}.mp4", i),
            2 => format!("documents/report-{}.pdf", i),
            3 => format!("data/dataset-{}.csv", i),
            4 => format!("logs/app-{}.log", i),
            5 => format!("backups/backup-{}.tar.gz", i),
            6 => format!("temp/tmp-{}.dat", i),
            7 => format!("archive/old-{}.zip", i),
            8 => format!("config/settings-{}.yaml", i),
            _ => format!("misc/file-{}-{}.bin", i, i * 7),
        };

        let cache_key = format!("{}/{}", bucket, object_key);

        // Get sharded path
        let path = get_sharded_path(&base_dir, &cache_key, "_0-1023.bin")
            .expect("Should generate valid path");

        // Extract directory components
        let path_str = path.to_string_lossy();
        let components: Vec<&str> = path_str.split('/').collect();

        // Find bucket and extract hash directories
        if let Some(bucket_idx) = components.iter().position(|&c| c == bucket) {
            if bucket_idx + 2 < components.len() {
                let level1 = components[bucket_idx + 1].to_string();
                let level2 = components[bucket_idx + 2].to_string();
                let full_dir = format!("{}/{}", level1, level2);

                level1_dirs.insert(level1);
                level2_dirs.insert(level2);
                full_paths.insert(full_dir);
            }
        }
    }

    println!("Distribution for {} objects:", NUM_OBJECTS);
    println!("  Unique level-1 directories (XX): {}", level1_dirs.len());
    println!("  Unique level-2 directories (YYY): {}", level2_dirs.len());
    println!("  Unique full paths (XX/YYY): {}", full_paths.len());

    // With 1000 objects and good distribution, we should use multiple directories
    // Expected: ~4 objects per directory if perfectly distributed across 256 L1 dirs
    // In practice, we should see at least 50+ unique L1 directories
    assert!(
        level1_dirs.len() >= 50,
        "Should use at least 50 level-1 directories, got {}",
        level1_dirs.len()
    );

    // We should also see good distribution at level-2
    // With 1000 objects, we should use many level-2 directories
    assert!(
        level2_dirs.len() >= 100,
        "Should use at least 100 level-2 directories, got {}",
        level2_dirs.len()
    );

    // Full paths should show even better distribution
    assert!(
        full_paths.len() >= 200,
        "Should use at least 200 unique full paths (XX/YYY), got {}",
        full_paths.len()
    );

    // Verify objects are NOT all in the same directory
    assert!(
        full_paths.len() > 1,
        "Objects should be distributed across multiple directories, not all in one"
    );

    println!("✓ Objects are well-distributed across multiple directories");
}

#[test]
fn test_hash_distribution_edge_cases() {
    let base_dir = PathBuf::from("/cache/objects");

    // Test that empty object keys are properly rejected
    let result = get_sharded_path(&base_dir, "bucket/", ".meta");
    assert!(result.is_err(), "Should reject empty object key");

    // Test that very short object keys work
    let path = get_sharded_path(&base_dir, "bucket/a", ".meta")
        .expect("Should handle single-char object key");
    let path_str = path.to_string_lossy();
    assert!(
        path_str.contains("/bucket/"),
        "Should contain bucket directory"
    );

    // Test that identical object keys produce identical paths
    let path1 = get_sharded_path(&base_dir, "bucket/test.txt", ".meta").unwrap();
    let path2 = get_sharded_path(&base_dir, "bucket/test.txt", ".meta").unwrap();
    assert_eq!(
        path1, path2,
        "Identical keys should produce identical paths"
    );

    // Test that similar but different keys produce different paths
    let path1 = get_sharded_path(&base_dir, "bucket/test1.txt", ".meta").unwrap();
    let path2 = get_sharded_path(&base_dir, "bucket/test2.txt", ".meta").unwrap();
    assert_ne!(
        path1, path2,
        "Different keys should produce different paths"
    );

    println!("✓ Edge cases handled correctly");
}
