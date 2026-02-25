//! Property-based tests for BucketSettings serialization
//!
//! Feature: bucket-level-cache-settings
//! Property 1: BucketSettings round-trip serialization
//!
//! For any valid BucketSettings struct (with arbitrary valid field values,
//! including prefix overrides), serializing to JSON and then deserializing
//! back should produce an equivalent struct.
//!
//! **Validates: Requirements 1.2, 1.5, 7.1, 7.5, 12.3**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::bucket_settings::{BucketSettings, PrefixOverride};
use std::time::Duration;

/// Generate a random Duration from a set of valid, round-trippable values.
/// We constrain to whole-second multiples of standard units (s, m, h, d)
/// and whole milliseconds, since those are what format_duration produces.
fn arbitrary_duration(g: &mut Gen) -> Duration {
    let unit = u8::arbitrary(g) % 5;
    match unit {
        0 => Duration::from_secs(u64::arbitrary(g) % 86400),       // 0..86399 seconds
        1 => Duration::from_secs((u64::arbitrary(g) % 1440) * 60), // 0..1439 minutes
        2 => Duration::from_secs((u64::arbitrary(g) % 168) * 3600), // 0..167 hours
        3 => Duration::from_secs((u64::arbitrary(g) % 365) * 86400), // 0..364 days
        _ => Duration::from_millis(u64::arbitrary(g) % 10000),      // 0..9999 ms
    }
}

/// Generate an optional Duration — roughly 50% chance of Some vs None.
fn arbitrary_optional_duration(g: &mut Gen) -> Option<Duration> {
    if bool::arbitrary(g) {
        Some(arbitrary_duration(g))
    } else {
        None
    }
}

/// Generate a valid prefix string starting with "/".
fn arbitrary_prefix(g: &mut Gen) -> String {
    let segments = (u8::arbitrary(g) % 3) + 1;
    let mut prefix = String::new();
    for _ in 0..segments {
        prefix.push('/');
        let len = (u8::arbitrary(g) % 8) + 1;
        for _ in 0..len {
            let c = b'a' + (u8::arbitrary(g) % 26);
            prefix.push(c as char);
        }
    }
    prefix.push('/');
    prefix
}

/// Wrapper for generating arbitrary PrefixOverride values via quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryPrefixOverride(PrefixOverride);

impl Arbitrary for ArbitraryPrefixOverride {
    fn arbitrary(g: &mut Gen) -> Self {
        ArbitraryPrefixOverride(PrefixOverride {
            prefix: arbitrary_prefix(g),
            get_ttl: arbitrary_optional_duration(g),
            head_ttl: arbitrary_optional_duration(g),
            put_ttl: arbitrary_optional_duration(g),
            read_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            write_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            compression_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            ram_cache_eligible: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
        })
    }
}

/// Wrapper for generating arbitrary BucketSettings values via quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryBucketSettings(BucketSettings);

impl Arbitrary for ArbitraryBucketSettings {
    fn arbitrary(g: &mut Gen) -> Self {
        let num_prefixes = u8::arbitrary(g) % 6; // 0-5 prefix overrides
        let prefix_overrides: Vec<PrefixOverride> = (0..num_prefixes)
            .map(|_| ArbitraryPrefixOverride::arbitrary(g).0)
            .collect();

        let schema = if bool::arbitrary(g) {
            Some("https://example.com/bucket-settings-schema.json".to_string())
        } else {
            None
        };

        ArbitraryBucketSettings(BucketSettings {
            schema,
            get_ttl: arbitrary_optional_duration(g),
            head_ttl: arbitrary_optional_duration(g),
            put_ttl: arbitrary_optional_duration(g),
            read_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            write_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            compression_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            ram_cache_eligible: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            prefix_overrides,
        })
    }
}

// ============================================================================
// Property 1: BucketSettings round-trip serialization
// Feature: bucket-level-cache-settings, Property 1: BucketSettings round-trip serialization
// **Validates: Requirements 1.2, 1.5, 7.1, 7.5, 12.3**
// ============================================================================

fn prop_bucket_settings_round_trip(arb: ArbitraryBucketSettings) -> TestResult {
    let original = arb.0;

    // Serialize to JSON
    let json = match serde_json::to_string_pretty(&original) {
        Ok(j) => j,
        Err(e) => return TestResult::error(format!("Serialization failed: {}", e)),
    };

    // Deserialize back
    let deserialized: BucketSettings = match serde_json::from_str(&json) {
        Ok(d) => d,
        Err(e) => {
            return TestResult::error(format!(
                "Deserialization failed: {}\nJSON was:\n{}",
                e, json
            ))
        }
    };

    // Assert equivalence
    if original != deserialized {
        return TestResult::error(format!(
            "Round-trip mismatch!\nOriginal:      {:?}\nDeserialized:  {:?}\nJSON:\n{}",
            original, deserialized, json
        ));
    }

    TestResult::passed()
}

#[test]
fn test_property_bucket_settings_round_trip_serialization() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_bucket_settings_round_trip as fn(ArbitraryBucketSettings) -> TestResult);
}

// ============================================================================
// Property 8: Validation rejects invalid field values
// Feature: bucket-level-cache-settings, Property 8: Validation rejects invalid field values
// **Validates: Requirements 10.1, 10.2, 10.4**
// ============================================================================

/// Generate an invalid prefix string: either empty or not starting with "/".
fn arbitrary_invalid_prefix(g: &mut Gen) -> String {
    if bool::arbitrary(g) {
        // Empty prefix
        String::new()
    } else {
        // Non-empty prefix that does not start with "/"
        let len = (u8::arbitrary(g) % 8) + 1;
        let mut prefix = String::new();
        for _ in 0..len {
            let c = b'a' + (u8::arbitrary(g) % 26);
            prefix.push(c as char);
        }
        // Ensure it doesn't accidentally start with '/'
        if prefix.starts_with('/') {
            prefix.insert(0, 'x');
        }
        prefix
    }
}

/// Wrapper for generating a BucketSettings with at least one invalid prefix override.
#[derive(Debug, Clone)]
struct ArbitraryInvalidBucketSettings(BucketSettings);

impl Arbitrary for ArbitraryInvalidBucketSettings {
    fn arbitrary(g: &mut Gen) -> Self {
        // Start with a valid base
        let mut settings = ArbitraryBucketSettings::arbitrary(g).0;

        // Generate 0-4 additional valid prefix overrides
        let num_valid = u8::arbitrary(g) % 5;
        let mut overrides: Vec<PrefixOverride> = (0..num_valid)
            .map(|_| ArbitraryPrefixOverride::arbitrary(g).0)
            .collect();

        // Insert at least one invalid prefix override at a random position
        let invalid = PrefixOverride {
            prefix: arbitrary_invalid_prefix(g),
            get_ttl: arbitrary_optional_duration(g),
            head_ttl: arbitrary_optional_duration(g),
            put_ttl: arbitrary_optional_duration(g),
            read_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            write_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            compression_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
            ram_cache_eligible: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
        };

        let insert_pos = if overrides.is_empty() {
            0
        } else {
            usize::arbitrary(g) % (overrides.len() + 1)
        };
        overrides.insert(insert_pos, invalid);

        settings.prefix_overrides = overrides;

        ArbitraryInvalidBucketSettings(settings)
    }
}

fn prop_validation_rejects_invalid_field_values(arb: ArbitraryInvalidBucketSettings) -> TestResult {
    let settings = arb.0;
    let errors = settings.validate();

    if errors.is_empty() {
        return TestResult::error(format!(
            "Expected validation errors for settings with invalid prefix, but got none.\nSettings: {:?}",
            settings
        ));
    }

    TestResult::passed()
}

#[test]
fn test_property_validation_rejects_invalid_field_values() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_validation_rejects_invalid_field_values
                as fn(ArbitraryInvalidBucketSettings) -> TestResult,
        );
}


// ============================================================================
// Property 2: Settings cascade resolution
// Feature: bucket-level-cache-settings, Property 2: Settings cascade resolution
//
// For any combination of GlobalDefaults, BucketSettings, and PrefixOverride
// values, and for any request path, resolve() returns the value from the most
// specific level that defines each field (prefix → bucket → global).
//
// **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 4.1, 4.2,
//   4.3, 5.1, 5.2, 5.3, 6.1, 6.2, 6.3, 7.2, 7.4, 8.1, 8.2**
// ============================================================================

use s3_proxy::bucket_settings::{BucketSettingsManager, GlobalDefaults};
use tempfile::TempDir;

/// Wrapper for generating arbitrary GlobalDefaults via quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryGlobalDefaults(GlobalDefaults);

impl Arbitrary for ArbitraryGlobalDefaults {
    fn arbitrary(g: &mut Gen) -> Self {
        ArbitraryGlobalDefaults(GlobalDefaults {
            get_ttl: arbitrary_duration(g),
            head_ttl: arbitrary_duration(g),
            put_ttl: arbitrary_duration(g),
            read_cache_enabled: bool::arbitrary(g),
            write_cache_enabled: bool::arbitrary(g),
            compression_enabled: bool::arbitrary(g),
            ram_cache_enabled: bool::arbitrary(g),
        })
    }
}

/// Input tuple for cascade resolution testing.
/// Contains global defaults, bucket settings, and a path that may or may not
/// match one of the prefix overrides.
#[derive(Debug, Clone)]
struct ArbitraryResolveInput {
    global: GlobalDefaults,
    bucket_settings: BucketSettings,
    path: String,
}

impl Arbitrary for ArbitraryResolveInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let global = ArbitraryGlobalDefaults::arbitrary(g).0;
        let bucket_settings = ArbitraryBucketSettings::arbitrary(g).0;

        // Generate a path that sometimes matches a prefix override (to exercise
        // all cascade levels) and sometimes doesn't.
        let path = if !bucket_settings.prefix_overrides.is_empty() && bool::arbitrary(g) {
            // Pick a random existing prefix and append a suffix so the path matches it
            let idx = usize::arbitrary(g) % bucket_settings.prefix_overrides.len();
            let prefix = &bucket_settings.prefix_overrides[idx].prefix;
            let suffix_len = (u8::arbitrary(g) % 8) + 1;
            let suffix: String = (0..suffix_len)
                .map(|_| (b'a' + (u8::arbitrary(g) % 26)) as char)
                .collect();
            format!("{}{}", prefix, suffix)
        } else {
            // Generate a random path that likely won't match any prefix
            let segments = (u8::arbitrary(g) % 3) + 1;
            let mut path = String::new();
            for _ in 0..segments {
                path.push('/');
                let len = (u8::arbitrary(g) % 10) + 1;
                for _ in 0..len {
                    let c = b'A' + (u8::arbitrary(g) % 26); // uppercase to avoid matching lowercase prefixes
                    path.push(c as char);
                }
            }
            path
        };

        ArbitraryResolveInput {
            global,
            bucket_settings,
            path,
        }
    }
}

/// Compute the expected cascade value for an Option field:
/// prefix (longest match) → bucket → global.
fn expected_cascade_opt<T: Copy>(
    prefix_val: Option<T>,
    bucket_val: Option<T>,
    global_val: T,
) -> T {
    prefix_val.or(bucket_val).unwrap_or(global_val)
}

/// Find the longest matching prefix override for a given path.
fn find_longest_prefix_match<'a>(
    overrides: &'a [PrefixOverride],
    path: &str,
) -> Option<&'a PrefixOverride> {
    overrides
        .iter()
        .filter(|po| path.starts_with(&po.prefix))
        .max_by_key(|po| po.prefix.len())
}

fn prop_settings_cascade_resolution(input: ArbitraryResolveInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp_dir = TempDir::new().unwrap();
        let cache_dir = tmp_dir.path().to_path_buf();
        let bucket = "test-bucket";

        // Create metadata/{bucket}/ directory and write _settings.json
        let meta_dir = cache_dir.join("metadata").join(bucket);
        std::fs::create_dir_all(&meta_dir).unwrap();
        let settings_path = meta_dir.join("_settings.json");
        let json = serde_json::to_string_pretty(&input.bucket_settings).unwrap();
        std::fs::write(&settings_path, &json).unwrap();

        // Create manager with a very long staleness threshold (no reload during test)
        let manager = BucketSettingsManager::new(
            cache_dir,
            input.global.clone(),
            Duration::from_secs(3600),
        );

        let resolved = manager.resolve(bucket, &input.path).await;

        // Find the longest matching prefix for this path
        let prefix_match = find_longest_prefix_match(
            &input.bucket_settings.prefix_overrides,
            &input.path,
        );

        let g = &input.global;
        let b = &input.bucket_settings;

        // Verify each field independently: prefix → bucket → global
        let expected_get_ttl = expected_cascade_opt(
            prefix_match.and_then(|p| p.get_ttl),
            b.get_ttl,
            g.get_ttl,
        );
        if resolved.get_ttl != expected_get_ttl {
            return TestResult::error(format!(
                "get_ttl mismatch: resolved={:?}, expected={:?}\npath={:?}, prefix_match={:?}",
                resolved.get_ttl, expected_get_ttl, input.path, prefix_match.map(|p| &p.prefix)
            ));
        }

        let expected_head_ttl = expected_cascade_opt(
            prefix_match.and_then(|p| p.head_ttl),
            b.head_ttl,
            g.head_ttl,
        );
        if resolved.head_ttl != expected_head_ttl {
            return TestResult::error(format!(
                "head_ttl mismatch: resolved={:?}, expected={:?}",
                resolved.head_ttl, expected_head_ttl
            ));
        }

        let expected_put_ttl = expected_cascade_opt(
            prefix_match.and_then(|p| p.put_ttl),
            b.put_ttl,
            g.put_ttl,
        );
        if resolved.put_ttl != expected_put_ttl {
            return TestResult::error(format!(
                "put_ttl mismatch: resolved={:?}, expected={:?}",
                resolved.put_ttl, expected_put_ttl
            ));
        }

        let expected_read_cache = expected_cascade_opt(
            prefix_match.and_then(|p| p.read_cache_enabled),
            b.read_cache_enabled,
            g.read_cache_enabled,
        );
        if resolved.read_cache_enabled != expected_read_cache {
            return TestResult::error(format!(
                "read_cache_enabled mismatch: resolved={}, expected={}",
                resolved.read_cache_enabled, expected_read_cache
            ));
        }

        let expected_write_cache = expected_cascade_opt(
            prefix_match.and_then(|p| p.write_cache_enabled),
            b.write_cache_enabled,
            g.write_cache_enabled,
        );
        if resolved.write_cache_enabled != expected_write_cache {
            return TestResult::error(format!(
                "write_cache_enabled mismatch: resolved={}, expected={}",
                resolved.write_cache_enabled, expected_write_cache
            ));
        }

        let expected_compression = expected_cascade_opt(
            prefix_match.and_then(|p| p.compression_enabled),
            b.compression_enabled,
            g.compression_enabled,
        );
        if resolved.compression_enabled != expected_compression {
            return TestResult::error(format!(
                "compression_enabled mismatch: resolved={}, expected={}",
                resolved.compression_enabled, expected_compression
            ));
        }

        // ram_cache_eligible: cascade value, then apply post-cascade invariants
        let mut expected_ram = expected_cascade_opt(
            prefix_match.and_then(|p| p.ram_cache_eligible),
            b.ram_cache_eligible,
            g.ram_cache_enabled,
        );
        // Post-cascade invariants: zero TTL or read_cache_disabled force false
        if expected_get_ttl == Duration::ZERO {
            expected_ram = false;
        }
        if !expected_read_cache {
            expected_ram = false;
        }
        if resolved.ram_cache_eligible != expected_ram {
            return TestResult::error(format!(
                "ram_cache_eligible mismatch: resolved={}, expected={}\n\
                 get_ttl={:?}, read_cache_enabled={}, path={:?}, prefix_match={:?}",
                resolved.ram_cache_eligible, expected_ram,
                expected_get_ttl, expected_read_cache,
                input.path, prefix_match.map(|p| &p.prefix)
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_settings_cascade_resolution() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_settings_cascade_resolution as fn(ArbitraryResolveInput) -> TestResult,
        );
}

// ============================================================================
// Property 3: Zero TTL forces RAM cache ineligibility
// Feature: bucket-level-cache-settings, Property 3: Zero TTL forces RAM cache ineligibility
//
// For any resolved settings where get_ttl is zero (whether set at global,
// bucket, or prefix level), ram_cache_eligible in the resolved output must
// be false, regardless of the explicit ram_cache_eligible setting at any level.
//
// **Validates: Requirements 2.8, 6.5, 7.6**
// ============================================================================

/// Input for zero-TTL property testing.
/// Generates settings where get_ttl is Duration::ZERO at one of three levels
/// (global, bucket, or prefix), while ram_cache_eligible is explicitly true
/// at various levels to verify the invariant overrides it.
#[derive(Debug, Clone)]
struct ArbitraryZeroTtlInput {
    global: GlobalDefaults,
    bucket_settings: BucketSettings,
    path: String,
    /// Which level has get_ttl = 0: 0 = global, 1 = bucket, 2 = prefix
    zero_level: u8,
}

impl Arbitrary for ArbitraryZeroTtlInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut global = ArbitraryGlobalDefaults::arbitrary(g).0;
        let mut bucket_settings = ArbitraryBucketSettings::arbitrary(g).0;

        // Force ram_cache_eligible to true at all levels to stress the invariant
        global.ram_cache_enabled = true;
        bucket_settings.ram_cache_eligible = Some(true);

        // Ensure read_cache_enabled is true so only the zero-TTL invariant is tested
        global.read_cache_enabled = true;
        bucket_settings.read_cache_enabled = Some(true);

        let zero_level = u8::arbitrary(g) % 3;

        match zero_level {
            0 => {
                // Global level: set global get_ttl to zero
                global.get_ttl = Duration::ZERO;
                // Bucket and prefix may have non-zero get_ttl (which would override global),
                // so clear them to let the zero propagate
                bucket_settings.get_ttl = None;
                for po in &mut bucket_settings.prefix_overrides {
                    po.get_ttl = None;
                }
            }
            1 => {
                // Bucket level: set bucket get_ttl to zero
                bucket_settings.get_ttl = Some(Duration::ZERO);
                // Clear prefix get_ttl so bucket value wins
                for po in &mut bucket_settings.prefix_overrides {
                    po.get_ttl = None;
                }
            }
            2 => {
                // Prefix level: ensure at least one prefix with get_ttl = 0
                // and generate a path that matches it
                if bucket_settings.prefix_overrides.is_empty() {
                    let po = PrefixOverride {
                        prefix: arbitrary_prefix(g),
                        get_ttl: Some(Duration::ZERO),
                        head_ttl: arbitrary_optional_duration(g),
                        put_ttl: arbitrary_optional_duration(g),
                        read_cache_enabled: Some(true),
                        write_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
                        compression_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
                        ram_cache_eligible: Some(true), // explicitly true to test override
                    };
                    bucket_settings.prefix_overrides.push(po);
                } else {
                    // Pick a random prefix and set its get_ttl to zero
                    let idx = usize::arbitrary(g) % bucket_settings.prefix_overrides.len();
                    bucket_settings.prefix_overrides[idx].get_ttl = Some(Duration::ZERO);
                    bucket_settings.prefix_overrides[idx].ram_cache_eligible = Some(true);
                    bucket_settings.prefix_overrides[idx].read_cache_enabled = Some(true);
                }
            }
            _ => unreachable!(),
        }

        // Generate a path that matches the zero-TTL level
        let path = match zero_level {
            0 | 1 => {
                // For global/bucket level, generate a path that does NOT match any prefix
                // (so the zero from global/bucket level is the effective value)
                let segments = (u8::arbitrary(g) % 3) + 1;
                let mut path = String::new();
                for _ in 0..segments {
                    path.push('/');
                    let len = (u8::arbitrary(g) % 10) + 1;
                    for _ in 0..len {
                        let c = b'A' + (u8::arbitrary(g) % 26);
                        path.push(c as char);
                    }
                }
                path
            }
            2 => {
                // For prefix level, generate a path that matches the zero-TTL prefix
                let zero_prefix = bucket_settings
                    .prefix_overrides
                    .iter()
                    .find(|po| po.get_ttl == Some(Duration::ZERO))
                    .expect("should have a zero-TTL prefix");
                let suffix_len = (u8::arbitrary(g) % 8) + 1;
                let suffix: String = (0..suffix_len)
                    .map(|_| (b'a' + (u8::arbitrary(g) % 26)) as char)
                    .collect();
                format!("{}{}", zero_prefix.prefix, suffix)
            }
            _ => unreachable!(),
        };

        ArbitraryZeroTtlInput {
            global,
            bucket_settings,
            path,
            zero_level,
        }
    }
}

fn prop_zero_ttl_forces_ram_cache_ineligible(input: ArbitraryZeroTtlInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp_dir = TempDir::new().unwrap();
        let cache_dir = tmp_dir.path().to_path_buf();
        let bucket = "test-bucket";

        // Create metadata/{bucket}/ directory and write _settings.json
        let meta_dir = cache_dir.join("metadata").join(bucket);
        std::fs::create_dir_all(&meta_dir).unwrap();
        let settings_path = meta_dir.join("_settings.json");
        let json = serde_json::to_string_pretty(&input.bucket_settings).unwrap();
        std::fs::write(&settings_path, &json).unwrap();

        let manager = BucketSettingsManager::new(
            cache_dir,
            input.global.clone(),
            Duration::from_secs(3600),
        );

        let resolved = manager.resolve(bucket, &input.path).await;

        // The core invariant: when get_ttl resolves to zero, ram_cache_eligible must be false
        if resolved.get_ttl != Duration::ZERO {
            return TestResult::error(format!(
                "Expected resolved get_ttl to be zero but got {:?}.\n\
                 zero_level={}, path={:?}, bucket_settings={:?}, global={:?}",
                resolved.get_ttl, input.zero_level, input.path,
                input.bucket_settings, input.global
            ));
        }

        if resolved.ram_cache_eligible {
            return TestResult::error(format!(
                "ram_cache_eligible should be false when get_ttl is zero, but got true.\n\
                 zero_level={}, path={:?}, resolved={:?}",
                input.zero_level, input.path, resolved
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_zero_ttl_forces_ram_cache_ineligibility() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_zero_ttl_forces_ram_cache_ineligible
                as fn(ArbitraryZeroTtlInput) -> TestResult,
        );
}

// ============================================================================
// Property 4: Read cache disabled forces RAM cache ineligibility
// Feature: bucket-level-cache-settings, Property 4: Read cache disabled forces RAM cache ineligibility
//
// For any resolved settings where read_cache_enabled is false (whether set at
// bucket level or prefix level), ram_cache_eligible in the resolved output must
// be false, regardless of the explicit ram_cache_eligible setting at any level.
//
// **Validates: Requirements 3.5, 3.6**
// ============================================================================

/// Input generator for Property 4: read_cache_enabled=false at one of three levels
/// (global, bucket, prefix), with ram_cache_eligible explicitly true everywhere
/// to stress the invariant.
#[derive(Debug, Clone)]
struct ArbitraryReadCacheDisabledInput {
    global: GlobalDefaults,
    bucket_settings: BucketSettings,
    path: String,
    /// Which level has read_cache_enabled = false: 0 = global, 1 = bucket, 2 = prefix
    disabled_level: u8,
}

impl Arbitrary for ArbitraryReadCacheDisabledInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut global = ArbitraryGlobalDefaults::arbitrary(g).0;
        let mut bucket_settings = ArbitraryBucketSettings::arbitrary(g).0;

        // Force ram_cache_eligible to true at all levels to stress the invariant
        global.ram_cache_enabled = true;
        bucket_settings.ram_cache_eligible = Some(true);
        for po in &mut bucket_settings.prefix_overrides {
            po.ram_cache_eligible = Some(true);
        }

        // Ensure get_ttl is non-zero so only the read_cache_enabled invariant is tested
        if global.get_ttl == Duration::ZERO {
            global.get_ttl = Duration::from_secs(60);
        }
        if bucket_settings.get_ttl == Some(Duration::ZERO) {
            bucket_settings.get_ttl = Some(Duration::from_secs(60));
        }
        for po in &mut bucket_settings.prefix_overrides {
            if po.get_ttl == Some(Duration::ZERO) {
                po.get_ttl = Some(Duration::from_secs(60));
            }
        }

        let disabled_level = u8::arbitrary(g) % 3;

        match disabled_level {
            0 => {
                // Global level: set global read_cache_enabled to false
                global.read_cache_enabled = false;
                // Clear bucket and prefix read_cache_enabled so global propagates
                bucket_settings.read_cache_enabled = None;
                for po in &mut bucket_settings.prefix_overrides {
                    po.read_cache_enabled = None;
                }
            }
            1 => {
                // Bucket level: set bucket read_cache_enabled to false
                bucket_settings.read_cache_enabled = Some(false);
                // Clear prefix read_cache_enabled so bucket value wins
                for po in &mut bucket_settings.prefix_overrides {
                    po.read_cache_enabled = None;
                }
            }
            2 => {
                // Prefix level: ensure at least one prefix with read_cache_enabled = false
                if bucket_settings.prefix_overrides.is_empty() {
                    let po = PrefixOverride {
                        prefix: arbitrary_prefix(g),
                        get_ttl: arbitrary_optional_duration(g),
                        head_ttl: arbitrary_optional_duration(g),
                        put_ttl: arbitrary_optional_duration(g),
                        read_cache_enabled: Some(false),
                        write_cache_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
                        compression_enabled: if bool::arbitrary(g) { Some(bool::arbitrary(g)) } else { None },
                        ram_cache_eligible: Some(true), // explicitly true to test override
                    };
                    // Ensure non-zero get_ttl on the new prefix
                    bucket_settings.prefix_overrides.push(po);
                } else {
                    // Pick a random prefix and set its read_cache_enabled to false
                    let idx = usize::arbitrary(g) % bucket_settings.prefix_overrides.len();
                    bucket_settings.prefix_overrides[idx].read_cache_enabled = Some(false);
                    bucket_settings.prefix_overrides[idx].ram_cache_eligible = Some(true);
                }
                // Ensure the chosen prefix doesn't have zero get_ttl
                for po in &mut bucket_settings.prefix_overrides {
                    if po.get_ttl == Some(Duration::ZERO) {
                        po.get_ttl = Some(Duration::from_secs(60));
                    }
                }
            }
            _ => unreachable!(),
        }

        // Generate a path that matches the disabled level
        let path = match disabled_level {
            0 | 1 => {
                // For global/bucket level, generate a path that does NOT match any prefix
                let segments = (u8::arbitrary(g) % 3) + 1;
                let mut path = String::new();
                for _ in 0..segments {
                    path.push('/');
                    let len = (u8::arbitrary(g) % 10) + 1;
                    for _ in 0..len {
                        let c = b'A' + (u8::arbitrary(g) % 26);
                        path.push(c as char);
                    }
                }
                path
            }
            2 => {
                // For prefix level, generate a path that matches the disabled prefix
                let disabled_prefix = bucket_settings
                    .prefix_overrides
                    .iter()
                    .find(|po| po.read_cache_enabled == Some(false))
                    .expect("should have a read_cache_enabled=false prefix");
                let suffix_len = (u8::arbitrary(g) % 8) + 1;
                let suffix: String = (0..suffix_len)
                    .map(|_| (b'a' + (u8::arbitrary(g) % 26)) as char)
                    .collect();
                format!("{}{}", disabled_prefix.prefix, suffix)
            }
            _ => unreachable!(),
        };

        ArbitraryReadCacheDisabledInput {
            global,
            bucket_settings,
            path,
            disabled_level,
        }
    }
}

fn prop_read_cache_disabled_forces_ram_cache_ineligible(
    input: ArbitraryReadCacheDisabledInput,
) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp_dir = TempDir::new().unwrap();
        let cache_dir = tmp_dir.path().to_path_buf();
        let bucket = "test-bucket";

        // Create metadata/{bucket}/ directory and write _settings.json
        let meta_dir = cache_dir.join("metadata").join(bucket);
        std::fs::create_dir_all(&meta_dir).unwrap();
        let settings_path = meta_dir.join("_settings.json");
        let json = serde_json::to_string_pretty(&input.bucket_settings).unwrap();
        std::fs::write(&settings_path, &json).unwrap();

        let manager = BucketSettingsManager::new(
            cache_dir,
            input.global.clone(),
            Duration::from_secs(3600),
        );

        let resolved = manager.resolve(bucket, &input.path).await;

        // The core invariant: when read_cache_enabled resolves to false,
        // ram_cache_eligible must be false
        if resolved.read_cache_enabled {
            return TestResult::error(format!(
                "Expected resolved read_cache_enabled to be false but got true.\n\
                 disabled_level={}, path={:?}, bucket_settings={:?}, global={:?}",
                input.disabled_level, input.path,
                input.bucket_settings, input.global
            ));
        }

        if resolved.ram_cache_eligible {
            return TestResult::error(format!(
                "ram_cache_eligible should be false when read_cache_enabled is false, but got true.\n\
                 disabled_level={}, path={:?}, resolved={:?}",
                input.disabled_level, input.path, resolved
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_read_cache_disabled_forces_ram_cache_ineligibility() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_read_cache_disabled_forces_ram_cache_ineligible
                as fn(ArbitraryReadCacheDisabledInput) -> TestResult,
        );
}

// ============================================================================
// Property 6: Longest prefix match
// Feature: bucket-level-cache-settings, Property 6: Longest prefix match
//
// For any set of prefix overrides where multiple prefixes match a given path,
// the resolve() function should use the settings from the longest (most
// specific) matching prefix. Specifically: if prefix A is a proper prefix of
// prefix B, and both match the path, then B's settings should take precedence.
//
// **Validates: Requirements 7.3**
// ============================================================================

/// Input generator for Property 6: two overlapping prefixes where one is a
/// proper prefix of the other, each with a distinct get_ttl. The generated
/// path matches both prefixes, so the longer one must win.
#[derive(Debug, Clone)]
struct ArbitraryLongestPrefixInput {
    global: GlobalDefaults,
    bucket_settings: BucketSettings,
    path: String,
    /// The get_ttl assigned to the shorter prefix
    short_ttl: Duration,
    /// The get_ttl assigned to the longer prefix
    long_ttl: Duration,
}

impl Arbitrary for ArbitraryLongestPrefixInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let global = ArbitraryGlobalDefaults::arbitrary(g).0;

        // Generate a short prefix: e.g. "/data/"
        let short_prefix = arbitrary_prefix(g);

        // Generate a longer prefix that starts with the short prefix.
        // Append one or more additional path segments.
        let extra_segments = (u8::arbitrary(g) % 2) + 1; // 1-2 extra segments
        let mut long_prefix = short_prefix.trim_end_matches('/').to_string();
        for _ in 0..extra_segments {
            long_prefix.push('/');
            let len = (u8::arbitrary(g) % 8) + 1;
            for _ in 0..len {
                let c = b'a' + (u8::arbitrary(g) % 26);
                long_prefix.push(c as char);
            }
        }
        long_prefix.push('/');

        // Generate two distinct TTL values so we can tell which prefix won.
        // Use values >= 1s to avoid triggering the zero-TTL RAM cache invariant.
        let short_ttl = Duration::from_secs((u64::arbitrary(g) % 3600) + 1);
        let mut long_ttl = Duration::from_secs((u64::arbitrary(g) % 3600) + 1);
        if long_ttl == short_ttl {
            // Ensure they differ
            long_ttl = short_ttl + Duration::from_secs(1);
        }

        let short_override = PrefixOverride {
            prefix: short_prefix.clone(),
            get_ttl: Some(short_ttl),
            head_ttl: None,
            put_ttl: None,
            read_cache_enabled: None,
            write_cache_enabled: None,
            compression_enabled: None,
            ram_cache_eligible: None,
        };

        let long_override = PrefixOverride {
            prefix: long_prefix.clone(),
            get_ttl: Some(long_ttl),
            head_ttl: None,
            put_ttl: None,
            read_cache_enabled: None,
            write_cache_enabled: None,
            compression_enabled: None,
            ram_cache_eligible: None,
        };

        // Randomize the order of prefix overrides to ensure ordering doesn't matter
        let prefix_overrides = if bool::arbitrary(g) {
            vec![short_override, long_override]
        } else {
            vec![long_override, short_override]
        };

        let bucket_settings = BucketSettings {
            schema: None,
            get_ttl: None,
            head_ttl: None,
            put_ttl: None,
            read_cache_enabled: None,
            write_cache_enabled: None,
            compression_enabled: None,
            ram_cache_eligible: None,
            prefix_overrides,
        };

        // Generate a path that matches the longer prefix (and therefore also the shorter one)
        let suffix_len = (u8::arbitrary(g) % 8) + 1;
        let suffix: String = (0..suffix_len)
            .map(|_| (b'a' + (u8::arbitrary(g) % 26)) as char)
            .collect();
        let path = format!("{}{}", long_prefix, suffix);

        ArbitraryLongestPrefixInput {
            global,
            bucket_settings,
            path,
            short_ttl,
            long_ttl,
        }
    }
}

fn prop_longest_prefix_match(input: ArbitraryLongestPrefixInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp_dir = TempDir::new().unwrap();
        let cache_dir = tmp_dir.path().to_path_buf();
        let bucket = "test-bucket";

        // Create metadata/{bucket}/ directory and write _settings.json
        let meta_dir = cache_dir.join("metadata").join(bucket);
        std::fs::create_dir_all(&meta_dir).unwrap();
        let settings_path = meta_dir.join("_settings.json");
        let json = serde_json::to_string_pretty(&input.bucket_settings).unwrap();
        std::fs::write(&settings_path, &json).unwrap();

        let manager = BucketSettingsManager::new(
            cache_dir,
            input.global.clone(),
            Duration::from_secs(3600),
        );

        let resolved = manager.resolve(bucket, &input.path).await;

        // The core property: the longer prefix's get_ttl must win
        if resolved.get_ttl != input.long_ttl {
            return TestResult::error(format!(
                "Longest prefix match failed: resolved get_ttl={:?}, expected={:?} (from longer prefix).\n\
                 short_ttl={:?}, path={:?}, prefixes={:?}",
                resolved.get_ttl,
                input.long_ttl,
                input.short_ttl,
                input.path,
                input.bucket_settings.prefix_overrides.iter().map(|p| &p.prefix).collect::<Vec<_>>()
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_longest_prefix_match() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_longest_prefix_match as fn(ArbitraryLongestPrefixInput) -> TestResult,
        );
}

// ============================================================================
// Property 5: Error recovery on invalid settings
// Feature: bucket-level-cache-settings, Property 5: Error recovery on invalid settings
//
// For any previously valid BucketSettings cached for a bucket, if the settings
// file is replaced with invalid JSON (or JSON with invalid field values), the
// resolve() function should return settings equivalent to the previously valid
// settings (not global defaults, and not the invalid values).
//
// When no previous valid settings exist, resolve() should return global defaults.
//
// **Validates: Requirements 1.4, 9.6, 10.5**
// ============================================================================

/// Input generator for Property 5: a valid BucketSettings + GlobalDefaults pair.
/// The test will load the valid settings, corrupt the file, and verify resolve()
/// still returns the previously valid values.
#[derive(Debug, Clone)]
struct ArbitraryErrorRecoveryInput {
    global: GlobalDefaults,
    bucket_settings: BucketSettings,
}

impl Arbitrary for ArbitraryErrorRecoveryInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let global = ArbitraryGlobalDefaults::arbitrary(g).0;
        let mut bucket_settings = ArbitraryBucketSettings::arbitrary(g).0;

        // Ensure the bucket settings have valid prefixes so they pass validation
        bucket_settings.prefix_overrides.retain(|po| {
            !po.prefix.is_empty() && po.prefix.starts_with('/')
        });

        ArbitraryErrorRecoveryInput {
            global,
            bucket_settings,
        }
    }
}

fn prop_error_recovery_on_invalid_settings(input: ArbitraryErrorRecoveryInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp_dir = TempDir::new().unwrap();
        let cache_dir = tmp_dir.path().to_path_buf();
        let bucket = "test-bucket";

        // Use a simple path that won't match any prefix override
        let path = "/NOMATCH/object.txt";

        // Step 1: Write valid settings to disk
        let meta_dir = cache_dir.join("metadata").join(bucket);
        std::fs::create_dir_all(&meta_dir).unwrap();
        let settings_path = meta_dir.join("_settings.json");
        let json = serde_json::to_string_pretty(&input.bucket_settings).unwrap();
        std::fs::write(&settings_path, &json).unwrap();

        // Create manager with Duration::ZERO staleness so every resolve() triggers a reload
        let manager = BucketSettingsManager::new(
            cache_dir,
            input.global.clone(),
            Duration::ZERO,
        );

        // Step 2: First resolve — loads valid settings
        let first_resolved = manager.resolve(bucket, path).await;

        // Sanity check: first resolve should reflect bucket settings cascade
        let expected_get_ttl = input.bucket_settings.get_ttl.unwrap_or(input.global.get_ttl);
        if first_resolved.get_ttl != expected_get_ttl {
            return TestResult::error(format!(
                "First resolve get_ttl mismatch: got {:?}, expected {:?}",
                first_resolved.get_ttl, expected_get_ttl
            ));
        }

        // Step 3: Replace the file with invalid JSON
        std::fs::write(&settings_path, "{{{{not valid json!!!!").unwrap();

        // Step 4: Second resolve — should return previous valid settings
        let second_resolved = manager.resolve(bucket, path).await;

        // Step 5: Assert the second resolve matches the first (previous valid settings preserved)
        if second_resolved.get_ttl != first_resolved.get_ttl {
            return TestResult::error(format!(
                "Error recovery failed for get_ttl: after invalid JSON, got {:?}, expected {:?} (previous valid)",
                second_resolved.get_ttl, first_resolved.get_ttl
            ));
        }
        if second_resolved.head_ttl != first_resolved.head_ttl {
            return TestResult::error(format!(
                "Error recovery failed for head_ttl: after invalid JSON, got {:?}, expected {:?}",
                second_resolved.head_ttl, first_resolved.head_ttl
            ));
        }
        if second_resolved.put_ttl != first_resolved.put_ttl {
            return TestResult::error(format!(
                "Error recovery failed for put_ttl: after invalid JSON, got {:?}, expected {:?}",
                second_resolved.put_ttl, first_resolved.put_ttl
            ));
        }
        if second_resolved.read_cache_enabled != first_resolved.read_cache_enabled {
            return TestResult::error(format!(
                "Error recovery failed for read_cache_enabled: after invalid JSON, got {}, expected {}",
                second_resolved.read_cache_enabled, first_resolved.read_cache_enabled
            ));
        }
        if second_resolved.write_cache_enabled != first_resolved.write_cache_enabled {
            return TestResult::error(format!(
                "Error recovery failed for write_cache_enabled: after invalid JSON, got {}, expected {}",
                second_resolved.write_cache_enabled, first_resolved.write_cache_enabled
            ));
        }
        if second_resolved.compression_enabled != first_resolved.compression_enabled {
            return TestResult::error(format!(
                "Error recovery failed for compression_enabled: after invalid JSON, got {}, expected {}",
                second_resolved.compression_enabled, first_resolved.compression_enabled
            ));
        }
        if second_resolved.ram_cache_eligible != first_resolved.ram_cache_eligible {
            return TestResult::error(format!(
                "Error recovery failed for ram_cache_eligible: after invalid JSON, got {}, expected {}",
                second_resolved.ram_cache_eligible, first_resolved.ram_cache_eligible
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_error_recovery_on_invalid_settings() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_error_recovery_on_invalid_settings
                as fn(ArbitraryErrorRecoveryInput) -> TestResult,
        );
}

// ============================================================================
// Feature: bucket-level-cache-settings
// Property 7: Staleness threshold triggers reload
//
// For any bucket with cached settings, if the settings file is modified on
// disk and the staleness threshold has elapsed since the last load, the next
// resolve() call should return settings reflecting the updated file contents.
//
// **Validates: Requirements 9.2, 9.4**
// ============================================================================

/// Input generator for Property 7: two distinct BucketSettings + GlobalDefaults.
/// The two BucketSettings are guaranteed to have different get_ttl values so we
/// can distinguish the original from the updated settings after reload.
#[derive(Debug, Clone)]
struct ArbitraryStalenessReloadInput {
    global: GlobalDefaults,
    original_settings: BucketSettings,
    updated_settings: BucketSettings,
}

impl Arbitrary for ArbitraryStalenessReloadInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let global = ArbitraryGlobalDefaults::arbitrary(g).0;
        let mut original = ArbitraryBucketSettings::arbitrary(g).0;
        let mut updated = ArbitraryBucketSettings::arbitrary(g).0;

        // Ensure both have valid prefixes
        original.prefix_overrides.retain(|po| {
            !po.prefix.is_empty() && po.prefix.starts_with('/')
        });
        updated.prefix_overrides.retain(|po| {
            !po.prefix.is_empty() && po.prefix.starts_with('/')
        });

        // Ensure both have explicit get_ttl values that differ, so we can
        // distinguish original from updated after reload.
        let ttl_a = arbitrary_duration(g);
        let mut ttl_b = arbitrary_duration(g);
        // If they happen to be equal, shift one
        if ttl_a == ttl_b {
            ttl_b = ttl_a + Duration::from_secs(1);
        }
        original.get_ttl = Some(ttl_a);
        updated.get_ttl = Some(ttl_b);

        ArbitraryStalenessReloadInput {
            global,
            original_settings: original,
            updated_settings: updated,
        }
    }
}

fn prop_staleness_threshold_triggers_reload(input: ArbitraryStalenessReloadInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp_dir = TempDir::new().unwrap();
        let cache_dir = tmp_dir.path().to_path_buf();
        let bucket = "test-bucket";

        // Use a simple path that won't match any prefix override
        let path = "/NOMATCH/object.txt";

        // Step 1: Write original settings to disk
        let meta_dir = cache_dir.join("metadata").join(bucket);
        std::fs::create_dir_all(&meta_dir).unwrap();
        let settings_path = meta_dir.join("_settings.json");
        let original_json = serde_json::to_string_pretty(&input.original_settings).unwrap();
        std::fs::write(&settings_path, &original_json).unwrap();

        // Create manager with Duration::ZERO staleness so every resolve() triggers reload
        let manager = BucketSettingsManager::new(
            cache_dir,
            input.global.clone(),
            Duration::ZERO,
        );

        // Step 2: First resolve — should load original settings
        let first_resolved = manager.resolve(bucket, path).await;

        let expected_original_ttl = input.original_settings.get_ttl.unwrap();
        // Zero TTL forces ram_cache_eligible=false which doesn't affect get_ttl,
        // but verify the TTL itself loaded correctly
        if first_resolved.get_ttl != expected_original_ttl {
            return TestResult::error(format!(
                "First resolve get_ttl mismatch: got {:?}, expected {:?}",
                first_resolved.get_ttl, expected_original_ttl
            ));
        }

        // Step 3: Write updated settings to the same file
        let updated_json = serde_json::to_string_pretty(&input.updated_settings).unwrap();
        std::fs::write(&settings_path, &updated_json).unwrap();

        // Step 4: Second resolve — with zero staleness, should reload and return updated values
        let second_resolved = manager.resolve(bucket, path).await;

        let expected_updated_ttl = input.updated_settings.get_ttl.unwrap();
        if second_resolved.get_ttl != expected_updated_ttl {
            return TestResult::error(format!(
                "Staleness reload failed for get_ttl: got {:?}, expected {:?} (updated), original was {:?}",
                second_resolved.get_ttl, expected_updated_ttl, expected_original_ttl
            ));
        }

        // Verify the two resolves actually returned different values (our generator guarantees this)
        if first_resolved.get_ttl == second_resolved.get_ttl {
            return TestResult::error(format!(
                "Original and updated get_ttl are the same ({:?}), cannot verify reload occurred",
                first_resolved.get_ttl
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_staleness_threshold_triggers_reload() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_staleness_threshold_triggers_reload
                as fn(ArbitraryStalenessReloadInput) -> TestResult,
        );
}
