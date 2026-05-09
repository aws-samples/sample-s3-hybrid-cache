//! Presigned URL parsing and validation
//!
//! Provides utilities for parsing AWS SigV4 presigned URLs and checking expiration
//! without making requests to S3.

use chrono::DateTime;
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

/// Maximum allowed value for X-Amz-Expires (7 days in seconds, per AWS documentation)
const MAX_PRESIGNED_EXPIRES_SECONDS: u64 = 604_800;

/// The only currently supported AWS SigV4 algorithm value
const SUPPORTED_ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// Errors that can occur during presigned URL validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresignedUrlError {
    /// X-Amz-Algorithm is not a supported value
    UnsupportedAlgorithm(String),
    /// X-Amz-Expires exceeds the 7-day maximum (604,800 seconds)
    ExpiresExceedsCap { value: u64, max: u64 },
    /// X-Amz-Expires is zero or negative (parsed as non-positive)
    ExpiresNotPositive(String),
    /// X-Amz-Expires is not a valid integer
    ExpiresInvalid(String),
    /// Adding expires duration to signed_at overflows SystemTime
    ExpiryOverflow {
        signed_at: SystemTime,
        expires_seconds: u64,
    },
    /// X-Amz-Date is missing or invalid
    InvalidDate(String),
}

impl fmt::Display for PresignedUrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedAlgorithm(alg) => {
                write!(
                    f,
                    "unsupported X-Amz-Algorithm '{}', expected '{}'",
                    alg, SUPPORTED_ALGORITHM
                )
            }
            Self::ExpiresExceedsCap { value, max } => {
                write!(
                    f,
                    "X-Amz-Expires {} exceeds maximum {} seconds (7 days)",
                    value, max
                )
            }
            Self::ExpiresNotPositive(raw) => {
                write!(f, "X-Amz-Expires must be > 0, got '{}'", raw)
            }
            Self::ExpiresInvalid(raw) => {
                write!(f, "X-Amz-Expires is not a valid integer: '{}'", raw)
            }
            Self::ExpiryOverflow {
                signed_at,
                expires_seconds,
            } => {
                write!(
                    f,
                    "expiry computation overflows: signed_at={:?} + {}s",
                    signed_at, expires_seconds
                )
            }
            Self::InvalidDate(raw) => {
                write!(f, "invalid X-Amz-Date: '{}'", raw)
            }
        }
    }
}

impl std::error::Error for PresignedUrlError {}

/// Presigned URL information extracted from query parameters
#[derive(Debug, Clone)]
pub struct PresignedUrlInfo {
    /// When the URL was signed (X-Amz-Date)
    pub signed_at: SystemTime,
    /// Validity duration in seconds (X-Amz-Expires)
    pub expires_in_seconds: u64,
    /// Calculated expiration time
    pub expires_at: SystemTime,
    /// Algorithm used (X-Amz-Algorithm)
    pub algorithm: Option<String>,
    /// Credential scope (X-Amz-Credential)
    pub credential: Option<String>,
}

impl PresignedUrlInfo {
    /// Check if the presigned URL has expired
    pub fn is_expired(&self) -> bool {
        SystemTime::now() > self.expires_at
    }

    /// Get time remaining until expiration (None if already expired)
    pub fn time_until_expiration(&self) -> Option<Duration> {
        self.expires_at.duration_since(SystemTime::now()).ok()
    }

    /// Get time since expiration (None if not yet expired)
    pub fn time_since_expiration(&self) -> Option<Duration> {
        SystemTime::now().duration_since(self.expires_at).ok()
    }
}

/// Parse presigned URL information from query parameters
///
/// Returns:
/// - `Ok(None)` if this is not a presigned URL (missing X-Amz-Algorithm)
/// - `Ok(Some(info))` if the presigned URL is valid
/// - `Err(PresignedUrlError)` if the URL has presigned parameters but they are invalid
pub fn parse_presigned_url(
    query_params: &HashMap<String, String>,
) -> Result<Option<PresignedUrlInfo>, PresignedUrlError> {
    // Check if this is a presigned URL by looking for X-Amz-Algorithm
    let algorithm_value = match query_params.get("X-Amz-Algorithm") {
        Some(v) => v,
        None => return Ok(None),
    };

    // Validate X-Amz-Algorithm is exactly AWS4-HMAC-SHA256
    if algorithm_value != SUPPORTED_ALGORITHM {
        return Err(PresignedUrlError::UnsupportedAlgorithm(
            algorithm_value.clone(),
        ));
    }

    // Extract X-Amz-Date (required)
    let amz_date = match query_params.get("X-Amz-Date") {
        Some(d) => d,
        None => return Ok(None), // Missing date means incomplete presigned URL params
    };
    let signed_at =
        parse_amz_date(amz_date).ok_or_else(|| PresignedUrlError::InvalidDate(amz_date.clone()))?;

    // Extract X-Amz-Expires (required)
    let expires_str = match query_params.get("X-Amz-Expires") {
        Some(e) => e,
        None => return Ok(None), // Missing expires means incomplete presigned URL params
    };

    // Parse as i64 first to detect negative values, then validate
    let expires_signed: i64 = expires_str
        .parse()
        .map_err(|_| PresignedUrlError::ExpiresInvalid(expires_str.clone()))?;

    if expires_signed <= 0 {
        return Err(PresignedUrlError::ExpiresNotPositive(expires_str.clone()));
    }

    let expires_in_seconds = expires_signed as u64;

    // Cap at 604,800 seconds (7 days) per AWS documentation
    if expires_in_seconds > MAX_PRESIGNED_EXPIRES_SECONDS {
        return Err(PresignedUrlError::ExpiresExceedsCap {
            value: expires_in_seconds,
            max: MAX_PRESIGNED_EXPIRES_SECONDS,
        });
    }

    // Calculate expiration time using checked_add to prevent overflow
    let expires_at = signed_at
        .checked_add(Duration::from_secs(expires_in_seconds))
        .ok_or(PresignedUrlError::ExpiryOverflow {
            signed_at,
            expires_seconds: expires_in_seconds,
        })?;

    // Extract optional fields
    let algorithm = query_params.get("X-Amz-Algorithm").cloned();
    let credential = query_params.get("X-Amz-Credential").cloned();

    debug!(
        "Parsed presigned URL: signed_at={:?}, expires_in={}s, expires_at={:?}",
        signed_at, expires_in_seconds, expires_at
    );

    Ok(Some(PresignedUrlInfo {
        signed_at,
        expires_in_seconds,
        expires_at,
        algorithm,
        credential,
    }))
}

/// Parse AWS X-Amz-Date format (ISO 8601 basic format: YYYYMMDDTHHMMSSZ)
///
/// Example: "20240115T120000Z" -> SystemTime
fn parse_amz_date(date_str: &str) -> Option<SystemTime> {
    // X-Amz-Date format: YYYYMMDDTHHMMSSZ (basic ISO 8601)
    // Example: 20240115T120000Z
    if date_str.len() != 16 || !date_str.ends_with('Z') {
        warn!("Invalid X-Amz-Date format: {}", date_str);
        return None;
    }

    // Parse components
    let year: i32 = date_str[0..4].parse().ok()?;
    let month: u32 = date_str[4..6].parse().ok()?;
    let day: u32 = date_str[6..8].parse().ok()?;
    let hour: u32 = date_str[9..11].parse().ok()?;
    let minute: u32 = date_str[11..13].parse().ok()?;
    let second: u32 = date_str[13..15].parse().ok()?;

    // Build RFC3339 format for chrono parsing
    let rfc3339 = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hour, minute, second
    );

    // Parse with chrono
    let datetime = DateTime::parse_from_rfc3339(&rfc3339).ok()?;
    let timestamp = datetime.timestamp();

    // Convert to SystemTime
    let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp as u64);

    Some(system_time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_parse_amz_date() {
        // Valid date
        let date_str = "20240115T120000Z";
        let result = parse_amz_date(date_str);
        assert!(result.is_some());

        // Invalid format (too short)
        let date_str = "20240115T1200Z";
        let result = parse_amz_date(date_str);
        assert!(result.is_none());

        // Invalid format (missing Z)
        let date_str = "20240115T120000";
        let result = parse_amz_date(date_str);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_presigned_url() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "3600".to_string());
        query_params.insert("X-Amz-Signature".to_string(), "abc123".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(result.is_ok());
        let info = result.unwrap().unwrap();
        assert_eq!(info.expires_in_seconds, 3600);
        assert_eq!(info.algorithm, Some("AWS4-HMAC-SHA256".to_string()));
    }

    #[test]
    fn test_parse_presigned_url_not_presigned() {
        let mut query_params = HashMap::new();
        query_params.insert("versionId".to_string(), "abc123".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_unsupported_algorithm() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA512".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "3600".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(matches!(
            result,
            Err(PresignedUrlError::UnsupportedAlgorithm(_))
        ));
    }

    #[test]
    fn test_expires_exceeds_cap() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "604801".to_string()); // 1 second over cap

        let result = parse_presigned_url(&query_params);
        assert!(matches!(
            result,
            Err(PresignedUrlError::ExpiresExceedsCap {
                value: 604801,
                max: 604_800
            })
        ));
    }

    #[test]
    fn test_expires_at_cap_is_valid() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "604800".to_string()); // Exactly at cap

        let result = parse_presigned_url(&query_params);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_expires_zero() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "0".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(matches!(
            result,
            Err(PresignedUrlError::ExpiresNotPositive(_))
        ));
    }

    #[test]
    fn test_expires_negative() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "-100".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(matches!(
            result,
            Err(PresignedUrlError::ExpiresNotPositive(_))
        ));
    }

    #[test]
    fn test_expires_invalid_string() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "not_a_number".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(matches!(result, Err(PresignedUrlError::ExpiresInvalid(_))));
    }

    #[test]
    fn test_is_expired() {
        // Create an expired presigned URL (signed 2 hours ago, expires in 1 hour)
        let signed_at = SystemTime::now() - Duration::from_secs(7200);
        let expires_at = signed_at.checked_add(Duration::from_secs(3600)).unwrap();

        let info = PresignedUrlInfo {
            signed_at,
            expires_in_seconds: 3600,
            expires_at,
            algorithm: Some("AWS4-HMAC-SHA256".to_string()),
            credential: None,
        };

        assert!(info.is_expired());
        assert!(info.time_until_expiration().is_none());
        assert!(info.time_since_expiration().is_some());
    }

    #[test]
    fn test_not_expired() {
        // Create a valid presigned URL (signed now, expires in 1 hour)
        let signed_at = SystemTime::now();
        let expires_at = signed_at.checked_add(Duration::from_secs(3600)).unwrap();

        let info = PresignedUrlInfo {
            signed_at,
            expires_in_seconds: 3600,
            expires_at,
            algorithm: Some("AWS4-HMAC-SHA256".to_string()),
            credential: None,
        };

        assert!(!info.is_expired());
        assert!(info.time_until_expiration().is_some());
        assert!(info.time_since_expiration().is_none());
    }

    #[test]
    fn test_invalid_date() {
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "not-a-date".to_string());
        query_params.insert("X-Amz-Expires".to_string(), "3600".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(matches!(result, Err(PresignedUrlError::InvalidDate(_))));
    }

    /// Property 26: No-panic presigned expiry — for every generated (signed_at, expires_in_seconds)
    /// pair, assert no panic and result is Ok or structured error.
    ///
    /// The property verifies:
    /// 1. parse_presigned_url never panics (implicit — if it panics, the test fails)
    /// 2. The result is always Ok(Some/None) or Err(PresignedUrlError)
    /// 3. If expires_in_seconds > 604_800, the result is Err(ExpiresExceedsCap)
    /// 4. If expires_in_seconds == 0, the result is Err(ExpiresNotPositive)
    ///
    /// **Validates: Requirements 26.1, 26.2, 26.3, 26.4**
    #[quickcheck]
    fn prop_presigned_url_no_panic(signed_at_secs: u64, expires_in_seconds: u64) -> TestResult {
        // Construct a valid X-Amz-Date from signed_at_secs.
        // Cap to a reasonable range for date formatting (year 1970 to ~9999).
        // Dates beyond what chrono can format are tested via the InvalidDate path.
        let capped_secs = signed_at_secs % 253_402_300_800; // Cap at 9999-12-31T23:59:59Z

        // Format as YYYYMMDDTHHMMSSZ
        let datetime = chrono::DateTime::from_timestamp(capped_secs as i64, 0);
        let amz_date = match datetime {
            Some(dt) => dt.format("%Y%m%dT%H%M%SZ").to_string(),
            None => {
                // If we can't construct a valid date, skip this input
                return TestResult::discard();
            }
        };

        // Build query params with valid algorithm, generated date, and generated expires
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), amz_date);
        query_params.insert("X-Amz-Expires".to_string(), expires_in_seconds.to_string());

        // Call parse_presigned_url — if it panics, the test fails automatically
        let result = parse_presigned_url(&query_params);

        // Assert: result is always Ok or a structured Err (never a panic)
        match &result {
            Ok(Some(info)) => {
                // Valid presigned URL — verify expires_in_seconds is within cap
                if info.expires_in_seconds > MAX_PRESIGNED_EXPIRES_SECONDS {
                    return TestResult::failed();
                }
                // Verify expires_at >= signed_at
                if info.expires_at < info.signed_at {
                    return TestResult::failed();
                }
            }
            Ok(None) => {
                // Not a presigned URL — this shouldn't happen since we provide X-Amz-Algorithm
                return TestResult::failed();
            }
            Err(PresignedUrlError::ExpiresExceedsCap { value, max }) => {
                // Verify the cap is enforced correctly
                if *value != expires_in_seconds || *max != MAX_PRESIGNED_EXPIRES_SECONDS {
                    return TestResult::failed();
                }
            }
            Err(PresignedUrlError::ExpiresNotPositive(_)) => {
                // expires_in_seconds must be 0 (negative values can't be generated from u64)
                if expires_in_seconds != 0 {
                    return TestResult::failed();
                }
            }
            Err(PresignedUrlError::ExpiryOverflow { .. }) => {
                // Overflow is acceptable for large signed_at + expires combinations
            }
            Err(PresignedUrlError::InvalidDate(_)) => {
                // Date parsing failed — acceptable for edge-case dates
            }
            Err(PresignedUrlError::UnsupportedAlgorithm(_)) => {
                // Shouldn't happen since we pass the correct algorithm
                return TestResult::failed();
            }
            Err(PresignedUrlError::ExpiresInvalid(_)) => {
                // This happens when expires_in_seconds > i64::MAX (can't parse as i64)
                // which is valid behavior — the function parses as i64 first to detect negatives
            }
        }

        // Additional assertions for specific input ranges:
        // If expires_in_seconds > 604_800 AND fits in i64, must be ExpiresExceedsCap
        // (values > i64::MAX will fail to parse as i64 and return ExpiresInvalid)
        if expires_in_seconds > MAX_PRESIGNED_EXPIRES_SECONDS
            && expires_in_seconds <= i64::MAX as u64
        {
            match &result {
                Err(PresignedUrlError::ExpiresExceedsCap { .. }) => {}
                Err(PresignedUrlError::InvalidDate(_)) => {
                    // Date parsing can fail before we reach the expires check
                }
                _ => return TestResult::failed(),
            }
        }

        // If expires_in_seconds > i64::MAX, must be ExpiresInvalid (can't parse as i64)
        if expires_in_seconds > i64::MAX as u64 {
            match &result {
                Err(PresignedUrlError::ExpiresInvalid(_)) => {}
                Err(PresignedUrlError::InvalidDate(_)) => {
                    // Date parsing can fail before we reach the expires check
                }
                _ => return TestResult::failed(),
            }
        }

        // If expires_in_seconds == 0, must be ExpiresNotPositive
        if expires_in_seconds == 0 {
            match &result {
                Err(PresignedUrlError::ExpiresNotPositive(_)) => {}
                Err(PresignedUrlError::InvalidDate(_)) => {
                    // Date parsing can fail before we reach the expires check
                }
                _ => return TestResult::failed(),
            }
        }

        TestResult::passed()
    }
}
