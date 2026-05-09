//! Unit tests for presigned URL parsing and validation
//!
//! Tests for Requirements 26.1, 26.2, 26.3, 26.4, 32.2:
//! - Algorithm validation (only AWS4-HMAC-SHA256 accepted)
//! - Expiry cap at 604,800 seconds (7 days)
//! - Overflow handling via checked_add
//! - No panics on any input
//! - Missing X-Amz-Algorithm returns Ok(None)
//! - Expired presigned URL correctly reports is_expired()

use s3_proxy::presigned_url::{parse_presigned_url, PresignedUrlError, PresignedUrlInfo};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Helper to build query params for a valid presigned URL
fn valid_presigned_params() -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert(
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );
    params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());
    params.insert("X-Amz-Expires".to_string(), "3600".to_string());
    params.insert(
        "X-Amz-Credential".to_string(),
        "AKIAIOSFODNN7EXAMPLE/20240115/us-east-1/s3/aws4_request".to_string(),
    );
    params.insert(
        "X-Amz-Signature".to_string(),
        "abcdef1234567890".to_string(),
    );
    params
}

// =============================================================================
// 1. Valid presigned URL with AWS4-HMAC-SHA256 parses successfully
// =============================================================================

#[test]
fn valid_presigned_url_parses_successfully() {
    let params = valid_presigned_params();
    let result = parse_presigned_url(&params);

    assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
    let info = result.unwrap();
    assert!(info.is_some(), "Expected Some(PresignedUrlInfo)");

    let info = info.unwrap();
    assert_eq!(info.expires_in_seconds, 3600);
    assert_eq!(info.algorithm, Some("AWS4-HMAC-SHA256".to_string()));
    assert!(info.credential.is_some());
}

// =============================================================================
// 2. Unsupported algorithm returns UnsupportedAlgorithm error
// =============================================================================

#[test]
fn unsupported_algorithm_sha512_returns_error() {
    let mut params = valid_presigned_params();
    params.insert(
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA512".to_string(),
    );

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    match result.unwrap_err() {
        PresignedUrlError::UnsupportedAlgorithm(alg) => {
            assert_eq!(alg, "AWS4-HMAC-SHA512");
        }
        other => panic!("Expected UnsupportedAlgorithm, got: {:?}", other),
    }
}

#[test]
fn unsupported_algorithm_empty_string_returns_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Algorithm".to_string(), "".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::UnsupportedAlgorithm(_)
    ));
}

#[test]
fn unsupported_algorithm_arbitrary_string_returns_error() {
    let mut params = valid_presigned_params();
    params.insert(
        "X-Amz-Algorithm".to_string(),
        "not-an-algorithm".to_string(),
    );

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::UnsupportedAlgorithm(_)
    ));
}

// =============================================================================
// 3. X-Amz-Expires at exactly 604800 (7 days) is accepted
// =============================================================================

#[test]
fn expires_at_cap_604800_is_accepted() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "604800".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_ok());
    let info = result.unwrap().unwrap();
    assert_eq!(info.expires_in_seconds, 604800);
}

// =============================================================================
// 4. X-Amz-Expires at 604801 returns ExpiresExceedsCap error
// =============================================================================

#[test]
fn expires_604801_returns_exceeds_cap_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "604801".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    match result.unwrap_err() {
        PresignedUrlError::ExpiresExceedsCap { value, max } => {
            assert_eq!(value, 604801);
            assert_eq!(max, 604_800);
        }
        other => panic!("Expected ExpiresExceedsCap, got: {:?}", other),
    }
}

#[test]
fn expires_very_large_value_returns_exceeds_cap_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "9999999999".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::ExpiresExceedsCap { .. }
    ));
}

// =============================================================================
// 5. X-Amz-Expires of 0 returns ExpiresNotPositive error
// =============================================================================

#[test]
fn expires_zero_returns_not_positive_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "0".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    match result.unwrap_err() {
        PresignedUrlError::ExpiresNotPositive(raw) => {
            assert_eq!(raw, "0");
        }
        other => panic!("Expected ExpiresNotPositive, got: {:?}", other),
    }
}

// =============================================================================
// 6. X-Amz-Expires of -1 returns ExpiresNotPositive error
// =============================================================================

#[test]
fn expires_negative_one_returns_not_positive_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "-1".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    match result.unwrap_err() {
        PresignedUrlError::ExpiresNotPositive(raw) => {
            assert_eq!(raw, "-1");
        }
        other => panic!("Expected ExpiresNotPositive, got: {:?}", other),
    }
}

#[test]
fn expires_large_negative_returns_not_positive_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "-100".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::ExpiresNotPositive(_)
    ));
}

// =============================================================================
// 7. X-Amz-Expires of "abc" returns ExpiresInvalid error
// =============================================================================

#[test]
fn expires_abc_returns_invalid_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "abc".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    match result.unwrap_err() {
        PresignedUrlError::ExpiresInvalid(raw) => {
            assert_eq!(raw, "abc");
        }
        other => panic!("Expected ExpiresInvalid, got: {:?}", other),
    }
}

#[test]
fn expires_float_returns_invalid_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "3.14".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::ExpiresInvalid(_)
    ));
}

#[test]
fn expires_empty_string_returns_invalid_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::ExpiresInvalid(_)
    ));
}

// =============================================================================
// 8. Invalid X-Amz-Date format returns InvalidDate error
// =============================================================================

#[test]
fn invalid_date_format_returns_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Date".to_string(), "not-a-date".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    match result.unwrap_err() {
        PresignedUrlError::InvalidDate(raw) => {
            assert_eq!(raw, "not-a-date");
        }
        other => panic!("Expected InvalidDate, got: {:?}", other),
    }
}

#[test]
fn invalid_date_too_short_returns_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Date".to_string(), "20240115T1200Z".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::InvalidDate(_)
    ));
}

#[test]
fn invalid_date_missing_z_suffix_returns_error() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Date".to_string(), "20240115T120000X".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        PresignedUrlError::InvalidDate(_)
    ));
}

// =============================================================================
// 9. Missing X-Amz-Algorithm returns Ok(None) (not a presigned URL)
// =============================================================================

#[test]
fn missing_algorithm_returns_ok_none() {
    let mut params = HashMap::new();
    params.insert("versionId".to_string(), "abc123".to_string());
    params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string());

    let result = parse_presigned_url(&params);
    assert!(matches!(result, Ok(None)));
}

#[test]
fn empty_params_returns_ok_none() {
    let params = HashMap::new();

    let result = parse_presigned_url(&params);
    assert!(matches!(result, Ok(None)));
}

// =============================================================================
// 10. Expired presigned URL correctly reports is_expired()
// =============================================================================

#[test]
fn expired_presigned_url_reports_is_expired() {
    // Signed 2 hours ago, expires in 1 hour → expired 1 hour ago
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
fn valid_presigned_url_reports_not_expired() {
    // Signed now, expires in 1 hour → still valid
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
fn presigned_url_just_expired_reports_is_expired() {
    // Signed 3601 seconds ago with 3600s expiry → just expired
    let signed_at = SystemTime::now() - Duration::from_secs(3601);
    let expires_at = signed_at.checked_add(Duration::from_secs(3600)).unwrap();

    let info = PresignedUrlInfo {
        signed_at,
        expires_in_seconds: 3600,
        expires_at,
        algorithm: Some("AWS4-HMAC-SHA256".to_string()),
        credential: None,
    };

    assert!(info.is_expired());
}

// =============================================================================
// Additional: Overflow handling (Requirement 26.3)
// =============================================================================

#[test]
fn expires_within_cap_does_not_overflow() {
    // Use a date far in the future but still within SystemTime range
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Date".to_string(), "20990101T000000Z".to_string());
    params.insert("X-Amz-Expires".to_string(), "604800".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_ok());
    let info = result.unwrap().unwrap();
    assert_eq!(info.expires_in_seconds, 604800);
}

#[test]
fn valid_boundary_expires_value_of_1_is_accepted() {
    let mut params = valid_presigned_params();
    params.insert("X-Amz-Expires".to_string(), "1".to_string());

    let result = parse_presigned_url(&params);
    assert!(result.is_ok());
    let info = result.unwrap().unwrap();
    assert_eq!(info.expires_in_seconds, 1);
}
