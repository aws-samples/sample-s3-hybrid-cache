//! Presigned URL parsing and validation
//!
//! Provides utilities for parsing AWS SigV4 presigned URLs and checking expiration
//! without making requests to S3.

use chrono::DateTime;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

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
/// Returns None if this is not a presigned URL (missing required parameters)
pub fn parse_presigned_url(query_params: &HashMap<String, String>) -> Option<PresignedUrlInfo> {
    // Check if this is a presigned URL by looking for X-Amz-Algorithm
    if !query_params.contains_key("X-Amz-Algorithm") {
        return None;
    }

    // Extract X-Amz-Date (required)
    let amz_date = query_params.get("X-Amz-Date")?;
    let signed_at = parse_amz_date(amz_date)?;

    // Extract X-Amz-Expires (required)
    let expires_str = query_params.get("X-Amz-Expires")?;
    let expires_in_seconds: u64 = expires_str.parse().ok()?;

    // Calculate expiration time
    let expires_at = signed_at + Duration::from_secs(expires_in_seconds);

    // Extract optional fields
    let algorithm = query_params.get("X-Amz-Algorithm").cloned();
    let credential = query_params.get("X-Amz-Credential").cloned();

    debug!(
        "Parsed presigned URL: signed_at={:?}, expires_in={}s, expires_at={:?}",
        signed_at, expires_in_seconds, expires_at
    );

    Some(PresignedUrlInfo {
        signed_at,
        expires_in_seconds,
        expires_at,
        algorithm,
        credential,
    })
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
        assert!(result.is_some());

        let info = result.unwrap();
        assert_eq!(info.expires_in_seconds, 3600);
        assert_eq!(info.algorithm, Some("AWS4-HMAC-SHA256".to_string()));
    }

    #[test]
    fn test_parse_presigned_url_not_presigned() {
        let mut query_params = HashMap::new();
        query_params.insert("versionId".to_string(), "abc123".to_string());

        let result = parse_presigned_url(&query_params);
        assert!(result.is_none());
    }

    #[test]
    fn test_is_expired() {
        // Create an expired presigned URL (signed 2 hours ago, expires in 1 hour)
        let signed_at = SystemTime::now() - Duration::from_secs(7200);
        let expires_at = signed_at + Duration::from_secs(3600);

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
        let expires_at = signed_at + Duration::from_secs(3600);

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
}
