//! Error Module
//!
//! Defines error types and result types used throughout the S3 proxy application.

use thiserror::Error;

/// Main error type for the S3 proxy
#[derive(Error, Debug, Clone)]
pub enum ProxyError {
    #[error("IO error: {0}")]
    IoError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("DNS error: {0}")]
    DnsError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("Lock contention: {0}")]
    LockContention(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("S3 error: {0}")]
    S3Error(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Invalid range: {0}")]
    InvalidRange(String),

    #[error("System error: {0}")]
    SystemError(String),

    #[error("Retry after {0} seconds")]
    RetryAfter(u64),
}

impl From<std::io::Error> for ProxyError {
    fn from(err: std::io::Error) -> Self {
        ProxyError::IoError(err.to_string())
    }
}

impl From<hyper::Error> for ProxyError {
    fn from(err: hyper::Error) -> Self {
        ProxyError::HttpError(err.to_string())
    }
}

impl From<serde_json::Error> for ProxyError {
    fn from(err: serde_json::Error) -> Self {
        ProxyError::SerializationError(err.to_string())
    }
}

impl From<serde_yaml::Error> for ProxyError {
    fn from(err: serde_yaml::Error) -> Self {
        ProxyError::SerializationError(err.to_string())
    }
}

impl From<trust_dns_resolver::error::ResolveError> for ProxyError {
    fn from(err: trust_dns_resolver::error::ResolveError) -> Self {
        ProxyError::DnsError(err.to_string())
    }
}

/// Result type alias for the S3 proxy
pub type Result<T> = std::result::Result<T, ProxyError>;
