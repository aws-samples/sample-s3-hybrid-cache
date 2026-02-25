//! S3 Proxy - High-performance HTTP/HTTPS proxy for S3 with intelligent caching
//!
//! This library provides the core functionality for the S3 proxy server,
//! including HTTP/HTTPS request handling, caching, compression, and logging.

pub mod aws_chunked_decoder;
pub mod background_recovery;
pub mod bucket_settings;
pub mod cache;
pub mod cache_hit_update_buffer;
pub mod cache_initialization_coordinator;
pub mod cache_size_tracker;
pub mod cache_types;
pub mod cache_validator;
pub mod cache_writer;
pub mod capacity_manager;
pub mod compression;
pub mod config;
pub mod connection_pool;
pub mod dashboard;
pub mod disk_cache;
pub mod error;
pub mod health;
pub mod http_proxy;
pub mod https_connector;
pub mod https_proxy;
pub mod hybrid_metadata_writer;
pub mod inflight_tracker;
pub mod journal_consolidator;
pub mod journal_manager;
pub mod log_sampler;
pub mod logging;
pub mod metadata_cache;
pub mod metadata_lock_manager;
pub mod metrics;
pub mod orphaned_range_recovery;
pub mod otlp;
pub mod permissions;
pub mod presigned_url;
pub mod ram_cache;
pub mod range_handler;
pub mod s3_client;
pub mod shutdown;
pub mod signed_put_handler;
pub mod signed_request_proxy;
pub mod tcp_proxy;
pub mod tee_stream;
pub mod write_cache_manager;

pub use error::{ProxyError, Result};
