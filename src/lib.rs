//! Hybrid Cache for Amazon S3 - High-performance HTTP/HTTPS proxy for S3 with intelligent caching
//!
//! This library provides the core functionality for the S3 proxy server,
//! including HTTP/HTTPS request handling, caching, compression, and logging.

pub mod accept_policy;
pub mod aws_chunked_decoder;
pub mod bandwidth_limiter;
pub mod bench_metadata;
pub mod bucket_settings;
pub mod bulk_write_invalidation;
pub mod cache;
pub mod cache_hit_update_buffer;
pub mod cache_initialization_coordinator;
pub mod cache_size_tracker;
pub mod cache_types;
pub mod cache_validator;
pub mod capacity_guard;
pub mod capacity_manager;
pub mod compression;
pub mod config;
pub mod connection_pool;
pub mod dashboard;
pub mod destination_policy;
pub mod disk_cache;
pub mod error;
pub mod eviction_result;
pub(crate) mod eviction_worklist;
pub mod fill_visibility_tracker;
pub mod health;
pub mod hedged_fetch;
pub mod http_proxy;
pub mod https_connector;
pub mod https_proxy;
pub mod hybrid_metadata_writer;
pub mod inflight_ledger;
pub mod inflight_tracker;
pub mod journal_consolidator;
pub mod journal_manager;
pub mod ledger_repair;
pub mod logging;
pub mod metadata_cache;
pub mod metadata_lock_manager;
pub mod metrics;
pub mod origin_identity;
pub mod orphan_evidence;
pub mod orphan_reclaim;
pub mod otlp;
pub mod path_safety;
pub mod permissions;
pub mod permit_body;
pub mod presigned_url;
pub mod ram_cache;
pub mod range_handler;
pub mod revalidation;
pub mod s3_client;
pub mod scanner_limiter;
pub mod scanner_registry;
pub mod shutdown;
pub mod signed_put_handler;
pub mod signed_request_proxy;
pub mod st_size_fold;
pub mod staged_authority;
pub mod staged_fold;
pub mod stamp_backlog;
pub mod tcp_proxy;
pub mod tee_stream;
pub mod throttle_stream;
pub mod tls_proxy_listener;
pub mod tls_trust_store;
pub mod upstream_overrides;
pub mod volume_stall;
pub mod write_cache_manager;
pub mod write_ledger;

pub use error::{ProxyError, Result, UpstreamSendFailureKind};

// Re-export the S3 client trait seam so tests (and downstream crates) can
// reach it without depending on internal module paths. The concrete `S3Client`
// type stays reachable via the `s3_client` module for callers that need it.
pub use s3_client::{S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};
