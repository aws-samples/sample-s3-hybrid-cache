//! Seeding and read-back helpers for the full-object tests moved here from `tests/`.
//!
//! The seeding calls the production full-object GET-miss fill,
//! `CacheManager::store_response_with_headers_witnessed`, with a witness captured the way the
//! request path captures one before its S3 request. It re-implements none of that fill.

use crate::cache::CacheManager;
use crate::cache_types::CacheMetadata;
use crate::Result;
use std::collections::HashMap;

/// A full-object fill through the production witnessed store, then one consolidation of the
/// key's journal so its `.meta` is visible to a read at once.
#[allow(async_fn_in_trait)]
pub(super) trait StoreResponseExt {
    async fn store_response(
        &self,
        cache_key: &str,
        response: &[u8],
        metadata: CacheMetadata,
    ) -> Result<()>;

    async fn store_response_with_headers(
        &self,
        cache_key: &str,
        response: &[u8],
        headers: HashMap<String, String>,
        metadata: CacheMetadata,
    ) -> Result<()>;
}

impl StoreResponseExt for CacheManager {
    async fn store_response(
        &self,
        cache_key: &str,
        response: &[u8],
        metadata: CacheMetadata,
    ) -> Result<()> {
        self.store_response_with_headers(cache_key, response, HashMap::new(), metadata)
            .await
    }

    async fn store_response_with_headers(
        &self,
        cache_key: &str,
        response: &[u8],
        headers: HashMap<String, String>,
        metadata: CacheMetadata,
    ) -> Result<()> {
        let witness = self.capture_fill_witness(cache_key).await;
        self.store_response_with_headers_witnessed(cache_key, response, headers, metadata, witness)
            .await?;
        if let Some(consolidator) = self.get_journal_consolidator().await {
            consolidator.consolidate_object(cache_key).await?;
        }
        Ok(())
    }
}

pub(super) use super::common::{
    put_through_write_cache, seed_validation_metadata, StoredObjectRead,
};

/// A range fill the way the request path makes one, for the moved tests that seed ranges
/// directly: `prepare_range_store`, a captured witness and `PreparedRangeFill::finish`
/// (`DiskCacheManager::store_range_witnessed_outcome`). A fill the manager declines stores
/// nothing and is not an error, as the tests that probe admission expect.
#[allow(async_fn_in_trait)]
pub(super) trait StoreRangeExt {
    #[allow(clippy::too_many_arguments)]
    async fn store_range(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<()>;
}
impl StoreRangeExt for crate::disk_cache::DiskCacheManager {
    async fn store_range(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<()> {
        self.store_range_witnessed_outcome(
            cache_key,
            start,
            end,
            data,
            object_metadata,
            ttl,
            compression_enabled,
        )
        .await
        .map(|_| ())
    }
}
/// Stores one part of a completed multipart object as the range it occupies: `start` through
/// `start + part.len() - 1` of an object `total` bytes long, then folds the key's journal so the
/// range is visible to a `.meta` read at once.
pub(super) async fn seed_part_range(
    cache_manager: &crate::cache::CacheManager,
    disk_cache: &std::sync::Arc<tokio::sync::RwLock<crate::disk_cache::DiskCacheManager>>,
    path: &str,
    start: u64,
    part: &[u8],
    total: u64,
) {
    let cache_key = crate::cache::CacheManager::generate_cache_key(path, None);
    let object_metadata = crate::cache_types::ObjectMetadata::new_with_headers(
        "\"mpu-etag\"".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        total,
        None,
        HashMap::new(),
    );
    disk_cache
        .write()
        .await
        .store_range(
            &cache_key,
            start,
            start + part.len() as u64 - 1,
            part,
            object_metadata,
            std::time::Duration::from_secs(3600),
            true,
        )
        .await
        .expect("seed part range");
    super::common::consolidate(cache_manager, &cache_key)
        .await
        .expect("consolidate the seeded part");
}

/// Give every witnessed `Add` in the cache's journals the token the key's write-order marker
/// holds now. Two fills on one volume share one marker, so their witnesses carry one token. A
/// fixture that builds a peer's fill in a second cache directory and copies its journal over
/// carries the peer's own marker token instead, and consolidation retires that `Add` as
/// superseded, which no fill on one volume can be. This restores the one-marker state.
pub(super) fn align_witness_tokens(meta_path: &std::path::Path) {
    let Some(token) = crate::cache::read_write_marker(meta_path).map(|m| m.token) else {
        return;
    };
    let Some(metadata_dir) = meta_path
        .ancestors()
        .find(|p| p.file_name().is_some_and(|n| n == "metadata"))
    else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(metadata_dir.join("_journals")) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("journal") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut out = String::new();
        for line in text.lines() {
            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(mut v) => {
                    if v["fill_witness"].is_string() {
                        v["fill_witness"] = serde_json::Value::String(token.clone());
                    }
                    out.push_str(&v.to_string());
                }
                Err(_) => out.push_str(line),
            }
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
}
