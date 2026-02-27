# Changelog

All notable changes to S3 Proxy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.6.8] - 2026-02-27

### Changed
- **Access log `source_region` field**: Added `source_region` as the 25th field in S3 server access log records, matching the current AWS S3 log format spec. Always emits `-` since the proxy cannot determine request origin region (PrivateLink, Direct Connect, and non-AWS IPs are also `-` in real S3 logs).

## [1.6.7] - 2026-02-25

### Security
- Updated `bytes` 1.11.0 → 1.11.1 (CVE-2026-25541: integer overflow in `BytesMut::reserve`)
- Updated `time` 0.3.44 → 0.3.47 (CVE-2026-25727: stack exhaustion in RFC 2822 parsing)

## [1.6.6] - 2026-02-25

### Added
- **`endpoint_overrides` config option**: Static hostname-to-IP mappings that bypass DNS resolution for S3 endpoints. Useful for S3 PrivateLink deployments where the proxy cannot use DNS to resolve S3 endpoints to PrivateLink ENI IPs (e.g., on-prem without Route 53 Resolver inbound endpoints). Works for both HTTP (connection pool) and HTTPS (TCP passthrough) traffic. Load-balances across multiple IPs per hostname.

### Changed
- Updated PrivateLink documentation in GETTING_STARTED.md and CONFIGURATION.md to document `endpoint_overrides` as alternative to Route 53 Resolver
- Added `endpoint_overrides` example to `config/config.example.yaml`

## [1.6.5] - 2026-02-24

### Fixed
- **Multipart upload part isolation**: Parts from concurrent multipart uploads to the same S3 key with different upload_ids no longer overwrite each other. Parts are now stored in upload-specific directories (`mpus_in_progress/{upload_id}/part{N}.bin`) instead of the shared `ranges/` directory. On CompleteMultipartUpload, parts are moved to their final `ranges/` location with byte offset names. Cleanup (abort/expiration) is simplified to a single `remove_dir_all()`.

### Changed
- Removed `range_file_path` field from `CachedPartInfo` struct (path is now deterministic from upload_id + part_number)
- Simplified `cleanup_multipart_upload()` and `cleanup_incomplete_multipart_cache()` to single directory removal
- Simplified incomplete upload eviction in `cache_size_tracker`, `cache.rs`, and `write_cache_manager`

## [1.6.4] - 2026-02-23

### Fixed
- **Path-style AP/MRAP alias SigV4 signature preservation**: Path-style access point alias requests (e.g., `--endpoint-url http://s3-accesspoint.eu-west-1.amazonaws.com` with alias in path) are now forwarded to S3 without host or path rewriting. Previously, the proxy reconstructed a virtual-hosted upstream host and stripped the alias from the path, which broke the AWS SigV4 signature (signed for the original host/path). S3 handles path-style AP routing natively; the alias in the first path segment provides correct cache key namespacing without rewriting.

## [1.6.3] - 2026-02-23

### Fixed
- **Journal consolidation TtlRefresh/AccessUpdate validation**: Object-level journal operations (TtlRefresh, AccessUpdate) are now validated by checking metadata file existence instead of range file existence. Previously, these operations used dummy range coordinates (0-0) which never matched actual range files, causing consolidation to skip them entirely.
- **Test suite JournalConsolidator initialization**: Removed erroneous `CacheManager.initialize()` calls from ~25 test files that don't set up JournalConsolidator. Fixed `eviction_buffer_test` to use `new_with_shared_storage` with correct `max_cache_size_limit`. Fixed flock-based lock release assertions in `global_eviction_lock_test`.

## [1.6.2] - 2026-02-22

### Fixed
- **Cache key namespace collision**: Access point cache key folders now include AWS reserved suffixes (`-s3alias` for regional APs, `.mrap` for MRAPs) to prevent collision with S3 bucket names. Previously, bare AP/MRAP identifiers could match bucket names, causing cross-namespace cache collisions.

### Added
- **Path-style AP alias support**: Requests with Host `s3-accesspoint.{region}.amazonaws.com` and an AP alias (ending in `-s3alias`) in the first path segment are now detected. The proxy reconstructs the correct upstream host, strips the alias from the forwarded path, and uses the alias as the cache key folder.
- **Path-style MRAP alias support**: Requests with Host `accesspoint.s3-global.amazonaws.com` and an MRAP alias (ending in `.mrap`) in the first path segment are now detected. The proxy reconstructs the upstream host (stripping `.mrap` from the hostname), strips the alias from the forwarded path, and uses the alias as the cache key folder.
- **AP/MRAP documentation updates**: Updated `docs/CACHING.md` with reserved suffix approach, path-style alias detection, and known ARN-vs-alias cache key divergence limitation. Updated `docs/GETTING_STARTED.md` with AP alias and MRAP alias usage examples.

## [1.6.1] - 2026-02-22

### Fixed
- **Graceful shutdown now fully wired**: Cache manager and connection pool were never registered with the shutdown coordinator, making cache lock release (Step 3) and connection pool closure (Step 4) dead code during shutdown. Both are now wired via `set_cache_manager()` and `set_connection_pool()`.
- **HTTP/HTTPS/TCP proxy accept loops are shutdown-aware**: All proxy `start()` methods now accept a `ShutdownSignal` and use `tokio::select!` to break the accept loop on shutdown. Previously, these infinite loops were killed by task cancellation with no cleanup.
- **HTTP proxy drains in-flight connections on shutdown**: After stopping the accept loop, the HTTP proxy waits up to 5 seconds for active connections (tracked via `active_connections` counter) to complete before returning.
- **Health and metrics servers are shutdown-aware**: Both servers now accept a `ShutdownSignal` and break their accept loops on shutdown, matching the existing dashboard server pattern.
- **Background tasks stop cleanly on shutdown**: Cache hit update buffer flush and journal consolidation background tasks now listen for the shutdown signal and break their loops. The cache hit buffer performs a final flush before stopping.
- **Process waits for shutdown coordinator to complete**: `main()` now awaits the shutdown coordinator task instead of using `tokio::select!` that could exit before teardown finished.
- **Shutdown coordinator type alignment**: `ShutdownCoordinator` now uses `Arc<CacheManager>` and `Arc<Mutex<ConnectionPoolManager>>` to match the actual types used throughout the system, instead of the previously mismatched `Arc<RwLock<...>>` wrappers.

## [1.6.0] - 2026-02-17

### Fixed
- **Access point and MRAP cache key collisions**: Cache keys for S3 Access Point and Multi-Region Access Point (MRAP) requests are now prefixed with the access point identifier extracted from the Host header. Regional AP requests (`{name}-{account_id}.s3-accesspoint.{region}.amazonaws.com`) use `{name}-{account_id}/` as the prefix. MRAP requests (`{mrap_alias}.accesspoint.s3-global.amazonaws.com`) use `{mrap_alias}/` as the prefix. Previously, all access point requests with the same object path produced identical cache keys regardless of which access point they came from, causing cross-access-point data collisions. Regular path-style and virtual-hosted-style requests are unaffected.

### Added
- **Access point documentation**: Updated `docs/CACHING.md` with access point cache key prefixing details. Updated `docs/GETTING_STARTED.md` with DNS routing, `--endpoint-url` usage, and hosts file / Route 53 configuration for access points and MRAPs.

## [1.5.3] - 2026-02-17

### Fixed
- **S3 error responses no longer cached**: The streaming GET path attempted to cache S3 error responses (403, 500, etc.) as if they were object data, causing "data size mismatch" errors in `store_range`. The error body (~8KB XML) was collected and passed to `store_range` which rejected it due to size mismatch with the expected range. Now checks `status.is_success()` before setting up the TeeStream cache channel, matching the existing behavior in the buffered response path.

## [1.5.2] - 2026-02-17

### Changed
- **Object-level cache expiration**: Expiration is now tracked at the object level (`NewCacheMetadata.expires_at`) instead of per-range (`RangeSpec.expires_at`). All cached ranges of the same object share a single freshness state. Simplifies expiration checks and TTL refresh after 304 responses.
- **Removed per-range expires_at**: The `expires_at` field, `is_expired()`, and `refresh_ttl()` methods are removed from `RangeSpec`. Eviction fields (`last_accessed`, `access_count`, `frequency_score`) remain per-range.
- **Expiration check API**: `check_range_expiration(cache_key, start, end)` replaced by `check_object_expiration(cache_key)`. `refresh_range_ttl(cache_key, start, end, ttl)` replaced by `refresh_object_ttl(cache_key, ttl)`.

### Fixed
- **Metadata read failures treated as expired (security fix)**: If the proxy cannot read or deserialize metadata during an expiration check, it now treats the cached data as expired and forwards the request to S3. Previously, metadata read errors were silently treated as "not expired," which could serve stale or unauthorized data — particularly dangerous with `get_ttl=0` buckets.
- **Correct TTL in all metadata creation paths**: Metadata created by the hybrid metadata writer and journal consolidator now uses the resolved per-bucket TTL instead of a ~100-year sentinel. Journal entries carry `object_ttl_secs` so consolidation creates metadata with the correct `expires_at`. Orphan recovery uses `Duration::ZERO` (force revalidation) since the original TTL is unknown.

## [1.5.1] - 2026-02-16

### Fixed
- **Zero-TTL bypass removal**: Removed three bypass blocks in `http_proxy.rs` that skipped cache lookup when `get_ttl=0` or `head_ttl=0`. Zero-TTL requests now go through the normal cache flow with immediate expiration and conditional revalidation via `If-Modified-Since`, enabling 304 bandwidth savings.
- **Full-object GET expiration checking**: Added expiration check to the full-object GET path (previously missing). Cached full-object data is now validated with S3 before serving when expired, matching the existing range request behavior.
- **TTL refresh after 304 uses resolved per-bucket TTL**: The TTL refresh after a 304 Not Modified response now uses the resolved per-bucket `get_ttl` instead of the global `config.cache.get_ttl`. Prevents zero-TTL bucket data from being refreshed with the global TTL (e.g., 10 years).

### Changed
- **Documentation**: Updated `docs/CACHING.md` "Zero TTL Revalidation" section to describe correct behavior. Added "Settings Apply at Cache-Write Time" subsection. Added cache-write-time note to `docs/CONFIGURATION.md`.

## [1.5.0] - 2026-02-15

### Added
- **Bucket-level cache settings**: Per-bucket and per-prefix cache configuration via `_settings.json` files at `cache_dir/metadata/{bucket}/_settings.json`. Configure TTLs, read/write caching, compression, and RAM cache eligibility per bucket with hot reload (no proxy restart). Settings cascade: Prefix → Bucket → Global.
- **Zero TTL revalidation**: `get_ttl: "0s"` caches data on disk but revalidates with S3 on every request. Saves bandwidth on 304 Not Modified responses.
- **Read cache control**: `read_cache_enabled: false` makes the proxy act as a pure pass-through for GET requests (no disk or RAM caching). Supports allowlist pattern with global `read_cache_enabled: false` and per-bucket overrides.
- **Per-bucket metrics**: `bucket_cache_hit_count` and `bucket_cache_miss_count` counters for buckets with `_settings.json` files.
- **Dashboard bucket stats table**: Sortable table with per-bucket hit/miss stats, resolved settings, and expandable prefix overrides. `/api/bucket-stats` API endpoint.
- **JSON schema**: `docs/bucket-settings-schema.json` for IDE validation of `_settings.json` files.
- **Example settings files**: Six example configurations in `docs/examples/`.

### Changed
- **Dashboard renamed**: "S3 Proxy Dashboard" → "S3 Hybrid Cache".
- **Compression control**: Per-bucket `compression_enabled` setting. `CompressionAlgorithm::None` variant for uncompressed storage.

### Removed
- **TTL overrides**: Removed `ttl_overrides` YAML config and `TtlOverride` struct. Replaced by bucket-level cache settings.

## [1.4.5] - 2026-02-11

### Changed
- **Percentage metrics renamed**: `cache_hit_rate` → `cache_hit_rate_percent`, `ram_cache_hit_rate` → `ram_cache_hit_rate_percent`, `success_rate` → `success_rate_percent` in `/metrics` JSON and OTLP. Makes units explicit in metric names.
- **RAM cache always compresses**: RAM cache now uses LZ4 compression regardless of the global `compression.enabled` flag, saving memory for compressible data even when disk compression is disabled.
- **OTLP_METRICS.md updated**: Replaced stale placeholder metric names with actual metric names matching the `/metrics` JSON API.

### Removed
- Dead code: `extract_path_from_cache_key` in RAM cache (unused after compression change).

## [1.4.4] - 2026-02-11

### Fixed
- **Request metrics always zero**: `record_request()` was never called from the HTTP proxy, so `request_metrics.total_requests`, `successful_requests`, `failed_requests`, `average_response_time_ms`, and `requests_per_second` were always 0 in `/metrics` JSON, OTLP, and CloudWatch. Added `record_request()` call at the end of `handle_request()` with actual elapsed time and success/failure status.

## [1.4.3] - 2026-02-11

### Added
- **Per-tier cache metrics in /metrics and OTLP**: The `/metrics` JSON endpoint and OTLP export now include RAM cache stats (`ram_cache_hits`, `ram_cache_misses`, `ram_cache_evictions`, `ram_cache_max_size`), metadata cache stats (`metadata_cache_hits`, `metadata_cache_misses`, `metadata_cache_entries`, `metadata_cache_max_entries`, `metadata_cache_evictions`, `metadata_cache_stale_refreshes`), and `bytes_served_from_cache`. These match the dashboard's per-tier breakdown — all three surfaces (dashboard, `/metrics`, OTLP) now use the same data source.

## [1.4.2] - 2026-02-11

### Changed
- **OTLP metric names match /metrics JSON API**: OTLP gauge names now use the JSON field path from the `/metrics` endpoint (e.g. `cache.cache_hits`, `coalescing.waits_total`, `request_metrics.total_requests`). Removed the `cache_type` dimension on `cache.size` — each size field is a separate metric (`cache.total_cache_size`, `cache.read_cache_size`, `cache.write_cache_size`, `cache.ram_cache_size`). All metrics share only the resource attributes (`host.name`, `service.name`, `service.version`).

### Fixed
- **RAM cache serving corrupted data for non-compressible files**: `compress_data_content_aware_with_fallback` returned `was_compressed = false` for content-types that skip compression (zip, jpg, etc.), even though the data was wrapped in LZ4 frame format with uncompressed blocks. On RAM cache retrieval, the `compressed: false` flag caused the LZ4 frame bytes to be served directly to clients without decompression, triggering `AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH`. The flag now correctly returns `true` whenever data is in frame format, since `FrameDecoder` is always needed to unwrap it.

## [1.4.1] - 2026-02-11

### Fixed
- **Broken conditional request validation in S3 client**: `parse_http_date()` in `s3_client.rs` always returned `SystemTime::now()` instead of parsing the date string, making `If-Modified-Since` and `If-Unmodified-Since` comparisons meaningless. Now uses the `httpdate` crate (already used in `cache.rs`).

### Removed
- **Dead per-IP request metrics in S3 client**: Removed `connection_ip` from `S3Response`, `record_request_success()`, `record_request_failure()`, and `extract_ip_from_error()`. These fed per-IP health metrics in the pool manager, but the feedback loop was broken — Hyper's opaque connection pool prevented accurate IP attribution, so success metrics were always attributed to `0.0.0.0` and failure metrics to `127.0.0.1`. The pool manager's DNS resolution and IP selection for new connections (via `CustomHttpsConnector`) continue to work correctly.

## [1.4.0] - 2026-02-11

### Added
- **Real OTLP metrics export**: Replaced placeholder OTLP exporter with a working implementation using the OpenTelemetry SDK. Exports cache, request, connection pool, compression, coalescing, and process metrics to any OTLP-compatible collector (CloudWatch Agent, Prometheus, OpenTelemetry Collector) via HTTP protobuf. Enable with `metrics.otlp.enabled: true` and point `endpoint` to your collector.

### Fixed
- **OpenTelemetry dependencies on Linux/macOS**: OpenTelemetry crates were accidentally scoped under `[target.'cfg(windows)'.dependencies]`, preventing compilation on non-Windows platforms. Moved to main `[dependencies]` section. Added `rt-tokio` feature to `opentelemetry_sdk` for async periodic export.

## [1.3.1] - 2026-02-11

### Fixed
- **Dashboard disk cache misses always showing 0**: Disk cache miss count was calculated as `get_misses - ram_misses`, but RAM misses include both "miss RAM, hit disk" and "miss RAM, miss disk" cases, making `ram_misses >= get_misses` and the subtraction always 0. Disk misses now correctly use `get_misses` directly since every overall cache miss is also a disk miss.

## [1.3.0] - 2026-02-10

### Changed
- **BREAKING: LZ4 frame format migration**: All cached data now uses LZ4 frame format with content checksum (xxHash-32) for integrity verification on every cache read. Existing cache must be flushed before upgrading (`rm -rf cache_dir/*`). Old block-format `.bin` files are not compatible with the new frame decoder.
- **Simplified versionId handling**: Requests with `?versionId=` bypass cache entirely (no cache read, no cache write). Removes the previous version-matching logic that compared cached `x-amz-version-id` headers. Bypass metric reason unified to `versioned_request`.
- **Non-compressible data uses frame format**: Content-aware compression now wraps non-compressible data (JPEG, PNG, etc.) in LZ4 frame format with uncompressed blocks instead of storing raw bytes. All `.bin` files use frame format regardless of compressibility.
- **Compression when globally disabled**: When `compression.enabled: false`, data is still wrapped in LZ4 frame format with uncompressed blocks for integrity checksums.

### Fixed
- **Signed DELETE cache invalidation**: `aws s3 rm` (signed DELETE with SigV4) now invalidates proxy cache on success. Previously, only unsigned DELETE requests triggered cache invalidation.

### Removed
- **`--compression-enabled` CLI flag**: Use `COMPRESSION_ENABLED` env var or `compression.enabled` config option instead.
- **`CompressionAlgorithm::None` variant**: All cached data uses LZ4 frame format. The `None` variant is removed; metadata records `Lz4` for all entries.
- **`get_cached_version_id()` method**: Dead code after versionId bypass simplification.

## [1.2.7] - 2026-02-10

### Added
- **Proxy identification header**: Adds a `Referer` header (`s3-hybrid-cache/{version} ({hostname})`) to requests forwarded to S3. Appears in S3 Server Access Logs for usage tracking and per-instance debugging. Skips injection when the header already exists or is included in SigV4 `SignedHeaders`. Configurable via `server.add_referer_header` (default: `true`).

### Fixed
- **Cache hit/miss statistics accuracy**: Coalescing waiter paths now correctly record cache hits when serving from cache and cache misses only when falling back to S3. Previously, all requests entering the coordination path were counted as misses regardless of outcome.

## [1.2.6] - 2026-02-10

### Changed
- **Validation scan: streaming parallel processing**: Daily validation scan no longer collects all `.meta` file paths into a `Vec` before processing. Uses `WalkDir` as a streaming iterator with rayon's `par_bridge()` to process files in parallel as they're discovered. Memory usage is O(rayon_threads) instead of O(total_files). Atomic counters accumulate results lock-free. Progress logged every 100K files. Scales to PB-sized caches with hundreds of millions of metadata files.

### Fixed
- **Coalescing waiters re-fetching from S3**: After a fetcher completed, waiters called `forward_get_head_to_s3_and_cache` which always goes to S3 — defeating the purpose of coalescing. Waiters now try the cache first via `serve_from_cache_or_s3`, only falling back to S3 on cache miss. Part-number waiters now try `lookup_part` before falling back. This eliminates redundant S3 fetches and the associated size over-counting from duplicate `store_range` calls.
- **Size tracking: persistent dedup across flush windows**: The `add_range` dedup `HashSet` was cleared every 5 seconds on flush, allowing the same range to be counted again in the next window. The dedup set now persists until the daily validation scan resets it.

## [1.2.5] - 2026-02-10

### Changed
- **RAM cache auto-disabled when get_ttl=0**: When `get_ttl` is set to `0s`, the RAM data cache is automatically disabled during config loading. RAM cache has no TTL check on the hit path and would serve stale data, bypassing the per-request S3 validation that `get_ttl=0` requires. The MetadataCache (for `.meta` object metadata) remains active regardless.

### Fixed
- **Cross-instance size over-counting**: Before adding to the size accumulator, check if the range file already exists on disk. If another instance already cached the same range on shared storage, skip the size increment. Reduces stampede over-counting from 23× to near-accurate. The `exists()` check is essentially free on NFS with `lookupcache=pos` (positive lookups are cached).

## [1.2.4] - 2026-02-10

### Fixed
- **Stale range data after PUT overwrite**: When an object is overwritten via PUT, the proxy now invalidates all cached range data (RAM and disk) for that cache key. Previously, old range files from prior GET requests survived the overwrite and could be served to clients, causing checksum mismatches. The fix adds prefix-based RAM cache invalidation (`invalidate_by_prefix`) to remove all `{cache_key}:range:*` entries, and ensures the metadata cache is refreshed after storing new PUT data.
- **Stampede size tracking (same instance)**: Range request waiters now recompute cache overlap after the fetcher completes instead of reusing the stale overlap from before the wait. Prevents waiters from re-fetching from S3 and double-counting size via `accumulator.add()` for data already cached by the fetcher.
- **Stampede size tracking (cross instance)**: `SizeAccumulator` now deduplicates range writes within each flush window (~5 seconds) using a `(cache_key_hash, start, end)` set. When multiple instances write the same range to shared storage, only the first write per flush window increments the size delta. The dedup set is cleared on flush. Existing `add()` and `subtract()` paths are unchanged.

## [1.2.3] - 2026-02-10

### Fixed
- **Dashboard property tests**: Updated tests to match current API — removed reference to deleted `cache_effectiveness` field, added `DashboardConfig` parameter to `ApiHandler::new`, added `cache_stats_refresh_ms` and `logs_refresh_ms` fields to `SystemInfoResponse` initializers.

## [1.2.2] - 2026-02-10

Closed off edge cases around part uploads and downloads, accelerated cache hits for Get Part requests, handled potential signing of range header, and optimized parallel requests for the same cache miss.

### Added
- **Download coordination (coalescing)**: When multiple concurrent requests arrive for the same uncached resource, only one request fetches from S3 while others wait. Covers full-object GETs, range requests (signed and unsigned), and part-number requests. Waiters serve from cache after the fetcher completes, reducing redundant S3 fetches. Configurable via `download_coordination.enabled` (default: true) and `download_coordination.wait_timeout_secs` (default: 30s).
- **Coalescing metrics**: New metrics track download coordination effectiveness: `waits_total`, `cache_hits_after_wait_total`, `timeouts_total`, `s3_fetches_saved_total`, `average_wait_duration_ms`, `fetcher_completions_success`, `fetcher_completions_error`. Exposed via `/metrics` endpoint.
- **Part ranges storage**: Multipart object parts now store exact byte ranges (`part_ranges: HashMap<u32, (u64, u64)>`) instead of assuming uniform part sizes. Enables accurate cache lookups for objects with variable-sized parts.
- **CompleteMultipartUpload filtering**: During multipart completion, only parts referenced in the request are retained. Unreferenced cached parts are deleted. ETag validation ensures cached parts match the request.
- **Content-Range parsing**: GET responses with `partNumber` parameter now parse the `Content-Range` header to store accurate byte ranges for external objects (not uploaded through proxy).

### Changed
- **ETag mismatch handling**: When storing a range with a different ETag than existing cached data, the proxy now invalidates existing ranges and caches the new data instead of returning an error. This handles object overwrites gracefully.
- **Range modification documentation**: Updated config comment to clarify dual-mode design: range consolidation applies only to unsigned requests; signed requests preserve exact Range headers for signature validity.

### Removed
- **Request delay behavior**: Removed the 5-second sleep and 503 retry mechanism for concurrent part requests. Replaced by InFlightTracker-based download coordination which is more efficient and doesn't block requests.

## [1.2.1] - 2026-02-10

### Fixed
- **Dashboard disk cache hit rate**: Disk cache stats now subtract RAM cache hits/misses from the overall totals, showing disk-tier-only performance. Previously the disk section displayed combined RAM+disk numbers.

### Changed
- **Dashboard overall stats**: Removed redundant "Cache Hit Rate" from overall statistics section. RAM and disk hit rates are shown separately in their respective sections.

## [1.2.0] - 2026-02-09

Stabilized multi-instance size tracking, fixed over-eviction race conditions, added streaming disk cache and parallel NFS operations for performance, improved dashboard accuracy, and reduced log noise. Shared-storage cache coordination is fully operational.

## [1.1.51] - 2026-02-09

### Changed
- **Dead code cleanup**: Removed 2 unused modules (`streaming_tee`, `performance_logger`), 4 unused functions, 1 deprecated method with zero callers, 9 unused struct fields, and their associated test file. Fixed incorrect `#[allow(dead_code)]` on `DiskCacheManager.write_cache_enabled` (field is actually used). Zero behavior change.

## [1.1.50] - 2026-02-09

### Changed
- **Lock file cleanup logging**: Downgraded "Failed to remove lock file on drop" from `warn!` to `debug!` when the error is `NotFound`. On shared NFS storage, another instance may have already cleaned up the lock file — this is expected, not an error.

## [1.1.49] - 2026-02-09

### Fixed
- **Range validation with zero content_length**: When cached metadata has `content_length: 0` (not yet populated), the proxy passed `Some(0)` to range parsing which rejected every range as "Start position exceeds content length". Now treats `content_length == 0` as unknown and skips range validation, forwarding to S3 instead.

### Changed
- **Range parse error logging**: Downgraded "Invalid range specification" from `warn!` to `debug!` since the proxy correctly forwards these to S3. Added `content_length` context to the log. Added `cache_key` to the forwarding-to-S3 debug message.

## [1.1.48] - 2026-02-09

### Fixed
- **Eviction stale file handle recovery (complete)**: Extended ESTALE recovery to cover both `open()` and `lock_exclusive()` calls during batch eviction lock acquisition. Previously only `open()` was retried; now the full open+lock sequence is retried once on stale NFS file handles.

### Changed
- **Eviction log deduplication**: Downgraded inner batch eviction lock failure messages (`disk_cache` and `BATCH_EVICTION`) from `warn!` to `debug!`. The top-level `EVICTION_ERROR` remains at `warn!`, eliminating triple-logging of the same error.

## [1.1.47] - 2026-02-09

### Fixed
- **Cache initialization coordinator size mismatch**: The `CacheConfig` passed to `CacheInitializationCoordinator` had a hardcoded 1 GB `max_cache_size` instead of reading the actual configured value from `inner.statistics.max_cache_size_limit`. This caused incorrect "Cache over capacity" warnings at startup when the configured limit differed from 1 GB.

### Changed
- **Log noise reduction (continued)**: Downgraded two remaining range-miss `warn!` messages to `debug!`: "Range file missing (will fetch from S3)" in `disk_cache.rs` and "Range spec not found for streaming" in `http_proxy.rs`. These are normal cache miss scenarios with graceful fallback, not operational concerns.

## [1.1.46] - 2026-02-09

### Fixed
- **Eviction stale file handle recovery**: Batch delete lock acquisition now recovers from stale NFS file handles (ESTALE/os error 116) by deleting the stale lock file and retrying once, preventing eviction from getting stuck when lock files have invalid handles on shared storage.

### Changed
- **Log noise reduction**: Downgraded "Range file missing" (fetching from S3), "Range file missing for streaming" (falling back to buffered), "Failed to create stream for range" (fallback), and "Eviction freed no ranges" from `warn!` to `debug!`. These are normal cache miss / eviction scenarios with graceful recovery, not operational concerns.

## [1.1.45] - 2026-02-09

### Changed
- **Dashboard: Disk Revalidated metric**: Renamed "Stale Refreshes" to "Disk Revalidated" and changed from raw count to percentage of total metadata lookups. Updated tooltip to accurately describe the TTL-based revalidation mechanism.

## [1.1.44] - 2026-02-09

### Fixed
- **RAM Cache Range Fix — Streaming Path**: The streaming path (`serve_range_from_cache`) bypassed RAM cache entirely for ranges >= `disk_streaming_threshold` (1 MiB). It never checked RAM, never promoted disk hits to RAM, and never recorded RAM hit/miss statistics. Added `get_range_from_ram_cache` and `promote_range_to_ram_cache` methods to CacheManager. The streaming path now checks RAM cache before disk I/O (serving hits as buffered 206 responses), collects streamed chunks on disk hits and promotes to RAM cache after completion (skipping promotion for ranges exceeding `max_ram_cache_size`), and records RAM cache hits/misses for dashboard statistics from both streaming and buffered paths.

## [1.1.43] - 2026-02-09

### Fixed
- **Dashboard: Metadata Cache Hit/Miss Counters**: Dashboard was using `head_hits`/`head_misses` from CacheManager statistics (which were never incremented for HEAD hits) instead of the MetadataCache's own hit/miss counters. Switched to `metadata_cache.metrics()` counters which are correctly tracked.

## [1.1.42] - 2026-02-09

### Fixed
- **Streaming Disk Cache Hit Counter**: The streaming range cache hit path (`serve_range_from_cache`) was not calling `update_statistics`, so disk cache hits for ranges >= `disk_streaming_threshold` (1 MiB) were not counted. Dashboard showed near-zero hit rate despite hundreds of streaming hits per second in logs.

### Changed
- **Dashboard: RAM Metadata Cache Card**: Renamed title from "Metadata Cache" to "RAM Metadata Cache", updated subtitle to "In-memory cache for .meta objects (HEAD + GET)", corrected tooltips to say "metadata lookups" instead of "HEAD requests". Added "Cached Entries" line showing current/max entries.

## [1.1.41] - 2026-02-09

### Fixed
- **Over-Eviction Race Condition**: Sequential evictions read stale `size_state.json`, causing cache to drop to ~37% instead of target 80%. Both eviction paths now update `size_state.json` directly under the eviction lock via `flush_and_apply_accumulator`. Write-path eviction (`evict_if_needed`) now re-reads size after lock acquisition and uses configurable trigger threshold.

## [1.1.40] - 2026-02-09

### Changed
- **Dashboard: Tooltip Descriptions on All Stats**: Every stat item in the cache statistics dashboard now shows a descriptive tooltip on hover, explaining what the metric means and how it's calculated.
- **Dashboard: Configurable Refresh Intervals**: JavaScript now reads `cache_stats_refresh_ms` and `logs_refresh_ms` from the `/api/system-info` endpoint, so YAML config values actually drive the dashboard refresh behavior instead of hardcoded 5s/10s.
- **Dashboard: RAM Cache Subtitle**: Changed from "Metadata and data" to "Object range data (GET responses)" to accurately reflect that the RAM cache stores GET response body data, not metadata.
- **Dashboard: Metadata Cache Card Title**: Changed from "Disk Cache: Object Metadata" to "Metadata Cache" with subtitle "HEAD request hit/miss tracking" — it's a RAM cache, not disk, and the hit/miss stats track HEAD requests specifically.
- **Dashboard: Total Disk Size Label**: Changed "Read Cache Size" to "Total Disk Size" — the underlying value (`size_state.total_size`) includes both read and write cache. Write cache size shown separately as a subset.
- **Dashboard: Write Cache Merged into Disk Cache Card**: Removed the separate Write Cache tile. Write cache size now displays inside the "Disk Cache: Object Ranges" card alongside total disk size.
- **Dashboard: Write Cache Description**: Updated from "PUT operations (multipart uploads)" to accurately describe that write cache holds MPUs in progress and PUT objects not yet read via GET.
- **Dashboard: Renamed WriteCacheStats.entries to evicted_uploads**: The API field was misleadingly named `entries` but actually contained `incomplete_uploads_evicted`. Renamed to `evicted_uploads` for accuracy.
- **Dashboard: Stale Refreshes Displayed**: Metadata cache stale refreshes now shown in the UI (previously API-only).
- **Dashboard: Overall Stats Labels**: "Total Requests" renamed to "Cache Requests (GET + HEAD)" with combined count. "Cache Effectiveness" renamed to "Cache Hit Rate" with clarifying tooltip that it reflects GET operations only.
- **Dashboard Documentation Rewrite**: Fixed concurrent connection limit (50, not 10), removed Docker section, documented `/api/logs` query parameters and `/api/system-info` response fields, added text search feature documentation.

## [1.1.39] - 2026-02-09

### Changed
- **Metadata Pass-Through in handle_range_request**: Load metadata once via `get_metadata_cached()` and pass through the call chain (`has_cached_ranges`, `find_cached_ranges`, `serve_range_from_cache`). NFS reads per cache hit reduced from ~5 to ~1.
- **Skip Full-Object Cache Check for Large Files**: When `content_length` exceeds `full_object_check_threshold` (default 64 MiB), skip the full-object cache check and proceed directly to range-specific lookup. Avoids scanning hundreds of cached ranges unnecessarily.
- **Connection Pool max_idle_per_host Default 1→10**: Keeps more idle TLS connections alive to S3, reducing handshake overhead during burst cache misses.
- **Consolidation Cycle Timeout**: Per-key processing phase in `run_consolidation_cycle()` enforces a configurable timeout (default 30s). On timeout, logs unprocessed key count and proceeds to delta collection and eviction. Unprocessed keys retry next cycle.
- **Streaming Range Data from Disk Cache**: Cached ranges at or above `disk_streaming_threshold` (default 1 MiB) are streamed in 512 KiB chunks instead of loaded fully into memory. LZ4-compressed ranges are decompressed first, then streamed. RAM cache hits continue to serve from memory.

## [1.1.38] - 2026-02-08

### Changed
- **Logging: Demoted 9 High-Volume INFO Sites to DEBUG**
  - Per-chunk "Range stored (hybrid)" in `disk_cache.rs`
  - Per-entry "SIZE_TRACK: Add COUNTED/SKIPPED" and "SIZE_TRACK: Remove COUNTED" in `calculate_size_delta()`
  - Per-key "Object metadata journal consolidation completed"
  - Per-entry "Removing journal entry for evicted range" and "Removing stale journal entry"
  - Per-call "Atomic size subtract", "Atomic size add", and "Atomic size add (non-blocking)"

- **Consolidation: KEY_CONCURRENCY_LIMIT Increased from 4 to 8**
  - Processes up to 8 cache keys concurrently via `buffer_unordered(8)`
  - Reduces wall-clock consolidation time when many keys have few entries each

- **Eviction: Batched Journal Writes by Cache Key**
  - `write_eviction_journal_entries()` groups entries by cache_key using a HashMap
  - New `append_range_entries_batch()` method writes all entries for a key in a single file operation
  - On batch failure for a key, logs warning and continues with remaining keys
  - Produces identical journal format to individual `append_range_entry()` calls

### Removed
- **Dead Code: Removed `calculate_size_delta()` and Related Tests**
  - Removed `calculate_size_delta()` function (superseded by accumulator-based size tracking in v1.1.33)
  - Removed `ConsolidationResult::success_with_size_delta()` constructor (never called)
  - Removed `create_journal_entry_with_size()` helper and 12 `test_calculate_size_delta_*` unit tests
  - Removed `prop_calculate_size_delta_correctness` property test
  - Cleaned up stale comments referencing `calculate_size_delta`

### Documentation
- **Updated `docs/CACHING.md`**: Eviction triggers section describes accumulator-based size tracking instead of journal-based approach
- **Updated `docs/ARCHITECTURE.md`**: Module organization table matches actual `src/` contents; accumulator-based size tracking section verified
- **Updated `docs/CONFIGURATION.md`**: Cache size tracking section describes accumulator-based approach with per-instance delta files

## [1.1.37] - 2026-02-08

### Changed
- **Consolidation: Parallel Cache Key Processing**
  - Consolidation cycle now processes up to 4 cache keys concurrently via `buffer_unordered(4)`
  - Reduces wall-clock consolidation time when individual keys hit NFS latency spikes
  - Per-key locks are independent flock-based locks — no contention between concurrent keys

- **Eviction: Re-read Size After Acquiring Global Lock**
  - `enforce_disk_cache_limits_internal()` re-reads `current_size` after acquiring the global eviction lock
  - Skips eviction if a previous instance's eviction already brought the cache under the limit
  - Prevents over-eviction caused by stale size snapshots taken before lock acquisition

- **Eviction: Immediate Accumulator Flush After Eviction**
  - Calls `size_accumulator.flush()` after eviction completes but before releasing the global eviction lock
  - Ensures the eviction subtract delta is written to a delta file promptly
  - Next consolidation cycle collects the delta and updates `size_state.json` before another instance can evict

- **Logging: Reduced SIZE_ACCUM Verbosity**
  - `SIZE_ACCUM add` and `SIZE_ACCUM subtract` log level changed from INFO to DEBUG
  - `SIZE_ACCUM flush`, `collect`, and `collect_total` remain at INFO
  - Reduces log volume by thousands of lines per download test

## [1.1.36] - 2026-02-08

### Fixed
- **Size Tracking: NFS Stale Read in Delta Collection**
  - Root cause of 120 MiB (8.6%) size tracking gap identified: NFS stale reads during cross-instance delta file collection
  - Changed from additive read-modify-write of a single per-instance delta file to append-only unique files per flush
  - Each `flush()` creates `delta_{instance}_{sequence}.json` — no read of existing file, eliminates stale read race
  - `collect_and_apply_deltas()` unchanged — already iterates all `delta_*.json` files and deletes after reading
  - Directory stays bounded: ~3 files per instance between collections (5s flush interval, 5s consolidation interval)

## [1.1.35] - 2026-02-08

### Changed
- **Eviction Performance: Decoupled Eviction from Consolidation Cycle**
  - Eviction now runs as a detached `tokio::spawn` task instead of blocking the consolidation cycle
  - `AtomicBool` guard (`eviction_in_progress`) prevents concurrent eviction spawns using `compare_exchange` with `SeqCst` ordering
  - `scopeguard` resets the guard on all exit paths (success, error, panic)
  - Consolidation cycle releases the global lock immediately, eliminating 100+ second lock holds during eviction

- **Eviction Performance: Parallel NFS File Deletes**
  - `batch_delete_ranges()` now uses `tokio::fs::remove_file` and `tokio::fs::metadata` (async) instead of `std::fs` (sync)
  - File deletes execute concurrently via `futures::stream::buffer_unordered` with a concurrency limit of 32
  - Object-level eviction processes up to 8 objects concurrently via `buffer_unordered`
  - Per-object metadata lock remains held for the entire batch delete operation

- **Eviction Performance: Early Exit Check**
  - Eviction loop stops processing objects once `total_bytes_freed >= bytes_to_free`
  - Avoids unnecessary file deletes when actual file sizes exceed `compressed_size` estimates

## [1.1.34] - 2026-02-08

### Fixed
- **Size Tracking: Delta File Race Condition**
  - Root cause: Consolidator reset delta files to zero after reading, but an instance could flush a new delta between the read and reset, causing the new value to be overwritten with zero (lost deltas)
  - Fix: Consolidator now DELETES delta files after reading instead of resetting to zero
  - Flush now uses additive writes: reads existing delta file, adds new delta, writes back. Handles missing file (deleted by consolidator) gracefully by starting from zero
  - `reset_all_delta_files()` (validation scan) now deletes files instead of resetting to zero
  - Removed dead `atomic_update_size_delta(0, 0)` call that was meant to increment consolidation_count but was skipped by early return

### Added
- **SIZE_ACCUM Logging**: INFO-level logging on every accumulator add, subtract, flush, and collect operation for production traceability
  - `SIZE_ACCUM add/subtract`: logs each individual size change with byte count and instance ID
  - `SIZE_ACCUM flush`: logs delta values being flushed to disk
  - `SIZE_ACCUM collect`: logs per-file delta values read by consolidator, plus total summary

## [1.1.33] - 2026-02-06

### Changed
- **Size Tracking: Replaced Journal-Based Tracking with In-Memory Accumulator**
  - Root cause: Journal-based size tracking suffered from timing gaps between when data is written and when size is counted, causing drift in multi-instance environments
  - Solution: In-memory `AtomicI64` accumulator tracks size at write/eviction time with zero NFS overhead
  - Size changes recorded immediately via `fetch_add`/`fetch_sub` operations
  - Each instance flushes accumulated delta to per-instance file (`size_tracking/delta_{instance_id}.json`) every consolidation cycle
  - Consolidator reads all delta files under global lock, sums into `size_state.json`, resets delta files
  - Journal entries continue to be processed for metadata updates only (no longer used for size tracking)
  - Daily validation scan corrects any drift and resets all delta files
  - Graceful shutdown flushes pending accumulator delta to disk

### Technical Details
- New `SizeAccumulator` struct in `journal_consolidator.rs` with `add()`, `subtract()`, `add_write_cache()`, `subtract_write_cache()`, `flush()`, `reset()` methods
- `store_range()` increments accumulator after successful HybridMetadataWriter write
- `perform_eviction_with_lock()` decrements accumulator using `compressed_size` from `RangeEvictionCandidate`
- `write_multipart_journal_entries()` increments accumulator for MPU completion ranges
- `run_consolidation_cycle()` flushes accumulator at cycle start, collects deltas under global lock
- `consolidate_object()` no longer calls `calculate_size_delta()` for size state updates
- `update_size_from_validation()` resets all delta files after correcting drift
- `shutdown()` flushes accumulator before final consolidation cycle

## [1.1.32] - 2026-02-05

### Fixed
- **Size Tracking: Global Consolidation Lock to Prevent Multi-Instance Race Conditions**
  - Root cause: Multiple instances could run consolidation cycles simultaneously, processing the same journal entries due to NFS caching delays in journal cleanup
  - Even with per-cache-key locking, instances would process the same cache_key sequentially (not simultaneously), causing duplicate size counting
  - Solution: Added global consolidation lock using flock-based file locking
  - Only one instance can run a consolidation cycle at a time across all instances
  - Lock file: `{cache_dir}/locks/global_consolidation.lock`
  - Uses non-blocking try_lock_exclusive() - if lock held, instance skips the cycle
  - Lock automatically released when cycle completes (via scopeguard RAII)
  - Added `GlobalConsolidationLock` struct for lock metadata (debugging)
  - Added `scopeguard` dependency for RAII-based lock release

## [1.1.31] - 2026-02-05

### Fixed
- **Size Tracking: Fix Over-Counting from Duplicate Journal Entries**
  - Root cause: `calculate_size_delta()` was counting ALL valid journal entries, but `apply_journal_entries()` skips Add entries where the range already exists in metadata
  - In multi-instance environments, the same range can have multiple journal entries (from retries or multiple instances), causing size to be counted multiple times
  - Solution: Only count size delta for entries that actually affect size:
    - Add entries that were actually applied (not skipped because range already in metadata)
    - All Remove entries (file was deleted)
  - Changed `apply_journal_entries()` to return `size_affecting_entries` instead of empty vector
  - Changed `consolidate_object()` to use `size_affecting_entries` for `calculate_size_delta()`

## [1.1.30] - 2026-02-05

### Fixed
- **Size Tracking: Fix Double Subtraction on Eviction**
  - Root cause: Eviction was subtracting bytes_freed twice:
    1. Directly via `atomic_subtract_size_with_retry()` after eviction
    2. Via Remove journal entries processed by consolidation
  - Solution: Removed direct subtraction; let consolidation handle all size updates via journal entries
  - This maintains single-writer pattern where consolidation is the only component updating size_state.json

## [1.1.29] - 2026-02-05

### Added
- **Size Tracking: Debug Logging for metadata_written Flag**
  - Added INFO-level logging to trace size tracking decisions
  - Logs each Add entry: COUNTED (metadata_written=false) or SKIPPED (metadata_written=true)
  - Logs each Remove entry with size
  - Summary log shows add_counted, add_skipped, remove_counted, total_delta
  - Purpose: Diagnose why v1.1.28 still shows ~7% under-reporting

## [1.1.28] - 2026-02-04

### Fixed
- **Size Tracking: metadata_written Flag for Accurate Tracking**
  - Root cause: v1.1.27 used metadata diff (size_after - size_before) but HybridMetadataWriter writes to .meta immediately, so ranges are already present when consolidation runs → delta = 0
  - Solution: Added `metadata_written: bool` field to JournalEntry
  - When HybridMetadataWriter succeeds (hybrid mode): `metadata_written: true` → consolidation skips size counting (already in .meta)
  - When falling back to journal-only: `metadata_written: false` → consolidation counts size
  - Remove operations always counted (range being deleted)
  - This correctly handles all scenarios without NFS lock overhead

## [1.1.27] - 2026-02-04

### Fixed
- **Size Tracking: Fixed Negative Size Delta Bug in v1.1.26**
  - Root cause: v1.1.26 calculated size_delta from journal entries, but Add entries are cleaned up after consolidation while Remove entries are created later during eviction
  - When eviction runs, Remove entries subtract size but the corresponding Add entries are already gone
  - Result: size_delta goes negative, total_size clamps to 0, cache appears empty
  - Fix: Calculate size_delta from metadata diff (size_after - size_before) instead of journal entries
  - This correctly handles:
    - Skipped Adds (range already in metadata from HybridMetadataWriter): delta = 0
    - Applied Adds (new range): delta = +size
    - Removes (range deleted): delta = -size
  - Metadata-based diff is the source of truth for actual changes, avoiding cross-cycle imbalances

## [1.1.26] - 2026-02-04

### Changed
- **Size Tracking: Reverted to Journal-Based Approach (v1.1.19-style)**
  - Removed direct size tracking from `store_range_data()` - eliminates per-write NFS lock attempts
  - Removed `size_tracked` field from `JournalEntry` - no longer needed
  - Size delta now calculated from journal entries with per-cycle deduplication
  - Deduplication uses HashSet by (start, end) to handle multiple instances caching same range
  - This restores download performance (removes ~30% throughput degradation from v1.1.23-v1.1.25)
  - May over-report size when multiple instances cache same range across consolidation cycles
  - Over-reporting is safe (eviction triggers early) vs under-reporting (disk fills)

## [1.1.25] - 2026-02-03

### Changed
- **Size Tracking Performance**: Replaced retry logic with non-blocking try-lock for direct size adds
  - v1.1.24 used 3 retries with exponential backoff (100-400ms delays) causing 30-90 second consolidation cycles
  - Now uses non-blocking `try_lock_exclusive()` - returns immediately if lock is busy
  - If lock busy, sets `size_tracked=false` and lets consolidation handle size tracking
  - Eliminates lock contention performance degradation in multi-instance deployments

## [1.1.24] - 2026-02-03

### Fixed
- **Size Tracking Double-Counting**: Fixed bug where v1.1.23's direct size tracking caused double-counting
  - Root cause: v1.1.23 added direct `atomic_add_size_with_retry()` in `store_range_data()`, but consolidation also adds size via `atomic_update_size_delta()` when processing journal entries
  - With `WriteMode::JournalOnly`, both paths add size = 2x actual size
  - Observed: Tracked 1.71 GiB, Actual 6 KB (empty after eviction). Eviction loops forever.
  - Fix: Added `size_tracked: bool` field to `JournalEntry` (defaults to false for backward compatibility)
  - When direct add succeeds, `size_tracked: true` is set on the journal entry
  - Consolidation skips size delta for entries with `size_tracked: true` to avoid double-counting

## [1.1.23] - 2026-02-03

### Fixed
- **Size Tracking - Add Path Not Updating Size**: Fixed bug where caching new ranges did not update size tracking
  - Root cause: HybridMetadataWriter writes ranges to `.meta` file immediately, then creates journal entry
  - When consolidation runs, range is already in metadata, so `size_before == size_after` → `size_delta = 0`
  - This caused tracked size to under-report by ~20-25% (e.g., 1.40 GiB tracked vs 1.75 GiB actual)
  - Fix: DiskCacheManager now calls `atomic_add_size_with_retry()` directly after storing a range
  - This mirrors how eviction works (direct subtract) - both add and subtract now bypass journal-based tracking
  - Added `atomic_add_size()` and `atomic_add_size_with_retry()` methods to JournalConsolidator
  - Added `journal_consolidator` field to DiskCacheManager for direct size updates

## [1.1.22] - 2026-02-03

### Fixed
- **Eviction Size Tracking - Missing Code Path**: Fixed bug where eviction triggered via `evict_if_needed()` did not update size tracking
  - Root cause: v1.1.21 added subtract code to `enforce_disk_cache_limits_internal()` but missed `evict_if_needed()`
  - `evict_if_needed()` is called from http_proxy.rs and range_handler.rs before caching new data
  - When eviction was triggered via this path, `perform_eviction_with_lock()` ran but size was never subtracted
  - Fix: Added same `atomic_subtract_size_with_retry()` call to `evict_if_needed()` after eviction completes

## [1.1.21] - 2026-02-02

### Fixed
- **Eviction Size Tracking**: Fixed bug where eviction did not reduce tracked size
  - Root cause: Eviction updates metadata directly (removes ranges from .meta file), then writes Remove journal entries
  - When consolidation processes Remove entries, ranges are already gone from metadata
  - Result: size_before = size_after = 0, so size_delta = 0 (no reduction tracked)
  - Fix: Directly call `atomic_subtract_size_with_retry(bytes_freed)` after eviction completes
  - This bypasses the journal-based approach for eviction since eviction already knows exact bytes freed
  - Consolidation still handles Add entries for size increases; eviction handles size decreases directly

## [1.1.20] - 2026-02-02

### Fixed
- **Size Tracking Accuracy - Metadata-Based Calculation**: Complete rewrite of size delta calculation to use metadata comparison instead of journal entries
  - Root cause: Journal-based size tracking was fundamentally flawed in multi-instance deployments
  - Multiple instances create journal entries for the same range (shared storage, same file path)
  - Previous fixes (v1.1.18, v1.1.19) tried to deduplicate journal entries but couldn't handle all edge cases
  - New approach: Calculate size_delta = (sum of compressed_size after) - (sum of compressed_size before)
  - This measures actual change in metadata, not journal entry counts
  - Eliminates all double-counting issues regardless of how many instances write journal entries
  - Simplified apply_journal_entries() by removing complex size tracking logic

## [1.1.19] - 2026-02-02

### Fixed
- **Size Tracking Double-Counting in Multi-Instance Deployments**: Fixed bug where the same range could be counted multiple times for size tracking
  - Root cause: When multiple instances cache the same range, each creates a journal entry. v1.1.18 fix counted ALL journal entries for size, even duplicates
  - Example: Instance A and B both cache range X → two journal entries → size counted twice
  - Fix: Track which ranges have been counted in each consolidation cycle using a HashSet
  - Only the first journal entry for each (start, end) range pair is counted for size delta
  - This applies to both Add and Remove operations to prevent over/under-counting
  - Fixes the ~350 MB over-reporting observed after v1.1.18 deployment

## [1.1.18] - 2026-02-02

### Fixed
- **Size Tracking Missed Ranges Written by Hybrid Mode**: Fixed bug where ranges written immediately by HybridMetadataWriter were not counted in size tracking
  - Root cause: In hybrid mode, metadata is written directly to `.meta` file, then a journal entry is created for redundancy
  - When consolidation ran, it found the range "already exists" in metadata and skipped adding it to `applied_entries`
  - Since size delta is calculated only from `applied_entries`, these ranges were never counted
  - Fix: Add journal entries to `applied_entries` for size tracking even when range already exists in metadata
  - The presence of a journal entry proves size hasn't been tracked yet (entries are removed after consolidation)
  - This fixes the ~120MB discrepancy observed after v1.1.17 deployment

## [1.1.17] - 2026-02-02

### Fixed
- **Full Object Caching Bypassed Journal System (Actual Fix)**: Fixed critical bug where `store_full_object_as_range_new()` wrote directly to disk without creating journal entries
  - Root cause: v1.1.15 CHANGELOG claimed this was fixed, but the actual code still bypassed the journal system entirely
  - Two issues fixed:
    1. `range_spec.file_path` used only filename instead of full relative path (e.g., `object_0-1023.bin` instead of `bucket/XX/YYY/object_0-1023.bin`)
    2. No journal entries were created after storing metadata, so consolidator never tracked the size
  - Impact: Full object GET responses cached via this path were never counted in size tracking
  - Fix: Now computes proper relative path and calls `write_multipart_journal_entries()` after storing metadata
  - This is the actual fix for the 273MB discrepancy observed after v1.1.16 deployment (which only fixed multipart uploads)

## [1.1.16] - 2026-02-02

### Fixed
- **Multipart Upload Completion Bypassed Journal System**: Fixed critical bug where CompleteMultipartUpload wrote metadata directly without creating journal entries
  - Root cause: `finalize_multipart_upload()` in `signed_put_handler.rs` wrote metadata and range files directly, bypassing the journal system
  - Impact: Multipart uploads were never counted in size tracking, causing size_state.json to under-report
  - Observed: 273MB discrepancy between actual disk usage (1.46GB) and tracked size (1.28GB)
  - Fix: Added `write_multipart_journal_entries()` method to JournalConsolidator, called after CompleteMultipartUpload creates metadata
  - This ensures all multipart upload ranges are tracked via journal entries for consolidation to process

## [1.1.15] - 2026-02-02

### Fixed
- **Full Object Caching Bypassed Journal System**: Fixed critical bug where `store_full_object_as_range_new()` wrote directly to disk without creating journal entries
  - Root cause: This method wrote range files and metadata directly, bypassing `DiskCacheManager::store_range()` which creates journal entries for size tracking
  - Impact: Full object GET responses cached via this path were never counted in size tracking, causing size_state.json to under-report by hundreds of MB
  - Observed: du showed 2.0GB actual disk usage, size_state.json showed 1.39GB tracked (~711MB under-counted)
  - Fix: Now uses `DiskCacheManager::store_range()` which properly writes journal entries for consolidation to process
  - Affected code paths: GET response caching, PUT body caching, write cache entry storage

## [1.1.14] - 2026-01-31

### Fixed
- **Remove Journal Entries Not Processed**: Fixed bug where Remove journal entries from eviction were not being processed for size tracking
  - Root cause 1: `validate_journal_entries_with_staleness()` checked if range file exists, but Remove entries have intentionally deleted files
  - Root cause 2: `apply_journal_entries()` only added Remove entries to `applied_entries` if the range was found in metadata
  - Fix: Remove operations now bypass file existence check (file is intentionally deleted) and always count for size tracking
  - This caused size state to show 2.2GB tracked when disk was actually 861MB after eviction

## [1.1.13] - 2026-01-31

### Changed
- **Journal-Based Size Tracking for Eviction**: Eviction now writes Remove journal entries instead of directly updating size state
  - Previous approach: Eviction called `atomic_subtract_size_with_retry()` which required locking and could race with consolidation
  - New approach: Eviction writes Remove entries to journal, consolidation processes them and updates size state
  - Benefits: Single writer to size_state.json (consolidation only), eliminates race conditions, no lock contention
  - Added `write_eviction_journal_entries()` method to JournalConsolidator
  - Consolidation already handles Remove operations via `calculate_size_delta()`

## [1.1.12] - 2026-01-31

### Fixed
- **Consolidation vs Eviction Race Condition**: Fixed race condition where consolidation's size state update could overwrite eviction's update
  - Root cause: Consolidation did a non-atomic read-modify-write without holding the `size_state.lock`
  - Sequence: Consolidation reads 2GB → Eviction subtracts 500MB (writes 1.5GB) → Consolidation adds +10MB to stale 2GB → Consolidation writes 2.01GB, overwriting eviction's 1.5GB
  - This caused size state to show 1.6GB tracked when disk was actually empty (all data evicted)
  - Fix: Added `atomic_update_size_delta()` method that uses the same file locking as `atomic_subtract_size()`
  - Consolidation now uses this atomic method, ensuring sequential consistency with eviction

## [1.1.11] - 2026-01-31

### Fixed
- **Size State Race Condition During Eviction**: Fixed race condition where size state was updated AFTER releasing the eviction lock
  - Previous behavior: Release eviction lock → Update size state
  - This allowed another instance to acquire the lock and read stale size state before the first instance updated it
  - New behavior: Update size state → Release eviction lock
  - This ensures sequential consistency - each eviction sees the result of the previous one

## [1.1.10] - 2026-01-30

### Fixed
- **Critical: Size Tracking Discrepancy (47MB actual vs 1.5GB tracked)**: Fixed bug in journal consolidation that caused massive size tracking inflation
  - Root cause: `validate_journal_entries_with_staleness()` was adding entries with missing range files but recent timestamps to `valid_entries`
  - These entries were then processed by `apply_journal_entries()`, which calculated size delta from them
  - But the range files didn't exist on disk (e.g., due to NFS caching delays), so size was counted for non-existent data
  - Fix: Entries with missing range files but recent timestamps are now kept in journal for retry (not added to `valid_entries`)
  - Only entries with existing range files are processed for size delta
  - Stale entries (missing files + old timestamps) are still removed from journal

## [1.1.9] - 2026-01-28

### Fixed
- **Consolidation Loop Deadlock During Idle Eviction**: Fixed deadlock where the consolidation loop would hang when triggering eviction during idle periods
  - Root cause: `enforce_disk_cache_limits()` was calling `consolidate_object()` for pre-eviction journal consolidation, but this was being called from within the consolidation loop itself
  - When called from the consolidation loop, we just finished consolidating, so pre-eviction consolidation is redundant and can cause blocking
  - Added `enforce_disk_cache_limits_skip_consolidation()` variant that skips pre-eviction consolidation
  - `maybe_trigger_eviction()` now uses this variant to avoid the deadlock
  - Other callers (maintenance operations) still do pre-eviction consolidation for accurate access times

## [1.1.8] - 2026-01-28

### Fixed
- **Eviction Not Triggering During Idle Periods**: Fixed bug where eviction would not trigger when cache was over capacity but no new data was being added
  - Previously, eviction was only checked when `size_delta > 0` (cache grew), meaning idle periods with over-capacity cache would never trigger eviction
  - Now eviction is checked at the end of EVERY consolidation cycle (every 5 seconds), regardless of whether there was activity
  - Modified `maybe_trigger_eviction()` to accept an optional `known_size` parameter to avoid redundant NFS reads
  - This ensures cache stays within capacity limits even during read-only workloads or idle periods

## [1.1.7] - 2026-01-28

### Fixed
- **Critical: Lost Updates in Size State**: Fixed race condition where concurrent evictions from multiple instances caused lost updates to size state
  - Previous fix (v1.1.4) did read-modify-write without locking, causing multiple instances to read the same value, subtract their bytes_freed, and overwrite each other
  - Added `atomic_subtract_size()` function that uses file locking (`size_state.lock`) to ensure atomic read-modify-write
  - This prevents size inflation when multiple instances evict concurrently

## [1.1.6] - 2026-01-28

### Fixed
- **Critical: TOCTOU Race in Eviction Lock**: Fixed race condition where multiple threads could acquire the eviction lock simultaneously
  - v1.1.5 fix had a TOCTOU (time-of-check-time-of-use) bug: threads checked `is_some()` then released the mutex before setting `Some`
  - Now the entire check-and-set operation is atomic within a single mutex guard scope
  - Restructured to drop the mutex guard before async metrics recording to satisfy Rust's `Send` requirements

## [1.1.5] - 2026-01-28

### Fixed
- **Critical: Concurrent Eviction Race Condition**: Fixed bug where multiple threads within the same instance could all acquire the eviction lock simultaneously
  - The `flock`-based lock was per-file-descriptor, not per-process - each thread opened a new file descriptor and got its own lock
  - This caused multiple concurrent evictions to run, each reading stale size state and writing back incorrect values
  - Fix: Added check at start of `try_acquire_global_eviction_lock()` to return `false` if `eviction_lock_file` is already `Some`
  - This ensures only one thread per instance can hold the eviction lock at a time

## [1.1.4] - 2026-01-28

### Fixed
- **Critical: Eviction Not Updating Size State**: Fixed bug where eviction triggered from `monitor_and_enforce_cache_limits()` did not update the size state
  - Two code paths could trigger eviction: (1) `JournalConsolidator::maybe_trigger_eviction()` and (2) `CacheManager::monitor_and_enforce_cache_limits()`
  - Only path (1) was updating the size state after eviction, causing size inflation when eviction happened via path (2)
  - Observed behavior: After heavy downloads filled the cache, two back-to-back evictions occurred but only the first one's `bytes_freed` was subtracted from size state
  - Fix: Moved size state update into `enforce_disk_cache_limits()` so ALL eviction paths update the size state
  - Removed duplicate size state update from `maybe_trigger_eviction()` to prevent double-counting

## [1.1.3] - 2026-01-28

### Fixed
- **Critical: Size Tracking Double-Counting Bug**: Fixed bug where cache size was inflated because journal entries that were already present in metadata were still counted in size delta
  - Previously, `calculate_size_delta()` was called on ALL valid journal entries, including entries that were already consolidated in a previous cycle
  - Now size delta is only calculated from entries that were actually applied (new entries not already in metadata)
  - This caused `size_state.json` to show sizes much higher than actual disk usage (e.g., 2.5GB tracked vs 565MB actual)
  - Root cause: Journal entries remain in journal files until cleanup, and were being re-counted on each consolidation cycle

## [1.1.2] - 2026-01-28

### Fixed
- **Multi-Instance Size Consistency (Complete Fix)**: Removed in-memory size state entirely - disk is now the single source of truth
  - Previously, each instance maintained its own in-memory `size_state` and could overwrite the shared disk file with stale values during consolidation
  - This caused size drops of a few hundred MB when one instance's stale in-memory state overwrote another instance's recent updates
  - Now all size operations (`get_current_size()`, `get_write_cache_size()`, `get_size_state()`) read directly from the shared `size_state.json` file
  - Eliminates race conditions where instances could see different sizes or overwrite each other's updates

### Changed
- **`get_current_size()` and `get_write_cache_size()` are now async**: These methods now read from disk instead of in-memory state
  - Callers must use `.await` when calling these methods
  - This ensures all instances see the same size values from the shared disk file

## [1.1.1] - 2026-01-27

### Fixed
- **Multi-Instance Size Consistency**: Dashboard and metrics now read cache size from the shared `size_state.json` file instead of in-memory state
  - All instances now show the same cache size value
  - `get_size_state()` reads from disk for multi-instance consistency
  - `get_current_size()` remains in-memory for hot paths (eviction checks)

- **Dashboard Timestamp Display**: Fixed "Invalid Date" display for Last Consolidation timestamp
  - JavaScript now correctly handles Rust's SystemTime serialization format (`secs_since_epoch`)

## [1.1.0] - 2026-01-27

### Changed
- **Journal-Based Size Tracking**: Size tracking is now handled by the JournalConsolidator instead of a separate delta buffer system
  - Size deltas are calculated from Add/Remove operations in journal entries during consolidation
  - Size state is persisted to `size_tracking/size_state.json` after each consolidation cycle (every 5s)
  - Eviction is triggered automatically by the consolidator when cache exceeds capacity
  - Consolidation interval changed from 30s to 5s for near-realtime size tracking
  - Use `shared_storage.consolidation_interval` to control frequency (default: 5s)

- **Removed `shared_storage.enabled` Config Option**: Journal-based metadata writes and distributed eviction locking are now always enabled
  - The `shared_storage.enabled` config option has been removed
  - All deployments (single-instance and multi-instance) use the same code path
  - This simplifies the codebase and ensures consistent behavior

- **Consolidation Loop Timing**: Changed from burst catch-up to delay behavior when consolidation takes longer than the interval
  - Prevents rapid back-to-back consolidation cycles after long evictions

### Removed
- **Deprecated Size Tracking Config**: Removed `size_tracking_flush_interval` and `size_tracking_buffer_size` config options
  - These were part of the buffered delta system which has been replaced by journal-based tracking

- **Dead Code Cleanup**: Removed ~200 lines of unused eviction lock methods (`write_global_eviction_lock`, `read_global_eviction_lock`)
  - These were superseded by flock-based locking via `try_acquire_global_eviction_lock()`

### Migration Notes

**Breaking Change - Cache Directory Migration Required**

This release changes the size tracking architecture. A fresh cache directory is recommended:

1. Stop all proxy instances
2. Clear the cache directory: `rm -rf /path/to/cache/*`
3. Update configuration:
   - Remove `shared_storage.enabled` if present (no longer supported)
   - Remove `size_tracking_flush_interval` if present (no longer supported)
   - Remove `size_tracking_buffer_size` if present (no longer supported)
4. Deploy new version
5. Start proxy instances

**Old files that will be automatically cleaned up:**
- `size_tracking/checkpoint.json` - replaced by `size_state.json`
- `size_tracking/delta-*.log` - no longer used

**New files created:**
- `size_tracking/size_state.json` - contains total_size, write_cache_size, last_consolidation timestamp

## [1.0.14] - 2026-01-25

### Fixed
- **Dashboard Object Metadata Hit Rate**: Fixed hit rate calculation to use HEAD request hits/misses instead of RAM metadata cache hits/misses. Now accurately reflects S3 HEAD request cache performance.

## [1.0.13] - 2026-01-25

### Changed
- **Removed NFS Propagation Delays**: Removed 50ms and 500ms delays in checkpoint sync that were workarounds for NFS visibility issues. With `lookupcache=pos` mount option, `sync_all()` is sufficient for cross-instance file visibility. Reduces checkpoint sync latency by ~550ms.

### Documentation
- **NFS Mount Requirements**: Added critical documentation for multi-instance deployments requiring `lookupcache=pos` mount option on NFS volumes. This caches positive lookups (file exists) but not negative lookups (file not found), ensuring new files from other instances are immediately visible while maintaining good cache hit performance. Added to CONFIGURATION.md and GETTING_STARTED.md.
- **Archived Investigation**: Moved INVESTIGATION-JOURNAL-CONSOLIDATION-BUG.md to archived/docs/ after successful resolution of all 5 journal consolidation bugs.

## [1.0.12] - 2026-01-25

### Changed
- **Reduced Log Noise**: Downgraded benign race condition logs from WARN/ERROR to INFO:
  - "Failed to read journal file for cleanup" - Expected when another instance already deleted the file
  - "Failed to break stale lock" - Expected when lock file was already removed by another instance
  - These race conditions are harmless on shared NFS storage and don't indicate real problems

## [1.0.11] - 2026-01-25

### Fixed
- **S3 Request Retry on Transient Failures**: Added retry logic (up to 2 retries with backoff) for S3 range requests that fail with connection errors like `SendRequest`. Previously, a single transient failure would return `BadGateway` to the client. Now the proxy retries before giving up.

### Changed
- **Stale Journal Lock File Cleanup**: Journal consolidation now cleans up orphaned `.journal.lock` files that remain after fresh journal files are deleted. These lock files accumulated during high-concurrency downloads but are now automatically removed.

## [1.0.10] - 2026-01-25

### Fixed
- **Critical: Cleanup vs Append Race Condition (Bug 5)**: Fixed race condition where journal cleanup could overwrite entries being appended concurrently. The v1.0.9 mutex only protected appends from each other, not from cleanup operations. Now uses file-level locking (`flock`) with a "fresh journal on lock contention" strategy:
  - Append tries non-blocking lock on primary journal file
  - If lock is busy (cleanup in progress), creates a fresh journal file with timestamp suffix
  - Cleanup acquires exclusive lock before read-modify-write
  - Appends never block - cache writes stay fast during consolidation
  - Fresh journal files are automatically discovered by consolidator and deleted when empty
  - Expected to reduce orphaned ranges from ~0.8% to 0%

## [1.0.9] - 2026-01-25

### Fixed
- **Critical: Journal Append Race Condition (Bug 4)**: Fixed thread-safety issue in `append_range_entry()` where concurrent appends within the same instance could overwrite each other. When multiple threads read the journal file simultaneously, appended their entries, and wrote back, the last writer would overwrite entries from other threads. Added `tokio::sync::Mutex` to serialize journal appends within each instance.
  - Evidence: Orphaned range `5GB-1:1216348160-1224736767` was stored at 13:41:29.912 with "Range stored (hybrid)" logged, but no journal entry existed - it was overwritten by a concurrent append within milliseconds.
  - Expected to reduce orphaned ranges from ~0.5% to 0% in high-concurrency scenarios.

## [1.0.8] - 2026-01-25

### Fixed
- **Critical: Non-Atomic Metadata Write in Journal Consolidator**: Fixed race condition where consolidation could read empty/corrupted metadata files. The `write_metadata_to_disk()` function used `tokio::fs::write()` which is NOT atomic on NFS - readers could see empty or partial files during the write. Now uses atomic write pattern (temp file + rename) like `hybrid_metadata_writer.rs`, ensuring readers always see complete, valid JSON.
  - Error symptom: "Failed to parse metadata file: EOF while parsing a value at line 1 column 0"
  - Reduced orphaned ranges from 1.2% to expected 0%

## [1.0.7] - 2026-01-25

### Fixed
- **Critical: Multi-Instance Consolidation Race Condition**: Fixed race condition where multiple proxy instances consolidating the same cache key simultaneously caused entries to be lost. The lock was acquired AFTER reading journal entries, allowing all instances to read the same entries before any acquired the lock. Now the lock is acquired BEFORE reading entries, ensuring only one instance processes each cache key at a time.
  - Reduced orphaned ranges from 0.7% to 0% in multi-instance deployments
  - Instances that can't acquire the lock skip the cache key (another instance is handling it)

## [1.0.6] - 2026-01-25

### Fixed
- **Critical: Journal Consolidation Losing Ranges**: Fixed bug where 12% of cached ranges were "orphaned" (range files existed but not tracked in metadata). The `cleanup_instance_journals()` function was truncating ALL journal files after consolidation, but `validate_journal_entries()` filtered out entries where range files weren't yet visible due to NFS attribute caching. This caused journal entries to be permanently lost before they could be consolidated.
  - Added `consolidated_entries` field to `ConsolidationResult` to track which entries were actually processed
  - New `cleanup_consolidated_entries()` method removes only specific entries that were successfully consolidated
  - Entries with missing range files (due to NFS caching delays) are now preserved and retried on the next consolidation cycle
  - Deprecated `cleanup_instance_journals()` which truncated everything unconditionally
  - Cache hit rate improved from ~88% to ~99% on repeat downloads

## [1.0.5] - 2026-01-24

### Fixed
- **Critical: NFS Directory Entry Caching Bug**: Removed `.exists()` checks before reading metadata files. The `.exists()` calls caused NFS to cache directory entries, making newly created files invisible to other instances even after journal consolidation. This caused 40%+ cache miss rate on repeat downloads. Now reads files directly, avoiding directory lookups entirely.

## [1.0.4] - 2026-01-24

### Added
- **Delta File Archiving**: Delta files are now archived with timestamps before truncation during checkpoint consolidation. Archives are kept for the last 20 checkpoints per instance to aid troubleshooting of size tracking discrepancies.
- **Range Storage Logging**: Added INFO-level logging for successful range storage operations to diagnose cache write failures.

### Changed
- **Dashboard Cleanup**: Removed "Stale Refreshes" counter (internal metric not useful to users).

## [1.0.3] - 2026-01-24

### Fixed
- **Dashboard Statistics Accuracy**: Separated HEAD and GET hit/miss counters. "Object Metadata" now shows HEAD request statistics only, "Object Ranges" shows GET request statistics only. Previously both sections showed combined stats, making it appear that GET requests were missing cache when only HEAD metadata was missing.

## [1.0.2] - 2026-01-24

### Fixed
- **Distributed Lock Reliability**: Replaced file rename-based locking with `flock()` for both eviction and checkpoint locks. The rename approach failed on NFS due to attribute caching, causing 75+ lock errors per minute and allowing multiple instances to evict simultaneously. `flock()` provides reliable distributed locking on NFS4 without consistency issues.

### Changed
- **Lock Mechanism**: Eviction and checkpoint locks now use persistent files with `flock()` instead of temp file + rename atomicity.
- **No Delays Needed**: Removed NFS propagation delays (50ms, 100ms, 200ms, 500ms) since `flock()` is atomic and works immediately.

## [1.0.1] - 2026-01-24

### Fixed
- **Eviction Not Triggered During Read-Only Workloads**: Fixed cache staying over capacity (230%) indefinitely when no new writes occur. Checkpoint sync now triggers eviction check every 30 seconds if cache exceeds limit, ensuring capacity is enforced even during read-only workloads.
- **Eviction Lock NFS Propagation**: Added 100ms delay after sync_all() before rename to account for NFS propagation time, reducing lock acquisition failures.

### Changed
- **Reduced Log Noise**: Changed delta buffer flush and cache size recovery logs from INFO to DEBUG level.
- **Error Severity**: Reduced eviction lock rename failures from ERROR to WARN since they're automatically retried and don't affect functionality.
- **Terminology**: Changed EFS-specific references to NFS (applies to all network filesystems, not just EFS).

## [1.0.0] - 2026-01-23

### Major Release
First stable 1.0.0 release with production-ready multi-instance cache coordination and comprehensive bug fixes.

### Fixed
- **Write Cache Critical Bug**: Fixed PUT operations storing incorrect file paths (filename only instead of full sharded path), causing "Failed to slice cached range data" errors on GET requests after PUT.
- **Cross-Instance Size Tracking**: Implemented near-realtime multi-instance cache size synchronization with 30-second checkpoint consolidation, randomized coordination, and NFS consistency handling.
- **NFS Consistency**: Added `flush()` and `sync_all()` to all critical file operations (checkpoint, delta, eviction lock) to ensure data visibility across instances on network filesystems.
- **Eviction Lock Failures**: Fixed "No such file or directory" errors during eviction lock acquisition by ensuring temp files are synced before rename.
- **Dashboard Log Parser**: Fixed log viewer to handle tracing's inconsistent spacing (single space for ERROR, double space for INFO/WARN/DEBUG).

### Changed
- **Checkpoint Interval**: Reduced from 5 minutes to 30 seconds for better cross-instance accuracy.
- **Checkpoint Coordination**: Added randomized delay (0-5s) and lock-based coordination so only one instance consolidates per interval.
- **Logging Format**: Added `.compact()` format to tracing configuration.
- **Reduced Log Noise**: Checkpoint operations use DEBUG level, sync only logs significant changes (>10 MB).

### Removed
- **Dead Code**: Removed unused `acquire_global_eviction_lock()` and `is_eviction_lock_stale()` methods.

## [0.10.1] - 2026-01-23

### Fixed
- **EFS Consistency for Checkpoint Writes**: Added `flush()` and `sync_all()` to checkpoint file writes to ensure data is fully committed to EFS before rename, preventing other instances from reading stale checkpoint data.
- **EFS Consistency for Delta Files**: Added `sync_all()` to delta file writes to ensure data is committed before checkpoint consolidation reads and truncates the files.
- **Cross-Instance Delta Timing**: Added 5-second wait after acquiring checkpoint lock to ensure all instances have flushed their deltas before consolidation reads them, accounting for random delay spread (0-5 seconds).
- **EFS Propagation Delay**: Increased checkpoint read delay from 100ms to 500ms to account for EFS eventual consistency when other instances update the checkpoint file.

### Changed
- **Checkpoint Interval**: Reduced from 60 seconds to 30 seconds for better cross-instance size accuracy with acceptable EFS I/O overhead.
- **Reduced Log Noise**: Changed checkpoint lock acquisition/skip from INFO to DEBUG level, and only log checkpoint sync when size changes by >10 MB.

### Removed
- **Dead Code**: Removed unused `acquire_global_eviction_lock()` method that was replaced by `try_acquire_global_eviction_lock()`.

## [0.10.0] - 2026-01-23

### Fixed
- **Write Cache Range Storage Bug**: Fixed critical bug where PUT operations stored only the filename instead of the full sharded relative path in RangeSpec, causing "Failed to slice cached range data" errors on subsequent GET requests. Now correctly stores paths like `bucket/XX/YYY/object_0-1023.bin`.
- **Cross-Instance Size Tracking**: Implemented near-realtime cross-node cache size synchronization. Checkpoint process now consolidates deltas from ALL instances every minute (down from 5 minutes), providing accurate size tracking across the cluster without filesystem scanning.
- **Checkpoint Coordination**: Added randomized delay (0-5 seconds) and lock-based coordination to ensure only ONE instance writes the consolidated checkpoint per minute, preventing wasted work and race conditions. All instances re-read the checkpoint to stay synchronized.
- **EFS Consistency for Delta Files**: Fixed critical race condition where delta files were being truncated before data was visible on EFS. Added `sync_all()` after delta flush to ensure data is committed to disk before checkpoint consolidation reads and truncates the files.
- **Eviction Lock EFS Consistency**: Fixed eviction lock failures on EFS/NFS by adding `sync_all()` before rename to ensure temp file is flushed to disk before atomic rename operation.
- **Dashboard Log Parser**: Fixed dashboard log viewer to handle tracing's inconsistent spacing (single space for ERROR, double space for INFO/WARN/DEBUG) by using `trim_start()` after timestamp extraction.

### Changed
- **Checkpoint Interval**: Reduced default checkpoint interval from 5 minutes to 1 minute for near-realtime cross-instance size accuracy.
- **Logging Format**: Added `.compact()` format to tracing configuration for more consistent log formatting.

## [0.9.23] - 2026-01-23

### Fixed
- **Cache Size Limit vs Current Size Confusion**: Fixed multiple places in the code that were using `total_cache_size` (current usage) when they should have been using `max_cache_size_limit` (configured limit). This affected:
  - Post-initialization eviction check
  - Write cache capacity calculation
  - `evict_if_needed()` threshold calculation
  - `enforce_disk_cache_limits()` check
  - `get_maintenance_recommendations()` utilization calculation
  - Write cache max size recalculation
- **Startup Over-Capacity Message**: Changed "eviction needed" to "eviction will take place the next time data is cached" for clarity.

## [0.9.22] - 2026-01-22

### Fixed
- **Dashboard Disk Cache Size Display**: Fixed dashboard showing size/size instead of size/limit. Added `max_cache_size_limit` field to `CacheStatistics` to track the configured limit separately from `total_cache_size` (current usage).

## [0.9.21] - 2026-01-22

### Fixed
- **Scalable Cache Size Tracking**: Replaced filesystem walks with size tracker for all cache size checks. Previously, `evict_if_needed()`, `get_cache_size_stats()`, `enforce_disk_cache_limits()`, and `get_maintenance_recommendations()` all walked the filesystem to calculate cache size, which doesn't scale to billions of files. Now all these functions use the incremental size tracker (updated on every cache write/delete, corrected daily by validation scan).
- **Eviction Now Triggers Correctly**: Fixed eviction not triggering because it was using stale checkpoint data. The size tracker's in-memory `current_size` is now used directly, which is updated in real-time as ranges are stored.
- **MetricsManager Cache Size Tracker**: Wired size tracker to MetricsManager so `cache_size` metrics are populated.
- **Dashboard Field Fix**: Fixed reference to non-existent `max_cache_size_limit` field in dashboard (now uses `total_cache_size`).
- **Missing HybridMetadataWriter Getter**: Added `get_hybrid_metadata_writer()` method to CacheManager for background orphan recovery.

## [0.9.18] - 2026-01-22

### Fixed
- **Cache Size Tracking for Range Storage**: Fixed critical bug where cache size was not being tracked when storing range data through `CacheManager` methods (`store_full_object_as_range_new`, `store_write_cache_entry`, `complete_multipart_upload`). The size tracker was only being updated in `DiskCacheManager.store_range()` but not in the `CacheManager` code paths. This caused the size tracker to show incorrect values (e.g., 702MB when actual disk usage was 1.5GB), preventing eviction from triggering.
- **Size Tracker Wiring Logging**: Added INFO-level logging when size tracker is wired up to disk cache manager, and WARN-level logging when size tracker is not available for range storage operations.

## [0.9.17] - 2026-01-22

### Fixed
- **Eviction Lock Logging**: Added INFO-level logging for eviction lock operations to diagnose lock acquisition issues. Logs now show when locks are acquired, when existing locks are found (with elapsed time and timeout), and when stale locks are forcibly acquired.

### Changed
- **Dashboard Uptime Auto-Refresh**: System info (including uptime) now refreshes automatically every 5 seconds along with other dashboard metrics.

## [0.9.16] - 2026-01-22

### Fixed
- **Distributed Eviction Over-Eviction**: In shared storage mode, after acquiring the eviction lock, proxies now re-check cache size using the size tracker before proceeding. This prevents over-eviction when multiple proxies detect over-capacity simultaneously - the second proxy will see the cache is already under target and skip eviction.
- **Dashboard Log Text Filter**: Text filter now searches server-side across all log entries, not just the already-displayed entries. Previously, filtering for "eviction" with level=All would only search the 100 most recent INFO entries; now it searches all entries matching the criteria.

## [0.9.15] - 2026-01-22

### Fixed
- **Cache Eviction Bug**: Fixed critical bug where cache eviction never triggered because `total_cache_size` was used for both the configured limit and current usage. Added separate `max_cache_size_limit` field to store the configured limit, ensuring eviction triggers correctly when cache exceeds capacity.
- **RAM Cache Excluded from Disk Total**: `total_cache_size` now only includes disk cache (read + write), not RAM cache, since RAM is separate and doesn't count against disk limit.
- **Range Spec Journal Fallback**: In shared storage mode, `load_range_data_from_new_storage` now checks journals as fallback when range not found in metadata file, fixing "Range spec not found" warnings caused by race conditions.

### Changed
- **Dashboard Size Display**: Dashboard now shows cache size with limit (e.g., "1.5 GiB / 1.2 GiB") for both disk cache and RAM cache.

## [0.9.14] - 2026-01-21

### Fixed
- **Coordinator Max Cache Size**: Cache initialization coordinator now uses actual configured `max_cache_size` instead of hardcoded 1GB, fixing misleading "Cache over capacity" warnings

## [0.9.13] - 2026-01-21

### Changed
- **Non-Destructive Metadata Error Handling**: Metadata files are no longer deleted when read/parse errors occur
  - JSON parse failures now retry up to 3 times with 50ms delays (handles partial reads during in-progress writes)
  - Empty file and I/O errors treated as cache miss without deletion
  - Prevents race condition where multiple proxies delete valid in-progress metadata writes
  - Orphan recovery system handles truly corrupt files over time

## [0.9.12] - 2026-01-21

### Fixed
- **Range File Rename Before Journal Write**: In shared storage mode, range files are now renamed to their final path BEFORE writing the journal entry. This eliminates the race condition where another proxy's consolidator could read a journal entry referencing a file that doesn't exist yet. If journal write fails after rename, the orphan recovery system will clean up the range file.

## [0.9.11] - 2026-01-21

### Fixed
- **Additional Journal Parse Warnings Downgraded**: All "Failed to parse journal entry" warnings in journal_manager.rs now debug level (4 additional locations)

## [0.9.10] - 2026-01-20

### Fixed
- **Downgraded Journal Warnings to Debug**: "Failed to read journal file" (stale file handle) and "Journal entry references non-existent range file" warnings are now debug level since they're expected during concurrent writes on shared storage

## [0.9.9] - 2026-01-20

### Fixed
- **Range Response Metadata Retry**: Added retry logic (5 attempts with increasing delays: 20-80ms) when retrieving cached metadata for range responses, reducing "Could not retrieve cached metadata" warnings during concurrent writes

## [0.9.8] - 2026-01-20

### Fixed
- **Metadata Read Retry with Delay**: `get_metadata_from_disk()` now retries up to 3 times with 10ms delay for transient errors (empty file, parse errors, I/O errors) before falling back to journal lookup

## [0.9.7] - 2026-01-20

### Fixed
- **Journal Fallback for Corrupted Metadata Files**: `get_metadata_from_disk()` now tries journal lookup when `.meta` file exists but fails to parse (EOF error, empty file, or corruption during concurrent writes)

## [0.9.6] - 2026-01-20

### Fixed
- **Journal Metadata Lookup for Range Responses**: `get_metadata_from_disk()` now checks pending journal entries when `.meta` file doesn't exist, eliminating "Could not retrieve cached metadata for range response" warnings during journal consolidation window

## [0.9.5] - 2026-01-20

### Added
- **Emergency Eviction on ENOSPC**: When disk write fails with "No space left on device", triggers cache eviction (80% target) and retries once before giving up
- **Post-Eviction Capacity Check**: After eviction completes, verifies sufficient space exists before allowing new cache writes
- **Hard Capacity Check**: Blocks new cache writes when disk usage exceeds configured max capacity

### Fixed
- **Journal Metadata Propagation**: Journal entries now include `object_metadata` field, ensuring response headers are preserved when metadata files are created by journal consolidation
- **Range Response Content-Length**: Fixed incorrect content_length override that caused "Start position exceeds content length" warnings
- **Journal Lookup Race Condition**: Added fallback journal lookup in `find_cached_ranges()` to check pending journal entries during the consolidation window

### Changed
- **Orphaned Range Recovery**: Integrated with BackgroundRecoverySystem for scalable sharded scanning of orphaned .bin files

## [0.9.2] - 2026-01-20

### Added
- **Buffered Access Logging**: Access logs are now buffered in RAM and flushed periodically
  - Reduces disk I/O on shared storage (EFS/NFS) by batching writes
  - Configurable flush interval (`access_log_flush_interval`, default: 5s)
  - Configurable buffer size (`access_log_buffer_size`, default: 1000 entries)
  - Force flush on graceful shutdown to minimize data loss
  - Maintains existing S3-compatible log format and date-partitioned directory structure

- **Buffered Size Delta Tracking**: Cache size deltas are now buffered and written to per-instance files
  - Eliminates lock contention between proxy instances on shared storage
  - Each instance writes to its own delta file (`size_tracking/delta-{instance_id}.log`)
  - Configurable flush interval (`size_tracking_flush_interval`, default: 5s)
  - Configurable buffer size (`size_tracking_buffer_size`, default: 10000 deltas)
  - Recovery reads all instance delta files and sums with checkpoint
  - Stale delta files from crashed instances cleaned up based on age

### Changed
- **Removed `AccessLogWriter`**: Replaced with `AccessLogBuffer` for buffered writes
- **Removed synchronous delta methods**: `try_append_delta()` and `try_append_write_cache_delta()` replaced with buffered `SizeDeltaBuffer`

### Performance
- **Shared Storage Optimization**: Significantly reduced disk I/O for EFS/NFS deployments
  - Access logs: Up to 99% reduction in write operations (1000 entries per flush vs per-request)
  - Size tracking: Eliminated per-operation disk writes and lock contention
  - Improved throughput for high-traffic multi-instance deployments

## [0.9.1] - 2026-01-15

### Added
- **Presigned URL Expiration Rejection**: Proxy now detects and rejects expired AWS SigV4 presigned URLs before cache lookup
  - Parses `X-Amz-Date` and `X-Amz-Expires` from query parameters
  - Checks expiration locally without S3 API calls
  - Returns 403 Forbidden immediately for expired URLs
  - INFO-level logging with expiration details (seconds expired, signed time, validity duration)
  - Prevents serving cached data with expired access credentials
  - Example: `cargo run --release --example presigned_url_demo`

### Documentation
- **Presigned URL Support**: Added comprehensive documentation in CACHING.md
  - Explains how presigned URLs interact with caching
  - Documents two TTL strategies: Long TTL (performance) vs Zero TTL (security)
  - Clarifies cache key generation (path only, excludes query parameters)
  - Security considerations for time-limited access control
  - Early rejection behavior for expired presigned URLs

## [0.9.0] - 2026-01-06

### Added
- **Per-Instance Part Request Deduplication**: Prevents duplicate S3 requests when concurrent part requests arrive for the same object
  - When a part request arrives but multipart metadata is missing (object cached via regular GET), the proxy checks if this instance is already fetching any part for the same object
  - If an active fetch is in progress: waits 5 seconds, then returns HTTP 503 with `Retry-After: 5` header
  - Maximum 3 deferrals (15 seconds total wait) before forwarding to S3 anyway
  - The in-flight request populates multipart metadata (`parts_count`, `part_size`) from S3 response headers
  - Active fetches automatically expire after 60 seconds (stale timeout) to handle edge cases
  - This is per-instance coordination only - no cross-instance state sharing required

### Changed
- **Simplified Concurrent Part Request Handling**: Replaced complex cross-instance metadata population coordination with simpler per-instance request deduplication
  - Previous approach attempted RAM-based cross-instance coordination which doesn't work with shared storage
  - New approach: each instance independently tracks its own active S3 fetches
  - More reliable and predictable behavior in multi-instance deployments

### Fixed
- **Part Requests Incorrectly Served from Range Cache**: Fixed critical bug where part requests without multipart metadata were incorrectly served from cached ranges
  - Previously, when `lookup_part` returned cache miss, the code fell through to range handling which served the full object data instead of the specific part
  - This returned incorrect data (full object) with wrong headers (no `x-amz-mp-parts-count`, wrong `Content-Range`)
  - Now, part requests that miss the cache go directly to S3, bypassing range handling entirely
  - Part requests are only served from cache when multipart metadata (`parts_count`, `part_size`) is known
- **Part 1 Not Included in Deduplication**: Fixed bug where part 1 requests bypassed deduplication logic
  - Previously, part 1 was treated as "single-part object" when multipart metadata was missing
  - Now ALL part requests without multipart metadata go through deduplication
  - Ensures only one S3 request is made regardless of which part number arrives first

### Technical Details
- `ActivePartFetch` struct tracks cache_key, part_number, start time, and deferral count
- `handle_missing_multipart_metadata` registers active fetch and defers concurrent requests
- ALL part requests without multipart metadata now go through deduplication (including part 1)
- After 3 deferrals, forwards to S3 to prevent indefinite blocking
- `complete_part_fetch` and `fail_part_fetch` methods clean up tracking after S3 response
- 503 responses include `Retry-After: 5` header for client retry guidance

## [0.8.2] - 2026-01-06

### Fixed
- **Dead Code Removal**: Cleaned up legacy and unused code
  - Removed deprecated no-op methods: `refresh_metadata_expiration()`, `merge_overlapping_ranges()`
  - Removed unused journal cleanup methods: `cleanup_processed_entries()`, `cleanup_invalid_entries()`
  - Fixed test expectations for shared storage default configuration
  - All functionality preserved, no breaking changes

## [0.8.1] - 2026-01-05

### Changed
- **Shared Storage Enabled by Default**: Multi-instance coordination is now enabled by default
  - `shared_storage.enabled` now defaults to `true` instead of `false`
  - Provides better safety for multi-instance deployments out of the box
  - Single-instance deployments can set `shared_storage.enabled: false` to disable coordination overhead

## [0.8.0] - 2026-01-04 - Performance Optimized Shared Cache

### Changed
- **Faster Cross-Instance Cache Visibility**: Reduced default journal consolidation interval from 30s to 5s
  - Improves cache hit rates in multi-instance deployments with shared storage (EFS, FSx)
  - When one instance caches data, other instances see it within ~5s instead of ~30s
  - Configurable via `shared_storage.consolidation_interval` (valid range: 1-60 seconds)
  - Tradeoff: Slightly more frequent consolidation I/O for significantly better cache utilization
- **Consolidation Interval Range**: Changed valid range from 5-300 seconds to 1-60 seconds
  - Allows sub-5-second consolidation for latency-sensitive workloads
  - Upper bound reduced since consolidation is fast (~3ms for 200+ entries)

## [0.7.5] - 2026-01-04

### Changed
- **Faster Cross-Instance Cache Visibility**: Reduced default journal consolidation interval from 30s to 5s
  - Improves cache hit rates in multi-instance deployments with shared storage (EFS, FSx)
  - When one instance caches data, other instances see it within ~5s instead of ~30s
  - Configurable via `shared_storage.consolidation_interval` (valid range: 5-300 seconds)
  - Tradeoff: Slightly more frequent consolidation I/O for significantly better cache utilization

## [0.7.4] - 2026-01-04

### Changed
- **Versioned Request Handling**: Requests with `versionId` query parameter now properly validate against cached version
  - If cached object has matching `x-amz-version-id`: serve from cache (cache hit)
  - If cached object has different version: bypass cache, forward to S3, do NOT cache response
  - If no cached object exists: bypass cache, forward to S3, do NOT cache response
  - Prevents serving wrong version data when requesting specific object versions
  - Prevents cache pollution with version-specific data that may not be the "current" version
  - New metrics: `versioned_request_mismatch` and `versioned_request_no_cache` for monitoring

### Fixed
- **Version ID Cache Correctness**: Previously, versioned GET requests would incorrectly use cached data regardless of version
  - Old behavior: `GET /bucket/object?versionId=v2` could return cached data from version v1
  - New behavior: Only serves from cache if `x-amz-version-id` in cached metadata matches requested `versionId`

## [0.7.4] - 2026-01-04

### Changed
- **Zero-Copy Cache Writes**: Eliminated unnecessary data copy in CacheWriter when compression is disabled
  - Previously: `data.to_vec()` copied every chunk even without compression
  - Now: Writes directly from original slice when no compression needed
  - Reduces memory allocations and CPU usage during cache-miss streaming
  - Improves throughput for large file transfers

## [0.7.3] - 2026-01-04

### Changed
- **JournalOnly Mode for Range Writes**: Changed cache-miss range metadata writes from `WriteMode::Hybrid` to `WriteMode::JournalOnly`
  - Eliminates lock contention on shared storage (EFS) during large file transfers
  - Journal consolidator merges entries asynchronously without blocking streaming
  - Addresses 4x performance gap (80MB/s vs 300MB/s) caused by metadata lock contention
  - Each 8MB range write no longer acquires exclusive lock on metadata file

## [0.7.2] - 2026-01-03

### Changed
- **DiskCacheManager Lock**: Changed from `Mutex` to `RwLock` for improved parallel read performance
  - Cache lookups (reads) now use `.read().await` allowing concurrent access
  - Cache mutations (writes) use `.write().await` for exclusive access
  - Significantly improves throughput for parallel range requests on cache hits
  - Addresses performance gap where HTTPS (bypassing proxy) was faster than HTTP for parallel downloads
- **Read Methods**: Changed read-only methods to take `&self` instead of `&mut self`
  - `load_range_data`, `get_cache_entry`, `get_full_object_as_range` now use `&self`
  - `decompress_data`, `decompress_with_algorithm` in CompressionHandler now use `&self`
  - Enables true parallel reads through RwLock

## [0.7.1] - 2026-01-03

### Removed
- **AccessTracker Module**: Removed redundant `src/access_tracker.rs` module
  - Time-bucketed access logs in `access_tracking/` directory no longer used
  - Functionality consolidated into journal system (`CacheHitUpdateBuffer`)
- **BatchFlushCoordinator**: Removed from RAM cache module
  - RAM cache access tracking now handled by journal system at DiskCacheManager level
  - Removed `batch_update_range_access` function from DiskCacheManager
  - Removed `set_flush_channel`, `start_flush_coordinator`, `shutdown_flush_coordinator` methods
- **RAM Cache AccessTracker**: Simplified RAM cache by removing internal AccessTracker
  - `record_disk_access`, `should_verify`, `record_verification`, `pending_disk_updates` now no-ops
  - Access tracking for disk metadata handled by `record_range_access` in DiskCacheManager

### Changed
- **Unified Access Tracking**: All cache-hit access tracking now uses journal system
  - Range accesses recorded via `DiskCacheManager::record_range_access()`
  - Buffered in `CacheHitUpdateBuffer`, flushed periodically, consolidated to metadata
  - Eliminates duplicate tracking systems and reduces code complexity

## [0.7.0] - 2026-01-03

### Added
- **Journal-Based Metadata Updates**: New system for atomic metadata updates on shared storage
  - Eliminates race conditions when multiple proxy instances share cache storage (EFS/NFS)
  - RAM-buffered cache-hit updates with periodic flush to per-instance journal files
  - Background consolidation applies journal entries to metadata files with full-duration locking
  - Lock acquisition with exponential backoff and jitter for contention handling
  - New `CacheHitUpdateBuffer` for buffering TTL refresh and access count updates
  - New journal operations: `TtlRefresh` and `AccessUpdate` for incremental metadata changes

### Changed
- **Shared Storage Mode**: Cache-hit updates now route through journal system
  - TTL refreshes buffered in RAM, flushed every 5 seconds to instance journal
  - Access count updates buffered similarly, applied during consolidation
  - Single-instance mode retains direct write behavior for performance
- **Journal Consolidation**: Enhanced to handle new operation types
  - `TtlRefresh` updates range expiration without replacing range data
  - `AccessUpdate` increments access count and updates last_accessed timestamp
  - Conflict resolution skips incremental operations (they don't carry full range data)
- **Lock Manager**: Added `acquire_lock_with_retry` with configurable backoff
  - Exponential backoff with jitter prevents thundering herd
  - Configurable max retries, initial/max backoff, jitter factor

### Fixed
- **Race Condition**: Concurrent metadata updates on shared storage no longer corrupt data
- **Conflict Resolution**: TtlRefresh/AccessUpdate operations no longer overwrite existing range data

## [0.6.0] - 2026-01-03

### Changed
- **Journal RAM Buffering**: Access tracking now buffers entries in RAM before flushing to disk
  - Entries buffered in memory with periodic flush (every 5 seconds by default)
  - Dramatically reduces disk I/O on shared storage (EFS/NFS)
  - Buffer auto-flushes when reaching 10,000 entries
  - Force flush available for shutdown/testing scenarios
- **Removed Immediate Metadata Updates**: Access tracking no longer updates metadata files on every access
  - Metadata updates now happen only during consolidation (every ~60 seconds)
  - Eliminates per-access disk writes, improving throughput significantly
  - Access counts and timestamps still accurately tracked via journal consolidation
- **Simplified Access Tracking Directory Structure**: 
  - Removed per-instance `.access.{instance_id}` files from metadata directories
  - All access logs now stored in `access_tracking/{time_bucket}/{instance_id}.log`
  - Cleaner separation between metadata and access tracking data

### Performance
- **Reduced Disk I/O**: Up to 99% reduction in disk writes for high-traffic workloads
- **Lower Latency**: Access recording completes in <1ms (RAM buffer only)
- **Better Shared Storage Performance**: Optimized for EFS/NFS with batched writes

## [0.5.0] - 2025-01-02

### Added
- **RAM Metadata Cache**: New in-memory cache for `NewCacheMetadata` objects
  - Reduces disk I/O by caching frequently accessed metadata in RAM
  - LRU eviction with configurable max entries (default: 10,000)
  - Per-key locking prevents concurrent disk reads for same key
  - Stale file handle recovery with configurable retry logic
  - Configurable via `metadata_cache` section in config

### Changed
- **Unified HEAD/GET metadata storage**: HEAD and GET requests now share a single `.meta` file
  - Independent TTLs: HEAD expiry doesn't affect cached ranges, range expiry doesn't affect HEAD validity
  - New fields in `NewCacheMetadata`: `head_expires_at`, `head_last_accessed`, `head_access_count`
  - HEAD access tracking via journal system with format `bucket/key:HEAD`
- **Directory rename**: `objects/` directory renamed to `metadata/`
  - Cache will be wiped on upgrade (no migration needed)
  - All metadata files now stored in `metadata/{bucket}/{XX}/{YYY}/`

### Removed
- **Legacy HEAD cache**: Removed separate `head_cache/` directory and associated code
  - Removed `HeadRamCacheEntry`, `HeadAccessStats`, `HeadPendingUpdate`, `HeadAccessTracker` structs
  - Removed HEAD-specific methods from `RamCache` and `ThreadSafeRamCache`
  - Removed HEAD cache scanning from `CacheSizeTracker`
  - Kept `HeadCacheEntry` as return type for backward compatibility

## [0.4.0] - 2024-12-30

### Added
- **Cache bypass headers support**: Clients can now explicitly bypass the cache using standard HTTP headers
  - `Cache-Control: no-cache` - Bypass cache lookup but cache the response for future requests
  - `Cache-Control: no-store` - Bypass cache lookup and do not cache the response
  - `Pragma: no-cache` - HTTP/1.0 compatible cache bypass (same behavior as no-cache)
  - Case-insensitive header parsing with support for multiple directives
  - `no-store` takes precedence when both `no-cache` and `no-store` are present
  - `Cache-Control` takes precedence over `Pragma` when both headers are present
  - Headers are stripped before forwarding requests to S3
  - Configurable via `cache_bypass_headers_enabled` option (enabled by default)
  - Metrics tracking for bypass reasons (no-cache, no-store, pragma)
  - INFO-level logging for cache bypass events

### Changed
- **Unified disk cache eviction**: Replaced dual-mode eviction with unified range-level eviction
  - Removed arbitrary 3-range threshold that determined eviction mode
  - All ranges now treated as independent eviction candidates
  - Consistent LRU/TinyLFU sorting across all objects regardless of range count
  - Metadata file deleted only when all ranges evicted
  - Empty directories cleaned up automatically after eviction
  - Efficient batching: one metadata update per object during eviction

## [0.3.0] - 2024-12-21

### Added
- **Web-based monitoring dashboard** with real-time cache statistics and log viewing
  - Accessible at `localhost:8081` (configurable port)
  - Real-time cache hit rates, sizes, and eviction statistics
  - Application log viewer with filtering and auto-refresh
  - System information display (hostname, version, uptime)
  - No authentication required for internal monitoring
  - Minimal performance impact (<10MB memory, supports 10 concurrent users)
- Dashboard configuration options in `config.example.yaml`
  - Configurable refresh intervals for cache stats and logs
  - Adjustable maximum log entries display
  - Bind address and port configuration

### Changed
- Moved deployment-related files to non-public directory for better organization
- Updated documentation to reflect dashboard functionality
- Enhanced repository structure and cleanup

### Removed
- Old debugging scripts from `old-or-reference/` directory

## [0.2.0] - Previous Release

### Added
- Multi-tier caching (RAM + disk) with intelligent HEAD metadata caching
- Sub-millisecond HEAD response times from RAM cache
- Streaming response architecture for large files
- Unified TinyLFU eviction algorithm across GET and HEAD entries
- Intelligent range request optimization with merging
- Content-aware LZ4 compression with per-entry metadata
- Connection pooling with IP load balancing
- Write-through caching for single and multipart object uploads
- Multi-instance shared cache coordination
- OpenTelemetry Protocol (OTLP) metrics export
- Comprehensive test suite with property-based testing
- Docker deployment support
- Health check and metrics endpoints

### Features
- HTTP (Port 80): Full caching with range optimization
- HTTPS (Port 443): TCP passthrough (no caching)
- S3-compatible access logs and structured application logs
- Configurable TTL overrides per bucket/prefix
- Distributed eviction coordination for multi-instance deployments
- Performance optimizations for large-scale deployments

## [0.1.0] - Initial Release

### Added
- Basic S3 proxy functionality
- Simple caching implementation
- Core HTTP/HTTPS proxy server
- Basic configuration system