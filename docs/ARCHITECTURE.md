# Hybrid Cache for Amazon S3 architecture

This page explains how Hybrid Cache for Amazon S3 is built: its design principles, module layout, request flow and security model. It is for evaluators reviewing the design and developers changing the code. Operators deploying the proxy start with [Getting started](GETTING_STARTED.md).

## Table of contents

- [Core principles](#core-principles)
- [System architecture](#system-architecture)
- [Module organization](#module-organization)
- [Key design decisions](#key-design-decisions)
- [Request flow](#request-flow)
- [Performance characteristics](#performance-characteristics)
- [Security considerations](#security-considerations)
- [Observability](#observability)

---

## Core principles

### Transparent forwarder
- Proxy only responds to client requests
- Cannot initiate requests to S3 (no AWS credentials)
- Cannot sign requests (relies on client-signed requests)
- Caches responses between client and S3

### Streaming architecture
- All S3 responses stream directly to client via TeeStream
- Simultaneous caching in background
- A streamed response is never buffered whole, so its memory use does not grow with object size
- Cache-hit paths that buffer a range reserve its length in the in-flight ledger; see the [Range Read-Path Map](#3a-range-read-path-map)

### Unified range storage
- All cached data stored as ranges
- PUT operations: Stored as range 0-N
- GET operations: Full objects or partial ranges
- Multipart uploads: Parts assembled into ranges
- No data copying on TTL transitions

## System architecture

```text
 ┌──────────────────────┐  ┌───────────────────────┐  ┌─────────────────────┐
 │  S3 Client (HTTP)    │  │ S3 Client (HTTP_PROXY)│  │  S3 Client (HTTPS)  │
 │  - DNS/hosts routing │  │ - HTTP_PROXY=https:// │  │  - Default HTTPS    │
 │  - AWS CLI / SDK     │  │ - AWS CLI / SDK       │  │  - AWS CLI / SDK    │
 └──────────┬───────────┘  └──────────┬────────────┘  └──────────┬──────────┘
            │                         │                          │
            ▼                         ▼                          ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                    Hybrid Cache for Amazon S3 (1..N)                       │
│                                                                            │
│  ┌────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐  │
│  │ HTTP (Port 80)     │ │ TLS Proxy (Port 3129)│ │ HTTPS (Port 443)     │  │
│  │ - Caching          │ │ - TLS Termination    │ │ - TCP Passthrough    │  │
│  │ - Range Merging    │ │ - Caching            │ │ - No Caching         │  │
│  │ - Streaming        │ │ - Range Merging      │ │ - Direct to S3       │  │
│  └────────┬───────────┘ └──────────┬───────────┘ └──────────┬───────────┘  │
│           │                        │                        │              │
│           └────────────┬───────────┘                        │              │
│                        ▼                                    │              │
│           ┌───────────────────────────┐                     │              │
│           │        RAM Cache          │                     │              │
│           │  - Metadata + ranges      │                     │              │
│           │  - Compression            │                     │              │
│           │  - Eviction               │                     │              │
│           └─────────────┬─────────────┘                     │              │
└─────────────────────────┼───────────────────────────────────┼──────────────┘
                          │                                   │
                          ▼                                   │
           ┌───────────────────────────────┐                  │
           │   Shared Disk Cache (NFS)     │                  │
           │  - Metadata + ranges          │                  │
           │  - Compression                │                  │
           │  - TinyLFU-like eviction      │                  │
           │  - Fixed or elastic size      │                  │
           │  - Journaled writes           │                  │
           └───────────────┬───────────────┘                  │
                           │                                  │
                           └────────────────┬─────────────────┘
                                            │
                                            ▼
                              ┌─────────────────────────┐
                              │    Amazon S3 (HTTPS)    │
                              └─────────────────────────┘
```

## Module organization

```text
src/
├── main.rs              # Entry point, server initialization
├── lib.rs               # Library exports, module declarations
│
│   # Cache layer
├── cache.rs             # Unified cache manager
├── cache_types.rs       # Cache data structures
├── cache_size_tracker.rs # Cache size tracking (delegates to consolidator)
├── cache_initialization_coordinator.rs # Coordinated cache initialization
├── cache_validator.rs   # Cache integrity validation and file scanning
├── capacity_manager.rs  # Cache capacity checks and bypass decisions
├── disk_cache.rs        # Disk cache with streaming support
├── ram_cache.rs         # Sharded RAM cache (Arc<Bytes>, per-shard tokio RwLock, TinyLFU)
├── metadata_cache.rs    # RAM cache for NewCacheMetadata objects
├── write_cache_manager.rs # Write-cache capacity, eviction, and incomplete upload cleanup
├── bucket_settings.rs   # Hot-reloadable glob rules from cache_rules.json
├── capacity_guard.rs    # Admission guard for cache data writes near capacity
├── eviction_result.rs   # Last read-cache eviction pass's result, on the cache volume
├── fill_visibility_tracker.rs # Streamed miss fills committed but not yet consolidated
├── origin_identity.rs   # Per-bucket marker binding a cached bucket to its origin
├── path_safety.rs       # Path-component validation for file system safety
├── revalidation.rs      # What a conditional-revalidation response proves about a cached entry
├── bulk_write_invalidation.rs # Keys a DeleteObjects or form POST invalidates
├── stamp_backlog.rs     # Node-local log of pending write-order stamps (local_state_dir)
├── volume_stall.rs      # Per-proxy breaker that sends requests to S3 while the cache volume stalls
│
│   # Journal, consolidation, and size accounting
├── journal_manager.rs   # Per-proxy journal file management
├── journal_consolidator.rs # Background consolidation + size tracking + eviction
├── hybrid_metadata_writer.rs # Journal-based metadata writes
├── metadata_lock_manager.rs # Lock management on the cache volume
├── cache_hit_update_buffer.rs # RAM buffer for cache-hit metadata updates
├── write_ledger.rs      # Append-only record of staged write-cache ranges
├── ledger_repair.rs     # Rotation that repairs write-ledger completeness
├── staged_fold.rs       # Fold of the write ledger into the staged-bytes figure
├── staged_authority.rs  # Activation epoch and fencing for the staged figure
├── st_size_fold.rs      # readdir-plus-stat producer of the per-shard byte aggregate
├── eviction_worklist.rs # Shared eviction work list: chunks, leases, completion markers
├── scanner_registry.rs  # Per-proxy records of which maintenance scans are running
├── scanner_limiter.rs   # How many proxies may run a maintenance scan at once
│
│   # Orphan reclamation
├── orphan_reclaim.rs    # Leased rotation that deletes range files no metadata or journal lists
├── orphan_evidence.rs   # Strict journal reader for the reclamation's per-candidate evidence
│
│   # HTTP proxy
├── http_proxy.rs        # HTTP proxy with streaming
├── inflight_tracker.rs  # Download coordination: coalesces concurrent cache misses
├── inflight_ledger.rs   # Byte ledger for buffered request/response bodies
├── permit_body.rs       # Body wrapper holding a concurrency permit for the body's lifetime
├── signed_put_handler.rs # Signed PUT, multipart upload handling and caching
├── signed_request_proxy.rs # SigV4 request forwarding and TLS connection management
├── presigned_url.rs     # Presigned URL parsing and expiration checking
├── aws_chunked_decoder.rs # AWS chunked transfer encoding decode/encode
├── s3_client.rs         # S3 client wrapper with streaming
├── tee_stream.rs        # TeeStream for simultaneous streaming/caching
├── range_handler.rs     # Range request parsing and merging
├── hedged_fetch.rs      # Hedged upstream requests and their cost governor
├── bandwidth_limiter.rs # Download bandwidth QoS: token bucket and fair scheduler
├── throttle_stream.rs   # Streaming backpressure adapter for bandwidth QoS
│
│   # Listeners, HTTPS / TCP proxy
├── accept_policy.rs     # Accept-error policy shared by every listener
├── tls_proxy_listener.rs # TLS proxy listener: terminates TLS and feeds the caching pipeline
├── https_proxy.rs       # HTTPS passthrough server (port 443)
├── https_connector.rs   # Custom HTTPS connector with connection pool integration
├── tcp_proxy.rs         # TCP tunneling with SNI extraction and IP load balancing
│
│   # Networking and compression
├── compression.rs       # LZ4 compression with content-aware detection
├── connection_pool.rs   # IP distribution, DNS resolution, and health tracking
├── destination_policy.rs # Upstream destination validation (SSRF protection)
├── upstream_overrides.rs # Per-destination upstream transport overrides
├── tls_trust_store.rs   # OS trust store loader
│
│   # Observability
├── logging.rs           # Access and application logging
├── metrics.rs           # Metrics collection
├── otlp.rs              # OpenTelemetry Protocol export
├── health.rs            # Health check endpoints and system status monitoring
├── dashboard.rs         # Web dashboard with cache stats, system info, and log viewer
│
│   # Configuration and infrastructure
├── config.rs            # Configuration management
├── error.rs             # Error types
├── permissions.rs       # Directory permission validation on startup
├── bench_metadata.rs    # `s3-proxy bench-metadata`: measures the cache volume's .meta read rate
└── shutdown.rs          # Graceful shutdown coordination
```

## Key design decisions

### 1. Streaming response architecture

**Design**: TeeStream architecture
- All streaming S3 responses stream directly to client via TeeStream
- Data simultaneously sent to background task for caching
- No buffering of entire response in memory

**Implementation**:
```rust
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Bytes>,
    bytes_sent: usize,
    // plus backpressure and idle-watchdog state; see src/tee_stream.rs
}

impl<S> Stream for TeeStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, hyper::Error>> + Unpin,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                // Send data frames to the cache writer, applying backpressure
                // when the channel is full rather than dropping bytes
                Poll::Ready(Some(Ok(frame)))
            }
            // ... backpressure, idle-watchdog, and error handling
        }
    }
}
```

**Benefits**:
- The client does not wait for a whole large object to buffer, so SDK throughput timeouts do not apply
- The first byte reaches the client as soon as S3 sends it

**RAM Cache Integration**: Both streaming and buffered paths check RAM cache before disk I/O. On a RAM hit, the streaming path serves data directly from memory as a buffered response. On a RAM miss, the streaming path collects chunks during disk streaming and promotes the range to RAM cache after completion. Promotion is bounded by **per-shard** capacity (`max_ram_cache_size / effective_shard_count`, 64 MiB at defaults), not by `max_ram_cache_size` as a whole; see [RAM sizing and the admission ceiling](CONFIGURATION.md#ram-sizing-and-the-admission-ceiling).

### 2. Bucket-first hash-based sharding

**Problem**: Flat directory structure doesn't scale beyond 10K files per directory.

**Solution**: Bucket-first hash-based sharding with BLAKE3
```text
cache_dir/
├── metadata/{bucket}/{XX}/{YYY}/
└── ranges/{bucket}/{XX}/{YYY}/
```

**Why BLAKE3**:
- Uniform output spreads object keys evenly across leaf directories
- Native Rust implementation

**Implementation**:
```rust
fn get_sharded_path(base_dir: &Path, cache_key: &str, suffix: &str) -> Result<PathBuf> {
    let (bucket, object_key) = parse_cache_key(cache_key)?;
    let hash = blake3::hash(object_key.as_bytes());
    let hash_hex = hash.to_hex();
    
    let level1 = &hash_hex[0..2];   // 256 directories
    let level2 = &hash_hex[2..5];   // 4,096 subdirectories
    let filename = sanitize_object_key_for_filename(object_key, suffix);

    Ok(base_dir.join(bucket).join(level1).join(level2).join(filename))
}
```

The real function is `get_sharded_path` in `src/disk_cache.rs`.

**Capacity**:
- 1,048,576 leaf directories per bucket (256 × 4,096)
- 10.5B files per bucket maximum (10K files/directory)
- 2.6B files per bucket comfortable (25% of that limit)
- Unlimited buckets

### 3. Multi-tier caching strategy

**RAM Cache (TinyLFU)**:
- Range data from both streaming and buffered paths
- Served from memory, with no disk or network read
- Frequency-based eviction
- Configurable size limits
- Streaming path checks RAM before disk, promotes to RAM after disk hits
- Sharded into `ram_cache_shard_count` independent `tokio::sync::RwLock` partitions (default 8), keyed by `blake3(cache_key) % shard_count`, so concurrent reads of different keys do not contend
- Stores data as `Arc<Bytes>` for O(1) zero-copy reads; the shard lock is released before decompression and response construction
- Access counters (`last_accessed`, `access_count`, hit/miss) are atomics updated under a shared read lock; TinyLFU victim scoring (`shard_find_tinylfu_victim`) reads them at eviction time, so a read maintains no ordering structure
- See [RAM cache concurrency model](CACHE_INTERNALS.md#ram-cache-concurrency-model)

**Disk Cache (LZ4 Compressed)**:
- All object data stored as ranges
- Content-aware compression
- Streaming read/write support
- TTL-based expiration

**Cache Coordination**:
- File locking for fleet coordination
- Atomic cache operations
- Consistent metadata management

### 3a. Range read-path map

`http_proxy.rs` and `cache.rs` each contain several similarly-named range read
paths that differ in exactly the details that matter for cache-tier
participation. Which tier a path reaches is not visible at its call site: RAM
access happens one level down, inside `CacheManager`, so searching a file for
`ram_cache` understates its RAM participation. This map records, per entry
point, what actually happens.

| Entry point | Consults RAM? | Promotes to RAM? | RAM key | Reserves against the in-flight ledger? |
|---|---|---|---|---|
| `serve_range_from_cache` → RAM pre-check hit (`http_proxy.rs`) | Yes, an exact-key lookup before the streaming decision | n/a (already resident) | `generate_ram_range_key` (colon grammar) | **No** |
| `serve_range_from_cache` (streaming, `http_proxy.rs`) | Only via the buffered/RAM pre-check before the streaming decision | On disk-hit collection during streaming, via the frame-verbatim path | `generate_ram_range_key` (colon grammar) | **No**; bounded by its 4-slot frame channel instead |
| `serve_range_from_cache_buffered` (`http_proxy.rs`) | Yes: `get_cached_range_data` → single-range branch checks RAM directly | Yes, on disk hit | `generate_ram_range_key` (colon grammar) | **Yes**: the requested range length, held by `PermitBody` until the client drains or disconnects |
| `serve_full_object_from_cache` (`http_proxy.rs`) | Yes, via `get_cached_range_data` | Yes, on disk hit | `generate_ram_range_key` (colon grammar) | **Yes**: the full object length, held by `PermitBody` |
| `get_cached_range_data` → single-range branch | Yes | Yes, on disk hit | `generate_ram_range_key` (colon grammar) | Only its own recovery fetch, scoped to that fetch |
| `get_cached_range_data` → merge branch → `RangeHandler::merge_range_segments` → `extract_bytes_from_cached_range` → `CacheManager::load_range_data_with_cache` | Yes, **per cached segment** | Yes, per segment on disk hit | `generate_ram_range_key` (colon grammar), keyed on each segment's stored-range bounds | Only its own fallback fetch, scoped to that fetch |
| `fill_page` (page mode) → `load_page_from_cache` | Yes: page-keyed RAM lookup (`get_range_from_ram_cache` with page bounds) | Yes, on disk hit only, never on the cold S3-fetch fill | `generate_ram_range_key` (colon grammar), keyed on page bounds, not the client's sub-range | **No** |

Notes:
- **A request covering an object's entire byte range does not reach the buffered
  path.** The RAM lookup is keyed on the exact `(cache_key, start, end)` triple,
  and caching a full object registers it under the whole-object range key. So
  `Range: bytes=0-<size-1>` is an exact-key RAM hit: served with `X-Cache: HIT`
  and no ledger reservation. Only a partial range the RAM cache has not seen
  reaches `serve_range_from_cache_buffered`.
- The two reserving paths hold their claim on the response body, so it lasts until
  the body is delivered or the client disconnects. Every other reservation in this
  table is scoped to an upstream fetch. See [in-flight memory](METRICS.md)
  when sizing a ceiling.
- Every path in the table shares the **same RAM key grammar** (`generate_ram_range_key`,
  colon-separated) via the single-source-of-truth helper next to
  `generate_range_cache_key` in `cache.rs`. This is distinct from the
  disk-cache key grammar (`generate_range_cache_key`, hyphen-separated).
- `merge_range_segments` itself never mentions RAM. The tier access is
  inside `extract_bytes_from_cached_range`, one call down.
- See [Caching](CACHING.md) for the maintained description of what gets
  cached and why; this map is about *which code path* reaches which tier, not
  about cache semantics.

### 4. Stateless proxy architecture

**Problem**: Traditional distributed caches require cluster membership, leader election, or inter-node communication, adding operational complexity.

**Solution**: Coordination only through the cache volume
- Proxies have no knowledge of each other
- All coordination goes through file-based locking on the cache volume
- No cluster configuration, membership protocols, or network discovery

**Benefits**:
- **Replaceable proxies**: proxies can be added, removed or replaced at any time without affecting the others
- **Simple Operations**: No cluster reconfiguration when scaling up/down
- **Fault isolation**: one proxy's failure does not affect the others
- **Recovery**: replace a failed proxy by starting a new one on the same cache volume

**Coordination Mechanisms**:
- File locks for write coordination (metadata updates, eviction)
- Per-proxy journal files for cache-hit updates (no write conflicts)
- Distributed eviction lock prevents over-eviction during scale events
- Journal consolidator merges updates from all proxies asynchronously

**Deployment Model**:
```text
Proxy 1    ──┐
Proxy 2    ──┼── Cache volume (NFS)
Proxy N    ──┘    ├── metadata/
                  │   ├── _meta_locks/   (fixed slot set, never deleted)
                  │   └── _journals/     (per-proxy journal files)
                  ├── ranges/
                  └── locks/             (eviction, consolidation, and initialization coordination)
```

Each proxy works independently; the cache volume is the only integration point.

### 5. Buffered logging and accumulator-based size tracking

**Problem**: Direct disk writes for access logs cause high I/O contention on a shared NFS file system, especially in a fleet.

**Solution**: 
- Access logs buffered in memory, flushed every 5 seconds or 1000 entries
- In-memory `AtomicI64` accumulator tracks size at write/eviction time with no NFS I/O
- Per-proxy delta files flushed periodically, summed by consolidator under global lock

**Implementation**:
```text
┌─────────────────────────────────────────────────────────────────┐
│                 Hybrid Cache for Amazon S3 Instance              │
│  ┌─────────────────────┐    ┌─────────────────────────────────┐ │
│  │  Request Handler    │    │  Cache Operations               │ │
│  │                     │    │                                 │ │
│  │  log_access()  ─────┼────┼──► AccessLogBuffer              │ │
│  │                     │    │    - entries: Vec<AccessLogEntry>│ │
│  │                     │    │    - flush every 5s             │ │
│  │                     │    │                                 │ │
│  │                     │    │  store_range() ─────────────────┼─┤
│  │                     │    │    │                            │ │
│  │                     │    │    ▼                            │ │
│  │                     │    │  SizeAccumulator.add()          │ │
│  │                     │    │    - AtomicI64 fetch_add        │ │
│  │                     │    │    - Zero NFS overhead          │ │
│  └─────────────────────┘    └─────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  JournalConsolidator (background task, every 5s)            ││
│  │    - Flushes accumulator to delta_{instance_id}_{seq}.json  ││
│  │    - Acquires global lock                                   ││
│  │    - Sums all delta files → updates size_state.json         ││
│  │    - Deletes each delta file after applying it              ││
│  │    - Processes journal entries for metadata updates only    ││
│  │    - Triggers eviction when cache exceeds capacity          ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ (every 5 seconds)
┌─────────────────────────────────────────────────────────────────┐
│                    Shared Storage (NFS)                         │
│                                                                 │
│  logs/access/YYYY/MM/DD/                                        │
│    └── {timestamp}-{hostname}     ← Access logs (per-instance)  │
│                                                                 │
│  cache/metadata/_journals/                                      │
│    └── {instance_id}.journal      ← Per-instance journal entries │
│                                                                 │
│  cache/size_tracking/                                           │
│    ├── size_state.json            ← Authoritative size state    │
│    └── delta_{inst}_{seq}.json    ← One new file per flush      │
└─────────────────────────────────────────────────────────────────┘
```

**Accumulator-Based Size Tracking**:
- Size recorded immediately at write/eviction time via atomic operations
- Each proxy flushes its accumulated delta to its own file every 5 seconds
- Consolidator sums all delta files under global lock, updates `size_state.json`, and deletes each file after applying it. A zero-valued file is deleted, and an unparseable one is renamed to `<name>.corrupt` with its delta not applied. After a rolling validation cycle installs a corrected size, every remaining delta file is deleted along with `delta_*.json.tmp` files older than an hour. A full validation scan leaves live delta files alone and removes only `delta_*.json.tmp` files more than an hour old, which a writer left behind when it stopped before renaming them
- Size deltas come from the delta files; journal entries update metadata
- Eviction triggered automatically when cache exceeds capacity

**Effect**:
- Access-log and size-delta writes happen once per flush interval, not once per request
- Per-instance files mean instances do not contend for one shared file on the hot path
- Maximum data loss on crash: 5 seconds of logs and size deltas

**Recovery**:
- On startup, consolidator loads `size_state.json`
- If missing, starts at 0 and the periodic validation scan will correct
- In-memory accumulator starts at zero; delta files are summed on next consolidation cycle

**Periodic validation scan**: a daily background scan reconciles the tracked size with what is on disk. [The validation scan](SHARED_STORAGE.md#the-validation-scan) describes it.

## Request flow

### GET request processing

1. **Request Validation**: Parse and validate incoming request
2. **Cache Lookup**: Check RAM cache for metadata
3. **Range Analysis**: Determine required byte ranges
4. **Cache Hit Path**: Serve from cache if available
5. **Download Coordination**: On cache miss, coalesce with any in-flight fetch for the same resource
6. **Cache Miss Path**: Forward to S3, stream response
7. **Background Caching**: Store response data asynchronously
8. **Response Assembly**: Merge ranges if needed

### PUT request processing

1. **Request Forwarding**: Stream the request body to S3, staging a copy in the cache as it passes
2. **Response Capture**: Capture S3 response headers
3. **Cache Invalidation**: Once S3 confirms, invalidate the key's previous cached version and record its write order
4. **Response Return**: Return S3 response to client
5. **Cache Storage**: Publish the new object and metadata in the background

[Write-through ordering](CACHING.md#write-through-ordering) has the full ordering
and its failure cases.

### Multipart upload processing

**Upload Part (UploadPart)**:
1. **Request Forwarding**: Stream part data to S3
2. **Part Caching**: Store part data in temporary location
3. **Tracker Update**: Record part metadata in upload tracker
4. **Response Return**: Return S3 response to client

**Complete Multipart Upload (CompleteMultipartUpload)**:
1. **Request Parsing**: Parse XML body to extract requested parts and ETags
2. **Request Forwarding**: Forward completion request to S3
3. **ETag Validation**: Verify cached part ETags match request ETags
4. **Part Filtering**: Retain only parts listed in the request, delete unreferenced parts
5. **Cache Finalization**: Build `part_ranges` from filtered parts with cumulative byte offsets
6. **Graceful Degradation**: On ETag mismatch, or a recorded part whose `.bin` is gone, skip caching and clean up the upload directory. A requested part still unrecorded after a bounded wait skips caching and leaves the directory for the TTL sweep
7. **Response Return**: Return S3 response to client

**Fleet safety**:
- Each upload isolated in `mpus_in_progress/{hash(uploadId)}/` directory
- An unrecorded part leaves staging in place, so a writer still mid-write on another proxy is not disturbed
- Prevents incomplete cache entries that could serve corrupted data

For the full multipart upload state machine, correctness gates, concurrency semantics, and threat model see [Multipart uploads](MULTIPART_UPLOAD.md).

## Performance characteristics

[Performance](../README.md#performance) in the project README has the measured throughput and request-time figures. This section states what each path does.

- **RAM cache hit**: served from process memory, with no disk or network read.
- **Disk cache hit**: read from the cache volume, so its latency is the volume's latency for the range size.
- **Cache miss**: an S3 request plus the proxy's own processing.
- **Streaming**: a response streams through without being buffered whole, so memory use does not grow with object size.
- **Compression**: LZ4 runs on every cache write. The space saved depends on the content; already-compressed formats are stored in uncompressed frames at about 1:1 ([Compression](COMPRESSION.md)).
- **Connection pooling**: pooled upstream connections are reused, so a request on a pooled connection skips the TCP and TLS handshakes.
- **Scale**: proxies coordinate only through the cache volume, so a fleet grows by adding proxies, and `server.max_concurrent_requests` bounds each proxy's concurrent requests.

## Security considerations

> **Your Responsibility**: You are responsible for securing both network access to the proxy and file system access to the cache volume. With a TTL above zero, cache hits are served without contacting S3, so any client that can reach the proxy can read any cached object. Anyone with access to the cache volume can read cached data directly. Restrict access to those clients and systems authorized to access the cached data. Copies cached through an access point or Multi-Region Access Point are kept apart from their bucket's, so a write through one surface leaves the other's copies served until their TTL ([details](CACHING.md#access-points-and-multi-region-access-points)).

### Network security requirements

The recommended client hop is encrypted. Clients reach the TLS proxy listener (port 3129) with `HTTP_PROXY=https://<proxy-host>:3129` and `--endpoint-url http://s3.<region>.amazonaws.com`; the proxy terminates TLS with its own certificate and caches the decrypted requests. Only the AWS CRT transfer client verifies the proxy's certificate. botocore-based clients (`aws s3api`, `aws s3` with the classic transfer client, boto3) encrypt the hop without verifying it. For them the hop is protected against passive observation but not against an attacker who can impersonate the proxy ([per-client table](GETTING_STARTED.md#configuring-clients-to-trust-the-certificate)).

The HTTP listener (port 80) and the forward proxy (port 3128) take cleartext HTTP, whether clients reach them through DNS routing or `HTTP_PROXY=http://<proxy-host>:80`. Use them from the proxy's own host, where the hop stays on loopback, or across a network segment that is secure and where cleartext is acceptable. [What a cleartext hop exposes](#what-a-cleartext-hop-exposes) lists what an observer on that segment could read and replay.

- **Network controls**: use security groups, firewalls or network segmentation to restrict proxy access to authorized clients.
- **Upstream**: by default the proxy connects to the origin over verified TLS on port 443. A per-destination [upstream transport override](CONFIGURATION.md#upstream-transport-overrides) can change that; its protection-waiving modes (plaintext HTTP, or HTTPS with certificate validation disabled) are for local development or secure network segments only.
- **Encrypting the hop across a fleet**: point `HTTP_PROXY` at [a name with one record per proxy](GETTING_STARTED.md#fleet-behind-one-proxy-name), or put a load balancer that re-encrypts to the TLS proxy listener in front of the fleet ([AWS deployment](AWS_DEPLOYMENT.md#load-balancer-encrypting-the-client-hop)). A self-managed Layer 7 router is another option, and can keep the cleartext hop on loopback by running on the client host; see [Request-aware routing](REQUEST_AWARE_ROUTING.md).

#### What a cleartext hop exposes

This section lists what a cleartext hop exposes, so you can judge whether a network segment is secure enough to carry one. SigV4 authenticates each request but does not encrypt it. The signature proves the request was not forged and cannot be redirected to a different object, but anything traveling in cleartext is readable by any party that can observe the traffic.

An observer on a cleartext hop captures:

- **The bucket and object key**, from the request line and `Host` header. Over time this reveals your bucket contents, object sizes, and access patterns.
- **The `Authorization` header**, which contains the caller's **access key ID** (in the `Credential` scope), the `SignedHeaders` list, and the signature.
- **Object payloads in both directions**: GET response bodies and PUT/UploadPart request bodies, in full.
- **Presigned URL query parameters**, when clients use presigned URLs, including `X-Amz-Signature` and `X-Amz-Expires`.

The secret access key is never transmitted and cannot be recovered from a captured request.

What an observer can then do:

- **Replay the captured request verbatim against S3.** The signature covers the request, not the channel, so a captured signed request is valid until its timestamp ages out. [AWS documents a 15-minute window](https://repost.aws/it/questions/QUwKI3rzGOSgO2fsFrOdPwYw/problem-with-recreation-of-aws-signature), and notes that a party holding a signed request can also alter its *unsigned* portions without invalidating it. A replayed GET returns the object; a replayed PUT rewrites the same content.
- **Replay a captured presigned URL for its full remaining validity**, which may be hours or days rather than 15 minutes. This is the longer-lived exposure of the two.
- **Not reach a different object.** Method, path, query, and the signed headers are all covered by the signature, so a captured request cannot be edited to read another key without the secret access key.

**Which hops are affected.** Cleartext applies to a hop only where the traffic crosses a network; the same listener on loopback exposes nothing to the network:

| Hop | Cleartext? |
|---|---|
| Client → HTTP listener (`:80`) or forward proxy (`:3128`) across a network | **yes** |
| Client → either listener on loopback (`127.0.0.1`, sidecar/local-dev) | no, never leaves the host |
| Load balancer → proxy, where the LB terminates TLS ([Pattern 1](GETTING_STARTED.md#pattern-1-load-balancer-terminates-tls--proxy-serves-http)) | **yes**: encrypted client→LB, cleartext LB→proxy |
| Client → TLS proxy listener (`:3129`), directly or via LB TCP passthrough | no |
| Load balancer → proxy, where the LB re-encrypts to `:3129` ([Pattern 3](GETTING_STARTED.md#pattern-3-load-balancer-terminates-and-re-encrypts--proxy-terminates-tls)) | no |
| Proxy → origin, default | no, verified TLS on 443 |
| Proxy → origin, with `scheme: http` in [`upstream_overrides`](CONFIGURATION.md#upstream-transport-overrides) | **yes** |
| Proxy → origin, with `validate_tls: false` | encrypted, but no MITM protection: an interceptor can read and alter it |
| Client → HTTPS passthrough (`:443`) | no, opaque bytes, and uncacheable for the same reason |

Every affected hop has an encrypted alternative that preserves caching. Where a hop must stay in cleartext, treat every client, load balancer, and host on that network segment as able to read all S3 traffic passing over it and replay it within the windows above. Size the network controls accordingly.

**TLS Certificate Management**: When TLS is enabled, the proxy loads a certificate and private key from paths specified in the config. For a fleet, store the certificate and key on the shared file system alongside the configuration (e.g., `/mnt/nfs/config/tls/cert.pem` and `/mnt/nfs/config/tls/key.pem`) so all proxies use the same certificate. Restrict file permissions on the private key (`chmod 600`). For clients that verify the certificate (see the [per-client table](GETTING_STARTED.md#configuring-clients-to-trust-the-certificate)), its Subject Alternative Names (SANs) must match how clients connect. Use `IP:` SANs for direct IP connections, or `DNS:` SANs when clients connect through a load balancer or DNS name.

### Shared cache access model

The proxy is a shared cache. It does not authenticate clients or authorize requests. S3 handles both, but only for requests that reach S3. With a TTL above zero, cache hits are served without contacting S3, so any client that can reach the proxy can read any cached object.

**What this means in practice**:
- Any client with network access to the proxy can read any cached object until its TTL expires
- A user whose S3 access was revoked can still retrieve cached data until TTL expires
- Different users share the same cached responses; there is no per-user isolation

**This is the same security model as any shared cache** (CDN edge cache, Mountpoint for Amazon S3's local cache, or a shared NFS export of downloaded files). You control who can access the cache, as you would for any local copy of S3 data.

**Two access paths to secure**:
1. **Network access to the proxy**: restrict to authorized clients using security groups, firewalls, or network segmentation
2. **File system access to the cache volume**: restrict to the proxies, using file permissions and mount controls

**Mitigation - Always-Revalidate Mode (TTL=0)**: For environments requiring per-request authentication and authorization enforcement, TTL values can be set to zero:

```yaml
cache:
  get_ttl: "0s"
  head_ttl: "0s"
  actively_remove_cached_data: false  # Required - must use lazy expiration
```

**Behavior with TTL>0 (expired)**: When a cached object's TTL has elapsed, the next request triggers the same conditional revalidation flow described below. See [Cache Revalidation](CACHE_FRESHNESS.md#get-ttl-get_ttl) in the Caching documentation for full details including 304, 200, and 403 outcomes.

**Behavior with TTL=0**:
- Cache still stores data (useful for range merging, bandwidth savings)
- Every request triggers S3 revalidation via conditional requests (`If-Modified-Since` + `If-None-Match`)
- Client's original request headers (including Authorization) are forwarded to S3
- S3 validates the requesting client's IAM credentials on every request
- S3 returns 304 Not Modified if unchanged → cached data served, bandwidth saved
- S3 returns 200 OK if changed → fresh data fetched and cached
- S3 returns 403 Forbidden if client lacks access → error returned, cache unchanged

**Important**: TTL=0 requires `actively_remove_cached_data: false` (lazy expiration). With active expiration enabled, cached data would be immediately deleted after storage, defeating the purpose.

**Use cases**:
- **Per-request IAM authentication and authorization**: Ensures every client's credentials are validated by S3, preventing unauthorized access to cached data
- **Maximum freshness guarantees**: Every request confirms data hasn't changed
- **Compliance scenarios**: Audit trail of S3 validating each access

**Trade-offs**:
- Every request incurs S3 round-trip latency
- Bandwidth savings only from 304 responses (no data transfer when unchanged)
- Higher S3 request costs

### Request validation limitations

**Bucket Owner Validation Not Supported**: The `x-amz-expected-bucket-owner` header cannot be validated for cached responses:

- **S3 Does Not Return Owner**: GetObject and HeadObject responses do not include bucket owner information
- **No Owner Stored in Cache**: Since owner is never received, it cannot be stored or validated
- **Cached Responses Bypass Validation**: Requests with `x-amz-expected-bucket-owner` served from cache will succeed regardless of the header value
- **Cache Miss Behavior**: Only on cache miss does S3 validate the header (and return 403 if mismatched)

### Data confidentiality

**Cache storage**: the proxy stores object data compressed where the content compresses, and adds no encryption of its own:

- **Plaintext storage**: object data is readable on disk, compressed but not encrypted
- **Metadata exposure**: object keys, sizes and access patterns are visible in cache metadata
- **File system access**: anyone with file system access can read cached data directly
- **Encryption at rest**: comes from the cache volume. Enable EFS or FSx encryption at rest, EBS volume encryption, or block-device encryption such as dm-crypt.

### Trust and integrity model

The situations in which the proxy can return data that differs from S3, each with its bound and mitigation, are listed in [When the proxy can return data that differs from S3](CACHE_FRESHNESS.md#when-the-proxy-can-return-data-that-differs-from-s3).

Separate from the access-control model above, the proxy's **data integrity model** describes what it verifies about cached bytes, what it trusts upstream components to guarantee, and what's explicitly out of scope.

**What the proxy trusts**:
- **S3 validates bytes on ingress.** Every PUT and UploadPart is forwarded unmodified to S3. If S3 returns 200, its stored bytes match what the client sent, and S3 records its own checksum (CRC64NVME by default since December 2024).
- **LZ4 frame content checksums catch disk corruption.** All cached bytes, compressed or stored in an uncompressed frame, carry an xxhash32 frame checksum (see [Compression](COMPRESSION.md#integrity-every-write-is-a-checksummed-lz4-frame)). Bit-flips surface as decode errors and are handled as cache misses.
- **The storage layer catches lower-level corruption.** File system and block-device integrity (EFS, ext4, XFS) cover everything below the frame checksum.

**What the proxy verifies**:
- **ETag equality for read invalidation.** On conditional revalidation and range reads, mismatched ETags invalidate cached entries.
- **ETag equality at multipart finalization.** `CompleteMultipartUpload` request body ETags per part are compared against the proxy's recorded per-part ETags. On any mismatch the cache finalization is skipped and the `mpus_in_progress/{hash(uploadId)}/` directory is cleaned up. The client still gets S3's success response; the cache just doesn't retain the object. Covered in [Multipart uploads](MULTIPART_UPLOAD.md).
- **aws-chunked decoded length.** When the `x-amz-decoded-content-length` header is present, the decoder verifies the decoded body length matches and skips caching on mismatch (see `aws_chunked_decoder`).
- **Cache metadata structural consistency.** A `.meta` that does not parse is treated as a miss, and one confidently corrupt is replaced by the next full-object fetch (see [Error handling](ERROR_HANDLING.md#1-corrupted-metadata-files)).
- **Cache keys are validated to stay within the cache directory.** `parse_cache_key` rejects bucket segments equal to `.`, `..`, empty, or containing `/`, `\`, NUL, or ASCII control characters (0x00–0x1F, 0x7F). This ensures every sharded path produced by `get_sharded_path` is a descendant of the configured `cache_dir`, defending against path-traversal input from the HTTP listener.
- **Host headers are validated against RFC 7230 / RFC 3986.** `parse_host_header` accepts bracketed IPv6 literals (`[::1]`, `[::1]:8081`, `[2001:db8::1]:443`), plain hostnames, and IPv4. It rejects unclosed brackets, stray `]`, unbracketed IPv6, invalid ports, and empty values with a 400 response.

**Cache staleness is not a security boundary.** Any client with valid S3 credentials can write directly to a bucket, bypassing the proxy entirely. When that happens, cached entries for affected objects become stale until their TTL expires and the next read revalidates them by ETag. This is ordinary read-through cache behavior, with the usual mitigations: tune TTLs, use TTL=0 for per-request authorization, or configure clients to always go through the proxy.

**Residual integrity gap**:
A motivated attacker could make the cache retain bytes that no longer match what S3 stored. This needs both (a) interception of a client's multipart upload in flight, with one part substituted through a direct-to-S3 UploadPart call, and (b) an MD5 collision for that part. This gap exists because per-part ETags on SSE-S3 single-part data are MD5 of the content, and MD5 is not collision-resistant. The same attacker could confuse any client or tool relying on ETag matching for integrity.

Step (a) requires an actor who can call `UploadPart` against
that upload, meaning one who already holds valid S3 write credentials for the bucket.
As stated above, such an actor can write to the bucket directly regardless of the proxy,
which is outside the threat model. Weigh any mitigation against that, since it is bought only
for the narrow case where an authorized writer wants the cache to diverge from S3,
having also defeated MD5.

Mitigation at the bucket/client layer (not the proxy):
- **Request a cryptographic checksum at `CreateMultipartUpload`** (e.g., `--checksum-algorithm SHA256`). Per-part checksums then land in the `CompleteMultipartUpload` request body and S3 verifies them end-to-end. Note that uploads carry a checksum by default: current AWS SDKs and CLI v2 apply CRC64NVME automatically, and S3 stores it as the default checksum. CRC64NVME detects corruption in transit but is not collision-resistant, so it does not defend against an attacker who controls the content; SHA256 does.

### Deployment guidelines

**Before deploying, ensure**:
1. Network access to the proxy is restricted to clients authorized to access all objects that may be cached
2. File system access to the cache volume is restricted to the proxies
3. TTL values reflect your tolerance for serving stale data after access revocation (use TTL=0 for per-request authorization)

**Appropriate Use Cases**:
- Internal networks where all clients are authorized to access the same S3 data
- Development and testing environments
- Single-tenant deployments with controlled network access
- On-premises environments with network segmentation between trust zones

**Not Recommended For**:
- A cache shared by clients that must not see each other's data. The proxy cannot tell clients apart, and cache hits are served without consulting S3. Run a separate proxy and cache volume per trust domain.
- Public-facing or untrusted network deployments
- Environments requiring per-object access control between clients (unless using TTL=0)

### Destination policy (SSRF protection)

The proxy includes a destination policy engine that prevents SSRF attacks by validating upstream destinations before forwarding. When enabled, it blocks requests to prohibited IP ranges (IMDS 169.254.169.254, loopback, private networks, link-local).

**Coverage**: The policy always gates the `CONNECT` and port-443 SNI passthrough paths. It gates the HTTP forwarding paths only when `server.tls.connect_allowlist` is set (see **Gating** below), so by default a request on port 80 reaches a private-IP origin unchecked. [AWS deployment](AWS_DEPLOYMENT.md#s3-compatible-store-outside-aws) traces each path. The paths and their checks:
- **HTTPS (CONNECT/SNI)**: Port 443 enforcement, hostname allowlist, IP classification
- **HTTP forwarding path**: the same checks, with the port gate set to `server.http_port`, applied to the `Host` header's port (the request-target port when there is no `Host`); a portless `Host` or `:80` counts as `server.http_port`
- **TLS proxy listener**: both of its paths. A `CONNECT` tunnel is evaluated against port 443; a non-CONNECT request takes the HTTP forwarding path and is evaluated against `server.http_port`. The two use separately-constructed policies because the port gate runs before the allowlist and before DNS, so a single instance cannot cover both ports.

**Gating**: The HTTP forwarding path's policy (on the port-80 listener and on the TLS proxy listener alike) is active only when `connect_allowlist` is configured in the `server.tls` section. Without an allowlist those paths skip the check entirely, which lets operators who rely on the proxy as a general forward proxy opt out. The CONNECT and SNI paths always apply the port gate and IP classification; an allowlist, when configured, adds hostname filtering to them.

**What the check does and does not do**: it is a pre-flight rejection of destinations, evaluated before any request is sent. The validated addresses it returns are discarded, because the S3 client re-resolves through the connection pool when it dials. So the policy bounds which destinations may be attempted; it does not pin the connection to an address the policy inspected.

**IP ranges blocked** (on every path the policy gates):
- `0.0.0.0/8`, `127.0.0.0/8` (loopback), `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` (private)
- `169.254.0.0/16` (link-local / IMDS)
- IPv6 equivalents: `::`, `::1`, `fe80::/10`, `fc00::/7`, `::ffff:` IPv4-mapped

**Endpoint override carve-out**: IPs listed in `connection_pool.endpoint_overrides` are exempted from the prohibited-range check, allowing PrivateLink ENIs in private address space.

**S3 endpoints**: All public S3 IPs pass the policy. The check has no impact on normal S3 traffic.

## Observability

### Dashboard

A read-only web dashboard (port 8081 by default) shows cache statistics, recent application log entries and system information. It is unauthenticated and offers no write operations or credentials. Its code default bind address is `127.0.0.1`; binding it to `0.0.0.0` needs network-layer restriction such as a security group or firewall. [Dashboard](DASHBOARD.md) covers configuration and security.

### Metrics

`/metrics` (port 9090 by default) and the optional OTLP export report cache hits and misses, cache sizes, eviction, connection pool and request figures. [Metrics reference](METRICS_REFERENCE.md) lists every field, and [OTLP metrics](OTLP_METRICS.md) covers the export.

### Logging

The proxy writes S3-style access logs ([Access log format](ACCESS_LOG_FORMAT.md)) and structured application logs.

### Health checks

`/health` (port 8080 by default) reports cache status, including usage against the size limit, connection pool status and compression handler status.
