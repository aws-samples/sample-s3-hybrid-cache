# Hybrid Cache for Amazon S3 Architecture

Technical architecture overview and design principles for Hybrid Cache for Amazon S3.

## Table of Contents

- [Core Principles](#core-principles)
- [System Architecture](#system-architecture)
- [Module Organization](#module-organization)
- [Key Design Decisions](#key-design-decisions)
- [Request Flow](#request-flow)
- [Performance Characteristics](#performance-characteristics)
- [Security Considerations](#security-considerations)
- [Observability](#observability)

---

## Core Principles

### Transparent Forwarder
- Proxy only responds to client requests
- Cannot initiate requests to S3 (no AWS credentials)
- Cannot sign requests (relies on client-signed requests)
- Acts as intelligent cache between client and S3

### Streaming Architecture
- All S3 responses stream directly to client via TeeStream
- Simultaneous caching in background
- Eliminates buffering and memory pressure
- Constant memory usage regardless of file size

### Unified Range Storage
- All cached data stored as ranges
- PUT operations: Stored as range 0-N
- GET operations: Full objects or partial ranges
- Multipart uploads: Parts assembled into ranges
- No data copying on TTL transitions

## System Architecture

```
 ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
 │  S3 Client (HTTP)    │  │ S3 Client (HTTP_PROXY)│  │  S3 Client (HTTPS)  │
 │  - DNS/hosts routing │  │ - HTTP_PROXY=https:// │  │  - Default HTTPS    │
 │  - AWS CLI / SDK     │  │ - AWS CLI / SDK       │  │  - AWS CLI / SDK    │
 └──────────┬───────────┘  └──────────┬────────────┘  └──────────┬──────────┘
            │                         │                          │
            ▼                         ▼                          ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                    Hybrid Cache for Amazon S3 (1..N)                        │
│                                                                            │
│  ┌────────────────────┐ ┌─────────────────────┐ ┌───────────────────────┐  │
│  │ HTTP (Port 80)     │ │ TLS Proxy (Port 3129)│ │ HTTPS (Port 443)     │  │
│  │ - Caching          │ │ - TLS Termination    │ │ - TCP Passthrough    │  │
│  │ - Range Merging    │ │ - Caching            │ │ - No Caching         │  │
│  │ - Streaming        │ │ - Range Merging      │ │ - Direct to S3       │  │
│  └────────┬───────────┘ └──────────┬──────────┘ └───────────┬───────────┘  │
│           │                        │                        │              │
│           └────────────┬───────────┘                        │              │
│                        ▼                                    │              │
│           ┌───────────────────────────┐                     │              │
│           │        RAM Cache          │                     │              │
│           │   - Metadata + ranges     │                     │              │
│           │   - Compression           │                     │              │
│           │   - Eviction              │                     │              │
│           └─────────────┬─────────────┘                     │              │
└─────────────────────────┼───────────────────────────────────┼──────────────┘
                 │                                │
                 ▼                                │
┌───────────────────────────────┐                 │
│   Shared Disk Cache (NFS)     │                 │
│   - Metadata + ranges         │                 │
│   - Compression               │                 │
│   - LRU/TinyLFU-like eviction │                 │
│   - Fixed or elastic size     │                 │
│   - Journaled writes          │                 │
└───────────────┬───────────────┘                 │
                │                                 │
                └────────────────┬────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │    Amazon S3 (HTTPS)    │
                    └─────────────────────────┘
```

## Module Organization

```
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
│
│   # Journal and consolidation
├── journal_manager.rs   # Per-instance journal file management
├── journal_consolidator.rs # Background consolidation + size tracking + eviction
├── hybrid_metadata_writer.rs # Journal-based metadata writes
├── metadata_lock_manager.rs # Lock management for shared storage
├── cache_hit_update_buffer.rs # RAM buffer for cache-hit metadata updates
│
│   # Recovery
├── background_recovery.rs # Background orphan detection and prioritized recovery
├── orphaned_range_recovery.rs # Scan and recover range files missing from metadata
│
│   # HTTP proxy
├── http_proxy.rs        # HTTP proxy with streaming
├── inflight_tracker.rs  # Download coordination: coalesces concurrent cache misses
├── signed_put_handler.rs # Signed PUT, multipart upload handling and caching
├── signed_request_proxy.rs # SigV4 request forwarding and TLS connection management
├── presigned_url.rs     # Presigned URL parsing and expiration checking
├── aws_chunked_decoder.rs # AWS chunked transfer encoding decode/encode
├── s3_client.rs         # S3 client wrapper with streaming
├── tee_stream.rs        # TeeStream for simultaneous streaming/caching
├── range_handler.rs     # Range request parsing and merging
│
│   # HTTPS / TCP proxy
├── https_proxy.rs       # HTTPS proxy server (TCP passthrough mode)
├── https_connector.rs   # Custom HTTPS connector with connection pool integration
├── tcp_proxy.rs         # TCP tunneling with SNI extraction and IP load balancing
│
│   # Networking and compression
├── compression.rs       # LZ4 compression with content-aware detection
├── connection_pool.rs   # IP distribution, DNS resolution, and health tracking
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
└── shutdown.rs          # Graceful shutdown coordination
```

## Key Design Decisions

### 1. Streaming Response Architecture

**Problem**: Large files (500MB+) caused AWS SDK throughput timeouts when buffering entire response.

**Solution**: TeeStream architecture
- All streaming S3 responses stream directly to client via TeeStream
- Data simultaneously sent to background task for caching
- No buffering of entire response in memory

**Implementation**:
```rust
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Bytes>,
    bytes_sent: usize,
    // plus backpressure and idle-watchdog state — see src/tee_stream.rs
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
- Eliminates timeout issues
- Constant memory usage (64KB chunks)
- Sub-100ms first byte latency
- No cache performance regression

**RAM Cache Integration**: Both streaming and buffered paths check RAM cache before disk I/O. On a RAM hit, the streaming path serves data directly from memory as a buffered response. On a RAM miss, the streaming path collects chunks during disk streaming and promotes the range to RAM cache after completion. Promotion is bounded by **per-shard** capacity (`max_ram_cache_size / effective_shard_count`, 64 MiB at defaults), not by `max_ram_cache_size` as a whole — see [CONFIGURATION.md — RAM Sizing and the Admission Ceiling](CONFIGURATION.md#ram-sizing-and-the-admission-ceiling).

### 2. Bucket-First Hash-Based Sharding

**Problem**: Flat directory structure doesn't scale beyond 10K files per directory.

**Solution**: Bucket-first hash-based sharding with BLAKE3
```
cache_dir/
├── metadata/{bucket}/{XX}/{YYY}/
└── ranges/{bucket}/{XX}/{YYY}/
```

**Why BLAKE3**:
- Cryptographically secure (prevents collision attacks)
- Excellent distribution properties
- Native Rust implementation

**Implementation**:
```rust
fn get_sharded_path(base_dir: &Path, cache_key: &str, suffix: &str) -> Result<PathBuf> {
    let (bucket, object_key) = parse_cache_key(cache_key)?;
    let hash = blake3::hash(object_key.as_bytes());
    let hash_hex = hash.to_hex();
    
    let level1 = &hash_hex[0..2];   // 256 directories
    let level2 = &hash_hex[2..5];   // 4,096 subdirectories
    
    Ok(base_dir.join(bucket).join(level1).join(level2).join(filename))
}
```

**Capacity**:
- 1,048,576 leaf directories per bucket (256 × 4,096)
- 10.5B files per bucket maximum (10K files/directory)
- 2.6B files per bucket optimal (40% safety margin)
- Unlimited buckets

### 3. Multi-Tier Caching Strategy

**RAM Cache (TinyLFU)**:
- Range data from both streaming and buffered paths
- Sub-millisecond response times
- Frequency-based eviction
- Configurable size limits
- Streaming path checks RAM before disk, promotes to RAM after disk hits
- Sharded into `ram_cache_shard_count` independent `tokio::sync::RwLock` partitions (default 8), keyed by `blake3(cache_key) % shard_count`, so concurrent reads of different keys do not contend
- Stores data as `Arc<Bytes>` for O(1) zero-copy reads; the shard lock is released before decompression and response construction
- Access counters (`last_accessed`, `access_count`, hit/miss) are atomics updated under a shared read lock; LRU/TinyLFU reorder is deferred to the next `put()`
- See [CACHING.md → RAM Cache Concurrency Model](CACHE_INTERNALS.md#ram-cache-concurrency-model)

**Disk Cache (LZ4 Compressed)**:
- All object data stored as ranges
- Content-aware compression
- Streaming read/write support
- TTL-based expiration

**Cache Coordination**:
- File locking for multi-instance coordination
- Atomic cache operations
- Consistent metadata management

### 3a. Range Read-Path Map

`http_proxy.rs` and `cache.rs` each contain several similarly-named range read
paths that differ in exactly the details that matter for cache-tier
participation. Which tier a path reaches is not visible at its call site: RAM
access happens one level down, inside `CacheManager`, so searching a file for
`ram_cache` understates its RAM participation. This map records, per entry
point, what actually happens.

| Entry point | Consults RAM? | Promotes to RAM? | RAM key | Reserves against the in-flight ledger? |
|---|---|---|---|---|
| `serve_range_from_cache` → RAM pre-check hit (`http_proxy.rs`) | Yes — exact-key lookup before the streaming decision | n/a (already resident) | `generate_ram_range_key` (colon grammar) | **No** |
| `serve_range_from_cache` (streaming, `http_proxy.rs`) | Only via the buffered/RAM pre-check before the streaming decision | On disk-hit collection during streaming, via the frame-verbatim path | `generate_ram_range_key` (colon grammar) | **No** — bounded by its 4-slot frame channel instead |
| `serve_range_from_cache_buffered` (`http_proxy.rs`) | Yes — `get_cached_range_data` → single-range branch checks RAM directly | Yes, on disk hit | `generate_ram_range_key` (colon grammar) | **Yes** — the requested range length, held by `PermitBody` until the client drains or disconnects |
| `serve_full_object_from_cache` (`http_proxy.rs`) | Yes, via `get_cached_range_data` | Yes, on disk hit | `generate_ram_range_key` (colon grammar) | **Yes** — the full object length, held by `PermitBody` |
| `get_cached_range_data` → single-range branch | Yes | Yes, on disk hit | `generate_ram_range_key` (colon grammar) | Only its own recovery fetch, scoped to that fetch |
| `get_cached_range_data` → merge branch → `RangeHandler::merge_range_segments` → `extract_bytes_from_cached_range` → `CacheManager::load_range_data_with_cache` | Yes, **per cached segment** | Yes, per segment on disk hit | `generate_ram_range_key` (colon grammar), keyed on each segment's stored-range bounds | Only its own fallback fetch, scoped to that fetch |
| `fill_page` (page mode) → `load_page_from_cache` | Yes — page-keyed RAM lookup (`get_range_from_ram_cache` with page bounds) | Yes, on disk hit only — never on the cold S3-fetch fill | `generate_ram_range_key` (colon grammar), keyed on page bounds, not the client's sub-range | **No** |

Notes:
- **A request covering an object's entire byte range does not reach the buffered
  path.** The RAM lookup is keyed on the exact `(cache_key, start, end)` triple,
  and caching a full object registers it under the whole-object range key. So
  `Range: bytes=0-<size-1>` is an exact-key RAM hit: served with `X-Cache: HIT`
  and no ledger reservation. Only a partial range the RAM tier has not seen
  reaches `serve_range_from_cache_buffered`.
- The two reserving paths hold their claim on the response body, so it lasts until
  the body is delivered or the client disconnects. Every other reservation in this
  table is scoped to an upstream fetch. See `docs/METRICS.md` → `inflight_memory`
  when sizing a ceiling.
- All four paths share the **same RAM key grammar** (`generate_ram_range_key`,
  colon-separated) via the single-source-of-truth helper next to
  `generate_range_cache_key` in `cache.rs`. This is distinct from the
  disk-cache key grammar (`generate_range_cache_key`, hyphen-separated).
- The merge branch's RAM participation is easy to miss because
  `merge_range_segments` itself never mentions RAM — the tier access is
  inside `extract_bytes_from_cached_range`, one call down.
- See [CACHING.md](CACHING.md) for the maintained description of what gets
  cached and why; this map is about *which code path* reaches which tier, not
  about cache semantics.

### 4. Stateless Instance Architecture

**Problem**: Traditional distributed caches require cluster membership, leader election, or inter-node communication, adding operational complexity.

**Solution**: Coordination exclusively through shared storage
- Instances have no knowledge of each other
- All coordination via file-based locking on shared volume
- No cluster configuration, membership protocols, or network discovery

**Benefits**:
- **Ephemeral Nodes**: Instances can be added, removed, or replaced at any time without affecting other instances
- **Simple Operations**: No cluster reconfiguration when scaling up/down
- **Fault Isolation**: One instance failure has no impact on others
- **Easy Recovery**: Replace failed instance by starting a new one pointing to same shared storage

**Coordination Mechanisms**:
- File locks for write coordination (metadata updates, eviction)
- Per-instance journal files for cache-hit updates (no write conflicts)
- Distributed eviction lock prevents over-eviction during scale events
- Journal consolidator merges updates from all instances asynchronously

**Deployment Model**:
```
Instance 1 ──┐
Instance 2 ──┼── Shared Storage (NFS)
Instance N ──┘    ├── metadata/
                  ├── ranges/
                  ├── journals/
                  └── locks/
```

Each instance operates independently - the shared storage is the only integration point.

### 5. Buffered Logging and Accumulator-Based Size Tracking

**Problem**: Direct disk writes for access logs cause high I/O contention on shared NFS filesystems, especially with multiple proxy instances. Journal-based size tracking suffered from timing gaps between when data is written and when size is counted.

**Solution**: 
- Access logs buffered in memory, flushed every 5 seconds or 1000 entries
- In-memory `AtomicI64` accumulator tracks size at write/eviction time with zero NFS overhead
- Per-instance delta files flushed periodically, summed by consolidator under global lock

**Implementation**:
```
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
│  │    - Resets delta files to zero                             ││
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
- Each instance flushes accumulated delta to per-instance file every 5 seconds
- Consolidator sums all delta files under global lock, updates `size_state.json`
- Journal entries processed for metadata updates only (not size tracking)
- Eviction triggered automatically when cache exceeds capacity

**Performance Impact**:
- Disk writes reduced by ~1000x (from per-request to periodic batch)
- NFS contention eliminated (per-instance files, no shared file writes)
- Maximum data loss on crash: 5 seconds of logs and size deltas

**Recovery**:
- On startup, consolidator loads `size_state.json`
- If missing, starts at 0 and the periodic validation scan will correct
- In-memory accumulator starts at zero; delta files are summed on next consolidation cycle

**Periodic validation scan**: A background scan (once daily at midnight local time, plus up to an hour of jitter) walks cached metadata to reconcile tracked size with actual disk usage. The scan automatically switches between *full* mode (all L1 shard directories in parallel) and *rolling* mode (subset per cycle, resumed from a persistent cursor) based on observed duration vs. the `validation_max_duration` budget (default 4h). Large caches that exceed the budget run in rolling mode and achieve full coverage over multiple daily cycles. See [CONFIGURATION.md - Validation Scan](CONFIGURATION.md#validation-scan).

## Request Flow

### GET Request Processing

1. **Request Validation**: Parse and validate incoming request
2. **Cache Lookup**: Check RAM cache for metadata
3. **Range Analysis**: Determine required byte ranges
4. **Cache Hit Path**: Serve from cache if available
5. **Download Coordination**: On cache miss, coalesce with any in-flight fetch for the same resource
6. **Cache Miss Path**: Forward to S3, stream response
7. **Background Caching**: Store response data asynchronously
8. **Response Assembly**: Merge ranges if needed

### PUT Request Processing

1. **Request Forwarding**: Stream request body to S3
2. **Response Capture**: Capture S3 response headers
3. **Cache Storage**: Store object data and metadata
4. **Cache Invalidation**: Remove conflicting cache entries
5. **Response Return**: Return S3 response to client

### Multipart Upload Processing

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
6. **Graceful Degradation**: On ETag mismatch or missing parts, skip caching and cleanup
7. **Response Return**: Return S3 response to client

**Multi-Instance Safety**:
- Each upload isolated in `mpus_in_progress/{upload_id}/` directory
- Missing parts trigger cleanup without affecting other uploads
- Prevents incomplete cache entries that could serve corrupted data

For the full multipart upload state machine, correctness gates, concurrency semantics, and threat model see [MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md).

## Performance Characteristics

### Latency
- **RAM Cache Hit**: < 1ms
- **Disk Cache Hit**: 5-50ms (depending on size)
- **Cache Miss**: S3 latency + minimal overhead

### Throughput
- **Streaming**: No throughput degradation for large files
- **Compression**: LZ4 on every cache write, with minimal CPU overhead. Space savings depend entirely on content — highly compressible payloads see large reductions, while already-compressed formats are written store-mode at roughly 1:1 (see [COMPRESSION.md](COMPRESSION.md))
- **Connection Pooling**: Reduced connection establishment overhead

### Scalability
- **Multi-Instance**: Shared cache coordination via file locking
- **Cache Size**: Supports petabyte-scale caches
- **Concurrent Requests**: Configurable request limits

## Security Considerations

> **Your Responsibility**: You are responsible for securing both network access to the proxy and file system access to the shared cache storage. Any client that can reach the proxy over the network can read any cached object with TTL > 0, because cache hits are served without contacting S3. Similarly, anyone with access to the cache storage volume can read cached data directly. Restrict access those clients and systems authorized to access the cached data.

### Network Security Requirements

**HTTP Traffic is Unencrypted**: Communication between clients and the HTTP listener (port 80) uses plaintext HTTP. This applies to both DNS-routed traffic and forward proxy traffic (`HTTP_PROXY=http://proxy:80`). This means:

- **Trusted Network Required**: Deploy only in secured network environments (VPCs, internal networks, isolated subnets)
- **Data in Transit**: S3 data flows unencrypted between client and proxy on port 80. By default the proxy connects to the upstream store over verified TLS on port 443; this is the secure default and applies unless you configure a per-destination [upstream transport override](CONFIGURATION.md#upstream-transport-overrides). The protection-waiving overrides (plaintext HTTP, or HTTPS with certificate validation disabled) are intended for local development or trusted networks only.
- **Network Controls**: Use security groups, firewalls, or network segmentation to restrict proxy access to authorized clients only
- **Encrypted Alternative**: The TLS proxy listener (port 3129) terminates TLS using the proxy's own certificate, providing encrypted client-to-proxy traffic with full caching. Clients use `HTTP_PROXY=https://proxy:3129` with `--endpoint-url http://s3.region.amazonaws.com`. See [Getting Started](GETTING_STARTED.md) for configuration details.
- **Encrypting the hop across a fleet**: a single `HTTP_PROXY` URL reaches one instance, so a multi-instance fleet needs something in front of it to select a member. A load balancer that re-encrypts to the TLS listener is one way ([AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md#load-balancer-encrypting-the-client-hop)); a self-managed L7 router is another, and can also keep the plaintext hop on loopback by running on the client host itself. See [Request-Aware Routing](REQUEST_AWARE_ROUTING.md).

#### What a Cleartext Hop Exposes

"Deploy only on a trusted network" is the requirement; this is what it protects against, so you can judge whether a given network qualifies. SigV4 authenticates the request — it proves the request was not forged and cannot be redirected to a different object — but it provides **no confidentiality**. Anything travelling in cleartext is readable by any party that can observe the traffic.

An observer on a cleartext hop captures:

- **The bucket and object key**, from the request line and `Host` header. Over time this reveals your bucket contents, object sizes, and access patterns.
- **The `Authorization` header**, which contains the caller's **access key ID** (in the `Credential` scope), the `SignedHeaders` list, and the signature.
- **Object payloads in both directions** — GET response bodies and PUT/UploadPart request bodies, in full.
- **Presigned URL query parameters**, when clients use presigned URLs, including `X-Amz-Signature` and `X-Amz-Expires`.

The **secret access key is never transmitted** and cannot be recovered from a captured request.

What an observer can then do:

- **Replay the captured request verbatim against S3.** The signature covers the request, not the channel, so a captured signed request is valid until its timestamp ages out — [AWS documents a 15-minute window](https://repost.aws/it/questions/QUwKI3rzGOSgO2fsFrOdPwYw/problem-with-recreation-of-aws-signature), and notes that a party holding a signed request can also alter its *unsigned* portions without invalidating it. A replayed GET returns the object; a replayed PUT rewrites the same content.
- **Replay a captured presigned URL for its full remaining validity**, which may be hours or days rather than 15 minutes. This is the longer-lived exposure of the two.
- **Not reach a different object.** Method, path, query, and the signed headers are all covered by the signature, so a captured request cannot be edited to read another key without the secret access key.

**Which hops are affected.** Cleartext applies to a hop only where the traffic crosses a network; the same listener on loopback exposes nothing to the network:

| Hop | Cleartext? |
|---|---|
| Client → HTTP listener (`:80`) or forward proxy (`:3128`) across a network | **yes** |
| Client → either listener on loopback (`127.0.0.1`, sidecar/local-dev) | no — never leaves the host |
| Load balancer → proxy, where the LB terminates TLS ([Pattern 1](GETTING_STARTED.md#pattern-1-load-balancer-terminates-tls--proxy-serves-http)) | **yes** — encrypted client→LB, cleartext LB→proxy |
| Client → TLS proxy listener (`:3129`), directly or via LB TCP passthrough | no |
| Load balancer → proxy, where the LB re-encrypts to `:3129` ([Pattern 3](GETTING_STARTED.md#pattern-3-load-balancer-terminates-and-re-encrypts--proxy-terminates-tls)) | no |
| Proxy → origin, default | no — verified TLS on 443 |
| Proxy → origin, with `scheme: http` in [`upstream_overrides`](CONFIGURATION.md#upstream-transport-overrides) | **yes** |
| Proxy → origin, with `validate_tls: false` | encrypted, but no MITM protection — an interceptor can read and alter it |
| Client → HTTPS listener (`:443`, TCP passthrough) | no — opaque bytes, and uncacheable for the same reason |

Every affected hop has an encrypted alternative that preserves caching. Where a hop must stay in cleartext, treat every client, load balancer, and host on that network segment as able to read all S3 traffic passing over it and to replay it within the windows above — and size the network controls accordingly.

**TLS Certificate Management**: When TLS is enabled, the proxy loads a certificate and private key from paths specified in the config. For multi-instance deployments with shared storage, store the certificate and key on the shared volume alongside the configuration (e.g., `/mnt/nfs/config/tls/cert.pem` and `/mnt/nfs/config/tls/key.pem`) so all instances use the same certificate. Restrict file permissions on the private key (`chmod 600`). The certificate's Subject Alternative Names (SANs) must match how clients connect — use `IP:` SANs for direct IP connections, or `DNS:` SANs when clients connect through a load balancer or DNS name.

### Shared Cache Access Model

The proxy is a shared cache. It does not authenticate clients or authorize requests — S3 handles both, but only when requests reach S3. With TTL > 0, cache hits bypass S3 entirely.

**What this means in practice**:
- Any client with network access to the proxy can read any cached object until its TTL expires
- A user whose S3 access was revoked can still retrieve cached data until TTL expires
- Different users share the same cached responses — there is no per-user isolation

**This is the same security model as any shared cache** (CDN edge cache, Mountpoint for Amazon S3's local cache, or a shared NFS export of downloaded files). The proxy does not weaken S3's security — it requires you to control who can access the cache, just as you would for any local copy of S3 data.

**Two access paths to secure**:
1. **Network access to the proxy** — restrict to authorized clients using security groups, firewalls, or network segmentation
2. **File system access to the cache volume** — restrict to authorized proxy instances only, using file permissions and mount controls

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

### Request Validation Limitations

**Bucket Owner Validation Not Supported**: The `x-amz-expected-bucket-owner` header cannot be validated for cached responses:

- **S3 Does Not Return Owner**: GetObject and HeadObject responses do not include bucket owner information
- **No Owner Stored in Cache**: Since owner is never received, it cannot be stored or validated
- **Cached Responses Bypass Validation**: Requests with `x-amz-expected-bucket-owner` served from cache will succeed regardless of the header value
- **Cache Miss Behavior**: Only on cache miss does S3 validate the header (and return 403 if mismatched)

This is an inherent limitation of the S3 API design, not a proxy implementation choice.

### Data Confidentiality

**Cache Storage**: Cached data is stored on disk with LZ4 compression but no encryption:

- **Plaintext Storage**: Object data stored in readable format (compressed but not encrypted)
- **Metadata Exposure**: Object keys, sizes, and access patterns visible in cache metadata
- **File System Access**: Anyone with file system access can read cached data directly
- **Encryption at Rest**: Use storage-level encryption if encryption at rest is required

### Trust and Integrity Model

Separate from the access-control model above, the proxy's **data integrity model** describes what it verifies about cached bytes, what it trusts upstream components to guarantee, and what's explicitly out of scope.

**What the proxy trusts**:
- **S3 validates bytes on ingress.** Every PUT and UploadPart is forwarded unmodified to S3. If S3 returns 200, its stored bytes match what the client sent (plus S3's own checksum of record — default CRC64NVME since December 2024).
- **LZ4 frame content checksums catch disk corruption.** All cached bytes — compressed or uncompressed-frame-wrapped — carry an xxhash32 frame checksum (see [COMPRESSION.md](COMPRESSION.md#integrity-every-write-is-a-checksummed-lz4-frame)). Bit-flips surface as decode errors and are handled as cache misses.
- **The storage layer catches lower-level corruption.** Filesystem and block-device integrity (EFS, ext4, XFS) cover everything below the frame checksum.

**What the proxy verifies**:
- **ETag equality for read invalidation.** On conditional revalidation and range reads, mismatched ETags invalidate cached entries (see `invalidate_stale_ranges`).
- **ETag equality at multipart finalization.** `CompleteMultipartUpload` request body ETags per part are compared against the proxy's recorded per-part ETags. On any mismatch the cache finalization is skipped and the `mpus_in_progress/{uploadId}/` directory is cleaned up — the client still gets S3's success response, the cache just doesn't retain the object. Covered in [MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md).
- **aws-chunked decoded length.** When the `x-amz-decoded-content-length` header is present, the decoder verifies the decoded body length matches and skips caching on mismatch (see `aws_chunked_decoder`).
- **Cache metadata structural consistency.** The `cache_validator` module checks metadata JSON parses, ranges don't overlap, range sizes match their offsets, and referenced range files exist.
- **Cache keys are validated to stay within the cache directory.** `parse_cache_key` rejects bucket segments equal to `.`, `..`, empty, or containing `/`, `\`, NUL, or ASCII control characters (0x00–0x1F, 0x7F). This ensures every sharded path produced by `get_sharded_path` is a descendant of the configured `cache_dir`, defending against path-traversal input from the HTTP listener.
- **Host headers are validated against RFC 7230 / RFC 3986.** `parse_host_header` accepts bracketed IPv6 literals (`[::1]`, `[::1]:8081`, `[2001:db8::1]:443`), plain hostnames, and IPv4, and rejects unclosed brackets, stray `]`, unbracketed IPv6, invalid ports, and empty values with a 400 response. This closes a correctness gap that previously made IPv6 clients unusable and produced nonsensical cache keys.

**Cache staleness is not a security boundary.** Any client with valid S3 credentials can write directly to a bucket, bypassing the proxy entirely. When that happens, cached entries for affected objects become stale until the next read triggers ETag-based revalidation. This is normal cache behaviour, not an attack scenario — the mitigations are the same as any read-through cache: tune TTLs, use TTL=0 for per-request authorization, or configure clients to always go through the proxy.

**Residual integrity gap**:
For a motivated attacker who can both (a) intercept a client's multipart upload in flight and substitute one part via a direct-to-S3 UploadPart call, and (b) produce an MD5 collision for that part, the cache could retain bytes that no longer match what S3 stored. This gap exists because per-part ETags on SSE-S3 single-part data are MD5 of the content, and MD5 is not collision-resistant. The same attacker could confuse any client or tool relying on ETag matching for integrity — this is not a proxy-specific weakness.

**Note the precondition.** Step (a) requires an actor who can call `UploadPart` against
that upload — meaning one who already holds valid S3 write credentials for the bucket.
As stated above, such an actor can write to the bucket directly regardless of the proxy,
which is outside the threat model. Weigh any mitigation against that: it is bought only
for the narrow case where an authorized writer wants the cache to diverge from S3,
having also defeated MD5.

Mitigation at the bucket/client layer (not the proxy):
- **Request a cryptographic checksum at `CreateMultipartUpload`** (e.g., `--checksum-algorithm SHA256`). Per-part checksums then land in the `CompleteMultipartUpload` request body and S3 verifies them end-to-end. Note that uploads are not unchecksummed by default — current AWS SDKs and CLI v2 apply CRC64NVME automatically, and S3 stores it as the default checksum. CRC64NVME detects corruption in transit but is not collision-resistant, so it does not defend against an attacker who controls the content; SHA256 does.

### Deployment Guidelines

**Before deploying, ensure**:
1. Network access to the proxy is restricted to clients authorized to access all objects that may be cached
2. File system access to the cache volume is restricted to authorized proxy instances
3. TTL values reflect your tolerance for serving stale data after access revocation (use TTL=0 for per-request authorization)

**Appropriate Use Cases**:
- Internal networks where all clients are authorized to access the same S3 data
- Development and testing environments
- Single-tenant deployments with controlled network access
- On-premises environments with network segmentation between trust zones

**Not Recommended For**:
- Multi-tenant environments where clients should not see each other's data
- Public-facing or untrusted network deployments
- Environments requiring per-object access control between clients (unless using TTL=0)

### Destination Policy (SSRF Protection)

The proxy includes a destination policy engine that prevents SSRF attacks by validating upstream destinations before forwarding. When enabled, it blocks requests to prohibited IP ranges (IMDS 169.254.169.254, loopback, private networks, link-local).

**Coverage**: The policy applies to all forwarding paths:
- **HTTPS (CONNECT/SNI)**: Port 443 enforcement, hostname allowlist, IP classification
- **TLS proxy listener**: Same checks as HTTPS path
- **HTTP forwarding path**: IP classification with configurable port, hostname allowlist

**Gating**: The HTTP-path destination policy is active when `connect_allowlist` is configured in the TLS section of the config. Without an allowlist, the HTTP path skips the policy check — this allows operators who rely on the proxy as a general forward proxy to opt out.

**IP ranges blocked** (all paths):
- `0.0.0.0/8`, `127.0.0.0/8` (loopback), `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` (private)
- `169.254.0.0/16` (link-local / IMDS)
- IPv6 equivalents: `::`, `::1`, `fe80::/10`, `fc00::/7`, `::ffff:` IPv4-mapped

**Endpoint override carve-out**: IPs listed in `connection_pool.endpoint_overrides` are exempted from the prohibited-range check, allowing PrivateLink ENIs in private address space.

**S3 endpoints**: All public S3 IPs pass the policy. The check has no impact on normal S3 traffic.

## Observability

### Dashboard

A lightweight web dashboard (port 8081, configurable) provides real-time cache statistics, application log viewer, and system information. The dashboard is **unauthenticated and read-only** — it exposes no write operations or credentials. The code default bind address is `127.0.0.1` (localhost only); binding to `0.0.0.0` requires network-layer access restriction (security groups, firewall). See [DASHBOARD.md](DASHBOARD.md) for configuration and security guidance.

### Metrics
- Cache hit/miss rates
- Response times
- Throughput statistics
- Error rates
- Resource utilization

### Logging
- S3-compatible access logs
- Structured application logs
- Performance metrics
- Error tracking

### Health Checks
- HTTP health endpoint
- Cache system status
- S3 connectivity validation
- Resource availability checks
