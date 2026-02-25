# S3 Proxy Architecture

Technical architecture overview and design principles for S3 Proxy.

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
- Large responses (> 1MB) stream directly to client
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
    ┌─────────────────────┐              ┌─────────────────────┐
    │   S3 Client (HTTP)  │              │  S3 Client (HTTPS)  │
    │   - AWS CLI         │              │   - AWS CLI         │
    │   - S3 SDK          │              │   - S3 SDK          │
    └──────────┬──────────┘              └──────────┬──────────┘
               │                                    │
               ▼                                    ▼
┌──────────────────────────────────────────────────────────────────┐
│                       S3 Proxy (1..N)                            │
│  ┌───────────────────────────┐    ┌───────────────────────────┐  │
│  │   HTTP Handler (Port 80)  │    │  HTTPS Handler (Port 443) │  │
│  │   - Caching               │    │   - TCP Passthrough       │  │
│  │   - Range Merging         │    │   - No Caching            │  │
│  │   - Streaming             │    │   - Direct to S3          │  │
│  └─────────────┬─────────────┘    └─────────────┬─────────────┘  │
│                │                                │                │
│                ▼                                │                │
│  ┌───────────────────────────┐                  │                │
│  │        RAM Cache          │                  │                │
│  │   - Metadata + ranges     │                  │                │
│  │   - Compression           │                  │                │
│  │   - Eviction              │                  │                │
│  └─────────────┬─────────────┘                  │                │
└────────────────┼────────────────────────────────┼────────────────┘
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
├── cache_writer.rs      # Async cache writer for streaming
├── cache_size_tracker.rs # Cache size tracking (delegates to consolidator)
├── cache_initialization_coordinator.rs # Coordinated cache initialization
├── cache_validator.rs   # Cache integrity validation and file scanning
├── capacity_manager.rs  # Cache capacity checks and bypass decisions
├── disk_cache.rs        # Disk cache with streaming support
├── ram_cache.rs         # RAM cache with TinyLFU for range data
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
├── connection_pool.rs   # Connection pooling and load balancing
│
│   # Observability
├── logging.rs           # Access and application logging
├── log_sampler.rs       # Rate-based log sampling for high-frequency operations
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
- Responses > 1MB stream directly to client
- Data simultaneously sent to background task for caching
- No buffering of entire response in memory

**Implementation**:
```rust
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Result<Bytes>>,
}

impl<S: Stream<Item = Result<Bytes>>> Stream for TeeStream<S> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                // Send to cache writer (non-blocking)
                let _ = self.sender.try_send(Ok(chunk.clone()));
                Poll::Ready(Some(Ok(chunk)))
            }
            // ... error handling
        }
    }
}
```

**Benefits**:
- Eliminates timeout issues
- Constant memory usage (64KB chunks)
- Sub-100ms first byte latency
- No cache performance regression

**RAM Cache Integration**: Both streaming and buffered paths check RAM cache before disk I/O. On a RAM hit, the streaming path serves data directly from memory as a buffered response. On a RAM miss, the streaming path collects chunks during disk streaming and promotes the range to RAM cache after completion (skipping promotion for ranges exceeding `max_ram_cache_size`).

### 2. Bucket-First Hash-Based Sharding

**Problem**: Flat directory structure doesn't scale beyond 10K files per directory.

**Solution**: Bucket-first hash-based sharding with BLAKE3
```
cache_dir/
├── metadata/{bucket}/{XX}/{YYY}/
└── ranges/{bucket}/{XX}/{YYY}/
```

**Why BLAKE3**:
- 10x faster than SHA-256
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

**Disk Cache (LZ4 Compressed)**:
- All object data stored as ranges
- Content-aware compression
- Streaming read/write support
- TTL-based expiration

**Cache Coordination**:
- File locking for multi-instance coordination
- Atomic cache operations
- Consistent metadata management

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
│                         S3 Proxy Instance                       │
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
│  │    - Flushes accumulator to delta_{instance_id}.json        ││
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
│    └── delta_{instance_id}.json   ← Per-instance delta files    │
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
- If missing, starts at 0 and daily validation will correct
- In-memory accumulator starts at zero; delta files are summed on next consolidation cycle

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

## Performance Characteristics

### Latency
- **RAM Cache Hit**: < 1ms
- **Disk Cache Hit**: 5-50ms (depending on size)
- **Cache Miss**: S3 latency + minimal overhead

### Throughput
- **Streaming**: No throughput degradation for large files
- **Compression**: 2-10x space savings with minimal CPU overhead
- **Connection Pooling**: Reduced connection establishment overhead

### Scalability
- **Multi-Instance**: Shared cache coordination via file locking
- **Cache Size**: Supports petabyte-scale caches
- **Concurrent Requests**: Configurable request limits

## Security Considerations

### Network Security Requirements

**HTTP Traffic is Unencrypted**: All communication between clients and the proxy uses HTTP (port 80) for caching functionality. This means:

- **Trusted Network Required**: Deploy only in secured network environments (VPCs, internal networks, isolated subnets)
- **Data in Transit**: All S3 data flows unencrypted between client and proxy
- **Not Suitable for**: Public networks, untrusted environments, or multi-tenant scenarios with sensitive data

### Shared Cache Access Model

**No Authentication or Authorization**: The proxy does not authenticate clients or authorize requests. It operates as a shared cache without access control:

- **Authentication (AuthN)**: The proxy does not verify client identity. It forwards the client's `Authorization` header to S3, but only on cache misses or revalidation.
- **Authorization (AuthZ)**: The proxy does not check permissions. S3 performs authorization checks, but only when requests reach S3.
- **With TTL > 0**: Cache hits bypass S3 entirely. A user whose access was revoked can still retrieve cached data until TTL expires. Different users share the same cached responses.

- **Any Client Can Read Any Cached Data**: All clients with network access can retrieve any cached object until its TTL expires (authentication and authorization are bypassed for cache hits)
- **Cross-Client Data Exposure**: In multi-tenant environments, one client's cached data is accessible to all other clients
- **Persistent Cache**: Cached data remains accessible across client sessions and proxy restarts until expiration

**Mitigation - Always-Revalidate Mode (TTL=0)**: For environments requiring per-request authentication and authorization enforcement, TTL values can be set to zero:

```yaml
cache:
  get_ttl: "0s"
  head_ttl: "0s"
  actively_remove_cached_data: false  # Required - must use lazy expiration
```

**Behavior with TTL=0**:
- Cache still stores data (useful for range merging, bandwidth savings)
- Every request triggers S3 revalidation via conditional requests (`If-Modified-Since`)
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

### Deployment Guidelines

**Appropriate Use Cases**:
- Internal corporate networks with trusted clients
- Development and testing environments
- Single-tenant deployments with controlled access
- Scenarios where performance outweighs confidentiality concerns

**Not Recommended For**:
- Multi-tenant SaaS environments
- Public-facing deployments
- Environments with strict data isolation requirements
- Scenarios involving sensitive or regulated data (unless network is properly secured)

## Observability

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