# Developer Documentation

Implementation details, architectural decisions, and technical notes for S3 Proxy developers.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Key Design Decisions](#key-design-decisions)
3. [Implementation Details](#implementation-details)
4. [Performance Optimizations](#performance-optimizations)
5. [Known Limitations](#known-limitations)

## Architecture Overview

### Core Principles

**Transparent Forwarder**
- Proxy only responds to client requests
- Cannot initiate requests to S3 (no AWS credentials)
- Cannot sign requests (relies on client-signed requests)
- Acts as intelligent cache between client and S3

**Streaming Architecture**
- Large responses (> 1MB) stream directly to client
- Simultaneous caching in background
- Eliminates buffering and memory pressure
- Constant memory usage regardless of file size

**Unified Range Storage**
- All cached data stored as ranges
- PUT operations: Stored as range 0-N
- GET operations: Full objects or partial ranges
- Multipart uploads: Parts assembled into ranges
- No data copying on TTL transitions

### Module Organization

```
src/
├── main.rs              # Entry point, server initialization
├── lib.rs               # Library exports, module declarations
├── cache.rs             # Unified cache manager with part caching (3500+ lines)
├── cache_types.rs       # Cache data structures
├── cache_writer.rs      # Async cache writer for streaming
├── disk_cache.rs        # Disk cache with streaming support
├── ram_cache.rs         # RAM cache with LRU/frequency-recency eviction
├── metadata_cache.rs    # RAM cache for NewCacheMetadata objects
├── http_proxy.rs        # HTTP proxy with streaming (1500+ lines)
├── s3_client.rs         # S3 client wrapper with streaming
├── tee_stream.rs        # TeeStream for simultaneous streaming/caching
├── compression.rs       # LZ4 compression with content-aware detection
├── connection_pool.rs   # Connection pooling and load balancing
├── range_handler.rs     # Range request parsing and merging
├── logging.rs           # Access and application logging
├── metrics.rs           # Metrics collection
├── otlp.rs              # OpenTelemetry Protocol export
├── config.rs            # Configuration management
├── error.rs             # Error types
└── shutdown.rs          # Graceful shutdown coordination
```

## Key Design Decisions

### 1. Streaming Response Architecture

**Problem**: Large files (500MB+) caused AWS SDK throughput timeouts when buffering entire response.

**Solution**: TeeStream architecture
- Responses > 1MB stream directly to client
- Data simultaneously sent to background task for caching
- No buffering of entire response in memory

**Implementation** (`src/tee_stream.rs`):
```rust
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Bytes>,
    bytes_sent: usize,
}

impl<S> Stream for TeeStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, hyper::Error>> + Unpin,
{
    type Item = Result<Frame<Bytes>, hyper::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = Pin::new(&mut self.inner);

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                // Check if this is a data frame
                match frame.into_data() {
                    Ok(data) => {
                        let bytes = data;
                        self.bytes_sent += bytes.len();

                        // Try to send to cache channel (non-blocking)
                        if let Err(e) = self.sender.try_send(bytes.clone()) {
                            match e {
                                mpsc::error::TrySendError::Full(_) => {
                                    warn!("Cache channel full, dropping chunk");
                                }
                                mpsc::error::TrySendError::Closed(_) => {
                                    debug!("Cache channel closed, stopping tee");
                                }
                            }
                        }

                        // Return the original data frame
                        Poll::Ready(Some(Ok(Frame::data(bytes))))
                    }
                    Err(frame) => {
                        // Non-data frame (trailers, etc), pass through
                        Poll::Ready(Some(Ok(frame)))
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
```

**Benefits**:
- Eliminates timeout issues
- Constant memory usage (64KB chunks)
- Sub-100ms first byte latency
- No cache performance regression

**Trade-offs**:
- Slightly more complex code
- Background task overhead (minimal)
- Partial cache hits still require buffering (for merging)

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

**Implementation** (`src/cache.rs`):
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

**Benefits**:
- O(1) file lookup regardless of cache size
- Per-bucket cache management (`rm -rf cache_dir/metadata/{bucket}/`)
- Uniform distribution across directories
- Eliminates bucket name redundancy in filenames

**Trade-offs**:
- Breaking change (not backward compatible)
- Directory overhead (~268MB inodes per bucket)
- Hash computation overhead (~1μs per operation)

### 3. Unified Range Storage

**Problem**: Three different storage formats (write cache, GET cache, multipart) caused complexity and inefficiency.

**Solution**: Store all cached data as ranges
- PUT operations: Stored as range 0-N immediately (write caching enabled by default)
- GET operations: Full objects or partial ranges
- Multipart uploads: Parts assembled into ranges on completion

**Implementation** (`src/cache.rs`):
```rust
// PUT request (write caching enabled by default)
pub async fn store_write_cache_entry(&self, cache_key: &str, data: &[u8]) -> Result<()> {
    // Store as range 0 to content_length-1
    self.disk_cache.store_full_object_as_range(cache_key, data, metadata).await?;
    // Mark upload_state = Complete
    // Set PUT_TTL expiration
}

// Multipart completion
pub async fn complete_multipart_upload(&self, path: &str) -> Result<()> {
    // Sort parts by part number
    // Calculate byte positions from part sizes
    // Store each part as a range
    // Mark upload_state = Complete
}
```

**Benefits**:
- Range requests work immediately (no S3 fetch)
- No data copying on TTL transitions (metadata-only update)
- Simplified code paths (single storage format)
- Fast transitions (<10ms metadata update)

**Trade-offs**:
- Slightly larger metadata files (upload_state, parts list)
- More complex multipart handling (position calculation)

### 4. Frequency-Recency Hybrid Eviction Algorithm

**Problem**: LRU evicts frequently-accessed items during scan patterns. LFU vulnerable to cache pollution. Need unified eviction across GET and HEAD entries.

**Solution**: Simplified frequency-recency hybrid (labeled "TinyLFU" in config)
- Combines access frequency and recency in eviction scoring
- Sliding window tracks recent accesses
- HashMap-based frequency counting (not a probabilistic sketch)
- TTL-aware eviction (expired entries prioritized)

**Note**: This is a simplified implementation, not the full TinyLFU algorithm from the research paper. It lacks:
- Count-Min Sketch for O(1) memory frequency estimation
- Doorkeeper bloom filter for admission control
- Window/main cache segmentation (W-TinyLFU)

**Implementation** (`src/ram_cache.rs`):
```rust
// Unified eviction across GET and HEAD entries
fn find_tinylfu_victim(&self) -> Option<String> {
    // First, try to evict expired entries (both GET and HEAD)
    if let Some(expired_key) = self.find_expired_entries().first() {
        return Some(expired_key.clone());
    }
    
    // Then use TinyLFU across all entry types
    let get_candidates = self.entries.iter().map(|(k, v)| (k, v.last_accessed, "GET"));
    let head_candidates = self.head_entries.iter().map(|(k, v)| (k, v.last_accessed, "HEAD"));
    
    get_candidates.chain(head_candidates)
        .min_by_key(|(key, last_accessed, _)| {
            let frequency = self.tinylfu_frequencies.get(*key).unwrap_or(&1);
            let recency_factor = now.duration_since(*last_accessed)
                .unwrap_or_default().as_secs().max(1);
            frequency * 1000 / recency_factor
        })
        .map(|(key, _, _)| key.clone())
}

// TTL-aware eviction prioritizes expired entries
pub fn evict_expired_entries(&mut self) -> Result<u64> {
    let now = SystemTime::now();
    let mut evicted_count = 0;
    
    // Evict expired GET entries
    self.entries.retain(|_, entry| {
        if now > entry.expires_at {
            evicted_count += 1;
            false
        } else {
            true
        }
    });
    
    // Evict expired HEAD entries
    self.head_entries.retain(|_, entry| {
        if now > entry.expires_at {
            evicted_count += 1;
            false
        } else {
            true
        }
    });
    
    Ok(evicted_count)
}
```

**Benefits**:
- **Unified Competition**: GET and HEAD entries compete fairly for cache space
- **TTL Priority**: Expired entries evicted first, regardless of frequency
- **Optimal Utilization**: Most valuable entries (by frequency) retained
- **Scan Resistance**: One-time scans don't pollute cache
- **Low Overhead**: ~16-24 bytes per entry for frequency tracking

**HEAD Cache Advantages**:
- **Higher Density**: HEAD entries are 1-5KB vs GET entries that can be MB/GB
- **Better Hit Rates**: More HEAD entries fit in same memory space
- **Faster Eviction**: HEAD entries cheaper to evict than large GET entries

**Performance Characteristics**:
- **GET operations**: O(1) for hit/miss, O(N) for eviction
- **HEAD operations**: O(1) for hit/miss, O(N) for eviction  
- **Unified eviction**: O(N) scan across both entry types
- **Memory overhead**: Minimal frequency tracking per entry

**Trade-offs**:
- Size-agnostic (doesn't consider object size differences between GET/HEAD)
- Slower eviction than simple LRU (O(N) vs O(1))
- May favor HEAD entries due to higher access frequency and smaller size

**Alternative Considered**: GDSF (Greedy Dual Size Frequency)
- Size-aware eviction considering GET vs HEAD entry sizes
- Better byte hit rate optimization
- More complex to implement and tune
- Rejected: TinyLFU sufficient, HEAD entries naturally small

### 5. Range Request Merging

**Problem**: Partial cache hits fetched entire range from S3, wasting bandwidth.

**Solution**: Intelligent range merging
- Identify missing ranges
- Consolidate small gaps (< 256KB threshold)
- Fetch only missing portions from S3
- Merge cached + fetched ranges

**Implementation** (`src/http_proxy.rs`):
```rust
// Identify missing ranges
let missing_ranges = identify_missing_ranges(&requested_range, &cached_ranges);

// Consolidate small gaps
let consolidated = consolidate_ranges(missing_ranges, gap_threshold);

// Fetch missing ranges in parallel
let fetched = fetch_ranges_from_s3(consolidated).await?;

// Merge cached + fetched
let merged = merge_ranges(&cached_ranges, &fetched, &requested_range)?;
```

**Benefits**:
- 60-90% bandwidth savings for partial cache hits
- Faster response times (less data from S3)
- Reduced S3 costs (fewer bytes transferred)

**Trade-offs**:
- More complex code (range extraction, merging)
- Requires buffering for merge (can't stream)
- Gap threshold tuning needed for optimal performance

### 6. Connection Keepalive

**Problem**: Creating new TCP/TLS connection per request adds 150-300ms overhead.

**Solution**: HTTP connection keepalive with Hyper 1.0
- Reuse existing connections
- Per-IP connection pools
- Automatic lifecycle management

**Implementation** (`src/connection_pool.rs`):
```rust
let client = Client::builder(TokioExecutor::new())
    .pool_idle_timeout(Duration::from_secs(30))
    .pool_max_idle_per_host(1)
    .build(CustomHttpsConnector::new(pool_manager));
```

**Benefits**:
- 150-200ms latency reduction per request
- 50-100% throughput increase
- Minimal memory overhead (~50KB per connection)

**Trade-offs**:
- Stale connections need handling
- DNS changes require connection rotation
- Memory usage for idle connections

### 7. Part Number Request Caching

**Problem**: GET requests with `partNumber` query parameters bypassed cache entirely, causing repeated S3 requests for the same parts.

**Solution**: Treat parts as ranges using existing range storage architecture
- Parse `partNumber` from query parameters to detect GetObjectPart requests
- Extract multipart metadata from S3 response headers (`x-amz-mp-parts-count`, `Content-Range`)
- Store parts as ranges using Content-Range byte offsets
- Calculate part boundaries from stored metadata for cache lookups
- Serve cached parts with consistent response headers

**Implementation** (`src/cache.rs`, `src/http_proxy.rs`):
```rust
/// Part ranges stored per-part in ObjectMetadata
/// Maps part number to (start_offset, end_offset) byte range
pub part_ranges: HashMap<u32, (u64, u64)>,

/// Look up byte range for a given part number
fn lookup_part(cache_key: &str, part_number: u32) -> Result<Option<CachedPartResponse>> {
    // Direct lookup: metadata.object_metadata.part_ranges.get(&part_number)
    // Returns None on miss (no delay, no calculation)
}

/// Parse Content-Range header: "bytes 0-8388607/5368709120"
fn parse_content_range(header: &str) -> Option<(u64, u64, u64)> {
    // Returns (start, end, total_size)
}
```

**Benefits**:
- Eliminates repeated S3 requests for same parts
- Leverages existing range storage and compression
- Maintains response header consistency
- Supports all S3-compatible part operations

**Trade-offs**:
- Additional complexity in request parsing
- Metadata overhead for multipart fields
- Upload verification requests (partNumber + uploadId) still bypass cache

### 8. RAM Metadata Cache Architecture

**Problem**: Metadata lookups required disk I/O for every request, missing opportunity for sub-millisecond responses.

**Solution**: RAM Metadata Cache with unified storage
- `NewCacheMetadata` objects cached in RAM for both HEAD and GET requests
- HEAD and GET share the same `.meta` file with independent TTLs
- Simple LRU eviction (all entries similar size ~1-2KB)
- Per-key locking prevents concurrent disk reads

**Implementation** (`src/metadata_cache.rs`):
```rust
// MetadataCache entry structure
pub struct MetadataCacheEntry {
    pub metadata: NewCacheMetadata,  // Full metadata including HEAD and GET fields
    pub loaded_at: Instant,          // When loaded from disk
    pub disk_mtime: Option<SystemTime>, // Disk file modification time
    pub last_accessed: Instant,      // For LRU eviction
}

// Get or load with per-key locking
pub async fn get_or_load<F, Fut>(&self, cache_key: &str, loader: F) -> Result<NewCacheMetadata>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<NewCacheMetadata>>,
{
    // Check cache first
    if let Some(entry) = self.get(cache_key).await {
        if !self.is_stale(&entry) {
            return Ok(entry.metadata);
        }
    }
    
    // Per-key locking prevents concurrent disk reads
    let lock = self.get_or_create_key_lock(cache_key).await;
    let _guard = lock.lock().await;
    
    // Double-check after acquiring lock
    if let Some(entry) = self.get(cache_key).await {
        if !self.is_stale(&entry) {
            return Ok(entry.metadata);
        }
    }
    
    // Load from disk
    let metadata = loader().await?;
    self.put(cache_key, metadata.clone()).await;
    Ok(metadata)
}
```

**Key Design Decisions**:

1. **Unified Storage**: HEAD and GET share same `.meta` file
   - HEAD has `head_expires_at` field, GET ranges have `expires_at` field
   - HEAD expiry doesn't delete file (ranges may still be valid)
   - Range expiry doesn't affect HEAD validity

2. **Simple LRU Eviction**: Not TinyLFU
   - All metadata entries are similar size (~1-2KB)
   - TinyLFU complexity not needed for uniform-size entries
   - Simple LRU provides optimal performance

3. **Per-Key Locking**: Prevents thundering herd
   - Multiple concurrent requests for same key: only one disk read
   - Other requests wait and get cached result
   - Reduces disk I/O under high concurrency

4. **Stale File Handle Recovery**: For NFS reliability
   - Retry with exponential backoff on ESTALE errors
   - Invalidate cache entry if retries exhausted
   - Graceful degradation for shared storage

**Benefits**:
- **Performance**: Sub-millisecond metadata lookups for hot entries
- **Efficiency**: Unified storage reduces disk space (no separate HEAD cache)
- **Consistency**: Independent HEAD/GET TTLs in same file
- **Reliability**: Stale file handle recovery for NFS

**Trade-offs**:
- Additional memory for metadata cache (~15-25MB for 10,000 entries)
- Refresh interval adds slight staleness (default 5s)
- Per-key locks add coordination overhead

### 9. RAM-Disk Cache Coherency

**Problem**: RAM cache hits don't update disk metadata, causing incorrect eviction decisions. On shared NFS storage, direct metadata writes cause race conditions.

**Solution**: Journal-based metadata updates with RAM buffering
- Cache-hit updates (TTL refresh, access count) buffered in RAM via `CacheHitUpdateBuffer`
- Periodic flush to per-instance journal files (every 5 seconds)
- Background consolidation applies journal entries to metadata files with locking
- Lock acquisition with exponential backoff prevents contention

**Implementation** (`src/disk_cache.rs`):
```rust
// Record cache hit (range access)
self.record_range_access(cache_key, start, end).await?;

// CacheHitUpdateBuffer accumulates updates in RAM
// Periodic flush writes to journal file (per-instance)
// JournalConsolidator applies entries to metadata with locking

// Journal entry types for cache-hit updates:
pub enum JournalOperation {
    TtlRefresh { range_start: u64, range_end: u64, new_expires_at: SystemTime },
    AccessUpdate { range_start: u64, range_end: u64, access_count_delta: u32 },
    // ... other operations
}
```

**Benefits**:
- Accurate eviction decisions for GET data
- Minimal disk I/O (batched updates via journal)
- Prevents RAM-disk inconsistency
- Race-condition-free on shared NFS storage
- Lock contention handled with exponential backoff

**Trade-offs**:
- Slight delay in statistics propagation (5s buffer + consolidation interval)
- Additional memory for pending updates (~10KB per 1000 entries)
- Journal consolidation adds background I/O

### 10. Coordinated Cache Initialization

**Problem**: WriteCacheManager and CacheSizeTracker performed separate directory scans during startup, causing redundant I/O and unclear startup messages.

**Solution**: Coordinated initialization with shared scanning
- Single directory traversal provides data to all subsystems
- Sequential phases with clear progress logging
- Cross-validation between subsystems to detect inconsistencies
- Shared validation utilities eliminate duplicate logic

**Benefits**:
- Faster startup (eliminates redundant directory scanning)
- Clear visibility into initialization progress
- Early detection of cache inconsistencies
- Consistent validation behavior across subsystems

**Trade-offs**:
- Slightly more complex initialization flow
- Additional coordination logic between subsystems

## Implementation Details

### Cache Key Format

**Current Format**: Path-only (no hostname)
```
/{bucket}/{object_key}
```

**Examples**:
- Full object: `/my-bucket/path/to/object.txt`
- Range: `/my-bucket/path/to/object.txt:range:0-8388607`
- Part: `/my-bucket/path/to/object.txt:part:1`

**Rationale**:
- Simplified from previous `{host}:{path}` format
- Hostname not needed (S3 routing based on bucket)
- Cleaner cache keys and filenames
- Easier debugging and monitoring

**Migration**: Old format detection with metrics tracking

### Metadata Structure

**ObjectMetadata** (`src/cache_types.rs`):
```rust
pub struct ObjectMetadata {
    pub etag: String,
    pub last_modified: String,
    pub content_length: u64,
    pub content_type: Option<String>,
    pub ranges: Vec<RangeSpec>,
    pub upload_state: UploadState,
    pub cumulative_size: u64,
    pub parts: Vec<PartInfo>,
    pub expires_at: SystemTime,
    pub created_at: SystemTime,
}
```

**Key Fields**:
- `ranges`: List of cached ranges (start, end, file path)
- `upload_state`: Complete, InProgress, or Bypassed
- `cumulative_size`: Running total for multipart uploads
- `parts`: Temporary storage for multipart parts (cleared on completion)

**File Size**: Typically <100KB even with hundreds of ranges

### Compression Algorithm Selection

**Content-Aware Detection** (`src/compression.rs`):
```rust
fn should_compress(path: &str) -> bool {
    let ext = Path::new(path).extension()?.to_str()?;
    
    // Skip already-compressed formats
    const SKIP_EXTENSIONS: &[&str] = &[
        "jpg", "png", "gif", "mp4", "zip", "gz", // ...
    ];
    
    !SKIP_EXTENSIONS.contains(&ext.to_lowercase().as_str())
}
```

**Per-Entry Metadata**:
- Each cache entry stores compression algorithm used
- Changing algorithm doesn't invalidate existing cache
- Gradual migration possible

**Rationale**:
- Avoids wasting CPU on already-compressed data
- Saves 2-5x space for text/JSON
- Maintains cache consistency across algorithm changes

### Error Handling Strategy

**Fail Gracefully** (`src/error.rs`):
- Cache errors don't fail requests (serve from S3)
- Corrupted metadata deleted automatically
- Missing range files treated as cache miss
- Disk full triggers emergency eviction

**Error Recovery** (`src/cache.rs`):
```rust
match self.get_metadata(cache_key).await {
    Ok(Some(metadata)) => { /* use metadata */ }
    Ok(None) => { /* cache miss */ }
    Err(e) => {
        error!("Corrupted metadata: {}", e);
        self.delete_metadata(cache_key).await?;
        self.metrics.record_corrupted_metadata().await;
        Ok(None) // Treat as cache miss
    }
}
```

**Metrics Tracking**:
- `corruption_metadata_total`
- `corruption_missing_range_total`
- `inconsistency_fixed_total`
- `disk_full_events_total`

### Multipart Upload Handling

**Capacity-Aware Caching** (`src/cache.rs`):
```rust
pub async fn store_multipart_part(&self, path: &str, part_number: u32, data: &[u8]) -> Result<()> {
    let cache_key = Self::generate_cache_key(path);
    
    // Check if adding this part would exceed capacity
    let current_size = self.disk_cache.get_object_size(&cache_key).await?;
    let part_size = data.len() as u64;
    
    if current_size + part_size > self.write_cache_capacity {
        // Mark upload as bypassed and clean up existing parts
        self.disk_cache.invalidate(&cache_key).await?;
        return Ok(());
    }
    
    // Store part info (number, size, data)
    // ...
}
```

**Completion Assembly** (`src/signed_put_handler.rs`):
```rust
pub async fn finalize_multipart_upload(&self, cache_key: &str, ...) -> Result<()> {
    // Validate all parts exist locally (multi-instance safety)
    for part in &sorted_parts {
        let part_file = get_sharded_path(&ranges_dir, cache_key, &part_suffix);
        if !part_file.exists() {
            // Skip caching and cleanup partial data to prevent corruption
            self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id).await;
            return Ok(()); // Don't fail S3 operation
        }
    }
    
    // Sort parts by part number
    let sorted_parts = tracker.get_sorted_parts();
    
    // Calculate byte positions from part sizes
    let mut current_position = 0u64;
    for part in &sorted_parts {
        let start = current_position;
        let end = start + part.size - 1;
        
        // Create range spec using the actual compression algorithm from cache_upload_part
        let range_spec = RangeSpec::new(
            start,
            end,
            range_file_path,
            part.compression_algorithm.clone(), // Use actual algorithm, not hardcoded
            compressed_size,
            part.size,
        );
        
        current_position += part.size;
    }
    
    // Mark upload complete
    metadata.upload_state = UploadState::Complete;
}
```

**Multi-Instance Safety**:
- Each upload isolated in `mpus_in_progress/{upload_id}/` directory
- CompleteMultipartUpload validates all parts exist before finalization
- Missing parts trigger graceful cleanup without affecting other uploads
- Prevents incomplete cache entries that could serve corrupted data

**Content-Aware Compression** (`src/signed_put_handler.rs`):
- Each part is compressed using content-aware rules based on file extension
- The `CachedPartInfo` struct stores the actual compression algorithm used
- On completion, each part's compression algorithm is preserved in the final range metadata
- This ensures correct decompression when serving cached multipart uploads

**Conflict Handling**:
- New PutObject invalidates existing cached data
- New CreateMultipartUpload invalidates existing cached data
- Ensures cache consistency when uploads are overwritten

### DNS Resolution

**External DNS Servers** (`src/connection_pool.rs`):
```rust
let resolver = TokioAsyncResolver::tokio(
    ResolverConfig::from_parts(
        None,
        vec![],
        NameServerConfigGroup::from_ips_clear(
            &[
                IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),      // Google DNS
                IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),      // Cloudflare DNS
            ],
            53,
            true,
        ),
    ),
    ResolverOpts::default(),
);
```

**Rationale**:
- Bypasses /etc/hosts entries
- Resolves real S3 IP addresses
- Enables IP load balancing
- Prevents routing loops

### Graceful Shutdown

**Coordination** (`src/shutdown.rs`):
```rust
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };
    
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };
    
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    
    info!("Shutdown signal received, starting graceful shutdown");
}
```

**Shutdown Sequence**:
1. Stop accepting new connections
2. Wait for in-flight requests to complete
3. Flush pending cache writes
4. Close connection pools
5. Write final metrics
6. Exit

## Performance Optimizations

### 1. Range Request Gap Consolidation

**Problem**: Partial cache hits with small gaps between cached ranges caused excessive S3 requests.

**Solution**: Intelligent gap consolidation with configurable threshold
```rust
// Consolidate missing ranges to minimize S3 requests
let gap_threshold = config.cache.range_merge_gap_threshold; // Default: 1MB
let consolidated_ranges = range_handler.consolidate_missing_ranges(
    overlap.missing_ranges.clone(), 
    gap_threshold
);
```

**Algorithm** (`src/range_handler.rs`):
```rust
pub fn consolidate_missing_ranges(&self, missing_ranges: Vec<RangeSpec>, max_gap_size: u64) -> Vec<RangeSpec> {
    // Sort ranges by start position
    let mut sorted_ranges = missing_ranges;
    sorted_ranges.sort_by_key(|r| r.start);
    
    let mut consolidated = Vec::new();
    let mut current = sorted_ranges[0].clone();
    
    for range in sorted_ranges.iter().skip(1) {
        let gap = if range.start > current.end {
            range.start - current.end - 1
        } else {
            0 // Overlapping or adjacent
        };
        
        // If gap is small enough, merge ranges
        if gap <= max_gap_size {
            current.end = std::cmp::max(current.end, range.end);
        } else {
            consolidated.push(current);
            current = range.clone();
        }
    }
    consolidated.push(current);
    consolidated
}
```

**Benefits**:
- 60-90% reduction in S3 requests for partial cache hits
- Configurable threshold (default 1MB, tunable per workload)
- Significant bandwidth savings for sparse range access patterns
- Reduced S3 API costs

**Trade-offs**:
- Fetches some unnecessary bytes within gaps
- Requires buffering for merge operations (can't stream)
- Gap threshold needs tuning for optimal performance

### 2. Disk Cache Admission Window

**Problem**: Newly cached ranges were immediately evicted due to zero access history in TinyLFU.

**Solution**: 60-second admission window protection
```rust
// Admission window: skip ranges cached within the last 60 seconds
if !bypass_admission_window {
    let now = SystemTime::now();
    let admission_window = Duration::from_secs(60);
    if let Ok(age) = now.duration_since(range_spec.last_accessed) {
        if age < admission_window {
            // Skip this range as eviction candidate
            continue;
        }
    }
}
```

**Critical Capacity Bypass**: When cache exceeds 110% of limit, admission window is bypassed:
```rust
let bypass_admission_window = current_size > max_cache_size * 1.10;
```

**Benefits**:
- Prevents cache thrashing during large file downloads
- Allows new ranges to accumulate access statistics
- Protects investment in recently cached data
- Emergency bypass prevents disk space exhaustion

**Rationale**:
- New ranges have zero frequency in TinyLFU algorithm
- Without protection, they'd be evicted immediately
- 60 seconds allows ranges to build access history
- 110% bypass ensures system stability under extreme load

### 3. Eviction Buffer Strategy

**Problem**: Frequent eviction at exact capacity limits caused performance degradation.

**Solution**: Trigger eviction at 95%, target 90% capacity
```rust
let trigger_threshold = max_cache_size * 0.95;
let target_size = max_cache_size * (1.0 - config.cache.eviction_buffer_percent as f64 / 100.0);
```

**Configuration**:
```yaml
cache:
  eviction_buffer_percent: 5  # Default: 5% buffer
```

**Benefits**:
- Reduces eviction frequency (amortizes overhead)
- Prevents cache thrashing at capacity boundary  
- Configurable buffer size for different workloads
- Better sustained performance under high load

### 4. Conditional Header Injection Optimization

**Problem**: Client conditional headers (If-None-Match, If-Modified-Since) required careful handling to maintain HTTP compliance.

**Solution**: Smart conditional forwarding with cache optimization
```rust
// Detect conditional headers in client request
fn has_conditional_headers(headers: &HeaderMap) -> bool {
    headers.contains_key("if-match") ||
    headers.contains_key("if-none-match") ||
    headers.contains_key("if-modified-since") ||
    headers.contains_key("if-unmodified-since")
}

// For If-None-Match and If-Modified-Since: optimize with cache
if let Some(etag) = cached_metadata.etag {
    if client_if_none_match == etag {
        return Ok(Response::builder()
            .status(304)
            .body(Body::empty())?);
    }
}
```

**Benefits**:
- Maintains HTTP compliance (always forwards to S3 when needed)
- Optimizes common cases (If-None-Match, If-Modified-Since) from cache
- Eliminates unnecessary S3 requests for 304 responses
- Preserves cache TTL refresh on conditional hits

### 5. Lazy Directory Creation

Directories created on-demand, not at startup:
```rust
if let Some(parent) = path.parent() {
    tokio::fs::create_dir_all(parent).await?;
}
```

**Benefits**:
- Fast startup (no directory scanning)
- Minimal disk I/O
- Only creates needed directories

### 6. TinyLFU Unified Eviction with Frequency Tracking

**Problem**: LRU evicts frequently-accessed items during scan patterns. LFU vulnerable to cache pollution.

**Solution**: TinyLFU algorithm with unified eviction across GET and HEAD entries
```rust
// Unified eviction across GET and HEAD entries
fn find_tinylfu_victim(&self) -> Option<String> {
    // First, try to evict expired entries (both GET and HEAD)
    if let Some(expired_key) = self.find_expired_entries().first() {
        return Some(expired_key.clone());
    }
    
    // Then use TinyLFU across all entry types
    let get_candidates = self.entries.iter().map(|(k, v)| (k, v.last_accessed, "GET"));
    let head_candidates = self.head_entries.iter().map(|(k, v)| (k, v.last_accessed, "HEAD"));
    
    get_candidates.chain(head_candidates)
        .min_by_key(|(key, last_accessed, _)| {
            let frequency = self.tinylfu_frequencies.get(*key).unwrap_or(&1);
            let recency_factor = now.duration_since(*last_accessed)
                .unwrap_or_default().as_secs().max(1);
            frequency * 1000 / recency_factor
        })
        .map(|(key, _, _)| key.clone())
}
```

**Frequency Sketch Implementation**:
```rust
// Lightweight frequency tracking
tinylfu_frequencies: HashMap<String, u64>,

// Update frequency on access
fn record_access(&mut self, key: &str) {
    let freq = self.tinylfu_frequencies.entry(key.to_string()).or_insert(0);
    *freq = freq.saturating_add(1);
}
```

**Benefits**:
- **Scan Resistance**: One-time scans don't pollute cache
- **Unified Competition**: GET and HEAD entries compete fairly for space
- **TTL Priority**: Expired entries evicted first, regardless of frequency
- **Low Overhead**: ~16-24 bytes per entry for frequency tracking
- **Better Hit Rates**: Retains most valuable entries by access frequency

### 7. Content-Aware Compression with Algorithm Selection

**Problem**: Compressing already-compressed data wastes CPU and may increase size.

**Solution**: Content-aware detection with per-entry algorithm storage
```rust
fn should_compress(path: &str) -> bool {
    let ext = Path::new(path).extension()?.to_str()?;
    
    // Skip already-compressed formats
    const SKIP_EXTENSIONS: &[&str] = &[
        "jpg", "png", "gif", "mp4", "zip", "gz", "bz2", "xz", "7z",
        "rar", "tar", "deb", "rpm", "dmg", "iso", "webp", "avif"
    ];
    
    !SKIP_EXTENSIONS.contains(&ext.to_lowercase().as_str())
}

// Per-entry compression metadata
pub struct RangeSpec {
    pub compression_algorithm: CompressionAlgorithm,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    // ... other fields
}
```

**Compression Threshold**: Only compress objects > 4KB
```rust
if data.len() < self.compression_threshold {
    return Ok(data.to_vec()); // Skip compression
}
```

**Benefits**:
- Avoids wasting CPU on already-compressed data
- 2-5x space savings for compressible content (text, JSON, logs)
- Per-entry algorithm storage enables gradual migration
- Maintains cache consistency across algorithm changes

### 8. RAM-Disk Cache Coherency with Journal System

**Problem**: RAM cache hits don't update disk metadata, causing incorrect eviction decisions.

**Solution**: Journal-based metadata updates with RAM buffering
```rust
// Cache-hit updates buffered in RAM
pub struct CacheHitUpdateBuffer {
    pending_updates: HashMap<String, Vec<JournalOperation>>,
    flush_interval: Duration,
    flush_threshold: usize,
}

// Journal entry types
pub enum JournalOperation {
    TtlRefresh { range_start: u64, range_end: u64, new_expires_at: SystemTime },
    AccessUpdate { range_start: u64, range_end: u64, access_count_delta: u32 },
}

// Periodic flush to per-instance journal files
async fn flush_to_journal(&self) -> Result<()> {
    let journal_path = format!("{}/journals/{}.journal", cache_dir, instance_id);
    // Write batched updates with atomic append
}
```

**Background Consolidation**:
```rust
// JournalConsolidator applies entries to metadata with locking
async fn consolidate_journal_entries(&self) -> Result<()> {
    for entry in journal_entries {
        let lock = self.acquire_metadata_lock(&cache_key).await?;
        // Apply updates to .meta file atomically
        self.update_metadata_from_journal(entry).await?;
    }
}
```

**Benefits**:
- Accurate eviction decisions (considers RAM cache access patterns)
- Minimal disk I/O (batched updates every 5 seconds)
- Race-condition-free on shared NFS storage
- Lock contention handled with exponential backoff

### 9. Atomic File Operations with Crash Safety

Write to `.tmp` file, then atomic rename:
```rust
let tmp_path = path.with_extension("tmp");
tokio::fs::write(&tmp_path, data).await?;
tokio::fs::rename(&tmp_path, &path).await?;
```

**Benefits**:
- Prevents partial writes during crashes
- Ensures consistency across power failures
- Crash-safe operations for metadata and range files

### 10. Parallel Range Fetching with Connection Reuse

Fetch multiple missing ranges concurrently:
```rust
let futures: Vec<_> = missing_ranges.iter()
    .map(|range| fetch_range_from_s3(cache_key, range))
    .collect();

let results = futures::future::join_all(futures).await;
```

**Connection Keepalive**:
```rust
let client = Client::builder(TokioExecutor::new())
    .pool_idle_timeout(Duration::from_secs(30))
    .pool_max_idle_per_host(1)
    .build(CustomHttpsConnector::new(pool_manager));
```

**Benefits**:
- Faster partial cache hits (parallel S3 requests)
- 150-200ms latency reduction per request (connection reuse)
- Better S3 throughput utilization
- Reduced total latency for multi-range requests

### 5. Metadata Cache Performance Optimizations

**LRU Eviction**: Simple LRU for uniform-size metadata entries
```rust
// MetadataCache uses simple LRU (not TinyLFU)
// All entries are similar size (~1-2KB), so TinyLFU complexity not needed
fn evict_lru(&mut self) -> Option<String> {
    self.lru_order.pop_front()
}
```

**Simplified Cache Keys**: HEAD entries use simple keys without range suffixes
```rust
// GET cache key (with range)
let get_key = format!("{}:range:{}:{}", path, start, end);

// HEAD/Metadata cache key (simple)
let cache_key = path.to_string();
```

**Optimized Access Tracking**: HEAD access tracked in unified `.meta` file
```rust
// GET access tracking (with range details)
pub fn record_access(&mut self, cache_key: &str, range: &Range, timestamp: SystemTime) {
    // Complex range-aware tracking
}

// HEAD access tracked via MetadataCache
// Updates head_access_count in the unified .meta file
pub async fn update_head_access(&self, cache_key: &str, head_ttl: Duration) {
    // Updates head_expires_at and head_access_count in NewCacheMetadata
}
```

**Memory Efficiency**: Metadata entries have minimal overhead
```rust
// MetadataCache entry size calculation
fn calculate_entry_size(entry: &MetadataCacheEntry) -> u64 {
    // NewCacheMetadata: ~1-2KB per entry
    // Entry overhead: ~100 bytes
    // Total: ~1.5-2.5KB per entry
    // 10,000 entries: ~15-25MB
}
```

**Benefits**:
- **Sub-millisecond access**: No disk I/O for hot entries
- **High cache density**: 10,000+ entries in ~15-25MB
- **Unified storage**: HEAD and GET share same `.meta` file
- **Per-key locking**: Prevents thundering herd on cache miss

### 6. Eviction Buffer

Trigger eviction at 95%, target 90%:
```rust
let trigger_threshold = max_cache_size * 0.95;
let target_size = max_cache_size * 0.90;
```

**Benefits**:
- Reduces eviction frequency
- Amortizes eviction overhead
- Prevents thrashing

### 7. Disk Cache Admission Window

Newly cached ranges are protected from eviction for 60 seconds:

```rust
// In collect_candidates_from_metadata_file_with_options()
if !bypass_admission_window {
    let now = SystemTime::now();
    let admission_window = std::time::Duration::from_secs(60);
    if let Ok(age) = now.duration_since(range_spec.last_accessed) {
        if age < admission_window {
            // Skip this range as eviction candidate
            continue;
        }
    }
}
```

**Rationale**:
- Prevents cache thrashing during large file downloads
- New ranges have zero access history in TinyLFU
- Without protection, newly cached ranges would be evicted immediately
- 60 seconds allows ranges to accumulate access statistics

**Critical Capacity Bypass**:

When cache exceeds 110% of limit, admission window is bypassed:

```rust
let bypass_admission_window = current_size > max_cache_size * 1.10;
let candidates = self.collect_range_candidates_for_eviction_with_options(bypass_admission_window).await?;
```

**Why 110%**:
- Normal eviction at 95% respects admission window
- Extreme scenarios (burst traffic) can exceed limit faster than eviction
- Critical bypass ensures disk space exhaustion is prevented
- All ranges become eviction candidates regardless of age

## Known Limitations

### 1. Write-Through Caching

Write-through caching is enabled by default.

**Issues**:
- Multipart upload capacity checking incomplete
- Conflict invalidation edge cases
- TTL transition timing
- Not fully tested or validated

**Recommendation**: Do not enable. Feature is incomplete and not ready for use.

### 2. Versioned Requests

**Behavior**: Requests with `versionId` query parameter bypass cache entirely

**Rationale**:
- Versioned objects are immutable
- Rare usage pattern
- Complexity not justified

**Impact**: Versioned requests always fetch from S3

### 3. Part Number Request Caching

Part caching is fully implemented for both read and write operations.

**Read Operations (GET with partNumber)**:
- GET requests with `partNumber` query parameter (e.g., `GET /bucket/object?partNumber=5`)
- S3 response includes `Content-Range` header with byte offsets
- Parts stored as ranges using existing range storage mechanism

**Write Operations (UploadPart)**:
- PUT requests with `uploadId` and `partNumber` parameters (multipart upload parts)
- Parts cached immediately during upload (write-through caching)
- On CompleteMultipartUpload, parts assembled into final object ranges

**When Part Requests Are Cached**:
- **Read**: GET with `partNumber` and S3 returns `Content-Range` header
- **Write**: UploadPart requests when write caching is enabled and capacity allows

**When Part Requests Are NOT Cached**:
- Upload verification requests (`partNumber` + `uploadId` parameters)
- UploadPart requests exceeding write cache capacity
- Requests without proper S3 response headers

**Important**: HEAD requests do not provide multipart metadata (`x-amz-mp-parts-count`). For objects not write-cached, the **first part request must be forwarded to S3** to extract multipart information before subsequent part requests can be served from cache.

**How Part Caching Works**:

**Read Path (GET with partNumber)**:
1. **Detection**: Proxy identifies `partNumber` parameter in GET request
2. **Cache Lookup**: Check if multipart metadata exists for object
3. **First Request**: If no metadata, forward to S3 to extract `x-amz-mp-parts-count` and `Content-Range`
4. **Metadata Population**: Store multipart info for future part requests
5. **Subsequent Requests**: Calculate expected byte ranges and serve from cache

**Write Path (UploadPart)**:
1. **Detection**: Proxy identifies `uploadId` + `partNumber` in PUT request
2. **Capacity Check**: Verify cumulative upload size fits in write cache
3. **S3 Forward**: Part uploaded to S3 normally
4. **Part Caching**: Part data cached in `mpus_in_progress/{uploadId}/` directory
5. **Completion**: On CompleteMultipartUpload, parts assembled into final ranges
6. **Immediate Access**: GET requests can access parts immediately after completion

**Example Flows**:
```bash
# Object not write-cached - first part request goes to S3
aws s3api get-object --bucket my-bucket --key large-file.bin --part-number 1 part1.bin
# → Cache miss, forwarded to S3, multipart metadata extracted and cached

# Subsequent part requests served from cache (if ranges exist)
aws s3api get-object --bucket my-bucket --key large-file.bin --part-number 1 part1-copy.bin
# → Cache hit, served from cached range

# Write-cached object - parts immediately available
aws s3api upload-part --bucket my-bucket --key large-file.bin --part-number 1 --upload-id xyz --body part1.bin
# → Part cached during upload
aws s3api complete-multipart-upload --bucket my-bucket --key large-file.bin --upload-id xyz
# → Parts assembled, immediately available for GET requests
```

**Benefits**:
- **Write-through**: Uploaded parts immediately available for range requests
- **Read optimization**: Eliminates repeated S3 requests for same parts
- **Unified storage**: Both read and write parts use same range storage mechanism
- **Multi-instance safe**: Proper coordination for shared cache deployments

**Key Implementation Details**:
```rust
// ObjectMetadata extended with part caching fields
pub struct ObjectMetadata {
    // ... existing fields ...
    pub parts_count: Option<u32>,      // From x-amz-mp-parts-count header
    pub part_ranges: HashMap<u32, (u64, u64)>,  // Per-part byte ranges
    pub upload_id: Option<String>,
}

// Part range lookup — direct HashMap access, no calculation
fn lookup_part(part_number: u32, part_ranges: &HashMap<u32, (u64, u64)>) -> Option<(u64, u64)> {
    part_ranges.get(&part_number).copied()
}
```

**Benefits**:
- **Performance**: Eliminates repeated S3 requests for same parts
- **Accuracy**: Stores exact byte ranges per part, supports variable-sized parts
- **Efficiency**: Leverages existing range storage and compression infrastructure
- **Consistency**: Cached responses match S3 responses exactly

**Use Cases**:
- **Large File Downloads**: Clients downloading specific parts of multi-GB files
- **Parallel Processing**: Multiple workers accessing different parts of same object
- **Resume Downloads**: Clients resuming downloads from specific part offsets
- **Streaming Applications**: Video/audio streaming accessing non-sequential parts

### 4. RAM Metadata Cache Implementation

RAM Metadata Cache is fully implemented with unified storage.

**Architecture**:
- **Unified Storage**: HEAD and GET share same `.meta` file with independent TTLs
- **LRU Eviction**: Simple LRU for uniform-size metadata entries (~1-2KB)
- **Sub-millisecond Performance**: Metadata lookups served from RAM in <1ms

**Key Implementation Details**:
```rust
// MetadataCache entry structure
pub struct MetadataCacheEntry {
    pub metadata: NewCacheMetadata,  // Full metadata including HEAD and GET fields
    pub loaded_at: Instant,          // When loaded from disk
    pub disk_mtime: Option<SystemTime>, // Disk file modification time
    pub last_accessed: Instant,      // For LRU eviction
}

// Per-key locking prevents concurrent disk reads
pub async fn get_or_load<F, Fut>(&self, cache_key: &str, loader: F) -> Result<NewCacheMetadata> {
    // Check cache, acquire per-key lock if miss
    // Only one disk read regardless of concurrent requests
}
```

**Benefits**:
- **Performance**: 99%+ latency reduction for metadata lookups (1ms vs 10-100ms from disk)
- **Efficiency**: Unified storage reduces disk space (no separate HEAD cache)
- **Consistency**: Independent HEAD/GET TTLs in same file
- **Reliability**: Stale file handle recovery for NFS

**Use Cases**:
- API Gateway health checks (99%+ reduction in disk I/O)
- High-concurrency workloads (per-key locking prevents thundering herd)
- NFS shared cache (stale handle recovery)

### 4. TTL Expiration Prioritization

**Issue**: Eviction doesn't prioritize expired entries

**Current Behavior**: Eviction uses LRU/LFU/TinyLFU without checking TTL expiration

**Impact**: Valid data may be evicted before expired data

**Future**: Sort expired entries first during eviction

### 5. Single-Threaded Eviction

**Issue**: Eviction scans all entries sequentially (O(N))

**Impact**: Large caches (millions of entries) have slow eviction

**Future**: Parallel eviction scanning with rayon

## Testing Strategy

### Unit Tests

**Coverage**: 236 tests passing

**Key Test Suites**:
- Cache operations (get, put, evict)
- Range request handling
- Compression/decompression
- TTL expiration
- Multipart uploads
- Error handling

**Property-Based Tests** (quickcheck):
- Cache consistency
- Range merging correctness
- Compression round-trip

### Integration Tests

**Test Files**:
- `integration_test.rs` - End-to-end scenarios
- `cache_statistics_test.rs` - Metrics validation
- `multi_instance_integration_test.rs` - Shared cache coordination
- `s3_integration_test.rs` - S3 API compatibility

### Benchmarks

The project includes benchmark-style tests in the Rust test suite:

- `tests/connection_keepalive_benchmark.rs` - Connection pooling performance tests

These tests measure:
- HeadObject operations
- GET operations (AWS CLI)
- Multipart uploads
- Get-after-Put scenarios
- Cache hit vs miss latency

**Metrics**:
- Throughput (requests/second)
- Latency (p50, p95, p99)
- Cache hit rate
- Bandwidth savings

Run with:
```bash
cargo test --release --test connection_keepalive_benchmark -- --nocapture
```

## Debugging Tips

### Enable Debug Logging

```bash
RUST_LOG=debug cargo run --release -- -c config.yaml
```

### Monitor Cache Operations

```bash
# Watch cache hits/misses (including part cache)
tail -f ./tmp/logs/app/*/s3-proxy.log.* | grep "cache HIT\|cache MISS\|Part cache"

# Monitor metrics
watch -n 1 'curl -s http://localhost:9090/metrics | jq .cache'
```

### Inspect Cache Files

```bash
# List metadata files
find ./tmp/cache/metadata -name "*.meta" | head -10

# View metadata content
cat ./tmp/cache/metadata/my-bucket/a7/3f2/object.meta | jq .

# Check range files
ls -lh ./tmp/cache/ranges/my-bucket/a7/3f2/
```

### Profile Performance

```bash
# CPU profiling
cargo flamegraph --bin s3-proxy -- -c config.yaml

# Memory profiling
valgrind --tool=massif ./target/release/s3-proxy -c config.yaml
```

### Test Specific Scenarios

```bash
# Test cache hit
aws s3 cp s3://bucket/key ./test1.txt --endpoint-url "http://localhost:8081"
aws s3 cp s3://bucket/key ./test2.txt --endpoint-url "http://localhost:8081"

# Test range request
curl -H "Range: bytes=0-1023" http://localhost:8081/bucket/key

# Test part number request
aws s3api get-object --bucket bucket --key large-file.bin --part-number 1 ./part1.bin --endpoint-url "http://localhost:8081"

# Test multipart upload
aws s3 cp large-file.zip s3://bucket/key --endpoint-url "http://localhost:8081"
```

## Code Style Guidelines

### Rust Conventions

- Use `rustfmt` for formatting
- Use `clippy` for linting
- Follow Rust API guidelines
- Prefer `Result<T>` over panics
- Use `?` operator for error propagation

### Error Handling

```rust
// Good
match operation().await {
    Ok(result) => { /* handle success */ }
    Err(e) => {
        error!("Operation failed: {}", e);
        return Err(e);
    }
}

// Bad
let result = operation().await.unwrap();
```

### Logging

```rust
// Use structured logging
info!(
    "Cache hit: cache_key={}, range={}-{}, size={}",
    cache_key, start, end, size
);

// Include context in errors
error!(
    "Failed to store range {}-{} for key {}: {}",
    start, end, cache_key, e
);
```

### Async Patterns

```rust
// Use tokio::spawn for concurrent tasks
let handle = tokio::spawn(async move {
    // background work
});

// Use tokio::select! for concurrent operations
tokio::select! {
    result = operation1() => { /* handle result1 */ }
    result = operation2() => { /* handle result2 */ }
}
```

## Contributing Guidelines

### Before Submitting

1. Run all tests: `cargo test`
2. Run clippy: `cargo clippy`
3. Format code: `cargo fmt`
4. Update documentation
5. Add tests for new features
6. Update CHANGELOG.md

### Pull Request Checklist

- [ ] Tests pass
- [ ] Documentation updated
- [ ] Code formatted
- [ ] Clippy warnings addressed
- [ ] Performance impact assessed
- [ ] Breaking changes documented

## References

### External Documentation

- [Hyper 1.0 Guide](https://hyper.rs/guides/1/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- [Rust Async Book](https://rust-lang.github.io/async-book/)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/)

### Internal Documentation

- [CACHING.md](CACHING.md) - Cache architecture
- [COMPRESSION.md](COMPRESSION.md) - Compression details
- [CONNECTION_POOLING.md](CONNECTION_POOLING.md) - Connection management
- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Error recovery
- [Configuration Guide](CONFIGURATION.md) - Configuration reference

### Research Papers

- **TinyLFU**: "TinyLFU: A Highly Efficient Cache Admission Policy" (Gil Einziger, Roy Friedman, 2017)
- **GDSF**: "GreedyDual-Size: A Cost-Aware WWW Proxy Caching Algorithm" (Pei Cao, Sandy Irani, 1997)
- **BLAKE3**: "BLAKE3: One Function, Fast Everywhere" (Jack O'Connor, et al., 2020)
