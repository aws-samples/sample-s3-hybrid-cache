//! Origin identity, and the per-bucket origin marker that binds a cached bucket to
//! the origin that cached it.
//!
//! # The problem this closes
//!
//! `CacheManager::generate_cache_key` derives the cache key's first segment from the
//! bucket — taken from the host for virtual-hosted addressing, or from the path for
//! path-style. A host that matches no AWS S3 hostname family contributes **nothing** to
//! the key. So `GET /mybucket/data.bin` to one S3-compatible origin and the identical
//! request to a second origin both key to `mybucket/data.bin` and shared one cache entry:
//! the second origin's request was answered with the first origin's bytes.
//!
//! That derivation assumes `(bucket, key)` globally identifies an object. It does for S3.
//! It does not across arbitrary S3-compatible origins, where each cluster has its own
//! bucket namespace.
//!
//! # The mechanism
//!
//! **Cache keys are not changed.** Each bucket's cache directory carries a marker
//! recording the origin that first cached it, and a request whose origin does not match
//! bypasses the cache — forwarding to its own origin, serving nothing from cache and
//! writing nothing to it. Nothing moves, nothing is orphaned, and an existing deployment
//! needs no cache wipe and no manual upgrade step.
//!
//! Three alternatives that change the path layout were rejected, and the reason is
//! `metadata/{bucket}/{XX}` being the **shard identity**: the write-cache accounting
//! checkpoints, the claim leases, the repair rotation and the per-shard write ledger are
//! all keyed on it. See the public security review readiness feature.
//!
//! # Why the AWS branch is asymmetric with the rest
//!
//! An AWS S3 endpoint collapses to its **partition suffix**; a non-S3 host under a partition
//! suffix is uncacheable; every other host is its own identity, verbatim. That asymmetry is
//! deliberate. Every S3 endpoint in a partition shares one bucket namespace, and all of a
//! single bucket's hostname forms live under it — which is what
//! `tests/cache_key_virtual_hosted_test.rs` asserts across roughly a thousand lines. A
//! suffix is not a namespace boundary for anything else: `minio-a.example.com` and
//! `minio-b.example.com` both reduce to `example.com`, so a suffix rule there could only
//! create false matches. See [`origin_identity`] for why non-S3 AWS hosts are refused.
//!
//! # Published residuals, stated rather than designed around
//!
//! 1. **Two transport modes on one host are one origin.** With the port excluded,
//!    a validated-TLS endpoint and an unvalidated one on the same host share a cache
//!    entry, so bytes fetched without certificate validation can be served to a request
//!    that asked for the validated endpoint. Reachable only when an operator configures
//!    both modes for the same host.
//! 2. **A partition suffix is not an exact partition identifier.** GovCloud endpoints sit
//!    under `amazonaws.com` while being a separate partition with a separate bucket
//!    namespace. Deriving the partition from the region label instead would be more code
//!    and more fragile, since the Accelerate and legacy-global hostname forms carry no
//!    region at all.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// AWS partition suffixes, **most specific first**.
///
/// Order is not load-bearing today — `amazonaws.com.cn` does not end with
/// `amazonaws.com`, so neither shadows the other — but it is maintained so that adding a
/// suffix which *is* a tail of another cannot silently take the wrong branch.
///
/// The values are the `dnsSuffix` of each partition in botocore's `partitions.json`:
/// `aws` and `aws-us-gov` (`amazonaws.com`), `aws-cn`, `aws-eusc` (European Sovereign
/// Cloud), `aws-iso`, `aws-iso-b`, `aws-iso-e` and `aws-iso-f`. Listing a partition matters
/// for safety, not only for caching: a host under a listed suffix is cacheable only if it is
/// an S3 endpoint ([`is_cacheable_s3_host`]), while a host under an unlisted one is treated
/// as a non-AWS origin and keyed verbatim, first claim wins.
const AWS_PARTITION_SUFFIXES: [&str; 7] = [
    "amazonaws.com.cn",
    "amazonaws.com",
    "amazonaws.eu",
    "csp.hci.ic.gov",
    "sc2s.sgov.gov",
    "c2s.ic.gov",
    "cloud.adc-e.uk",
];

/// The marker file's name, inside the bucket's own cache directory.
///
/// A **file**, not a directory, and that is the property the whole placement rests on.
/// Every production enumeration of `metadata/{bucket}/*` filters to directories before
/// doing anything with an entry, so a file at that level is invisible to all of them —
/// see `ledger_repair::observed_shards_on_disk` and
/// `CacheSizeTracker::select_l1_directories`, each of which carries a test planting this
/// exact name. A future internal entry at bucket level that is a *directory* would be
/// picked up as a shard candidate by two of those enumerators.
///
/// There is no name collision with the retired per-bucket settings feature, whose
/// `bucket_settings::find_legacy_settings_files` matches the exact filename
/// `_settings.json`.
pub(crate) const MARKER_FILE_NAME: &str = "_origin.json";

/// The marker's schema version. An unrecognised version is treated as **unreadable** rather than
/// ignored, which is the convention the per-shard write-cache checkpoint documents already follow.
///
/// (Named descriptively rather than by symbol on purpose: `staged_fold.rs` carries a source-grep
/// guard asserting that only it and `metrics.rs` mention those types by name, and that guard reads
/// raw file text so a doc comment trips it. Deliberately, so it cannot be talked out of.)
const MARKER_SCHEMA: u32 = 1;

/// Cache bypasses caused by the request's origin differing from the bucket's recorded
/// origin. Per-instance, since process start.
///
/// **Non-zero is a finding, not a routine reading.** On a single-origin deployment this
/// reads zero forever; a rising count means either a genuine multi-origin
/// bucket-name collision or a misconfiguration, and either way the bucket's requests from
/// one of the two origins are being served uncached.
static ORIGIN_MISMATCH_BYPASSES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Cache bypasses caused by a marker that is present and could not be read.
///
/// Counted apart from an ordinary mismatch because the cause and the fix differ: this one
/// is a damaged or hand-edited file, not a second origin. Neither default is safe here —
/// treating it as absent reintroduces the collision, treating it as a mismatch disables
/// caching for that bucket — so bypassing is the conservative direction and this counter
/// is what stops it being silent.
static ORIGIN_MARKER_UNPARSEABLE_BYPASSES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `cache.origin_marker_mismatch_bypasses_total`, per-instance since process start.
pub fn origin_marker_mismatch_bypasses_total() -> u64 {
    ORIGIN_MISMATCH_BYPASSES_TOTAL.load(Ordering::Relaxed)
}
/// Cache bypasses caused by a marker read or claim that did not finish within
/// [`ORIGIN_CLAIM_BOUND`].
///
/// A stalled cache volume, not a second origin. The request is forwarded uncached rather than
/// held before its body is read, because without the marker the proxy cannot say this origin
/// owns the bucket's entries. Non-zero means the shared volume was slow to answer a first
/// request per bucket; the claim keeps running and the next request after it lands is
/// answered from the memo.
static ORIGIN_MARKER_UNRESOLVED_BYPASSES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// How long a request waits for its bucket's origin-marker read or claim before it is
/// forwarded uncached. Same figure as the write-cache setup bound, an order of magnitude
/// inside the clients' 60 s read timeout; a healthy claim is milliseconds.
pub const ORIGIN_CLAIM_BOUND: std::time::Duration = std::time::Duration::from_secs(5);
/// `cache.origin_marker_unresolved_bypasses_total`, per-instance since process start.
pub fn origin_marker_unresolved_bypasses_total() -> u64 {
    ORIGIN_MARKER_UNRESOLVED_BYPASSES_TOTAL.load(Ordering::Relaxed)
}

/// Cache bypasses caused by a request whose host is under an AWS partition suffix and is not
/// an S3 endpoint the proxy caches ([`origin_identity`] returned `None`). Per-instance, since
/// process start.
static UNCACHEABLE_AWS_HOST_BYPASSES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Bypasses counted by [`OriginVerdict::UncacheableHost`], per-instance since process start.
pub fn uncacheable_aws_host_bypasses_total() -> u64 {
    UNCACHEABLE_AWS_HOST_BYPASSES_TOTAL.load(Ordering::Relaxed)
}

/// `cache.origin_marker_unparseable_bypasses_total`, per-instance since process start.
pub fn origin_marker_unparseable_bypasses_total() -> u64 {
    ORIGIN_MARKER_UNPARSEABLE_BYPASSES_TOTAL.load(Ordering::Relaxed)
}

/// The canonical origin identity of a client-supplied host.
///
/// Derived from the **client's** host and never from the resolved upstream, because the
/// host is available where the cache key is built and the upstream transport is not
/// resolved until connect time.
///
/// - An S3 endpoint under a known AWS partition suffix returns `Some(suffix)`.
/// - Any other host under a partition suffix returns `None`: **uncacheable**. The request is
///   forwarded, never reads or writes cache, and never claims or matches a marker.
/// - Any host outside every partition suffix returns `Some(host)`, lowercased, verbatim.
/// - A port is never part of the result.
///
/// # Only S3 endpoints share the partition identity
///
/// The partition collapse exists so the many hostname forms of one bucket (path-style,
/// virtual-hosted, dualstack, Accelerate, access point, MRAP) share one identity, which is
/// what `tests/cache_key_virtual_hosted_test.rs` asserts. Bucket names are unique per
/// partition, so that is sound for S3 endpoints. It is not sound for any other host under the
/// suffix: anyone can create an `execute-api`, `elb` or `lambda-url` endpoint with a publicly
/// trusted certificate serving any bytes at any path, and with the partition identity its
/// response to `GET /victim-bucket/key` would be cached under `victim-bucket/key` and served
/// to every later S3 request for that object.
///
/// The same reasoning excludes a bucket-carrying S3 host that the cache key does not read the
/// bucket from: `attacker.s3-us-west-2.amazonaws.com` keys `GET /victim-bucket/key` as
/// `victim-bucket/key` while S3 answers from the attacker's bucket. So the S3 test is
/// [`is_cacheable_s3_host`], which accepts exactly the endpoint forms that carry no resource
/// in the host and the forms the cache-key extractors recognise. Case matters for the second
/// group, because the extractors do not lowercase: `MYBUCKET.S3.US-EAST-1.AMAZONAWS.COM` keys
/// path-only, so it is uncacheable rather than given the partition identity.
pub fn origin_identity(host: &str) -> Option<String> {
    let stripped = strip_port(host);
    let lowered = stripped.to_ascii_lowercase();
    for suffix in AWS_PARTITION_SUFFIXES {
        if is_at_or_under(&lowered, suffix) {
            let labels = if lowered == suffix {
                ""
            } else {
                &lowered[..lowered.len() - suffix.len() - 1]
            };
            return if is_cacheable_s3_host(stripped, &lowered, labels, suffix) {
                Some(suffix.to_string())
            } else {
                None
            };
        }
    }
    Some(lowered)
}

/// Is a host under partition `suffix` an S3 endpoint whose cache key names the resource the
/// endpoint will actually serve?
///
/// `labels` is the lowercased host with `.suffix` removed (empty for the bare suffix). Two
/// shapes qualify, and nothing else does:
///
/// 1. **An S3 service endpoint that carries no bucket or access point in the host**:
///    `s3`, `s3.<region>`, `s3.dualstack.<region>`, legacy `s3-<region>` and
///    `s3-external-1`, `s3-fips.<region>`, `s3-fips.dualstack.<region>`, bare
///    `s3-accelerate[.dualstack]`, and the access point base domains
///    `s3-accesspoint[.dualstack].<region>` and `accesspoint.s3-global`. AWS operates
///    these, and the bucket is the path's first segment, which is also the cache key's.
/// 2. **A host that names a bucket or access point, in a form the cache key reads the
///    bucket or access point from**: exactly what `cache::extract_virtual_hosted_bucket` and
///    `cache::extract_access_point_prefix` recognise, applied to the host as the client sent
///    it. A bucket-carrying host those two do not recognise keys path-only, so the bucket in
///    the host (which anyone can own) would answer under the path's bucket name. That covers
///    legacy `<bucket>.s3-<region>`, `<bucket>.s3-fips.<region>`, website endpoints, S3
///    Express, Object Lambda, PrivateLink `vpce` names, an uppercase virtual-hosted host, and
///    every virtual-hosted form outside `amazonaws.com`. Those are uncacheable.
///
/// Every other host under a partition suffix (`execute-api`, `elb`, `lambda-url` and the
/// rest) is uncacheable: anyone can create one serving any bytes at any path. A family
/// missing from this list costs caching for that family, never correctness.
fn is_cacheable_s3_host(raw: &str, lowered: &str, labels: &str, suffix: &str) -> bool {
    if is_s3_service_endpoint(labels) {
        return true;
    }
    // Shape 2. The extractors match the raw host case-sensitively and only under
    // `amazonaws.com`; requiring an all-lowercase host keeps this classification and the
    // cache key reading the same string.
    if suffix != "amazonaws.com" || raw != lowered {
        return false;
    }
    let parts: Vec<&str> = labels.split('.').collect();
    let shape_ok = match parts.as_slice() {
        // <ap>.s3-accesspoint[.dualstack].<region>
        [ap, "s3-accesspoint", region] | [ap, "s3-accesspoint", "dualstack", region] => {
            !ap.is_empty() && is_region_label(region)
        }
        // <mrap>.accesspoint.s3-global
        [mrap, "accesspoint", "s3-global"] => !mrap.is_empty(),
        // <bucket>.s3-accelerate[.dualstack]: no-dot bucket names only.
        [bucket, "s3-accelerate"] | [bucket, "s3-accelerate", "dualstack"] => !bucket.is_empty(),
        _ => {
            // <bucket>.s3[.dualstack].<region> and legacy <bucket>.s3; the bucket may be dotted.
            let n = parts.len();
            (n >= 3 && parts[n - 2] == "s3" && is_region_label(parts[n - 1]))
                || (n >= 4
                    && parts[n - 3] == "s3"
                    && parts[n - 2] == "dualstack"
                    && is_region_label(parts[n - 1]))
                || (n >= 2 && parts[n - 1] == "s3")
        }
    };
    shape_ok
        && (crate::cache::extract_access_point_prefix(raw).is_some()
            || crate::cache::extract_virtual_hosted_bucket(raw).is_some())
}

/// Shape 1 of [`is_cacheable_s3_host`]: an S3 endpoint with no resource in the host.
fn is_s3_service_endpoint(labels: &str) -> bool {
    let parts: Vec<&str> = labels.split('.').collect();
    match parts.as_slice() {
        ["s3"] | ["s3-external-1"] => true,
        ["s3-accelerate"] | ["s3-accelerate", "dualstack"] => true,
        ["accesspoint", "s3-global"] => true,
        ["s3", region]
        | ["s3", "dualstack", region]
        | ["s3-fips", region]
        | ["s3-fips", "dualstack", region]
        | ["s3-accesspoint", region]
        | ["s3-accesspoint", "dualstack", region] => is_region_label(region),
        [legacy] => legacy.strip_prefix("s3-").is_some_and(is_region_label),
        _ => false,
    }
}

/// Does `label` look like an AWS region name (`us-east-1`, `eusc-de-east-1`,
/// `us-gov-west-1`, `us-isob-east-1`)?
///
/// Two to four letters, one or more lowercase-letter groups, then a number. The short first
/// group is what rejects `website-us-east-1`, so `s3-website-us-east-1` is not mistaken for
/// the legacy `s3-<region>` form.
fn is_region_label(label: &str) -> bool {
    let groups: Vec<&str> = label.split('-').collect();
    if groups.len() < 3 {
        return false;
    }
    let first = groups[0];
    let last = groups[groups.len() - 1];
    (2..=4).contains(&first.len())
        && first.bytes().all(|b| b.is_ascii_lowercase())
        && groups[1..groups.len() - 1]
            .iter()
            .all(|g| !g.is_empty() && g.bytes().all(|b| b.is_ascii_lowercase()))
        && !last.is_empty()
        && last.bytes().all(|b| b.is_ascii_digit())
}

/// Is `host` exactly `suffix`, or a subdomain of it?
///
/// The dot-boundary check is what stops `evil-amazonaws.com` — or any lookalike whose
/// name merely *ends* with a partition suffix — being treated as an AWS host, which a
/// bare `ends_with` would do.
fn is_at_or_under(host: &str, suffix: &str) -> bool {
    if host == suffix {
        return true;
    }
    host.len() > suffix.len()
        && host.ends_with(suffix)
        && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
}

/// Drop a trailing `:port`, leaving an unbracketed IPv6 literal alone.
///
/// Production callers pass the already-port-stripped host that
/// `HttpProxy::parse_host_header` produced — that function, not
/// `disk_cache::validate_bucket_segment`, is where the no-port guarantee comes from, since
/// the latter permits `:` in a segment. This is belt-and-braces over it, and it is worth
/// having rather than relying on the caller: a future call site that passed an authority
/// would otherwise silently give one host a **per-port** identity, which inverts published
/// residual 1 from "two transport modes share an entry" into "the second mode bypasses".
fn strip_port(host: &str) -> &str {
    // Bracketed IPv6 (`[::1]:8080`): everything from `]` onwards is not part of the host.
    if let Some(rest) = host.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return &rest[..end];
        }
        return host;
    }
    match host.rsplit_once(':') {
        // Exactly one colon and a numeric tail is an authority. More than one colon and no
        // brackets is an unbracketed IPv6 literal, which must be left intact.
        Some((left, right))
            if !left.contains(':') && !right.is_empty() && right.parse::<u16>().is_ok() =>
        {
            left
        }
        _ => host,
    }
}

/// The marker's on-disk body. Schema header first, following the write ledger's convention.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OriginMarker {
    schema: u32,
    origin: String,
}

/// What the bucket's marker says, as read once per bucket per process.
///
/// This memoises the **recorded origin** rather than a verdict, and that distinction is
/// load-bearing: a verdict is a function of `(recorded origin, request origin)`, and the
/// whole point of this mechanism is a bucket reached from more than one origin. A memo of
/// verdicts would answer the second origin with the first origin's verdict, which is the
/// defect rather than the fix. (Design § R2 → "Memoisation" describes it as a map from
/// bucket to verdict; that wording is wrong for the multi-origin case and is named as owed
/// in this unit's return.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MarkerState {
    /// The bucket is bound to this origin identity.
    Recorded(String),
    /// A marker file exists and could not be read as a marker.
    Unparseable,
}

/// Whether a request may read or write cache for its bucket.
///
/// # There is deliberately no `Default`
///
/// Following `crate::cache_types::RangeLookupPurpose`, whose missing `Default` exists so a
/// new call site cannot inherit another's safety condition. The condition here is not
/// recoverable from the surrounding code at all: the cache key does not contain the origin
/// — that absence *is* this requirement's premise — so no cache-layer site can compute
/// this value from what it holds. It must be threaded from the HTTP layer, where the
/// client's host is in scope.
///
/// Ask: **is this request's origin the one this bucket's cached bytes came from?** If the
/// answer is not available at a call site, the value must be a parameter there, not a
/// literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginVerdict {
    /// The recorded origin matches this request's origin, or no marker existed and this
    /// request claimed one. Cache reads and writes proceed exactly as before.
    Matches,
    /// The recorded origin differs. Forward to the origin; serve no cached bytes and write
    /// no cache entry.
    Mismatch,
    /// A marker exists and could not be read. Treated as [`Self::Mismatch`] for the serve
    /// decision, counted separately so a damaged file is distinguishable from a second
    /// origin.
    Unparseable,
    /// The marker could not be read or claimed within [`ORIGIN_CLAIM_BOUND`]. The binding is
    /// unknown, so this request neither serves nor writes cache. Unlike [`Self::Mismatch`] it
    /// does not suppress invalidation: a PUT (signed, unsigned or SSE-C) is forwarded uncached and
    /// invalidates the key on success, and DELETE and DeleteObjects invalidate their keys,
    /// because skipping that would leave the replaced object servable. Counted separately
    /// because the cause is a slow volume, not a second origin. Never memoised.
    Unresolved,
    /// The host is under an AWS partition suffix and is not an S3 endpoint the proxy caches
    /// ([`origin_identity`] returned `None`). Treated as [`Self::Mismatch`]: forward, serve
    /// and write nothing, invalidate nothing. Decided before any marker read, so such a host
    /// never claims a bucket.
    UncacheableHost,
}

impl OriginVerdict {
    /// May this request read or write cache?
    ///
    /// The one predicate every guard branches on, so the two bypassing variants cannot
    /// drift apart at a call site.
    pub fn permits_cache(self) -> bool {
        matches!(self, Self::Matches)
    }

    /// Stable label for the bypass reason, passed to the existing forward-without-caching
    /// path and used in logs.
    pub(crate) fn bypass_reason(self) -> &'static str {
        match self {
            Self::Matches => "origin_matches",
            Self::Mismatch => "origin_mismatch",
            Self::Unparseable => "origin_marker_unparseable",
            Self::Unresolved => "origin_marker_unresolved",
            Self::UncacheableHost => "origin_uncacheable_aws_host",
        }
    }

    /// Count one bypass attributable to this verdict. A no-op for [`Self::Matches`].
    ///
    /// Call once per bypassed request, at the guard that forwards it.
    pub(crate) fn record_bypass(self) {
        match self {
            Self::Matches => {}
            Self::Mismatch => {
                ORIGIN_MISMATCH_BYPASSES_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Self::Unparseable => {
                ORIGIN_MARKER_UNPARSEABLE_BYPASSES_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Self::Unresolved => {
                ORIGIN_MARKER_UNRESOLVED_BYPASSES_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Self::UncacheableHost => {
                UNCACHEABLE_AWS_HOST_BYPASSES_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl MarkerState {
    /// The verdict for a request arriving from `request_origin`.
    pub(crate) fn verdict_for(&self, request_origin: &str) -> OriginVerdict {
        match self {
            Self::Unparseable => OriginVerdict::Unparseable,
            Self::Recorded(recorded) if recorded == request_origin => OriginVerdict::Matches,
            Self::Recorded(_) => OriginVerdict::Mismatch,
        }
    }
}

/// The marker's path for one bucket.
pub(crate) fn marker_path(metadata_dir: &Path, bucket: &str) -> PathBuf {
    metadata_dir.join(bucket).join(MARKER_FILE_NAME)
}

/// What a bucket's metadata directory held at the moment it was claimed.
///
/// Three-valued deliberately. A directory that cannot be read is **not** evidence that
/// entries were there, and reporting it as though it were would put a warning in front of an
/// operator on the strength of a failed `read_dir`. Only [`Self::Present`] is a signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreClaimEntries {
    /// At least one `.meta` was found under the bucket's metadata directory, so the bucket
    /// held cached entries before any origin was recorded against it.
    Present,
    /// The directory was absent, or held no `.meta`. A first-ever request for the bucket, or
    /// one whose entries have all been evicted.
    NoneFound,
    /// The directory could not be enumerated, or the search budget ran out before the tree
    /// was exhausted. Nothing is known in either direction.
    Unknown,
}

/// How many `read_dir` calls [`detect_pre_claim_entries`] will spend before giving up.
///
/// The healthy shape needs **three** — `{bucket}/`, one `{XX}/`, one `{YYY}/` — because
/// `disk_cache::get_sharded_path` puts every `.meta` at `{bucket}/{XX}/{YYY}/`, and the walk
/// returns on the first one it sees. The budget exists only so that a bucket directory left
/// holding many empty shard directories by an interrupted cleanup cannot turn a claim into a
/// long traversal. Exhausting it reports [`PreClaimEntries::Unknown`], which warns about
/// nothing.
const PRE_CLAIM_SCAN_BUDGET: u32 = 64;

/// Did this bucket's metadata directory already hold a cached entry?
///
/// A `.meta` is the evidence, and it has to be — a *directory* under `{bucket}/` proves
/// nothing, because the three cleanup sites that remove an empty directory are best-effort
/// and an evicted bucket can be left holding empty shard directories. `metadata/` is also the
/// right tree rather than `ranges/`: the two are siblings under the cache directory, a range
/// file is only reachable through the `.meta` that lists it, and `metadata/{bucket}/` is
/// where the marker itself lives, so this is a read of the directory the claim is about to
/// write into.
///
/// A `{key}.meta.tmp[.{host}.{pid}]` does not count, and that falls out of testing the
/// extension rather than a substring: an interrupted write is not a published entry.
///
/// Depth-first with an explicit stack, so the common case returns after three reads rather
/// than enumerating a level at a time. Symbolic links are not followed — `file_type` reports
/// the link, not its target — which production never creates here and which cannot loop.
fn detect_pre_claim_entries(bucket_dir: &Path) -> PreClaimEntries {
    let mut stack = vec![bucket_dir.to_path_buf()];
    let mut budget = PRE_CLAIM_SCAN_BUDGET;
    // Any single failure to read makes a `NoneFound` verdict unsound, because the entry that
    // would have proved otherwise may be behind it.
    let mut could_not_tell = false;

    while let Some(dir) = stack.pop() {
        if budget == 0 {
            return PreClaimEntries::Unknown;
        }
        budget -= 1;

        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            // Absent is knowledge, not a failure: there is nothing under a directory that
            // does not exist. This is the whole of a first-ever claim's cost.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                debug!(
                    "Origin marker: could not read {:?} while checking for pre-existing \
                     entries: {}",
                    dir, e
                );
                could_not_tell = true;
                continue;
            }
        };

        for entry in entries {
            let Ok(entry) = entry else {
                could_not_tell = true;
                continue;
            };
            match entry.file_type() {
                Ok(t) if t.is_dir() => stack.push(entry.path()),
                Ok(t) if t.is_file() => {
                    // `get_sharded_path` is called with the suffix `.meta`, so this is the
                    // extension of every published metadata file and of nothing else.
                    if entry.path().extension().and_then(|e| e.to_str()) == Some("meta") {
                        return PreClaimEntries::Present;
                    }
                }
                Ok(_) => {}
                Err(_) => could_not_tell = true,
            }
        }
    }

    if could_not_tell {
        PreClaimEntries::Unknown
    } else {
        PreClaimEntries::NoneFound
    }
}

/// Report that a bucket was bound to an origin over entries that predate the binding.
///
/// # The residual this makes visible, and why it cannot be closed
///
/// Entries cached before any marker existed carry no origin, and no marker design can give
/// them one: a regular-bucket cache key is `{bucket}/{path}` and holds no origin. So the
/// claiming origin is **served** those entries as its own — measured at byte level, as a
/// cache hit with the other origin's body — and the marker then records the claiming origin,
/// which owns the binding from that point on. This is a residual, not a defect.
///
/// # It is invisible to both bypass counters, which is the reason this line exists
///
/// An absent marker is a no-op rather than a mismatch, so neither
/// [`origin_marker_mismatch_bypasses_total`] nor
/// [`origin_marker_unparseable_bypasses_total`] moves while it happens. The metric an
/// operator would reach for reads zero. A counter of this condition would report a number
/// without naming a bucket; a log line names the bucket, and a claim happens once per bucket,
/// so it costs nothing on the request path.
///
/// # THE MESSAGE STATES WHAT IS UNKNOWN. It does not allege a cross-origin serve
///
/// The proxy cannot know whose entries they were — that is the residual — so the only
/// detectable condition is "this bucket is being claimed over a directory that already holds
/// entries", which is **true of every bucket of every deployment upgraded onto this
/// mechanism**, single-origin ones very much included. Wording that alleged a cross-origin
/// serve would therefore be a false alarm on the overwhelming majority of deployments, fired
/// once per bucket at exactly the moment an operator is watching an upgrade, which trains
/// them to ignore it and destroys the only value it has. So it says the entries cannot be
/// attributed, tells a single-origin reader plainly that no action is needed, and leaves the
/// conditional remedy in `docs/CACHING.md` rather than instructing anyone to clear a cache.
///
/// # At most once per bucket per process, and in practice once per bucket per volume
///
/// `CacheManager::origin_verdict` memoises the marker state per bucket, so `read_or_claim`
/// runs at most once per bucket per process except for a concurrent first-request race on the
/// same bucket. This is called from the successful-`link` branch only, and exactly one racer's
/// `link` can succeed — every other gets `EEXIST` and takes the read-back branch — so even
/// that race emits one line. The fail-open branches deliberately do not call it: each already
/// warns about the I/O failure, which is the larger problem, and none of them recorded a
/// binding.
fn warn_claimed_over_pre_existing_entries(bucket: &str, origin: &str) {
    warn!(
        "Origin marker: bucket {} is now recorded as belonging to origin {}, and its \
         directory on the cache volume already held cached entries from before any origin \
         was recorded for it. Which origin wrote those entries is not knowable — a cache \
         key is a bucket name and an object path and carries no origin — so they are \
         treated as this origin's from now on. Where this bucket name is served from a \
         single origin that is the expected state and no action is needed. Where it is \
         served from more than one, the entries may have come from either and neither \
         origin-marker bypass counter records it; see \"One Bucket Name at Two Origins\" in \
         docs/CACHING.md.",
        bucket, origin
    );
}

/// This process's identity, for the claim's temp filename. Matches the shape used by
/// `cache::instance_scoped_meta_tmp_path`, so two instances sharing a volume cannot share
/// a temp file and produce a torn one.
fn instance_suffix() -> String {
    format!(
        "{}.{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    )
}

/// Read the bucket's marker, claiming it for `origin` if none exists.
///
/// # An absent marker is a no-op, and that is the upgrade-safety property
///
/// An existing deployment has no markers, so the first post-upgrade request for every
/// bucket takes the claim path and then matches. Treating absent as a *mismatch* would
/// make the release bypass the cache for every bucket until each was re-claimed, which is
/// the one way a careless implementation of this guard breaks a working deployment. The
/// claim is what makes absent a single-request condition rather than a permanent one.
///
/// A claim over a directory that already holds entries is reported once, at `WARN` — see
/// [`warn_claimed_over_pre_existing_entries`] for what the message can honestly say and why a
/// counter would not do.
///
/// # Claiming is write-then-`link`, never `O_CREAT|O_EXCL`-then-write
///
/// `create_new` followed by a write can publish an **empty** file if the process dies
/// between the two, and a later reader then has to decide what an empty marker means. This
/// project has already paid for that exact shape: the retired `{key}.meta.lock` protocol
/// treated an empty payload as stale and unlinked a live holder's lock. Writing the body to
/// a temp file first and then `link`-ing it into place cannot publish a partial file, and
/// `link` fails `EEXIST` rather than overwriting, so it never destroys a peer's claim.
///
/// On `EEXIST` the winner's marker is read and used. That fallback is also what makes the
/// protocol robust to an NFS retransmit returning `EEXIST` for the caller's **own** earlier
/// success: in the self-win case the value read back is the value just written, so the two
/// cases need not be distinguished. Do not replace it with an errno-based branch.
///
/// No lock is needed. Two instances racing on a fresh bucket converge on whichever `link`
/// succeeded.
///
/// # A MARKER PINS ITS BUCKET DIRECTORY FOREVER. That is ACCEPTED, deliberately
///
/// Three cleanup sites remove an empty directory bottom-up — `DiskCache::cleanup_empty_directory`
/// (called on the bucket path by both `traverse_and_cleanup_metadata` and
/// `traverse_and_cleanup_ranges`), `DiskCache::batch_cleanup_empty_directories`, whose
/// `is_protected_dir` protects only the cache-type roots, and the upward walk in
/// `cleanup_orphaned_files` — and **a bucket directory under `metadata/` is protected at none of
/// them.** All three remove a directory only when `read_dir(..).next().is_none()`, so once a
/// marker exists that directory is never empty again, even after every `.meta` for the bucket has
/// been evicted or invalidated. Two consequences, both accepted rather than handled:
///
/// 1. **A stale marker outlives all of its bucket's data.** A bucket whose cache is fully
///    evicted and which is then legitimately repointed at a new origin is bypassed indefinitely,
///    on a marker describing data that no longer exists. There is no reset path other than an
///    operator removing the file — which is a supported action, and is why the changelog says so.
///    Accepted because the alternative is a reset rule that fires on an empty directory, and an
///    empty bucket directory is indistinguishable from one whose objects are merely all evicted;
///    such a rule would silently unbind a live bucket during ordinary eviction.
/// 2. **The bucket count becomes monotone in buckets TOUCHED, not buckets CACHED.** Every bucket
///    ever requested through the proxy permanently adds a directory, a file and one `readdir` to
///    the census and to the ledger-repair rotation, whose own cost note is "one readdir of
///    `metadata/` plus one per bucket". Mitigated to the point where **only a request that could
///    genuinely have cached something claims**, and the set is enumerated rather than described
///    because a first version of this comment got it wrong:
///
///    - **Does not claim — the guard is below them and they return first**: the SSE-C bypass, the
///      part-scoped-`HEAD` bypass, the `no-store` arm of the cache-bypass-header branch, **every
///      `HttpProxy::should_bypass_cache` condition** (`ListBuckets`, `list-type`, `delimiter`,
///      `versions`, `uploads`, `acl`, `attributes`, `legal-hold`, `object-lock`, `retention`,
///      `tagging`, `torrent`) and the **`versionId`** read. The last two families were hoisted
///      above the guard specifically so a request the proxy never caches pays neither the marker
///      read nor the claim.
///    - **Does not claim — the cache key has no bucket segment** for
///      `disk_cache::parse_cache_key` to return, so nothing can be stored under it and there is
///      nothing to bind. This covers `ListBuckets` (`GET /`) a second time and every
///      bucket-scoped operation on `/{bucket}`.
///    - **Does claim**: a `GET`/`HEAD` that reaches the cacheable pipeline, including one that
///      404s.
///
///    **An earlier version of this comment claimed a "prefixed LIST" was in the last group. That
///    distinction does not exist**: `ListObjectsV2` carries its prefix as a *query parameter*
///    (`GET /mybucket?list-type=2&prefix=a/b/`), never as a path segment, so a LIST path is always
///    `/` or `/{bucket}` — one segment — and was already exempt on cache-key grounds before the
///    hoist. The operations the hoist actually saves are the **object-scoped** metadata reads,
///    whose paths are `/{bucket}/{key}?acl` and the like.
///
/// Neither is a reason to move the marker, and the second is bounded by the number of distinct
/// bucket names an operator actually addresses.
///
/// # A failed claim reports `Recorded(origin)`, which is fail-open by intent
///
/// If the marker cannot be written at all — a read-only volume, no space — there is no
/// marker, which is the migration case, so the request proceeds as it does today. Refusing
/// to cache on an I/O error would turn a transient volume problem into a fleet-wide caching
/// outage. The caller memoises the result, so the failed attempt is not repeated per
/// request; a process that could not record the origin still binds the bucket to it
/// **within that process**, which is where its own cached bytes came from.
pub(crate) fn read_or_claim(metadata_dir: &Path, bucket: &str, origin: &str) -> MarkerState {
    let path = marker_path(metadata_dir, bucket);

    if let Some(state) = read_marker(&path) {
        return state;
    }

    // Absent. Claim it — but read the directory as it stands first. Entries already there
    // predate any origin binding and cannot be attributed to one, which is worth telling the
    // operator once. This MUST run before the `create_dir_all` below, which would otherwise
    // leave a first-ever claim's absent directory indistinguishable from an empty one.
    let bucket_dir = path.parent().unwrap_or(metadata_dir);
    let pre_claim = detect_pre_claim_entries(bucket_dir);

    if let Err(e) = std::fs::create_dir_all(bucket_dir) {
        warn!(
            "Origin marker: could not create {:?}: {}. Proceeding as though the bucket were \
             unclaimed, which is the pre-upgrade behaviour.",
            bucket_dir, e
        );
        return MarkerState::Recorded(origin.to_string());
    }

    let tmp = bucket_dir.join(format!("{}.tmp.{}", MARKER_FILE_NAME, instance_suffix()));
    let body = match serde_json::to_vec(&OriginMarker {
        schema: MARKER_SCHEMA,
        origin: origin.to_string(),
    }) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                "Origin marker: could not serialize marker for {}: {}",
                bucket, e
            );
            return MarkerState::Recorded(origin.to_string());
        }
    };

    let write_result = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&body)?;
        f.sync_all()
    })();

    if let Err(e) = write_result {
        warn!(
            "Origin marker: could not stage {:?}: {}. Proceeding as though the bucket were \
             unclaimed.",
            tmp, e
        );
        let _ = std::fs::remove_file(&tmp);
        return MarkerState::Recorded(origin.to_string());
    }

    let link_result = std::fs::hard_link(&tmp, &path);
    // Unlink the temp file on both outcomes, before deciding anything: the link either
    // published its content under the real name or lost to a peer, and in neither case is
    // the temp name wanted again.
    let _ = std::fs::remove_file(&tmp);

    match link_result {
        Ok(()) => {
            debug!(
                "Origin marker: claimed bucket {} for origin {}",
                bucket, origin
            );
            if pre_claim == PreClaimEntries::Present {
                warn_claimed_over_pre_existing_entries(bucket, origin);
            }
            MarkerState::Recorded(origin.to_string())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // A peer won — or this instance won and the reply was lost. Either way the
            // marker on disk is authoritative and the read below distinguishes nothing,
            // which is the point.
            match read_marker(&path) {
                Some(state) => state,
                None => {
                    warn!(
                        "Origin marker: {:?} reported as existing but could not be read back. \
                         Proceeding as though the bucket were unclaimed.",
                        path
                    );
                    MarkerState::Recorded(origin.to_string())
                }
            }
        }
        Err(e) => {
            warn!(
                "Origin marker: could not publish {:?}: {}. Proceeding as though the bucket \
                 were unclaimed.",
                path, e
            );
            MarkerState::Recorded(origin.to_string())
        }
    }
}

/// Read an existing marker. `None` means the file is absent — distinct from
/// [`MarkerState::Unparseable`], which means it is there and unusable.
fn read_marker(path: &Path) -> Option<MarkerState> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(
                "Origin marker: {:?} exists and could not be read: {}. Treating it as \
                 unparseable, which bypasses the cache for this bucket.",
                path, e
            );
            return Some(MarkerState::Unparseable);
        }
    };
    match serde_json::from_slice::<OriginMarker>(&bytes) {
        Ok(m) if m.schema == MARKER_SCHEMA && !m.origin.is_empty() => {
            Some(MarkerState::Recorded(m.origin))
        }
        Ok(m) => {
            warn!(
                "Origin marker: {:?} has schema {} and origin {:?}, which this build cannot \
                 use (expected schema {} and a non-empty origin). Treating it as unparseable, \
                 which bypasses the cache for this bucket.",
                path, m.schema, m.origin, MARKER_SCHEMA
            );
            Some(MarkerState::Unparseable)
        }
        Err(e) => {
            warn!(
                "Origin marker: {:?} could not be parsed: {}. Treating it as unparseable, \
                 which bypasses the cache for this bucket.",
                path, e
            );
            Some(MarkerState::Unparseable)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ── origin_identity: the AWS families all collapse to one suffix ──────────
    //
    // Every family named in the spec gets its own assertion, even though the
    // partition-suffix test covers them all by construction. That is the point: if
    // someone later narrows the matcher to an enumeration of S3 service labels, these
    // hold them to covering the whole set rather than the ones they remembered.

    #[test]
    fn every_aws_family_collapses_to_the_partition_suffix() {
        for host in [
            // Path-style — the form `extract_virtual_hosted_bucket` returns None for, and
            // therefore the one a matcher assembled from the bucket extractors would miss.
            "s3.us-west-2.amazonaws.com",
            "s3.amazonaws.com",
            "s3.dualstack.us-west-2.amazonaws.com",
            // Virtual-hosted, regional and dualstack.
            "mybucket.s3.us-east-1.amazonaws.com",
            "mybucket.s3.dualstack.us-east-1.amazonaws.com",
            // Accelerate, plain and dualstack.
            "mybucket.s3-accelerate.amazonaws.com",
            "mybucket.s3-accelerate.dualstack.amazonaws.com",
            // Legacy global virtual-hosted.
            "mybucket.s3.amazonaws.com",
            // Regional access point, and its base domain (the path-style alias form).
            "myap-123456789012.s3-accesspoint.us-east-1.amazonaws.com",
            "s3-accesspoint.us-east-1.amazonaws.com",
            // MRAP, and its base domain.
            "mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com",
            "accesspoint.s3-global.amazonaws.com",
            // A dotted bucket name, which the regional form permits.
            "my.company.logs.s3.us-east-1.amazonaws.com",
            // Path-style endpoints with no resource in the host: legacy dash, FIPS, bare
            // Accelerate, GovCloud.
            "s3-us-west-2.amazonaws.com",
            "s3-external-1.amazonaws.com",
            "s3-fips.us-east-1.amazonaws.com",
            "s3-fips.dualstack.us-east-1.amazonaws.com",
            "s3-accelerate.amazonaws.com",
            "s3.us-gov-west-1.amazonaws.com",
            "mybucket.s3.us-gov-west-1.amazonaws.com",
            "myap-123456789012.s3-accesspoint.dualstack.us-east-1.amazonaws.com",
        ] {
            assert_eq!(
                origin_identity(host).as_deref(),
                Some("amazonaws.com"),
                "AWS host `{}` must collapse to the partition suffix, or one bucket reached \
                 through two of these forms records two origins and bypasses — which is the \
                 collapsing tests/cache_key_virtual_hosted_test.rs exists to protect",
                host
            );
        }
    }

    #[test]
    fn non_s3_hosts_under_a_partition_suffix_are_uncacheable() {
        for host in [
            // The poisoning shape: anyone can create one, with a trusted certificate.
            "abc.execute-api.us-east-1.amazonaws.com",
            "x.elb.us-west-2.amazonaws.com",
            "case64-option-d-af2c0a8fb27f9034.elb.us-west-2.amazonaws.com",
            "ec2.us-west-2.amazonaws.com",
            "amazonaws.com",
            // A new partition: the non-S3 host is refused there too.
            "x.execute-api.eusc-de-east-1.amazonaws.eu",
            "x.execute-api.cn-north-1.amazonaws.com.cn",
            // Bucket-carrying S3 hosts the cache key does not read the bucket from, so the
            // host's bucket would answer under the path's bucket name.
            "attacker.s3-us-west-2.amazonaws.com",
            "attacker.s3-fips.us-east-1.amazonaws.com",
            "attacker.s3-website-us-east-1.amazonaws.com",
            "attacker.s3-website.us-east-1.amazonaws.com",
            "s3-website-us-east-1.amazonaws.com",
            "ap-123456789012.s3-object-lambda.us-east-1.amazonaws.com",
            "bkt--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com",
            "s3express-control.us-east-1.amazonaws.com",
            "bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com",
            "mybucket.s3.cn-north-1.amazonaws.com.cn",
            "mybucket.s3.eusc-de-east-1.amazonaws.eu",
            // The extractors do not lowercase, so an uppercase virtual-hosted host keys
            // path-only and must not share the partition identity.
            "MYBUCKET.S3.US-EAST-1.AMAZONAWS.COM",
        ] {
            assert_eq!(
                origin_identity(host),
                None,
                "`{}` is under a partition suffix and is not an S3 endpoint whose cache key \
                 names what it serves; giving it the partition identity lets it write into \
                 other buckets' entries",
                host
            );
        }
    }

    #[test]
    fn every_listed_partition_admits_its_s3_endpoint() {
        for (host, partition) in [
            ("s3.eusc-de-east-1.amazonaws.eu", "amazonaws.eu"),
            ("s3.cn-north-1.amazonaws.com.cn", "amazonaws.com.cn"),
            ("s3.us-iso-east-1.c2s.ic.gov", "c2s.ic.gov"),
            ("s3.us-isob-east-1.sc2s.sgov.gov", "sc2s.sgov.gov"),
            ("s3.eu-isoe-west-1.cloud.adc-e.uk", "cloud.adc-e.uk"),
            ("s3.us-isof-south-1.csp.hci.ic.gov", "csp.hci.ic.gov"),
        ] {
            assert_eq!(
                origin_identity(host).as_deref(),
                Some(partition),
                "{}",
                host
            );
        }
    }

    #[test]
    fn a_host_outside_every_partition_keeps_its_own_identity() {
        // `on.aws` is a dual-stack suffix, not a partition suffix: verbatim, first claim wins.
        assert_eq!(
            origin_identity("x.lambda-url.us-east-1.on.aws").as_deref(),
            Some("x.lambda-url.us-east-1.on.aws")
        );
    }

    #[test]
    fn the_china_partition_is_a_distinct_identity() {
        let cn = origin_identity("s3.cn-north-1.amazonaws.com.cn");
        let com = origin_identity("mybucket.s3.us-east-1.amazonaws.com");
        assert_eq!(cn.as_deref(), Some("amazonaws.com.cn"));
        assert_eq!(com.as_deref(), Some("amazonaws.com"));
        assert_ne!(
            cn, com,
            "the China partition has its own bucket namespace, so it must not share an \
             identity with the commercial partition"
        );
    }

    #[test]
    fn two_non_aws_hosts_sharing_a_domain_suffix_are_distinct() {
        let a = origin_identity("minio-a.example.com");
        let b = origin_identity("minio-b.example.com");
        assert_eq!(a.as_deref(), Some("minio-a.example.com"));
        assert_eq!(b.as_deref(), Some("minio-b.example.com"));
        assert_ne!(
            a, b,
            "a suffix is not a namespace boundary outside AWS: collapsing these to \
             `example.com` could only create false matches between two separate clusters"
        );
    }

    #[test]
    fn identity_is_case_insensitive() {
        // A path-style S3 endpoint carries no bucket in the host, so case cannot move the
        // cache key and the partition identity is safe.
        assert_eq!(
            origin_identity("S3.US-EAST-1.AMAZONAWS.COM").as_deref(),
            Some("amazonaws.com")
        );
        assert_eq!(
            origin_identity("MinIO-A.Example.COM").as_deref(),
            Some("minio-a.example.com")
        );
        assert_eq!(
            origin_identity("MinIO-A.Example.COM"),
            origin_identity("minio-a.example.com"),
            "hosts are case-insensitive, so one origin must not get two identities from the \
             case a client happened to send"
        );
    }

    #[test]
    fn a_lookalike_suffix_is_not_an_aws_host() {
        // A bare `ends_with` would classify all three of these as AWS and collapse them
        // onto the commercial partition's identity.
        for host in [
            "evil-amazonaws.com",
            "notamazonaws.com",
            "amazonaws.com.evil.example.com",
        ] {
            assert_eq!(
                origin_identity(host).as_deref(),
                Some(host),
                "`{}` is not under an AWS partition and must keep its own identity",
                host
            );
        }
        // And the dot-boundary check must not reject the genuine article.
        assert_eq!(
            origin_identity("s3.amazonaws.com").as_deref(),
            Some("amazonaws.com")
        );
    }

    #[test]
    fn no_port_enters_the_identity_in_either_branch() {
        assert_eq!(
            origin_identity("minio.example.com:9000"),
            origin_identity("minio.example.com"),
            "a port must not split one host into two origins — that would invert the \
             accepted residual (two transport modes SHARE an entry) into a bypass"
        );
        assert_eq!(
            origin_identity("minio.example.com:9443"),
            origin_identity("minio.example.com:9444"),
            "the two transport modes the fleet configures on one MinIO host are one origin"
        );
        assert_eq!(
            origin_identity("s3.us-west-2.amazonaws.com:80").as_deref(),
            Some("amazonaws.com"),
            "an authority must still reach the AWS branch"
        );
        assert_eq!(
            origin_identity("mybucket.s3.us-west-2.amazonaws.com:80").as_deref(),
            Some("amazonaws.com"),
            "the port must be stripped before the extractor check, not passed to it"
        );
        // IPv6 literals, bracketed and bare.
        assert_eq!(origin_identity("[::1]:8080").as_deref(), Some("::1"));
        assert_eq!(
            origin_identity("::1").as_deref(),
            Some("::1"),
            "an unbracketed IPv6 literal must not have its last group mistaken for a port"
        );
        assert_eq!(origin_identity("fe80::1").as_deref(), Some("fe80::1"));
        assert_eq!(
            origin_identity("127.0.0.1:80").as_deref(),
            Some("127.0.0.1")
        );
    }

    // ── the marker ────────────────────────────────────────────────────────────

    #[test]
    fn an_absent_marker_is_claimed_and_then_matches() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        let path = marker_path(&metadata, "mybucket");
        assert!(!path.exists(), "fixture precondition: no marker yet");

        let state = read_or_claim(&metadata, "mybucket", "minio-a.example.com");
        assert_eq!(state, MarkerState::Recorded("minio-a.example.com".into()));
        assert!(path.exists(), "the claim must have published the marker");
        assert_eq!(
            state.verdict_for("minio-a.example.com"),
            OriginVerdict::Matches
        );

        // Read back through a fresh call — the on-disk value, not the in-memory one.
        assert_eq!(
            read_or_claim(&metadata, "mybucket", "minio-a.example.com"),
            MarkerState::Recorded("minio-a.example.com".into())
        );
    }

    #[test]
    fn a_second_origin_reads_the_recorded_one_and_mismatches() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        read_or_claim(&metadata, "mybucket", "minio-a.example.com");

        // The second origin's claim must LOSE and report the incumbent, not overwrite it.
        let state = read_or_claim(&metadata, "mybucket", "minio-b.example.com");
        assert_eq!(
            state,
            MarkerState::Recorded("minio-a.example.com".into()),
            "a later origin must not overwrite the recorded one — `link` fails EEXIST and \
             the winner's value is read instead"
        );
        assert_eq!(
            state.verdict_for("minio-b.example.com"),
            OriginVerdict::Mismatch
        );
        assert_eq!(
            state.verdict_for("minio-a.example.com"),
            OriginVerdict::Matches
        );
    }

    #[test]
    fn the_claim_leaves_no_temp_file_behind() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        read_or_claim(&metadata, "mybucket", "minio-a.example.com");
        // Both outcomes unlink the temp, so the winner's directory holds exactly the marker.
        let entries: Vec<String> = std::fs::read_dir(metadata.join("mybucket"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            entries,
            vec![MARKER_FILE_NAME.to_string()],
            "a leaked `.tmp.` file would accumulate one per instance per bucket forever, \
             since nothing sweeps this directory"
        );
        // And the loser's attempt leaves nothing either.
        read_or_claim(&metadata, "mybucket", "minio-b.example.com");
        let entries: Vec<String> = std::fs::read_dir(metadata.join("mybucket"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(entries, vec![MARKER_FILE_NAME.to_string()]);
    }

    #[test]
    fn an_unparseable_marker_bypasses_rather_than_being_ignored() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        std::fs::create_dir_all(metadata.join("mybucket")).unwrap();

        for body in [
            &b""[..],                              // the empty-file shape
            &b"not json at all"[..],               // damaged
            &br#"{"schema":99,"origin":"x"}"#[..], // a version this build cannot use
            &br#"{"schema":1,"origin":""}"#[..],   // present but says nothing
        ] {
            std::fs::write(marker_path(&metadata, "mybucket"), body).unwrap();
            let state = read_or_claim(&metadata, "mybucket", "minio-a.example.com");
            assert_eq!(
                state,
                MarkerState::Unparseable,
                "a marker that exists and cannot be used must NOT be treated as absent — \
                 that would reintroduce the cross-origin collision. Body: {:?}",
                String::from_utf8_lossy(body)
            );
            // Every origin bypasses, including the one that would otherwise match.
            assert_eq!(
                state.verdict_for("minio-a.example.com"),
                OriginVerdict::Unparseable
            );
        }
    }

    #[test]
    fn an_unparseable_marker_is_not_overwritten_by_a_claim() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        std::fs::create_dir_all(metadata.join("mybucket")).unwrap();
        let path = marker_path(&metadata, "mybucket");
        std::fs::write(&path, b"corrupt").unwrap();

        read_or_claim(&metadata, "mybucket", "minio-a.example.com");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"corrupt",
            "healing a damaged marker by claiming it would let whichever origin arrives \
             first silently rebind a bucket whose real owner is unknown"
        );
    }

    // ── claiming over entries that predate the binding ────────────────────────
    //
    // These assert the DECISION, not that a `warn!` fired. The line is the decision's only
    // caller, and a test that captured tracing output would assert that a macro ran rather
    // than that the state was read correctly. Both arms are here, and the negative one is
    // the one that matters: the condition is true of every bucket of every upgraded
    // deployment, so a check that cannot report NoneFound would fire on a first-ever
    // request as well and be a false alarm everywhere.

    /// Seed a `.meta` at the path production would use, derived from the production helper.
    ///
    /// Deliberately not a hand-written `{bucket}/XX/YYY/` — if `get_sharded_path`'s depth
    /// ever changes, a hardcoded fixture would keep passing while the detection went blind.
    fn seed_meta(metadata_dir: &Path, bucket: &str, object_key: &str) -> PathBuf {
        let path = crate::disk_cache::get_sharded_path(
            metadata_dir,
            &format!("{}/{}", bucket, object_key),
            ".meta",
        )
        .expect("the fixture's cache key must be well formed");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, br#"{"fixture":true}"#).unwrap();
        path
    }

    #[test]
    fn a_claim_over_a_directory_holding_a_meta_detects_the_pre_existing_entries() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        let seeded = seed_meta(&metadata, "mybucket", "data.bin");
        assert!(
            seeded.exists(),
            "fixture precondition: a .meta must be there"
        );
        assert!(
            !marker_path(&metadata, "mybucket").exists(),
            "fixture precondition: the pre-binding state is NO marker. With one present the \
             claim path is never reached and this asserts nothing"
        );

        assert_eq!(
            detect_pre_claim_entries(&metadata.join("mybucket")),
            PreClaimEntries::Present,
            "an entry cached before any origin was recorded is the condition the operator is \
             told about, and it is un-attributable by construction — the cache key carries no \
             origin"
        );

        // The claim still succeeds and still binds the bucket. The signal is additive.
        assert_eq!(
            read_or_claim(&metadata, "mybucket", "minio-a.example.com"),
            MarkerState::Recorded("minio-a.example.com".into())
        );
        assert!(
            seeded.exists(),
            "detecting the entry must not disturb it — this is a read"
        );
    }

    #[test]
    fn a_first_ever_claim_detects_nothing_and_an_empty_or_shard_only_directory_does_not_either() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");

        // 1. The directory does not exist at all — a first-ever request for this bucket, and
        //    the whole of a fresh deployment's cost: one `read_dir` that fails ENOENT, no
        //    traversal, no line.
        assert_eq!(
            detect_pre_claim_entries(&metadata.join("mybucket")),
            PreClaimEntries::NoneFound,
            "a bucket directory that does not exist held nothing. Warning here would fire on \
             every first request every deployment ever makes"
        );

        // 2. Present and empty.
        std::fs::create_dir_all(metadata.join("emptybucket")).unwrap();
        assert_eq!(
            detect_pre_claim_entries(&metadata.join("emptybucket")),
            PreClaimEntries::NoneFound
        );

        // 3. Present, holding shard directories, but no `.meta` — what an interrupted
        //    cleanup leaves behind after every entry for the bucket has been evicted. A
        //    directory is not an entry, which is why the evidence has to be a `.meta`.
        let shard = seed_meta(&metadata, "evictedbucket", "gone.bin");
        std::fs::remove_file(&shard).unwrap();
        assert!(shard.parent().unwrap().exists());
        assert_eq!(
            detect_pre_claim_entries(&metadata.join("evictedbucket")),
            PreClaimEntries::NoneFound,
            "empty shard directories left by eviction are not cached entries"
        );

        // 4. An interrupted metadata write, under both temp-name shapes. Neither is a
        //    published entry, and both are excluded by testing the EXTENSION rather than
        //    matching a substring.
        for tmp_name in ["gone.bin.meta.tmp", "gone.bin.meta.tmp.host-1.4242"] {
            std::fs::write(shard.parent().unwrap().join(tmp_name), b"partial").unwrap();
        }
        assert_eq!(
            detect_pre_claim_entries(&metadata.join("evictedbucket")),
            PreClaimEntries::NoneFound,
            "a `.meta.tmp` is a torn write, not an entry the claiming origin can be served"
        );

        // 5. And a stranded claim temp file beside the marker is not an entry either.
        std::fs::write(
            metadata
                .join("emptybucket")
                .join(format!("{}.tmp.host-1.4242", MARKER_FILE_NAME)),
            b"{}",
        )
        .unwrap();
        assert_eq!(
            detect_pre_claim_entries(&metadata.join("emptybucket")),
            PreClaimEntries::NoneFound
        );
    }

    #[test]
    #[cfg(unix)]
    fn an_unreadable_directory_is_not_reported_as_pre_existing_entries() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        let seeded = seed_meta(&metadata, "mybucket", "data.bin");
        let shard_l1 = seeded.parent().unwrap().parent().unwrap().to_path_buf();

        // Take away the search/read bit on the level-1 shard directory, so the `.meta` below
        // it is genuinely unreachable rather than merely absent.
        let original = std::fs::metadata(&shard_l1).unwrap().permissions();
        std::fs::set_permissions(&shard_l1, std::fs::Permissions::from_mode(0o000)).unwrap();
        let verdict = detect_pre_claim_entries(&metadata.join("mybucket"));
        std::fs::set_permissions(&shard_l1, original).unwrap();

        // Running as root defeats the fixture — the read succeeds and finds the entry. Skip
        // rather than assert, so this cannot fail for a reason unrelated to the property.
        if verdict == PreClaimEntries::Present {
            eprintln!("skipped: the permission fixture did not deny the read (running as root?)");
            return;
        }
        assert_eq!(
            verdict,
            PreClaimEntries::Unknown,
            "a directory that could not be read is not evidence of anything. Reporting it as \
             NoneFound would be equally wrong — the entry that would prove otherwise is \
             behind the failed read"
        );
        assert_ne!(
            verdict,
            PreClaimEntries::Present,
            "and it must never produce the warning: an operator would be told their bucket \
             holds un-attributable entries on the strength of a failed `read_dir`"
        );
    }

    #[test]
    fn the_scan_budget_bounds_a_claim_over_a_pathological_directory() {
        let tmp = TempDir::new().unwrap();
        let metadata = tmp.path().join("metadata");
        let bucket_dir = metadata.join("mybucket");
        // More empty directories than the budget allows reads for. The verdict must be
        // Unknown — bounded, and warning about nothing — rather than a full traversal.
        for i in 0..(PRE_CLAIM_SCAN_BUDGET + 8) {
            std::fs::create_dir_all(bucket_dir.join(format!("{:02x}", i))).unwrap();
        }
        assert_eq!(
            detect_pre_claim_entries(&bucket_dir),
            PreClaimEntries::Unknown,
            "the budget must stop the walk, and stopping early is not evidence of entries"
        );
    }

    #[test]
    fn the_two_bypassing_verdicts_are_counted_apart() {
        let mismatch_before = origin_marker_mismatch_bypasses_total();
        let unparseable_before = origin_marker_unparseable_bypasses_total();

        OriginVerdict::Mismatch.record_bypass();
        OriginVerdict::Unparseable.record_bypass();
        OriginVerdict::Matches.record_bypass();

        // `>=` rather than `==`: these are process-global counters and libtest runs this
        // binary's tests as parallel threads in one process, so an exact delta can observe
        // a concurrent test's increment. Both counters are asserted to have moved, which is
        // what distinguishes "counted apart" from "counted together".
        assert!(
            origin_marker_mismatch_bypasses_total() > mismatch_before,
            "a mismatch must move the mismatch counter"
        );
        assert!(
            origin_marker_unparseable_bypasses_total() > unparseable_before,
            "an unparseable marker must move its OWN counter, not the mismatch one — the \
             cause and the operator's fix differ"
        );
    }

    #[test]
    fn permits_cache_is_true_only_for_a_match() {
        assert!(OriginVerdict::Matches.permits_cache());
        assert!(!OriginVerdict::Mismatch.permits_cache());
        assert!(
            !OriginVerdict::Unparseable.permits_cache(),
            "bypassing is the conservative direction for a marker that cannot be read"
        );
    }
}
