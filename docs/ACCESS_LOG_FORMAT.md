# Access log format

Hybrid Cache for Amazon S3 writes an S3-style server access log, so tooling built for S3 access
logs mostly works. Two differences matter before you point a parser at it:

- The record has 25 fields and no `version_id`, so positions do not line up with S3's documented
  layout past the front of the record.
- Seven fields are never populated and always hold a literal `-`. They exist for format
  compatibility.

Application logs are a separate stream in a different format; see
[Logging configuration](CONFIGURATION.md#logging-configuration).

## Record layout

One record per line, with a single space between fields. An absent value is a literal hyphen
(`-`), never an empty field. Fields 3 (`time`), 15 (`referer`) and 16 (`user_agent`) can contain
spaces inside their brackets or quotes, so split on the delimiters rather than on whitespace.

| # | Field | Quoting | What the proxy writes |
|---|---|---|---|
| 1 | `bucket_owner` | | The bucket name, not an owner ID. The proxy cannot resolve a canonical owner |
| 2 | `bucket` | | Bucket parsed from the request URI |
| 3 | `time` | `[...]` | `[19/Aug/2026:13:01:39 +0000]`, format `%d/%b/%Y:%H:%M:%S %z`, UTC |
| 4 | `remote_ip` | | Client address as seen by the proxy |
| 5 | `requester` | | Always `-` |
| 6 | `request_id` | | A UUID v4 generated per request, not S3's request ID, so it cannot be correlated with an S3-side log |
| 7 | `operation` | | `REST.{METHOD}.OBJECT`. See the caveat below |
| 8 | `request_uri` | `"..."` | The request URI, path and query, not a `"GET /path HTTP/1.1"` request line. Presigned parameters are redacted, see below |
| 9 | `http_status` | | Status returned to the client |
| 10 | `error_code` | | The numeric status as a string (for example `404`) on any non-2xx response, `-` otherwise. Not an S3 error code like `NoSuchKey` |
| 11 | `bytes_sent` | | Response `content-length`; `0` for HEAD |
| 12 | `object_size` | | Response `content-length`, or `0` when the header is absent or unparseable |
| 13 | `total_time` | | Milliseconds from request start to response construction |
| 14 | `turn_around_time` | | Identical to `total_time`: the same value is passed to both |
| 15 | `referer` | `"..."` | Client `Referer` header, sanitized |
| 16 | `user_agent` | `"..."` | Client `User-Agent` header, sanitized |
| 17 | `host_id` | | The proxy's hostname |
| 18 | `signature_version` | | Always `-` |
| 19 | `cipher_suite` | | Always `-` |
| 20 | `authentication_type` | | Always `Anonymous` |
| 21 | `host_header` | | Client `Host` header |
| 22 | `tls_version` | | Always `-` |
| 23 | `access_point_arn` | | Always `-` |
| 24 | `acl_required` | | Always `-` |
| 25 | `source_region` | | Always `-` |

### Fields that carry no information

Fields 5, 18, 19, 22, 23, 24 and 25 are hardcoded and never populated from the request. Field 20
is hardcoded to `Anonymous`. Build no dashboard dimension on any of them.

- `tls_version` and `cipher_suite` are `-` even for requests through the TLS proxy listener
  (port 3129). Those requests go through the same logging path, but the record does not include
  the negotiated version or cipher.
- `source_region` is always `-`. It is present in every record and counted in the field total, so
  a parser has to account for it, but it carries no value.

### `operation` is always `.OBJECT`

The operation string comes from the HTTP method alone:

| Method | `operation` |
|---|---|
| `GET` | `REST.GET.OBJECT` |
| `HEAD` | `REST.HEAD.OBJECT` |
| `PUT` | `REST.PUT.OBJECT` |
| `DELETE` | `REST.DELETE.OBJECT` |
| anything else | `REST.{METHOD}.OBJECT` |

So a bucket-level list is logged as `REST.GET.OBJECT`, and a `CreateMultipartUpload` as
`REST.POST.OBJECT`. S3's own logs distinguish `REST.GET.BUCKET`, `REST.POST.UPLOAD` and similar;
this log does not. To tell operations apart, parse `request_uri` (field 8).

The per-bucket metrics counters use a different request taxonomy, which does distinguish a list
from an object GET and `UploadPart` from `PutObject`; see
[Per-bucket traffic metrics](METRICS.md).

### No `version_id` field

S3's record includes `version_id`; this one does not, and it has no separate object key field
either: the key is inside `request_uri`. Check field positions against a real record from the
proxy rather than against S3's documentation. A plain whitespace split yields at least 26 tokens,
because the `time` value always contains one space, and more when `referer` or `user_agent`
contain spaces. With `awk`, `http_status` is `$10`, not `$9`.

### Field-count changes break parsers

New fields are appended at the end, so a parser that reads the first N fields by position keeps
working after an addition, while one that asserts an exact count or indexes from the end does
not. Check [Upgrading](UPGRADING.md) when adopting a new release.

## Escaping and redaction

Three transformations run before a line is written, all to stop a client-controlled string from
corrupting the log.

### Presigned URL parameters are redacted

In field 8, these query parameters have their values replaced with the literal `REDACTED`.
Matching is case-insensitive and runs after percent-decoding the parameter name, so encoded
variants are caught:

- `X-Amz-Signature`
- `X-Amz-Credential`
- `X-Amz-Security-Token`
- `Signature` and `AWSAccessKeyId`, the SigV2 presigned URL parameters

`Expires` stays visible.

```text
"/my-bucket/key?X-Amz-Signature=REDACTED&X-Amz-Expires=3600"
```

The parameter name is written as it appeared, so a percent-encoded name stays encoded while its
value is still redacted. Other `X-Amz-*` parameters (`X-Amz-Expires`, `X-Amz-Date`,
`X-Amz-Algorithm`, `X-Amz-SignedHeaders`) are logged in full; they are not secrets. A bare
parameter with no `=` passes through unchanged.

### Free-text fields are escaped

Fields 15 (`referer`) and 16 (`user_agent`) come straight from client headers, so three
characters are escaped to keep the value inside its quotes and on one line:

| Input | Written as |
|---|---|
| CR | `\r` (a literal backslash, then `r`) |
| LF | `\n` (a literal backslash, then `n`) |
| `"` | `\"` |

A parser must un-escape these two fields and must not treat `\"` inside them as a closing quote.
No other field is escaped, because the proxy generates every other field itself.

### Nothing else is masked

Object keys are logged verbatim inside `request_uri`. If your key names embed customer
identifiers, tokens or email addresses, those reach the access log. Keep them out of key names,
or restrict access to the log directory. The proxy sets its umask to `0o077` at startup, so log
files are readable by their owner only, but that is a file permission, not redaction.

## File layout and rotation

```text
{access_log_dir}/YYYY/MM/DD/{YYYY-MM-DD-HH-MM-SS}-{hostname}
```

Files are partitioned by UTC date and have no extension. The timestamp in a file's name is the
time of the first entry in that file, and the hostname suffix keeps proxies that share a log
directory from colliding.

Within a rotation window the proxy appends to the current file. A new file starts when the date
partition rolls over or when `access_log_file_rotation_interval` elapses. A new file is created
with a temporary file and a rename, so a reader never sees a partly created file; appends within
a window are ordinary appends.

Records are buffered in RAM and flushed every `access_log_flush_interval`, or when the buffer
reaches `access_log_buffer_size` entries, whichever comes first. Two consequences follow:

- **Access logs are best-effort, as Amazon S3 server access logs are.** The proxy tries to write
  a record for every request. A request that finishes while the logger is busy hands its record
  to a background task, which waits up to 30 seconds. A record that still cannot be written, or
  that finds 10,000 records already waiting, is dropped and counted in
  `request_metrics.access_log_records_dropped`. Recent records may not be on disk until the next
  flush, so a missing record does not prove a request did not happen.
- **Records are ordered by flush batch**, not strictly by completion time. Sort on field 3 for
  chronological order.

On a graceful shutdown the buffer is flushed; if that fails, the application log reports how
many entries were lost. A record still waiting on the background hand-off when the process stops
is lost and is not counted.

## Multi-instance aggregation

Each proxy writes its own files under the same date directory, so a shared log directory holds
one file series per proxy:

```bash
cat "$ACCESS_LOG_DIR"/2026/08/19/*  # Every proxy, one date
```

Because proxies buffer independently, records from different hosts interleave in no fixed order.
Sort the whole set by field 3 rather than relying on the order within a file.

## Retention applies to the directory, not to the writer

`access_log_retention_days` is a property of `access_log_dir`. The cleanup sweep runs whether or
not this proxy writes access logs, so `access_log_enabled: false` stops new records while old
files are still removed. The first sweep removes every expired file already in the directory, so
copy off anything you need to keep, or raise `access_log_retention_days`, before starting the
proxy on an existing log directory.

Only files that match the naming convention above are removed. Anything else in `access_log_dir`
is left alone whatever its age, so an unrelated file placed there is never deleted.

The sweep also covers files written by other proxies, so the files of a decommissioned host are
still removed. A file this proxy did not write gets one extra day of grace before it becomes a
candidate, so a badly skewed clock on one host cannot reach a live peer's files that are still
within retention:

| File | Deleted after |
|---|---|
| Written by this proxy | `access_log_retention_days` |
| Written by another proxy, including a decommissioned one | `access_log_retention_days` + 1 day |

For a compliance-driven hard limit of N days, configure `N - 1`. The extra day of grace is not
configurable.

The same rules apply to `app_log_retention_days` and `app_log_dir`, including the per-hostname
subdirectories the application log uses.

## Related configuration

| Field | Effect |
|---|---|
| `logging.access_log_dir` | Root directory |
| `logging.access_log_enabled` | `false` writes no access log. Retention cleanup still runs |
| `logging.access_log_mode` | `all`, or `cached_only` to log only cache hits |
| `logging.access_log_flush_interval` | How often the buffer is flushed |
| `logging.access_log_buffer_size` | Entries before a forced flush |
| `logging.access_log_file_rotation_interval` | How often a new file starts within a date |
| `logging.access_log_retention_days` | Age at which the cleanup task deletes files matching the naming convention, anywhere in `access_log_dir`. Add a day for files written by another proxy |

Only the directory has an environment variable override (`ACCESS_LOG_DIR`); set the others in the
configuration file. See
[Environment variable reference](CONFIGURATION.md#environment-variable-reference).

## See also

- [Logging configuration](CONFIGURATION.md#logging-configuration): every `logging.*` field
- [Metrics reference](METRICS_REFERENCE.md): counters, which are cheaper than parsing logs for
  aggregate figures
- [Per-bucket traffic metrics](METRICS.md): per-bucket accounting, including the request taxonomy
  that `operation` does not reflect
- [Upgrading](UPGRADING.md): releases that changed the record layout
