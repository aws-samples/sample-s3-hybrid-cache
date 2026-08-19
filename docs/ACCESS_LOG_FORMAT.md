# Access Log Format

The proxy writes an S3-style server access log, so tooling built for S3 access logs
mostly works. Two differences matter before you point a parser at it:

- The record has **25 space-separated fields** and has **no `version_id`**, so positions
  do not line up with S3's documented layout past the front of the record.
- **Seven fields are never populated** and are always a literal `-`. They exist for
  format compatibility, not because the proxy knows their value.

Application logs are a separate stream in a different format; see
[CONFIGURATION.md — Logging Configuration](CONFIGURATION.md#logging-configuration).

## Record layout

One record per line, single space between fields. Absent values are a literal hyphen
(`-`), never a zero-width field.

| # | Field | Quoting | What the proxy actually writes |
|---|---|---|---|
| 1 | `bucket_owner` | | **The bucket name**, not an owner ID. The proxy has no way to resolve a canonical owner |
| 2 | `bucket` | | Bucket parsed from the request URI |
| 3 | `time` | `[...]` | `[19/Aug/2026:13:01:39 +0000]`, format `%d/%b/%Y:%H:%M:%S %z`, UTC |
| 4 | `remote_ip` | | Client address as seen by the proxy |
| 5 | `requester` | | Always `-` |
| 6 | `request_id` | | A UUID v4 generated per request. **Not** S3's request ID, so it cannot be correlated with an S3-side log |
| 7 | `operation` | | `REST.{METHOD}.OBJECT`. See the caveat below |
| 8 | `request_uri` | `"..."` | The request URI, path and query. **Not** a `"GET /path HTTP/1.1"` request line. Presigned params redacted, see below |
| 9 | `http_status` | | Status returned to the client |
| 10 | `error_code` | | **The numeric status as a string** (e.g. `404`) on any non-2xx, `-` otherwise. Not an S3 error code like `NoSuchKey` |
| 11 | `bytes_sent` | | Response `content-length`; `0` for HEAD |
| 12 | `object_size` | | Response `content-length`, or `0` when the header is absent or unparseable |
| 13 | `total_time` | | Milliseconds from request start to response construction |
| 14 | `turn_around_time` | | **Identical to `total_time`** — the same value is passed to both |
| 15 | `referer` | `"..."` | Client `Referer` header, sanitized |
| 16 | `user_agent` | `"..."` | Client `User-Agent` header, sanitized |
| 17 | `host_id` | | The proxy instance's hostname |
| 18 | `signature_version` | | Always `-` |
| 19 | `cipher_suite` | | Always `-` |
| 20 | `authentication_type` | | Always `Anonymous` |
| 21 | `host_header` | | Client `Host` header |
| 22 | `tls_version` | | Always `-` |
| 23 | `access_point_arn` | | Always `-` |
| 24 | `acl_required` | | Always `-` |
| 25 | `source_region` | | Always `-` |

### Fields that carry no information

Fields 5, 18, 19, 22, 23, 24, and 25 are hardcoded and never populated from the request.
Field 20 is hardcoded to `Anonymous`. Do not build a dashboard dimension on any of them.

Two of these are worth calling out because their names suggest otherwise:

- **`tls_version` and `cipher_suite` are `-` even on the TLS-terminating listener.**
  Traffic through `tls_proxy_port` is decrypted by the proxy and does route through the
  same logging path, but the negotiated version and cipher are not threaded into the log
  record. There is currently no way to see TLS parameters in the access log.
- **`source_region` was added in 1.6.8 and is always `-`.** `UPGRADING.md` correctly flags
  that release as changing the field count, so parsers did need updating, but the field
  has never carried a value.

### `operation` is always `.OBJECT`

The operation string is derived from the HTTP method alone:

| Method | `operation` |
|---|---|
| `GET` | `REST.GET.OBJECT` |
| `HEAD` | `REST.HEAD.OBJECT` |
| `PUT` | `REST.PUT.OBJECT` |
| `DELETE` | `REST.DELETE.OBJECT` |
| anything else | `REST.{METHOD}.OBJECT` |

So a bucket-level list is logged as `REST.GET.OBJECT`, and a multipart
`CreateMultipartUpload` as `REST.POST.OBJECT`. S3's own logs distinguish
`REST.GET.BUCKET`, `REST.POST.UPLOAD`, and similar; this log does not. To tell operations
apart, parse `request_uri` (field 8) rather than trusting `operation`.

Note this differs from the request taxonomy used for the per-bucket metrics counters,
which *does* discriminate list from object GET and `UploadPart` from `PutObject`. See
[METRICS.md](METRICS.md).

### No `version_id` field

S3's record includes `version_id`; this one does not, and it carries no separate object
key field either — the key is inside `request_uri`. Verify field positions against a real
record from this proxy rather than against S3's documentation. A whitespace split yields
25 tokens on every record, including ones where most optional values are `-`.

### Field-count changes break parsers

New fields are appended at the end, so a parser reading the first N fields positionally
survives an addition, while one asserting an exact count or indexing from the end does
not. `source_region` (field 25) arrived in 1.6.8. Check
[UPGRADING.md](UPGRADING.md) when adopting a new release.

## Escaping and redaction

Three transformations are applied before a line is written, all to stop a
client-controlled string from corrupting the log.

### Presigned URL parameters are redacted

In field 8, three query parameters have their values replaced with the literal
`REDACTED`. Matching is case-insensitive and applied after percent-decoding the parameter
name, so encoded variants are caught:

- `X-Amz-Signature`
- `X-Amz-Credential`
- `X-Amz-Security-Token`

```
"/my-bucket/key?X-Amz-Signature=REDACTED&X-Amz-Expires=3600"
```

The parameter *name* is written as it appeared, so a percent-encoded name stays encoded
while its value is still redacted. Other `X-Amz-*` parameters — `X-Amz-Expires`,
`X-Amz-Date`, `X-Amz-Algorithm`, `X-Amz-SignedHeaders` — are logged in full; they are not
secrets. A bare parameter with no `=` passes through unchanged.

### Free-text fields are escaped

Fields 15 (`referer`) and 16 (`user_agent`) come straight from client headers, so three
characters are escaped to keep the value inside its quotes and on one line:

| Input | Written as |
|---|---|
| CR | `\r` (literal backslash then `r`) |
| LF | `\n` (literal backslash then `n`) |
| `"` | `\"` |

A parser must un-escape these two fields, and must not treat `\"` inside them as a
closing quote. No other field is escaped, because every other field is proxy-generated.

### Nothing else is masked

Object keys are logged verbatim inside `request_uri`. If your key namespace embeds
customer identifiers, tokens, or email addresses, those reach the access log. Keep them
out of key names or restrict access to the log directory. The proxy sets its umask to
`0o077` at startup so log files are owner-only, but that is a filesystem control, not
redaction.

## File layout and rotation

```
{access_log_dir}/YYYY/MM/DD/{YYYY-MM-DD-HH-MM-SS}-{hostname}
```

Partitioned by UTC date. The filename timestamp is the time of the **first entry** in
that file; the hostname suffix keeps instances sharing a volume from colliding. Files
have no extension.

Within a rotation window the proxy appends to the current file. A new file starts when
the date partition rolls over or when `access_log_file_rotation_interval` elapses. A new
file is created via temp-file-plus-rename, so a reader never sees a partially created
file; appends within a window are ordinary appends.

Records are buffered in RAM and flushed on `access_log_flush_interval`, or when the
buffer reaches `access_log_buffer_size` entries, whichever comes first. Two consequences:

- **Recent requests may not be on disk.** Absence of a record is not evidence a request
  did not happen until a flush interval has passed.
- **Records are ordered by flush batch**, not strictly by completion time. Sort on field
  3 if you need chronological order.

On graceful shutdown the buffer is flushed; if that fails, the count of lost entries is
reported in the application log.

## Multi-instance aggregation

Each instance writes its own filename under the same date directory, so a shared volume
accumulates one file series per host:

```bash
cat "$ACCESS_LOG_DIR"/2026/08/19/*   # Every instance, one date
```

Because instances buffer independently, records from different hosts interleave
arbitrarily. Sort by field 3 across the whole set rather than relying on per-file order.

## Related configuration

| Field | Effect |
|---|---|
| `logging.access_log_dir` | Root directory |
| `logging.access_log_enabled` | `false` writes no access log at all |
| `logging.access_log_mode` | `all`, or `cached_only` to log only cache hits |
| `logging.access_log_flush_interval` | Buffer flush cadence |
| `logging.access_log_buffer_size` | Entries before a forced flush |
| `logging.access_log_file_rotation_interval` | New-file cadence within a date |
| `logging.access_log_retention_days` | Age at which the cleanup task deletes files |

Only the directory has an environment override (`ACCESS_LOG_DIR`). The others must be set
in the config file — see
[CONFIGURATION.md — Environment Variable Reference](CONFIGURATION.md#environment-variable-reference).

## See also

- [CONFIGURATION.md — Logging Configuration](CONFIGURATION.md#logging-configuration) — every `logging.*` field
- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) — counters, which are cheaper than parsing logs for aggregate figures
- [METRICS.md](METRICS.md) — per-bucket traffic accounting, including the request taxonomy that `operation` does not reflect
- [UPGRADING.md](UPGRADING.md) — releases that changed the record layout
