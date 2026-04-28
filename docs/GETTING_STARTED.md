# Getting Started with S3 Proxy

Quick start guide for installing, configuring, and running S3 Proxy.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [1. Clone and Build](#1-clone-and-build)
  - [2. Start the Proxy](#2-start-the-proxy)
  - [3. Configure Client Routing](#3-configure-client-routing)
    - [Option A: HTTP_PROXY (Single-Instance / No DNS Changes)](#option-a-http_proxy-single-instance--no-dns-changes)
    - [Option B: DNS Zone (Production Deployments)](#option-b-dns-zone-production-deployments)
    - [Option C: Hosts File (Testing/Development)](#option-c-hosts-file-testingdevelopment)
    - [S3 PrivateLink (Interface VPC Endpoints)](#s3-privatelink-interface-vpc-endpoints)
    - [Access Point and MRAP Endpoint Requirements](#access-point-and-mrap-endpoint-requirements)
    - [Access Point Alias Usage](#access-point-alias-usage)
    - [MRAP Alias Usage](#mrap-alias-usage)
    - [Access Point Endpoint Summary](#access-point-endpoint-summary)
- [Basic Usage](#basic-usage)
  - [Upload and Download Files](#upload-and-download-files)
- [Simplifying Client Configuration](#simplifying-client-configuration)
  - [AWS CLI Environment Variable](#aws-cli-environment-variable)
  - [s5cmd Configuration](#s5cmd-configuration)
  - [Minio Go Client (Forgejo, Gitea, etc.)](#minio-go-client-forgejo-gitea-etc)
  - [boto3 Per-Client Configuration](#boto3-per-client-configuration)
  - [Mountpoint for Amazon S3](#mountpoint-for-amazon-s3)
  - [Shell Profile Configuration](#shell-profile-configuration)
  - [Check Cache Performance](#check-cache-performance)
- [Port Configuration Options](#port-configuration-options)
  - [Recommended: Standard Ports (80/443)](#recommended-standard-ports-80443)
  - [Custom Ports](#custom-ports)
- [Proxy-Only Mode](#proxy-only-mode)
  - [Configuration](#configuration)
  - [Client Configuration](#client-configuration)
  - [High Availability: Shared NFS Cache Pattern](#high-availability-shared-nfs-cache-pattern)
- [Configuration Files](#configuration-files)
- [Verification](#verification)
  - [Test Basic Functionality](#test-basic-functionality)
  - [Monitor Logs](#monitor-logs)
- [Troubleshooting](#troubleshooting)
  - [Common Issues](#common-issues)
  - [Debug Mode](#debug-mode)
- [Next Steps](#next-steps)
- [Performance Tips](#performance-tips)
- [System Requirements and Sizing](#system-requirements-and-sizing)

## Prerequisites

- **Rust 1.70+** toolchain
- **sudo access** (required for ports 80/443)

## Installation

### 1. Clone and Build

```bash
# Clone repository
git clone <repository>
cd s3-proxy

# Build release binary
cargo build --release
```

### 2. Start the Proxy

```bash
# Start on standard ports 80/443 (requires sudo)
sudo cargo run --release -- -c config/config.example.yaml
```

The proxy starts on:
- **HTTP**: `<proxy-ip>:80` (caching enabled)
- **HTTPS**: `<proxy-ip>:443` (passthrough)
- **TLS Proxy**: `<proxy-ip>:3129` (TLS-terminated caching, when enabled)
- **Health**: `<proxy-ip>:8080/health`
- **Dashboard**: `<proxy-ip>:8081` (real-time statistics)
- **Metrics**: `<proxy-ip>:9090/metrics`

**Security Note**: All communication between the proxy and Amazon S3 uses HTTPS encryption, regardless of whether clients connect via HTTP or HTTPS. Secure client-to-proxy HTTP traffic using network controls (VPC, security groups, firewall rules).

### 3. Configure Client Routing

The proxy intercepts S3 traffic by routing client requests to the proxy's IP address. Choose the appropriate method for your environment:

- **Option A (HTTP_PROXY)** — set an environment variable for single-instance setups with no DNS changes
- **Option B (DNS Zone)** — configure DNS for production multi-instance deployments
- **Option C (Hosts File)** — edit `/etc/hosts` for local testing

#### Option A: HTTP_PROXY (Single-Instance / No DNS Changes)

Instead of modifying DNS or hosts files, you can route S3 traffic through the proxy using the `HTTP_PROXY` environment variable. The client sends requests with absolute URIs (`GET http://s3.amazonaws.com/bucket/key`), and the proxy extracts the target host from the URI and processes the request through the caching pipeline.

**When to use HTTP_PROXY vs DNS routing:**

| Approach | Best for | Trade-offs |
|----------|----------|------------|
| `HTTP_PROXY` | Single-instance, dev/test, quick setup | No HA — if the proxy is down, clients fail. No multi-instance load balancing. |
| DNS routing (Option B/C) | Production, multi-instance, HA | Requires DNS infrastructure. Supports multi-value A records for load balancing and failover. |

**Unencrypted (private networks):**

```bash
export HTTP_PROXY=http://<proxy-ip>:80
export NO_PROXY=169.254.169.254  # Exclude IMDS from proxying
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

**Encrypted (recommended):**

```bash
export HTTP_PROXY=https://<proxy-ip>:3129
export NO_PROXY=169.254.169.254  # Exclude IMDS from proxying
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

The `--endpoint-url http://s3.region.amazonaws.com` is required so the SDK signs the request against the real S3 hostname (`Host: s3.us-east-1.amazonaws.com`), not the proxy hostname. SigV4 signatures are computed over HTTP-level content (Host header, path, query string), not the transport layer, so they remain valid regardless of whether the client connects via HTTP or TLS.

`NO_PROXY=169.254.169.254` prevents EC2 instance metadata (IMDS) requests from being sent through the proxy. Without this, credential retrieval from the instance metadata service will fail.

**Why `HTTP_PROXY`, not `HTTPS_PROXY`?** When `HTTPS_PROXY` is set, the SDK sends a `CONNECT` request asking the proxy to establish a TCP tunnel to S3. The traffic inside the tunnel is end-to-end encrypted between the client and S3 — the proxy cannot decrypt, inspect, or cache it. The TLS proxy listener handles `CONNECT` as a passthrough tunnel (same as port 443), so the request succeeds but bypasses the cache entirely. Use `HTTP_PROXY` instead. The `HTTP_PROXY` variable controls where the SDK sends HTTP requests, regardless of whether the proxy URL uses `http://` or `https://`. With `HTTP_PROXY=https://proxy:3129`, the client opens a TLS connection to the proxy, then sends plaintext HTTP inside that tunnel. The proxy decrypts the TLS layer and sees the HTTP request for caching.

##### TLS Proxy Listener Configuration

To use `HTTP_PROXY=https://...`, enable the TLS proxy listener in your config:

```yaml
server:
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/proxy/tls/cert.pem"
    key_path: "/etc/proxy/tls/key.pem"
```

The `tls_proxy_port` is distinct from `https_port` — the HTTPS port (443) does TCP passthrough without caching, while the TLS proxy port terminates TLS using the proxy's own certificate and processes decrypted HTTP through the caching pipeline.

##### Generating a Self-Signed Certificate

For development or testing, generate a self-signed certificate with `openssl`:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=proxy-host" \
  -addext "subjectAltName=DNS:proxy-host,IP:10.0.1.101"
```

The `-addext "subjectAltName=..."` entries must match how clients connect to the proxy:
- `IP:10.0.1.101` — clients connecting by IP address (e.g., `HTTP_PROXY=https://10.0.1.101:3129`). Use this for direct connections to proxy instances.
- `DNS:proxy-host` — clients connecting by hostname (e.g., `HTTP_PROXY=https://proxy-host:3129`). Use this when clients connect through a DNS name or load balancer.

Add multiple SAN entries to cover all proxy instances or connection methods. For example, a multi-instance deployment where clients connect by IP:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=s3-proxy" \
  -addext "subjectAltName=IP:10.0.1.101,IP:10.0.1.102,IP:10.0.1.103"
```

Or when clients connect through a load balancer or DNS name:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=s3-proxy.internal" \
  -addext "subjectAltName=DNS:s3-proxy.internal,DNS:*.s3-proxy.internal"
```

Without a matching SAN, TLS clients will reject the certificate with a hostname verification error.

##### Configuring Clients to Trust the Certificate

AWS CLI and SDKs need to trust the proxy's certificate. Two options:

```bash
export AWS_CA_BUNDLE=/path/to/cert.pem  # Tell the SDK to trust this CA
```

Or for quick testing (disables all TLS verification — not for production):

```bash
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --no-verify-ssl
```

For production, use a certificate signed by your organization's internal CA, or add the self-signed certificate to the system trust store.

#### Option B: DNS Zone (Production Deployments)

For production deployments, configure your DNS server to resolve S3 endpoints to your proxy instances. This provides centralized management and load balancing.

The example below uses AWS Route 53 private hosted zones, but the same approach works with any DNS hosting.

**Architecture**: Designate one S3 regional zone as the "primary" — it holds the multi-value A records for your proxy instances. All other zones (additional regions, access points, MRAPs) use wildcard CNAMEs that point to the primary zone's apex. This keeps proxy IPs defined in as few places as possible.

> **Why apex A records are duplicated**: DNS does not allow CNAME records at a zone apex. Each hosted zone must have its own apex A records. Wildcard records (e.g., `*.s3.us-west-2.amazonaws.com`) can be CNAMEs, so those chain back to the primary zone. When adding or removing proxy instances, update the A records in each zone's apex.

**1. Create the primary S3 regional zone:**

```bash
aws route53 create-hosted-zone \
  --name s3.us-west-2.amazonaws.com \
  --vpc VPCRegion=us-west-2,VPCId=vpc-xxxxxxxx \
  --caller-reference "s3-proxy-$(date +%s)" \
  --hosted-zone-config PrivateZone=true \
  --no-cli-pager
```

**2. Add A records and wildcard CNAME to the primary zone:**

Create `dns-primary.json`:

```json
{
    "Changes": [
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy1",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.101"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy2",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.102"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "*.s3.us-west-2.amazonaws.com",
                "Type": "CNAME",
                "TTL": 60,
                "ResourceRecords": [{"Value": "s3.us-west-2.amazonaws.com"}]
            }
        }
    ]
}
```

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id Z_PRIMARY_ZONE_ID \
  --change-batch file://dns-primary.json \
  --no-cli-pager
```

**3. Create zones for access points, MRAPs, and additional regions:**

Each zone needs apex A records (same IPs as the primary) and a wildcard CNAME that chains to the primary zone's apex. The VPC resolver walks the CNAME chain across private hosted zones, so `*.s3-accesspoint.us-west-2.amazonaws.com` → `s3.us-west-2.amazonaws.com` → proxy IPs.

```bash
aws route53 create-hosted-zone \
  --name s3-accesspoint.us-west-2.amazonaws.com \
  --vpc VPCRegion=us-west-2,VPCId=vpc-xxxxxxxx \
  --caller-reference "s3-proxy-ap-$(date +%s)" \
  --hosted-zone-config PrivateZone=true \
  --no-cli-pager

aws route53 create-hosted-zone \
  --name accesspoint.s3-global.amazonaws.com \
  --vpc VPCRegion=us-west-2,VPCId=vpc-xxxxxxxx \
  --caller-reference "s3-proxy-mrap-$(date +%s)" \
  --hosted-zone-config PrivateZone=true \
  --no-cli-pager
```

For each zone, create a change batch with apex A records and a wildcard CNAME to the primary. Example for the access point zone (`dns-accesspoint.json`):

```json
{
    "Changes": [
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3-accesspoint.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy1",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.101"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3-accesspoint.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy2",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.102"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "*.s3-accesspoint.us-west-2.amazonaws.com",
                "Type": "CNAME",
                "TTL": 60,
                "ResourceRecords": [{"Value": "s3.us-west-2.amazonaws.com"}]
            }
        }
    ]
}
```

The MRAP zone (`dns-mrap.json`) follows the same pattern — apex A records plus wildcard CNAME to `s3.us-west-2.amazonaws.com`. Additional regional zones (e.g., `s3.eu-west-1.amazonaws.com`) use the same template: apex A records with the proxy IPs, wildcard CNAME → `s3.us-west-2.amazonaws.com`.

**DNS zone summary:**

| Zone | Apex Records | Wildcard CNAME Target |
|------|-------------|----------------------|
| `s3.us-west-2.amazonaws.com` (primary) | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` (self) |
| `s3-accesspoint.us-west-2.amazonaws.com` | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` |
| `accesspoint.s3-global.amazonaws.com` | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` |
| `s3.{other-region}.amazonaws.com` | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` |

**Adding or removing a proxy instance**: Update the apex A records in each zone. The wildcard CNAMEs require no changes — they resolve through the chain automatically.

**Key DNS configuration concepts:**

| Concept | Purpose |
|---------|---------|
| Multi-value A records | Load balancing across proxy instances |
| Wildcard CNAME chain | Routes all subdomains (buckets, access points) to proxy IPs via the primary zone |
| Short TTL (60s) | Enables quick failover if a proxy becomes unavailable |

This approach mirrors how [S3 itself uses multi-value answer routing for DNS queries](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-s3-multivalue-answer-response-dns-queries/). The AWS Common Runtime (CRT), available as a transfer client in AWS CLI v2 and modern SDKs, resolves all IPs from DNS, distributes requests across them, and retries against alternate IPs on connection failure — providing load balancing and failover without an external load balancer.

**Multi-Instance Shared Storage**: When running multiple proxy instances with shared NFS storage, the volume MUST be mounted with `lookupcache=pos` for reliable cache coordination. See [Configuration Guide - Multi-Instance Coordination](CONFIGURATION.md#multi-instance-coordination) for details.

#### Option C: Hosts File (Testing/Development)

For local testing or single-machine setups, edit `/etc/hosts`:

```bash
sudo nano /etc/hosts

# Add entries for your region's S3 endpoints
127.0.0.1 s3.amazonaws.com
127.0.0.1 s3.us-east-1.amazonaws.com
127.0.0.1 s3.eu-west-1.amazonaws.com
# Add other regions as needed

# For bucket-style URLs, add each bucket explicitly
127.0.0.1 your-bucket.s3.us-east-1.amazonaws.com
127.0.0.1 another-bucket.s3.us-east-1.amazonaws.com

# For S3 Access Points, add each access point explicitly
# Regional access points: {name}-{account_id}.s3-accesspoint.{region}.amazonaws.com
127.0.0.1 my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com
127.0.0.1 other-ap-123456789012.s3-accesspoint.eu-west-1.amazonaws.com

# Multi-Region Access Points (MRAPs): {mrap_alias}.accesspoint.s3-global.amazonaws.com
127.0.0.1 mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com
```

**Important Limitations**: 
- Hosts files don't support wildcards, so you must add each bucket and access point individually
- Hosts files don't support multi-value responses, preventing load distribution across multiple proxy instances
- No high availability - if the single proxy instance fails, all S3 traffic fails
- For production deployments with multiple proxy instances or high availability requirements, use DNS-based routing instead

See [AWS S3 endpoints documentation](https://docs.aws.amazon.com/general/latest/gr/s3.html) for complete list of regional endpoints.

#### S3 PrivateLink (Interface VPC Endpoints)

If your environment uses S3 interface VPC endpoints (PrivateLink), the proxy needs additional configuration to route traffic to S3 through the private endpoint rather than over the public internet.

Client-to-proxy DNS routing (Option B or C above) remains unchanged. PrivateLink only affects how the proxy resolves and connects to S3 on the backend.

**The problem**: By default, the proxy uses external DNS servers (Google DNS, Cloudflare DNS) to resolve S3 endpoints. These public resolvers return public S3 IPs, bypassing your PrivateLink endpoints entirely. Traffic would leave your private network instead of using the private path.

**The fix**: Configure `dns_servers` to point at Route 53 Resolver inbound endpoints in the VPC. These are ENIs on your private subnets, reachable from on-premises over VPN or Direct Connect. The inbound endpoint queries the VPC's internal resolver, which returns PrivateLink IPs when private DNS is enabled on the interface endpoint.

```yaml
connection_pool:
  dns_servers: ["10.0.1.50", "10.0.2.50"]  # Route 53 Resolver inbound endpoint IPs
```

The proxy cannot use your on-prem DNS server for this, because on-prem DNS resolves S3 endpoints to the proxy's own IP (for client routing). Pointing `dns_servers` at on-prem DNS would create a loop. The Route 53 Resolver inbound endpoint provides a separate resolution path that returns the PrivateLink IPs.

**Requirement**: This approach requires Route 53 Resolver inbound endpoints in the VPC. If you don't have Route 53 Resolver, use `endpoint_overrides` to map S3 hostnames directly to PrivateLink ENI IPs:

```yaml
connection_pool:
  endpoint_overrides:
    # Suffix patterns cover all bucket/AP hostnames in a region
    "*.s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]
    # MRAP global endpoint (separate VPCE: com.amazonaws.s3-global.accesspoint)
    "*.accesspoint.s3-global.amazonaws.com": ["10.0.3.100"]
```

Keys starting with `*.` are suffix patterns — they match any hostname ending with that suffix. Exact-match keys (without `*.`) are also supported and take precedence. See [Configuration Guide - endpoint_overrides](CONFIGURATION.md#s3-privatelink-interface-vpc-endpoints) for the full syntax and precedence rules.

When any `endpoint_overrides` are configured, outbound TLS is locked to version 1.2 for VPC interface endpoint compatibility.

**Verifying PrivateLink resolution**: From the proxy host, confirm the inbound endpoint returns private IPs for S3 endpoints:

```bash
dig +short s3.us-west-2.amazonaws.com @10.0.1.50
```

This should return private IPs (e.g., `10.0.x.x` or `172.x.x.x`) from the interface endpoint ENIs, not public S3 IPs.

**Private DNS disabled**: If the interface endpoint has private DNS disabled, S3 endpoints resolve to public IPs even through the VPC resolver. In this case, use the endpoint-specific DNS names (e.g., `*.vpce-0abc123def456.s3.us-west-2.vpce.amazonaws.com`) or enable private DNS on the endpoint.

**HTTPS passthrough**: The `dns_servers` config applies to the HTTP connection pool. The HTTPS passthrough handler (port 443) uses its own DNS resolver hardcoded to Google/Cloudflare DNS. For full PrivateLink coverage of HTTPS traffic, ensure the proxy instances can reach the PrivateLink ENIs via network routing, or use HTTP endpoints for all cached traffic.

#### Access Point and MRAP Endpoint Requirements

S3 Access Points and Multi-Region Access Points (MRAPs) require different client configuration than regular bucket requests.

**Why `AWS_ENDPOINT_URL_S3` doesn't work for access points**: The AWS SDK resolves access point ARNs to their own hostnames (e.g., `my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com`), bypassing `AWS_ENDPOINT_URL_S3`. Each access point request must specify `--endpoint-url` explicitly.

**Simplified endpoint approach**: The `--endpoint-url` does not need the full access-point-specific hostname. The base regional endpoint works for all access points in a region:

```bash
--endpoint-url http://s3-accesspoint.{region}.amazonaws.com
```

The SDK resolves the ARN from the S3 URI, constructs the correct Host header (e.g., `my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com`), and uses the `--endpoint-url` only as the connection target.

**Example — download via regional access point:**

```bash
aws s3 cp s3://arn:aws:s3:eu-west-1:123456789012:accesspoint/my-ap/bigfiles/5GB ~/tmp/ \
  --region eu-west-1 \
  --endpoint-url http://s3-accesspoint.eu-west-1.amazonaws.com
```

The ARN in the S3 URI provides the SDK with all information needed to build the correct Host header and SigV4 signature. The `--endpoint-url` only controls where the HTTP connection goes (i.e., the proxy).

**MRAP requests** follow the same pattern but use the global endpoint:

```bash
aws s3 cp s3://arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw/bigfiles/5GB ~/tmp/ \
  --endpoint-url http://accesspoint.s3-global.amazonaws.com
```

The AWS CLI automatically uses SigV4A (`AWS4-ECDSA-P256-SHA256`) for MRAP requests. The proxy recognizes SigV4A signatures and handles them identically to classic SigV4 — signed headers are preserved, range signatures are detected, and referer injection respects the `SignedHeaders` list.

#### Access Point Alias Usage

S3 Access Points can be accessed by alias (e.g., `my-ap-abcdef123456-s3alias`) instead of ARN. The simplest way to cache AP alias requests through the proxy is to use the regular S3 regional endpoint:

```bash
# AP alias via regular S3 endpoint (recommended for caching)
aws s3 cp s3://my-ap-abcdef123456-s3alias/data/file.txt ~/tmp/ \
  --region us-east-1 \
  --endpoint-url http://s3.us-east-1.amazonaws.com
```

The AWS CLI resolves the alias to a virtual-hosted Host header (`my-ap-abcdef123456-s3alias.s3.us-east-1.amazonaws.com`), which the Route 53 wildcard `*.s3.{region}.amazonaws.com` routes to the proxy. S3 accepts AP alias bucket names on the regular S3 endpoint.

**Why not `--endpoint-url http://s3-accesspoint.{region}.amazonaws.com`?**

The base domain `s3-accesspoint.{region}.amazonaws.com` does not resolve to S3 IPs via public DNS — it only exists as a base for virtual-hosted subdomains. The proxy uses external DNS (Google/Cloudflare) to resolve upstream endpoints, so it cannot connect to this hostname. Use the regular S3 endpoint instead.

**ARN-based access point requests** work with the AP-specific endpoint because the CLI constructs the full virtual-hosted hostname from the ARN:

```bash
# ARN-based AP access (works with AP endpoint)
aws s3 cp s3://arn:aws:s3:eu-west-1:123456789012:accesspoint/my-ap/data/file.txt ~/tmp/ \
  --region eu-west-1 \
  --endpoint-url http://s3-accesspoint.eu-west-1.amazonaws.com
```

#### MRAP Alias Usage

Multi-Region Access Point aliases work similarly. Use the MRAP-specific endpoint with the full alias:

```bash
# MRAP alias via MRAP endpoint
aws s3 cp s3://mfzwi23gnjvgw.mrap/data/file.txt ~/tmp/ \
  --endpoint-url http://accesspoint.s3-global.amazonaws.com
```

The CLI resolves the alias to `mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com`, which the Route 53 wildcard routes to the proxy.

#### Access Point Endpoint Summary

| Access Pattern | Endpoint URL | Cached? | Notes |
|---|---|---|---|
| AP alias | `http://s3.{region}.amazonaws.com` | ✅ Yes | Alias resolves under `*.s3.{region}` wildcard. Proxy can resolve this upstream. |
| AP ARN | `http://s3.{region}.amazonaws.com` | ✅ Yes | CLI builds Host from ARN. Either endpoint works for ARN-based requests. |
| AP ARN | `http://s3-accesspoint.{region}.amazonaws.com` | ✅ Yes | CLI builds Host from ARN. Either endpoint works for ARN-based requests. |
| AP alias | `http://s3-accesspoint.{region}.amazonaws.com` | ❌ No | Bare `s3-accesspoint` domain doesn't resolve via public DNS; proxy can't connect upstream. |
| MRAP alias | `http://accesspoint.s3-global.amazonaws.com` | ✅ Yes | CLI builds correct Host from alias. |
| MRAP ARN | `http://accesspoint.s3-global.amazonaws.com` | ✅ Yes | CLI builds correct Host from ARN. |
| Any AP/MRAP | *(none — default HTTPS)* | ❌ No | HTTPS uses TCP passthrough, bypassing cache. |

## Basic Usage

### Upload and Download Files

```bash
# Upload a file (write-through cached - stored in S3 and cache)
aws s3 cp file.txt s3://your-bucket/key \
  --endpoint-url "http://s3.us-east-1.amazonaws.com" \
  --region us-east-1

# Download the same file (cache hit - served from proxy)
aws s3 cp s3://your-bucket/key ./downloaded.txt \
  --endpoint-url "http://s3.us-east-1.amazonaws.com" \
  --region us-east-1
```

## Simplifying Client Configuration

Instead of adding `--endpoint-url` to every AWS CLI command, you can set environment variables to automatically route S3 traffic through the proxy.

### AWS CLI Environment Variable

Set `AWS_ENDPOINT_URL_S3` to use HTTP for all S3 operations (example for us-east-1):

```bash
export AWS_ENDPOINT_URL_S3=http://s3.us-east-1.amazonaws.com
```

Now AWS CLI S3 commands use the proxy without the `--endpoint-url` flag:

```bash
# Works for buckets in us-east-1
aws s3 cp file.txt s3://your-bucket/key
aws s3 ls s3://your-bucket/
```

**Region Limitation**: The environment variable only works for buckets in the endpoint's region. For buckets in other regions, you must specify both `--region` and `--endpoint-url`:

```bash
# Bucket in eu-west-1 (different from environment variable region)
aws s3 cp file.txt s3://eu-bucket/key \
  --region eu-west-1 \
  --endpoint-url http://s3.eu-west-1.amazonaws.com
```

### s5cmd Configuration

[s5cmd](https://github.com/peak/s5cmd) is a high-performance S3 client that has been tested with the proxy. Use the `S3_ENDPOINT_URL` environment variable:

```bash
export S3_ENDPOINT_URL=http://s3.us-east-1.amazonaws.com
```

Then use s5cmd normally:

```bash
s5cmd cp file.txt s3://your-bucket/key
s5cmd ls s3://your-bucket/
```

The same region limitation applies - the endpoint URL must match the bucket's region.

### Minio Go Client (Forgejo, Gitea, etc.)

Applications using the [Minio Go client](https://github.com/minio/minio-go) (including Forgejo and Gitea) default to HTTPS, which bypasses the cache. Set `MINIO_USE_SSL = false` to use HTTP:

```ini
MINIO_USE_SSL = false
```

For Go applications using minio-go directly, set `Secure: false` in `minio.Options`.

### boto3 Per-Client Configuration

boto3 clients accept `endpoint_url` at creation time, so a single application can route some S3 traffic through the proxy while keeping other traffic direct:

```python
import boto3

# Cached: reads go through proxy
s3_cached = boto3.client('s3',
    endpoint_url='http://s3.us-east-1.amazonaws.com',
    region_name='us-east-1'
)

# Direct: writes bypass proxy entirely
s3_direct = boto3.client('s3', region_name='us-east-1')
```

This avoids the all-or-nothing behavior of environment variables like `AWS_ENDPOINT_URL_S3`.

### Mountpoint for Amazon S3

[Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3) is a file client that mounts S3 buckets as local filesystems. To enable caching through the proxy, use the `--endpoint-url` flag with HTTP:

```bash
mount-s3 --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1 \
  your-bucket /mnt/s3
```

**Important**: Mountpoint uses HTTPS by default, which bypasses the cache (TCP passthrough on port 443). The `--endpoint-url` flag with HTTP is required for caching when using DNS routing. Alternatively, configure Mountpoint to use `HTTP_PROXY=https://proxy:3129` with the TLS proxy listener for encrypted caching.

**DNS routing**: If using Route53 private hosted zones or hosts file DNS routing, the endpoint URL should match your configured DNS zone. The proxy will cache HEAD responses (metadata) and range requests from Mountpoint, significantly improving performance for repeated reads.

**Mountpoint caching**: When using Mountpoint with the proxy, configure Mountpoint's [metadata TTL](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#metadata-cache) but disable Mountpoint's [data cache](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#data-cache). The proxy provides data caching, so Mountpoint's data cache would be redundant. Use `--metadata-ttl` to control how long Mountpoint caches file metadata.

**Performance**: Testing showed 2x faster cached downloads, with the proxy handling Mountpoint's HEAD and range requests efficiently.

### Shell Profile Configuration

For persistent configuration, add to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.):

```bash
# Route S3 traffic through proxy (HTTP for caching)
# Set to your primary region - cross-region requests need explicit --endpoint-url
export AWS_ENDPOINT_URL_S3=http://s3.us-east-1.amazonaws.com
export S3_ENDPOINT_URL=http://s3.us-east-1.amazonaws.com
```

### Check Cache Performance

```bash
# View real-time dashboard (open in browser)
open http://<proxy-ip>:8081

# View cache metrics (JSON format)
curl http://<proxy-ip>:9090/metrics | grep cache_hit_rate

# Check proxy health
curl http://<proxy-ip>:8080/health
```

## Port Configuration Options

### Recommended: Standard Ports (80/443)

```yaml
# config/config.example.yaml
server:
  http_port: 80
  https_port: 443
```

- **Pros**: Works with all S3 clients, HTTPS passthrough on standard port
- **Cons**: Requires `sudo` for privileged ports

### Custom Ports

Custom port numbers are possible but not recommended. HTTP clients can use `--endpoint-url` with a custom port, but HTTPS clients with non-configurable endpoints will fail. For development or environments where `sudo` is not available, use [Proxy-Only Mode](#proxy-only-mode) instead.

## Proxy-Only Mode

Proxy-only mode starts only the HTTP forward proxy listener (default port 3128) without binding to ports 80 or 443. No `sudo`, DNS changes, or `/etc/hosts` modifications needed — clients set `HTTP_PROXY` and traffic flows through the proxy.

Use proxy-only mode for:
- **Localhost deployments** — run the proxy on the same machine as your clients
- **Development and testing** — quick setup with no system-level changes
- **Environments where DNS changes are impractical** — no infrastructure dependencies

### Configuration

Set `server.mode` to `"proxy_only"` in your config file:

```yaml
server:
  mode: "proxy_only"
  # proxy_port: 3128  (default)

cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 524288000  # 500MB
```

Start the proxy (no `sudo` required):

```bash
cargo run --release -- -c config.yaml
```

The proxy listens on port 3128 for unencrypted HTTP forward proxy traffic.

To add encrypted client-to-proxy connections, enable the TLS proxy listener:

```yaml
server:
  mode: "proxy_only"
  proxy_port: 3128
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/proxy/tls/cert.pem"
    key_path: "/etc/proxy/tls/key.pem"
```

This starts both listeners — port 3128 (unencrypted) and port 3129 (TLS-terminated). The TLS listener terminates TLS using the proxy's own certificate, then processes decrypted HTTP through the caching pipeline. See [Option A: HTTP_PROXY](#option-a-http_proxy-single-instance--no-dns-changes) for certificate generation and client trust configuration.

### Client Configuration

Set `HTTP_PROXY` to route S3 traffic through the proxy. Use `--endpoint-url` so the SDK signs requests against the real S3 hostname (not the proxy hostname). SigV4 signatures are computed over HTTP-level content (Host header, path, query string), not the transport layer, so they remain valid regardless of whether the client connects via HTTP or TLS.

**Unencrypted (private networks or localhost):**

```bash
export HTTP_PROXY=http://127.0.0.1:3128
export NO_PROXY=169.254.169.254  # Exclude IMDS from proxying
aws s3 cp s3://bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

**Encrypted via TLS listener (recommended):**

```bash
export HTTP_PROXY=https://127.0.0.1:3129
export NO_PROXY=169.254.169.254  # Exclude IMDS from proxying
export AWS_CA_BUNDLE=/path/to/proxy-cert.pem
aws s3 cp s3://bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

**Why `HTTP_PROXY`, not `HTTPS_PROXY`?** See [Option A: HTTP_PROXY](#option-a-http_proxy-single-instance--no-dns-changes) for why `HTTP_PROXY` is required instead of `HTTPS_PROXY`.

`NO_PROXY=169.254.169.254` prevents EC2 instance metadata (IMDS) requests from being sent through the proxy. Without this, credential retrieval from the instance metadata service will fail.

See [Option A: HTTP_PROXY](#option-a-http_proxy-single-instance--no-dns-changes) for TLS certificate generation, self-signed certificate SANs, and client trust configuration details.

### High Availability: Shared NFS Cache Pattern

Instead of centralized proxy instances behind DNS multi-value routing, each compute node runs its own localhost proxy with a shared cache volume (NFS/EFS). HA comes from the storage layer, not the proxy layer.

```
Node A:  proxy (127.0.0.1:3128) ──┐
Node B:  proxy (127.0.0.1:3128) ──┼──► /mnt/shared-cache (NFS/EFS) ──► S3
Node C:  proxy (127.0.0.1:3128) ──┘
```

Each node's config points to the shared mount:

```yaml
server:
  mode: "proxy_only"

cache:
  cache_dir: "/mnt/shared-cache"
  max_cache_size: 107374182400  # 100GB
```

How it works:
- Each client talks to `127.0.0.1` — no load balancer or DNS infrastructure needed
- Objects cached by any node are available to all nodes via the shared volume
- If a node's proxy crashes, its process supervisor (systemd, etc.) restarts it — other nodes are unaffected
- NFS mount requirement: `lookupcache=pos` for reliable cache coordination

**Comparison with DNS multi-value routing:**

| | DNS Multi-Value Routing | Shared NFS Cache |
|---|---|---|
| **Architecture** | Centralized proxies behind Route 53 | Per-node localhost proxies with shared storage |
| **HA mechanism** | DNS failover across proxy IPs | Process supervisor restarts local proxy |
| **Load balancing** | DNS distributes clients across proxies | Each client uses its own local proxy |
| **Infrastructure** | Route 53 private hosted zones, multi-value A records | NFS/EFS mount, process supervisor |
| **Network hops** | Client → network → proxy → S3 | Client → localhost → S3 |
| **Best for** | Shared proxy fleet serving many clients | Compute clusters where each node runs its own workload |

DNS multi-value routing (see [Option B: DNS Zone](#option-b-dns-zone-production-deployments)) is better when a small number of proxy instances serve many clients. The shared NFS cache pattern is better when each compute node generates its own S3 traffic and you want to avoid DNS infrastructure entirely.

## Configuration Files

- `config/config.example.yaml` - Complete configuration with all options documented

## Verification

### Test Basic Functionality

```bash
# Test proxy is running
curl -I http://<proxy-ip>:8080/health

# Test S3 access through proxy
aws s3 ls s3://your-bucket \
  --endpoint-url "http://s3.us-east-1.amazonaws.com"
```

### Monitor Logs

```bash
# Watch application logs (replace <app-log-dir> with your configured app_log_dir)
tail -f <app-log-dir>/$(hostname)/s3-proxy.log*

# Watch access logs (replace <access-log-dir> with your configured access_log_dir)
tail -f <access-log-dir>/$(date +%Y/%m/%d)/*
```

## Troubleshooting

### Common Issues

1. **Permission denied on ports 80/443**:
   ```bash
   # Use sudo for privileged ports
   sudo cargo run --release -- -c config/config.example.yaml
   ```

2. **AWS credentials not configured on the client**:
   The proxy does not use AWS credentials — it forwards pre-signed requests from clients. Ensure credentials are configured on the *client* machine:
   ```bash
   aws configure
   # or set environment variables
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

3. **Connection refused**:
   - Check proxy is running: `curl http://<proxy-ip>:8080/health`
   - Verify hosts file configuration
   - Check firewall settings

4. **Cache not working**:
   - Ensure using HTTP endpoint or TLS proxy (HTTPS port 443 is passthrough-only, no caching)
   - Check cache directory permissions: `ls -la <cache-dir>/`
   - Monitor cache metrics: `curl http://<proxy-ip>:9090/metrics`

### Debug Mode

Enable debug logging by setting environment variable:

```bash
RUST_LOG=debug sudo cargo run --release -- -c config/config.example.yaml
```

## Next Steps

- **Dashboard**: See [Dashboard Guide](DASHBOARD.md) for real-time monitoring
- **Configuration**: See [Configuration Guide](CONFIGURATION.md) for detailed settings
- **Testing**: See [Testing Guide](TESTING.md) for validation procedures
- **Features**: Explore feature documentation in `docs/`
- **Development**: See [Developer Guide](DEVELOPER.md) for implementation details

## Performance Tips

1. **Use HTTP or TLS proxy endpoints** for caching (HTTPS port 443 bypasses cache via TCP passthrough)
2. **Monitor cache hit rates** via metrics endpoint
3. **Adjust cache sizes** based on your workload
4. **Use range requests** for large files when possible
5. **Configure TTL values** appropriate for your data freshness requirements


## System Requirements and Sizing

The proxy is network-bound, not CPU or disk bound. Sizing should prioritize network bandwidth.

### Observed Resource Usage

Tested on EC2 (m6in.2xlarge) with 100 concurrent clients downloading 1.4 GB each (140 GB total) through 3 proxy instances sharing an NFS cache volume:

| Resource | Peak Usage | Notes |
|----------|-----------|-------|
| CPU | 10% of 8 vCPU | ~1 core used |
| Network out (to clients) | 4.5 Gbps per instance | 10.7 Gbps aggregate |
| Network in (from S3) | 0.18 Gbps per instance | 95% served from cache |
| RAM cache | 1 GB configured | Holds hot data for repeated reads |

### Sizing Guidance

| Deployment | Clients | Server Spec | Notes |
|------------|---------|-------------|-------|
| Development | 1-5 | 2 vCPU, 1 Gbps network | Any modern server |
| Small | 5-50 | 2-4 vCPU, 10+ Gbps network | Network is the constraint |
| Medium | 50-200 | 4 vCPU, 25 Gbps network | More RAM for larger cache |
| Large | 200+ | Multiple instances with shared NFS | Scale out, not up |

**Key considerations**:
- Network bandwidth is the primary constraint — choose servers or instances with high network throughput
- CPU usage is minimal (~1 core at 100 clients) — 2-4 vCPU is sufficient for most workloads
- RAM cache (1 GB default) accelerates repeated reads — increase for hot working sets
- Disk cache size depends on working set — NFS provides shared storage across instances
- Download coordination reduces S3 fetches by 94% during concurrent access, so S3 bandwidth is rarely a bottleneck
- Scale out by adding proxy instances behind DNS multivalue routing rather than scaling up
