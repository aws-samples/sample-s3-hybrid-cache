# Getting Started with S3 Proxy

Quick start guide for installing, configuring, and running S3 Proxy.

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
- **HTTP**: `localhost:80` (caching enabled)
- **HTTPS**: `localhost:443` (passthrough)
- **Health**: `localhost:8080/health`
- **Dashboard**: `localhost:8081` (real-time statistics)
- **Metrics**: `localhost:9090/metrics`

**Security Note**: All communication between the proxy and Amazon S3 uses HTTPS encryption, regardless of whether clients connect via HTTP or HTTPS. Secure client-to-proxy HTTP traffic using network controls (VPC, security groups, firewall rules).

### 3. Configure DNS Routing

The proxy intercepts S3 traffic by resolving S3 endpoints to the proxy's IP address. Choose the appropriate method for your environment:

#### Option A: Hosts File (Testing/Development)

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

#### S3 PrivateLink (Interface VPC Endpoints)

If your environment uses S3 interface VPC endpoints (PrivateLink), the proxy needs additional configuration to route traffic to S3 through the private endpoint rather than over the public internet.

Client-to-proxy DNS routing (Option A or B above) remains unchanged. PrivateLink only affects how the proxy resolves and connects to S3 on the backend.

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
    "s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]
```

This bypasses DNS entirely for the specified hostnames. The proxy load-balances across the listed IPs. See [Configuration Guide - endpoint_overrides](CONFIGURATION.md#s3-privatelink-interface-vpc-endpoints) for details.

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

| Access Pattern | Endpoint URL | Works? | Notes |
|---|---|---|---|
| AP alias | `http://s3.{region}.amazonaws.com` | Yes | Recommended for caching |
| AP ARN | `http://s3-accesspoint.{region}.amazonaws.com` | Yes | CLI builds correct Host from ARN |
| AP alias | `http://s3-accesspoint.{region}.amazonaws.com` | No | Base domain doesn't resolve via public DNS |
| MRAP alias | `http://accesspoint.s3-global.amazonaws.com` | Yes | CLI builds correct Host from alias |
| MRAP ARN | `http://accesspoint.s3-global.amazonaws.com` | Yes | CLI builds correct Host from ARN |
| Any AP/MRAP | (no endpoint, default HTTPS) | Bypasses cache | HTTPS uses TCP passthrough |

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

### Mountpoint for Amazon S3

[Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3) is a file client that mounts S3 buckets as local filesystems. To enable caching through the proxy, use the `--endpoint-url` flag with HTTP:

```bash
mount-s3 --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1 \
  your-bucket /mnt/s3
```

**Important**: Mountpoint uses HTTPS by default, which bypasses the cache (TCP passthrough). The `--endpoint-url` flag with HTTP is required for caching.

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
open http://localhost:8081

# View cache metrics (Prometheus format)
curl http://localhost:9090/metrics | grep cache_hit_rate

# Check proxy health
curl http://localhost:8080/health
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

### Development: Non-Privileged Ports

```yaml
server:
  http_port: 8081
  https_port: 8443
```

- **Pros**: No `sudo` required
- **Cons**: HTTPS clients may fail if they can't configure port 8443

### Custom Ports

Possible but not recommended. HTTP clients can use `--endpoint-url`, but HTTPS clients with non-configurable endpoints will fail.

## Configuration Files

- `config/config.example.yaml` - Complete configuration with all options documented

## Verification

### Test Basic Functionality

```bash
# Test proxy is running
curl -I http://localhost:8080/health

# Test S3 access through proxy
aws s3 ls s3://your-bucket \
  --endpoint-url "http://s3.us-east-1.amazonaws.com"
```

### Monitor Logs

```bash
# Watch application logs
tail -f ./tmp/logs/app/$(hostname)/*.log

# Watch access logs
tail -f ./tmp/logs/access/*.log
```

## Troubleshooting

### Common Issues

1. **Permission denied on ports 80/443**:
   ```bash
   # Use sudo for privileged ports
   sudo cargo run --release -- -c config/config.example.yaml
   ```

2. **AWS credentials not configured**:
   ```bash
   aws configure
   # or set environment variables
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

3. **Connection refused**:
   - Check proxy is running: `curl http://localhost:8080/health`
   - Verify hosts file configuration
   - Check firewall settings

4. **Cache not working**:
   - Ensure using HTTP endpoint (not HTTPS)
   - Check cache directory permissions: `ls -la ./tmp/cache/`
   - Monitor cache metrics: `curl http://localhost:9090/metrics`

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

1. **Use HTTP endpoints** for caching (HTTPS bypasses cache)
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
