#!/bin/bash
# Manual cache test script for debugging cache behavior
# This script provides a simpler alternative to the Rust integration test
# for manual testing and debugging

set -e

# Configuration
TEST_BUCKET="egummett-testing-source-1"
TEST_OBJECT="bigfiles/100MB"
S3_ENDPOINT="http://s3.eu-west-1.amazonaws.com"
PROXY_PORT=80
HEALTH_PORT=8080
CACHE_DIR="./tmp/manual_test_cache"
DOWNLOAD_DIR="./tmp/manual_test_downloads"
CONFIG_FILE="./tmp/manual_test_config.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "\n${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  $1${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check if running as root
    if [ "$EUID" -ne 0 ]; then
        print_error "This script must be run with sudo"
        echo "Usage: sudo -E ./tests/manual_cache_test.sh"
        exit 1
    fi
    print_success "Running with sudo"
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI not found. Please install it first."
        exit 1
    fi
    print_success "AWS CLI found: $(aws --version)"
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured"
        echo "Run: aws configure"
        exit 1
    fi
    print_success "AWS credentials configured"
    
    # Check S3 access
    if ! aws s3 ls "s3://${TEST_BUCKET}/${TEST_OBJECT}" --endpoint-url "${S3_ENDPOINT}" &> /dev/null; then
        print_error "Cannot access s3://${TEST_BUCKET}/${TEST_OBJECT}"
        exit 1
    fi
    print_success "S3 access verified"
    
    # Check if port 80 is available
    if lsof -Pi :${PROXY_PORT} -sTCP:LISTEN -t >/dev/null 2>&1; then
        print_error "Port ${PROXY_PORT} is already in use"
        echo "Kill the process using: sudo kill -9 \$(lsof -t -i:${PROXY_PORT})"
        exit 1
    fi
    print_success "Port ${PROXY_PORT} is available"
}

# Setup test environment
setup_environment() {
    print_header "Setting Up Test Environment"
    
    # Create directories
    mkdir -p "${CACHE_DIR}"
    mkdir -p "${DOWNLOAD_DIR}"
    mkdir -p "${CACHE_DIR}/logs/access"
    mkdir -p "${CACHE_DIR}/logs/app"
    
    print_success "Created test directories"
    
    # Create config file
    cat > "${CONFIG_FILE}" << EOF
server:
  http_port: ${PROXY_PORT}
  https_port: 443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "300s"

cache:
  cache_dir: "${CACHE_DIR}"
  max_cache_size: 10737418240
  ram_cache_enabled: false
  eviction_algorithm: "tinylfu"
  write_cache_enabled: false
  get_ttl: "315360000s"
  head_ttl: "60s"
  actively_remove_cached_data: false
  range_merge_gap_threshold: 262144
  eviction_buffer_percent: 5
  metadata_lock_timeout_seconds: 60

logging:
  access_log_dir: "${CACHE_DIR}/logs/access"
  app_log_dir: "${CACHE_DIR}/logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "debug"

health:
  enabled: true
  endpoint: "/health"
  port: ${HEALTH_PORT}
  check_interval: "30s"

metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  collection_interval: "60s"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true

connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "60s"

compression:
  enabled: true
  threshold: 4096
  preferred_algorithm: "lz4"
  content_aware: true
EOF
    
    print_success "Created config file: ${CONFIG_FILE}"
    print_info "Cache directory: ${CACHE_DIR}"
    print_info "Download directory: ${DOWNLOAD_DIR}"
}

# Clear cache
clear_cache() {
    print_header "Clearing Cache"
    
    rm -rf "${CACHE_DIR}/objects"
    rm -rf "${CACHE_DIR}/ranges"
    rm -rf "${CACHE_DIR}/head_cache"
    
    mkdir -p "${CACHE_DIR}/objects"
    mkdir -p "${CACHE_DIR}/ranges"
    mkdir -p "${CACHE_DIR}/head_cache"
    
    print_success "Cache cleared"
}

# Build proxy
build_proxy() {
    print_header "Building Proxy"
    
    cargo build --release
    
    if [ ! -f "target/release/s3-proxy" ]; then
        print_error "Proxy binary not found after build"
        exit 1
    fi
    
    print_success "Proxy built successfully"
}

# Start proxy
start_proxy() {
    print_header "Starting Proxy"
    
    # Start proxy in background
    ./target/release/s3-proxy -c "${CONFIG_FILE}" > "${CACHE_DIR}/proxy.log" 2>&1 &
    PROXY_PID=$!
    
    print_info "Proxy started with PID: ${PROXY_PID}"
    echo "${PROXY_PID}" > "${CACHE_DIR}/proxy.pid"
    
    # Wait for proxy to be ready
    print_info "Waiting for proxy to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${HEALTH_PORT}/health" > /dev/null 2>&1; then
            print_success "Proxy is ready"
            return 0
        fi
        sleep 1
    done
    
    print_error "Proxy failed to start within 30 seconds"
    print_info "Check logs: ${CACHE_DIR}/proxy.log"
    exit 1
}

# Stop proxy
stop_proxy() {
    print_header "Stopping Proxy"
    
    if [ -f "${CACHE_DIR}/proxy.pid" ]; then
        PROXY_PID=$(cat "${CACHE_DIR}/proxy.pid")
        if kill -0 "${PROXY_PID}" 2>/dev/null; then
            kill "${PROXY_PID}"
            sleep 2
            if kill -0 "${PROXY_PID}" 2>/dev/null; then
                kill -9 "${PROXY_PID}"
            fi
            print_success "Proxy stopped"
        else
            print_info "Proxy already stopped"
        fi
        rm -f "${CACHE_DIR}/proxy.pid"
    else
        print_info "No proxy PID file found"
    fi
}

# Download file
download_file() {
    local attempt=$1
    local output_file="${DOWNLOAD_DIR}/100MB_attempt_${attempt}"
    
    print_header "Download Attempt ${attempt}"
    
    print_info "Source: s3://${TEST_BUCKET}/${TEST_OBJECT}"
    print_info "Destination: ${output_file}"
    print_info "Endpoint: ${S3_ENDPOINT}"
    
    # Download with timing
    local start_time=$(date +%s)
    
    if aws s3 cp "s3://${TEST_BUCKET}/${TEST_OBJECT}" "${output_file}" --endpoint-url "${S3_ENDPOINT}"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        local file_size=$(stat -f%z "${output_file}" 2>/dev/null || stat -c%s "${output_file}" 2>/dev/null)
        local file_size_mb=$(echo "scale=2; ${file_size} / 1048576" | bc)
        
        print_success "Download completed in ${duration} seconds"
        print_success "File size: ${file_size} bytes (${file_size_mb} MB)"
        return 0
    else
        print_error "Download failed"
        return 1
    fi
}

# Analyze logs
analyze_logs() {
    local attempt=$1
    
    print_header "Analyzing Logs (Attempt ${attempt})"
    
    local log_dir="${CACHE_DIR}/logs/app"
    
    if [ ! -d "${log_dir}" ]; then
        print_error "Log directory not found: ${log_dir}"
        return
    fi
    
    # Count cache-related events
    local cache_hits=$(grep -r "cache_hit" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    local cache_misses=$(grep -r "cache_miss" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    local s3_requests=$(grep -r "S3 request\|Fetching from S3" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    local exact_matches=$(grep -r "\[RANGE_OVERLAP\].*result=exact_match" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    local partial_overlaps=$(grep -r "\[RANGE_OVERLAP\].*result=partial_overlap" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    local range_merges=$(grep -r "\[RANGE_MERGE\]" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    local merge_errors=$(grep -r "\[RANGE_MERGE\].*error\|mismatch" "${log_dir}" 2>/dev/null | wc -l | tr -d ' ')
    
    echo "Cache Hits: ${cache_hits}"
    echo "Cache Misses: ${cache_misses}"
    echo "S3 Requests: ${s3_requests}"
    echo "Exact Matches: ${exact_matches}"
    echo "Partial Overlaps: ${partial_overlaps}"
    echo "Range Merges: ${range_merges}"
    echo "Merge Errors: ${merge_errors}"
    
    # Calculate cache hit rate
    local total=$((cache_hits + cache_misses))
    if [ ${total} -gt 0 ]; then
        local hit_rate=$(echo "scale=2; ${cache_hits} * 100 / ${total}" | bc)
        echo "Cache Hit Rate: ${hit_rate}%"
    fi
}

# Verify cache files
verify_cache_files() {
    print_header "Verifying Cache Files"
    
    local objects_count=$(find "${CACHE_DIR}/objects" -type f -name "*.meta" 2>/dev/null | wc -l | tr -d ' ')
    local ranges_count=$(find "${CACHE_DIR}/ranges" -type f -name "*.bin" 2>/dev/null | wc -l | tr -d ' ')
    
    echo "Metadata files: ${objects_count}"
    echo "Range files: ${ranges_count}"
    
    if [ ${objects_count} -gt 0 ] && [ ${ranges_count} -gt 0 ]; then
        print_success "Cache files created"
    else
        print_error "No cache files found"
    fi
}

# Main test flow
main() {
    print_header "Real-World S3 Proxy Cache Test"
    
    # Setup
    check_prerequisites
    setup_environment
    clear_cache
    build_proxy
    
    # Start proxy
    start_proxy
    sleep 2
    
    # First download (cache miss)
    print_header "FIRST DOWNLOAD - Expected: Cache Miss"
    if ! download_file 1; then
        stop_proxy
        exit 1
    fi
    
    sleep 2
    analyze_logs 1
    verify_cache_files
    
    # Second download (cache hit)
    print_header "SECOND DOWNLOAD - Expected: 100% Cache Hit"
    if ! download_file 2; then
        stop_proxy
        exit 1
    fi
    
    sleep 2
    analyze_logs 2
    
    # Compare files
    print_header "Comparing Downloaded Files"
    local file1="${DOWNLOAD_DIR}/100MB_attempt_1"
    local file2="${DOWNLOAD_DIR}/100MB_attempt_2"
    
    if cmp -s "${file1}" "${file2}"; then
        print_success "Files are identical"
    else
        print_error "Files differ!"
    fi
    
    # Stop proxy
    stop_proxy
    
    # Summary
    print_header "Test Complete"
    print_info "Cache directory: ${CACHE_DIR}"
    print_info "Download directory: ${DOWNLOAD_DIR}"
    print_info "Proxy logs: ${CACHE_DIR}/proxy.log"
    print_info "App logs: ${CACHE_DIR}/logs/app/"
    
    print_success "Test completed successfully"
}

# Cleanup on exit
cleanup() {
    if [ -f "${CACHE_DIR}/proxy.pid" ]; then
        stop_proxy
    fi
}

trap cleanup EXIT

# Run main
main
