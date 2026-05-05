# S3 Hybrid Cache Documentation

Deep-dive reference documentation for S3 Hybrid Cache. For an overview and quick start, see the [project README](../README.md).

## Table of Contents

### Getting Started
- [Quick Start Guide](GETTING_STARTED.md) - Installation and basic setup
- [Configuration Reference](CONFIGURATION.md) - Complete configuration options

### Core Concepts
- [Architecture Overview](ARCHITECTURE.md) - Technical architecture and design principles
- [Caching System](CACHING.md) - Multi-tier caching with RAM and disk
- [Compression](COMPRESSION.md) - LZ4 compression and content detection
- [Connection Pooling](CONNECTION_POOLING.md) - Connection management and load balancing

### Advanced Features
- [Write-Through Caching](CACHING.md#write-through-cache) - PUT operation caching
- [Multipart Upload Caching](MULTIPART_UPLOAD.md) - Multipart upload cache internals and correctness model
- [Range Request Optimization](CACHING.md#intelligent-range-merging) - Intelligent range handling
- [Compression Optimization](COMPRESSION.md#ram-cache-compression-optimization) - Efficient memory usage
- [Dashboard](DASHBOARD.md) - Web-based monitoring interface
- [OTLP Metrics](OTLP_METRICS.md) - OpenTelemetry metrics export

### Operations & Maintenance
- [Error Handling](ERROR_HANDLING.md) - Robust error recovery mechanisms
- [Testing Guide](TESTING.md) - Test suite and validation procedures
- [Developer Guide](DEVELOPER.md) - Implementation details and development notes

### Reference
- [Error Handling](ERROR_HANDLING.md) - Error recovery and maintenance operations
- [Performance Tuning](CONFIGURATION.md#cache-hit-performance-tuning) - Optimization guidelines

---

## Quick Links

- **[Configuration Reference](CONFIGURATION.md)** - All configuration options with examples
- **[Caching Documentation](CACHING.md)** - Detailed caching behavior and TTL management
- **[Error Handling](ERROR_HANDLING.md)** - Recovery mechanisms for cache corruption and disk issues
- **[Dashboard](DASHBOARD.md)** - Web interface for monitoring cache statistics