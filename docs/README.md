# S3 Proxy Documentation

Welcome to the S3 Proxy documentation. This guide provides comprehensive information about the S3 Proxy's architecture, configuration, and usage.

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
- [Write-Through Caching](CACHING.md#write-cache-configuration) - PUT operation caching
- [Range Request Optimization](CACHING.md#range-request-optimization) - Intelligent range handling
- [Compression Optimization](COMPRESSION.md#ram-cache-compression-optimization) - Efficient memory usage
- [Dashboard](DASHBOARD.md) - Web-based monitoring interface
- [OTLP Metrics](OTLP_METRICS.md) - OpenTelemetry metrics export

### Operations & Maintenance
- [Error Handling](ERROR_HANDLING.md) - Robust error recovery mechanisms
- [Testing Guide](TESTING.md) - Test suite and validation procedures
- [Developer Guide](DEVELOPER.md) - Implementation details and development notes

### Reference
- [Troubleshooting Guide](ERROR_HANDLING.md#troubleshooting) - Common issues and solutions
- [Performance Tuning](CONFIGURATION.md#cache-hit-performance-tuning) - Optimization guidelines

---

## Quick Links

- **[Configuration Reference](CONFIGURATION.md)** - All configuration options with examples
- **[Caching Documentation](CACHING.md)** - Detailed caching behavior and TTL management
- **[Error Handling](ERROR_HANDLING.md)** - Recovery mechanisms for cache corruption and disk issues
- **[Dashboard](DASHBOARD.md)** - Web interface for monitoring cache statistics