# Hybrid Cache for Amazon S3 Documentation

Deep-dive reference documentation for Hybrid Cache for Amazon S3. For an overview and quick start, see the [project README](../README.md).

## Getting Started

- [Quick Start Guide](GETTING_STARTED.md) - Installation and first run
- [Docker Deployment](DOCKER.md) - Building and running in a container, as an alternative to the systemd path
- [Deploying on AWS for Distant Origins](AWS_DEPLOYMENT.md) - Reference architecture and FSx for OpenZFS sizing for cross-region and non-AWS origins
- [Upgrading](UPGRADING.md) - Per-release manual steps and default changes
- [Configuration Reference](CONFIGURATION.md) - Complete configuration options

## Core Concepts

- [Architecture Overview](ARCHITECTURE.md) - Technical architecture and design principles
- [Security Considerations](ARCHITECTURE.md#security-considerations) - Shared cache access model, what a cleartext hop exposes, trust and integrity model
- [Caching System](CACHING.md) - Multi-tier caching with RAM and disk
- [Compression](COMPRESSION.md) - LZ4 compression and content detection
- [Connection Pooling](CONNECTION_POOLING.md) - Connection management and load balancing

## Features

- [Write-Through Caching](CACHING.md#write-through-cache) - PUT operation caching
- [Multipart Upload Caching](MULTIPART_UPLOAD.md) - Multipart upload cache internals and correctness model
- [Range Request Optimization](CACHING.md#intelligent-range-merging) - Intelligent range handling
- [Compression Optimization](COMPRESSION.md#ram-cache-compression-optimization) - Efficient memory usage
- [Download Bandwidth QoS](BANDWIDTH_QOS.md) - Origin download rate ceiling and fair sharing

## Monitoring

- [Dashboard](DASHBOARD.md) - Web-based monitoring interface
- [Metrics](METRICS.md) - Per-bucket traffic metrics and cache savings inference
- [OTLP Metrics](OTLP_METRICS.md) - OpenTelemetry metrics export

## Operations

- [Error Handling](ERROR_HANDLING.md) - Recovery from cache corruption and disk issues
- [Performance Tuning](CONFIGURATION.md#cache-hit-performance-tuning) - Optimization guidelines
- [Testing Guide](TESTING.md) - Test suite and validation procedures
- [Developer Guide](DEVELOPER.md) - Build, test, and coverage workflow

## Reference

- [Cache rules schema](cache-rules-schema.json) - JSON Schema for `cache_rules.json`
- [Cache rules examples](examples/) - Worked `cache_rules.json` files for common patterns
- [`config/config.example.yaml`](../config/config.example.yaml) - Annotated example configuration
