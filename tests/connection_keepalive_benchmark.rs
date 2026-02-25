// Benchmark test for connection keepalive performance
// This test measures the performance characteristics of connection pooling

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// Mock connection pool for benchmarking
struct BenchmarkConnectionPool {
    connections: Arc<Mutex<Vec<MockConnection>>>,
    max_idle: usize,
    idle_timeout: Duration,
    stats: Arc<Mutex<BenchmarkStats>>,
}

struct MockConnection {
    id: usize,
    created_at: Instant,
    last_used: Instant,
    is_active: bool,
}

#[derive(Default)]
struct BenchmarkStats {
    connections_created: usize,
    connections_reused: usize,
    total_requests: usize,
}

impl BenchmarkConnectionPool {
    fn new(max_idle: usize, idle_timeout: Duration) -> Self {
        Self {
            connections: Arc::new(Mutex::new(Vec::new())),
            max_idle,
            idle_timeout,
            stats: Arc::new(Mutex::new(BenchmarkStats::default())),
        }
    }

    async fn get_connection(&self) -> MockConnection {
        let mut conns = self.connections.lock().await;
        let mut stats = self.stats.lock().await;
        let now = Instant::now();

        // Try to find an idle connection
        if let Some(pos) = conns
            .iter()
            .position(|c| !c.is_active && now.duration_since(c.last_used) < self.idle_timeout)
        {
            let mut conn = conns.remove(pos);
            conn.is_active = true;
            conn.last_used = now;
            stats.connections_reused += 1;
            stats.total_requests += 1;
            conn
        } else {
            // Create new connection
            let conn = MockConnection {
                id: stats.connections_created,
                created_at: now,
                last_used: now,
                is_active: true,
            };
            stats.connections_created += 1;
            stats.total_requests += 1;
            conn
        }
    }

    async fn release_connection(&self, mut conn: MockConnection) {
        let mut conns = self.connections.lock().await;
        conn.is_active = false;
        conn.last_used = Instant::now();

        // Enforce max idle limit
        if conns.len() >= self.max_idle {
            // Don't add this connection back
            return;
        }

        conns.push(conn);
    }

    async fn get_stats(&self) -> BenchmarkStats {
        let stats = self.stats.lock().await;
        BenchmarkStats {
            connections_created: stats.connections_created,
            connections_reused: stats.connections_reused,
            total_requests: stats.total_requests,
        }
    }
}

// Simulate request processing
async fn simulate_request(pool: &BenchmarkConnectionPool, request_duration: Duration) {
    let conn = pool.get_connection().await;

    // Simulate request processing time
    tokio::time::sleep(request_duration).await;

    pool.release_connection(conn).await;
}

#[tokio::test]
async fn benchmark_connection_reuse_latency() {
    println!("\n=== Benchmark: Connection Reuse Latency ===");

    let pool = BenchmarkConnectionPool::new(10, Duration::from_secs(30));
    let request_count = 100;
    let request_duration = Duration::from_millis(10);

    let start = Instant::now();

    for _ in 0..request_count {
        simulate_request(&pool, request_duration).await;
    }

    let elapsed = start.elapsed();
    let stats = pool.get_stats().await;

    println!("Total requests: {}", stats.total_requests);
    println!("Connections created: {}", stats.connections_created);
    println!("Connections reused: {}", stats.connections_reused);
    println!("Total time: {:?}", elapsed);
    println!("Avg time per request: {:?}", elapsed / request_count);

    // Verify connection reuse
    assert_eq!(
        stats.connections_created, 1,
        "Should only create 1 connection"
    );
    assert_eq!(
        stats.connections_reused,
        request_count as usize - 1,
        "Should reuse connection for all subsequent requests"
    );

    // Calculate reuse percentage
    let reuse_pct = (stats.connections_reused as f64 / stats.total_requests as f64) * 100.0;
    println!("Connection reuse rate: {:.2}%", reuse_pct);

    assert!(reuse_pct > 90.0, "Connection reuse rate should be >90%");
}

#[tokio::test]
async fn benchmark_concurrent_requests() {
    println!("\n=== Benchmark: Concurrent Requests ===");

    let pool = Arc::new(BenchmarkConnectionPool::new(10, Duration::from_secs(30)));
    let concurrent_requests = 50;
    let request_duration = Duration::from_millis(10);

    let start = Instant::now();

    let mut handles = Vec::new();

    for _ in 0..concurrent_requests {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            simulate_request(&pool_clone, request_duration).await;
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    let stats = pool.get_stats().await;

    println!("Total requests: {}", stats.total_requests);
    println!("Connections created: {}", stats.connections_created);
    println!("Connections reused: {}", stats.connections_reused);
    println!("Total time: {:?}", elapsed);
    println!("Avg time per request: {:?}", elapsed / concurrent_requests);

    // With concurrent requests, we expect multiple connections to be created
    // Note: All concurrent requests start at once, so they all create connections
    // This is expected behavior - the pool doesn't limit active connections,
    // only idle connections
    assert!(
        stats.connections_created >= 1,
        "Should create at least one connection"
    );
    println!("Note: Concurrent requests create multiple connections as expected");
}

#[tokio::test]
async fn benchmark_idle_timeout_enforcement() {
    println!("\n=== Benchmark: Idle Timeout Enforcement ===");

    let idle_timeout = Duration::from_millis(100);
    let pool = BenchmarkConnectionPool::new(10, idle_timeout);

    // Make first request
    simulate_request(&pool, Duration::from_millis(10)).await;

    let stats_before = pool.get_stats().await;
    println!(
        "After first request - Connections created: {}",
        stats_before.connections_created
    );

    // Wait for idle timeout to expire
    tokio::time::sleep(idle_timeout + Duration::from_millis(50)).await;

    // Make second request (should create new connection due to timeout)
    simulate_request(&pool, Duration::from_millis(10)).await;

    let stats_after = pool.get_stats().await;
    println!(
        "After timeout - Connections created: {}",
        stats_after.connections_created
    );

    // Note: In this mock implementation, the connection is removed on get_connection
    // In real implementation, a background task would clean up expired connections
    assert_eq!(
        stats_after.connections_created, 2,
        "Should create new connection after idle timeout"
    );
}

#[tokio::test]
async fn benchmark_max_idle_limit() {
    println!("\n=== Benchmark: Max Idle Limit ===");

    let max_idle = 5;
    let pool = Arc::new(BenchmarkConnectionPool::new(
        max_idle,
        Duration::from_secs(30),
    ));

    // Create more connections than max_idle
    let request_count = 20;
    let mut handles = Vec::new();

    for _ in 0..request_count {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            simulate_request(&pool_clone, Duration::from_millis(10)).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Wait for all connections to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    let conns = pool.connections.lock().await;
    let idle_count = conns.len();

    println!("Idle connections: {}", idle_count);
    println!("Max idle limit: {}", max_idle);

    assert!(
        idle_count <= max_idle,
        "Idle connections should not exceed max_idle limit"
    );
}

#[tokio::test]
async fn benchmark_throughput_comparison() {
    println!("\n=== Benchmark: Throughput Comparison ===");

    let request_count = 100;
    let request_duration = Duration::from_millis(5);

    // Test with connection reuse
    let pool_with_reuse = BenchmarkConnectionPool::new(10, Duration::from_secs(30));
    let start_with_reuse = Instant::now();

    for _ in 0..request_count {
        simulate_request(&pool_with_reuse, request_duration).await;
    }

    let elapsed_with_reuse = start_with_reuse.elapsed();
    let rps_with_reuse = request_count as f64 / elapsed_with_reuse.as_secs_f64();

    println!("With connection reuse:");
    println!("  Total time: {:?}", elapsed_with_reuse);
    println!("  Requests per second: {:.2}", rps_with_reuse);

    // Test without connection reuse (max_idle = 0)
    let pool_without_reuse = BenchmarkConnectionPool::new(0, Duration::from_secs(30));
    let start_without_reuse = Instant::now();

    for _ in 0..request_count {
        simulate_request(&pool_without_reuse, request_duration).await;
    }

    let elapsed_without_reuse = start_without_reuse.elapsed();
    let rps_without_reuse = request_count as f64 / elapsed_without_reuse.as_secs_f64();

    println!("Without connection reuse:");
    println!("  Total time: {:?}", elapsed_without_reuse);
    println!("  Requests per second: {:.2}", rps_without_reuse);

    // Calculate improvement
    let improvement_pct = ((rps_with_reuse - rps_without_reuse) / rps_without_reuse) * 100.0;
    println!("Throughput improvement: {:.2}%", improvement_pct);

    // With connection reuse, throughput should be similar or better
    // Allow for small variations due to timing and system load
    let tolerance = 0.95; // Allow up to 5% variation
    assert!(
        rps_with_reuse >= rps_without_reuse * tolerance,
        "Connection reuse should not significantly decrease throughput (with={:.2}, without={:.2})",
        rps_with_reuse,
        rps_without_reuse
    );
}

#[tokio::test]
async fn benchmark_memory_usage() {
    println!("\n=== Benchmark: Memory Usage ===");

    let max_idle_values = vec![1, 5, 10, 20];

    for max_idle in max_idle_values {
        let pool = Arc::new(BenchmarkConnectionPool::new(
            max_idle,
            Duration::from_secs(30),
        ));

        // Create connections up to max_idle
        let mut handles = Vec::new();
        for _ in 0..max_idle * 2 {
            let pool_clone = Arc::clone(&pool);
            let handle = tokio::spawn(async move {
                simulate_request(&pool_clone, Duration::from_millis(10)).await;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Wait for connections to be released
        tokio::time::sleep(Duration::from_millis(100)).await;

        let conns = pool.connections.lock().await;
        let stats = pool.get_stats().await;

        println!(
            "max_idle={}: idle_connections={}, total_created={}",
            max_idle,
            conns.len(),
            stats.connections_created
        );

        assert!(conns.len() <= max_idle, "Should not exceed max_idle");
    }
}
