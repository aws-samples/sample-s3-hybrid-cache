//! The TLS proxy listener enforces the destination policy on its HTTP forwarding path.
//!
//! Spec: `public-security-review-readiness`. Requirements: 1.1, 1.2, 1.3, 1.4, 1.6.
//!
//! # What was wrong
//!
//! `TlsProxyListener::start`'s `service_fn` closure routed `CONNECT` to
//! `handle_connect`, which evaluates `DestinationPolicy::check`, and passed **`None`
//! for both the policy and the resolver** into `HttpProxy::handle_request` for every
//! other method. So with `server.tls.connect_allowlist` configured, port 80 enforced
//! the policy and the TLS listener's port did not — the two listeners diverging
//! precisely in the configuration where the operator asked for the protection. Within
//! the TLS listener, a client using `HTTPS_PROXY` (CONNECT) was gated while the same
//! client using `HTTP_PROXY` against the same port was not.
//!
//! # Why these tests go through the listener rather than calling `handle_request`
//!
//! The defect is in the **wiring**, not in the policy object. `handle_request`'s inline
//! check was always correct; nothing handed it a policy. A test that called
//! `handle_request` directly with `Some(policy)` would therefore pass both before and
//! after the fix — vacuous by construction, and indistinguishable from a real test.
//! `tests/test_tls_proxy_listener.rs` is entirely `DestinationPolicy` unit tests and
//! never constructs a `TlsProxyListener`; `http_proxy.rs`'s `destination_policy_http_tests`
//! call `classify_ips` without going through `handle_request`. Neither can fail if this
//! enforcement disappears, which is why this file stands up the real listener, terminates
//! real TLS, and sends real bytes.
//!
//! # The two vacuity traps, and how each is avoided
//!
//! **Asserting the adjacent predicate.** The predicate is *whether the request was
//! forwarded upstream*, not whether a 403 came back. A status-only assertion passes
//! against an implementation that rejects *and also* forwards, and against one that
//! returns 403 for an unrelated reason (a bad `Host` header, a missing allowlist entry,
//! a DNS failure). So [`enforcement_rejects_prohibited_destination_and_forwards_nothing`]
//! asserts on `StubS3Client::captured()` — the upstream call set, empty — as its primary
//! signal, and prints the status and body on failure so "rejected for the right reason"
//! and "rejected for some other reason" are distinguishable in the output.
//!
//! **A fixture that cannot fail.** A fix that enforced *unconditionally*, or that handed
//! the CONNECT path's 443-valued policy to the HTTP path, would satisfy the enforcement
//! test while breaking every legitimate request. Two tests guard that width and neither is
//! optional: [`allowed_host_on_the_http_port_is_not_rejected`] (allowlisted host, public
//! address, must forward) and [`no_allowlist_configured_forwards_unchanged`] (the default
//! deployment must evaluate nothing).
//!
//! # The internal control
//!
//! [`enforcement_rejects_prohibited_destination_and_forwards_nothing`] and
//! [`no_allowlist_configured_forwards_unchanged`] send the **same request** — same method,
//! same path, same `Host: 169.254.169.254`, same `authorization` — through the same listener
//! code, and differ in exactly one input: whether `connect_allowlist` is configured. One must
//! reject with nothing forwarded, the other must forward. Without that pair, a rejection could
//! equally mean the fixture never reached the policy at all.
//!
//! # No network, and why the fixture can assert on IP-range blocking offline
//!
//! Every destination is an **IP literal**. `hickory_resolver`'s `lookup_ip` returns an IP
//! literal directly without issuing a query (`resolver.rs`, the `host.to_ip()` early
//! return, taken whenever `ndots <= 4` — the default is 1), so `DestinationPolicy::check`
//! reaches its IP-classification step with no DNS server involved. `169.254.169.254` is the
//! IMDS address and is link-local, so `is_prohibited` rejects it; `93.184.216.34` is public,
//! so it passes. Both verdicts are deterministic and offline.
//!
//! # Red side
//!
//! Restore `None, None` at the `HttpProxy::handle_request` call site in
//! `TlsProxyListener::start`. Only
//! [`enforcement_rejects_prohibited_destination_and_forwards_nothing`] may fail; the other
//! three must pass in both states, which is what shows the fixture is sound either way and
//! that the flip is attributable to the enforcement alone.
//!
//! Tests 5 and 6 cover the port gate. Restoring `handle_request`'s old `check_port` (the
//! request-target port, or `http_port` in origin-form) turns the origin-form arm of
//! [`host_header_port_is_the_checked_port`] red; restoring the host-only conflict check
//! turns its absolute-form arm red. [`non_standard_port_store_still_passes`] must pass in
//! every state.
//!
//! The two levers on the port gate are not equivalent. Restoring the old `check_port` (lever
//! A) is a real bypass: a `Host` port the policy never admitted is forwarded upstream.
//! Restoring the host-only conflict check (lever B) is a layer-attribution pin only: the
//! request is still refused and nothing is forwarded, but by the policy as a 403 instead of
//! by the conflict check as a 400. Do not read lever B going red as a security regression.

mod common;

use common::expired_fixture::{signed_authorization_without_range, Fixture};
use common::{StubResponse, StubS3Client};

use rcgen::generate_simple_self_signed;
use s3_proxy::config::{Config, TlsConfig, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::shutdown::ShutdownSignal;
use s3_proxy::tls_proxy_listener::TlsProxyListener;
use s3_proxy::S3ClientApi;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Semaphore};
use tokio_rustls::TlsConnector;

/// IMDS. Link-local, so `destination_policy::is_prohibited` classifies it as prohibited.
const PROHIBITED_HOST: &str = "169.254.169.254";
/// A public address, so `is_prohibited` returns false and the policy admits it.
const ALLOWED_HOST: &str = "93.184.216.34";
/// The object every test requests. Identical across tests so the only difference
/// between the enforcement case and its control is the allowlist.
const REQUEST_PATH: &str = "/test-bucket/policy-probe.bin";

/// Write an rcgen self-signed cert and its key to PEM files, since
/// `TlsProxyListener::new` takes PEM **paths** rather than parsed material.
fn write_cert_pem(dir: &Path) -> (String, String) {
    let generated = generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen failed to generate a self-signed cert");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, generated.cert.pem()).expect("write cert pem");
    std::fs::write(&key_path, generated.key_pair.serialize_pem()).expect("write key pem");
    (
        cert_path.display().to_string(),
        key_path.display().to_string(),
    )
}

/// A config with the TLS listener enabled and `connect_allowlist` set to `allowlist`.
///
/// `None` is the default deployment — the state acceptance criterion 1.3 requires to be
/// unchanged. Download coordination and the RAM range tier are off for the same reason
/// every other fixture in `tests/common` turns them off: they are confounds for a test
/// about which requests reach S3 at all.
///
/// `http_port` is the port the HTTP-path policy admits. When it is not 80, the fixture also
/// declares an `upstream_overrides` entry for `ALLOWED_HOST` at that port, the documented
/// shape of a store on a non-standard port.
fn listener_config(
    cert_path: &str,
    key_path: &str,
    allowlist: Option<Vec<String>>,
    http_port: u16,
) -> Arc<Config> {
    let mut config = Config::default();
    config.server.http_port = http_port;
    if http_port != 80 {
        config.connection_pool.upstream_overrides.insert(
            format!("{ALLOWED_HOST}:{http_port}"),
            UpstreamOverrideConfig {
                scheme: UpstreamScheme::Http,
                validate_tls: true,
            },
        );
    }
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.server.tls = Some(TlsConfig {
        enabled: true,
        // Overwritten by the caller's bound port; `TlsProxyListener::new` takes the
        // listen address as a separate argument and never reads this field.
        tls_proxy_port: 0,
        cert_path: cert_path.to_string(),
        key_path: key_path.to_string(),
        connect_allowlist: allowlist,
    });
    Arc::new(config)
}

/// Everything a test needs to keep alive for the duration of a request.
struct Harness {
    addr: SocketAddr,
    stub: StubS3Client,
    /// Held so `ShutdownSignal::wait_for_shutdown` stays pending. Dropping it ends the
    /// listener's accept loop.
    _shutdown_tx: broadcast::Sender<()>,
    _cert_dir: TempDir,
    _fixture: Fixture,
}

/// Stand up a real `TlsProxyListener` on an ephemeral loopback port.
///
/// The port is obtained by binding a probe listener, reading its address and dropping it,
/// because `TlsProxyListener::start` binds internally and does not expose the bound
/// address. `connect_tls` then polls until the real listener is accepting, so the gap
/// between releasing the probe and the listener binding is not a race the test can lose
/// silently — a port stolen in that window surfaces as a connect failure naming the
/// fixture, not as a policy verdict.
async fn spawn_listener(allowlist: Option<Vec<String>>) -> Harness {
    spawn_listener_on(allowlist, 80).await
}

/// [`spawn_listener`] with the HTTP-path policy's port set to `http_port`.
async fn spawn_listener_on(allowlist: Option<Vec<String>>, http_port: u16) -> Harness {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_dir = TempDir::new().expect("cert tempdir");
    let (cert_path, key_path) = write_cert_pem(cert_dir.path());
    let config = listener_config(&cert_path, &key_path, allowlist, http_port);

    let fixture = Fixture::new(Arc::clone(&config)).await;

    // Any 2xx will do: these tests assert on *whether* S3 was called, never on the
    // response payload.
    let stub = StubS3Client::new().with_default(StubResponse::ok(vec![b'x'; 64]));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = stub.clone().into_trait_object();

    let probe = TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
    let addr = probe.local_addr().expect("probe local addr");
    drop(probe);

    let listener = TlsProxyListener::new(
        addr,
        &cert_path,
        &key_path,
        Arc::clone(&config),
        Arc::clone(&fixture.cache_manager),
        s3_client,
        Arc::clone(&fixture.range_handler),
        Arc::new(Semaphore::new(config.server.max_concurrent_requests)),
        Arc::new(AtomicUsize::new(0)),
        None,
        None,
        Arc::clone(&fixture.inflight_tracker),
        None,
    )
    .expect("TlsProxyListener::new");

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    tokio::spawn(async move {
        let _ = listener.start(ShutdownSignal::new(shutdown_rx)).await;
    });

    Harness {
        addr,
        stub,
        _shutdown_tx: shutdown_tx,
        _cert_dir: cert_dir,
        _fixture: fixture,
    }
}

/// Complete a TLS handshake against the listener, retrying while it comes up.
async fn connect_tls(addr: SocketAddr) -> tokio_rustls::client::TlsStream<TcpStream> {
    // `build_tls_accept_any_config` skips chain and hostname verification, which is what
    // a self-signed listener cert needs. It is the crate's own accept-any config rather
    // than a bespoke verifier so the test cannot drift from production's TLS setup.
    let connector = TlsConnector::from(Arc::new(
        s3_proxy::https_connector::build_tls_accept_any_config(),
    ));
    let server_name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");

    let mut last_err = String::new();
    for _ in 0..50 {
        match TcpStream::connect(addr).await {
            Ok(tcp) => match connector.connect(server_name.clone(), tcp).await {
                Ok(tls) => return tls,
                Err(e) => last_err = format!("TLS handshake: {e}"),
            },
            Err(e) => last_err = format!("TCP connect: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not reach the TLS listener at {addr} within 1s: {last_err}");
}

/// Send one raw HTTP/1.1 request over the decrypted stream and return
/// `(status_line, whole_response)`.
///
/// `connection: close` makes the response self-delimiting so `read_to_end` terminates
/// without needing a full HTTP client. An `authorization` header is always attached
/// because its absence sends the proxy down a different, unsigned path — which would
/// silently change which code is under test.
async fn send_request(addr: SocketAddr, request_line: &str, host: &str) -> (String, String) {
    let mut tls = connect_tls(addr).await;
    let request = format!(
        "{request_line}\r\nHost: {host}\r\nauthorization: {}\r\nconnection: close\r\n\r\n",
        signed_authorization_without_range()
    );
    tls.write_all(request.as_bytes())
        .await
        .expect("write request");
    tls.flush().await.expect("flush request");

    let mut raw = Vec::new();
    // A tunnel-establishing CONNECT never closes, so bound the read. Every response these
    // tests assert on is a normal `connection: close` response and completes immediately.
    let _ = tokio::time::timeout(Duration::from_secs(10), tls.read_to_end(&mut raw)).await;
    let response = String::from_utf8_lossy(&raw).to_string();
    let status_line = response.lines().next().unwrap_or_default().to_string();
    (status_line, response)
}

/// **Test 1 — enforcement.** With `connect_allowlist` configured, a non-CONNECT request to
/// a host the allowlist permits but whose address is link-local is rejected 403
/// `AccessDenied`, **and nothing is sent upstream**.
///
/// The allowlist deliberately contains the target, so the rejection cannot come from the
/// allowlist gate. It has to come from IP classification — which is the gate that only runs
/// if the policy reached `handle_request` at all.
///
/// This is the ONLY test in this file that fails when the enforcement is removed.
#[tokio::test]
async fn enforcement_rejects_prohibited_destination_and_forwards_nothing() {
    let harness = spawn_listener(Some(vec![PROHIBITED_HOST.to_string()])).await;

    let (status_line, response) = send_request(
        harness.addr,
        &format!("GET {REQUEST_PATH} HTTP/1.1"),
        PROHIBITED_HOST,
    )
    .await;

    let captured = harness.stub.captured();

    // The upstream call set is the predicate. Status and body are printed rather than
    // asserted first, because "403 for the right reason" and "403 for some other reason"
    // are different outcomes and a bare verdict cannot tell them apart.
    assert!(
        captured.is_empty(),
        "R1.2: a request to a prohibited destination was forwarded upstream from the TLS \
         listener's HTTP path. status_line={status_line:?} captured={captured:#?}"
    );
    assert!(
        status_line.contains("403"),
        "R1.2: expected 403 for a prohibited destination, got {status_line:?}. \
         Full response: {response:?}"
    );
    assert!(
        response.contains("AccessDenied"),
        "R1.2: the rejection must carry the AccessDenied error code. Response: {response:?}"
    );
}

/// **Test 2 — the default deployment is unchanged, and the control for test 1.**
///
/// Same request as test 1, same host, no allowlist. It must forward: with no allowlist
/// there is no policy on either listener, so nothing is evaluated. Without this, test 1's
/// rejection could equally mean the request never reached the forwarding path.
#[tokio::test]
async fn no_allowlist_configured_forwards_unchanged() {
    let harness = spawn_listener(None).await;

    let (status_line, response) = send_request(
        harness.addr,
        &format!("GET {REQUEST_PATH} HTTP/1.1"),
        PROHIBITED_HOST,
    )
    .await;

    assert!(
        !response.contains("AccessDenied"),
        "R1.3: with no allowlist configured no policy may be evaluated, but the request \
         was rejected by one. status_line={status_line:?} response={response:?}"
    );
    assert!(
        !harness.stub.captured().is_empty(),
        "R1.3: the default deployment must forward as it does today, but nothing reached \
         S3. status_line={status_line:?} response={response:?}"
    );
}

/// **Test 3 — CONNECT is unchanged, and still evaluated against 443.**
///
/// Two arms, both under a configured allowlist that permits the target host, so neither
/// rejection can come from the allowlist gate:
///
/// - `CONNECT host:80` must be **rejected**. `DestinationPolicy::check` runs the port gate
///   first, and the CONNECT policy's `allowed_port` is still 443. This is the assertion that
///   fails if someone "fixes" R1 by repointing the existing policy at `server.http_port`
///   instead of adding a second one — a change that would leave CONNECT tunnels reachable on
///   arbitrary HTTP ports.
/// - `CONNECT host:443` to a link-local address must be **rejected** too, showing the CONNECT
///   path's IP classification still runs.
///
/// A prohibited address is used on the 443 arm deliberately: an admitted CONNECT would spawn
/// a real tunnel and dial the network.
#[tokio::test]
async fn connect_still_evaluates_against_port_443() {
    let harness = spawn_listener(Some(vec![PROHIBITED_HOST.to_string()])).await;

    let (port_80_status, port_80_response) = send_request(
        harness.addr,
        &format!("CONNECT {PROHIBITED_HOST}:80 HTTP/1.1"),
        PROHIBITED_HOST,
    )
    .await;
    assert!(
        port_80_status.contains("403"),
        "R1.4: CONNECT must still be gated at port 443, so a CONNECT to :80 must be \
         refused. Got {port_80_status:?}. Full response: {port_80_response:?}"
    );

    let (port_443_status, port_443_response) = send_request(
        harness.addr,
        &format!("CONNECT {PROHIBITED_HOST}:443 HTTP/1.1"),
        PROHIBITED_HOST,
    )
    .await;
    assert!(
        port_443_status.contains("403"),
        "R1.4: CONNECT to a link-local address on 443 must still be refused by IP \
         classification. Got {port_443_status:?}. Full response: {port_443_response:?}"
    );

    assert!(
        harness.stub.captured().is_empty(),
        "a CONNECT must never reach the caching egress: {:#?}",
        harness.stub.captured()
    );
}

/// **Test 4 — width. An allowed host on the HTTP port is NOT rejected.**
///
/// The guard against an over-broad fix, and not optional. Handing the CONNECT path's
/// 443-valued policy to the HTTP path would satisfy test 1 — `PortNotAllowed` is also a
/// 403 with nothing forwarded — while rejecting every legitimate request an operator sends.
/// Test 1 alone cannot tell those apart; this one can, because it fails in exactly that
/// case and passes for the real fix.
#[tokio::test]
async fn allowed_host_on_the_http_port_is_not_rejected() {
    let harness = spawn_listener(Some(vec![ALLOWED_HOST.to_string()])).await;

    let (status_line, response) = send_request(
        harness.addr,
        &format!("GET {REQUEST_PATH} HTTP/1.1"),
        ALLOWED_HOST,
    )
    .await;

    assert!(
        !response.contains("AccessDenied"),
        "R1.1: an allowlisted host with a public address on the HTTP port must not be \
         rejected — this is the over-broad-fix signature (e.g. a policy whose allowed_port \
         is 443 rejecting a port-80 forward). status_line={status_line:?} response={response:?}"
    );
    assert!(
        !harness.stub.captured().is_empty(),
        "R1.1: an admitted destination must still be forwarded upstream, but nothing \
         reached S3. status_line={status_line:?} response={response:?}"
    );
}

/// **Test 5 — the port gate evaluates the port the egress dials.**
///
/// The egress takes its port from the `Host` header (`build_egress_authority`), so the
/// policy must too. Allowlisted host, public address, `http_port` 80, and a `Host` naming
/// port 6379. Two arms, both of which must be refused with nothing forwarded:
///
/// - **Origin-form** (`GET /path`): before the fix the policy checked `http_port` here
///   whatever the `Host` header said, so this arm forwarded.
/// - **Absolute-form** (`GET http://host/path`): the request-target port (absent, so 80)
///   differs from the `Host` port. Before the fix the policy checked the request-target
///   port and the conflict check compared hosts only, so this arm forwarded too.
///
/// Test 4 is the width control: the same host with a portless `Host` must still forward.
#[tokio::test]
async fn host_header_port_is_the_checked_port() {
    let harness = spawn_listener(Some(vec![ALLOWED_HOST.to_string()])).await;
    let split_host = format!("{ALLOWED_HOST}:6379");

    let (origin_status, origin_response) = send_request(
        harness.addr,
        &format!("GET {REQUEST_PATH} HTTP/1.1"),
        &split_host,
    )
    .await;
    let (absolute_status, absolute_response) = send_request(
        harness.addr,
        &format!("GET http://{ALLOWED_HOST}{REQUEST_PATH} HTTP/1.1"),
        &split_host,
    )
    .await;

    let captured = harness.stub.captured();
    assert!(
        captured.is_empty(),
        "a Host port the policy did not admit was forwarded upstream. \
         origin-form: {origin_status:?}; absolute-form: {absolute_status:?}; \
         captured={captured:#?}"
    );
    assert!(
        origin_status.contains("403") && origin_response.contains("AccessDenied"),
        "origin-form: expected 403 AccessDenied (port 6379 not allowed), got \
         {origin_status:?}. Full response: {origin_response:?}"
    );
    assert!(
        absolute_status.contains("400"),
        "absolute-form: expected 400 (Host port differs from the request-target port), got \
         {absolute_status:?}. Full response: {absolute_response:?}"
    );
}

/// **Test 6 — width. A store on a non-standard port still passes.**
///
/// `http_port` 9000 and an `upstream_overrides` entry for `ALLOWED_HOST:9000`: the
/// standard-mode recipe for fronting a store on a non-standard port. The policy never reads
/// `upstream_overrides`; what the entry adds is the documented shape. Three requests, all of
/// which must forward:
///
/// - origin-form with `Host: host:9000`, the shape a client aimed at the store sends;
/// - origin-form with a portless `Host`, the shape a load balancer forwarding port 80 to a
///   proxy listening on another port delivers;
/// - absolute-form `http://host:9000/` with `Host: host:9000`.
///
/// A fix that checked a portless `Host` as port 80 rather than as the listener's port
/// would refuse the second, and one that dropped the authority port would refuse none
/// while test 5 still caught it, which is why both tests exist.
#[tokio::test]
async fn non_standard_port_store_still_passes() {
    let harness = spawn_listener_on(Some(vec![ALLOWED_HOST.to_string()]), 9000).await;
    let store = format!("{ALLOWED_HOST}:9000");

    // A distinct key per request, so a cache hit cannot stand in for a forward.
    let requests = [
        (format!("GET {REQUEST_PATH}.a HTTP/1.1"), store.clone()),
        (
            format!("GET {REQUEST_PATH}.b HTTP/1.1"),
            ALLOWED_HOST.to_string(),
        ),
        (
            format!("GET http://{store}{REQUEST_PATH}.c HTTP/1.1"),
            store.clone(),
        ),
    ];
    for (i, (request_line, host)) in requests.iter().enumerate() {
        let before = harness.stub.captured().len();
        let (status_line, response) = send_request(harness.addr, request_line, host).await;
        assert!(
            !response.contains("AccessDenied") && !status_line.contains("400"),
            "request {i} ({request_line}, Host: {host}) to an allowlisted store on the \
             listener's port was refused. status_line={status_line:?} response={response:?}"
        );
        assert!(
            harness.stub.captured().len() > before,
            "request {i} ({request_line}, Host: {host}) was admitted but nothing reached S3. \
             status_line={status_line:?} response={response:?}"
        );
        // The egress dials the Host port, so a request that names :9000 must reach the
        // stub addressed to :9000, not only pass the policy.
        if host.ends_with(":9000") {
            let captured = harness.stub.captured();
            let uri = &captured.last().expect("captured request").uri;
            assert!(
                uri.contains(":9000"),
                "request {i} ({request_line}, Host: {host}) was forwarded but the upstream \
                 URI does not carry the dialled port :9000: {uri:?}"
            );
        }
    }
}
