// Feature: dependency-security-upgrades, Task 2.2: TLS handshake preservation
//
// Establishes a baseline green run on the unchanged workspace before Task 4 (rustls-webpki bump).
// After the Cluster B fix lands, this test must continue to pass — any regression in the
// rustls / tokio-rustls / rustls-webpki surface will surface here before it reaches production.
//
// This is a STANDALONE test. It does NOT spin up TlsProxyListener (which requires too many
// dependencies). Instead it builds a minimal TLS server directly using tokio-rustls + rustls,
// with an rcgen-generated self-signed certificate for "localhost".
//
// **Validates: Cluster B preservation (TLS handshake correctness after rustls-webpki bump)**

use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Spin up a minimal TLS server on an ephemeral port, connect a client that trusts the
/// self-signed cert, exchange a trivial HTTP/1.1 request/response, and assert the response
/// starts with "HTTP/1.1 200".
///
/// No outbound network, no real CA — the cert is generated in-process by rcgen.
#[tokio::test]
async fn tls_handshake_and_http_roundtrip() {
    // -----------------------------------------------------------------------
    // 1. Generate a self-signed certificate for "localhost" using rcgen.
    // -----------------------------------------------------------------------
    let subject_alt_names = vec!["localhost".to_string()];
    let cert = generate_simple_self_signed(subject_alt_names)
        .expect("rcgen failed to generate self-signed cert");

    // Serialize to DER for rustls
    let cert_der: CertificateDer<'static> = CertificateDer::from(cert.cert.der().to_vec());
    let key_der: PrivateKeyDer<'static> =
        PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

    // -----------------------------------------------------------------------
    // 2. Build rustls::ServerConfig from the generated cert/key.
    // -----------------------------------------------------------------------
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .expect("rustls ServerConfig::with_single_cert failed");

    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // -----------------------------------------------------------------------
    // 3. Bind a TcpListener on 127.0.0.1:0 (ephemeral port).
    // -----------------------------------------------------------------------
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let server_addr = listener.local_addr().expect("failed to get local addr");

    // -----------------------------------------------------------------------
    // 4. Server task: accept one connection, perform TLS handshake, read the
    //    HTTP request, write a minimal HTTP/1.1 200 response.
    // -----------------------------------------------------------------------
    let server_task = tokio::spawn(async move {
        let (tcp_stream, _peer_addr) = listener.accept().await.expect("server: accept failed");

        let mut tls_stream = tls_acceptor
            .accept(tcp_stream)
            .await
            .expect("server: TLS handshake failed");

        // Read until we see the end of the HTTP request headers (\r\n\r\n)
        let mut request_buf = vec![0u8; 4096];
        let mut total_read = 0usize;
        loop {
            let n = tls_stream
                .read(&mut request_buf[total_read..])
                .await
                .expect("server: read failed");
            if n == 0 {
                break;
            }
            total_read += n;
            // Check for end-of-headers marker
            if request_buf[..total_read]
                .windows(4)
                .any(|w| w == b"\r\n\r\n")
            {
                break;
            }
            if total_read >= request_buf.len() {
                break;
            }
        }

        // Write a minimal HTTP/1.1 200 OK response
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
        tls_stream
            .write_all(response)
            .await
            .expect("server: write_all failed");
        tls_stream
            .shutdown()
            .await
            .expect("server: shutdown failed");
    });

    // -----------------------------------------------------------------------
    // 5. Client task: build rustls::ClientConfig with the self-signed cert in
    //    its root store, connect, perform TLS handshake, send an HTTP request,
    //    read the response, assert it starts with "HTTP/1.1 200".
    // -----------------------------------------------------------------------
    let client_task = tokio::spawn(async move {
        // Add the self-signed cert to a custom root store
        let mut root_store = RootCertStore::empty();
        root_store
            .add(cert_der)
            .expect("client: failed to add self-signed cert to root store");

        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(client_config));

        let tcp_stream = TcpStream::connect(server_addr)
            .await
            .expect("client: TCP connect failed");

        let server_name = ServerName::try_from("localhost").expect("client: invalid server name");

        let mut tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .expect("client: TLS handshake failed");

        // Send a minimal HTTP/1.1 GET request
        let request = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        tls_stream
            .write_all(request)
            .await
            .expect("client: write_all failed");

        // Read the full response
        let mut response_buf = Vec::new();
        tls_stream
            .read_to_end(&mut response_buf)
            .await
            .expect("client: read_to_end failed");

        let response_str = String::from_utf8_lossy(&response_buf);

        assert!(
            response_str.starts_with("HTTP/1.1 200"),
            "Expected response to start with 'HTTP/1.1 200', got: {:?}",
            &response_str[..response_str.len().min(80)]
        );
    });

    // -----------------------------------------------------------------------
    // 6. Run server and client concurrently; propagate any panics.
    // -----------------------------------------------------------------------
    let (server_result, client_result) = tokio::join!(server_task, client_task);
    server_result.expect("server task panicked");
    client_result.expect("client task panicked");
}
