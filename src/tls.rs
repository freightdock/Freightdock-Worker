//! TLS to a manager. Before enrolment: no client certificate, and the manager trusted only if its chain holds the CA
//! the join token pins. After: the client certificate, and the manager checked against the kept CA. Either way the
//! name checked is `MANAGER_SERVER_NAME`, whatever address was dialled.
//!
//! tonic is given a connector of our own (TCP with keepalive, then this TLS), so the rustls settings are ours and not
//! whatever tonic's TLS feature allows.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tonic::transport::{Channel, Endpoint, Uri};

use freightdock_proto::{MANAGER_SERVER_NAME, sha256_hex};

use crate::Files;

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Checks the manager's certificate against the CA in its chain whose fingerprint is the token's, as a normal check
/// against that CA as the only root.
#[derive(Debug)]
struct PinnedCa {
    pin: String,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for PinnedCa {
    fn verify_server_cert(&self, end: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], name: &ServerName<'_>, ocsp: &[u8], now: UnixTime) -> Result<ServerCertVerified, rustls::Error> {
        let ca = intermediates.iter().find(|c| sha256_hex(c) == self.pin).ok_or_else(|| rustls::Error::General("this is not the cluster the join token is for".into()))?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.clone().into_owned()).map_err(|e| rustls::Error::General(e.to_string()))?;
        let v = rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), self.provider.clone()).build().map_err(|e| rustls::Error::General(e.to_string()))?;
        v.verify_server_cert(end, &[], name, ocsp, now)
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

fn h2(mut c: rustls::ClientConfig) -> Arc<rustls::ClientConfig> {
    c.alpn_protocols = vec![b"h2".to_vec()];
    Arc::new(c)
}

/// For enrolment: no client certificate, the manager checked against the pinned CA.
pub fn pinned(pin: &str) -> Result<Arc<rustls::ClientConfig>> {
    let p = provider();
    let c = rustls::ClientConfig::builder_with_provider(p.clone())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCa { pin: pin.to_string(), provider: p }))
        .with_no_client_auth();
    Ok(h2(c))
}

/// For the session: this node's certificate, the manager checked against the kept CA.
pub fn mutual(files: &Files) -> Result<Arc<rustls::ClientConfig>> {
    let ca = CertificateDer::from_pem_file(&files.ca).context("the kept cluster CA")?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca)?;
    let chain = vec![CertificateDer::from_pem_file(&files.cert).context("the kept certificate")?];
    let key = PrivateKeyDer::from_pem_file(&files.key).context("the kept key")?;
    let c = rustls::ClientConfig::builder_with_provider(provider()).with_safe_default_protocol_versions()?.with_root_certificates(roots).with_client_auth_cert(chain, key)?;
    Ok(h2(c))
}

async fn dial(addr: String, tls: Arc<rustls::ClientConfig>) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let within = Duration::from_secs(10);
    let tcp = tokio::time::timeout(within, tokio::net::TcpStream::connect(&addr)).await.map_err(|_| anyhow!("{addr} did not answer within 10s"))?.with_context(|| format!("connecting to {addr}"))?;
    let _ = tcp.set_nodelay(true);
    let k = socket2::TcpKeepalive::new().with_time(Duration::from_secs(30)).with_interval(Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let k = k.with_retries(3);
    let _ = socket2::SockRef::from(&tcp).set_tcp_keepalive(&k);
    let name = ServerName::try_from(MANAGER_SERVER_NAME).expect("a valid name");
    let tls = tokio::time::timeout(within, tokio_rustls::TlsConnector::from(tls).connect(name, tcp)).await.map_err(|_| anyhow!("the TLS handshake with {addr} took too long"))?.with_context(|| format!("the TLS handshake with {addr}"))?;
    Ok(tls)
}

/// A gRPC channel to the manager at `addr`, over our TLS.
pub async fn channel(addr: &str, tls: Arc<rustls::ClientConfig>) -> Result<Channel> {
    // The scheme is http because TLS is the connector's business; the URI is only a label to tonic.
    let ep = Endpoint::from_shared(format!("http://{addr}"))?
        .connect_timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    let addr = addr.to_string();
    let connector = tower::service_fn(move |_: Uri| {
        let (addr, tls) = (addr.clone(), tls.clone());
        async move { dial(addr, tls).await.map(hyper_util::rt::TokioIo::new).map_err(|e| std::io::Error::other(format!("{e:#}"))) }
    });
    Ok(ep.connect_with_connector(connector).await?)
}
