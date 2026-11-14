//! Enrolment: the join token spent on a client certificate. The worker makes its key here and sends only a
//! certificate request; it trusts the manager only if the manager's chain holds the CA the token pins.

use anyhow::{Context, Result, anyhow, bail};
use rustls::pki_types::{CertificateDer, pem::PemObject};

use freightdock_proto::sha256_hex;
use freightdock_proto::token::JoinToken;
use freightdock_proto::v1::{EnrollRequest, enrollment_client::EnrollmentClient};

use crate::{Config, Files, write_private};

/// The token given (or, when none is, the one in the token file), when it is one this worker has not spent already.
pub fn fresh_token(config: &Config, files: &Files) -> Option<String> {
    let from_file = || config.token_file.as_ref().and_then(|p| std::fs::read_to_string(p).ok()).map(|t| t.trim().to_string());
    let t = config.token.clone().or_else(from_file)?;
    let t = t.as_str();
    let parsed = JoinToken::parse(t)?;
    let spent = std::fs::read_to_string(&files.spent).unwrap_or_default();
    (spent.trim() != parsed.secret_hash()).then(|| t.to_string())
}

/// Spends the token at the first manager that answers. Keeps key, certificate, CA and the managers it was told of.
pub async fn enrol(config: &Config, files: &Files, token: &str, hostname: &str) -> Result<String> {
    let t = JoinToken::parse(token).ok_or_else(|| anyhow!("not a join token"))?;
    let node_id = files.node_id()?;
    let key = rcgen::KeyPair::generate()?;
    let csr = rcgen::CertificateParams::new(Vec::<String>::new())?.serialize_request(&key)?.pem()?;
    let tls = crate::tls::pinned(&t.ca_sha256)?;
    let mut last = anyhow!("no manager to enrol with: set FREIGHTDOCK_MANAGERS");
    for addr in crate::dial_order(&config.managers, &files.known_managers()) {
        let ch = match crate::tls::channel(&addr, tls.clone()).await {
            Ok(ch) => ch,
            Err(e) => {
                tracing::warn!("{addr}: {e:#}");
                last = e;
                continue;
            }
        };
        let req = EnrollRequest { join_token: token.to_string(), csr_pem: csr.clone(), node_id: node_id.clone(), hostname: hostname.to_string() };
        let r = match EnrollmentClient::new(ch).enroll(req).await {
            Ok(r) => r.into_inner(),
            Err(s) if matches!(s.code(), tonic::Code::PermissionDenied | tonic::Code::InvalidArgument | tonic::Code::AlreadyExists) => {
                // Spent, expired or another cluster's: trying it again would not help.
                let _ = std::fs::write(&files.spent, t.secret_hash());
                bail!("the manager refused the join token: {}", s.message());
            }
            Err(s) => {
                last = anyhow!("{addr}: {}", s.message());
                continue;
            }
        };
        let ca_der = CertificateDer::from_pem_slice(r.ca_pem.as_bytes()).context("the cluster CA")?;
        if sha256_hex(&ca_der) != t.ca_sha256 {
            bail!("the manager sent a CA that is not the one the join token names");
        }
        write_private(&files.key, key.serialize_pem().as_bytes())?;
        std::fs::write(&files.cert, &r.cert_pem).context("keeping the certificate")?;
        std::fs::write(&files.ca, &r.ca_pem).context("keeping the cluster CA")?;
        std::fs::write(&files.spent, t.secret_hash()).context("noting the token as spent")?;
        files.keep_managers(&r.managers);
        return Ok(r.cluster_id);
    }
    Err(last)
}

/// A new key and certificate (G2.7), asked on `ch` (a channel made with the node's current certificate). Kept only
/// when it comes from the cluster's own CA; the current ones stay good at the managers until the next renewal.
pub async fn renew(files: &Files, ch: tonic::transport::Channel) -> Result<()> {
    let key = rcgen::KeyPair::generate()?;
    let csr = rcgen::CertificateParams::new(Vec::<String>::new())?.serialize_request(&key)?.pem()?;
    let r = freightdock_proto::v1::worker_session_client::WorkerSessionClient::new(ch).renew(freightdock_proto::v1::RenewRequest { csr_pem: csr }).await.map_err(|s| anyhow!("renewing the certificate: {}", s.message()))?.into_inner();
    let ours = CertificateDer::from_pem_file(&files.ca).context("the kept cluster CA")?;
    let theirs = CertificateDer::from_pem_slice(r.ca_pem.as_bytes()).context("the CA the manager sent")?;
    if sha256_hex(&ours) != sha256_hex(&theirs) {
        bail!("the manager sent a certificate of another CA");
    }
    CertificateDer::from_pem_slice(r.cert_pem.as_bytes()).context("the renewed certificate")?;
    files.keep_renewed(&key.serialize_pem(), &r.cert_pem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_is_fresh_until_it_is_spent() {
        let dir = tempfile::tempdir().unwrap();
        let files = Files::in_dir(dir.path());
        let t = JoinToken { ca_sha256: "a".repeat(64), secret: "b".repeat(64) };
        let mut c = Config { managers: vec![], token: Some(t.to_string()), token_file: None, data_dir: dir.path().into(), docker: "/x".into(), labels: Default::default(), host_shell: true };
        assert_eq!(fresh_token(&c, &files), Some(t.to_string()));
        std::fs::write(&files.spent, t.secret_hash()).unwrap();
        assert_eq!(fresh_token(&c, &files), None, "a token is tried once");
        c.token = None;
        assert_eq!(fresh_token(&c, &files), None);
    }

    #[test]
    fn a_token_can_come_from_a_file_that_appears_later() {
        let dir = tempfile::tempdir().unwrap();
        let files = Files::in_dir(dir.path());
        let path = dir.path().join("local-worker.token");
        let c = Config { managers: vec![], token: None, token_file: Some(path.clone()), data_dir: dir.path().into(), docker: "/x".into(), labels: Default::default(), host_shell: true };
        assert_eq!(fresh_token(&c, &files), None, "not there yet: the next start tries again");
        let t = JoinToken { ca_sha256: "a".repeat(64), secret: "c".repeat(64) };
        std::fs::write(&path, format!("{t}\n")).unwrap();
        assert_eq!(fresh_token(&c, &files), Some(t.to_string()));
    }
}
