//! Upgrades (the Upgrade command): the release's binary is fetched from the manager this worker is connected to
//! (WorkerSession.Fetch, over the session's own mutual TLS, so a worker needs no way out to the internet), checked
//! against the size and SHA-256 the command names, written beside the running binary and renamed over it, so the
//! binary in place is always whole: the old one or the new one. Under systemd (Restart=always) the worker then exits
//! and comes back on the new one; its containers keep running, as they do over any restart of the worker.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
pub use freightdock_proto::sha256_hex;

/// Whether `bytes` are the binary the command names: its size and checksum both.
pub fn check(bytes: &[u8], sha256: &str, size: u64) -> Result<()> {
    if bytes.is_empty() {
        bail!("the binary came empty");
    }
    if bytes.len() as u64 != size {
        bail!("the binary is {} bytes, not the {size} the release says", bytes.len());
    }
    if sha256_hex(bytes) != sha256.to_ascii_lowercase() {
        bail!("the binary does not match its checksum");
    }
    Ok(())
}

/// Puts `bytes` in place of `exe`: written beside it, made executable, synced and renamed over it.
pub fn replace(exe: &Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = exe.parent().context("the binary has no folder")?;
    let tmp = dir.join(format!(".{}.new", exe.file_name().and_then(|n| n.to_str()).unwrap_or("freightdock-worker")));
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o755).open(&tmp).with_context(|| format!("writing {}", tmp.display()))?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, exe).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    }).with_context(|| format!("putting it in place of {}", exe.display()))?;
    Ok(())
}

/// The binary an Upgrade names, from the manager at the other end of `ch`: no larger than it says, then checked.
pub async fn fetch(ch: tonic::transport::Channel, sha256: &str, size: u64) -> Result<Vec<u8>> {
    use freightdock_proto::v1::{FetchRequest, worker_session_client::WorkerSessionClient};
    let mut s = WorkerSessionClient::new(ch).fetch(FetchRequest { sha256: sha256.to_string() }).await.map_err(|s| anyhow::anyhow!("fetching the binary: {}", s.message()))?.into_inner();
    let mut bytes = Vec::with_capacity(size.min(512 << 20) as usize);
    while let Some(c) = s.message().await.map_err(|s| anyhow::anyhow!("fetching the binary: {}", s.message()))? {
        bytes.extend_from_slice(&c.data);
        if bytes.len() as u64 > size {
            bail!("the binary is larger than the release says");
        }
    }
    check(&bytes, sha256, size)?;
    Ok(bytes)
}

/// Whether systemd runs this worker (and so starts it again when it exits).
pub fn under_systemd() -> bool {
    std::env::var_os("INVOCATION_ID").is_some()
}

/// The running binary's path, as it was started (Linux says a replaced one is "(deleted)").
pub fn running_exe() -> Result<PathBuf> {
    let p = std::env::current_exe().context("finding the worker's own binary")?;
    let s = p.to_string_lossy();
    Ok(PathBuf::from(s.strip_suffix(" (deleted)").unwrap_or(&s).to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_binary_is_taken_only_with_its_size_and_checksum_both() {
        let b = b"\x7fELF new binary";
        let sum = sha256_hex(b);
        assert_eq!(sum.len(), 64);
        assert!(check(b, &sum, b.len() as u64).is_ok());
        assert!(check(b, &sum.to_uppercase(), b.len() as u64).is_ok(), "hex in either case");
        assert!(check(b, &sum, b.len() as u64 + 1).is_err(), "another size");
        assert!(check(b, &sha256_hex(b"other"), b.len() as u64).is_err(), "another checksum");
        assert!(check(b"", &sha256_hex(b""), 0).is_err(), "never an empty binary");
    }

    #[test]
    fn the_binary_in_place_is_replaced_whole_and_stays_executable() {
        let d = tempfile::tempdir().unwrap();
        let exe = d.path().join("freightdock-worker");
        std::fs::write(&exe, b"old").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        replace(&exe, b"new binary").unwrap();
        assert_eq!(std::fs::read(&exe).unwrap(), b"new binary");
        assert_eq!(std::fs::metadata(&exe).unwrap().permissions().mode() & 0o777, 0o755);
        let left: Vec<_> = std::fs::read_dir(d.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(left.len(), 1, "nothing left beside it: {left:?}");
    }
}
