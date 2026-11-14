//! What the managers ask of a Ceph cluster (G12.6), run here: the `ceph` command line once, in a throwaway container
//! of the Ceph image (host network), so a node needs no Ceph packages of its own and the client always matches the
//! cluster's release. The configuration and the key go into the container as files before it starts (never on the
//! node's own disk, never on a command line or in its environment) and go with it when it is removed.

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters as q;
use futures_util::StreamExt;
use serde_json::{Value, json};

use freightdock_proto::v1;

/// The label on the throwaway containers, so one left behind (a worker stopped mid-run) is known for what it is.
pub const LABEL: &str = "io.freightdock.ceph-command";

fn plain(s: &str, extra: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
}

/// The monitors as `mon_host` takes them: addresses (IP, maybe a port), comma separated.
fn mons_ok(mons: &str) -> bool {
    let one = |m: &str| m.parse::<std::net::IpAddr>().is_ok() || m.parse::<std::net::SocketAddr>().is_ok() || m.strip_prefix('[').and_then(|r| r.strip_suffix(']')).is_some_and(|v6| v6.parse::<std::net::Ipv6Addr>().is_ok());
    !mons.is_empty() && mons.split(',').all(one)
}

/// The command checked: an image, monitors, a client name and key of plain characters, arguments without a NUL.
pub fn check(c: &v1::CephCommand) -> Result<()> {
    if !plain(&c.image, "._-/:@") {
        bail!("{:?} is not an image", c.image);
    }
    if !mons_ok(&c.mons) {
        bail!("{:?} are not the monitors' addresses", c.mons);
    }
    if !plain(&c.client, "._-") || c.client.starts_with("client.") {
        bail!("{:?} is not a client's name (without client.)", c.client);
    }
    if !plain(&c.secret, "+/=") {
        bail!("the key is not a Ceph key");
    }
    if c.args.is_empty() || c.args.iter().any(|a| a.contains('\0')) {
        bail!("no arguments for ceph, or one with a NUL");
    }
    Ok(())
}

/// The files the container gets in /etc/ceph: ceph.conf with the monitors, and the client's keyring.
pub fn files(c: &v1::CephCommand) -> Vec<(String, String)> {
    vec![
        ("ceph.conf".into(), format!("[global]\n\tmon_host = {}\n", c.mons)),
        (format!("ceph.client.{}.keyring", c.client), format!("[client.{}]\n\tkey = {}\n", c.client, c.secret)),
    ]
}

/// A tar archive (ustar) of plain files, readable by root only, as Docker's archive upload takes it.
pub fn tar(files: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, body) in files {
        let mut h = [0u8; 512];
        let field = |h: &mut [u8; 512], at: usize, len: usize, v: &[u8]| h[at..at + v.len().min(len)].copy_from_slice(&v[..v.len().min(len)]);
        field(&mut h, 0, 100, name.as_bytes());
        field(&mut h, 100, 8, b"0000600\0");
        field(&mut h, 108, 8, b"0000000\0");
        field(&mut h, 116, 8, b"0000000\0");
        field(&mut h, 124, 12, format!("{:011o}\0", body.len()).as_bytes());
        field(&mut h, 136, 12, b"00000000000\0");
        h[156] = b'0';
        field(&mut h, 257, 8, b"ustar\x0000");
        h[148..156].copy_from_slice(b"        ");
        let sum: u32 = h.iter().map(|b| *b as u32).sum();
        field(&mut h, 148, 8, format!("{sum:06o}\0 ").as_bytes());
        out.extend_from_slice(&h);
        out.extend_from_slice(body.as_bytes());
        out.resize(out.len().div_ceil(512) * 512, 0);
    }
    out.resize(out.len() + 1024, 0);
    out
}

/// The command line in the container.
pub fn argv(c: &v1::CephCommand) -> Vec<String> {
    let t = if c.timeout_s == 0 { 60 } else { c.timeout_s };
    let mut v = vec!["ceph".into(), "--connect-timeout".into(), t.to_string(), "-n".into(), format!("client.{}", c.client), "--format".into(), "json".into()];
    v.extend(c.args.iter().cloned());
    v
}

/// Runs it: pulls the image if it is not here, makes the container, gives it its files, starts it, waits (at most
/// the timeout and a little), reads what it printed and removes it. Answers its stdout; an exit other than 0 is an
/// error with what it wrote to stderr.
pub async fn run(d: &Docker, c: &v1::CephCommand) -> Result<Value> {
    check(c)?;
    if d.inspect_image(&c.image).await.is_err() {
        crate::docker::pull(d, &c.image, None).await?;
    }
    let body = ContainerCreateBody {
        image: Some(c.image.clone()),
        entrypoint: Some(vec![]),
        cmd: Some(argv(c)),
        labels: Some([(LABEL.to_string(), "true".to_string())].into()),
        host_config: Some(HostConfig { network_mode: Some("host".into()), ..Default::default() }),
        ..Default::default()
    };
    let made = d.create_container(None::<q::CreateContainerOptions>, body).await.context("making the ceph container")?;
    let id = made.id;
    let r = async {
        let archive = bytes::Bytes::from(tar(&files(c)));
        d.upload_to_container(&id, Some(q::UploadToContainerOptions { path: "/etc/ceph".into(), ..Default::default() }), bollard::body_full(archive)).await.context("giving it its configuration")?;
        d.start_container(&id, None::<q::StartContainerOptions>).await.context("starting the ceph container")?;
        let limit = Duration::from_secs(u64::from(if c.timeout_s == 0 { 60 } else { c.timeout_s }) + 30);
        let mut wait = d.wait_container(&id, None::<q::WaitContainerOptions>);
        let code = match tokio::time::timeout(limit, wait.next()).await {
            Err(_) => bail!("ceph did not finish within {} s", limit.as_secs()),
            Ok(Some(Ok(w))) => w.status_code,
            // A non-zero exit comes as an error carrying the code.
            Ok(Some(Err(bollard::errors::Error::DockerContainerWaitError { code, .. }))) => code,
            Ok(Some(Err(e))) => return Err(anyhow!(e).context("waiting for ceph")),
            Ok(None) => bail!("ceph's container went away"),
        };
        let (mut out, mut err) = (String::new(), String::new());
        let mut logs = d.logs(&id, Some(q::LogsOptions { stdout: true, stderr: true, ..Default::default() }));
        while let Some(l) = logs.next().await {
            match l.context("reading what ceph printed")? {
                bollard::container::LogOutput::StdOut { message } => out.push_str(&String::from_utf8_lossy(&message)),
                bollard::container::LogOutput::StdErr { message } => err.push_str(&String::from_utf8_lossy(&message)),
                _ => {}
            }
        }
        if code != 0 {
            let said = err.lines().rfind(|l| !l.trim().is_empty()).unwrap_or("").trim().to_string();
            bail!("ceph {} exited {code}: {said}", c.args.join(" "));
        }
        Ok(json!({ "stdout": out }))
    }
    .await;
    if let Err(e) = d.remove_container(&id, Some(q::RemoveContainerOptions { force: true, v: true, ..Default::default() })).await {
        tracing::warn!(container = %id, "removing the ceph container: {e}");
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd() -> v1::CephCommand {
        v1::CephCommand { image: "quay.io/ceph/ceph:v20".into(), mons: "192.0.2.1,192.0.2.2:6789,[2001:db8::3]:3300".into(), client: "fd-fence".into(), secret: "AQBkZXZpY2Uta2V5LW9mLXRlc3Q=".into(), args: vec!["tell".into(), "mds.fs1:0".into(), "client".into(), "ls".into()], timeout_s: 0 }
    }

    #[test]
    fn a_command_runs_as_its_client_with_its_arguments_as_given() {
        check(&cmd()).unwrap();
        assert_eq!(argv(&cmd()), ["ceph", "--connect-timeout", "60", "-n", "client.fd-fence", "--format", "json", "tell", "mds.fs1:0", "client", "ls"]);
        let f = files(&cmd());
        assert_eq!(f[0], ("ceph.conf".to_string(), "[global]\n\tmon_host = 192.0.2.1,192.0.2.2:6789,[2001:db8::3]:3300\n".to_string()));
        assert_eq!(f[1].0, "ceph.client.fd-fence.keyring");
        assert!(f[1].1.contains("key = AQBkZXZpY2Uta2V5LW9mLXRlc3Q="));
        assert!(!argv(&cmd()).iter().any(|a| a.contains("AQBk")), "the key is in a file, never on the command line");
    }

    #[test]
    fn nothing_odd_is_run() {
        for bad in [
            v1::CephCommand { image: "x; rm".into(), ..cmd() },
            v1::CephCommand { mons: "mon.example".into(), ..cmd() },
            v1::CephCommand { mons: "".into(), ..cmd() },
            v1::CephCommand { client: "client.admin".into(), ..cmd() },
            v1::CephCommand { client: "a b".into(), ..cmd() },
            v1::CephCommand { secret: "x\ny".into(), ..cmd() },
            v1::CephCommand { args: vec![], ..cmd() },
            v1::CephCommand { args: vec!["a\0b".into()], ..cmd() },
        ] {
            assert!(check(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_files_go_as_a_tar_docker_can_read() {
        let t = tar(&files(&cmd()));
        assert_eq!(t.len() % 512, 0);
        assert_eq!(&t[0..9], b"ceph.conf");
        assert_eq!(&t[257..263], b"ustar\0");
        assert_eq!(&t[100..107], b"0000600", "root's only");
        let sum: u32 = t[..512].iter().enumerate().map(|(i, b)| if (148..156).contains(&i) { b' ' as u32 } else { *b as u32 }).sum();
        let said = u32::from_str_radix(std::str::from_utf8(&t[148..154]).unwrap(), 8).unwrap();
        assert_eq!(sum, said, "the header's checksum");
        let size = usize::from_str_radix(std::str::from_utf8(&t[124..135]).unwrap(), 8).unwrap();
        assert_eq!(&t[512..512 + size], files(&cmd())[0].1.as_bytes());
        if std::process::Command::new("tar").arg("--version").output().is_ok() {
            let d = tempfile::tempdir().unwrap();
            std::fs::write(d.path().join("a.tar"), &t).unwrap();
            let out = std::process::Command::new("tar").args(["-tf", "a.tar"]).current_dir(d.path()).output().unwrap();
            assert_eq!(String::from_utf8_lossy(&out.stdout), "ceph.conf\nceph.client.fd-fence.keyring\n", "{}", String::from_utf8_lossy(&out.stderr));
        }
    }
}
