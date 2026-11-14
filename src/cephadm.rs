//! Ceph made from the UI (G12.6): the steps of setting a Ceph cluster up with cephadm that run on this node, as root,
//! one `Cephadm` command each (see the contract for the actions). The worker knows each action and what it runs;
//! nothing from the managers is run as a shell line but the `ceph` arguments of `shell`, each one as given. Each action
//! is safe to run again: a bootstrap finds the cluster it made, a key is added once, a client is made or found.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use freightdock_proto::v1;
use serde_json::{Value, json};

use crate::storage::Run;

/// Where it looks and writes on the host; the tests point it elsewhere.
pub struct Paths {
    /// The worker's data directory: cephadm is kept here.
    pub data: PathBuf,
    /// /etc/ceph
    pub etc: PathBuf,
    /// /root/.ssh/authorized_keys
    pub authorized_keys: PathBuf,
}

impl Paths {
    pub fn system(data: &Path) -> Paths {
        Paths { data: data.to_path_buf(), etc: "/etc/ceph".into(), authorized_keys: "/root/.ssh/authorized_keys".into() }
    }

    fn cephadm(&self) -> PathBuf {
        self.data.join("cephadm")
    }
}

fn plain(s: &str, extra: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
}

/// cephadm with the image and Docker, under `timeout` so a hung step ends.
fn cephadm<'a>(p: &Paths, c: &'a v1::Cephadm, secs: &'a str) -> Vec<String> {
    let mut v: Vec<String> = vec![secs.into(), "python3".into(), p.cephadm().to_string_lossy().into_owned()];
    if !c.image.is_empty() {
        v.extend(["--image".into(), c.image.clone()]);
    }
    v.push("--docker".into());
    v
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// The fsid in a ceph.conf.
pub fn fsid_of(conf: &str) -> Option<String> {
    conf.lines().find_map(|l| {
        let (k, v) = l.split_once('=')?;
        (k.trim() == "fsid").then(|| v.trim().to_string())
    })
}

/// The address of the default route (`ip route get`'s src).
pub fn route_source(out: &str) -> Option<String> {
    let mut words = out.split_whitespace();
    while let Some(w) = words.next() {
        if w == "src" {
            return words.next().map(str::to_string);
        }
    }
    None
}

/// The script `keys` runs in `cephadm shell`: both clients made or found, the mount client's key made of the older
/// type when Ceph made it of the newer one (the kernel takes only the older: `AQ...`).
pub fn keys_script(fs: &str, client: &str, fence: &str) -> String {
    format!(
        "set -e
ceph auth get-or-create client.{client} mon 'allow r fsname={fs}' mds 'allow rw fsname={fs}' osd 'allow rw tag cephfs data={fs}' >/dev/null
k=$(ceph auth print-key client.{client})
case $k in Ag*)
  ceph mon set auth_allowed_ciphers aes256k,aes >/dev/null 2>&1 || true
  ceph config set mon mon_auth_allow_insecure_key true >/dev/null 2>&1 || true
  ceph auth rotate client.{client} --key_type aes >/dev/null
  k=$(ceph auth print-key client.{client});;
esac
ceph auth get-or-create client.{fence} mon 'allow r' mds 'allow * fsname={fs}' >/dev/null
echo MOUNT=$k
echo FENCE=$(ceph auth print-key client.{fence})
echo FSID=$(ceph fsid)
"
    )
}

/// The node's disks as `lsblk -J -b -o NAME,PATH,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,ROTA,RO` lists them, each with
/// whether Ceph may take it whole: a disk with no partitions, no filesystem or LVM on it, mounted nowhere, writable,
/// and of 5 GB or more (cephadm's own floor). A disk that is already Ceph's says so.
pub fn disks(lsblk: &str) -> Result<Value> {
    disks_in(lsblk, false)
}

/// Whether this node is a container (an LXC, say): its lsblk lists the host's disks, which are never its to give.
pub fn in_container() -> bool {
    std::path::Path::new("/run/systemd/container").exists() || std::fs::read("/proc/1/environ").is_ok_and(|e| e.split(|b| *b == 0).any(|v| v.starts_with(b"container=")))
}

/// As `disks`; in a container none is free.
pub fn disks_in(lsblk: &str, container: bool) -> Result<Value> {
    let v: Value = serde_json::from_str(lsblk).context("reading lsblk")?;
    let list = v["blockdevices"].as_array().cloned().unwrap_or_default();
    Ok(Value::Array(
        list.iter()
            .filter(|d| d["type"] == "disk")
            .map(|d| {
                let children = d["children"].as_array().cloned().unwrap_or_default();
                let mounted = d["mountpoints"].as_array().is_some_and(|m| m.iter().any(|x| !x.is_null()));
                let size = d["size"].as_u64().unwrap_or(0);
                let ceph = children.iter().any(|c| c["fstype"] == "ceph_bluestore");
                let why = if container {
                    Some("this node is a container: Ceph needs a VM or a machine")
                } else if ceph {
                    Some("already a Ceph disk (an OSD)")
                } else if d["ro"] == true {
                    Some("read-only")
                } else if !children.is_empty() {
                    Some("has partitions or volumes")
                } else if !d["fstype"].is_null() {
                    Some("has a filesystem")
                } else if mounted {
                    Some("mounted")
                } else if size < 5_000_000_000 {
                    Some("smaller than 5 GB")
                } else {
                    None
                };
                json!({ "path": d["path"], "size": size, "model": d["model"], "rotational": d["rota"], "free": why.is_none(), "why": why, "ceph": ceph })
            })
            .collect(),
    ))
}

/// Runs one action. Blocks (cephadm takes minutes): call it off the async threads.
pub fn run(c: &v1::Cephadm, p: &Paths, r: &mut impl Run) -> Result<Value> {
    let secs = (if c.timeout_s == 0 { 900 } else { c.timeout_s }).to_string();
    if !c.image.is_empty() && !plain(&c.image, "._-/:@") {
        bail!("{:?} is not an image", c.image);
    }
    let arg = |i: usize| c.args.get(i).map(String::as_str).unwrap_or("");
    match c.action.as_str() {
        "prepare" => {
            let url = arg(0);
            if !url.starts_with("https://") || !plain(url, ":/._-") {
                bail!("{url:?} is not where cephadm is");
            }
            let at = p.cephadm();
            if !at.exists() {
                std::fs::create_dir_all(&p.data).with_context(|| format!("making {}", p.data.display()))?;
                let tmp = at.with_extension("part");
                let t = tmp.to_string_lossy().into_owned();
                r.run("curl", &["-fsSL", "--max-time", "300", "-o", &t, url]).or_else(|_| r.run("wget", &["-q", "-T", "300", "-O", &t, url])).with_context(|| format!("downloading cephadm from {url}"))?;
                std::fs::rename(&tmp, &at).context("keeping cephadm")?;
            }
            r.run("python3", &["--version"]).context("cephadm needs python3 on the node")?;
            if r.run("sh", &["-c", "command -v lvcreate"]).is_err() {
                r.run("sh", &["-c", "DEBIAN_FRONTEND=noninteractive apt-get install -y -qq lvm2 || dnf install -y -q lvm2 || apk add -q lvm2"]).context("installing lvm2, which Ceph's disks need")?;
            }
            let address = route_source(&r.run("ip", &["-4", "route", "get", "192.0.2.1"])?).context("this node has no default route to tell its address by")?;
            let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default().trim().to_string();
            Ok(json!({ "address": address, "hostname": hostname }))
        }
        "bootstrap" => {
            let (conf, pubkey) = (p.etc.join("ceph.conf"), p.etc.join("ceph.pub"));
            if !(conf.exists() && pubkey.exists()) {
                let ip = arg(0);
                if ip.parse::<std::net::IpAddr>().is_err() {
                    bail!("{ip:?} is not the monitor's address");
                }
                let mut v = cephadm(p, c, &secs);
                v.extend(["bootstrap", "--mon-ip", ip, "--skip-dashboard", "--skip-monitoring-stack"].map(String::from));
                if arg(1) == "single-host" {
                    v.push("--single-host-defaults".into());
                }
                r.run("timeout", &strs(&v)).context("cephadm bootstrap")?;
            }
            let fsid = fsid_of(&std::fs::read_to_string(&conf).with_context(|| format!("reading {}", conf.display()))?).context("ceph.conf names no fsid")?;
            let public_key = std::fs::read_to_string(&pubkey).with_context(|| format!("reading {}", pubkey.display()))?.trim().to_string();
            Ok(json!({ "fsid": fsid, "public_key": public_key }))
        }
        "trust" => {
            let key = arg(0).trim();
            let ok = key.starts_with("ssh-") && !key.contains(['\n', '\r']) && key.split_whitespace().count() >= 2;
            if !ok {
                bail!("not an SSH public key");
            }
            let have = std::fs::read_to_string(&p.authorized_keys).unwrap_or_default();
            if !have.lines().any(|l| l.trim() == key) {
                if let Some(dir) = p.authorized_keys.parent() {
                    std::fs::create_dir_all(dir).with_context(|| format!("making {}", dir.display()))?;
                    let _ = std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o700));
                }
                let sep = if have.is_empty() || have.ends_with('\n') { "" } else { "\n" };
                std::fs::write(&p.authorized_keys, format!("{have}{sep}{key}\n")).with_context(|| format!("writing {}", p.authorized_keys.display()))?;
                let _ = std::fs::set_permissions(&p.authorized_keys, std::os::unix::fs::PermissionsExt::from_mode(0o600));
            }
            Ok(json!({ "trusted": true }))
        }
        "shell" => {
            if c.args.is_empty() || c.args.iter().any(|a| a.contains('\0')) {
                bail!("no arguments for ceph, or one with a NUL");
            }
            let mut v = cephadm(p, c, &secs);
            v.extend(["shell", "--", "ceph"].map(String::from));
            v.extend(c.args.iter().cloned());
            Ok(json!({ "stdout": r.run("timeout", &strs(&v)).with_context(|| format!("ceph {}", c.args.join(" ")))? }))
        }
        "keys" => {
            let (fs, client, fence) = (arg(0), arg(1), arg(2));
            if ![fs, client, fence].iter().all(|n| plain(n, "._-")) || client.starts_with("client.") || fence.starts_with("client.") {
                bail!("a filesystem and two clients' names (without client.) of letters, digits and . _ -");
            }
            let mut v = cephadm(p, c, &secs);
            v.extend(["shell", "--", "sh", "-c"].map(String::from));
            v.push(keys_script(fs, client, fence));
            let out = r.run("timeout", &strs(&v)).context("making the clients' keys")?;
            let said = |k: &str| out.lines().find_map(|l| l.trim().strip_prefix(k)).map(str::to_string).filter(|v| !v.is_empty());
            let (Some(mount_key), Some(fence_key), Some(fsid)) = (said("MOUNT="), said("FENCE="), said("FSID=")) else { bail!("the keys were not made") };
            Ok(json!({ "mount_key": mount_key, "fence_key": fence_key, "fsid": fsid }))
        }
        a => bail!("{a:?} is not a cephadm action this worker knows"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[derive(Default)]
    struct Fake {
        calls: Vec<String>,
        answers: VecDeque<Result<String, String>>,
    }

    impl Run for &mut Fake {
        fn run(&mut self, prog: &str, args: &[&str]) -> Result<String> {
            self.calls.push(format!("{prog} {}", args.join(" ")));
            match self.answers.pop_front() {
                Some(Err(e)) => bail!("{e}"),
                Some(Ok(o)) => Ok(o),
                None => Ok(String::new()),
            }
        }

        fn mount(&mut self, _: &str, _: &str, _: &Path, _: &str) -> Result<()> {
            bail!("no mounts here")
        }
    }

    fn paths(d: &Path) -> Paths {
        Paths { data: d.join("data"), etc: d.join("etc"), authorized_keys: d.join("ssh/authorized_keys") }
    }

    fn op(action: &str, args: &[&str]) -> v1::Cephadm {
        v1::Cephadm { action: action.into(), args: args.iter().map(|a| a.to_string()).collect(), image: "quay.io/ceph/ceph:v20".into(), timeout_s: 0 }
    }

    #[test]
    fn prepare_fetches_cephadm_once_installs_lvm2_when_missing_and_tells_the_address() {
        let d = tempfile::tempdir().unwrap();
        let p = paths(d.path());
        std::fs::create_dir_all(&p.data).unwrap();
        // The download is the fake's: the file is made here as curl would.
        std::fs::write(p.cephadm().with_extension("part"), "#!/usr/bin/env python3").unwrap();
        let mut f = Fake { answers: [Ok(String::new()), Ok("Python 3.13".into()), Err("not found".into()), Ok(String::new()), Ok("192.0.2.1 via 192.0.2.254 dev ens18 src 192.0.2.7 uid 0".into())].into(), ..Default::default() };
        let v = run(&op("prepare", &["https://download.ceph.com/rpm-20.2.0/el9/noarch/cephadm"]), &p, &mut &mut f).unwrap();
        assert_eq!(v["address"], "192.0.2.7");
        assert!(f.calls[0].starts_with("curl -fsSL") && f.calls[0].ends_with("https://download.ceph.com/rpm-20.2.0/el9/noarch/cephadm"), "{:?}", f.calls);
        assert!(f.calls[3].contains("apt-get install -y -qq lvm2"), "{:?}", f.calls);
        assert!(p.cephadm().exists());
        let mut f = Fake { answers: [Ok("Python 3.13".into()), Ok("/usr/sbin/lvcreate".into()), Ok("x src 192.0.2.7".into())].into(), ..Default::default() };
        run(&op("prepare", &["https://download.ceph.com/rpm-20.2.0/el9/noarch/cephadm"]), &p, &mut &mut f).unwrap();
        assert!(!f.calls.iter().any(|c| c.starts_with("curl") || c.contains("apt-get")), "kept, and lvm2 there: {:?}", f.calls);
        assert!(run(&op("prepare", &["http://example/cephadm"]), &p, &mut &mut Fake::default()).is_err(), "https only");
    }

    #[test]
    fn bootstrap_makes_a_cluster_once_and_answers_its_fsid_and_key() {
        let d = tempfile::tempdir().unwrap();
        let p = paths(d.path());
        let mut f = Fake::default();
        assert!(run(&op("bootstrap", &["192.0.2.7", ""]), &p, &mut &mut f).is_err(), "nothing made: no ceph.conf after");
        let data = p.data.display().to_string();
        assert_eq!(f.calls, [format!("timeout 900 python3 {data}/cephadm --image quay.io/ceph/ceph:v20 --docker bootstrap --mon-ip 192.0.2.7 --skip-dashboard --skip-monitoring-stack")]);
        std::fs::create_dir_all(&p.etc).unwrap();
        std::fs::write(p.etc.join("ceph.conf"), "# minimal\n[global]\n\tfsid = 0b2d6c1e-0000-4000-8000-000000000001\n\tmon_host = x\n").unwrap();
        std::fs::write(p.etc.join("ceph.pub"), "ssh-rsa AAAAB3Nza ceph-0b2d\n").unwrap();
        let mut f = Fake::default();
        let v = run(&op("bootstrap", &["192.0.2.7", "single-host"]), &p, &mut &mut f).unwrap();
        assert!(f.calls.is_empty(), "the cluster here is answered, not made again");
        assert_eq!((v["fsid"].as_str(), v["public_key"].as_str()), (Some("0b2d6c1e-0000-4000-8000-000000000001"), Some("ssh-rsa AAAAB3Nza ceph-0b2d")));
        std::fs::remove_file(p.etc.join("ceph.conf")).unwrap();
        let mut f = Fake::default();
        let _ = run(&op("bootstrap", &["192.0.2.7", "single-host"]), &p, &mut &mut f);
        assert!(f.calls[0].ends_with("--single-host-defaults"), "{:?}", f.calls);
        assert!(run(&op("bootstrap", &["not-an-ip", ""]), &p, &mut &mut Fake::default()).is_err());
    }

    #[test]
    fn trust_adds_the_key_once_and_takes_only_a_key() {
        let d = tempfile::tempdir().unwrap();
        let p = paths(d.path());
        std::fs::create_dir_all(p.authorized_keys.parent().unwrap()).unwrap();
        std::fs::write(&p.authorized_keys, "ssh-ed25519 AAAAold someone").unwrap();
        for _ in 0..2 {
            run(&op("trust", &["ssh-rsa AAAAB3Nza ceph-0b2d"]), &p, &mut &mut Fake::default()).unwrap();
        }
        assert_eq!(std::fs::read_to_string(&p.authorized_keys).unwrap(), "ssh-ed25519 AAAAold someone\nssh-rsa AAAAB3Nza ceph-0b2d\n");
        for bad in ["", "AAAA only", "ssh-rsa AAAA x\nssh-rsa BBBB y"] {
            assert!(run(&op("trust", &[bad]), &p, &mut &mut Fake::default()).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn shell_runs_ceph_with_the_arguments_as_given() {
        let d = tempfile::tempdir().unwrap();
        let p = paths(d.path());
        let mut f = Fake { answers: [Ok("Added host 'n2'\n".into())].into(), ..Default::default() };
        let v = run(&op("shell", &["orch", "host", "add", "n2", "192.0.2.8"]), &p, &mut &mut f).unwrap();
        assert_eq!(v["stdout"], "Added host 'n2'\n");
        assert!(f.calls[0].ends_with("--docker shell -- ceph orch host add n2 192.0.2.8"), "{:?}", f.calls);
        assert!(run(&op("shell", &[]), &p, &mut &mut Fake::default()).is_err());
        assert!(run(&op("nope", &[]), &p, &mut &mut Fake::default()).is_err());
        assert!(run(&v1::Cephadm { image: "x;y".into(), ..op("shell", &["status"]) }, &p, &mut &mut Fake::default()).is_err());
    }

    #[test]
    fn keys_makes_both_clients_and_answers_their_keys() {
        let d = tempfile::tempdir().unwrap();
        let p = paths(d.path());
        let mut f = Fake { answers: [Ok("Inferring fsid\nMOUNT=AQBtb3VudA==\nFENCE=AgBmZW5jZQ==\nFSID=0b2d\n".into())].into(), ..Default::default() };
        let v = run(&op("keys", &["fs1", "freightdock", "fd-fence"]), &p, &mut &mut f).unwrap();
        assert_eq!((v["mount_key"].as_str(), v["fence_key"].as_str(), v["fsid"].as_str()), (Some("AQBtb3VudA=="), Some("AgBmZW5jZQ=="), Some("0b2d")));
        assert!(f.calls[0].contains("shell -- sh -c set -e"), "{:?}", f.calls);
        let s = keys_script("fs1", "freightdock", "fd-fence");
        assert!(s.contains("ceph auth rotate client.freightdock --key_type aes") && s.contains("mds 'allow * fsname=fs1'"), "{s}");
        for bad in [["fs 1", "a", "b"], ["fs1", "client.admin", "b"], ["fs1", "a", "b;rm"]] {
            assert!(run(&op("keys", &bad), &p, &mut &mut Fake::default()).is_err(), "{bad:?}");
        }
        let mut f = Fake { answers: [Ok("MOUNT=\n".into())].into(), ..Default::default() };
        assert!(run(&op("keys", &["fs1", "a", "b"]), &p, &mut &mut f).is_err(), "no keys, no success");
    }

    #[test]
    fn only_a_whole_empty_disk_is_offered_to_ceph() {
        let lsblk = r#"{"blockdevices":[
            {"name":"vda","path":"/dev/vda","size":34359738368,"type":"disk","fstype":null,"mountpoints":[null],"model":null,"rota":true,"ro":false,
             "children":[{"name":"vda1","path":"/dev/vda1","size":1,"type":"part","fstype":"ext4","mountpoints":["/"]}]},
            {"name":"vdb","path":"/dev/vdb","size":17179869184,"type":"disk","fstype":null,"mountpoints":[null],"model":"QEMU","rota":true,"ro":false},
            {"name":"vdc","path":"/dev/vdc","size":17179869184,"type":"disk","fstype":"LVM2_member","mountpoints":[],"rota":true,"ro":false,
             "children":[{"name":"ceph--x","path":"/dev/mapper/ceph--x","size":1,"type":"lvm","fstype":"ceph_bluestore","mountpoints":[]}]},
            {"name":"vdd","path":"/dev/vdd","size":17179869184,"type":"disk","fstype":"xfs","mountpoints":[],"rota":false,"ro":false},
            {"name":"vde","path":"/dev/vde","size":1073741824,"type":"disk","fstype":null,"mountpoints":[],"rota":false,"ro":false},
            {"name":"sr0","path":"/dev/sr0","size":1073741824,"type":"rom","fstype":null,"mountpoints":[]}
        ]}"#;
        let d = disks(lsblk).unwrap();
        let said: Vec<(String, bool, String)> = d.as_array().unwrap().iter().map(|x| (x["path"].as_str().unwrap().to_string(), x["free"].as_bool().unwrap(), x["why"].as_str().unwrap_or("").to_string())).collect();
        assert_eq!(said, [
            ("/dev/vda".into(), false, "has partitions or volumes".into()),
            ("/dev/vdb".into(), true, String::new()),
            ("/dev/vdc".into(), false, "already a Ceph disk (an OSD)".into()),
            ("/dev/vdd".into(), false, "has a filesystem".into()),
            ("/dev/vde".into(), false, "smaller than 5 GB".into()),
        ]);
        assert_eq!(d[2]["ceph"], true);
        let boxed = disks_in(lsblk, true).unwrap();
        assert!(boxed.as_array().unwrap().iter().all(|x| x["free"] == false), "a container's lsblk lists its host's disks: none is offered");
    }

    #[test]
    fn the_fsid_and_the_route_s_source_are_read() {
        assert_eq!(fsid_of("[global]\n\tfsid = abc\n").as_deref(), Some("abc"));
        assert_eq!(route_source("192.0.2.1 via 192.0.2.254 dev ens18 src 192.0.2.7 uid 0\n    cache").as_deref(), Some("192.0.2.7"));
        assert_eq!(route_source("unreachable"), None);
    }
}
