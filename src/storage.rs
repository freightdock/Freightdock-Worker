//! The cluster's shared storage on this node (G11, plan 8.1): made ready before a workload whose volumes are on it
//! runs, and its writes taken away when the node fences. No filesystem of our own and nothing in the read and write
//! path: an NFS export is mounted with the system's `mount` under `<data>/storage/<name>`, a CephFS by the kernel's
//! own client there (mount(2), its key in the call and nowhere else: no ceph-common needed, nothing on disk), an SMB
//! share by the kernel's client the same way (its password in the call only; no cifs-utils needed), and a
//! `path` storage is a directory the host already shares (a mount it has, a bind mount into a container), only checked. Each volume is a
//! directory `<stack>/<volume>` on it, bound into the container in place of a Docker volume of that name.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use freightdock_proto::v1;

/// Runs a program and gives back what it printed; an exit other than 0 is an error with what it wrote to stderr.
pub trait Run {
    fn run(&mut self, prog: &str, args: &[&str]) -> Result<String>;
    /// mount(2): a filesystem the kernel mounts itself, `data` its options as the kernel takes them.
    fn mount(&mut self, fstype: &str, source: &str, target: &Path, data: &str) -> Result<()>;
}

/// The real one.
pub struct System;

impl Run for System {
    fn run(&mut self, prog: &str, args: &[&str]) -> Result<String> {
        let out = std::process::Command::new(prog).args(args).stdin(std::process::Stdio::null()).output().with_context(|| format!("running {prog}"))?;
        if !out.status.success() {
            bail!("{prog} {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn mount(&mut self, fstype: &str, source: &str, target: &Path, data: &str) -> Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let c = |s: &[u8]| std::ffi::CString::new(s).context("a NUL in a mount argument");
        let (src, at, ty, opts) = (c(source.as_bytes())?, c(target.as_os_str().as_bytes())?, c(fstype.as_bytes())?, c(data.as_bytes())?);
        // SAFETY: NUL-terminated strings that live through the call.
        if unsafe { libc::mount(src.as_ptr(), at.as_ptr(), ty.as_ptr(), 0, opts.as_ptr().cast()) } != 0 {
            bail!("mount -t {fstype} {source} {}: {}", target.display(), std::io::Error::last_os_error());
        }
        Ok(())
    }
}

/// A CephFS source as the kernel takes it: the monitors' addresses (IP, maybe a port; the kernel resolves no names),
/// then `:` and an absolute path, `/` for the whole filesystem.
fn ceph_source(s: &str) -> bool {
    let Some(i) = s.find(":/") else { return false };
    let (mons, path) = (&s[..i], &s[i + 1..]);
    let mon = |m: &str| m.parse::<std::net::IpAddr>().is_ok() || m.parse::<std::net::SocketAddr>().is_ok() || m.strip_prefix('[').and_then(|r| r.strip_suffix(']')).is_some_and(|v6| v6.parse::<std::net::Ipv6Addr>().is_ok());
    !mons.is_empty() && mons.split(',').all(mon) && (path == "/" || absolute(path))
}

/// Options that carry no key: the key comes on its own, never from the storage's options.
fn keyless(options: &str) -> bool {
    plain(options, "=,._:-/") && !options.split(',').any(|o| matches!(o.split('=').next(), Some("secret" | "secretfile" | "key" | "keyfile")))
}

fn plain(s: &str, extra: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
}

/// An absolute path with nothing in it that climbs out or reads as another part of a bind.
fn absolute(p: &str) -> bool {
    p.starts_with('/') && p != "/" && !p.split('/').any(|c| c == "..") && plain(p, "/._-")
}

/// An SMB share as `//host/share` and maybe a path in it: a host name or address, a share name (a hidden one ends in
/// `$`), and plain path parts that climb nowhere.
fn smb_source(s: &str) -> Option<(&str, &str)> {
    let rest = s.strip_prefix("//")?;
    let (host, path) = rest.split_once('/')?;
    let mut parts = path.split('/');
    let share = parts.next()?;
    let ok = !host.is_empty() && plain(host, ".-[]:") && !share.is_empty() && plain(share, "._-$") && parts.all(|p| !p.is_empty() && p != ".." && p != "." && plain(p, "._-"));
    ok.then_some((host, share))
}

/// SMB options that carry no password: it comes on its own, never from the options or a file the node has.
fn smb_keyless(options: &str) -> bool {
    plain(options, "=,._:-/") && !options.split(',').any(|o| matches!(o.split('=').next(), Some("password" | "pass" | "password2" | "credentials" | "cred")))
}

/// The address the kernel's SMB client is given (`ip=`): it resolves no names itself without a helper.
fn smb_ip(host: &str) -> Result<String> {
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(ip.to_string());
    }
    use std::net::ToSocketAddrs;
    let a = (bare, 445).to_socket_addrs().with_context(|| format!("resolving {bare}"))?.next().with_context(|| format!("{bare} has no address"))?;
    Ok(a.ip().to_string())
}

/// Where the storage is on this node: `<base>/<name>` for nfs (mounted there), the directory itself for path.
pub fn root_of(base: &Path, v: &v1::StorageVolume) -> Result<PathBuf> {
    if v.storage.is_empty() || !plain(&v.storage, "-") {
        bail!("{:?} is not a storage name", v.storage);
    }
    match v.driver.as_str() {
        "nfs" => {
            let ok = v.source.split_once(":/").is_some_and(|(host, path)| !host.is_empty() && plain(host, ".-[]") && absolute(&format!("/{path}")));
            if !ok || !plain(&v.options, "=,._:-/") {
                bail!("storage {}: {:?} is not host:/export (with plain options)", v.storage, v.source);
            }
            Ok(base.join(&v.storage))
        }
        "cephfs" => {
            if !ceph_source(&v.source) || !keyless(&v.options) || !plain(&v.secret, "+/=") {
                bail!("storage {}: {:?} is not monitors:/path (with plain options and the key on its own)", v.storage, v.source);
            }
            Ok(base.join(&v.storage))
        }
        "smb" => {
            if smb_source(&v.source).is_none() || !smb_keyless(&v.options) || v.secret.chars().any(|c| c == '\0' || c.is_control()) {
                bail!("storage {}: {:?} is not //host/share (with plain options and the password on its own)", v.storage, v.source);
            }
            Ok(base.join(&v.storage))
        }
        "path" if absolute(&v.source) => Ok(PathBuf::from(&v.source)),
        "path" => bail!("storage {}: {:?} is not an absolute directory", v.storage, v.source),
        d => bail!("storage {}: the driver {d:?} is not known here", v.storage),
    }
}

/// The mount points `/proc/self/mountinfo` lists (its fifth field), with `\040` and the like read back.
pub fn mount_points(mountinfo: &str) -> Vec<PathBuf> {
    let unescape = |s: &str| {
        let mut out = String::new();
        let mut it = s.chars().peekable();
        while let Some(c) = it.next() {
            if c == '\\' {
                let oct: String = (0..3).filter_map(|_| it.next()).collect();
                out.push(u8::from_str_radix(&oct, 8).map(char::from).unwrap_or('?'));
            } else {
                out.push(c);
            }
        }
        out
    };
    mountinfo.lines().filter_map(|l| l.split(' ').nth(4)).map(|p| PathBuf::from(unescape(p))).collect()
}

/// Makes the volume ready on this node and gives back the directory to bind: the storage mounted (nfs, once) or found
/// (path), and the volume's directory made on it.
pub fn prepare(base: &Path, v: &v1::StorageVolume, mounted: &[PathBuf], run: &mut impl Run) -> Result<PathBuf> {
    let root = root_of(base, v)?;
    // `<stack>/<volume>`, or the folder the Compose file names: 1 to 8 parts, none empty or starting with a dot, so
    // never `..` and never out of the storage.
    let parts: Vec<&str> = v.dir.split('/').collect();
    let ok_dir = parts.len() <= 8 && parts.iter().all(|p| !p.is_empty() && !p.starts_with('.') && plain(p, "._-"));
    if !ok_dir {
        bail!("{:?} is not a volume's folder on the storage", v.dir);
    }
    match v.driver.as_str() {
        "nfs" if !mounted.contains(&root) => {
            std::fs::create_dir_all(&root).with_context(|| format!("making {}", root.display()))?;
            let at = root.to_string_lossy().into_owned();
            let mut args = vec!["-t", "nfs"];
            if !v.options.is_empty() {
                args.extend(["-o", v.options.as_str()]);
            }
            args.extend([v.source.as_str(), at.as_str()]);
            run.run("mount", &args).with_context(|| format!("storage {}: mounting {}", v.storage, v.source))?;
        }
        "cephfs" if !mounted.contains(&root) => {
            std::fs::create_dir_all(&root).with_context(|| format!("making {}", root.display()))?;
            // `fs=` is mount.ceph's word for it; the kernel takes `mds_namespace=`.
            let mut data = v.options.split(',').filter(|o| !o.is_empty()).map(|o| o.strip_prefix("fs=").map_or_else(|| o.to_string(), |f| format!("mds_namespace={f}"))).collect::<Vec<_>>().join(",");
            if !v.secret.is_empty() {
                data = if data.is_empty() { format!("secret={}", v.secret) } else { format!("{data},secret={}", v.secret) };
            }
            run.mount("ceph", &v.source, &root, &data).with_context(|| format!("storage {}: mounting {}", v.storage, v.source))?;
        }
        "smb" if !mounted.contains(&root) => {
            std::fs::create_dir_all(&root).with_context(|| format!("making {}", root.display()))?;
            let (host, _) = smb_source(&v.source).context("checked")?;
            let mut data: Vec<String> = v.options.split(',').filter(|o| !o.is_empty()).map(str::to_string).collect();
            if !data.iter().any(|o| o.starts_with("ip=") || o.starts_with("addr=")) {
                data.push(format!("ip={}", smb_ip(host).with_context(|| format!("storage {}", v.storage))?));
            }
            if !v.secret.is_empty() {
                // The kernel splits its options at commas: one in the password is written twice.
                data.push(format!("password={}", v.secret.replace(',', ",,")));
            }
            run.mount("cifs", &v.source, &root, &data.join(",")).with_context(|| format!("storage {}: mounting {}", v.storage, v.source))?;
        }
        "path" if !root.is_dir() => bail!("storage {}: {} is not on this node", v.storage, root.display()),
        _ => {}
    }
    let dir = root.join(&v.dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("storage {}: making {}", v.storage, dir.display()))?;
    Ok(dir)
}

/// The run's volumes with each one on storage bound from its directory: `pg:/var/lib/pg` becomes `<dir>:/var/lib/pg`.
pub fn bind(volumes: &[String], dirs: &BTreeMap<String, PathBuf>) -> Vec<String> {
    volumes
        .iter()
        .map(|v| match v.split_once(':') {
            Some((src, rest)) => dirs.get(src).map_or_else(|| v.clone(), |d| format!("{}:{rest}", d.display())),
            None => v.clone(),
        })
        .collect()
}

/// Takes the node's writes to shared storage away (plan 7.1, after its workloads stopped): every NFS, SMB and CephFS
/// mount under `base` is made read-only, then unmounted, forced and lazily. Read-only first, where the filesystem lets
/// it (it refuses while a file is open for writing): a lazy unmount only hides the tree, and a process that still holds
/// a file there could otherwise go on writing through it. A path storage is the host's to take away. Gives back how
/// many it unmounted.
pub fn unmount_all(base: &Path, mounted: &[PathBuf], run: &mut impl Run) -> usize {
    let mut n = 0;
    for m in mounted.iter().filter(|m| m.starts_with(base) && m.as_path() != base) {
        let at = m.to_string_lossy();
        if let Err(e) = run.run("mount", &["-o", "remount,bind,ro", &at]) {
            tracing::warn!("fence: making {at} read-only: {e:#}");
        }
        match run.run("umount", &["-f", "-l", &at]) {
            Ok(_) => n += 1,
            Err(e) => tracing::error!("fence: unmounting {at}: {e:#}"),
        }
    }
    n
}

/// Every volume of the run on storage made ready, and the volumes as they are to be bound.
pub fn ready(base: &Path, r: &v1::RunContainer, run: &mut impl Run) -> Result<Vec<String>> {
    let mounted = mount_points(&std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default());
    let mut dirs = BTreeMap::new();
    for v in &r.storage {
        if !r.volumes.iter().any(|x| x.split(':').next() == Some(v.volume.as_str())) {
            bail!("storage {}: the volume {:?} is none of the workload's", v.storage, v.volume);
        }
        dirs.insert(v.volume.clone(), prepare(base, v, &mounted, run)?);
    }
    Ok(bind(&r.volumes, &dirs))
}

/// How long a probe may take before the storage is said not to answer (an NFS server gone hangs it, maybe for good).
pub const HANG: std::time::Duration = std::time::Duration::from_secs(10);
/// How often each storage is probed.
pub const EVERY: std::time::Duration = std::time::Duration::from_secs(15);

fn unix_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Free and total bytes of the filesystem `p` is on.
fn space(p: &Path) -> Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(p.as_os_str().as_bytes())?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: a NUL-terminated path and a statvfs the call fills.
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        bail!("{}: {}", p.display(), std::io::Error::last_os_error());
    }
    let frsize = st.f_frsize as u64;
    Ok((st.f_blocks as u64 * frsize, st.f_bavail as u64 * frsize))
}

/// One storage probed now: an NFS export mounted at its root (else a write would land on the node's own disk), its
/// space, and a small file of this node's written, synced, read back and removed. Blocks: an export gone may hang it.
pub fn probe(name: &str, driver: &str, root: &Path, node_id: &str, mounted: &[PathBuf]) -> v1::StorageHealth {
    let started = std::time::Instant::now();
    let mut h = v1::StorageHealth { name: name.into(), driver: driver.into(), at_ms: unix_ms(), ..Default::default() };
    let r: Result<()> = (|| {
        if matches!(driver, "nfs" | "cephfs" | "smb") && !mounted.iter().any(|m| m == root) {
            bail!("{} is not mounted at {}", name, root.display());
        }
        if !root.is_dir() {
            bail!("{} is not on this node", root.display());
        }
        (h.total_bytes, h.free_bytes) = space(root)?;
        let file = root.join(format!(".freightdock-probe-{node_id}"));
        let said = format!("{node_id} {}", h.at_ms);
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&file).with_context(|| format!("writing {}", file.display()))?;
            f.write_all(said.as_bytes()).and_then(|_| f.sync_all()).with_context(|| format!("writing {}", file.display()))?;
        }
        let back = std::fs::read_to_string(&file).with_context(|| format!("reading {} back", file.display()))?;
        std::fs::remove_file(&file).with_context(|| format!("removing {}", file.display()))?;
        if back != said {
            bail!("{} read back other than written", file.display());
        }
        Ok(())
    })();
    h.probe_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
    match r {
        Ok(()) => h.ok = true,
        Err(e) => h.error = format!("{e:#}"),
    }
    h
}

/// A storage this node made ready, and how it last answered.
#[derive(Clone)]
struct Known {
    driver: String,
    root: PathBuf,
    last: Option<v1::StorageHealth>,
    /// Since when a probe has been running, if one is.
    probing: Option<std::time::Instant>,
}

/// The health of the shared storage this node made ready (G11.8): each probed on a thread of its own every `EVERY`,
/// never two at once (a probe stuck on an export gone stays stuck; it is said, not piled on), the latest answer kept
/// for the heartbeat. What it knows is kept in a file, so a restarted worker goes on probing.
pub struct Health {
    node_id: String,
    file: Option<PathBuf>,
    known: std::sync::Mutex<BTreeMap<String, Known>>,
    last_round: std::sync::Mutex<Option<std::time::Instant>>,
}

impl Health {
    pub fn new(node_id: String, file: Option<PathBuf>) -> std::sync::Arc<Self> {
        let saved: BTreeMap<String, (String, PathBuf)> = file.as_ref().and_then(|f| std::fs::read(f).ok()).and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        let known = saved.into_iter().map(|(n, (driver, root))| (n, Known { driver, root, last: None, probing: None })).collect();
        std::sync::Arc::new(Health { node_id, file, known: std::sync::Mutex::new(known), last_round: Default::default() })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Known>> {
        self.known.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Each storage made ready here and where it is on the node.
    pub fn roots(&self) -> Vec<(String, PathBuf)> {
        self.lock().iter().map(|(n, x)| (n.clone(), x.root.clone())).collect()
    }

    /// A storage made ready here, to probe from now on.
    pub fn remember(&self, name: &str, driver: &str, root: &Path) {
        let mut k = self.lock();
        if k.get(name).is_some_and(|x| x.driver == driver && x.root == root) {
            return;
        }
        k.insert(name.into(), Known { driver: driver.into(), root: root.into(), last: None, probing: None });
        self.save(&k);
    }

    /// The storage the node mounts (nfs, smb, cephfs) forgotten, as fencing unmounted it: it is not this node's to answer
    /// for until a workload needs it again (and mounts it), so the managers do not take it for failing here.
    pub fn forget_mounted(&self) {
        let mut k = self.lock();
        k.retain(|_, x| !matches!(x.driver.as_str(), "nfs" | "cephfs" | "smb"));
        self.save(&k);
    }

    fn save(&self, k: &BTreeMap<String, Known>) {
        if let Some(f) = &self.file {
            let saved: BTreeMap<&String, (&String, &PathBuf)> = k.iter().map(|(n, x)| (n, (&x.driver, &x.root))).collect();
            if let Err(e) = serde_json::to_vec(&saved).map_err(std::io::Error::other).and_then(|b| std::fs::write(f, b)) {
                tracing::warn!("keeping the storage list in {}: {e}", f.display());
            }
        }
    }

    /// Starts a probe of each storage not being probed, if the last round is `EVERY` ago.
    pub fn tick(self: &std::sync::Arc<Self>) {
        {
            let mut last = self.last_round.lock().unwrap_or_else(|p| p.into_inner());
            if last.is_some_and(|l| l.elapsed() < EVERY) {
                return;
            }
            *last = Some(std::time::Instant::now());
        }
        let mounted = mount_points(&std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default());
        let mut k = self.lock();
        for (name, x) in k.iter_mut().filter(|(_, x)| x.probing.is_none()) {
            x.probing = Some(std::time::Instant::now());
            let (me, name, driver, root, mounted) = (self.clone(), name.clone(), x.driver.clone(), x.root.clone(), mounted.clone());
            let spawned = std::thread::Builder::new().name("storage-probe".into()).spawn(move || {
                let h = probe(&name, &driver, &root, &me.node_id, &mounted);
                if let Some(x) = me.lock().get_mut(&name) {
                    x.last = Some(h);
                    x.probing = None;
                }
            });
            if let Err(e) = spawned {
                x.probing = None;
                tracing::warn!("probing storage: {e}");
            }
        }
    }

    /// What to say in the heartbeat: each storage's last answer, or that its probe has hung. None when it knows none.
    pub fn report(&self) -> Option<v1::StorageReport> {
        let k = self.lock();
        if k.is_empty() {
            return None;
        }
        let storages = k
            .iter()
            .filter_map(|(name, x)| match x.probing {
                Some(since) if since.elapsed() >= HANG => Some(v1::StorageHealth {
                    name: name.clone(),
                    driver: x.driver.clone(),
                    ok: false,
                    error: format!("the storage has not answered for {} s", since.elapsed().as_secs()),
                    at_ms: unix_ms(),
                    ..x.last.clone().unwrap_or_default()
                }),
                _ => x.last.clone(),
            })
            .collect();
        Some(v1::StorageReport { storages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Fake {
        calls: Vec<String>,
        secrets: Vec<String>,
        fail: bool,
    }

    impl Run for &mut Fake {
        fn run(&mut self, prog: &str, args: &[&str]) -> Result<String> {
            self.calls.push(format!("{prog} {}", args.join(" ")));
            if self.fail {
                bail!("no");
            }
            Ok(String::new())
        }

        fn mount(&mut self, fstype: &str, source: &str, target: &Path, data: &str) -> Result<()> {
            let (shown, secret) = match data.split_once(",secret=").map(|(o, k)| (o, k, "secret")).or_else(|| data.split_once(",password=").map(|(o, k)| (o, k, "password"))) {
                Some((o, k, word)) => (format!("{o},{word}=<key>"), Some(k.to_string())),
                None => (data.to_string(), None),
            };
            self.calls.push(format!("mount({fstype}) {source} {} {shown}", target.display()));
            self.secrets.extend(secret);
            if self.fail {
                bail!("no");
            }
            Ok(())
        }
    }

    fn nfs() -> v1::StorageVolume {
        v1::StorageVolume { volume: "pg".into(), storage: "nas".into(), driver: "nfs".into(), source: "192.0.2.50:/export/fd".into(), options: "vers=4.2,hard".into(), dir: "app/pg".into(), secret: String::new() }
    }

    fn cephfs() -> v1::StorageVolume {
        v1::StorageVolume { storage: "ceph".into(), driver: "cephfs".into(), source: "192.0.2.1,192.0.2.2:6789,[2001:db8::3]:/".into(), options: "name=fd,fs=fs1".into(), secret: "AQBkZXZpY2Uta2V5LW9mLXRlc3Q=".into(), ..nfs() }
    }

    fn smb() -> v1::StorageVolume {
        v1::StorageVolume { storage: "share".into(), driver: "smb".into(), source: "//192.0.2.60/fd/apps".into(), options: "username=fd,vers=3.0".into(), secret: "pa,ss word".into(), ..nfs() }
    }

    #[test]
    fn an_smb_share_is_mounted_by_the_kernel_with_its_password_handed_over_and_never_on_a_command_line() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        let dir = prepare(base.path(), &smb(), &[], &mut &mut f).unwrap();
        let root = base.path().join("share");
        assert_eq!(dir, root.join("app/pg"));
        assert_eq!(f.calls, vec![format!("mount(cifs) //192.0.2.60/fd/apps {} username=fd,vers=3.0,ip=192.0.2.60,password=<key>", root.display())], "the kernel resolves no names: the address goes with it");
        assert_eq!(f.secrets, vec!["pa,,ss word"], "a comma in the password doubled, as the kernel reads it");
        let mut f = Fake::default();
        prepare(base.path(), &smb(), std::slice::from_ref(&root), &mut &mut f).unwrap();
        assert!(f.calls.is_empty(), "mounted already: {:?}", f.calls);
        let guest = v1::StorageVolume { options: "guest,vers=3.0".into(), secret: String::new(), ..smb() };
        let mut f = Fake::default();
        prepare(base.path(), &guest, &[], &mut &mut f).unwrap();
        assert_eq!(f.calls, vec![format!("mount(cifs) //192.0.2.60/fd/apps {} guest,vers=3.0,ip=192.0.2.60", root.display())]);
    }

    #[test]
    fn an_smb_share_is_named_plainly_and_takes_no_password_in_its_options() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        for bad in [
            v1::StorageVolume { source: "192.0.2.60:/fd".into(), ..smb() },
            v1::StorageVolume { source: "//192.0.2.60".into(), ..smb() },
            v1::StorageVolume { source: "//192.0.2.60/fd/../etc".into(), ..smb() },
            v1::StorageVolume { source: "///fd".into(), ..smb() },
            v1::StorageVolume { options: "username=fd,password=x".into(), ..smb() },
            v1::StorageVolume { options: "username=fd,pass=x".into(), ..smb() },
            v1::StorageVolume { options: "credentials=/etc/x".into(), ..smb() },
            v1::StorageVolume { options: "username=fd vers=3".into(), ..smb() },
            v1::StorageVolume { secret: "nul\0here".into(), ..smb() },
        ] {
            assert!(prepare(base.path(), &bad, &[], &mut &mut f).is_err(), "{bad:?}");
        }
        assert!(f.calls.is_empty(), "nothing run: {:?}", f.calls);
        assert!(root_of(base.path(), &v1::StorageVolume { source: "//nas.example/fd$".into(), ..smb() }).is_ok(), "a name, and a hidden share");
    }

    #[test]
    fn an_smb_share_not_mounted_is_unhealthy() {
        let d = tempfile::tempdir().unwrap();
        assert!(!probe("share", "smb", d.path(), "n1", &[]).ok);
    }

    #[test]
    fn a_cephfs_is_mounted_by_the_kernel_with_its_key_handed_over_and_never_on_a_command_line() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        let dir = prepare(base.path(), &cephfs(), &[], &mut &mut f).unwrap();
        let root = base.path().join("ceph");
        assert_eq!(dir, root.join("app/pg"));
        assert_eq!(f.calls, vec![format!("mount(ceph) 192.0.2.1,192.0.2.2:6789,[2001:db8::3]:/ {} name=fd,mds_namespace=fs1,secret=<key>", root.display())], "fs= is mount.ceph's word; the kernel's is mds_namespace=");
        assert_eq!(f.secrets, vec!["AQBkZXZpY2Uta2V5LW9mLXRlc3Q="], "the key goes to the kernel");
        assert!(std::fs::read_dir(base.path()).unwrap().all(|e| !e.unwrap().file_name().to_string_lossy().contains("secret")), "and to no file");
        let mut f = Fake::default();
        prepare(base.path(), &cephfs(), std::slice::from_ref(&root), &mut &mut f).unwrap();
        assert!(f.calls.is_empty(), "mounted already: {:?}", f.calls);
    }

    #[test]
    fn a_cephfs_is_named_by_its_monitors_addresses_and_takes_no_key_in_its_options() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        for bad in [
            v1::StorageVolume { source: "mon.example:/".into(), ..cephfs() },
            v1::StorageVolume { source: "192.0.2.1:/../x".into(), ..cephfs() },
            v1::StorageVolume { source: "192.0.2.1".into(), ..cephfs() },
            v1::StorageVolume { source: ",192.0.2.1:/".into(), ..cephfs() },
            v1::StorageVolume { options: "name=fd,secret=abc".into(), ..cephfs() },
            v1::StorageVolume { options: "name=fd,secretfile=/etc/x".into(), ..cephfs() },
            v1::StorageVolume { options: "name=fd fs=x".into(), ..cephfs() },
            v1::StorageVolume { secret: "not,base64".into(), ..cephfs() },
        ] {
            assert!(prepare(base.path(), &bad, &[], &mut &mut f).is_err(), "{bad:?}");
        }
        assert!(f.calls.is_empty(), "nothing run: {:?}", f.calls);
        assert!(root_of(base.path(), &v1::StorageVolume { source: "192.0.2.1:/shared/fd".into(), ..cephfs() }).is_ok());
    }

    #[test]
    fn a_cephfs_not_mounted_is_unhealthy_as_an_nfs_one_is() {
        let d = tempfile::tempdir().unwrap();
        assert!(!probe("ceph", "cephfs", d.path(), "n1", &[]).ok);
        assert!(probe("ceph", "cephfs", d.path(), "n1", &[d.path().to_path_buf()]).ok);
    }

    #[test]
    fn an_nfs_export_is_mounted_once_under_the_data_dir_and_the_volume_s_directory_made_on_it() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        let dir = prepare(base.path(), &nfs(), &[], &mut &mut f).unwrap();
        let root = base.path().join("nas");
        assert_eq!(f.calls, vec![format!("mount -t nfs -o vers=4.2,hard 192.0.2.50:/export/fd {}", root.display())]);
        assert_eq!(dir, root.join("app/pg"));
        assert!(dir.is_dir());
        let mut f = Fake::default();
        prepare(base.path(), &nfs(), std::slice::from_ref(&root), &mut &mut f).unwrap();
        assert!(f.calls.is_empty(), "mounted already: {:?}", f.calls);
        let mut f = Fake { fail: true, ..Default::default() };
        let e = prepare(base.path(), &nfs(), &[], &mut &mut f).unwrap_err();
        assert!(format!("{e:#}").contains("mounting 192.0.2.50:/export/fd"), "{e:#}");
    }

    #[test]
    fn a_path_storage_is_only_found_never_mounted() {
        let shared = tempfile::tempdir().unwrap();
        let v = v1::StorageVolume { driver: "path".into(), source: shared.path().to_string_lossy().into(), options: String::new(), ..nfs() };
        let mut f = Fake::default();
        assert_eq!(prepare(Path::new("/unused"), &v, &[], &mut &mut f).unwrap(), shared.path().join("app/pg"));
        assert!(f.calls.is_empty());
        let gone = v1::StorageVolume { source: "/nowhere/at/all".into(), ..v };
        assert!(format!("{:#}", prepare(Path::new("/unused"), &gone, &[], &mut &mut f).unwrap_err()).contains("not on this node"));
    }

    #[test]
    fn a_volume_folder_named_in_the_compose_file_is_made_where_it_says() {
        let shared = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        for dir in ["ntfy", "apps/ntfy/data", "a/b/c/d/e/f/g/h"] {
            let v = v1::StorageVolume { driver: "path".into(), source: shared.path().to_string_lossy().into_owned(), options: String::new(), dir: dir.into(), ..nfs() };
            assert_eq!(prepare(Path::new("/unused"), &v, &[], &mut &mut f).unwrap(), shared.path().join(dir));
            assert!(shared.path().join(dir).is_dir());
        }
    }

    #[test]
    fn nothing_from_the_managers_climbs_out_of_the_storage_or_reads_as_another_option() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        for bad in [
            v1::StorageVolume { dir: "../etc".into(), ..nfs() },
            v1::StorageVolume { dir: "app/..".into(), ..nfs() },
            v1::StorageVolume { dir: "".into(), ..nfs() },
            v1::StorageVolume { dir: "/etc".into(), ..nfs() },
            v1::StorageVolume { dir: "a//b".into(), ..nfs() },
            v1::StorageVolume { dir: "a/b/".into(), ..nfs() },
            v1::StorageVolume { dir: "a/.git".into(), ..nfs() },
            v1::StorageVolume { dir: "a/b c".into(), ..nfs() },
            v1::StorageVolume { dir: "1/2/3/4/5/6/7/8/9".into(), ..nfs() },
            v1::StorageVolume { storage: "../x".into(), ..nfs() },
            v1::StorageVolume { source: "192.0.2.50:/export/../etc".into(), ..nfs() },
            v1::StorageVolume { options: "vers=4,x y".into(), ..nfs() },
            v1::StorageVolume { driver: "path".into(), source: "/".into(), options: String::new(), ..nfs() },
            v1::StorageVolume { driver: "ceph".into(), ..nfs() },
            v1::StorageVolume { driver: "cephfs".into(), source: "nas.example:/export".into(), ..nfs() },
        ] {
            assert!(prepare(base.path(), &bad, &[], &mut &mut f).is_err(), "{bad:?}");
        }
        assert!(f.calls.is_empty(), "nothing run: {:?}", f.calls);
    }

    #[test]
    fn a_volume_on_storage_is_bound_from_its_directory_and_the_rest_left_as_written() {
        let dirs = BTreeMap::from([("pg".to_string(), PathBuf::from("/var/lib/freightdock-worker/storage/nas/app/pg"))]);
        assert_eq!(bind(&["pg:/var/lib/pg".into(), "cache:/cache".into(), "/srv/x:/x:ro".into()], &dirs), vec!["/var/lib/freightdock-worker/storage/nas/app/pg:/var/lib/pg", "cache:/cache", "/srv/x:/x:ro"]);
    }

    #[test]
    fn fencing_unmounts_every_nfs_mount_under_the_storage_dir_and_nothing_else() {
        let info = "22 1 0:21 / / rw - ext4 /dev/sda1 rw\n90 22 0:50 / /var/lib/freightdock-worker/storage/nas rw - nfs4 192.0.2.50:/export rw\n91 22 0:51 / /var/lib/freightdock-worker/storage/my\\040nas rw - nfs4 192.0.2.50:/b rw\n92 22 0:52 / /mnt/other rw - nfs4 192.0.2.50:/c rw\n";
        let m = mount_points(info);
        assert_eq!(m[2], PathBuf::from("/var/lib/freightdock-worker/storage/my nas"));
        let mut f = Fake::default();
        assert_eq!(unmount_all(Path::new("/var/lib/freightdock-worker/storage"), &m, &mut &mut f), 2);
        assert_eq!(
            f.calls,
            vec![
                "mount -o remount,bind,ro /var/lib/freightdock-worker/storage/nas",
                "umount -f -l /var/lib/freightdock-worker/storage/nas",
                "mount -o remount,bind,ro /var/lib/freightdock-worker/storage/my nas",
                "umount -f -l /var/lib/freightdock-worker/storage/my nas",
            ],
            "read-only first: a file still open there can write no more, even through the lazily unmounted tree"
        );
        let mut f = Fake { fail: true, ..Default::default() };
        assert_eq!(unmount_all(Path::new("/var/lib/freightdock-worker/storage"), &m[1..2], &mut &mut f), 0);
        assert_eq!(f.calls.len(), 2, "a refused remount does not keep the unmount from being tried");
    }

    #[test]
    fn a_path_storage_answers_with_its_space_and_leaves_nothing_behind() {
        let d = tempfile::tempdir().unwrap();
        let h = probe("share", "path", d.path(), "n1", &[]);
        assert!(h.ok, "{}", h.error);
        assert!(h.total_bytes > 0 && h.free_bytes <= h.total_bytes);
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 0, "the probe file is removed");
        let gone = probe("share", "path", Path::new("/nowhere/at/all"), "n1", &[]);
        assert!(!gone.ok && gone.error.contains("not on this node"), "{}", gone.error);
    }

    #[test]
    fn an_nfs_storage_not_mounted_is_unhealthy_though_its_directory_is_there() {
        let d = tempfile::tempdir().unwrap();
        let h = probe("nas", "nfs", d.path(), "n1", &[]);
        assert!(!h.ok && h.error.contains("not mounted"), "a write there lands on the node's own disk: {}", h.error);
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 0, "and nothing is written");
        assert!(probe("nas", "nfs", d.path(), "n1", &[d.path().to_path_buf()]).ok);
    }

    #[test]
    fn the_storage_made_ready_is_probed_reported_and_remembered_over_a_restart() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("storage.json");
        let h = Health::new("n1".into(), Some(file.clone()));
        assert!(h.report().is_none(), "nothing known, nothing said");
        h.remember("share", "path", d.path());
        h.tick();
        let mut said = None;
        for _ in 0..200 {
            said = h.report().and_then(|r| r.storages.into_iter().next());
            if said.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let said = said.expect("probed");
        assert!(said.ok && said.name == "share", "{said:?}");
        let again = Health::new("n1".into(), Some(file));
        assert_eq!(again.lock().keys().cloned().collect::<Vec<_>>(), ["share"]);
    }

    #[test]
    fn the_storage_fencing_unmounted_is_forgotten_until_it_is_mounted_again() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("storage.json");
        let h = Health::new("n1".into(), Some(file.clone()));
        h.remember("nas", "nfs", Path::new("/x/nas"));
        h.remember("ceph", "cephfs", Path::new("/x/ceph"));
        h.remember("share", "path", d.path());
        h.forget_mounted();
        assert_eq!(h.lock().keys().cloned().collect::<Vec<_>>(), ["share"], "a path storage is the host's, still there");
        assert_eq!(Health::new("n1".into(), Some(file)).lock().keys().cloned().collect::<Vec<_>>(), ["share"], "and so after a restart");
        h.remember("ceph", "cephfs", Path::new("/x/ceph"));
        assert!(h.lock().contains_key("ceph"));
    }

    #[test]
    fn a_probe_that_hangs_is_said_and_not_piled_on() {
        let h = Health::new("n1".into(), None);
        h.remember("nas", "nfs", Path::new("/nowhere"));
        h.lock().get_mut("nas").unwrap().probing = Some(std::time::Instant::now() - HANG);
        let r = h.report().unwrap();
        assert!(!r.storages[0].ok && r.storages[0].error.contains("not answered"), "{:?}", r.storages[0]);
        h.tick();
        assert!(h.lock()["nas"].probing.unwrap().elapsed() >= HANG, "no second probe beside the stuck one");
    }
}
