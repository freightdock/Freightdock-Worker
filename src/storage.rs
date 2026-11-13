//! The cluster's shared storage on this node (G11, plan 8.1): made ready before a workload whose volumes are on it
//! runs, and its writes taken away when the node fences. No filesystem of our own and nothing in the read and write
//! path: an NFS export is mounted with the system's `mount` under `<data>/storage/<name>`, and a `path` storage is a
//! directory the host already shares (a mount it has, a bind mount into a container), only checked. Each volume is a
//! directory `<stack>/<volume>` on it, bound into the container in place of a Docker volume of that name.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use freightdock_proto::v1;

/// Runs a program and gives back what it printed; an exit other than 0 is an error with what it wrote to stderr.
pub trait Run {
    fn run(&mut self, prog: &str, args: &[&str]) -> Result<String>;
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
}

fn plain(s: &str, extra: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
}

/// An absolute path with nothing in it that climbs out or reads as another part of a bind.
fn absolute(p: &str) -> bool {
    p.starts_with('/') && p != "/" && !p.split('/').any(|c| c == "..") && plain(p, "/._-")
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
    let ok_dir = v.dir.split_once('/').is_some_and(|(stack, vol)| !stack.is_empty() && !vol.is_empty() && plain(stack, "-") && plain(vol, "._-") && !vol.starts_with('.'));
    if !ok_dir {
        bail!("{:?} is not a volume's directory (stack/volume)", v.dir);
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

/// Takes the node's writes to shared storage away (plan 7.1, after its workloads stopped): every NFS mount under
/// `base` is unmounted, forced and lazily, so a process still holding a file there loses it. A path storage is the
/// host's to take away. Gives back how many it unmounted.
pub fn unmount_all(base: &Path, mounted: &[PathBuf], run: &mut impl Run) -> usize {
    let mut n = 0;
    for m in mounted.iter().filter(|m| m.starts_with(base) && m.as_path() != base) {
        let at = m.to_string_lossy();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Fake {
        calls: Vec<String>,
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
    }

    fn nfs() -> v1::StorageVolume {
        v1::StorageVolume { volume: "pg".into(), storage: "nas".into(), driver: "nfs".into(), source: "192.0.2.50:/export/fd".into(), options: "vers=4.2,hard".into(), dir: "app/pg".into() }
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
    fn nothing_from_the_managers_climbs_out_of_the_storage_or_reads_as_another_option() {
        let base = tempfile::tempdir().unwrap();
        let mut f = Fake::default();
        for bad in [
            v1::StorageVolume { dir: "../etc".into(), ..nfs() },
            v1::StorageVolume { dir: "app/..".into(), ..nfs() },
            v1::StorageVolume { dir: "app".into(), ..nfs() },
            v1::StorageVolume { storage: "../x".into(), ..nfs() },
            v1::StorageVolume { source: "192.0.2.50:/export/../etc".into(), ..nfs() },
            v1::StorageVolume { options: "vers=4,x y".into(), ..nfs() },
            v1::StorageVolume { driver: "path".into(), source: "/".into(), options: String::new(), ..nfs() },
            v1::StorageVolume { driver: "ceph".into(), ..nfs() },
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
        assert_eq!(f.calls, vec!["umount -f -l /var/lib/freightdock-worker/storage/nas", "umount -f -l /var/lib/freightdock-worker/storage/my nas"]);
    }
}
