//! What the node's disk and its shared storage folders hold, by stack (the UI's Storage page), measured every
//! `EVERY` in the background and said once in the next heartbeat of each stream. Docker says what its containers,
//! volumes, images and build cache take (`docker system df`); a container's stack is its label. A shared storage
//! folder in use is a bind mount of a container from inside a storage's mount point; its size is a walk of its files,
//! as `du` counts them (allocated blocks, symlinks not followed, one filesystem), stopped at `MAX_FILES` or `MAX_WALK`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bollard::models::{MountPointTypeEnum, SystemDataUsageResponse};
use freightdock_proto::v1;

/// How often the node is measured.
pub const EVERY: Duration = Duration::from_secs(300);
/// How long Docker may take to say (it walks its volumes).
pub const DOCKER_WAIT: Duration = Duration::from_secs(120);
/// A folder's walk stops after this many entries, or this long: the size is then "at least".
pub const MAX_FILES: u64 = 2_000_000;
pub const MAX_WALK: Duration = Duration::from_secs(120);

const LABEL_STACK: &str = crate::net::LABEL_STACK;

/// Docker's answer added up by stack, and the shared storage folders its containers bind: (storage, dir, path).
#[derive(Debug, Default, PartialEq)]
pub struct Tally {
    pub stacks: Vec<v1::StackDisk>,
    pub images_bytes: u64,
    pub loose_volumes_bytes: u64,
    pub build_cache_bytes: u64,
    pub folders: Vec<(String, String, PathBuf)>,
}

fn size(n: Option<i64>) -> u64 {
    n.unwrap_or(0).max(0) as u64
}

/// `df` added up: each stack's containers' writable layers and the local volumes they mount (a volume of several
/// stacks counted for each), the rest by kind; and the folders of `roots` (a storage's name and where it is) bound.
pub fn tally(df: &SystemDataUsageResponse, roots: &[(String, PathBuf)]) -> Tally {
    let mut by: BTreeMap<String, v1::StackDisk> = BTreeMap::new();
    let mut volume_stacks: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut folders: BTreeSet<(String, String, PathBuf)> = BTreeSet::new();
    for c in df.containers.iter().flatten() {
        let Some(stack) = c.labels.as_ref().and_then(|l| l.get(LABEL_STACK)).filter(|s| !s.is_empty()) else { continue };
        let d = by.entry(stack.clone()).or_insert_with(|| v1::StackDisk { stack: stack.clone(), ..Default::default() });
        d.containers_bytes += size(c.size_rw);
        for m in c.mounts.iter().flatten() {
            match m.typ {
                Some(MountPointTypeEnum::VOLUME) => {
                    if let Some(n) = &m.name {
                        volume_stacks.entry(n.clone()).or_default().insert(stack.clone());
                    }
                }
                Some(MountPointTypeEnum::BIND) => {
                    let Some(src) = m.source.as_deref().map(Path::new) else { continue };
                    for (name, root) in roots {
                        if let Ok(rel) = src.strip_prefix(root) {
                            let dir = rel.to_string_lossy().into_owned();
                            if !dir.is_empty() {
                                folders.insert((name.clone(), dir, src.to_path_buf()));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let mut loose = 0;
    for v in df.volumes.iter().flatten() {
        let bytes = size(v.usage_data.as_ref().map(|u| u.size));
        match volume_stacks.get(&v.name) {
            Some(stacks) => {
                for s in stacks {
                    if let Some(d) = by.get_mut(s) {
                        d.volumes_bytes += bytes;
                    }
                }
            }
            None => loose += bytes,
        }
    }
    let cache = df.build_cache.iter().flatten().filter(|b| b.shared != Some(true)).map(|b| size(b.size)).sum();
    Tally { stacks: by.into_values().collect(), images_bytes: size(df.layers_size), loose_volumes_bytes: loose, build_cache_bytes: cache, folders: folders.into_iter().collect() }
}

/// What the files under `dir` take on its filesystem (allocated blocks), not following symlinks nor leaving the
/// filesystem; and whether the walk stopped early, at `max_files` entries or `max_time`.
pub fn folder_size(dir: &Path, max_files: u64, max_time: Duration) -> std::io::Result<(u64, bool)> {
    use std::os::unix::fs::MetadataExt;
    let top = std::fs::symlink_metadata(dir)?;
    let dev = top.dev();
    let started = Instant::now();
    let (mut bytes, mut seen) = (top.blocks() * 512, 1u64);
    let mut todo = vec![dir.to_path_buf()];
    while let Some(d) = todo.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else { continue };
        for e in entries.flatten() {
            seen += 1;
            if seen > max_files || started.elapsed() > max_time {
                return Ok((bytes, true));
            }
            let Ok(m) = e.path().symlink_metadata() else { continue };
            if m.dev() != dev {
                continue;
            }
            bytes += m.blocks() * 512;
            if m.is_dir() {
                todo.push(e.path());
            }
        }
    }
    Ok((bytes, false))
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// The latest measure, and the round's pacing: one at a time, every `EVERY`.
#[derive(Default)]
pub struct Usage {
    last: Mutex<Option<v1::UsageReport>>,
    round: Mutex<Option<Instant>>,
}

impl Usage {
    /// Starts a measure when the last began `EVERY` ago and none runs.
    pub fn tick(self: &std::sync::Arc<Self>, docker: &bollard::Docker, roots: Vec<(String, PathBuf)>) {
        {
            let mut r = self.round.lock().unwrap_or_else(|p| p.into_inner());
            if r.is_some_and(|at| at.elapsed() < EVERY) {
                return;
            }
            *r = Some(Instant::now());
        }
        let (me, docker) = (self.clone(), docker.clone());
        tokio::spawn(async move {
            let started = Instant::now();
            let df = match tokio::time::timeout(DOCKER_WAIT, docker.df(None::<bollard::query_parameters::DataUsageOptions>)).await {
                Ok(Ok(df)) => df,
                Ok(Err(e)) => return tracing::warn!("measuring the disk: Docker: {e}"),
                Err(_) => return tracing::warn!("measuring the disk: Docker did not answer in {} s", DOCKER_WAIT.as_secs()),
            };
            let t = tally(&df, &roots);
            let folders = tokio::task::spawn_blocking(move || {
                t.folders
                    .iter()
                    .map(|(storage, dir, path)| match folder_size(path, MAX_FILES, MAX_WALK) {
                        Ok((bytes, partial)) => v1::FolderSize { storage: storage.clone(), dir: dir.clone(), bytes, partial, error: String::new() },
                        Err(e) => v1::FolderSize { storage: storage.clone(), dir: dir.clone(), error: e.to_string(), ..Default::default() },
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default();
            let report = v1::UsageReport { stacks: t.stacks, images_bytes: t.images_bytes, loose_volumes_bytes: t.loose_volumes_bytes, build_cache_bytes: t.build_cache_bytes, folders, at_ms: unix_ms(), took_ms: started.elapsed().as_millis() as u32 };
            *me.last.lock().unwrap_or_else(|p| p.into_inner()) = Some(report);
        });
    }

    /// The latest measure if it is newer than `sent` (the measure time said on this stream), and that time.
    pub fn newer_than(&self, sent: i64) -> Option<v1::UsageReport> {
        self.last.lock().unwrap_or_else(|p| p.into_inner()).as_ref().filter(|r| r.at_ms > sent).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::{BuildCache, ContainerSummary, MountPoint, Volume, VolumeUsageData};
    use std::collections::HashMap;

    fn container(stack: Option<&str>, rw: i64, mounts: Vec<MountPoint>) -> ContainerSummary {
        ContainerSummary { labels: stack.map(|s| HashMap::from([(LABEL_STACK.to_string(), s.to_string())])), size_rw: Some(rw), mounts: Some(mounts), ..Default::default() }
    }
    fn vol(name: &str) -> MountPoint {
        MountPoint { typ: Some(MountPointTypeEnum::VOLUME), name: Some(name.into()), ..Default::default() }
    }
    fn bind(src: &str) -> MountPoint {
        MountPoint { typ: Some(MountPointTypeEnum::BIND), source: Some(src.into()), ..Default::default() }
    }
    fn volume(name: &str, size: i64) -> Volume {
        Volume { name: name.into(), usage_data: Some(VolumeUsageData { size, ref_count: 1 }), ..Default::default() }
    }

    #[test]
    fn the_disk_is_added_up_by_the_stack_its_containers_carry() {
        let df = SystemDataUsageResponse {
            layers_size: Some(5_000),
            containers: Some(vec![
                container(Some("web"), 100, vec![vol("site")]),
                container(Some("web"), 50, vec![vol("site")]),
                container(Some("db"), 10, vec![vol("pg"), vol("shared")]),
                container(Some("ntfy"), 1, vec![vol("shared")]),
                container(None, 999, vec![vol("ceph-own")]),
            ]),
            volumes: Some(vec![volume("site", 1_000), volume("pg", 2_000), volume("shared", 300), volume("ceph-own", 7), volume("left", 40), volume("unknown", -1)]),
            build_cache: Some(vec![BuildCache { size: Some(60), shared: Some(false), ..Default::default() }, BuildCache { size: Some(9), shared: Some(true), ..Default::default() }]),
            ..Default::default()
        };
        let t = tally(&df, &[]);
        let got: Vec<(&str, u64, u64)> = t.stacks.iter().map(|d| (d.stack.as_str(), d.containers_bytes, d.volumes_bytes)).collect();
        assert_eq!(got, [("db", 10, 2_300), ("ntfy", 1, 300), ("web", 150, 1_000)], "a volume counted once per stack, for each stack mounting it");
        assert_eq!((t.images_bytes, t.loose_volumes_bytes, t.build_cache_bytes), (5_000, 47, 60), "a volume only an unlabelled container mounts is loose; a shared cache record counted once elsewhere");
    }

    #[test]
    fn a_bind_from_inside_a_storage_is_its_folder() {
        let df = SystemDataUsageResponse {
            containers: Some(vec![container(Some("ntfy"), 0, vec![bind("/var/lib/freightdock-worker/storage/ceph/ntfy/data"), bind("/srv/certs"), bind("/var/lib/freightdock-worker/storage/ceph")])]),
            ..Default::default()
        };
        let roots = [("ceph".to_string(), PathBuf::from("/var/lib/freightdock-worker/storage/ceph")), ("share".to_string(), PathBuf::from("/mnt/share"))];
        assert_eq!(tally(&df, &roots).folders, [("ceph".to_string(), "ntfy/data".to_string(), PathBuf::from("/var/lib/freightdock-worker/storage/ceph/ntfy/data"))], "the storage's root itself is no volume's folder");
    }

    #[test]
    fn a_folder_is_what_its_files_take_and_a_walk_can_stop_early() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("a/b")).unwrap();
        std::fs::write(d.path().join("a/b/big"), vec![1u8; 100_000]).unwrap();
        std::fs::write(d.path().join("small"), b"x").unwrap();
        std::os::unix::fs::symlink("/usr", d.path().join("link")).unwrap();
        let (bytes, partial) = folder_size(d.path(), MAX_FILES, MAX_WALK).unwrap();
        assert!(!partial);
        assert!((100_000..200_000).contains(&bytes), "{bytes}: the file's blocks and the folders', not /usr behind the link");
        let (_, stopped) = folder_size(d.path(), 2, MAX_WALK).unwrap();
        assert!(stopped);
        assert!(folder_size(&d.path().join("gone"), MAX_FILES, MAX_WALK).is_err());
    }

    #[test]
    fn a_measure_is_said_once_on_a_stream() {
        let u = Usage::default();
        assert!(u.newer_than(0).is_none());
        *u.last.lock().unwrap() = Some(v1::UsageReport { at_ms: 10, ..Default::default() });
        assert_eq!(u.newer_than(0).map(|r| r.at_ms), Some(10));
        assert!(u.newer_than(10).is_none(), "said already");
    }
}
