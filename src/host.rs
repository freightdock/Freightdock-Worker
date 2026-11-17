//! What the worker tells the managers of its host: names, the boot id, resources and capabilities.

use std::sync::Mutex;

use freightdock_proto::v1::{Capabilities, NodeResources};

pub fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname").ok().map(|h| h.trim().to_string()).filter(|h| !h.is_empty()).or_else(sysinfo::System::host_name).unwrap_or_default()
}

/// The kernel's boot id: a new one means the host rebooted.
pub fn boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map(|s| s.trim().to_string()).unwrap_or_default()
}

pub struct Probe {
    sys: Mutex<sysinfo::System>,
}

impl Default for Probe {
    fn default() -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_cpu_usage();
        sys.refresh_memory();
        Probe { sys: Mutex::new(sys) }
    }
}

impl Probe {
    /// Now's numbers. CPU use is since the previous call, so the first is 0.
    pub fn resources(&self, containers_running: u32) -> NodeResources {
        let mut s = self.sys.lock().expect("probe lock");
        s.refresh_cpu_usage();
        s.refresh_memory();
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let root = disks.list().iter().filter(|d| std::path::Path::new("/").starts_with(d.mount_point())).max_by_key(|d| d.mount_point().as_os_str().len());
        NodeResources {
            cpu_count: s.cpus().len() as u32,
            cpu_percent: s.global_cpu_usage(),
            memory_total_bytes: s.total_memory(),
            memory_used_bytes: s.used_memory(),
            disk_total_bytes: root.map(|d| d.total_space()).unwrap_or(0),
            disk_used_bytes: root.map(|d| d.total_space() - d.available_space()).unwrap_or(0),
            containers_running,
        }
    }
}

pub fn capabilities(docker_version: Option<String>, host_shell: bool) -> Capabilities {
    let (mounts, mounts_why) = node_mounts();
    Capabilities {
        docker: docker_version.is_some(),
        docker_version: docker_version.unwrap_or_default(),
        os: sysinfo::System::long_os_version().unwrap_or_default(),
        arch: std::env::consts::ARCH.to_string(),
        kernel: sysinfo::System::kernel_version().unwrap_or_default(),
        docker_requests: true,
        shells: true,
        host_shell,
        upgrade: true,
        mounts,
        mounts_known: true,
        mounts_why,
    }
}

/// What the node's kernel and namespace let it mount (`Capabilities.mounts`).
pub struct MountFacts<'a> {
    /// `/proc/self/uid_map`: the initial namespace maps every id to itself.
    pub uid_map: &'a str,
    pub euid: u32,
    /// `/proc/filesystems`: what the kernel has loaded.
    pub filesystems: &'a str,
    /// Whether the kernel has the module under `kernel/fs/<dir>`, loaded or not (built in, or a file to load).
    pub module: &'a dyn Fn(&str) -> bool,
    /// `mount.nfs` is on the node (nfs-common), which `mount -t nfs` needs.
    pub mount_nfs: bool,
}

/// The storage drivers this node can mount, and why none when it can mount none.
pub fn mounts(f: &MountFacts) -> (Vec<String>, String) {
    let initial = f.uid_map.split_whitespace().collect::<Vec<_>>() == ["0", "0", "4294967295"];
    if !initial {
        return (vec![], "it runs in a user namespace (an unprivileged container), where the kernel lets nothing be mounted".into());
    }
    if f.euid != 0 {
        return (vec![], "its worker does not run as root".into());
    }
    let has = |fs: &[&str], dirs: &[&str]| fs.iter().any(|n| f.filesystems.lines().any(|l| l.split_whitespace().last() == Some(n))) || dirs.iter().any(|d| (f.module)(d));
    let mut out = vec![];
    if f.mount_nfs && has(&["nfs", "nfs4"], &["nfs"]) {
        out.push("nfs".to_string());
    }
    if has(&["ceph"], &["ceph"]) {
        out.push("cephfs".to_string());
    }
    if has(&["cifs", "smb3"], &["smb/client", "cifs"]) {
        out.push("smb".to_string());
    }
    let why = if out.is_empty() { "its kernel has no NFS, CephFS or SMB client (and mount.nfs is not installed)".to_string() } else { String::new() };
    (out, why)
}

/// `mounts` for this node, read from its kernel.
pub fn node_mounts() -> (Vec<String>, String) {
    let read = |p: &str| std::fs::read_to_string(p).unwrap_or_default();
    let release = read("/proc/sys/kernel/osrelease").trim().to_string();
    let base = std::path::PathBuf::from("/lib/modules").join(&release);
    let builtin = read(&base.join("modules.builtin").to_string_lossy());
    let module = |dir: &str| base.join("kernel/fs").join(dir).is_dir() || builtin.contains(&format!("kernel/fs/{dir}/"));
    let mount_nfs = ["/sbin/mount.nfs", "/usr/sbin/mount.nfs"].iter().any(|p| std::path::Path::new(p).exists());
    // SAFETY: geteuid has no preconditions.
    let euid = unsafe { libc::geteuid() };
    mounts(&MountFacts { uid_map: &read("/proc/self/uid_map"), euid, filesystems: &read("/proc/filesystems"), module: &module, mount_nfs })
}

#[cfg(test)]
mod tests {
    use super::*;

    const INITIAL: &str = "         0          0 4294967295\n";

    #[test]
    fn an_unprivileged_container_mounts_nothing_and_says_why() {
        let f = MountFacts { uid_map: "0 100000 65536\n", euid: 0, filesystems: "nodev\tceph\n\tnfs4\n", module: &|_| true, mount_nfs: true };
        let (m, why) = mounts(&f);
        assert!(m.is_empty());
        assert!(why.contains("unprivileged container"), "{why}");
    }

    #[test]
    fn a_host_mounts_what_its_kernel_has_loaded_or_can_load() {
        let f = MountFacts { uid_map: INITIAL, euid: 0, filesystems: "nodev\tsysfs\n\text4\nnodev\tnfs4\n", module: &|d| d == "smb/client", mount_nfs: true };
        assert_eq!(mounts(&f), (vec!["nfs".to_string(), "smb".to_string()], String::new()), "ceph neither loaded nor a module");
        let no_helper = MountFacts { mount_nfs: false, ..f };
        assert_eq!(mounts(&no_helper).0, ["smb"], "mount -t nfs wants mount.nfs");
        let all = MountFacts { uid_map: INITIAL, euid: 0, filesystems: "", module: &|_| true, mount_nfs: true };
        assert_eq!(mounts(&all).0, ["nfs", "cephfs", "smb"]);
    }

    #[test]
    fn nothing_without_root_or_a_client() {
        let f = MountFacts { uid_map: INITIAL, euid: 1000, filesystems: "nodev\tceph\n", module: &|_| true, mount_nfs: true };
        assert!(mounts(&f).1.contains("root"));
        let bare = MountFacts { uid_map: INITIAL, euid: 0, filesystems: "\text4\n", module: &|_| false, mount_nfs: false };
        let (m, why) = mounts(&bare);
        assert!(m.is_empty() && !why.is_empty());
    }
}
