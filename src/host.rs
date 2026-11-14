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
    Capabilities {
        docker: docker_version.is_some(),
        docker_version: docker_version.unwrap_or_default(),
        os: sysinfo::System::long_os_version().unwrap_or_default(),
        arch: std::env::consts::ARCH.to_string(),
        kernel: sysinfo::System::kernel_version().unwrap_or_default(),
        docker_requests: true,
        shells: true,
        host_shell,
    }
}
