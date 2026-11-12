//! Self-fencing (G6, plan 7.1). A worker that hears from no manager for `stop` stops every managed container and is
//! fenced; `storage` later it would stop shared-storage writes (nothing to do until G11); `reboot` after that it
//! reboots if a managed container still runs (or always, or never, as configured). A fenced worker runs no managed
//! workload until the managers authorize it (`Authorize`), and it stays fenced across a restart and a reboot: the
//! state is a file in its data folder. A reboot the worker did not ask for fences it too, since it cannot know what
//! ran while it was down.
//!
//! The countdown is measured from the last word of any manager, kept on disk (`contact`), so a worker restarted by
//! its watchdog in the middle of an outage carries on counting where it was.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RebootPolicy {
    /// Only when a managed container could not be stopped.
    IfNeeded,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timers {
    pub stop: Duration,
    pub storage: Duration,
    pub reboot: Duration,
}

impl Default for Timers {
    fn default() -> Self {
        Timers { stop: Duration::from_secs(30), storage: Duration::from_secs(15), reboot: Duration::from_secs(15) }
    }
}

impl Timers {
    /// The cluster's timers from a Welcome; a zero keeps what is known.
    pub fn with_welcome(self, stop_s: u32, storage_s: u32, reboot_s: u32) -> Timers {
        let or = |s: u32, d: Duration| if s == 0 { d } else { Duration::from_secs(s as u64) };
        Timers { stop: or(stop_s, self.stop), storage: or(storage_s, self.storage), reboot: or(reboot_s, self.reboot) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stage {
    Normal,
    /// Stop managed workloads; fenced.
    Stop,
    /// Stop shared-storage writes.
    Storage,
    /// Reboot if needed.
    Reboot,
}

/// How far along the countdown is after `silent` without a manager.
pub fn stage(silent: Duration, t: &Timers) -> Stage {
    if silent >= t.stop + t.storage + t.reboot {
        Stage::Reboot
    } else if silent >= t.stop + t.storage {
        Stage::Storage
    } else if silent >= t.stop {
        Stage::Stop
    } else {
        Stage::Normal
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fenced {
    pub reason: String,
    pub at_ms: i64,
    /// The boot it fenced in: a reboot for the fence is asked at most once per boot.
    pub boot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Kept {
    fenced: Option<Fenced>,
    /// The boot the worker last ran in.
    boot_id: String,
    /// When a manager was last heard from, unix ms.
    contact_ms: i64,
    timers: Timers,
}

/// The fence: the countdown's start and the fenced state, kept in `<data>/fence.json`.
pub struct Fence {
    path: Option<PathBuf>,
    kept: Mutex<Kept>,
    /// The contact as an Instant, for the countdown (the wall clock only crosses restarts).
    contact: Mutex<Instant>,
    boot_id: String,
    pub reboot: RebootPolicy,
    pub enabled: bool,
}

impl Fence {
    /// The fence kept at `path` (none: in memory). A worker started in another boot than it last ran in is fenced.
    pub fn open(path: Option<PathBuf>, boot_id: String, defaults: Timers, reboot: RebootPolicy, enabled: bool) -> Self {
        let read: Option<Kept> = path.as_ref().and_then(|p| std::fs::read(p).ok()).and_then(|b| serde_json::from_slice(&b).ok());
        let now = now_ms();
        let mut kept = read.clone().unwrap_or(Kept { fenced: None, boot_id: boot_id.clone(), contact_ms: now, timers: defaults });
        if enabled && read.is_some() && kept.boot_id != boot_id && kept.fenced.is_none() {
            tracing::warn!("this node rebooted since the worker last ran: fenced until the managers authorize it");
            kept.fenced = Some(Fenced { reason: "the node rebooted".into(), at_ms: now, boot_id: boot_id.clone() });
        }
        kept.boot_id = boot_id.clone();
        let silent = Duration::from_millis((now - kept.contact_ms).max(0) as u64);
        let contact = Instant::now().checked_sub(silent).unwrap_or_else(Instant::now);
        let f = Fence { path, kept: Mutex::new(kept), contact: Mutex::new(contact), boot_id, reboot, enabled };
        f.save();
        f
    }

    fn save(&self) {
        let Some(p) = &self.path else { return };
        let k = self.kept.lock().expect("fence lock");
        let tmp = p.with_extension("json.tmp");
        let w = serde_json::to_vec(&*k).map_err(std::io::Error::other).and_then(|b| std::fs::write(&tmp, b)).and_then(|_| std::fs::rename(&tmp, p));
        if let Err(e) = w {
            tracing::warn!("keeping the fence state in {}: {e}", p.display());
        }
    }

    /// A manager was heard from. Kept on disk at most every few seconds.
    pub fn heard(&self) {
        *self.contact.lock().expect("contact lock") = Instant::now();
        let now = now_ms();
        let stale = {
            let mut k = self.kept.lock().expect("fence lock");
            let stale = now - k.contact_ms > 3_000;
            if stale {
                k.contact_ms = now;
            }
            stale
        };
        if stale {
            self.save();
        }
    }

    pub fn silent_for(&self) -> Duration {
        self.contact.lock().expect("contact lock").elapsed()
    }

    pub fn timers(&self) -> Timers {
        self.kept.lock().expect("fence lock").timers
    }

    pub fn set_timers(&self, t: Timers) {
        let changed = {
            let mut k = self.kept.lock().expect("fence lock");
            let changed = k.timers != t;
            k.timers = t;
            changed
        };
        if changed {
            tracing::info!(stop_s = t.stop.as_secs(), storage_s = t.storage.as_secs(), reboot_s = t.reboot.as_secs(), "the cluster's fencing timers");
            self.save();
        }
    }

    pub fn fenced(&self) -> Option<Fenced> {
        self.kept.lock().expect("fence lock").fenced.clone()
    }

    pub fn enter(&self, reason: &str) {
        {
            let mut k = self.kept.lock().expect("fence lock");
            if k.fenced.is_some() {
                return;
            }
            k.fenced = Some(Fenced { reason: reason.into(), at_ms: now_ms(), boot_id: self.boot_id.clone() });
        }
        self.save();
    }

    /// The managers authorized this worker again.
    pub fn clear(&self) {
        let was = self.kept.lock().expect("fence lock").fenced.take();
        if was.is_some() {
            self.save();
        }
    }

    /// Whether a reboot for the fence may still be asked in this boot.
    pub fn reboot_due(&self, still_running: bool) -> bool {
        let fenced_here = self.fenced().is_some_and(|f| f.boot_id == self.boot_id);
        fenced_here
            && match self.reboot {
                RebootPolicy::Always => true,
                RebootPolicy::IfNeeded => still_running,
                RebootPolicy::Never => false,
            }
    }
}

/// The countdown, once a second until the process ends: past `stop` it fences and stops every managed container
/// (again each second while one still runs), past `stop + storage + reboot` it reboots once if the policy says so.
/// Fenced, it holds no floating address either (G10.3), taken off before the workloads stop. Each round also tells systemd's watchdog the worker is alive: a worker frozen anywhere stops saying so, and its
/// unit restarts it (and reboots the node if it will not come back), so a frozen worker still fences.
pub async fn watch(f: std::sync::Arc<Fence>, docker: Option<bollard::Docker>, net: Option<std::sync::Arc<crate::net::Net>>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut rebooting = false;
    loop {
        tick.tick().await;
        notify("WATCHDOG=1");
        if !f.enabled {
            continue;
        }
        let silent = f.silent_for();
        let st = stage(silent, &f.timers());
        if st < Stage::Stop {
            continue;
        }
        if f.fenced().is_none() {
            tracing::warn!("no manager heard from for {}s: fencing (stopping every managed workload)", silent.as_secs());
            f.enter(&format!("no manager heard from for {}s", silent.as_secs()));
        }
        if let Some(n) = &net {
            n.clone().drop_floating().await;
        }
        let running = match &docker {
            Some(d) => crate::docker::stop_all(d).await.unwrap_or_else(|e| {
                tracing::error!("fence: Docker did not answer: {e:#}");
                usize::MAX
            }),
            None => 0,
        };
        if st == Stage::Reboot && !rebooting && f.reboot_due(running > 0) {
            rebooting = true;
            tracing::error!("fence: rebooting the node ({} managed container(s) still running)", if running == usize::MAX { "unknown".to_string() } else { running.to_string() });
            match std::process::Command::new("systemctl").arg("reboot").status() {
                Ok(s) if s.success() => {}
                r => tracing::error!("fence: the reboot was refused: {r:?}"),
            }
        }
    }
}

/// `FREIGHTDOCK_FENCE` (on, off), `FREIGHTDOCK_FENCE_REBOOT` (if-needed, always, never) and the timers' defaults
/// before the cluster gives its own: `FREIGHTDOCK_FENCE_STOP_S`, `_STORAGE_S`, `_REBOOT_S`.
pub fn config(get: impl Fn(&str) -> Option<String>) -> Result<(bool, RebootPolicy, Timers)> {
    let enabled = match get("FREIGHTDOCK_FENCE").as_deref().map(str::trim) {
        None | Some("on") => true,
        Some("off") => false,
        Some(v) => bail!("FREIGHTDOCK_FENCE is on or off, not {v}"),
    };
    let reboot = match get("FREIGHTDOCK_FENCE_REBOOT").as_deref().map(str::trim) {
        None | Some("if-needed") => RebootPolicy::IfNeeded,
        Some("always") => RebootPolicy::Always,
        Some("never") => RebootPolicy::Never,
        Some(v) => bail!("FREIGHTDOCK_FENCE_REBOOT is if-needed, always or never, not {v}"),
    };
    let d = Timers::default();
    let secs = |k: &str, d: Duration| -> Result<Duration> {
        match get(k) {
            Some(v) => Ok(Duration::from_secs(v.trim().parse().map_err(|_| anyhow::anyhow!("{k} is not a number of seconds"))?)),
            None => Ok(d),
        }
    };
    Ok((enabled, reboot, Timers { stop: secs("FREIGHTDOCK_FENCE_STOP_S", d.stop)?, storage: secs("FREIGHTDOCK_FENCE_STORAGE_S", d.storage)?, reboot: secs("FREIGHTDOCK_FENCE_REBOOT_S", d.reboot)? }))
}

/// Tells systemd the worker is alive (`WATCHDOG=1`) or ready (`READY=1`), when it runs under a unit with
/// `NOTIFY_SOCKET` set; nothing otherwise. A unit with `WatchdogSec` restarts a worker that stops saying it.
pub fn notify(what: &str) {
    #[cfg(unix)]
    {
        let Some(sock) = std::env::var_os("NOTIFY_SOCKET") else { return };
        let s = sock.to_string_lossy();
        let Ok(u) = std::os::unix::net::UnixDatagram::unbound() else { return };
        let r = if let Some(abstract_name) = s.strip_prefix('@') {
            #[cfg(target_os = "linux")]
            {
                use std::os::linux::net::SocketAddrExt;
                std::os::unix::net::SocketAddr::from_abstract_name(abstract_name.as_bytes()).and_then(|a| u.send_to_addr(what.as_bytes(), &a))
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = abstract_name;
                return;
            }
        } else {
            u.send_to(what.as_bytes(), Path::new(&*s))
        };
        if let Err(e) = r {
            tracing::debug!("telling systemd {what}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> Timers {
        Timers { stop: Duration::from_secs(30), storage: Duration::from_secs(10), reboot: Duration::from_secs(5) }
    }

    #[test]
    fn the_countdown_goes_stop_storage_reboot() {
        assert_eq!(stage(Duration::from_secs(29), &t()), Stage::Normal);
        assert_eq!(stage(Duration::from_secs(30), &t()), Stage::Stop);
        assert_eq!(stage(Duration::from_secs(40), &t()), Stage::Storage);
        assert_eq!(stage(Duration::from_secs(45), &t()), Stage::Reboot);
    }

    #[test]
    fn the_cluster_s_timers_replace_the_defaults_but_zero_keeps_them() {
        let w = t().with_welcome(60, 0, 20);
        assert_eq!((w.stop, w.storage, w.reboot), (Duration::from_secs(60), Duration::from_secs(10), Duration::from_secs(20)));
    }

    #[test]
    fn fenced_stays_fenced_across_a_restart_until_authorized() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fence.json");
        let f = Fence::open(Some(p.clone()), "boot1".into(), t(), RebootPolicy::IfNeeded, true);
        assert!(f.fenced().is_none(), "a first start is not fenced");
        f.enter("no manager for 30s");
        drop(f);
        let f = Fence::open(Some(p.clone()), "boot1".into(), t(), RebootPolicy::IfNeeded, true);
        assert_eq!(f.fenced().unwrap().reason, "no manager for 30s");
        f.clear();
        let f = Fence::open(Some(p), "boot1".into(), t(), RebootPolicy::IfNeeded, true);
        assert!(f.fenced().is_none());
    }

    #[test]
    fn a_reboot_fences_and_is_not_asked_again_in_the_new_boot() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fence.json");
        let f = Fence::open(Some(p.clone()), "boot1".into(), t(), RebootPolicy::Always, true);
        assert!(!f.reboot_due(true), "not fenced");
        f.enter("no manager");
        assert!(f.reboot_due(false), "always");
        drop(f);
        let f = Fence::open(Some(p.clone()), "boot2".into(), t(), RebootPolicy::Always, true);
        assert!(f.fenced().is_some());
        assert!(!f.reboot_due(true), "it fenced in another boot: rebooting again would loop");

        // A reboot the worker did not ask for fences it too.
        f.clear();
        drop(f);
        let f = Fence::open(Some(p), "boot3".into(), t(), RebootPolicy::IfNeeded, true);
        assert_eq!(f.fenced().unwrap().reason, "the node rebooted");
    }

    #[test]
    fn if_needed_reboots_only_when_something_still_runs() {
        let f = Fence::open(None, "b".into(), t(), RebootPolicy::IfNeeded, true);
        f.enter("x");
        assert!(!f.reboot_due(false));
        assert!(f.reboot_due(true));
        let f = Fence::open(None, "b".into(), t(), RebootPolicy::Never, true);
        f.enter("x");
        assert!(!f.reboot_due(true));
    }

    #[test]
    fn the_countdown_carries_on_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fence.json");
        let f = Fence::open(Some(p.clone()), "b".into(), t(), RebootPolicy::Never, true);
        drop(f);
        // As if the last word was 40 seconds ago.
        let mut k: Kept = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        k.contact_ms -= 40_000;
        std::fs::write(&p, serde_json::to_vec(&k).unwrap()).unwrap();
        let f = Fence::open(Some(p), "b".into(), t(), RebootPolicy::Never, true);
        assert!(f.silent_for() >= Duration::from_secs(39));
        f.heard();
        assert!(f.silent_for() < Duration::from_secs(1));
    }

    #[test]
    fn the_settings_read_from_the_environment() {
        let env = |pairs: &'static [(&'static str, &'static str)]| move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string());
        let (on, r, tm) = config(env(&[("FREIGHTDOCK_FENCE_REBOOT", "never"), ("FREIGHTDOCK_FENCE_STOP_S", "12")])).unwrap();
        assert!(on);
        assert_eq!(r, RebootPolicy::Never);
        assert_eq!(tm.stop, Duration::from_secs(12));
        assert!(config(env(&[("FREIGHTDOCK_FENCE", "maybe")])).is_err());
    }
}
