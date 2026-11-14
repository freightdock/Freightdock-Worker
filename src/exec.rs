//! Commands: received, checked, run, reported. A command is known by its id: one seen again (a resend after a
//! reconnect, another manager's copy) is answered with the state it already has and is never run twice. Updates go to
//! whichever manager stream is open now, so a command that finishes while the worker is between managers is reported
//! to the next one (the manager sends unfinished commands again on connect, and the answer is the kept state).
//!
//! What it knows is kept on disk too (`<data>/commands.json`, written whole and renamed into place at every change), so
//! a restarted worker still answers a resend with what it did. A command it was in the middle of when it stopped is
//! reported failed ("the worker restarted while running it"): the worker cannot know how far it got, and running it
//! again blind is how things get done twice. The manager decides what to do next.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;

use freightdock_proto::v1::{self, CommandStatus, CommandUpdate, WorkerMessage, command::Op, worker_message};

/// How long a finished command is remembered, for resends.
const KEEP_FINISHED: Duration = Duration::from_secs(3600);

pub struct Executor {
    node_id: String,
    docker: Option<bollard::Docker>,
    known: Mutex<HashMap<String, Kept>>,
    out: Mutex<Option<mpsc::Sender<WorkerMessage>>>,
    journal: Option<PathBuf>,
    /// While it says fenced, no managed workload is started (plan 7.4).
    fence: std::sync::OnceLock<Arc<crate::fence::Fence>>,
    /// The overlay, told when a stack's bridge is there (G9).
    net: std::sync::OnceLock<Arc<crate::net::Net>>,
    /// Where the cluster's NFS storage is mounted (G11): `<data>/storage`.
    storage: std::sync::OnceLock<PathBuf>,
    /// How the shared storage made ready here answers (G11.8), for the heartbeat.
    health: std::sync::OnceLock<Arc<crate::storage::Health>>,
}

/// A command's last update, as kept on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Kept {
    status: i32,
    message: String,
    result_json: String,
    /// Unix milliseconds of the update; a finished command is forgotten `KEEP_FINISHED` after it.
    at_ms: i64,
}

impl Kept {
    fn update(&self, id: &str) -> CommandUpdate {
        CommandUpdate { command_id: id.to_string(), status: self.status, message: self.message.clone(), result_json: self.result_json.clone() }
    }
}

fn terminal(s: i32) -> bool {
    matches!(CommandStatus::try_from(s), Ok(CommandStatus::Succeeded | CommandStatus::Failed | CommandStatus::Rejected | CommandStatus::Expired))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl Executor {
    /// An executor that keeps its journal at `journal` (none: in memory only), read back from there.
    pub fn new(node_id: String, docker: Option<bollard::Docker>, journal: Option<PathBuf>) -> Arc<Self> {
        let mut known: HashMap<String, Kept> = journal.as_ref().and_then(|p| std::fs::read(p).ok()).and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        for (id, k) in known.iter_mut().filter(|(_, k)| !terminal(k.status)) {
            tracing::warn!(command = %id, "was running when the worker stopped; reported failed");
            *k = Kept { status: CommandStatus::Failed as i32, message: "the worker restarted while running it".into(), result_json: String::new(), at_ms: now_ms() };
        }
        let e = Executor { node_id, docker, known: Mutex::new(known), out: Mutex::default(), journal, fence: Default::default(), net: Default::default(), storage: Default::default(), health: Default::default() };
        e.save(&e.known.lock().expect("known lock"));
        Arc::new(e)
    }

    fn save(&self, known: &HashMap<String, Kept>) {
        let Some(p) = &self.journal else { return };
        let tmp = p.with_extension("json.tmp");
        let write = serde_json::to_vec(known).map_err(std::io::Error::other).and_then(|b| std::fs::write(&tmp, b)).and_then(|_| std::fs::rename(&tmp, p));
        if let Err(e) = write {
            tracing::warn!("keeping the command journal in {}: {e}", p.display());
        }
    }

    /// The fence that keeps runs from starting while the node is fenced.
    pub fn guard(&self, f: Arc<crate::fence::Fence>) {
        let _ = self.fence.set(f);
    }

    /// The overlay this worker runs.
    pub fn with_net(&self, n: Arc<crate::net::Net>) {
        let _ = self.net.set(n);
    }

    /// Mounts shared storage (G11) under `dir`.
    pub fn with_storage(&self, dir: PathBuf) {
        let _ = self.health.set(crate::storage::Health::new(self.node_id.clone(), Some(dir.with_extension("json"))));
        let _ = self.storage.set(dir);
    }

    /// The shared storage's health, probed now and then, if this worker keeps any.
    pub fn storage_health(&self) -> Option<Arc<crate::storage::Health>> {
        self.health.get().cloned()
    }

    /// The stream updates go to from now on; `None` when there is none.
    pub fn attach(&self, tx: Option<mpsc::Sender<WorkerMessage>>) {
        *self.out.lock().expect("out lock") = tx;
    }

    /// Commands seen and not finished.
    pub fn in_flight(&self) -> Vec<String> {
        self.known.lock().expect("known lock").iter().filter(|(_, k)| !terminal(k.status)).map(|(id, _)| id.clone()).collect()
    }

    fn send(&self, u: CommandUpdate) {
        if let Some(tx) = self.out.lock().expect("out lock").as_ref() {
            let _ = tx.try_send(WorkerMessage { body: Some(worker_message::Body::CommandUpdate(u)) });
        }
    }

    fn report(&self, id: &str, status: CommandStatus, message: impl Into<String>, result_json: String) {
        let kept = Kept { status: status as i32, message: message.into(), result_json, at_ms: now_ms() };
        let u = kept.update(id);
        {
            let mut k = self.known.lock().expect("known lock");
            let cutoff = now_ms() - KEEP_FINISHED.as_millis() as i64;
            k.retain(|_, k| !terminal(k.status) || k.at_ms > cutoff);
            k.insert(id.to_string(), kept);
            self.save(&k);
        }
        self.send(u);
    }

    pub fn handle(self: &Arc<Self>, c: v1::Command) {
        let known = self.known.lock().expect("known lock").get(&c.command_id).map(|k| k.update(&c.command_id));
        if let Some(u) = known {
            self.send(u);
            return;
        }
        let id = c.command_id.clone();
        if c.node_id != self.node_id {
            return self.report(&id, CommandStatus::Rejected, "this command is for another node", String::new());
        }
        if c.deadline_ms > 0 && now_ms() > c.deadline_ms {
            return self.report(&id, CommandStatus::Expired, "its deadline passed before it reached this worker", String::new());
        }
        let Some(op) = c.op else { return self.report(&id, CommandStatus::Rejected, "an operation this worker does not know", String::new()) };
        if matches!(op, Op::RunContainer(_) | Op::CephCommand(_) | Op::Cephadm(_)) && self.fence.get().is_some_and(|f| f.fenced().is_some()) {
            return self.report(&id, CommandStatus::Rejected, "this node is fenced and waits for the managers to authorize it", String::new());
        }
        self.report(&id, CommandStatus::Received, "", String::new());
        let me = self.clone();
        tokio::spawn(async move {
            me.report(&id, CommandStatus::Accepted, "", String::new());
            match me.run(op).await {
                Ok((msg, result)) => me.report(&id, CommandStatus::Succeeded, msg, result.to_string()),
                Err(e) => me.report(&id, CommandStatus::Failed, format!("{e:#}"), String::new()),
            }
        });
    }

    async fn run(&self, op: Op) -> anyhow::Result<(String, serde_json::Value)> {
        let docker = || self.docker.as_ref().ok_or_else(|| anyhow::anyhow!("this worker has no Docker"));
        Ok(match op {
            Op::Ping(_) => ("pong".into(), serde_json::json!({ "pong": true, "at_ms": now_ms() })),
            Op::DockerInfo(_) => ("".into(), crate::docker::info(docker()?).await?),
            Op::RunContainer(mut r) => {
                if !r.storage.is_empty() {
                    let base = anyhow::Context::context(self.storage.get().cloned(), "this worker keeps no shared storage")?;
                    let run = r.clone();
                    let b = base.clone();
                    let made = r.clone();
                    r.volumes = tokio::task::spawn_blocking(move || crate::storage::ready(&b, &run, &mut crate::storage::System)).await??;
                    if let Some(h) = self.health.get() {
                        for v in &made.storage {
                            if let Ok(root) = crate::storage::root_of(&base, v) {
                                h.remember(&v.storage, &v.driver, &root);
                            }
                        }
                    }
                }
                let v = crate::docker::run(docker()?, &r).await?;
                if let (Some(n), false) = (self.net.get(), r.network_stack.is_empty()) {
                    // Without it the stack's workloads on other nodes are not reached from here: said, not fatal.
                    if let Err(e) = n.clone().bridge_ready(r.network_stack.clone()).await {
                        tracing::warn!(stack = %r.network_stack, "proxy ARP on the stack's bridge: {e:#}");
                    }
                }
                ("running".into(), v)
            }
            Op::StopContainer(s) => ("stopped".into(), crate::docker::stop(docker()?, &s).await?),
            Op::CephCommand(c) => ("ran ceph".into(), crate::ceph::run(docker()?, &c).await?),
            Op::Cephadm(c) => {
                // cephadm lives beside the worker's other files (the storage directory's parent).
                let data = self.storage.get().and_then(|s| s.parent()).map(Path::to_path_buf).ok_or_else(|| anyhow::anyhow!("this worker has no data directory"))?;
                let action = c.action.clone();
                let v = tokio::task::spawn_blocking(move || crate::cephadm::run(&c, &crate::cephadm::Paths::system(&data), &mut crate::storage::System)).await??;
                (format!("cephadm {action}"), v)
            }
            Op::RemoveContainer(r) => {
                let d = docker()?;
                let (v, left) = crate::docker::remove(d, &r).await?;
                if let Some((stack, addr)) = left {
                    if let Some(n) = self.net.get() {
                        n.clone().forget(crate::docker::stack_pids(d, &stack).await, addr).await;
                    }
                    // The last of the stack here: its network goes too, so a node keeps only what it runs.
                    match crate::docker::drop_network_if_unused(d, &stack).await {
                        Ok(true) => tracing::info!(%stack, "removed the stack's network, no container of it is left here"),
                        Ok(false) => {}
                        Err(e) => tracing::warn!(%stack, "{e:#}"),
                    }
                }
                ("removed".into(), v)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(id: &str, node: &str, op: Op) -> v1::Command {
        v1::Command { command_id: id.into(), node_id: node.into(), config_version: 1, deadline_ms: now_ms() + 60_000, op: Some(op) }
    }

    async fn updates(rx: &mut mpsc::Receiver<WorkerMessage>, until_terminal: bool) -> Vec<(i32, String)> {
        let mut v = vec![];
        while let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            let Some(worker_message::Body::CommandUpdate(u)) = m.body else { continue };
            let t = terminal(u.status);
            v.push((u.status, u.message));
            if t && until_terminal {
                break;
            }
        }
        v
    }

    #[tokio::test]
    async fn a_ping_goes_received_accepted_succeeded() {
        let e = Executor::new("n1".into(), None, None);
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        e.handle(cmd("c1", "n1", Op::Ping(v1::Ping {})));
        let s: Vec<i32> = updates(&mut rx, true).await.into_iter().map(|u| u.0).collect();
        assert_eq!(s, vec![CommandStatus::Received as i32, CommandStatus::Accepted as i32, CommandStatus::Succeeded as i32]);
        assert!(e.in_flight().is_empty());
    }

    #[tokio::test]
    async fn a_command_seen_again_is_answered_not_run() {
        let e = Executor::new("n1".into(), None, None);
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        e.handle(cmd("c1", "n1", Op::Ping(v1::Ping {})));
        updates(&mut rx, true).await;
        e.handle(cmd("c1", "n1", Op::Ping(v1::Ping {})));
        let again = updates(&mut rx, false).await;
        assert_eq!(again.len(), 1, "one answer, its kept state");
        assert_eq!(again[0].0, CommandStatus::Succeeded as i32);
    }

    #[tokio::test]
    async fn another_node_s_or_a_late_command_is_not_run() {
        let e = Executor::new("n1".into(), None, None);
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        e.handle(cmd("c1", "n2", Op::Ping(v1::Ping {})));
        let mut late = cmd("c2", "n1", Op::Ping(v1::Ping {}));
        late.deadline_ms = 1;
        e.handle(late);
        let s: Vec<i32> = updates(&mut rx, false).await.into_iter().map(|u| u.0).collect();
        assert_eq!(s, vec![CommandStatus::Rejected as i32, CommandStatus::Expired as i32]);
    }

    #[tokio::test]
    async fn without_docker_a_docker_op_fails_and_says_why() {
        let e = Executor::new("n1".into(), None, None);
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        e.handle(cmd("c1", "n1", Op::DockerInfo(v1::DockerInfo {})));
        let u = updates(&mut rx, true).await;
        assert_eq!(u.last().map(|u| u.0), Some(CommandStatus::Failed as i32));
        assert!(u.last().unwrap().1.contains("no Docker"));
    }

    #[tokio::test]
    async fn a_fenced_worker_starts_nothing_but_still_stops() {
        let e = Executor::new("n1".into(), None, None);
        let f = Arc::new(crate::fence::Fence::open(None, "b".into(), Default::default(), crate::fence::RebootPolicy::Never, true));
        e.guard(f.clone());
        f.enter("test");
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        let run = v1::RunContainer { workload_id: "w".into(), image: "nginx".into(), ..Default::default() };
        e.handle(cmd("c1", "n1", Op::RunContainer(run.clone())));
        let u = updates(&mut rx, true).await;
        assert_eq!(u.last().map(|u| u.0), Some(CommandStatus::Rejected as i32));
        assert!(u.last().unwrap().1.contains("fenced"));
        e.handle(cmd("c2", "n1", Op::StopContainer(v1::StopContainer { workload_id: "w".into(), timeout_s: 1 })));
        let u = updates(&mut rx, true).await;
        assert_eq!(u.last().map(|u| u.0), Some(CommandStatus::Failed as i32), "a stop is tried (and fails here only for want of Docker)");
        f.clear();
        e.handle(cmd("c3", "n1", Op::RunContainer(run)));
        let u = updates(&mut rx, true).await;
        assert!(!u.last().unwrap().1.contains("fenced"));
    }

    #[tokio::test]
    async fn a_restarted_worker_remembers_and_does_not_run_again() {
        let dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("commands.json");
        {
            let e = Executor::new("n1".into(), None, Some(journal.clone()));
            e.handle(cmd("done", "n1", Op::Ping(v1::Ping {})));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // A command caught mid-run by the stop, as the journal would have it.
        let mut j: HashMap<String, Kept> = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
        j.insert("midway".into(), Kept { status: CommandStatus::Accepted as i32, message: String::new(), result_json: String::new(), at_ms: now_ms() });
        std::fs::write(&journal, serde_json::to_vec(&j).unwrap()).unwrap();

        let e = Executor::new("n1".into(), None, Some(journal));
        assert!(e.in_flight().is_empty(), "nothing is left half done after a restart");
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        e.handle(cmd("done", "n1", Op::Ping(v1::Ping {})));
        e.handle(cmd("midway", "n1", Op::Ping(v1::Ping {})));
        let u = updates(&mut rx, false).await;
        assert_eq!(u[0], (CommandStatus::Succeeded as i32, "pong".to_string()), "answered from the journal");
        assert_eq!(u[1].0, CommandStatus::Failed as i32, "not run again blind");
        assert_eq!(u.len(), 2);
    }

    #[tokio::test]
    async fn an_update_made_between_streams_is_kept_for_the_next() {
        let e = Executor::new("n1".into(), None, None);
        e.handle(cmd("c1", "n1", Op::Ping(v1::Ping {})));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (tx, mut rx) = mpsc::channel(16);
        e.attach(Some(tx));
        e.handle(cmd("c1", "n1", Op::Ping(v1::Ping {})));
        assert_eq!(updates(&mut rx, false).await, vec![(CommandStatus::Succeeded as i32, "pong".to_string())]);
    }
}
