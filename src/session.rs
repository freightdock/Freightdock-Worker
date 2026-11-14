//! The session with the managers: one stream at a time to one manager, any manager. Hello, then a heartbeat every
//! interval the manager asks for, and commands handled as they come. A stream that breaks, or a manager silent for
//! longer than `SILENT`, sends the worker to the next manager in its list straight away; only after every manager has
//! failed in a row does it wait, from one second doubling to thirty, a quarter either way at random, so workers do not
//! all come back at once.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use rand::RngExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use freightdock_proto::v1::{self, FencingState, WorkerMessage, manager_message, worker_message, worker_session_client::WorkerSessionClient};

use crate::exec::Executor;
use crate::host::Probe;
use crate::{Config, Files};

/// A manager that has sent nothing (not even a heartbeat's ack) for this long is given up on.
const SILENT: Duration = Duration::from_secs(10);
const MAX_WAIT: Duration = Duration::from_secs(30);

pub struct Worker {
    pub config: Config,
    pub files: Files,
    pub node_id: String,
    pub hostname: String,
    pub docker: Option<bollard::Docker>,
    pub exec: Arc<Executor>,
    pub probe: Probe,
    /// The self-fencing state and the countdown from the last word of any manager.
    pub fence: Arc<crate::fence::Fence>,
    /// The overlay (G9); `None` when its key could not be made.
    pub net: Option<Arc<crate::net::Net>>,
    /// The shells open now (the UI's terminal).
    pub shells: Arc<crate::shell::Shells>,
}

/// What the hello and heartbeats say of the fence.
pub fn fencing_state(f: &crate::fence::Fence) -> FencingState {
    if f.fenced().is_some() { FencingState::AwaitingAuthorization } else { FencingState::None }
}

/// The heartbeat's interval a manager asks for, kept from half a second to thirty.
pub fn pace(ms: u32) -> Duration {
    Duration::from_millis(ms.clamp(500, 30_000) as u64)
}

/// The wait after `failed_rounds` rounds in which no manager would talk.
pub fn backoff(failed_rounds: u32, jitter: f64) -> Duration {
    let base = Duration::from_secs(1).saturating_mul(1 << failed_rounds.min(5)).min(MAX_WAIT);
    base.mul_f64(jitter.clamp(0.75, 1.25))
}

impl Worker {
    /// Runs until the process ends.
    pub async fn run(&self) -> Result<()> {
        let mut failed_rounds = 0u32;
        loop {
            let order = crate::dial_order(&self.config.managers, &self.files.known_managers());
            if order.is_empty() {
                bail!("no manager to dial: set FREIGHTDOCK_MANAGERS");
            }
            let mut talked = false;
            for addr in order {
                match self.session(&addr, &mut talked).await {
                    Ok(()) => tracing::info!("{addr} ended the stream"),
                    Err(e) => tracing::warn!("{addr}: {e:#}"),
                }
                if talked {
                    break;
                }
            }
            if talked {
                failed_rounds = 0;
                continue;
            }
            // Phase 7 turns this into the self-fencing countdown (stop managed workloads at N, storage at N+X, reboot
            // at N+X+Y). Until then it is only said.
            tracing::warn!("no manager heard from for {}s", self.fence.silent_for().as_secs());
            let wait = backoff(failed_rounds, rand::rng().random_range(0.75..1.25));
            failed_rounds = failed_rounds.saturating_add(1);
            tokio::time::sleep(wait).await;
        }
    }

    async fn session(&self, addr: &str, talked: &mut bool) -> Result<()> {
        let tls = crate::tls::mutual(&self.files)?;
        let ch = crate::tls::channel(addr, tls).await?;
        let (tx, rx) = mpsc::channel::<WorkerMessage>(256);
        let docker_version = match &self.docker {
            Some(d) => crate::docker::version(d).await,
            None => None,
        };
        let running = match &self.docker {
            Some(d) => crate::docker::running(d).await,
            None => 0,
        };
        let hello = v1::Hello {
            node_id: self.node_id.clone(),
            hostname: self.hostname.clone(),
            agent_version: env!("CARGO_PKG_VERSION").into(),
            boot_id: crate::host::boot_id(),
            fencing_state: fencing_state(&self.fence) as i32,
            network: self.net.as_ref().map(|n| n.report()),
            labels: self.config.labels.clone().into_iter().collect(),
            resources: Some(self.probe.resources(running)),
            capabilities: Some(crate::host::capabilities(docker_version, self.config.host_shell)),
            in_flight: self.exec.in_flight(),
            workloads: match &self.docker {
                Some(d) => crate::docker::workloads(d).await,
                None => None,
            },
        };
        tx.send(WorkerMessage { body: Some(worker_message::Body::Hello(hello)) }).await?;
        let mut inbound = WorkerSessionClient::new(ch.clone()).open(ReceiverStream::new(rx)).await?.into_inner();

        let welcome = match tokio::time::timeout(SILENT, inbound.message()).await {
            Ok(Ok(Some(v1::ManagerMessage { body: Some(manager_message::Body::Welcome(w)) }))) => w,
            Ok(Err(s)) => bail!("{}", s.message()),
            Err(_) => bail!("no welcome within {}s", SILENT.as_secs()),
            _ => bail!("the manager's first message was not a welcome"),
        };
        *talked = true;
        self.fence.heard();
        self.files.keep_managers(&welcome.managers);
        self.fence.set_timers(self.fence.timers().with_welcome(welcome.fence_stop_s, welcome.fence_storage_s, welcome.fence_reboot_s));
        let every = pace(welcome.heartbeat_interval_ms);
        // The pace a later Welcome may change (the cluster's settings), read by the heartbeats.
        let paced = Arc::new(std::sync::atomic::AtomicU64::new(every.as_millis() as u64));
        tracing::info!(manager = %welcome.manager_id, cluster = %welcome.cluster_id, "connected to {addr}; heartbeat every {}ms", every.as_millis());
        self.exec.attach(Some(tx.clone()));
        if welcome.renew_certificate {
            // Its certificate expires within a year: a new one now, used from the next stream on.
            let (files, ch) = (Files::in_dir(&self.config.data_dir), ch.clone());
            tokio::spawn(async move {
                match crate::enrol::renew(&files, ch).await {
                    Ok(()) => tracing::info!("renewed this node's certificate; the next stream uses it"),
                    Err(e) => tracing::warn!("{e:#} (tried again at the next stream)"),
                }
            });
        }

        let beats = {
            let (tx, docker, fence, net, paced) = (tx.clone(), self.docker.clone(), self.fence.clone(), self.net.clone(), paced.clone());
            let health = self.exec.storage_health();
            let probe = Probe::default();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut seq = 0u64;
                let mut every = every;
                loop {
                    tick.tick().await;
                    let now = Duration::from_millis(paced.load(std::sync::atomic::Ordering::Relaxed));
                    if now != every {
                        every = now;
                        tick = tokio::time::interval(every);
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    seq += 1;
                    let running = match &docker {
                        Some(d) => crate::docker::running(d).await,
                        None => 0,
                    };
                    let sent_at_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
                    let workloads = match &docker {
                        Some(d) => crate::docker::workloads(d).await,
                        None => None,
                    };
                    let traffic = match &net {
                        Some(n) => n.clone().traffic().await,
                        None => None,
                    };
                    let storage = health.as_ref().and_then(|h| {
                        h.tick();
                        h.report()
                    });
                    let hb = v1::Heartbeat { seq, sent_at_ms, resources: Some(probe.resources(running)), workloads, network: net.as_ref().map(|n| n.report()), fencing_state: fencing_state(&fence) as i32, traffic, storage };
                    if tx.send(WorkerMessage { body: Some(worker_message::Body::Heartbeat(hb)) }).await.is_err() {
                        break;
                    }
                }
            })
        };

        let end = loop {
            let next = tokio::time::timeout(SILENT, inbound.message()).await;
            if matches!(next, Ok(Ok(Some(_)))) {
                self.fence.heard();
            }
            match next {
                Ok(Ok(Some(m))) => match m.body {
                    Some(manager_message::Body::Command(c)) => {
                        tracing::info!(command = %c.command_id, "command received");
                        self.exec.handle(c);
                    }
                    Some(manager_message::Body::HeartbeatAck(_)) => {}
                    // A person's look at, or change to, this node's Docker: answered beside the stream.
                    Some(manager_message::Body::Docker(r)) => {
                        let (docker, tx) = (self.docker.clone(), tx.clone());
                        tokio::spawn(async move {
                            let reply = crate::pages::answer(docker.as_ref(), &r).await;
                            let _ = tx.send(WorkerMessage { body: Some(worker_message::Body::DockerReply(reply)) }).await;
                        });
                    }
                    // A person's shell: on a stream of its own to this manager, beside the session.
                    Some(manager_message::Body::ShellOpen(o)) => {
                        tokio::spawn(crate::shell::serve(ch.clone(), self.docker.clone(), self.shells.clone(), o));
                    }
                    // The node's overlay, whole: applied beside the stream, an older one than applied ignored.
                    Some(manager_message::Body::Network(n)) => {
                        if let Some(net) = &self.net {
                            tokio::spawn(net.clone().apply(n));
                        }
                    }
                    Some(manager_message::Body::Authorize(a)) => {
                        if let Some(f) = self.fence.fenced() {
                            tracing::warn!(by = %a.by, "authorized by the managers again (fenced since {}: {})", f.at_ms, f.reason);
                            self.fence.clear();
                        }
                    }
                    // Again on a stream: the cluster's settings changed. The new timers and pace from now on.
                    Some(manager_message::Body::Welcome(w)) => {
                        self.files.keep_managers(&w.managers);
                        let timers = self.fence.timers().with_welcome(w.fence_stop_s, w.fence_storage_s, w.fence_reboot_s);
                        if timers != self.fence.timers() {
                            tracing::info!(?timers, "the cluster's fencing timers changed");
                        }
                        self.fence.set_timers(timers);
                        paced.store(pace(w.heartbeat_interval_ms).as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                    None => {}
                },
                Ok(Ok(None)) => break Ok(()),
                Ok(Err(s)) => break Err(anyhow!("{}", s.message())),
                Err(_) => break Err(anyhow!("the manager was silent for {}s", SILENT.as_secs())),
            }
        };
        self.exec.attach(None);
        beats.abort();
        end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_heartbeat_s_pace_is_the_manager_s_within_limits() {
        assert_eq!(pace(2000), Duration::from_secs(2));
        assert_eq!(pace(0), Duration::from_millis(500));
        assert_eq!(pace(999_999), Duration::from_secs(30));
    }

    #[test]
    fn the_wait_doubles_to_thirty_seconds() {
        assert_eq!(backoff(0, 1.0), Duration::from_secs(1));
        assert_eq!(backoff(1, 1.0), Duration::from_secs(2));
        assert_eq!(backoff(4, 1.0), Duration::from_secs(16));
        assert_eq!(backoff(5, 1.0), Duration::from_secs(30));
        assert_eq!(backoff(50, 1.0), Duration::from_secs(30));
        assert_eq!(backoff(0, 9.0), Duration::from_millis(1250), "the jitter is a quarter at most");
    }
}
