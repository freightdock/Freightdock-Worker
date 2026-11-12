//! A real manager and a real worker in one process: enrolment with a join token, the mutual-TLS session, heartbeats,
//! a command and its report, a token refused a second time, and the worker coming back after the manager restarts.

use std::sync::Arc;
use std::time::Duration;

use freightdock_manager::state::{CommandRecord, CommandStatus, Entry, Op};
use freightdock_manager::{Config as MConfig, Manager, now_ms, sessions::Health};
use freightdock_proto::token::JoinToken;
use freightdock_worker::exec::Executor;
use freightdock_worker::host::Probe;
use freightdock_worker::session::Worker;
use freightdock_worker::{Config, Files, enrol};

async fn manager(dir: &std::path::Path, port: u16) -> (Arc<Manager>, tokio::task::JoinHandle<()>) {
    let config = MConfig { data_dir: dir.into(), bind: format!("127.0.0.1:{port}").parse().unwrap(), api_bind: None, api_tls_bind: None, ui_bind: None, advertise: vec![format!("127.0.0.1:{port}")], thresholds: Default::default(), bootstrap: true, join: None, local_worker_token: None, fencing: Default::default() };
    let m = Manager::open(&config).await.unwrap();
    let l = tokio::net::TcpListener::bind(config.bind).await.unwrap();
    let h = tokio::spawn({
        let m = m.clone();
        async move {
            let _ = freightdock_manager::serve_workers(m, l).await;
        }
    });
    (m, h)
}

async fn token(m: &Manager) -> String {
    let t = JoinToken { ca_sha256: m.ca.fingerprint(), secret: "ab".repeat(32) };
    m.store.propose(Entry::JoinTokenIssued { secret_hash: t.secret_hash(), expires_at: now_ms() + 60_000, at: now_ms(), for_manager: false }).await.unwrap();
    t.to_string()
}

async fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..100 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test]
async fn a_worker_enrols_connects_and_runs_a_command() {
    let (mdir, wdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let port = free_port();
    let (m, server) = manager(mdir.path(), port).await;
    let t = token(&m).await;

    let config = Config { managers: vec![format!("127.0.0.1:{port}")], token: Some(t.clone()), token_file: None, data_dir: wdir.path().into(), docker: "/nonexistent".into(), labels: [("region".to_string(), "lab".to_string())].into() };
    let files = Files::in_dir(wdir.path());
    enrol::enrol(&config, &files, &t, "test-host").await.unwrap();
    assert!(files.enrolled());
    let node_id = files.node_id().unwrap();

    // The same token again, from another worker: refused.
    let other = tempfile::tempdir().unwrap();
    let err = enrol::enrol(&Config { data_dir: other.path().into(), ..config.clone() }, &Files::in_dir(other.path()), &t, "thief").await.unwrap_err();
    assert!(err.to_string().contains("used"), "{err}");

    let exec = Executor::new(node_id.clone(), None, Some(wdir.path().join("commands.json")));
    let w = Arc::new(Worker { config, files, node_id: node_id.clone(), hostname: "test-host".into(), docker: None, exec, probe: Probe::default(), fence: Arc::new(freightdock_worker::fence::Fence::open(None, "boot".into(), Default::default(), freightdock_worker::fence::RebootPolicy::Never, true)) });
    let run = tokio::spawn({
        let w = w.clone();
        async move { w.run().await }
    });

    until("the worker's hello", || m.store.read(|s| s.nodes[&node_id].report.clone())).await;
    assert_eq!(m.sessions.liveness(&node_id).health, Health::Healthy);
    assert_eq!(m.store.read(|s| s.nodes[&node_id].report.as_ref().unwrap().labels.get("region").cloned()), Some("lab".into()));

    let c = CommandRecord { id: "c1".into(), node_id: node_id.clone(), op: Op::Ping, config_version: 0, created_at: now_ms(), deadline: now_ms() + 60_000, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: now_ms() };
    m.store.propose(Entry::CommandCreated { command: c.clone() }).await.unwrap();
    m.deliver(&c);
    until("the ping's end", || m.store.read(|s| (s.commands["c1"].status == CommandStatus::Succeeded).then_some(()))).await;
    assert!(m.store.read(|s| s.commands["c1"].result_json.contains("pong")));

    // The manager goes away and comes back with its state; the worker finds it again.
    m.sessions.kick_all();
    server.abort();
    m.store.shutdown().await;
    drop(m);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (m, _server) = manager(mdir.path(), port).await;
    until("the worker back", || (m.sessions.liveness(&node_id).health == Health::Healthy).then_some(())).await;
    assert_eq!(m.store.read(|s| s.commands["c1"].status), CommandStatus::Succeeded, "the log kept it");
    run.abort();
}
