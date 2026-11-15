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
    let config = MConfig { data_dir: dir.into(), bind: format!("127.0.0.1:{port}").parse().unwrap(), api_bind: None, api_tls_bind: None, advertise: vec![format!("127.0.0.1:{port}")], thresholds: Default::default(), bootstrap: true, join: None, local_worker_token: None, fencing: Default::default() };
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

    let config = Config { managers: vec![format!("127.0.0.1:{port}")], token: Some(t.clone()), token_file: None, data_dir: wdir.path().into(), docker: "/nonexistent".into(), labels: [("region".to_string(), "lab".to_string())].into(), host_shell: true };
    let files = Files::in_dir(wdir.path());
    enrol::enrol(&config, &files, &t, "test-host").await.unwrap();
    assert!(files.enrolled());
    let node_id = files.node_id().unwrap();

    // The same token again, from another worker: refused.
    let other = tempfile::tempdir().unwrap();
    let err = enrol::enrol(&Config { data_dir: other.path().into(), ..config.clone() }, &Files::in_dir(other.path()), &t, "thief").await.unwrap_err();
    assert!(err.to_string().contains("used"), "{err}");

    let exec = Executor::new(node_id.clone(), None, Some(wdir.path().join("commands.json")));
    let net = freightdock_worker::net::Net::open(wdir.path()).unwrap();
    let key = net.report().public_key;
    let w = Arc::new(Worker { config, files, node_id: node_id.clone(), hostname: "test-host".into(), docker: None, exec, probe: Probe::default(), net: Some(net), shells: freightdock_worker::shell::Shells::new(true), fence: Arc::new(freightdock_worker::fence::Fence::open(None, "boot".into(), Default::default(), freightdock_worker::fence::RebootPolicy::Never, true)) });
    let run = tokio::spawn({
        let w = w.clone();
        async move { w.run().await }
    });

    until("the worker's hello", || m.store.read(|s| s.nodes[&node_id].report.clone())).await;
    assert_eq!(m.sessions.liveness(&node_id).health, Health::Healthy);
    assert_eq!(m.store.read(|s| s.nodes[&node_id].report.as_ref().unwrap().labels.get("region").cloned()), Some("lab".into()));
    assert_eq!(m.store.read(|s| s.nodes[&node_id].report.as_ref().unwrap().wg_public_key.clone()), key, "the node's WireGuard key, for the others' peers");

    let c = CommandRecord { id: "c1".into(), node_id: node_id.clone(), op: Op::Ping, config_version: 0, created_at: now_ms(), deadline: now_ms() + 60_000, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: now_ms() };
    m.store.propose(Entry::CommandCreated { command: c.clone() }).await.unwrap();
    m.deliver(&c);
    until("the ping's end", || m.store.read(|s| (s.commands["c1"].status == CommandStatus::Succeeded).then_some(()))).await;
    assert!(m.store.read(|s| s.commands["c1"].result_json.contains("pong")));

    // The node's Docker pages: a request on the stream, the reply by its id (this worker has no Docker to ask).
    assert!(m.store.read(|s| s.nodes[&node_id].report.as_ref().unwrap().docker_requests), "the worker says it answers");
    let e = m.docker(&node_id, "containers", "", Default::default()).await.unwrap_err();
    assert_eq!(e.to_string(), "this worker has no Docker");
    let e = m.docker(&node_id, "exec", "x", Default::default()).await.unwrap_err();
    assert!(e.to_string().contains("not something"), "{e}");

    // A shell on the host (the UI's terminal): opened on the worker's own stream to the manager, what is typed run on
    // a terminal of its own, its output and exit code back. A container's needs a Docker this worker has not.
    let caps = m.store.read(|s| s.nodes[&node_id].report.clone().unwrap());
    assert!(caps.shells && caps.host_shell, "the worker says it opens shells");
    let mut pipe = m.shell(&node_id, "", 100, 30).await.unwrap();
    use freightdock_proto::v1::{ShellDown, shell_down, shell_up};
    pipe.down.send(ShellDown { body: Some(shell_down::Body::Input(b"stty size; echo hi-$((1+2)); exit 7\n".to_vec())) }).await.unwrap();
    let (mut seen, mut code) = (String::new(), None);
    while let Ok(Some(u)) = tokio::time::timeout(Duration::from_secs(20), pipe.up.recv()).await {
        match u.body {
            Some(shell_up::Body::Output(b)) => seen.push_str(&String::from_utf8_lossy(&b)),
            Some(shell_up::Body::End(e)) => {
                code = Some((e.exit_code, e.error));
                break;
            }
            _ => {}
        }
    }
    assert!(seen.contains("hi-3") && seen.contains("30 100"), "{seen:?}");
    assert_eq!(code, Some((7, String::new())), "{seen:?}");
    let mut pipe = m.shell(&node_id, "web", 80, 24).await.unwrap();
    match tokio::time::timeout(Duration::from_secs(10), pipe.up.recv()).await.unwrap().and_then(|u| u.body) {
        Some(shell_up::Body::End(e)) => assert!(e.error.contains("no Docker"), "{e:?}"),
        other => panic!("{other:?}"),
    }

    // An upgrade: the release's binary is in the manager's folder, the worker fetches it over its own session, checks
    // it and puts it in place (here a file of the test's, never its own binary, and no restart).
    assert!(m.store.read(|s| s.nodes[&node_id].report.as_ref().unwrap().upgrade), "the worker says it takes upgrades");
    let bin = wdir.path().join("bin").join("freightdock-worker");
    std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
    std::fs::write(&bin, b"old").unwrap();
    w.exec.upgrade_to(bin.clone(), false);
    let new: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let sum = freightdock_manager::release::put_blob(mdir.path(), &new).unwrap();
    let release = |size| freightdock_manager::release::Release { version: "9.0.0".into(), arch: "amd64".into(), worker: Some(freightdock_manager::release::Binary { sha256: sum.clone(), size }), manager: None, source: "upload".into(), at: now_ms() };
    m.store.propose(Entry::ReleaseSet { release: release(new.len() as u64) }).await.unwrap();
    let upgrade = |id: &str, sha256: &str, size: u64| CommandRecord { id: id.into(), node_id: node_id.clone(), op: Op::Upgrade { version: "9.0.0".into(), sha256: sha256.into(), size }, config_version: 0, created_at: now_ms(), deadline: now_ms() + 60_000, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: now_ms() };
    let ended = |id: &'static str| {
        let m = m.clone();
        move || m.store.read(|s| s.commands.get(id).filter(|c| matches!(c.status, CommandStatus::Succeeded | CommandStatus::Failed)).map(|c| (c.status, c.message.clone())))
    };
    let c = upgrade("u1", &sum, new.len() as u64);
    m.store.propose(Entry::CommandCreated { command: c.clone() }).await.unwrap();
    m.deliver(&c);
    let (st, msg) = until("the upgrade's end", ended("u1")).await;
    assert_eq!(st, CommandStatus::Succeeded, "{msg}");
    assert_eq!(std::fs::read(&bin).unwrap(), new, "the new binary in place, whole");
    assert!(msg.contains("restart the worker"), "{msg}");
    // A release that says another size: the worker takes no more than it says, and keeps what it has.
    std::fs::write(&bin, b"old").unwrap();
    m.store.propose(Entry::ReleaseSet { release: release(1000) }).await.unwrap();
    let c = upgrade("u2", &sum, 1000);
    m.store.propose(Entry::CommandCreated { command: c.clone() }).await.unwrap();
    m.deliver(&c);
    let (st, msg) = until("the short upgrade's end", ended("u2")).await;
    assert_eq!((st, std::fs::read(&bin).unwrap()), (CommandStatus::Failed, b"old".to_vec()), "{msg}");
    // A checksum that is no release's: the manager hands nothing out.
    let c = upgrade("u3", &"0".repeat(64), 3);
    m.store.propose(Entry::CommandCreated { command: c.clone() }).await.unwrap();
    m.deliver(&c);
    let (st, msg) = until("the unknown upgrade's end", ended("u3")).await;
    assert_eq!(st, CommandStatus::Failed);
    assert!(msg.contains("no binary of the cluster's release"), "{msg}");

    // G2.7: a new certificate, asked with the current one; the worker comes back with it, and the old one still opens.
    let before = m.store.read(|s| s.nodes[&node_id].cert_sha256.clone());
    let ch = freightdock_worker::tls::channel(&format!("127.0.0.1:{port}"), freightdock_worker::tls::mutual(&w.files).unwrap()).await.unwrap();
    freightdock_worker::enrol::renew(&w.files, ch).await.unwrap();
    let after = m.store.read(|s| s.nodes[&node_id].cert_sha256.clone());
    assert_ne!(before, after);
    let kept: rustls::pki_types::CertificateDer = rustls::pki_types::pem::PemObject::from_pem_file(&w.files.cert).unwrap();
    assert_eq!(freightdock_proto::sha256_hex(&kept), after, "the worker keeps the one the managers know");
    assert!(m.store.read(|s| s.node_by_cert(&before).is_some()), "the old one too, until the next renewal");
    m.sessions.kick_all();
    until("the worker back with its new certificate", || (m.sessions.liveness(&node_id).health == Health::Healthy).then_some(())).await;

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
