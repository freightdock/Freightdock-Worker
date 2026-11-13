use std::sync::Arc;

use anyhow::{Context, Result, bail};

use freightdock_worker::exec::Executor;
use freightdock_worker::fence::Fence;
use freightdock_worker::host::{Probe, hostname};
use freightdock_worker::session::Worker;
use freightdock_worker::{Config, Files, enrol};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "freightdock_worker=info,warn".into())).init();
    let config = Config::from(|k| std::env::var(k).ok())?;
    std::fs::create_dir_all(&config.data_dir).with_context(|| format!("making {}", config.data_dir.display()))?;
    let files = Files::in_dir(&config.data_dir);
    let node_id = files.node_id()?;
    // A renewal cut off before both its key and certificate were in place (G2.7).
    files.finish_renewal()?;
    let hostname = hostname();
    tracing::info!(node = %node_id, %hostname, "freightdock-worker {}", env!("CARGO_PKG_VERSION"));

    if !files.enrolled() {
        let Some(token) = enrol::fresh_token(&config, &files) else {
            if let Some(f) = &config.token_file {
                bail!("not enrolled yet, and no unspent token in {} yet (its manager writes it at start)", f.display());
            }
            bail!("not enrolled yet: set FREIGHTDOCK_JOIN_TOKEN to a token from `freightdock-manager join-token`");
        };
        let cluster = enrol::enrol(&config, &files, &token, &hostname).await?;
        tracing::info!(%cluster, "enrolled");
    }

    let docker = match freightdock_worker::docker::connect(&config.docker) {
        Ok(d) if freightdock_worker::docker::version(&d).await.is_some() => Some(d),
        _ => {
            tracing::warn!("no Docker at {}: the worker connects, but cannot run workloads", config.docker.display());
            None
        }
    };
    let exec = Executor::new(node_id.clone(), docker.clone(), Some(config.data_dir.join("commands.json")));
    let (enabled, reboot, timers) = freightdock_worker::fence::config(|k| std::env::var(k).ok())?;
    let fence = Arc::new(Fence::open(Some(config.data_dir.join("fence.json")), freightdock_worker::host::boot_id(), timers, reboot, enabled));
    if !enabled {
        tracing::warn!("self-fencing is off (FREIGHTDOCK_FENCE=off): this node keeps running its workloads without any manager");
    }
    if let Some(f) = fence.fenced() {
        tracing::warn!("fenced ({}): no managed workload starts until the managers authorize this node", f.reason);
    }
    exec.guard(fence.clone());
    exec.with_storage(config.data_dir.join("storage"));
    let net = match freightdock_worker::net::Net::open(&config.data_dir) {
        Ok(n) => {
            exec.with_net(n.clone());
            Some(n)
        }
        Err(e) => {
            tracing::warn!("no overlay on this node: {e:#}");
            None
        }
    };
    tokio::spawn(freightdock_worker::fence::watch(fence.clone(), docker.clone(), net.clone(), Some(config.data_dir.join("storage"))));
    freightdock_worker::fence::notify("READY=1");
    let w = Worker { config, files, node_id, hostname, docker, exec, probe: Probe::default(), fence, net };
    tokio::select! {
        r = w.run() => r,
        _ = tokio::signal::ctrl_c() => { tracing::info!("stopping"); Ok(()) }
    }
}
