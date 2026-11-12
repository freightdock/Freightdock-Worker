//! The worker's Docker: every container it makes is labelled with the workload it is for, so the worker finds it again
//! by workload (after a restart, after a resend) and never runs two for one workload.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, RestartPolicy, RestartPolicyNameEnum};
use bollard::query_parameters as q;
use futures_util::StreamExt;
use serde_json::{Value, json};

use freightdock_proto::v1;

pub const LABEL_MANAGED: &str = "io.freightdock.managed";
pub const LABEL_WORKLOAD: &str = "io.freightdock.workload";
pub const LABEL_GENERATION: &str = "io.freightdock.generation";

pub fn connect(socket: &Path) -> Result<Docker> {
    Ok(Docker::connect_with_unix(&socket.to_string_lossy(), 120, bollard::API_DEFAULT_VERSION)?)
}

pub async fn version(d: &Docker) -> Option<String> {
    d.version().await.ok().and_then(|v| v.version)
}

pub async fn running(d: &Docker) -> u32 {
    let f = HashMap::from([("status".to_string(), vec!["running".to_string()])]);
    d.list_containers(Some(q::ListContainersOptions { filters: Some(f), ..Default::default() })).await.map(|l| l.len() as u32).unwrap_or(0)
}

pub async fn info(d: &Docker) -> Result<Value> {
    let v = d.version().await?;
    let i = d.info().await?;
    Ok(json!({
        "version": v.version, "api_version": v.api_version, "os": i.operating_system, "kernel": i.kernel_version,
        "containers": i.containers, "running": i.containers_running, "images": i.images,
    }))
}

/// `nginx` to (`nginx`, `latest`); `ghcr.io:443/a/b:1.2` to (`ghcr.io:443/a/b`, `1.2`); a digest stays whole.
pub fn split_tag(image: &str) -> (&str, &str) {
    if image.contains('@') {
        return (image, "");
    }
    let last = image.rfind('/').map_or(0, |i| i + 1);
    match image[last..].rfind(':') {
        Some(i) => (&image[..last + i], &image[last + i + 1..]),
        None => (image, "latest"),
    }
}

/// A container of `workload` on this worker: its id, whether it runs, and the generation it was made for.
struct Found {
    id: String,
    up: bool,
    generation: u64,
}

fn generation_of(labels: Option<&HashMap<String, String>>) -> u64 {
    labels.and_then(|l| l.get(LABEL_GENERATION)).and_then(|g| g.parse().ok()).unwrap_or(0)
}

/// The container of `workload`, if this worker has one.
async fn find(d: &Docker, workload: &str) -> Result<Option<Found>> {
    let f = HashMap::from([("label".to_string(), vec![format!("{LABEL_WORKLOAD}={workload}")])]);
    let l = d.list_containers(Some(q::ListContainersOptions { all: true, filters: Some(f), ..Default::default() })).await?;
    Ok(l.into_iter().next().map(|c| Found { generation: generation_of(c.labels.as_ref()), up: c.state == Some(bollard::models::ContainerSummaryStateEnum::RUNNING), id: c.id.unwrap_or_default() }))
}

/// Every managed container here, for the heartbeat; `None` when Docker does not answer (the manager then acts on
/// nothing it cannot see).
pub async fn workloads(d: &Docker) -> Option<v1::WorkloadReport> {
    let f = HashMap::from([("label".to_string(), vec![format!("{LABEL_MANAGED}=true")])]);
    let l = d.list_containers(Some(q::ListContainersOptions { all: true, filters: Some(f), ..Default::default() })).await.ok()?;
    let workloads = l
        .into_iter()
        .filter_map(|c| {
            let workload_id = c.labels.as_ref()?.get(LABEL_WORKLOAD)?.clone();
            Some(v1::WorkloadState {
                workload_id,
                generation: generation_of(c.labels.as_ref()),
                container_id: c.id.unwrap_or_default(),
                state: c.state.map(|s| s.to_string()).unwrap_or_default(),
                exit_code: exit_code(c.status.as_deref().unwrap_or("")),
                image: c.image.unwrap_or_default(),
            })
        })
        .collect();
    Some(v1::WorkloadReport { workloads })
}

/// The code in Docker's status line, "Exited (137) 5 minutes ago"; 0 for any other line.
fn exit_code(status: &str) -> i32 {
    status.strip_prefix("Exited (").and_then(|r| r.split(')').next()).and_then(|c| c.parse().ok()).unwrap_or(0)
}

/// The published ports, as Docker wants them: the exposed set and the bindings.
type Ports = (HashMap<String, HashMap<(), ()>>, bollard::models::PortMap);

fn ports(list: &[v1::PortMapping]) -> Result<Ports> {
    let mut exposed = HashMap::new();
    let mut bound: bollard::models::PortMap = HashMap::new();
    for p in list {
        let proto = if p.protocol.is_empty() { "tcp" } else { p.protocol.as_str() };
        if !matches!(proto, "tcp" | "udp") || p.container_port == 0 || p.container_port > 65535 || p.host_port > 65535 {
            return Err(anyhow!("a port must be 1 to 65535 over tcp or udp"));
        }
        let key = format!("{}/{proto}", p.container_port);
        exposed.insert(key.clone(), HashMap::new());
        let b = bollard::models::PortBinding { host_ip: (!p.host_ip.is_empty()).then(|| p.host_ip.clone()), host_port: (p.host_port != 0).then(|| p.host_port.to_string()) };
        bound.entry(key).or_insert_with(|| Some(vec![])).get_or_insert_with(Vec::new).push(b);
    }
    Ok((exposed, bound))
}

/// `source:target[:ro|rw]`, the source a volume name or an absolute path, the target absolute.
fn valid_volume(v: &str) -> Result<()> {
    let parts: Vec<&str> = v.split(':').collect();
    let ok = match parts.as_slice() {
        [src, dst] | [src, dst, "ro" | "rw"] => !src.is_empty() && dst.starts_with('/') && (src.starts_with('/') || src.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))),
        _ => false,
    };
    if ok { Ok(()) } else { Err(anyhow!("{v:?} is not a volume as source:target[:ro]")) }
}

fn valid_workload(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 128 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)) {
        return Err(anyhow!("the workload id must be 1 to 128 of letters, digits and . _ -"));
    }
    Ok(())
}

async fn pull(d: &Docker, image: &str) -> Result<()> {
    let (from_image, tag) = split_tag(image);
    let mut pull = d.create_image(Some(q::CreateImageOptions { from_image: Some(from_image.to_string()), tag: Some(tag.to_string()), ..Default::default() }), None, None);
    while let Some(step) = pull.next().await {
        let step = step.with_context(|| format!("pulling {image}"))?;
        if let Some(e) = step.error_detail.and_then(|e| e.message) {
            return Err(anyhow!("pulling {image}: {e}"));
        }
    }
    Ok(())
}

/// What a failed pull means: with the image on this node (built or loaded here, never pushed anywhere) it runs from
/// that one, `Ok(true)`; without, the pull's error stands.
fn after_pull(pulled: Result<()>, here: bool) -> Result<bool> {
    match pulled {
        Ok(()) => Ok(false),
        Err(_) if here => Ok(true),
        Err(e) => Err(e),
    }
}

pub async fn run(d: &Docker, r: &v1::RunContainer) -> Result<Value> {
    valid_workload(&r.workload_id)?;
    r.volumes.iter().try_for_each(|v| valid_volume(v))?;
    let (exposed, bound) = ports(&r.ports)?;
    if let Some(f) = find(d, &r.workload_id).await? {
        if f.generation == r.generation {
            if !f.up {
                d.start_container(&f.id, None::<q::StartContainerOptions>).await.context("starting the workload's container")?;
            }
            return Ok(json!({ "container_id": f.id, "created": false }));
        }
        // Made for another spec: made again.
        d.remove_container(&f.id, Some(q::RemoveContainerOptions { force: true, ..Default::default() })).await.context("removing the container of the old spec")?;
    }
    let pulled = pull(d, &r.image).await;
    let here = pulled.is_err() && d.inspect_image(&r.image).await.is_ok();
    if after_pull(pulled, here)? {
        tracing::warn!(image = %r.image, "could not pull it; running the image already on this node");
    }
    let mut labels = r.labels.clone();
    labels.insert(LABEL_MANAGED.into(), "true".into());
    labels.insert(LABEL_WORKLOAD.into(), r.workload_id.clone());
    labels.insert(LABEL_GENERATION.into(), r.generation.to_string());
    let body = ContainerCreateBody {
        image: Some(r.image.clone()),
        cmd: (!r.command.is_empty()).then(|| r.command.clone()),
        env: Some(r.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
        labels: Some(labels),
        exposed_ports: (!exposed.is_empty()).then_some(exposed),
        // Never restarted by Docker on its own: a managed workload restarts only when the cluster says so, which is
        // what keeps a fenced worker from bringing its containers back after a reboot.
        host_config: Some(HostConfig {
            restart_policy: Some(RestartPolicy { name: Some(RestartPolicyNameEnum::NO), maximum_retry_count: None }),
            port_bindings: (!bound.is_empty()).then_some(bound),
            binds: (!r.volumes.is_empty()).then(|| r.volumes.clone()),
            nano_cpus: (r.cpu_millis > 0).then(|| r.cpu_millis as i64 * 1_000_000),
            memory: (r.memory_bytes > 0).then_some(r.memory_bytes as i64),
            ..Default::default()
        }),
        ..Default::default()
    };
    let name = (!r.name.is_empty()).then(|| r.name.clone());
    let made = d.create_container(Some(q::CreateContainerOptions { name, ..Default::default() }), body).await.context("creating the container")?;
    d.start_container(&made.id, None::<q::StartContainerOptions>).await.context("starting the container")?;
    Ok(json!({ "container_id": made.id, "created": true }))
}

/// Stops every managed container that runs (the fence at N). Answers how many still run after.
pub async fn stop_all(d: &Docker) -> Result<usize> {
    let f = HashMap::from([("label".to_string(), vec![format!("{LABEL_MANAGED}=true")]), ("status".to_string(), vec!["running".to_string()])]);
    let list = |f: HashMap<String, Vec<String>>| d.list_containers(Some(q::ListContainersOptions { filters: Some(f), ..Default::default() }));
    for c in list(f.clone()).await? {
        let id = c.id.unwrap_or_default();
        if let Err(e) = d.stop_container(&id, Some(q::StopContainerOptions { t: Some(10), signal: None })).await {
            tracing::warn!(container = %id, "fence: stopping: {e}");
        }
    }
    Ok(list(f).await?.len())
}

pub async fn stop(d: &Docker, s: &v1::StopContainer) -> Result<Value> {
    valid_workload(&s.workload_id)?;
    let Some(Found { id, up, .. }) = find(d, &s.workload_id).await? else { return Ok(json!({ "container_id": null })) };
    if up {
        let t = if s.timeout_s == 0 { 10 } else { s.timeout_s.min(600) as i32 };
        match d.stop_container(&id, Some(q::StopContainerOptions { t: Some(t), signal: None })).await {
            Ok(()) | Err(bollard::errors::Error::DockerResponseServerError { status_code: 304, .. }) => {}
            Err(e) => return Err(e).context("stopping the container"),
        }
    }
    Ok(json!({ "container_id": id }))
}

pub async fn remove(d: &Docker, r: &v1::RemoveContainer) -> Result<Value> {
    valid_workload(&r.workload_id)?;
    let Some(Found { id, .. }) = find(d, &r.workload_id).await? else { return Ok(json!({ "container_id": null })) };
    d.remove_container(&id, Some(q::RemoveContainerOptions { force: true, ..Default::default() })).await.context("removing the container")?;
    Ok(json!({ "container_id": id }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_split_at_the_last_colon_of_the_last_part() {
        assert_eq!(split_tag("nginx"), ("nginx", "latest"));
        assert_eq!(split_tag("ghcr.io:443/a/b:1.2"), ("ghcr.io:443/a/b", "1.2"));
        assert_eq!(split_tag("ghcr.io:443/a/b"), ("ghcr.io:443/a/b", "latest"));
        assert_eq!(split_tag("a@sha256:abc"), ("a@sha256:abc", ""));
    }

    #[test]
    fn the_exit_code_is_read_from_docker_s_status_line() {
        assert_eq!(exit_code("Exited (137) 5 minutes ago"), 137);
        assert_eq!(exit_code("Up 3 hours"), 0);
        assert_eq!(exit_code(""), 0);
    }

    #[test]
    fn ports_become_exposed_ports_and_bindings() {
        let p = |h, c, proto: &str| v1::PortMapping { host_port: h, container_port: c, protocol: proto.into(), host_ip: String::new() };
        let (exposed, bound) = ports(&[p(8080, 80, ""), p(5353, 53, "udp"), p(0, 9000, "tcp")]).unwrap();
        assert!(exposed.contains_key("80/tcp") && exposed.contains_key("53/udp") && exposed.contains_key("9000/tcp"));
        assert_eq!(bound["80/tcp"].as_ref().unwrap()[0].host_port.as_deref(), Some("8080"));
        assert_eq!(bound["9000/tcp"].as_ref().unwrap()[0].host_port, None, "0: any free port");
        assert!(ports(&[p(1, 0, "")]).is_err());
        assert!(ports(&[p(1, 80, "sctp")]).is_err());
    }

    #[test]
    fn volumes_are_source_target_and_an_optional_mode() {
        assert!(valid_volume("data:/var/lib/data").is_ok());
        assert!(valid_volume("/srv/www:/usr/share/nginx/html:ro").is_ok());
        assert!(valid_volume("data:relative").is_err());
        assert!(valid_volume("../x:/y").is_err());
        assert!(valid_volume("a:/b:rx").is_err());
        assert!(valid_volume("/only").is_err());
    }

    #[test]
    fn an_image_that_cannot_be_pulled_runs_from_the_one_here_if_there_is_one() {
        assert!(matches!(after_pull(Ok(()), false), Ok(false)));
        assert!(matches!(after_pull(Err(anyhow!("no registry")), true), Ok(true)), "built or loaded here, never pushed");
        assert_eq!(after_pull(Err(anyhow!("pulling x: denied")), false).unwrap_err().to_string(), "pulling x: denied");
    }

    #[test]
    fn a_workload_id_is_a_label_value_safe_name() {
        assert!(valid_workload("web-1.a_b").is_ok());
        assert!(valid_workload("").is_err());
        assert!(valid_workload("a,b=c").is_err());
    }
}
