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
    /// Its addresses while it runs (`addresses_of`).
    addresses: Vec<String>,
}

fn generation_of(labels: Option<&HashMap<String, String>>) -> u64 {
    labels.and_then(|l| l.get(LABEL_GENERATION)).and_then(|g| g.parse().ok()).unwrap_or(0)
}

/// The container of `workload`, if this worker has one.
async fn find(d: &Docker, workload: &str) -> Result<Option<Found>> {
    let f = HashMap::from([("label".to_string(), vec![format!("{LABEL_WORKLOAD}={workload}")])]);
    let l = d.list_containers(Some(q::ListContainersOptions { all: true, filters: Some(f), ..Default::default() })).await?;
    Ok(l.into_iter().next().map(|c| Found { generation: generation_of(c.labels.as_ref()), up: c.state == Some(bollard::models::ContainerSummaryStateEnum::RUNNING), addresses: addresses_of(&c), id: c.id.unwrap_or_default() }))
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
            let addresses = addresses_of(&c);
            Some(v1::WorkloadState {
                workload_id,
                generation: generation_of(c.labels.as_ref()),
                container_id: c.id.unwrap_or_default(),
                state: c.state.map(|s| s.to_string()).unwrap_or_default(),
                exit_code: exit_code(c.status.as_deref().unwrap_or("")),
                addresses,
                image: c.image.unwrap_or_default(),
            })
        })
        .collect();
    Some(v1::WorkloadReport { workloads })
}

/// A running container's IPv4 addresses on its networks, sorted; none for one not running.
fn addresses_of(c: &bollard::models::ContainerSummary) -> Vec<String> {
    if c.state != Some(bollard::models::ContainerSummaryStateEnum::RUNNING) {
        return vec![];
    }
    let mut v: Vec<String> = c.network_settings.as_ref().and_then(|n| n.networks.as_ref()).into_iter().flat_map(|n| n.values()).filter_map(|e| e.ip_address.clone()).filter(|a| !a.is_empty()).collect();
    v.sort();
    v
}

/// Whether the container there is the run's: made for its generation and, running, on its overlay address. One made
/// before the node knew the overlay is not, and is made again.
fn kept(f: &Found, r: &v1::RunContainer) -> bool {
    f.generation == r.generation && (r.network_stack.is_empty() || !f.up || f.addresses.contains(&r.address))
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

/// The ports Docker publishes on this node for `r`: none for a workload on its stack's network, whose ports the
/// routing mesh answers on every node (G10.2); they are checked all the same.
fn published(r: &v1::RunContainer) -> Result<Ports> {
    let p = ports(&r.ports)?;
    Ok(if r.network_stack.is_empty() { p } else { Default::default() })
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

/// How a workload of a stack is attached (G9): its stack's network, its fixed address, its stack's names.
#[derive(Debug)]
pub struct Attach {
    pub stack: String,
    pub network_mode: String,
    pub hosts: Vec<String>,
    pub config: bollard::models::NetworkingConfig,
}

/// The network part of `r`; `None` for a workload of no stack (Docker's default network, as before G9).
pub fn attach(r: &v1::RunContainer) -> Result<Option<Attach>> {
    if r.network_stack.is_empty() {
        return Ok(None);
    }
    valid_workload(&r.network_stack).map_err(|_| anyhow!("{:?} is not a stack's name", r.network_stack))?;
    crate::net::stack_net(&r.address)?;
    if let Some(h) = r.hosts.iter().find(|h| !crate::net::valid_host(h)) {
        return Err(anyhow!("{h:?} is not a host as name:address"));
    }
    let name = crate::net::network_name(&r.network_stack);
    let ep = bollard::models::EndpointSettings { ipam_config: Some(bollard::models::EndpointIpamConfig { ipv4_address: Some(r.address.clone()), ..Default::default() }), ..Default::default() };
    Ok(Some(Attach { stack: r.network_stack.clone(), network_mode: name.clone(), hosts: r.hosts.clone(), config: bollard::models::NetworkingConfig { endpoints_config: Some(HashMap::from([(name, ep)])) } }))
}

/// The stack's network on this node, made when it is not there; one made for another subnet is an error, never
/// changed under the containers on it. Answers whether it was made now.
pub async fn ensure_network(d: &Docker, stack: &str, address: &str) -> Result<bool> {
    let want = crate::net::network_request(stack, address, crate::net::MTU)?;
    let subnet = want.ipam.as_ref().and_then(|i| i.config.as_ref()).and_then(|c| c[0].subnet.clone());
    match d.inspect_network(&want.name, None::<q::InspectNetworkOptions>).await {
        Ok(n) => {
            let have = n.ipam.and_then(|i| i.config).and_then(|c| c.into_iter().next()).and_then(|c| c.subnet);
            if have != subnet {
                return Err(anyhow!("the network {} is for {}, not {}", want.name, have.unwrap_or_default(), subnet.unwrap_or_default()));
            }
            Ok(false)
        }
        Err(bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }) => {
            d.create_network(want).await.with_context(|| format!("making the network of stack {stack}"))?;
            Ok(true)
        }
        Err(e) => Err(e).context("asking Docker for the stack's network"),
    }
}

/// A registry login as Docker takes it with a pull; none for an empty one. Docker Hub's server is its v1 index, as
/// `docker login` names it.
pub fn credentials(auth: Option<&v1::RegistryAuth>) -> Option<bollard::auth::DockerCredentials> {
    let a = auth.filter(|a| !a.username.is_empty())?;
    let server = if a.server == "docker.io" { "https://index.docker.io/v1/".to_string() } else { a.server.clone() };
    Some(bollard::auth::DockerCredentials { username: Some(a.username.clone()), password: Some(a.password.clone()), serveraddress: Some(server), ..Default::default() })
}

/// Pulls the image, with the registry's login when the managers sent one.
pub async fn pull(d: &Docker, image: &str, auth: Option<&v1::RegistryAuth>) -> Result<()> {
    let (from_image, tag) = split_tag(image);
    let tag = (!tag.is_empty()).then(|| tag.to_string());
    let mut pull = d.create_image(Some(q::CreateImageOptions { from_image: Some(from_image.to_string()), tag, ..Default::default() }), None, credentials(auth));
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
    let (exposed, bound) = published(r)?;
    let net = attach(r)?;
    if let Some(f) = find(d, &r.workload_id).await? {
        if kept(&f, r) {
            if !f.up {
                d.start_container(&f.id, None::<q::StartContainerOptions>).await.context("starting the workload's container")?;
            }
            return Ok(json!({ "container_id": f.id, "created": false }));
        }
        // Made for another spec: made again.
        d.remove_container(&f.id, Some(q::RemoveContainerOptions { force: true, ..Default::default() })).await.context("removing the container of the old spec")?;
    }
    let pulled = pull(d, &r.image, r.auth.as_ref()).await;
    let here = pulled.is_err() && d.inspect_image(&r.image).await.is_ok();
    if after_pull(pulled, here)? {
        tracing::warn!(image = %r.image, "could not pull it; running the image already on this node");
    }
    if let Some(a) = &net {
        ensure_network(d, &a.stack, &r.address).await?;
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
            network_mode: net.as_ref().map(|a| a.network_mode.clone()),
            extra_hosts: net.as_ref().filter(|a| !a.hosts.is_empty()).map(|a| a.hosts.clone()),
            ..Default::default()
        }),
        networking_config: net.map(|a| a.config),
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

/// Removes the workload's container. Answers, besides, the stack and the overlay address it leaves on this node, if
/// it was on its stack's network: the others of the stack here may still hold its MAC for that address.
pub async fn remove(d: &Docker, r: &v1::RemoveContainer) -> Result<(Value, Option<(String, String)>)> {
    valid_workload(&r.workload_id)?;
    let Some(Found { id, .. }) = find(d, &r.workload_id).await? else { return Ok((json!({ "container_id": null }), None)) };
    let left = d.inspect_container(&id, None::<q::InspectContainerOptions>).await.ok().and_then(|c| left_behind(&c));
    d.remove_container(&id, Some(q::RemoveContainerOptions { force: true, ..Default::default() })).await.context("removing the container")?;
    Ok((json!({ "container_id": id }), left))
}

/// The stack and the address on its network of a container, if it was on its stack's network.
fn left_behind(c: &bollard::models::ContainerInspectResponse) -> Option<(String, String)> {
    let stack = c.config.as_ref()?.labels.as_ref()?.get(crate::net::LABEL_STACK)?.clone();
    let ip = c.network_settings.as_ref()?.networks.as_ref()?.get(&crate::net::network_name(&stack))?.ip_address.clone().filter(|ip| !ip.is_empty())?;
    Some((stack, ip))
}

/// Removes the stack's network from this node when no container of the stack is left here, running or not (a
/// stopped one starts on it again). Answers whether it was removed.
pub async fn drop_network_if_unused(d: &Docker, stack: &str) -> Result<bool> {
    let f = HashMap::from([("label".to_string(), vec![format!("{}={stack}", crate::net::LABEL_STACK), format!("{LABEL_MANAGED}=true")])]);
    if !d.list_containers(Some(q::ListContainersOptions { all: true, filters: Some(f), ..Default::default() })).await?.is_empty() {
        return Ok(false);
    }
    match d.remove_network(&crate::net::network_name(stack)).await {
        Ok(()) => Ok(true),
        Err(bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }) => Ok(false),
        Err(e) => Err(e).context("removing the stack's network"),
    }
}

/// The processes of the stack's containers running here, whose network namespaces hold their neighbours.
pub async fn stack_pids(d: &Docker, stack: &str) -> Vec<u32> {
    let f = HashMap::from([("label".to_string(), vec![format!("{}={stack}", crate::net::LABEL_STACK), format!("{LABEL_MANAGED}=true")]), ("status".to_string(), vec!["running".to_string()])]);
    let Ok(list) = d.list_containers(Some(q::ListContainersOptions { filters: Some(f), ..Default::default() })).await else { return vec![] };
    let mut pids = vec![];
    for c in list {
        if let Ok(i) = d.inspect_container(&c.id.unwrap_or_default(), None::<q::InspectContainerOptions>).await
            && let Some(pid) = i.state.and_then(|s| s.pid).filter(|p| *p > 0)
        {
            pids.push(pid as u32);
        }
    }
    pids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registry_login_goes_with_the_pull_as_docker_takes_it() {
        let a = |server: &str, user: &str| v1::RegistryAuth { server: server.into(), username: user.into(), password: "p".into() };
        let c = credentials(Some(&a("ghcr.io", "bot"))).unwrap();
        assert_eq!((c.username.as_deref(), c.password.as_deref(), c.serveraddress.as_deref()), (Some("bot"), Some("p"), Some("ghcr.io")));
        assert_eq!(credentials(Some(&a("docker.io", "bot"))).unwrap().serveraddress.as_deref(), Some("https://index.docker.io/v1/"));
        assert!(credentials(Some(&a("ghcr.io", ""))).is_none());
        assert!(credentials(None).is_none());
    }

    #[test]
    fn a_container_of_the_generation_is_kept_unless_it_runs_off_its_overlay_address() {
        let r = v1::RunContainer { generation: 3, network_stack: "app".into(), address: "10.240.1.5".into(), ..Default::default() };
        let f = |generation: u64, up: bool, at: &str| Found { id: "c".into(), up, generation, addresses: vec![at.to_string()] };
        assert!(kept(&f(3, true, "10.240.1.5"), &r));
        assert!(!kept(&f(2, true, "10.240.1.5"), &r), "another generation");
        assert!(!kept(&f(3, true, "172.17.0.3"), &r), "made before the overlay");
        assert!(kept(&f(3, false, ""), &r), "stopped: started, and seen once it runs");
        assert!(kept(&f(3, true, "172.17.0.3"), &v1::RunContainer { network_stack: String::new(), ..r }), "off the overlay by design");
    }

    #[test]
    fn a_running_container_s_addresses_are_those_of_its_networks() {
        use bollard::models::{ContainerSummary, ContainerSummaryNetworkSettings, ContainerSummaryStateEnum, EndpointSettings};
        let on = |ip: &str| EndpointSettings { ip_address: Some(ip.into()), ..Default::default() };
        let c = ContainerSummary {
            state: Some(ContainerSummaryStateEnum::RUNNING),
            network_settings: Some(ContainerSummaryNetworkSettings { networks: Some(HashMap::from([("fd-app".into(), on("10.240.1.5")), ("bridge".into(), on("172.17.0.3")), ("none".into(), on(""))])) }),
            ..Default::default()
        };
        assert_eq!(addresses_of(&c), vec!["10.240.1.5", "172.17.0.3"]);
        assert!(addresses_of(&ContainerSummary { state: Some(ContainerSummaryStateEnum::EXITED), ..c }).is_empty(), "only while it runs");
    }

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
    fn docker_publishes_the_ports_only_of_a_workload_off_the_overlay() {
        let p = vec![v1::PortMapping { host_port: 8088, container_port: 80, protocol: "tcp".into(), host_ip: String::new() }];
        let off = v1::RunContainer { workload_id: "w".into(), ports: p.clone(), ..Default::default() };
        let (exposed, bound) = published(&off).unwrap();
        assert!(exposed.contains_key("80/tcp") && bound.contains_key("80/tcp"));
        let on = v1::RunContainer { network_stack: "web".into(), address: "10.240.1.2".into(), ..off };
        let (exposed, bound) = published(&on).unwrap();
        assert!(exposed.is_empty() && bound.is_empty(), "the mesh answers it on every node (G10.2)");
        assert!(published(&v1::RunContainer { ports: vec![v1::PortMapping { container_port: 0, ..p[0].clone() }], ..on }).is_err(), "still checked");
    }

    #[test]
    fn a_workload_of_a_stack_joins_its_network_at_its_address_with_its_hosts() {
        assert!(attach(&v1::RunContainer { workload_id: "w".into(), ..Default::default() }).unwrap().is_none(), "no stack, Docker's default network");
        let r = v1::RunContainer { workload_id: "web.api.1".into(), network_stack: "web".into(), address: "10.240.3.2".into(), hosts: vec!["api:10.240.3.2".into(), "db:10.240.3.3".into()], ..Default::default() };
        let a = attach(&r).unwrap().unwrap();
        assert_eq!(a.network_mode, "fd-web");
        assert_eq!(a.hosts, vec!["api:10.240.3.2".to_string(), "db:10.240.3.3".to_string()]);
        let ep = &a.config.endpoints_config.as_ref().unwrap()["fd-web"];
        assert_eq!(ep.ipam_config.as_ref().unwrap().ipv4_address.as_deref(), Some("10.240.3.2"));
        assert!(attach(&v1::RunContainer { hosts: vec!["bad host:1.2.3.4".into()], ..r.clone() }).is_err());
        assert!(attach(&v1::RunContainer { network_stack: "../x".into(), ..r.clone() }).is_err());
        assert!(attach(&v1::RunContainer { address: "10.240.3.1".into(), ..r }).is_err(), "never the gateway");
    }

    #[test]
    fn a_container_on_its_stack_s_network_leaves_its_stack_and_address() {
        use bollard::models::{ContainerConfig, ContainerInspectResponse, EndpointSettings, NetworkSettings};
        let c = |labels: &[(&str, &str)], nets: &[(&str, &str)]| ContainerInspectResponse {
            config: Some(ContainerConfig { labels: Some(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()), ..Default::default() }),
            network_settings: Some(NetworkSettings { networks: Some(nets.iter().map(|(n, ip)| (n.to_string(), EndpointSettings { ip_address: Some(ip.to_string()), ..Default::default() })).collect()), ..Default::default() }),
            ..Default::default()
        };
        assert_eq!(left_behind(&c(&[("io.freightdock.stack", "g9")], &[("fd-g9", "10.240.5.3")])), Some(("g9".to_string(), "10.240.5.3".to_string())));
        assert_eq!(left_behind(&c(&[("io.freightdock.stack", "g9")], &[("bridge", "172.17.0.4")])), None, "off the overlay");
        assert_eq!(left_behind(&c(&[], &[("fd-g9", "10.240.5.3")])), None, "no stack's");
        assert_eq!(left_behind(&c(&[("io.freightdock.stack", "g9")], &[("fd-g9", "")])), None);
    }

    #[test]
    fn a_workload_id_is_a_label_value_safe_name() {
        assert!(valid_workload("web-1.a_b").is_ok());
        assert!(valid_workload("").is_err());
        assert!(valid_workload("a,b=c").is_err());
    }
}
