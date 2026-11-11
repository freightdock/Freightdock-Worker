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

/// The container of `workload`, if this worker has one: (id, running).
async fn find(d: &Docker, workload: &str) -> Result<Option<(String, bool)>> {
    let f = HashMap::from([("label".to_string(), vec![format!("{LABEL_WORKLOAD}={workload}")])]);
    let l = d.list_containers(Some(q::ListContainersOptions { all: true, filters: Some(f), ..Default::default() })).await?;
    Ok(l.into_iter().next().map(|c| (c.id.unwrap_or_default(), c.state == Some(bollard::models::ContainerSummaryStateEnum::RUNNING))))
}

fn valid_workload(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 128 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)) {
        return Err(anyhow!("the workload id must be 1 to 128 of letters, digits and . _ -"));
    }
    Ok(())
}

pub async fn run(d: &Docker, r: &v1::RunContainer) -> Result<Value> {
    valid_workload(&r.workload_id)?;
    if let Some((id, up)) = find(d, &r.workload_id).await? {
        if !up {
            d.start_container(&id, None::<q::StartContainerOptions>).await.context("starting the workload's container")?;
        }
        return Ok(json!({ "container_id": id, "created": false }));
    }
    let (from_image, tag) = split_tag(&r.image);
    let mut pull = d.create_image(Some(q::CreateImageOptions { from_image: Some(from_image.to_string()), tag: Some(tag.to_string()), ..Default::default() }), None, None);
    while let Some(step) = pull.next().await {
        let step = step.with_context(|| format!("pulling {}", r.image))?;
        if let Some(e) = step.error_detail.and_then(|e| e.message) {
            return Err(anyhow!("pulling {}: {e}", r.image));
        }
    }
    let mut labels = r.labels.clone();
    labels.insert(LABEL_MANAGED.into(), "true".into());
    labels.insert(LABEL_WORKLOAD.into(), r.workload_id.clone());
    let body = ContainerCreateBody {
        image: Some(r.image.clone()),
        cmd: (!r.command.is_empty()).then(|| r.command.clone()),
        env: Some(r.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
        labels: Some(labels),
        // Never restarted by Docker on its own: a managed workload restarts only when the cluster says so, which is
        // what keeps a fenced worker from bringing its containers back after a reboot.
        host_config: Some(HostConfig { restart_policy: Some(RestartPolicy { name: Some(RestartPolicyNameEnum::NO), maximum_retry_count: None }), ..Default::default() }),
        ..Default::default()
    };
    let name = (!r.name.is_empty()).then(|| r.name.clone());
    let made = d.create_container(Some(q::CreateContainerOptions { name, ..Default::default() }), body).await.context("creating the container")?;
    d.start_container(&made.id, None::<q::StartContainerOptions>).await.context("starting the container")?;
    Ok(json!({ "container_id": made.id, "created": true }))
}

pub async fn stop(d: &Docker, s: &v1::StopContainer) -> Result<Value> {
    valid_workload(&s.workload_id)?;
    let Some((id, up)) = find(d, &s.workload_id).await? else { return Ok(json!({ "container_id": null })) };
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
    let Some((id, _)) = find(d, &r.workload_id).await? else { return Ok(json!({ "container_id": null })) };
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
    fn a_workload_id_is_a_label_value_safe_name() {
        assert!(valid_workload("web-1.a_b").is_ok());
        assert!(valid_workload("").is_err());
        assert!(valid_workload("a,b=c").is_err());
    }
}
