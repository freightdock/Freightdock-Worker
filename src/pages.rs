//! The node's Docker for a person (the UI's per-node pages): `DockerRequest` in, `DockerReply` out. Not a command,
//! never journalled: a read is answered now and a change is done once, by whoever asked. The cluster's own things
//! (containers, volumes, networks labelled `io.freightdock.managed`) are never changed this way: a workload belongs
//! to its stack, and the next reconcile would undo it anyway.

use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use bollard::Docker;
use bollard::query_parameters as q;
use futures_util::StreamExt;
use serde_json::{Value, json};

use freightdock_proto::v1::{DockerReply, DockerRequest};

use crate::docker::{LABEL_MANAGED, LABEL_WORKLOAD};
use crate::net::LABEL_STACK;

/// The most log lines one read gives, and the most bytes of them.
const MAX_TAIL: u32 = 5000;
const DEFAULT_TAIL: u32 = 500;
const MAX_LOG_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Info,
    Containers,
    Container,
    Logs,
    Images,
    Volumes,
    Networks,
    Start,
    Stop,
    Restart,
    RemoveContainer,
    RemoveImage,
    PullImage,
    PruneImages,
    RemoveVolume,
    RemoveNetwork,
}

/// What a target names, to check it before it goes into a Docker API path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    None,
    Object,
    Image,
}

impl Action {
    pub fn parse(s: &str) -> Option<Action> {
        use Action::*;
        Some(match s {
            "info" => Info,
            "containers" => Containers,
            "container" => Container,
            "logs" => Logs,
            "images" => Images,
            "volumes" => Volumes,
            "networks" => Networks,
            "container.start" => Start,
            "container.stop" => Stop,
            "container.restart" => Restart,
            "container.remove" => RemoveContainer,
            "image.remove" => RemoveImage,
            "image.pull" => PullImage,
            "images.prune" => PruneImages,
            "volume.remove" => RemoveVolume,
            "network.remove" => RemoveNetwork,
            _ => return None,
        })
    }

    fn target(self) -> Kind {
        use Action::*;
        match self {
            Info | Containers | Images | Volumes | Networks | PruneImages | PullImage => Kind::None,
            RemoveImage => Kind::Image,
            _ => Kind::Object,
        }
    }
}

/// A container's, volume's or network's id or name: letters, digits, `_.-`. An image: also `/:@`. Never `..`, so a
/// target cannot reach another Docker endpoint through the path it is put in.
fn valid(kind: Kind, t: &str) -> bool {
    let extra: &[u8] = match kind {
        Kind::None => return true,
        Kind::Object => b"_.-",
        Kind::Image => b"_.-/:@",
    };
    !t.is_empty() && t.len() <= 255 && !t.contains("..") && t.as_bytes()[0].is_ascii_alphanumeric() && t.bytes().all(|b| b.is_ascii_alphanumeric() || extra.contains(&b))
}

/// The cluster's own: made by a worker for a workload, a stack's network or a volume on the cluster's storage.
pub fn cluster_s(labels: Option<&HashMap<String, String>>) -> bool {
    labels.is_some_and(|l| l.get(LABEL_MANAGED).is_some_and(|v| v == "true") || l.contains_key(LABEL_WORKLOAD))
}

/// How many lines of log to read: `tail` from the request, 1 to 5000, 500 when not given.
pub fn tail(params: &HashMap<String, String>) -> u32 {
    params.get("tail").and_then(|t| t.parse().ok()).unwrap_or(DEFAULT_TAIL).clamp(1, MAX_TAIL)
}

/// The log's lines, the newest kept when they pass the byte limit: `{lines, cut}`.
pub fn lines(chunks: Vec<String>) -> Value {
    let text: String = chunks.concat();
    let mut all: Vec<&str> = text.lines().collect();
    let mut size: usize = all.iter().map(|l| l.len() + 1).sum();
    let mut cut = false;
    while size > MAX_LOG_BYTES && !all.is_empty() {
        size -= all[0].len() + 1;
        all.remove(0);
        cut = true;
    }
    json!({ "lines": all, "cut": cut })
}

/// Answers one request. Never fails: what went wrong is the reply's `error`.
pub async fn answer(docker: Option<&Docker>, r: &DockerRequest) -> DockerReply {
    let out = match docker {
        None => Err(anyhow!("this worker has no Docker")),
        Some(d) => run(d, r).await,
    };
    match out {
        Ok(v) => DockerReply { request_id: r.request_id.clone(), error: String::new(), json: v.to_string() },
        Err(e) => DockerReply { request_id: r.request_id.clone(), error: format!("{e:#}"), json: String::new() },
    }
}

async fn run(d: &Docker, r: &DockerRequest) -> Result<Value> {
    let a = Action::parse(&r.action).ok_or_else(|| anyhow!("{:?} is not something a worker does with its Docker", r.action))?;
    let t = r.target.as_str();
    if !valid(a.target(), t) {
        bail!("{t:?} is not a name or id Docker gives");
    }
    let force = r.params.get("force").is_some_and(|v| v == "true");
    use Action::*;
    Ok(match a {
        Info => crate::docker::info(d).await?,
        Containers => {
            let list = d.list_containers(Some(q::ListContainersOptions { all: true, ..Default::default() })).await?;
            Value::Array(list.into_iter().map(|c| {
                let labels = c.labels.clone().unwrap_or_default();
                json!({
                    "id": c.id, "names": c.names.unwrap_or_default().into_iter().map(|n| n.trim_start_matches('/').to_string()).collect::<Vec<_>>(),
                    "image": c.image, "state": c.state.map(|s| s.to_string()), "status": c.status, "created": c.created,
                    "ports": c.ports.unwrap_or_default().into_iter().map(|p| json!({ "ip": p.ip, "private": p.private_port, "public": p.public_port, "type": p.typ.map(|t| t.to_string()) })).collect::<Vec<_>>(),
                    "workload": labels.get(LABEL_WORKLOAD), "stack": labels.get(LABEL_STACK), "cluster": cluster_s(Some(&labels)),
                })
            }).collect())
        }
        Container => serde_json::to_value(d.inspect_container(t, None::<q::InspectContainerOptions>).await?)?,
        Logs => {
            let opts = q::LogsOptions { stdout: true, stderr: true, tail: tail(&r.params).to_string(), timestamps: r.params.get("timestamps").is_some_and(|v| v == "true"), ..Default::default() };
            let mut s = d.logs(t, Some(opts));
            let mut chunks = Vec::new();
            while let Some(c) = s.next().await {
                chunks.push(c?.to_string());
            }
            lines(chunks)
        }
        Images => {
            let list = d.list_images(Some(q::ListImagesOptions { all: false, ..Default::default() })).await?;
            Value::Array(list.into_iter().map(|i| json!({ "id": i.id, "tags": i.repo_tags, "size": i.size, "created": i.created, "containers": i.containers })).collect())
        }
        Volumes => {
            let list = d.list_volumes(None::<q::ListVolumesOptions>).await?.volumes.unwrap_or_default();
            Value::Array(list.into_iter().map(|v| json!({ "name": v.name, "driver": v.driver, "mountpoint": v.mountpoint, "created": v.created_at, "cluster": cluster_s(Some(&v.labels)), "stack": v.labels.get(LABEL_STACK) })).collect())
        }
        Networks => {
            let list = d.list_networks(None::<q::ListNetworksOptions>).await?;
            Value::Array(list.into_iter().map(|n| {
                let labels = n.labels.clone().unwrap_or_default();
                json!({ "id": n.id, "name": n.name, "driver": n.driver, "scope": n.scope, "internal": n.internal, "created": n.created, "cluster": cluster_s(Some(&labels)), "stack": labels.get(LABEL_STACK) })
            }).collect())
        }
        Start | Stop | Restart | RemoveContainer => {
            let c = d.inspect_container(t, None::<q::InspectContainerOptions>).await?;
            if cluster_s(c.config.as_ref().and_then(|c| c.labels.as_ref())) {
                bail!("this container is a workload of the cluster: change it through its stack");
            }
            let id = c.id.unwrap_or_else(|| t.to_string());
            match a {
                Start => match d.start_container(&id, None::<q::StartContainerOptions>).await {
                    Ok(()) | Err(bollard::errors::Error::DockerResponseServerError { status_code: 304, .. }) => {}
                    Err(e) => return Err(e.into()),
                },
                Stop => match d.stop_container(&id, Some(q::StopContainerOptions { t: Some(10), signal: None })).await {
                    Ok(()) | Err(bollard::errors::Error::DockerResponseServerError { status_code: 304, .. }) => {}
                    Err(e) => return Err(e.into()),
                },
                Restart => d.restart_container(&id, Some(q::RestartContainerOptions { t: Some(10), signal: None })).await?,
                _ => d.remove_container(&id, Some(q::RemoveContainerOptions { force, ..Default::default() })).await?,
            }
            json!({ "id": id })
        }
        RemoveImage => {
            let gone = d.remove_image(t, Some(q::RemoveImageOptions { force, noprune: false }), None).await?;
            json!({ "removed": gone.len() })
        }
        PullImage => {
            let image = r.params.get("image").map(String::as_str).unwrap_or("");
            if !valid(Kind::Image, image) {
                bail!("{image:?} is not an image name");
            }
            let (repo, tag) = crate::docker::split_tag(image);
            let mut s = d.create_image(Some(q::CreateImageOptions { from_image: Some(repo.to_string()), tag: (!tag.is_empty()).then(|| tag.to_string()), ..Default::default() }), None, None);
            while let Some(p) = s.next().await {
                p?;
            }
            json!({ "image": image })
        }
        PruneImages => {
            let p = d.prune_images(None::<q::PruneImagesOptions>).await?;
            json!({ "removed": p.images_deleted.map_or(0, |v| v.len()), "space": p.space_reclaimed })
        }
        RemoveVolume => {
            let v = d.inspect_volume(t).await?;
            if cluster_s(Some(&v.labels)) {
                bail!("this volume is the cluster's: it goes with its stack");
            }
            d.remove_volume(t, Some(q::RemoveVolumeOptions { force: false })).await?;
            json!({ "name": t })
        }
        RemoveNetwork => {
            let n = d.inspect_network(t, None::<q::InspectNetworkOptions>).await?;
            if cluster_s(n.labels.as_ref()) {
                bail!("this network is the cluster's: it goes with its stack");
            }
            if matches!(n.name.as_deref(), Some("bridge" | "host" | "none")) {
                bail!("Docker's own networks stay");
            }
            d.remove_network(t).await?;
            json!({ "name": t })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_actions_named_are_taken() {
        assert_eq!(Action::parse("container.restart"), Some(Action::Restart));
        assert_eq!(Action::parse("images.prune"), Some(Action::PruneImages));
        for bad in ["exec", "container.exec", "system.prune", ""] {
            assert_eq!(Action::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_target_never_leaves_its_docker_path() {
        assert!(valid(Kind::Object, "freightdock-ui-1"));
        assert!(valid(Kind::Object, "4f1c2d3e"));
        assert!(valid(Kind::Image, "ghcr.io/a/b:1.2"));
        assert!(valid(Kind::Image, "sha256:abcdef"));
        for bad in ["", "../images/x", "a/../../b", "x?force=1", "-rf", "a b", "a/b"] {
            assert!(!valid(Kind::Object, bad), "{bad}");
        }
        assert!(!valid(Kind::Image, "a/../../containers/x/kill"));
        assert!(!valid(Kind::Image, "x#y"));
    }

    #[test]
    fn the_cluster_s_own_are_known_by_their_labels() {
        let l = |k: &str, v: &str| HashMap::from([(k.to_string(), v.to_string())]);
        assert!(cluster_s(Some(&l(LABEL_MANAGED, "true"))));
        assert!(cluster_s(Some(&l(LABEL_WORKLOAD, "web.nginx.1"))));
        assert!(!cluster_s(Some(&l("com.docker.compose.project", "x"))));
        assert!(!cluster_s(None));
    }

    #[test]
    fn a_log_read_is_bounded() {
        assert_eq!(tail(&HashMap::new()), 500);
        assert_eq!(tail(&HashMap::from([("tail".to_string(), "999999".to_string())])), 5000);
        assert_eq!(tail(&HashMap::from([("tail".to_string(), "0".to_string())])), 1);
        let v = lines(vec!["a\nb\n".into(), "c\n".into()]);
        assert_eq!((v["lines"].clone(), v["cut"].clone()), (json!(["a", "b", "c"]), json!(false)));
        let big = vec![format!("{}\n", "x".repeat(1000)); 2000];
        let v = lines(big);
        assert!(v["cut"].as_bool().unwrap());
        assert!(v["lines"].as_array().unwrap().len() < 2000);
    }

    #[tokio::test]
    async fn without_docker_the_reply_says_so() {
        let r = answer(None, &DockerRequest { request_id: "r1".into(), action: "containers".into(), ..Default::default() }).await;
        assert_eq!((r.request_id.as_str(), r.error.as_str(), r.json.as_str()), ("r1", "this worker has no Docker", ""));
    }
}
