//! `freightdock-worker`: the worker-agent. It joins a cluster by dialling any of its managers (it never needs to know
//! which one leads), enrols once with a join token for a client certificate from the cluster CA, and from then on keeps
//! one long-lived gRPC stream open to one manager: hello, heartbeats every couple of seconds, and reports on the
//! commands it is given. When that manager goes away it dials the next one it knows of.
//!
//! It executes what the managers decide and decides nothing cluster-wide itself.

pub mod ceph;
pub mod cephadm;
pub mod docker;
pub mod enrol;
pub mod exec;
pub mod fence;
pub mod host;
pub mod net;
pub mod pages;
pub mod session;
pub mod storage;
pub mod tls;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use freightdock_proto::token::JoinToken;

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// The manager endpoints given at start (host:port). Those learned from the managers since are kept in the data
    /// folder and tried after these.
    pub managers: Vec<String>,
    pub token: Option<String>,
    /// Where to read the token when none is given: on a node that runs a manager too, the file that manager leaves
    /// for its local worker (`FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN`), so one node starts a whole cluster by itself.
    pub token_file: Option<PathBuf>,
    pub data_dir: PathBuf,
    /// The Docker socket.
    pub docker: PathBuf,
    pub labels: BTreeMap<String, String>,
}

impl Config {
    pub fn from(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let managers: Vec<String> = get("FREIGHTDOCK_MANAGERS").unwrap_or_default().split(',').map(str::trim).filter(|s| !s.is_empty()).map(freightdock_proto::with_default_port).collect();
        let token = get("FREIGHTDOCK_JOIN_TOKEN").map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
        if token.as_deref().is_some_and(|t| JoinToken::parse(t).is_none()) {
            bail!("FREIGHTDOCK_JOIN_TOKEN is not a cluster join token (FDCT1-...): make one with `freightdock-manager join-token`");
        }
        let token_file = get("FREIGHTDOCK_JOIN_TOKEN_FILE").map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).map(PathBuf::from);
        let data_dir = PathBuf::from(get("FREIGHTDOCK_WORKER_DATA").unwrap_or_else(|| "/var/lib/freightdock-worker".into()));
        let docker = get("FREIGHTDOCK_DOCKER").unwrap_or_else(|| "unix:///var/run/docker.sock".into());
        let docker = PathBuf::from(docker.strip_prefix("unix://").unwrap_or(&docker));
        let labels = parse_labels(&get("FREIGHTDOCK_LABELS").unwrap_or_default())?;
        Ok(Config { managers, token, token_file, data_dir, docker, labels })
    }
}

/// `region=north, storage=ssd` into a map.
pub fn parse_labels(s: &str) -> Result<BTreeMap<String, String>> {
    let mut m = BTreeMap::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some((k, v)) = part.split_once('=') else { bail!("label \"{part}\" is not key=value") };
        let k = k.trim();
        if k.is_empty() || !k.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-/".contains(&b)) {
            bail!("label key \"{k}\" may have only letters, digits and . _ - /");
        }
        m.insert(k.to_string(), v.trim().to_string());
    }
    Ok(m)
}

/// What the worker keeps in its data folder.
pub struct Files {
    pub node_id: PathBuf,
    pub key: PathBuf,
    pub cert: PathBuf,
    pub ca: PathBuf,
    /// The manager endpoints last learned from the cluster, one per line.
    pub managers: PathBuf,
    /// The hash of the join token last spent, so a token is not tried twice.
    pub spent: PathBuf,
}

impl Files {
    pub fn in_dir(dir: &Path) -> Self {
        Files { node_id: dir.join("node.id"), key: dir.join("key.pem"), cert: dir.join("cert.pem"), ca: dir.join("ca.pem"), managers: dir.join("managers"), spent: dir.join("token.sha256") }
    }

    /// Where a renewed key and certificate are kept, whole, until both are in place (G2.7).
    pub fn renewal(&self) -> PathBuf {
        self.key.with_file_name("renewal.json")
    }

    /// Keeps a renewed key and certificate: first both in one file (written aside and renamed, so it is there whole
    /// or not at all), then each in its place. Cut off between, the next start finishes it (`finish_renewal`).
    pub fn keep_renewed(&self, key_pem: &str, cert_pem: &str) -> Result<()> {
        let tmp = self.renewal().with_extension("json.tmp");
        write_private(&tmp, serde_json::json!({ "key_pem": key_pem, "cert_pem": cert_pem }).to_string().as_bytes())?;
        std::fs::rename(&tmp, self.renewal()).context("keeping the renewal")?;
        self.finish_renewal()
    }

    /// Puts a kept renewal's key and certificate in place, if there is one; doing it again does the same.
    pub fn finish_renewal(&self) -> Result<()> {
        #[derive(serde::Deserialize)]
        struct Kept {
            key_pem: String,
            cert_pem: String,
        }
        let Ok(text) = std::fs::read_to_string(self.renewal()) else { return Ok(()) };
        let k: Kept = serde_json::from_str(&text).context("the kept renewal")?;
        let put = |path: &Path, bytes: &str| -> Result<()> {
            let tmp = path.with_extension("pem.tmp");
            write_private(&tmp, bytes.as_bytes())?;
            std::fs::rename(&tmp, path).with_context(|| format!("putting {} in place", path.display()))
        };
        put(&self.key, &k.key_pem)?;
        put(&self.cert, &k.cert_pem)?;
        std::fs::remove_file(self.renewal()).context("done with the renewal")?;
        tracing::info!("the renewed certificate is in place");
        Ok(())
    }

    pub fn enrolled(&self) -> bool {
        self.key.exists() && self.cert.exists() && self.ca.exists()
    }

    /// This node's UUID, made on the first start and kept: it is the node's identity for its whole life.
    pub fn node_id(&self) -> Result<String> {
        if let Ok(s) = std::fs::read_to_string(&self.node_id)
            && let Ok(u) = uuid::Uuid::parse_str(s.trim())
        {
            return Ok(u.to_string());
        }
        let id = uuid::Uuid::new_v4().to_string();
        std::fs::write(&self.node_id, &id).context("keeping the node id")?;
        Ok(id)
    }

    pub fn known_managers(&self) -> Vec<String> {
        std::fs::read_to_string(&self.managers).unwrap_or_default().lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_string).collect()
    }

    pub fn keep_managers(&self, list: &[String]) {
        if !list.is_empty() && list != self.known_managers().as_slice() {
            let _ = std::fs::write(&self.managers, list.join("\n") + "\n");
        }
    }
}

/// The endpoints to try, in order: those given at start, then those the cluster told us of, each once.
pub fn dial_order(given: &[String], learned: &[String]) -> Vec<String> {
    let mut v: Vec<String> = Vec::new();
    for m in given.iter().chain(learned) {
        if !v.contains(m) {
            v.push(m.clone());
        }
    }
    v
}

pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    o.mode(0o600);
    let mut f = o.open(path).with_context(|| format!("writing {}", path.display()))?;
    f.write_all(bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_renewal_is_kept_whole_or_not_at_all_and_finished_at_the_next_start() {
        let d = tempfile::tempdir().unwrap();
        let f = Files::in_dir(d.path());
        std::fs::write(&f.key, "old key").unwrap();
        std::fs::write(&f.cert, "old cert").unwrap();
        f.keep_renewed("new key", "new cert").unwrap();
        assert_eq!((std::fs::read_to_string(&f.key).unwrap(), std::fs::read_to_string(&f.cert).unwrap()), ("new key".into(), "new cert".into()));
        assert!(!f.renewal().exists(), "done with");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&f.key).unwrap().permissions().mode() & 0o777, 0o600, "the key only root reads");
        }
        // Cut off after the renewal was kept, before both were in place: the next start puts them there.
        std::fs::write(f.renewal(), serde_json::json!({ "key_pem": "k3", "cert_pem": "c3" }).to_string()).unwrap();
        std::fs::write(&f.cert, "c3").unwrap();
        f.finish_renewal().unwrap();
        assert_eq!((std::fs::read_to_string(&f.key).unwrap(), std::fs::read_to_string(&f.cert).unwrap()), ("k3".into(), "c3".into()));
        // Cut off while it was written: nothing is taken from half a file.
        std::fs::write(d.path().join("renewal.json.tmp"), "{\"key_pem\": \"k4").unwrap();
        f.finish_renewal().unwrap();
        assert_eq!(std::fs::read_to_string(&f.key).unwrap(), "k3");
    }

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
    }

    #[test]
    fn the_config_reads_the_environment() {
        let c = Config::from(env(&[("FREIGHTDOCK_MANAGERS", "192.0.2.1, 192.0.2.2:9000,"), ("FREIGHTDOCK_LABELS", "region=north,gpu=true")])).unwrap();
        assert_eq!(c.managers, vec!["192.0.2.1:7450", "192.0.2.2:9000"]);
        assert_eq!(c.labels.get("gpu").map(String::as_str), Some("true"));
        assert_eq!(c.docker, PathBuf::from("/var/run/docker.sock"));
        assert!(Config::from(env(&[("FREIGHTDOCK_JOIN_TOKEN", "FDJT1-abc")])).is_err(), "the single-host agent's token is not this one");
    }

    #[test]
    fn labels_are_key_value() {
        assert!(parse_labels("a=b, c = d ").unwrap().len() == 2);
        assert!(parse_labels("nonsense").is_err());
        assert!(parse_labels("bad key=x").is_err());
        assert!(parse_labels("").unwrap().is_empty());
    }

    #[test]
    fn given_managers_come_first_and_none_twice() {
        let given = vec!["a:1".to_string(), "b:1".to_string()];
        let learned = vec!["b:1".to_string(), "c:1".to_string()];
        assert_eq!(dial_order(&given, &learned), vec!["a:1", "b:1", "c:1"]);
    }

    #[test]
    fn the_node_id_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let f = Files::in_dir(dir.path());
        let a = f.node_id().unwrap();
        assert_eq!(f.node_id().unwrap(), a);
        f.keep_managers(&["x:1".into(), "y:1".into()]);
        assert_eq!(f.known_managers(), vec!["x:1", "y:1"]);
    }
}
