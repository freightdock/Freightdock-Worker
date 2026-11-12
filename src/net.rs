//! The node's overlay (G9): the WireGuard key made here and kept in the data folder, the managers' `NetworkState`
//! applied by Freightdock-Network's agent, and a stack's Docker network made the first time a workload of it runs
//! here. What the hello and every heartbeat say of it is `report`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};
use bollard::models::{Ipam, IpamConfig, NetworkCreateRequest};

use freightdock_network::apply::{Agent, System, WG_PORT};
use freightdock_network::key::Key;
use freightdock_proto::v1;

/// What a stack's network is called in Docker.
pub fn network_name(stack: &str) -> String {
    format!("fd-{stack}")
}

/// The stack's bridge (Freightdock-Network's name for it, so the agent's rules and Docker's bridge agree).
pub use freightdock_network::plan::bridge_name;

/// The stack's /24 and its gateway, from a workload's address in it (the managers give each stack a /24 with the
/// gateway at `.1`, and workloads from `.2`).
pub fn stack_net(address: &str) -> Result<(String, String)> {
    let Ok(ip) = address.parse::<std::net::Ipv4Addr>() else { bail!("{address:?} is not an IPv4 address") };
    let [a, b, c, d] = ip.octets();
    if !(2..=254).contains(&d) {
        bail!("{address} is not a workload's address");
    }
    Ok((format!("{a}.{b}.{c}.0/24"), format!("{a}.{b}.{c}.1")))
}

/// `name:address`, as the managers write a service's name for the hosts file.
pub fn valid_host(h: &str) -> bool {
    h.split_once(':').is_some_and(|(n, a)| !n.is_empty() && n.len() <= 253 && n.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)) && a.parse::<std::net::Ipv4Addr>().is_ok())
}

/// What Docker is asked for a stack's network: a bridge with the stack's /24, no protection against routed traffic
/// and no masquerade of its own (the overlay brings it, the agent's table masquerades), the overlay's MTU.
pub fn network_request(stack: &str, address: &str, mtu: u32) -> Result<NetworkCreateRequest> {
    let (subnet, gateway) = stack_net(address)?;
    let options = HashMap::from([
        ("com.docker.network.bridge.name".to_string(), bridge_name(stack)),
        ("com.docker.network.bridge.gateway_mode_ipv4".to_string(), "nat-unprotected".to_string()),
        ("com.docker.network.bridge.enable_ip_masquerade".to_string(), "false".to_string()),
        ("com.docker.network.driver.mtu".to_string(), mtu.to_string()),
    ]);
    let labels = HashMap::from([(crate::docker::LABEL_MANAGED.to_string(), "true".to_string()), (LABEL_STACK.to_string(), stack.to_string())]);
    Ok(NetworkCreateRequest {
        name: network_name(stack),
        driver: Some("bridge".into()),
        ipam: Some(Ipam { driver: Some("default".into()), config: Some(vec![IpamConfig { subnet: Some(subnet), gateway: Some(gateway), ..Default::default() }]), options: None }),
        options: Some(options),
        labels: Some(labels),
        ..Default::default()
    })
}

pub const LABEL_STACK: &str = "io.freightdock.stack";
/// The overlay's MTU, as the managers send it; a stack network made before the first state uses it too.
pub const MTU: u32 = 1420;

/// The overlay on this node.
pub struct Net {
    public: String,
    agent: Mutex<Agent<System>>,
    /// The version applied and why the last one could not be, if it could not.
    status: Mutex<(u64, String)>,
}

impl Net {
    /// The node's key in `dir/wg.key`, made the first time.
    pub fn open(dir: &Path) -> Result<Arc<Self>> {
        let path = dir.join("wg.key");
        let key = Key::load_or_make(&path)?;
        Ok(Arc::new(Net { public: key.public(), agent: Mutex::new(Agent::new(System, path)), status: Mutex::default() }))
    }

    pub fn report(&self) -> v1::NetworkReport {
        let (applied_version, error) = self.status.lock().expect("net status").clone();
        v1::NetworkReport { public_key: self.public.clone(), listen_port: WG_PORT as u32, applied_version, error }
    }

    /// Makes the node's overlay what `s` says; a state older than the one applied is ignored. What went wrong is
    /// kept for the report and logged, never fatal: the workloads run on, only the overlay waits.
    pub async fn apply(self: Arc<Self>, s: v1::NetworkState) {
        let me = self.clone();
        let r = tokio::task::spawn_blocking(move || {
            if s.version < me.status.lock().expect("net status").0 {
                return Ok(None);
            }
            let mut a = me.agent.lock().expect("net agent");
            let changes = a.apply(&s)?;
            for st in &s.stacks {
                // The bridge is there once a workload of the stack has run here.
                if std::path::Path::new("/sys/class/net").join(bridge_name(&st.stack)).exists() {
                    a.proxy_arp(&bridge_name(&st.stack))?;
                }
            }
            Ok::<_, anyhow::Error>(Some((s.version, changes.len())))
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|r| r);
        let mut st = self.status.lock().expect("net status");
        match r {
            Ok(Some((v, n))) => {
                if n > 0 {
                    tracing::info!(version = v, changes = n, "the overlay applied");
                }
                *st = (v, String::new());
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("the overlay: {e:#}");
                st.1 = format!("{e:#}");
            }
        }
    }

    /// After a container of `stack` at `addr` left this node: the stack's containers still here forget the MAC they
    /// knew for `addr`, so they ask again and the host answers for its new node at once.
    pub async fn forget(self: Arc<Self>, pids: Vec<u32>, addr: String) {
        let r = tokio::task::spawn_blocking(move || {
            let mut a = self.agent.lock().expect("net agent");
            pids.iter().try_for_each(|p| a.forget_neighbour(*p, &addr))
        })
        .await;
        if let Ok(Err(e)) | Err(e) = r.map_err(anyhow::Error::from) {
            tracing::warn!("forgetting a moved address's neighbour: {e:#}");
        }
    }

    /// Takes the floating addresses off the node (it fenced itself; the managers move them once it is fenced).
    pub async fn drop_floating(self: Arc<Self>) {
        let r = tokio::task::spawn_blocking(move || self.agent.lock().expect("net agent").drop_floating()).await;
        if let Ok(Err(e)) | Err(e) = r.map_err(anyhow::Error::from) {
            tracing::error!("fence: the floating addresses could not be taken off: {e:#}");
        }
    }

    /// Proxy ARP on a stack's bridge, once Docker made it.
    pub async fn bridge_ready(self: Arc<Self>, stack: String) -> Result<()> {
        tokio::task::spawn_blocking(move || self.agent.lock().expect("net agent").proxy_arp(&bridge_name(&stack))).await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stack_s_subnet_and_gateway_come_from_a_workload_s_address() {
        assert_eq!(stack_net("10.240.3.7").unwrap(), ("10.240.3.0/24".to_string(), "10.240.3.1".to_string()));
        assert!(stack_net("10.240.3.1").is_err(), "the gateway is nobody's");
        assert!(stack_net("10.240.3.255").is_err());
        assert!(stack_net("10.240.3.0").is_err());
        assert!(stack_net("web").is_err());
    }

    #[test]
    fn docker_s_network_name_is_whole_and_its_bridge_fits_linux() {
        assert_eq!(network_name("a-rather-long-stack-name"), "fd-a-rather-long-stack-name");
        assert!(bridge_name("a-rather-long-stack-name").len() <= 15);
    }

    #[test]
    fn a_stack_network_is_asked_for_as_the_overlay_needs_it() {
        let r = network_request("web", "10.240.3.7", 1420).unwrap();
        assert_eq!(r.name, "fd-web");
        let ipam = &r.ipam.as_ref().unwrap().config.as_ref().unwrap()[0];
        assert_eq!((ipam.subnet.as_deref(), ipam.gateway.as_deref()), (Some("10.240.3.0/24"), Some("10.240.3.1")));
        let o = r.options.as_ref().unwrap();
        assert_eq!(o["com.docker.network.bridge.gateway_mode_ipv4"], "nat-unprotected");
        assert_eq!(o["com.docker.network.bridge.enable_ip_masquerade"], "false");
        assert_eq!(o["com.docker.network.driver.mtu"], "1420");
        assert_eq!(o["com.docker.network.bridge.name"], "fd-web");
        assert_eq!(r.labels.as_ref().unwrap()[LABEL_STACK], "web");
        assert!(network_request("web", "nope", 1420).is_err());
    }

    #[test]
    fn hosts_are_a_name_and_an_address() {
        assert!(valid_host("web:10.240.3.2"));
        assert!(valid_host("web-1:10.240.3.2"));
        assert!(!valid_host("web 1:10.240.3.2"));
        assert!(!valid_host("web:somewhere"));
        assert!(!valid_host(":10.240.3.2"));
    }

    #[tokio::test]
    async fn the_report_has_the_key_made_here_and_nothing_applied_yet() {
        let d = tempfile::tempdir().unwrap();
        let n = Net::open(d.path()).unwrap();
        let r = n.report();
        assert_eq!((r.listen_port, r.applied_version, r.error.as_str()), (51820, 0, ""));
        assert_eq!(r.public_key, Net::open(d.path()).unwrap().report().public_key, "the same key after a restart");
    }
}
