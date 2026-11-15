# freightdock-worker

The worker-agent of a Freightdock cluster. It enrols with a cluster join token (`FDCT1-...`), keeps one mutual-TLS
gRPC stream to any of the cluster's managers (and moves to another when that one goes), sends heartbeats, and runs
the commands it is given against the local Docker. It runs the node's overlay too (Freightdock-Network): a WireGuard
interface `fd0` to the other nodes, which needs `wg` (wireguard-tools) and `nft` (nftables) on the host. It decides
nothing cluster-wide itself.

Its contract is the **Freightdock-Contracts** repository (`freightdock-proto`); the managers are the **Freightdock-Manager** repository,
whose README has the install steps and whose ROADMAP.md has the plan.

```
src/lib.rs       settings, the files in the data folder, the order managers are dialled in
src/enrol.rs     the join token spent on a client certificate (the manager checked against the CA the token pins)
src/session.rs   the stream: hello, heartbeats, commands in, updates out; failover and backoff over the managers
src/exec.rs      the command executor, idempotent by command id, its journal on disk
src/docker.rs    Docker operations, workloads found by the label io.freightdock.workload
src/pages.rs     the node's Docker for a person (the UI's per-node pages): DockerRequest answered beside the
                 stream, never journalled; the cluster's own containers, networks and volumes never changed this way
src/host.rs      what it reports of its host: names, boot id, resources, capabilities
src/storage.rs   shared storage (G11): an NFS export mounted, a CephFS mounted by the kernel (mount(2), its key in the
                 call only), or a shared directory found, a volume's directory on it, unmounted when fencing (and
                 forgotten until mounted again); each one made ready probed every 15 s (mounted, space, a file written
                 and read back, a hung probe said after 10 s) and reported in the heartbeat (G11.8)
src/ceph.rs      CephCommand (G12.6): the `ceph` CLI once in a throwaway container of the Ceph image, its config and
                 key put into the container before it starts, never on the node's disk or a command line
src/net.rs       the overlay (G9): the WireGuard key (wg.key), the managers' network state applied by
                 Freightdock-Network's agent, a stack's Docker network (fd-<stack>) made when a workload of it runs
tests/e2e.rs     a real manager in process: enrolment, the session, commands
deploy/          systemd unit
```

| Variable | Default | What |
|---|---|---|
| `FREIGHTDOCK_MANAGERS` | (required) | Manager endpoints, `host[:port]`, comma separated; 7450 when no port is given |
| `FREIGHTDOCK_JOIN_TOKEN` | | The cluster join token, spent once on a certificate |
| `FREIGHTDOCK_JOIN_TOKEN_FILE` | | Read the token from this file instead: on a node that runs a manager too, the file it leaves (`FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN`) |
| `FREIGHTDOCK_WORKER_DATA` | `/var/lib/freightdock-worker` | Node id, key, certificate, cluster CA, managers learned, command journal, the WireGuard key |
| `FREIGHTDOCK_DOCKER` | `unix:///var/run/docker.sock` | The Docker socket |
| `FREIGHTDOCK_LABELS` | | Node labels, `key=value,key=value` |
| `FREIGHTDOCK_WORKER_HOST_SHELL` | `on` | `off`: no shell on this host from the UI (a container's is still offered); the managers cannot turn it back on |
| `RUST_LOG` | `freightdock_worker=info,warn` | How much it logs |

`freightdock-worker --version` and `--help` answer; any other argument is refused (exit 2), never taken as leave to
start a second worker as this node. An upgrade from the managers (the Upgrade command) fetches the release's binary
from the manager over the session, checks its size and SHA-256, writes it beside the running binary and renames it
over, then, under systemd only, exits so that systemd starts it again on the new one; its containers keep running.

While Contracts and Manager are not published, this crate takes them by path (`../Freightdock-Contracts`, and `../Freightdock-Manager` for
the end-to-end test), so it builds only with those checkouts beside it.

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Work here follows Freightdock's rules: test first, conventional commits, no em-dashes. GPL-3.0-only, as Freightdock.
