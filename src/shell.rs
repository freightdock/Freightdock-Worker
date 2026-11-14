//! Shells for a person (the UI's terminal): on a ShellOpen from the manager this worker's session is with, a stream of
//! its own to that manager (WorkerSession.Shell), and on it either a login shell as root on the host, on a terminal of
//! its own, or `docker exec` with a terminal in a container. What is typed comes down, what the shell writes goes up;
//! either end going away ends the other (the host shell's whole session is hung up). Nothing typed or written is kept
//! or logged here. At most `MAX` at once; the host's own can be turned off on the node (`FREIGHTDOCK_WORKER_HOST_SHELL=off`).

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use freightdock_proto::v1::{ShellDown, ShellEnd, ShellOpen, ShellUp, shell_down, shell_up, worker_session_client::WorkerSessionClient};

/// Shells open at once on one node.
pub const MAX: usize = 8;
/// What is read from a shell at a time, and so the most one ShellUp carries.
const CHUNK: usize = 16 * 1024;

/// The shells open now, and whether the host's own may be opened.
pub struct Shells {
    pub host: bool,
    open: AtomicUsize,
}

impl Shells {
    pub fn new(host: bool) -> Arc<Self> {
        Arc::new(Shells { host, open: AtomicUsize::new(0) })
    }

    /// A place for one more, or None when `MAX` are open.
    fn take(self: &Arc<Self>) -> Option<Slot> {
        let mut n = self.open.load(Ordering::SeqCst);
        loop {
            if n >= MAX {
                return None;
            }
            match self.open.compare_exchange(n, n + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return Some(Slot(self.clone())),
                Err(now) => n = now,
            }
        }
    }
}

struct Slot(Arc<Shells>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A terminal size as Docker and the kernel take it: 1 to 1000 each way, 80x24 when not given.
pub fn size(cols: u32, rows: u32) -> (u16, u16) {
    let one = |v: u32, d: u16| if v == 0 { d } else { v.clamp(1, 1000) as u16 };
    (one(cols, 80), one(rows, 24))
}

/// The container's shell: bash where it has one, else sh, as a login shell.
pub fn container_command() -> Vec<String> {
    ["/bin/sh", "-c", "if command -v bash >/dev/null 2>&1; then exec bash -l; else exec sh -l; fi"].map(String::from).to_vec()
}

/// root's login shell as /etc/passwd names it, else /bin/sh.
pub fn root_shell(passwd: &str) -> String {
    passwd
        .lines()
        .find_map(|l| {
            let f: Vec<&str> = l.split(':').collect();
            (f.len() >= 7 && f[0] == "root" && f[6].starts_with('/')).then(|| f[6].trim().to_string())
        })
        .unwrap_or_else(|| "/bin/sh".into())
}

/// A process on a terminal of its own: the pty's master end, and the child leading its own session on the other.
pub struct Pty {
    master: OwnedFd,
    pub child: std::process::Child,
}

impl Pty {
    /// Starts `program` (with `argv0` as its name, `-bash` for a login shell) on a new terminal of `cols` by `rows`.
    pub fn spawn(program: &str, argv0: &str, args: &[&str], env: &[(&str, &str)], dir: &str, cols: u16, rows: u16) -> Result<Pty> {
        let (mut master, mut slave) = (-1, -1);
        let ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
        // SAFETY: openpty fills the two descriptors it is given; the name and termios are not asked for.
        if unsafe { libc::openpty(&mut master, &mut slave, std::ptr::null_mut(), std::ptr::null(), &ws) } != 0 {
            bail!("making a terminal: {}", std::io::Error::last_os_error());
        }
        // SAFETY: both are open descriptors that nothing else owns.
        let (master, slave) = unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
        let mut cmd = std::process::Command::new(program);
        cmd.arg0(argv0).args(args).env_clear().current_dir(dir);
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.stdin(slave.try_clone()?).stdout(slave.try_clone()?).stderr(slave);
        // SAFETY: only async-signal-safe calls between fork and exec: a new session, with the terminal as its own.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = cmd.spawn().with_context(|| format!("starting {program}"))?;
        Ok(Pty { master, child })
    }

    /// The terminal's new size.
    pub fn resize(&self, cols: u16, rows: u16) {
        let ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
        // SAFETY: TIOCSWINSZ reads the winsize given; the descriptor is ours and open.
        unsafe { libc::ioctl(self.master.as_raw_fd(), libc::TIOCSWINSZ as _, &ws) };
    }

    /// The master end to read and write, as tokio files (each its own descriptor).
    pub fn ends(&self) -> Result<(tokio::fs::File, tokio::fs::File)> {
        Ok((tokio::fs::File::from_std(std::fs::File::from(self.master.try_clone()?)), tokio::fs::File::from_std(std::fs::File::from(self.master.try_clone()?))))
    }

    /// Hangs up the whole session the shell leads (whatever it started too), then kills what is left.
    pub fn hang_up(&mut self) {
        let pid = self.child.id() as i32;
        // SAFETY: signals to the process group the child leads (setsid made its id the group's).
        unsafe { libc::kill(-pid, libc::SIGHUP) };
        for _ in 0..20 {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        unsafe { libc::kill(-pid, libc::SIGKILL) };
        let _ = self.child.wait();
    }
}

/// Opens the shell asked for and pumps it over a stream of its own to the manager on `ch`, until either end goes.
pub async fn serve(ch: tonic::transport::Channel, docker: Option<bollard::Docker>, shells: Arc<Shells>, open: ShellOpen) {
    let (up, rx) = mpsc::channel::<ShellUp>(64);
    let _ = up.send(ShellUp { body: Some(shell_up::Body::SessionId(open.session_id.clone())) }).await;
    let mut down = match WorkerSessionClient::new(ch).shell(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(s) => {
            tracing::warn!(session = %open.session_id, "the manager took no shell stream: {}", s.message());
            return;
        }
    };
    let what = if open.container.is_empty() { "the host".to_string() } else { format!("container {}", open.container) };
    tracing::info!(session = %open.session_id, "a shell opened on {what}");
    let end = match shells.take() {
        None => ShellEnd { exit_code: -1, error: format!("{MAX} shells are open on this node already") },
        Some(_slot) => {
            let r = if open.container.is_empty() { host(&shells, &open, &mut down, &up).await } else { container(docker.as_ref(), &open, &mut down, &up).await };
            match r {
                Ok(code) => ShellEnd { exit_code: code, error: String::new() },
                Err(e) => ShellEnd { exit_code: -1, error: format!("{e:#}") },
            }
        }
    };
    tracing::info!(session = %open.session_id, code = end.exit_code, "the shell on {what} ended");
    let _ = up.send(ShellUp { body: Some(shell_up::Body::End(end)) }).await;
    // The stream down is held until the end has gone up: dropping it resets the whole call, the end with it.
    drop(up);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), down.message()).await;
}

/// What comes down, as the pumps take it: typed bytes, a new size, or the end.
enum Down {
    Input(Vec<u8>),
    Resize(u16, u16),
    Gone,
}

async fn next(down: &mut tonic::Streaming<ShellDown>) -> Down {
    loop {
        match down.message().await {
            Ok(Some(ShellDown { body: Some(shell_down::Body::Input(b)) })) => return Down::Input(b),
            Ok(Some(ShellDown { body: Some(shell_down::Body::Resize(s)) })) => {
                let (c, r) = size(s.cols, s.rows);
                return Down::Resize(c, r);
            }
            Ok(Some(_)) => continue,
            _ => return Down::Gone,
        }
    }
}

fn output(b: Vec<u8>) -> ShellUp {
    ShellUp { body: Some(shell_up::Body::Output(b)) }
}

async fn host(shells: &Shells, open: &ShellOpen, down: &mut tonic::Streaming<ShellDown>, up: &mpsc::Sender<ShellUp>) -> Result<i32> {
    if !shells.host {
        bail!("the host's shell is turned off on this node (FREIGHTDOCK_WORKER_HOST_SHELL)");
    }
    let shell = root_shell(&std::fs::read_to_string("/etc/passwd").unwrap_or_default());
    let name = format!("-{}", shell.rsplit('/').next().unwrap_or("sh"));
    let home = if std::fs::read_dir("/root").is_ok() { "/root" } else { "/" };
    let (cols, rows) = size(open.cols, open.rows);
    let env = [("HOME", home), ("USER", "root"), ("LOGNAME", "root"), ("SHELL", shell.as_str()), ("TERM", "xterm-256color"), ("LANG", "C.UTF-8"), ("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")];
    let mut pty = Pty::spawn(&shell, &name, &[], &env, home, cols, rows)?;
    let (mut reader, mut writer) = pty.ends()?;
    let up2 = up.clone();
    let mut reading = tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK];
        // A read of the master fails (EIO) once the last process on the terminal has gone: that is the end.
        while let Ok(n) = reader.read(&mut buf).await {
            if n == 0 || up2.send(output(buf[..n].to_vec())).await.is_err() {
                break;
            }
        }
    });
    let gone = loop {
        tokio::select! {
            _ = &mut reading => break false,
            d = next(down) => match d {
                Down::Input(b) => { if writer.write_all(&b).await.is_err() { break false; } let _ = writer.flush().await; }
                Down::Resize(c, r) => pty.resize(c, r),
                Down::Gone => break true,
            },
        }
    };
    if gone {
        reading.abort();
    }
    let code = tokio::task::spawn_blocking(move || {
        if gone {
            pty.hang_up();
        }
        pty.child.wait().ok().and_then(|s| s.code()).unwrap_or(-1)
    })
    .await
    .unwrap_or(-1);
    Ok(code)
}

async fn container(docker: Option<&bollard::Docker>, open: &ShellOpen, down: &mut tonic::Streaming<ShellDown>, up: &mpsc::Sender<ShellUp>) -> Result<i32> {
    use bollard::exec::{ResizeExecOptions, StartExecOptions, StartExecResults};
    let d = docker.ok_or_else(|| anyhow!("this node has no Docker"))?;
    let (cols, rows) = size(open.cols, open.rows);
    let config = bollard::models::ExecConfig {
        attach_stdin: Some(true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        tty: Some(true),
        console_size: Some(vec![usize::from(rows), usize::from(cols)]),
        env: Some(vec!["TERM=xterm-256color".into()]),
        cmd: Some(container_command()),
        ..Default::default()
    };
    let exec = d.create_exec(&open.container, config).await.with_context(|| format!("a shell in {}", open.container))?.id;
    let StartExecResults::Attached { mut output, mut input } = d.start_exec(&exec, Some(StartExecOptions { detach: false, tty: true, output_capacity: Some(CHUNK) })).await.context("starting the shell")? else {
        bail!("Docker started the shell detached");
    };
    let _ = d.resize_exec(&exec, ResizeExecOptions { width: cols, height: rows }).await;
    let gone = loop {
        tokio::select! {
            o = output.next() => match o {
                Some(Ok(l)) => { if up.send(output_of(l)).await.is_err() { break true; } }
                _ => break false,
            },
            dn = next(down) => match dn {
                Down::Input(b) => { if input.write_all(&b).await.is_err() { break false; } let _ = input.flush().await; }
                Down::Resize(c, r) => { let _ = d.resize_exec(&exec, ResizeExecOptions { width: c, height: r }).await; }
                Down::Gone => break true,
            },
        }
    };
    if gone {
        // The person went: hang the shell up as a terminal closing would (end of input, then the stream dropped).
        let _ = input.shutdown().await;
        drop(input);
        drop(output);
        return Ok(-1);
    }
    Ok(d.inspect_exec(&exec).await.ok().and_then(|i| i.exit_code).map(|c| c as i32).unwrap_or(-1))
}

fn output_of(l: bollard::container::LogOutput) -> ShellUp {
    output(l.into_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_terminal_s_size_is_kept_sane() {
        assert_eq!(size(0, 0), (80, 24));
        assert_eq!(size(120, 40), (120, 40));
        assert_eq!(size(100_000, 1), (1000, 1));
    }

    #[test]
    fn root_s_shell_is_read_from_passwd() {
        assert_eq!(root_shell("daemon:x:1:1::/:/usr/sbin/nologin\nroot:x:0:0:root:/root:/bin/bash\n"), "/bin/bash");
        assert_eq!(root_shell("root:x:0:0:root:/root:\n"), "/bin/sh", "none named");
        assert_eq!(root_shell(""), "/bin/sh");
    }

    #[test]
    fn a_container_gets_bash_or_sh_as_a_login_shell() {
        let c = container_command();
        assert_eq!(c[0], "/bin/sh");
        assert!(c[2].contains("exec bash -l") && c[2].contains("exec sh -l"));
    }

    #[test]
    fn at_most_max_shells_are_open_at_once() {
        let s = Shells::new(true);
        let slots: Vec<_> = (0..MAX).map(|_| s.take().expect("a place")).collect();
        assert!(s.take().is_none(), "no more than {MAX}");
        drop(slots);
        assert!(s.take().is_some(), "places come back as shells end");
    }

    #[tokio::test]
    async fn a_program_runs_on_a_terminal_of_the_size_asked_and_its_session_is_hung_up() {
        let mut p = Pty::spawn("/bin/sh", "sh", &["-c", "stty size; tty >/dev/null && echo on-a-tty; read line; echo got:$line"], &[("PATH", "/usr/bin:/bin")], "/", 100, 30).unwrap();
        let (mut r, mut w) = p.ends().unwrap();
        let mut seen = String::new();
        let mut buf = [0u8; 1024];
        while !seen.contains("on-a-tty") {
            let n = tokio::time::timeout(std::time::Duration::from_secs(5), r.read(&mut buf)).await.expect("output").unwrap();
            seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
        assert!(seen.contains("30 100"), "{seen:?}");
        w.write_all(b"typed\n").await.unwrap();
        w.flush().await.unwrap();
        while !seen.contains("got:typed") {
            let Ok(Ok(n)) = tokio::time::timeout(std::time::Duration::from_secs(5), r.read(&mut buf)).await else { break };
            if n == 0 {
                break;
            }
            seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
        assert!(seen.contains("got:typed"), "{seen:?}");
        assert_eq!(p.child.wait().unwrap().code(), Some(0));

        let mut long = Pty::spawn("/bin/sh", "sh", &["-c", "sleep 30 & wait"], &[("PATH", "/usr/bin:/bin")], "/", 80, 24).unwrap();
        let started = std::time::Instant::now();
        long.hang_up();
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "hung up, not waited out");
    }
}
