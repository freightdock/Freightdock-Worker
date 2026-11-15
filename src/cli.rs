//! The worker's command line: it is configured by its environment alone, so it takes no arguments but asking for its
//! version or its usage. Anything else is refused, never taken as leave to start: a worker started by mistake would
//! dial the managers as this node beside the one systemd runs.

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Cli {
    Run,
    Version,
    Help,
    Unknown(String),
}

pub const USAGE: &str = "freightdock-worker              run the worker (configured by its environment, see the README)
freightdock-worker --version    print its version
freightdock-worker --help       print this";

pub fn parse(args: &[String]) -> Cli {
    match args {
        [] => Cli::Run,
        [a] if matches!(a.as_str(), "--version" | "-V" | "version") => Cli::Version,
        [a] if matches!(a.as_str(), "--help" | "-h" | "help") => Cli::Help,
        [a] => Cli::Unknown(a.clone()),
        [_, rest @ ..] => Cli::Unknown(rest[0].clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(a: &[&str]) -> Cli {
        parse(&a.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn only_no_argument_at_all_starts_the_worker() {
        assert_eq!(p(&[]), Cli::Run);
        for v in ["--version", "-V", "version"] {
            assert_eq!(p(&[v]), Cli::Version, "{v}");
        }
        for h in ["--help", "-h", "help"] {
            assert_eq!(p(&[h]), Cli::Help, "{h}");
        }
        assert_eq!(p(&["--verison"]), Cli::Unknown("--verison".into()));
        assert_eq!(p(&["run"]), Cli::Unknown("run".into()));
        assert_eq!(p(&["--version", "x"]), Cli::Unknown("x".into()), "nothing after it either");
    }
}
