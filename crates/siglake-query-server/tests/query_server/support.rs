/// Whether this environment allows binding a loopback listener. Restricted
/// sandboxes (e.g. coding-agent jails) forbid it; socket-bound integration
/// tests skip there — loudly, so a green run in such an environment is
/// visibly partial. In CI and normal dev environments this always returns
/// true and every test runs.
pub fn loopback_available() -> bool {
    let ok = std::net::TcpListener::bind("127.0.0.1:0").map(drop).is_ok();
    if !ok {
        eprintln!("SKIP: loopback bind unavailable in this environment; socket-bound test skipped");
    }
    ok
}
