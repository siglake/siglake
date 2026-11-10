//! Owned synthetic TLS fixture. Keys exist only in its private directory and are removed on drop.
use sqlx::{postgres::PgConnectOptions, Connection, PgConnection};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, process::Command, time::Duration};

struct Fixture {
    root: PathBuf,
    started: bool,
}
impl Fixture {
    fn command(&self, tool: &str, args: &[&str]) {
        let output = Command::new(tool)
            .current_dir(&self.root)
            .args(args)
            .output()
            .expect("owned fixture tool");
        assert!(output.status.success(), "owned fixture tool failed: {tool}");
        // Tool output is memory-only, never included in diagnostics/evidence.
    }
    fn start(&mut self, tls: bool, port: u16) {
        let options = format!("-k {} -h 127.0.0.1 -p {port} -c ssl={} -c ssl_cert_file={} -c ssl_key_file={} -c log_min_messages=panic -c log_min_error_statement=panic -c log_error_verbosity=terse", self.root.display(), if tls {"on"} else {"off"}, self.root.join("server.crt").display(), self.root.join("server.key").display());
        self.started = true;
        self.command(
            "pg_ctl",
            &[
                "-D",
                self.root.join("data").to_str().unwrap(),
                "-l",
                "/dev/null",
                "-o",
                &options,
                "-w",
                "start",
            ],
        );
        self.started = true;
    }
    fn stop(&mut self) {
        self.command(
            "pg_ctl",
            &[
                "-D",
                self.root.join("data").to_str().unwrap(),
                "-m",
                "immediate",
                "-w",
                "stop",
            ],
        );
        self.started = false;
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.started {
            let _ = Command::new("pg_ctl")
                .args([
                    "-D",
                    self.root.join("data").to_str().unwrap(),
                    "-m",
                    "immediate",
                    "-w",
                    "stop",
                ])
                .output();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

// Rustls handshake validation comes through SQLx0.8.6 complete_io as Io(InvalidData),
// while TLS setup/no-upgrade errors use Error::Tls. Never accept a generic IO failure.
fn certificate_failure(result: Result<PgConnection, sqlx::Error>, reason: &str) -> bool {
    match result {
        Err(sqlx::Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData => {
            error.to_string().contains(reason)
        }
        Err(sqlx::Error::Tls(error)) => error.to_string().contains(reason),
        _ => false,
    }
}

pub(crate) async fn verify_connector(
    role: &str,
    make_options: impl Fn(&str, Option<&str>) -> PgConnectOptions,
) {
    let root = std::env::temp_dir().join(format!("rds-trust-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut fixture = Fixture {
        root,
        started: false,
    };
    fixture.command(
        "initdb",
        &[
            "-D",
            fixture.root.join("data").to_str().unwrap(),
            "--username=fixture_admin",
            "--no-locale",
            "--encoding=UTF8",
            "--auth=trust",
        ],
    );
    // openssl creates key files under an owned 0700 directory; explicitly force 0600 too.
    fixture.command(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-noenc",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=Owned synthetic RDS trust fixture",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
    );
    fixture.command(
        "openssl",
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-noenc",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
        ],
    );
    std::fs::write(fixture.root.join("extensions"), "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n").unwrap();
    fixture.command(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.crt",
            "-days",
            "2",
            "-extfile",
            "extensions",
        ],
    );
    for key in ["ca.key", "server.key"] {
        std::fs::set_permissions(
            fixture.root.join(key),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    fixture.start(true, port);
    fixture.command("psql", &["-X", "-v", "ON_ERROR_STOP=1", "-h", fixture.root.to_str().unwrap(), "-p", &port.to_string(), "-U", "fixture_admin", "-d", "postgres", "-c", &format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION")]);
    let uri =
        format!("postgresql://{role}:synthetic@localhost:{port}/postgres?sslmode=verify-full");
    let selected = make_options(&uri, Some("aws-rds-us-east-1-rsa2048-g1"));
    // Test-only replacement exercises the same SQLx PEM-in-memory path. Never a runtime input.
    let ca = std::fs::read(fixture.root.join("ca.pem")).unwrap();
    let trusted = selected.clone().ssl_root_cert_from_pem(ca);
    for _ in 0..2 {
        let mut connection = tokio::time::timeout(
            Duration::from_secs(3),
            PgConnection::connect_with(&trusted.clone()),
        )
        .await
        .unwrap()
        .map_err(|_| ())
        .expect("owned correct CA/host TLS success");
        let ssl: bool =
            sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
                .fetch_one(&mut connection)
                .await
                .map_err(|_| ())
                .expect("owned synthetic TLS state");
        let user: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut connection)
            .await
            .map_err(|_| ())
            .expect("owned synthetic query");
        assert!(ssl && user == role);
        connection
            .close()
            .await
            .map_err(|_| ())
            .expect("owned connection close");
    }
    // Missing selector keeps bundled roots; our synthetic private CA is not trusted.
    let unselected = PgConnection::connect_with(&make_options(&uri, None)).await;
    assert!(certificate_failure(unselected, "UnknownIssuer"));
    // A real reachable TLS server, not TCP refusal: only the CA or host differs.
    let wrong_ca = PgConnection::connect_with(&selected).await;
    assert!(
        certificate_failure(wrong_ca, "UnknownIssuer"),
        "wrong CA must fail TLS validation"
    );
    let wrong_host = PgConnection::connect_with(&trusted.clone().host("127.0.0.1")).await;
    assert!(
        certificate_failure(wrong_host, "certificate not valid for name"),
        "wrong host must fail TLS validation"
    );
    fixture.stop();
    fixture.start(false, port);
    let no_tls = PgConnection::connect_with(&trusted).await;
    assert!(
        matches!(no_tls, Err(sqlx::Error::Tls(_))),
        "verify-full must not downgrade on a reachable non-TLS server"
    );
}
