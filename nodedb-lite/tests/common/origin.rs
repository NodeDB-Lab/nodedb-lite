//! Spawn/teardown helpers for a real Origin server process.
//!
//! Tests that need a live Origin sync endpoint use [`OriginServer`].
//! The guard kills the process on drop.
//!
//! The Origin binary is located in one of three ways (in priority order):
//! 1. `NODEDB_BIN` env var — set by the nextest setup script.
//! 2. `<project-root>/nodedb/target/release/nodedb` (pre-built release).
//! 3. `<project-root>/nodedb/target/debug/nodedb` (pre-built debug).
//!
//! If no binary is found, [`OriginServer::try_spawn`] returns `None` and
//! the calling test should print a skip message and return early.
//!
//! The sync WebSocket listener always binds to `0.0.0.0:9090` (the
//! `SyncListenerConfig` default). All interop test files are placed in the
//! `heavy` nextest group so they run strictly one at a time, preventing
//! port-9090 collisions between parallel test processes.
//!
//! Each test case gets its own `OriginServer` with a private temp data
//! directory so WAL / storage state from previous runs cannot interfere.

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Locate the nodedb Origin binary, if available.
///
/// Returns `None` when no binary can be found — interop tests treat that as a
/// skip (see [`OriginServer::try_spawn`]), so a Lite-only checkout still passes.
///
/// Search order:
/// 1. `NODEDB_BIN` env var — exported by the `build-origin` nextest setup
///    script (`scripts/ensure-origin.sh`), which runs `cargo build -p nodedb`
///    against this workspace's dev-dependency. This is the normal path under
///    `cargo nextest run`.
/// 2. This workspace's own `target/{debug,release}/nodedb` — for a manual
///    `cargo build -p nodedb` outside nextest.
///
/// There is deliberately NO hardcoded sibling-repo path: the Origin crate is
/// resolved through cargo (crates.io or the local `[patch.crates-io]`), so its
/// binary always lands in this workspace's own target directory.
pub fn find_origin_binary() -> Option<PathBuf> {
    if let Ok(val) = env::var("NODEDB_BIN") {
        let p = PathBuf::from(&val);
        if p.is_file() {
            return Some(p);
        }
    }

    // This workspace's own cargo target dir. CARGO_MANIFEST_DIR is
    // nodedb-lite/nodedb-lite/; its parent is the workspace root nodedb-lite/.
    let manifest = env::var("CARGO_MANIFEST_DIR").ok()?;
    let workspace_root = Path::new(&manifest).parent()?.to_path_buf();
    let target = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root.join("target"));

    for profile in ["debug", "release"] {
        let candidate = target.join(profile).join("nodedb");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    None
}

/// The sync WebSocket URL that Origin always listens on.
pub const ORIGIN_WS: &str = "ws://127.0.0.1:9090";

/// The pgwire address that Origin listens on (port 6432 by default).
pub const ORIGIN_PGWIRE_ADDR: &str = "127.0.0.1:6432";

/// Guard for a running Origin server process.
///
/// Kills the process on drop. Tests obtain an instance via
/// [`OriginServer::try_spawn`] or [`OriginServer::try_spawn_with_pgwire`].
///
/// Each instance has its own temporary data directory so WAL / storage
/// state from previous runs cannot interfere.
pub struct OriginServer {
    child: Child,
    /// The WebSocket sync URL (always `ws://127.0.0.1:9090`).
    pub ws_url: &'static str,
    /// Temporary data directory. Kept alive until drop.
    _data_dir: tempfile::TempDir,
    /// Optional config file dir (kept alive so the file isn't deleted early).
    _config_dir: Option<tempfile::TempDir>,
}

impl OriginServer {
    /// Spawn a fresh Origin server with a private temp data directory.
    ///
    /// Returns `None` if the Origin binary cannot be found (Origin repo absent
    /// or not built). The caller should print a skip message and return early.
    ///
    /// Blocks (up to 30 s) until the sync WebSocket port is accepting TCP
    /// connections.
    pub fn try_spawn() -> Option<Self> {
        let binary = find_origin_binary()?;
        Some(Self::spawn_inner(binary, false))
    }

    /// Spawn a fresh Origin server with both the sync WebSocket (port 9090)
    /// and the pgwire listener (port 6432) enabled in trust auth mode.
    ///
    /// Returns `None` if the Origin binary cannot be found.
    ///
    /// Blocks until **both** ports are accepting TCP connections (up to 30 s).
    pub fn try_spawn_with_pgwire() -> Option<Self> {
        let binary = find_origin_binary()?;
        Some(Self::spawn_inner(binary, true))
    }

    fn spawn_inner(binary: PathBuf, with_pgwire: bool) -> Self {
        let data_dir = tempfile::tempdir().expect("create temp data dir for Origin");

        let (mut cmd, config_dir) = if with_pgwire {
            // Write a minimal config file that enables trust auth so the
            // pgwire client in sql_parity tests can connect without a password.
            let cfg_dir = tempfile::tempdir().expect("create temp config dir for Origin");
            let cfg_path = cfg_dir.path().join("nodedb.toml");
            let cfg_content = "[auth]\nmode = \"trust\"\nsuperuser_name = \"nodedb\"\nmin_password_length = 8\nmax_failed_logins = 10\nlockout_duration_secs = 300\nidle_timeout_secs = 0\nmax_connections_per_user = 0\npassword_expiry_days = 0\naudit_retention_days = 0\n";
            std::fs::write(&cfg_path, cfg_content).expect("write Origin trust config file");

            let mut c = Command::new(&binary);
            c.arg(cfg_path.to_str().expect("config path is valid UTF-8"))
                .env("NODEDB_DATA_DIR", data_dir.path())
                .env_remove("RUST_LOG")
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            (c, Some(cfg_dir))
        } else {
            let mut c = Command::new(&binary);
            c.env("NODEDB_DATA_DIR", data_dir.path())
                .env_remove("RUST_LOG")
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            (c, None)
        };

        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn Origin binary {}: {e}", binary.display()));

        let ports: &[(&str, u16)] = if with_pgwire {
            &[("sync WebSocket", 9090), ("pgwire", 6432)]
        } else {
            &[("sync WebSocket", 9090)]
        };

        let deadline = Instant::now() + Duration::from_secs(30);
        'outer: loop {
            let all_ready = ports
                .iter()
                .all(|(_, port)| std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok());
            if all_ready {
                break 'outer;
            }
            if Instant::now() > deadline {
                let pending: Vec<&str> = ports
                    .iter()
                    .filter(|(_, port)| {
                        std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_err()
                    })
                    .map(|(name, _)| *name)
                    .collect();
                panic!(
                    "Origin server did not become ready within 30 seconds.\n\
                     Pending ports: {pending:?}\n\
                     Binary: {}\n\
                     Data dir: {}",
                    binary.display(),
                    data_dir.path().display(),
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        OriginServer {
            child,
            ws_url: ORIGIN_WS,
            _data_dir: data_dir,
            _config_dir: config_dir,
        }
    }
}

impl Drop for OriginServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // _data_dir and _config_dir are dropped after child is killed,
        // cleaning up temp dirs.
    }
}

/// Connect to Origin and complete the sync handshake in trust mode (empty JWT).
///
/// Panics if the connection or handshake fails.
pub async fn connect_and_handshake(
    ws_url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use std::time::Duration;

    use futures::SinkExt;
    use futures::StreamExt;
    use nodedb_types::sync::wire::{HandshakeAckMsg, HandshakeMsg, SyncFrame, SyncMessageType};
    use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url)
        .await
        .unwrap_or_else(|e| panic!("connect to Origin at {ws_url}: {e}"));

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "interop-test".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: WIRE_FORMAT_VERSION,
    };

    let frame_bytes = SyncFrame::try_encode(SyncMessageType::Handshake, &hs)
        .expect("encode handshake frame")
        .to_bytes();

    ws.send(Message::Binary(frame_bytes.into()))
        .await
        .expect("send handshake");

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("handshake ack timeout")
        .expect("stream ended before ack")
        .expect("WebSocket error waiting for handshake ack");

    let frame =
        SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode handshake ack frame");

    assert_eq!(
        frame.msg_type,
        SyncMessageType::HandshakeAck,
        "expected HandshakeAck, got {:?}",
        frame.msg_type
    );

    let ack: HandshakeAckMsg = frame.decode_body().expect("decode HandshakeAckMsg");
    assert!(ack.success, "handshake rejected by Origin: {:?}", ack.error);

    ws
}
