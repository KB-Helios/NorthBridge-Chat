use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use northbridge_core_api::custom_local_model_selection;
use northbridge_domain::{DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY, ReadyEvent};
use northbridge_storage::Database;
use tempfile::TempDir;
use url::Url;

const TEST_TOKEN: &str = "process-test-session-token-with-at-least-32-bytes";
const TEST_CONTROL_TOKEN: &str = "process-test-control-token-with-at-least-32-bytes";

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn launches_on_an_ephemeral_loopback_port_and_enforces_authentication() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("process.sqlite");
    let mut child = Command::new(env!("CARGO_BIN_EXE_desktop-core"))
        .arg("--database")
        .arg(&database_path)
        .env("NORTHBRIDGE_SESSION_TOKEN", TEST_TOKEN)
        .env("NORTHBRIDGE_CONTROL_TOKEN", TEST_CONTROL_TOKEN)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut guard = ProcessGuard(child);
    let ready = read_ready(stdout, &mut guard, &directory);
    let url = Url::parse(&ready.address).unwrap();
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    assert_ne!(url.port(), Some(0));
    assert_eq!(ready.schema_version, 11);
    assert!(database_path.exists());

    let unauthorized = http_get(&url, "/v1/health", None);
    assert!(unauthorized.starts_with("HTTP/1.1 401 Unauthorized"));
    let authorized = http_get(&url, "/v1/health", Some(TEST_TOKEN));
    assert!(authorized.starts_with("HTTP/1.1 200 OK"));
    assert!(authorized.contains("\"schemaVersion\":11"));

    let missing_control =
        http_request(&url, "POST", "/v1/runtime/shutdown", Some(TEST_TOKEN), None);
    assert!(missing_control.starts_with("HTTP/1.1 401 Unauthorized"));
    assert!(guard.0.try_wait().unwrap().is_none());

    let accepted = http_request(
        &url,
        "POST",
        "/v1/runtime/shutdown",
        Some(TEST_TOKEN),
        Some(TEST_CONTROL_TOKEN),
    );
    assert!(accepted.starts_with("HTTP/1.1 202 Accepted"));
    let status = wait_for_exit(&mut guard.0);
    assert!(status.success(), "desktop-core exited with {status}");
}

#[test]
fn emits_ready_before_warming_a_missing_persisted_model() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("persisted-model.sqlite");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let database = Database::connect(&database_path).await.unwrap();
        let selection = custom_local_model_selection("missing-persisted-model.gguf").unwrap();
        database
            .set_json_setting(
                DESKTOP_SETTINGS_SCOPE,
                MODEL_SELECTION_SETTING_KEY,
                &serde_json::to_value(selection).unwrap(),
            )
            .await
            .unwrap();
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_desktop-core"))
        .arg("--database")
        .arg(&database_path)
        .env("NORTHBRIDGE_SESSION_TOKEN", TEST_TOKEN)
        .env("NORTHBRIDGE_CONTROL_TOKEN", TEST_CONTROL_TOKEN)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut guard = ProcessGuard(child);
    let ready = read_ready(stdout, &mut guard, &directory);
    let url = Url::parse(&ready.address).unwrap();

    let health = http_get(&url, "/v1/health", Some(TEST_TOKEN));
    assert!(health.starts_with("HTTP/1.1 200 OK"));
    let accepted = http_request(
        &url,
        "POST",
        "/v1/runtime/shutdown",
        Some(TEST_TOKEN),
        Some(TEST_CONTROL_TOKEN),
    );
    assert!(accepted.starts_with("HTTP/1.1 202 Accepted"));
    assert!(wait_for_exit(&mut guard.0).success());
}

fn read_ready(
    stdout: impl Read + Send + 'static,
    guard: &mut ProcessGuard,
    _directory: &TempDir,
) -> ReadyEvent {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = sender.send(result);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(20))
        .unwrap_or_else(|_| {
            let status = guard.0.try_wait().ok().flatten();
            panic!("desktop-core did not emit a ready event; status: {status:?}");
        })
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

fn http_get(base_url: &Url, path: &str, token: Option<&str>) -> String {
    http_request(base_url, "GET", path, token, None)
}

fn http_request(
    base_url: &Url,
    method: &str,
    path: &str,
    token: Option<&str>,
    control_token: Option<&str>,
) -> String {
    let address = format!(
        "{}:{}",
        base_url.host_str().unwrap(),
        base_url.port().unwrap()
    );
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let authorization = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let control = control_token
        .map(|token| format!("x-northbridge-control-token: {token}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{authorization}{control}Content-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "desktop-core did not stop after the authenticated shutdown request"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
