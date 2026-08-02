use std::{collections::BTreeMap, ffi::OsString, net::TcpListener, path::PathBuf, time::Duration};

use northbridge_sidecar_supervisor::{
    HttpProbe, IntegrityPolicy, RestartPolicy, SidecarPhase, SidecarSpec, SidecarSupervisor,
};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_northbridge-sidecar-fixture"))
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn fixture_spec(port: u16, lifetime_ms: Option<u64>) -> SidecarSpec {
    let mut args = vec![OsString::from("--port"), OsString::from(port.to_string())];
    if let Some(lifetime_ms) = lifetime_ms {
        args.push(OsString::from("--lifetime-ms"));
        args.push(OsString::from(lifetime_ms.to_string()));
    }
    SidecarSpec {
        name: "fixture".to_string(),
        executable: fixture_path(),
        args,
        env: BTreeMap::new(),
        working_directory: None,
        readiness: HttpProbe::loopback(
            format!("http://127.0.0.1:{port}/health").parse().unwrap(),
            200,
        )
        .unwrap(),
        shutdown: None,
        startup_timeout: Duration::from_secs(3),
        probe_interval: Duration::from_millis(20),
        request_timeout: Duration::from_millis(200),
        shutdown_timeout: Duration::from_millis(200),
        restart: RestartPolicy {
            max_restarts: 1,
            initial_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(50),
        },
        integrity: IntegrityPolicy::SkipForDevelopment,
        log_capacity: 32,
        redactions: vec!["fixture-secret".to_string()],
    }
}

#[tokio::test]
async fn starts_probes_redacts_logs_and_stops() {
    let supervisor = SidecarSupervisor::spawn(fixture_spec(free_port(), None)).unwrap();
    let ready = supervisor.ensure_started().await.unwrap();
    assert_eq!(ready.phase, SidecarPhase::Ready);
    assert!(ready.pid.is_some());

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        supervisor
            .logs()
            .iter()
            .any(|entry| entry.message.contains("[REDACTED]"))
    );

    let stopped = supervisor.stop().await.unwrap();
    assert_eq!(stopped.phase, SidecarPhase::Stopped);
    assert!(stopped.pid.is_none());
}

#[tokio::test]
async fn restarts_a_crashed_process_then_fails_closed() {
    let supervisor = SidecarSupervisor::spawn(fixture_spec(free_port(), Some(750))).unwrap();
    supervisor.ensure_started().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = supervisor.snapshot();
        if snapshot.phase == SidecarPhase::Failed {
            assert_eq!(snapshot.restart_count, 1);
            assert!(snapshot.last_error.is_some());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "supervisor did not fail closed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn refuses_a_hash_mismatch_before_spawn() {
    let mut spec = fixture_spec(free_port(), None);
    spec.integrity = IntegrityPolicy::Sha256("00".repeat(32));
    spec.restart.max_restarts = 0;
    let supervisor = SidecarSupervisor::spawn(spec).unwrap();

    let error = supervisor.ensure_started().await.unwrap_err();
    assert!(error.to_string().contains("SHA-256 verification"));
    assert_eq!(supervisor.snapshot().phase, SidecarPhase::Failed);
}
