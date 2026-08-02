use std::{process::Stdio, time::Duration};

use reqwest::{Client, redirect::Policy};
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
    time::{Instant, sleep, timeout},
};

use crate::{
    error::{Result, SidecarError},
    log_buffer::LogBuffer,
    model::{LogStream, SidecarLogEntry, SidecarPhase, SidecarSnapshot},
    spec::{HttpMethod, IntegrityPolicy, SidecarSpec, decode_sha256},
};

const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct SidecarSupervisor {
    commands: mpsc::Sender<CommandMessage>,
    snapshot: watch::Receiver<SidecarSnapshot>,
    logs: LogBuffer,
}

impl SidecarSupervisor {
    pub fn spawn(spec: SidecarSpec) -> Result<Self> {
        spec.validate()?;
        let initial = SidecarSnapshot::stopped(spec.name.clone());
        let (snapshot_tx, snapshot_rx) = watch::channel(initial);
        let (command_tx, command_rx) = mpsc::channel(16);
        let logs = LogBuffer::new(spec.log_capacity, spec.redactions.clone());
        tokio::spawn(run_actor(spec, command_rx, snapshot_tx, logs.clone()));
        Ok(Self {
            commands: command_tx,
            snapshot: snapshot_rx,
            logs,
        })
    }

    pub async fn ensure_started(&self) -> Result<SidecarSnapshot> {
        self.request(CommandKind::EnsureStarted).await
    }

    pub async fn stop(&self) -> Result<SidecarSnapshot> {
        self.request(CommandKind::Stop).await
    }

    pub async fn shutdown(&self) -> Result<SidecarSnapshot> {
        self.request(CommandKind::Shutdown).await
    }

    #[must_use]
    pub fn snapshot(&self) -> SidecarSnapshot {
        self.snapshot.borrow().clone()
    }

    #[must_use]
    pub fn logs(&self) -> Vec<SidecarLogEntry> {
        self.logs.snapshot()
    }

    async fn request(&self, kind: CommandKind) -> Result<SidecarSnapshot> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(CommandMessage {
                kind,
                reply: reply_tx,
            })
            .await
            .map_err(|_| SidecarError::SupervisorUnavailable)?;
        reply_rx
            .await
            .map_err(|_| SidecarError::SupervisorUnavailable)?
    }
}

#[derive(Debug, Clone, Copy)]
enum CommandKind {
    EnsureStarted,
    Stop,
    Shutdown,
}

struct CommandMessage {
    kind: CommandKind,
    reply: oneshot::Sender<Result<SidecarSnapshot>>,
}

enum StartOutcome {
    Ready(Box<Child>),
    Stopped,
    Shutdown,
    Failed(SidecarError),
}

enum RunningOutcome {
    Stopped,
    Shutdown,
    Crashed(String),
}

enum BackoffOutcome {
    Retry,
    Stopped,
    Shutdown,
}

async fn run_actor(
    spec: SidecarSpec,
    mut commands: mpsc::Receiver<CommandMessage>,
    snapshot_tx: watch::Sender<SidecarSnapshot>,
    logs: LogBuffer,
) {
    let client = match Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(spec.request_timeout)
        .timeout(spec.request_timeout)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            let current = snapshot_tx.borrow().clone();
            snapshot_tx.send_replace(current.transition(
                SidecarPhase::Failed,
                None,
                0,
                Some(format!("failed to create sidecar HTTP client: {error}")),
            ));
            return;
        }
    };

    while let Some(command) = commands.recv().await {
        match command.kind {
            CommandKind::EnsureStarted => {
                let mut pending = vec![command.reply];
                let mut restart_count = 0;
                loop {
                    match start_once(
                        &spec,
                        &client,
                        &mut commands,
                        &snapshot_tx,
                        &logs,
                        &mut pending,
                        restart_count,
                    )
                    .await
                    {
                        StartOutcome::Ready(mut child) => {
                            complete_pending(&mut pending, Ok(snapshot_tx.borrow().clone()));
                            match supervise_ready(
                                &spec,
                                &client,
                                &mut commands,
                                &snapshot_tx,
                                &logs,
                                &mut child,
                                restart_count,
                            )
                            .await
                            {
                                RunningOutcome::Stopped => break,
                                RunningOutcome::Shutdown => return,
                                RunningOutcome::Crashed(error) => {
                                    if restart_count >= spec.restart.max_restarts {
                                        publish(
                                            &snapshot_tx,
                                            SidecarPhase::Failed,
                                            None,
                                            restart_count,
                                            Some(error.clone()),
                                        );
                                        complete_pending(
                                            &mut pending,
                                            Err(SidecarError::Lifecycle(error)),
                                        );
                                        break;
                                    }
                                    restart_count += 1;
                                    match wait_for_restart(
                                        &spec,
                                        &mut commands,
                                        &snapshot_tx,
                                        &mut pending,
                                        restart_count,
                                        error,
                                    )
                                    .await
                                    {
                                        BackoffOutcome::Retry => {}
                                        BackoffOutcome::Stopped => break,
                                        BackoffOutcome::Shutdown => return,
                                    }
                                }
                            }
                        }
                        StartOutcome::Failed(error) => {
                            let message = error.to_string();
                            if restart_count >= spec.restart.max_restarts {
                                publish(
                                    &snapshot_tx,
                                    SidecarPhase::Failed,
                                    None,
                                    restart_count,
                                    Some(message.clone()),
                                );
                                complete_pending(&mut pending, Err(error));
                                break;
                            }
                            restart_count += 1;
                            match wait_for_restart(
                                &spec,
                                &mut commands,
                                &snapshot_tx,
                                &mut pending,
                                restart_count,
                                message,
                            )
                            .await
                            {
                                BackoffOutcome::Retry => {}
                                BackoffOutcome::Stopped => break,
                                BackoffOutcome::Shutdown => return,
                            }
                        }
                        StartOutcome::Stopped => break,
                        StartOutcome::Shutdown => return,
                    }
                }
            }
            CommandKind::Stop => {
                let snapshot = publish(&snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                let _ = command.reply.send(Ok(snapshot));
            }
            CommandKind::Shutdown => {
                let snapshot = publish(&snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                let _ = command.reply.send(Ok(snapshot));
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_once(
    spec: &SidecarSpec,
    client: &Client,
    commands: &mut mpsc::Receiver<CommandMessage>,
    snapshot_tx: &watch::Sender<SidecarSnapshot>,
    logs: &LogBuffer,
    pending: &mut Vec<oneshot::Sender<Result<SidecarSnapshot>>>,
    restart_count: u32,
) -> StartOutcome {
    publish(
        snapshot_tx,
        SidecarPhase::Starting,
        None,
        restart_count,
        None,
    );

    if let Err(error) =
        verify_executable_integrity(&spec.name, &spec.executable, &spec.integrity).await
    {
        return StartOutcome::Failed(error);
    }
    let mut child = match spawn_process(spec, logs) {
        Ok(child) => child,
        Err(error) => return StartOutcome::Failed(error),
    };
    publish(
        snapshot_tx,
        SidecarPhase::Starting,
        child.id(),
        restart_count,
        None,
    );

    let deadline = Instant::now() + spec.startup_timeout;
    let mut last_probe_error = None;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return StartOutcome::Failed(SidecarError::Lifecycle(format!(
                    "{} exited before readiness with {status}",
                    spec.name
                )));
            }
            Ok(None) => {}
            Err(error) => {
                return StartOutcome::Failed(SidecarError::Lifecycle(format!(
                    "failed to inspect {}: {error}",
                    spec.name
                )));
            }
        }

        match probe_ready(client, spec).await {
            Ok(true) => {
                publish(
                    snapshot_tx,
                    SidecarPhase::Ready,
                    child.id(),
                    restart_count,
                    None,
                );
                return StartOutcome::Ready(Box::new(child));
            }
            Ok(false) => {}
            Err(error) => last_probe_error = Some(error),
        }

        if Instant::now() >= deadline {
            terminate_process(spec, client, &mut child, logs).await;
            let detail = last_probe_error
                .map(|error| format!("; last probe error: {error}"))
                .unwrap_or_default();
            return StartOutcome::Failed(SidecarError::Lifecycle(format!(
                "{} did not become ready within {:?}{detail}",
                spec.name, spec.startup_timeout
            )));
        }

        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(CommandMessage { kind: CommandKind::EnsureStarted, reply }) => pending.push(reply),
                    Some(CommandMessage { kind: CommandKind::Stop, reply }) => {
                        publish(snapshot_tx, SidecarPhase::Stopping, child.id(), restart_count, None);
                        terminate_process(spec, client, &mut child, logs).await;
                        let snapshot = publish(snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                        let _ = reply.send(Ok(snapshot));
                        complete_pending(pending, Err(SidecarError::Lifecycle(format!("{} startup was stopped", spec.name))));
                        return StartOutcome::Stopped;
                    }
                    Some(CommandMessage { kind: CommandKind::Shutdown, reply }) => {
                        publish(snapshot_tx, SidecarPhase::Stopping, child.id(), restart_count, None);
                        terminate_process(spec, client, &mut child, logs).await;
                        let snapshot = publish(snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                        let _ = reply.send(Ok(snapshot));
                        complete_pending(pending, Err(SidecarError::Lifecycle(format!("{} startup was shut down", spec.name))));
                        return StartOutcome::Shutdown;
                    }
                    None => {
                        terminate_process(spec, client, &mut child, logs).await;
                        complete_pending(pending, Err(SidecarError::SupervisorUnavailable));
                        return StartOutcome::Shutdown;
                    }
                }
            }
            () = sleep(spec.probe_interval) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_ready(
    spec: &SidecarSpec,
    client: &Client,
    commands: &mut mpsc::Receiver<CommandMessage>,
    snapshot_tx: &watch::Sender<SidecarSnapshot>,
    logs: &LogBuffer,
    child: &mut Child,
    restart_count: u32,
) -> RunningOutcome {
    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(CommandMessage { kind: CommandKind::EnsureStarted, reply }) => {
                        let _ = reply.send(Ok(snapshot_tx.borrow().clone()));
                    }
                    Some(CommandMessage { kind: CommandKind::Stop, reply }) => {
                        publish(snapshot_tx, SidecarPhase::Stopping, child.id(), restart_count, None);
                        terminate_process(spec, client, child, logs).await;
                        let snapshot = publish(snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                        let _ = reply.send(Ok(snapshot));
                        return RunningOutcome::Stopped;
                    }
                    Some(CommandMessage { kind: CommandKind::Shutdown, reply }) => {
                        publish(snapshot_tx, SidecarPhase::Stopping, child.id(), restart_count, None);
                        terminate_process(spec, client, child, logs).await;
                        let snapshot = publish(snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                        let _ = reply.send(Ok(snapshot));
                        return RunningOutcome::Shutdown;
                    }
                    None => {
                        terminate_process(spec, client, child, logs).await;
                        return RunningOutcome::Shutdown;
                    }
                }
            }
            () = sleep(PROCESS_POLL_INTERVAL) => {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        return RunningOutcome::Crashed(format!("{} exited unexpectedly with {status}", spec.name));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return RunningOutcome::Crashed(format!("failed to inspect {}: {error}", spec.name));
                    }
                }
            }
        }
    }
}

async fn wait_for_restart(
    spec: &SidecarSpec,
    commands: &mut mpsc::Receiver<CommandMessage>,
    snapshot_tx: &watch::Sender<SidecarSnapshot>,
    pending: &mut Vec<oneshot::Sender<Result<SidecarSnapshot>>>,
    restart_count: u32,
    error: String,
) -> BackoffOutcome {
    publish(
        snapshot_tx,
        SidecarPhase::Backoff,
        None,
        restart_count,
        Some(error),
    );
    let factor = 1_u32
        .checked_shl(restart_count.saturating_sub(1).min(31))
        .unwrap_or(u32::MAX);
    let delay = spec
        .restart
        .initial_delay
        .saturating_mul(factor)
        .min(spec.restart.max_delay);
    let deadline = sleep(delay);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => return BackoffOutcome::Retry,
            command = commands.recv() => {
                match command {
                    Some(CommandMessage { kind: CommandKind::EnsureStarted, reply }) => pending.push(reply),
                    Some(CommandMessage { kind: CommandKind::Stop, reply }) => {
                        let snapshot = publish(snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                        let _ = reply.send(Ok(snapshot));
                        complete_pending(pending, Err(SidecarError::Lifecycle(format!("{} restart was stopped", spec.name))));
                        return BackoffOutcome::Stopped;
                    }
                    Some(CommandMessage { kind: CommandKind::Shutdown, reply }) => {
                        let snapshot = publish(snapshot_tx, SidecarPhase::Stopped, None, 0, None);
                        let _ = reply.send(Ok(snapshot));
                        complete_pending(pending, Err(SidecarError::Lifecycle(format!("{} restart was shut down", spec.name))));
                        return BackoffOutcome::Shutdown;
                    }
                    None => {
                        complete_pending(pending, Err(SidecarError::SupervisorUnavailable));
                        return BackoffOutcome::Shutdown;
                    }
                }
            }
        }
    }
}

async fn probe_ready(client: &Client, spec: &SidecarSpec) -> std::result::Result<bool, String> {
    let response = client
        .get(spec.readiness.url.clone())
        .send()
        .await
        .map_err(|error| error.to_string())?;
    Ok(response.status().as_u16() == spec.readiness.expected_status)
}

fn spawn_process(spec: &SidecarSpec, logs: &LogBuffer) -> Result<Child> {
    let mut command = Command::new(&spec.executable);
    command
        .args(&spec.args)
        .envs(&spec.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(directory) = &spec.working_directory {
        command.current_dir(directory);
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
    }

    let mut child = command.spawn().map_err(|error| {
        SidecarError::Lifecycle(format!(
            "failed to spawn {} from {}: {error}",
            spec.name,
            spec.executable.display()
        ))
    })?;
    if let Some(stdout) = child.stdout.take() {
        tokio::spawn(capture_output(stdout, LogStream::Stdout, logs.clone()));
    }
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(capture_output(stderr, LogStream::Stderr, logs.clone()));
    }
    Ok(child)
}

async fn capture_output<R>(reader: R, stream: LogStream, logs: LogBuffer)
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => logs.push(stream, line),
            Ok(None) => return,
            Err(error) => {
                logs.push(stream, format!("[log stream error: {error}]"));
                return;
            }
        }
    }
}

pub async fn verify_executable_integrity(
    name: &str,
    executable: &std::path::Path,
    integrity: &IntegrityPolicy,
) -> Result<()> {
    let metadata = tokio::fs::metadata(executable).await.map_err(|error| {
        SidecarError::Lifecycle(format!(
            "cannot inspect {} executable at {}: {error}",
            name,
            executable.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(SidecarError::Lifecycle(format!(
            "{} executable is not a file: {}",
            name,
            executable.display()
        )));
    }

    let IntegrityPolicy::Sha256(expected) = integrity else {
        tracing::warn!(sidecar = %name, "sidecar integrity verification skipped for development");
        return Ok(());
    };
    let expected = decode_sha256(expected)?;
    let mut file = File::open(executable).await.map_err(|error| {
        SidecarError::Lifecycle(format!("failed to open {name} for hashing: {error}"))
    })?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| SidecarError::Lifecycle(format!("failed to hash {name}: {error}")))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let actual: [u8; 32] = digest.finalize().into();
    if actual != expected {
        return Err(SidecarError::Lifecycle(format!(
            "{name} executable failed SHA-256 verification"
        )));
    }
    Ok(())
}

async fn terminate_process(
    spec: &SidecarSpec,
    client: &Client,
    child: &mut Child,
    logs: &LogBuffer,
) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }

    if let Some(shutdown) = &spec.shutdown {
        let request = match shutdown.method {
            HttpMethod::Get => client.get(shutdown.url.clone()),
            HttpMethod::Post => client.post(shutdown.url.clone()),
        };
        if let Err(error) = request.send().await {
            logs.push(
                LogStream::Stderr,
                format!("graceful shutdown request failed: {error}"),
            );
        }
        if timeout(spec.shutdown_timeout, child.wait()).await.is_ok() {
            return;
        }
    }

    if let Err(error) = child.kill().await {
        logs.push(
            LogStream::Stderr,
            format!("forced process termination failed: {error}"),
        );
    }
}

fn publish(
    snapshot_tx: &watch::Sender<SidecarSnapshot>,
    phase: SidecarPhase,
    pid: Option<u32>,
    restart_count: u32,
    error: Option<String>,
) -> SidecarSnapshot {
    let snapshot = snapshot_tx
        .borrow()
        .transition(phase, pid, restart_count, error);
    snapshot_tx.send_replace(snapshot.clone());
    snapshot
}

fn complete_pending(
    pending: &mut Vec<oneshot::Sender<Result<SidecarSnapshot>>>,
    result: Result<SidecarSnapshot>,
) {
    for reply in pending.drain(..) {
        let _ = reply.send(result.clone());
    }
}
