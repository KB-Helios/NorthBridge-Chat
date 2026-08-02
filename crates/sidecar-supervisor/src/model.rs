use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarPhase {
    Stopped,
    Starting,
    Ready,
    Backoff,
    Stopping,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SidecarSnapshot {
    pub name: String,
    pub phase: SidecarPhase,
    pub pid: Option<u32>,
    pub restart_count: u32,
    pub last_error: Option<String>,
    pub changed_at_ms: u64,
}

impl SidecarSnapshot {
    pub(crate) fn stopped(name: String) -> Self {
        Self {
            name,
            phase: SidecarPhase::Stopped,
            pid: None,
            restart_count: 0,
            last_error: None,
            changed_at_ms: epoch_millis(),
        }
    }

    pub(crate) fn transition(
        &self,
        phase: SidecarPhase,
        pid: Option<u32>,
        restart_count: u32,
        last_error: Option<String>,
    ) -> Self {
        Self {
            name: self.name.clone(),
            phase,
            pid,
            restart_count,
            last_error,
            changed_at_ms: epoch_millis(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SidecarLogEntry {
    pub timestamp_ms: u64,
    pub stream: LogStream,
    pub message: String,
}

pub(crate) fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
