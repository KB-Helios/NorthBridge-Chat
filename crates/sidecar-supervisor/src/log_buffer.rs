use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::model::{LogStream, SidecarLogEntry, epoch_millis};

#[derive(Clone)]
pub(crate) struct LogBuffer {
    entries: Arc<Mutex<VecDeque<SidecarLogEntry>>>,
    capacity: usize,
    redactions: Arc<Vec<String>>,
}

impl LogBuffer {
    pub(crate) fn new(capacity: usize, redactions: Vec<String>) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            redactions: Arc::new(redactions),
        }
    }

    pub(crate) fn push(&self, stream: LogStream, mut message: String) {
        for secret in self.redactions.iter().filter(|secret| !secret.is_empty()) {
            message = message.replace(secret, "[REDACTED]");
        }

        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(SidecarLogEntry {
            timestamp_ms: epoch_millis(),
            stream,
            message,
        });
    }

    pub(crate) fn snapshot(&self) -> Vec<SidecarLogEntry> {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}
