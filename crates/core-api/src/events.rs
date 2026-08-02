use std::{collections::HashMap, sync::Arc};

use northbridge_domain::{GenerationEvent, GenerationStatus, Message};
use northbridge_storage::{Database, Result};
use serde_json::Value;
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

const EVENT_CHANNEL_CAPACITY: usize = 1_024;

#[derive(Clone)]
pub struct GenerationEvents {
    inner: Arc<GenerationEventsInner>,
}

struct GenerationEventsInner {
    database: Database,
    senders: Mutex<HashMap<Uuid, broadcast::Sender<GenerationEvent>>>,
}

impl GenerationEvents {
    pub(crate) fn new(database: Database) -> Self {
        Self {
            inner: Arc::new(GenerationEventsInner {
                database,
                senders: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub async fn append(
        &self,
        generation_id: Uuid,
        event_type: &str,
        payload: Value,
    ) -> Result<GenerationEvent> {
        let event = self
            .inner
            .database
            .append_generation_event(generation_id, event_type, payload)
            .await?;
        let sender = self.sender(generation_id).await;
        let _ = sender.send(event.clone());
        Ok(event)
    }

    pub async fn set_status(&self, generation_id: Uuid, status: GenerationStatus) -> Result<()> {
        self.inner
            .database
            .set_generation_status(generation_id, status)
            .await
    }

    pub async fn start_run(&self, generation_id: Uuid) -> Result<GenerationEvent> {
        let event = self
            .inner
            .database
            .start_generation_run(generation_id)
            .await?;
        self.publish(event.clone()).await;
        Ok(event)
    }

    pub async fn finish_without_output(
        &self,
        generation_id: Uuid,
        status: GenerationStatus,
        event_type: &str,
        payload: Value,
    ) -> Result<GenerationEvent> {
        let event = self
            .inner
            .database
            .finalize_generation(generation_id, status, event_type, payload)
            .await?;
        self.publish(event.clone()).await;
        Ok(event)
    }

    pub async fn complete_with_text(
        &self,
        generation_id: Uuid,
        content: String,
    ) -> Result<Message> {
        let (message, event) = self
            .inner
            .database
            .complete_generation_with_text(generation_id, content)
            .await?;
        self.publish(event).await;
        Ok(message)
    }

    pub async fn complete_with_text_metadata(
        &self,
        generation_id: Uuid,
        content: String,
        metadata: Value,
    ) -> Result<Message> {
        let (message, event) = self
            .inner
            .database
            .complete_generation_with_text_metadata(generation_id, content, metadata)
            .await?;
        self.publish(event).await;
        Ok(message)
    }

    pub async fn cancel_with_text(&self, generation_id: Uuid, content: String) -> Result<Message> {
        let (message, event) = self
            .inner
            .database
            .cancel_generation_with_text(generation_id, content)
            .await?;
        self.publish(event).await;
        Ok(message)
    }

    pub(crate) async fn subscribe(
        &self,
        generation_id: Uuid,
    ) -> broadcast::Receiver<GenerationEvent> {
        self.sender(generation_id).await.subscribe()
    }

    pub(crate) async fn replay(
        &self,
        generation_id: Uuid,
        after_sequence: i64,
    ) -> Result<Vec<GenerationEvent>> {
        self.inner
            .database
            .list_generation_events(generation_id, after_sequence, 10_000)
            .await
    }

    async fn sender(&self, generation_id: Uuid) -> broadcast::Sender<GenerationEvent> {
        let mut senders = self.inner.senders.lock().await;
        senders
            .entry(generation_id)
            .or_insert_with(|| broadcast::channel(EVENT_CHANNEL_CAPACITY).0)
            .clone()
    }

    async fn publish(&self, event: GenerationEvent) {
        let sender = self.sender(event.generation_id).await;
        let _ = sender.send(event);
    }
}
