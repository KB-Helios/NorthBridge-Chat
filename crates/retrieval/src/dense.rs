use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use async_trait::async_trait;
use northbridge_domain::KnowledgeStack;
use northbridge_storage::Database;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use turbovec::IdMapIndex;
use uuid::Uuid;

use crate::{Result, RetrievalError};

const MANIFEST_VERSION: u32 = 1;
const BIT_WIDTH: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct VectorRecord {
    pub id: u64,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredId {
    pub id: u64,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DenseIndexStatus {
    pub stack_id: Uuid,
    pub dimensions: usize,
    pub bit_width: usize,
    pub record_count: usize,
    pub revision: u64,
    pub snapshot: Option<String>,
}

#[async_trait]
pub trait DenseIndex: Send + Sync {
    async fn upsert(&self, items: &[VectorRecord]) -> Result<()>;
    async fn remove(&self, ids: &[u64]) -> Result<()>;
    async fn search(
        &self,
        vector: &[f32],
        allowed_ids: Option<&[u64]>,
        limit: usize,
    ) -> Result<Vec<ScoredId>>;
    async fn checkpoint(&self) -> Result<()>;
    async fn rebuild(&self) -> Result<()>;
}

#[async_trait]
pub trait DenseIndexSync: DenseIndex {
    async fn synchronize(&self) -> Result<DenseIndexStatus>;
    async fn status(&self) -> Result<DenseIndexStatus>;
}

#[derive(Clone)]
pub struct TurboVecDenseIndex {
    inner: Arc<TurboVecDenseIndexInner>,
}

struct TurboVecDenseIndexInner {
    database: Database,
    stack_id: Uuid,
    dimensions: usize,
    sender: mpsc::Sender<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    version: u32,
    stack_id: Uuid,
    embedding_model: String,
    dimensions: usize,
    bit_width: usize,
    chunker_hash: String,
    revision: u64,
    record_count: usize,
    snapshot: String,
}

struct ActorState {
    index: IdMapIndex,
    directory: PathBuf,
    manifest: Manifest,
}

enum Command {
    Upsert {
        records: Vec<VectorRecord>,
        response: oneshot::Sender<Result<()>>,
    },
    Remove {
        ids: Vec<u64>,
        response: oneshot::Sender<Result<()>>,
    },
    Search {
        vector: Vec<f32>,
        allowed_ids: Option<Vec<u64>>,
        limit: usize,
        response: oneshot::Sender<Result<Vec<ScoredId>>>,
    },
    Checkpoint {
        revision: u64,
        response: oneshot::Sender<Result<()>>,
    },
    ReplaceAll {
        records: Vec<VectorRecord>,
        revision: u64,
        response: oneshot::Sender<Result<()>>,
    },
    Status {
        response: oneshot::Sender<DenseIndexStatus>,
    },
}

impl TurboVecDenseIndex {
    pub async fn open(
        database: Database,
        root: impl AsRef<Path>,
        stack: &KnowledgeStack,
    ) -> Result<Self> {
        let dimensions = usize::try_from(stack.embedding_dimensions).map_err(|_| {
            RetrievalError::InvalidInput("embedding dimensions exceed this platform".to_string())
        })?;
        if dimensions == 0 || !dimensions.is_multiple_of(8) {
            return Err(RetrievalError::InvalidInput(
                "TurboVec dimensions must be a nonzero multiple of eight".to_string(),
            ));
        }
        let directory = root.as_ref().join(stack.id.to_string());
        tokio::fs::create_dir_all(&directory).await?;
        let stack_clone = stack.clone();
        let directory_clone = directory.clone();
        let (index, manifest, valid_snapshot) =
            tokio::task::spawn_blocking(move || load_or_empty(&directory_clone, &stack_clone))
                .await
                .map_err(|error| {
                    RetrievalError::Dense(format!("index loader panicked: {error}"))
                })??;
        let (sender, receiver) = mpsc::channel(64);
        let thread_name = format!("knowledge-index-{}", &stack.id.to_string()[..8]);
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                actor_loop(
                    ActorState {
                        index,
                        directory,
                        manifest,
                    },
                    receiver,
                )
            })
            .map_err(RetrievalError::Io)?;
        let index = Self {
            inner: Arc::new(TurboVecDenseIndexInner {
                database,
                stack_id: stack.id,
                dimensions,
                sender,
            }),
        };
        if valid_snapshot {
            if index.synchronize().await.is_err() {
                index.rebuild().await?;
            }
        } else {
            index.rebuild().await?;
        }
        Ok(index)
    }

    async fn checkpoint_revision(&self, revision: u64) -> Result<()> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Checkpoint { revision, response })
            .await?;
        receive(receiver).await
    }

    async fn replace_all(&self, records: Vec<VectorRecord>, revision: u64) -> Result<()> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::ReplaceAll {
            records,
            revision,
            response,
        })
        .await?;
        receive(receiver).await
    }

    async fn send(&self, command: Command) -> Result<()> {
        self.inner
            .sender
            .send(command)
            .await
            .map_err(|_| RetrievalError::WorkerStopped)
    }
}

#[async_trait]
impl DenseIndex for TurboVecDenseIndex {
    async fn upsert(&self, items: &[VectorRecord]) -> Result<()> {
        validate_records(items, self.inner.dimensions)?;
        let (response, receiver) = oneshot::channel();
        self.send(Command::Upsert {
            records: items.to_vec(),
            response,
        })
        .await?;
        receive(receiver).await
    }

    async fn remove(&self, ids: &[u64]) -> Result<()> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Remove {
            ids: ids.to_vec(),
            response,
        })
        .await?;
        receive(receiver).await
    }

    async fn search(
        &self,
        vector: &[f32],
        allowed_ids: Option<&[u64]>,
        limit: usize,
    ) -> Result<Vec<ScoredId>> {
        if vector.len() != self.inner.dimensions || !vector.iter().all(|value| value.is_finite()) {
            return Err(RetrievalError::InvalidInput(format!(
                "query vector must contain {} finite dimensions",
                self.inner.dimensions
            )));
        }
        if !(1..=200).contains(&limit) {
            return Err(RetrievalError::InvalidInput(
                "dense search limit must be between 1 and 200".to_string(),
            ));
        }
        let (response, receiver) = oneshot::channel();
        self.send(Command::Search {
            vector: vector.to_vec(),
            allowed_ids: allowed_ids.map(<[u64]>::to_vec),
            limit,
            response,
        })
        .await?;
        receive(receiver).await
    }

    async fn checkpoint(&self) -> Result<()> {
        let revision = self
            .inner
            .database
            .knowledge_stack(self.inner.stack_id)
            .await?
            .index_revision;
        self.checkpoint_revision(revision).await
    }

    async fn rebuild(&self) -> Result<()> {
        let stack = self
            .inner
            .database
            .knowledge_stack(self.inner.stack_id)
            .await?;
        let records = self
            .inner
            .database
            .all_stack_embeddings(self.inner.stack_id)
            .await?
            .into_iter()
            .map(|embedding| VectorRecord {
                id: embedding.dense_id,
                vector: embedding.vector,
            })
            .collect::<Vec<_>>();
        validate_records(&records, self.inner.dimensions)?;
        self.replace_all(records, stack.index_revision).await?;
        self.inner
            .database
            .mark_dense_journal_applied(self.inner.stack_id, stack.index_revision)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl DenseIndexSync for TurboVecDenseIndex {
    async fn synchronize(&self) -> Result<DenseIndexStatus> {
        let status = self.status().await?;
        let stack = self
            .inner
            .database
            .knowledge_stack(self.inner.stack_id)
            .await?;
        if status.revision == stack.index_revision {
            return Ok(status);
        }
        if status.revision > stack.index_revision {
            self.rebuild().await?;
            return self.status().await;
        }
        let journal = self
            .inner
            .database
            .dense_journal_after(self.inner.stack_id, status.revision)
            .await?;
        if journal.is_empty()
            || !revisions_are_contiguous(status.revision, stack.index_revision, &journal)
        {
            self.rebuild().await?;
            return self.status().await;
        }

        for entry in journal {
            match entry.operation.as_str() {
                "remove" => self.remove(&[entry.dense_id]).await?,
                "upsert" => {
                    if let Some(embedding) =
                        self.inner.database.stack_embedding(entry.dense_id).await?
                    {
                        self.upsert(&[VectorRecord {
                            id: embedding.dense_id,
                            vector: embedding.vector,
                        }])
                        .await?;
                    }
                }
                operation => {
                    return Err(RetrievalError::Dense(format!(
                        "unknown journal operation {operation:?}"
                    )));
                }
            }
        }
        self.checkpoint_revision(stack.index_revision).await?;
        self.inner
            .database
            .mark_dense_journal_applied(self.inner.stack_id, stack.index_revision)
            .await?;
        self.status().await
    }

    async fn status(&self) -> Result<DenseIndexStatus> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Status { response }).await?;
        receiver.await.map_err(|_| RetrievalError::WorkerStopped)
    }
}

fn actor_loop(mut state: ActorState, mut receiver: mpsc::Receiver<Command>) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::Upsert { records, response } => {
                let result = actor_upsert(&mut state.index, records);
                let _ = response.send(result);
            }
            Command::Remove { ids, response } => {
                for id in ids {
                    state.index.remove(id);
                }
                let _ = response.send(Ok(()));
            }
            Command::Search {
                vector,
                allowed_ids,
                limit,
                response,
            } => {
                let result = actor_search(&state.index, &vector, allowed_ids.as_deref(), limit);
                let _ = response.send(result);
            }
            Command::Checkpoint { revision, response } => {
                let result = write_checkpoint(&mut state, revision);
                let _ = response.send(result);
            }
            Command::ReplaceAll {
                records,
                revision,
                response,
            } => {
                let result = replace_all(&mut state, records, revision);
                let _ = response.send(result);
            }
            Command::Status { response } => {
                let _ = response.send(status_from_state(&state));
            }
        }
    }
}

fn actor_upsert(index: &mut IdMapIndex, records: Vec<VectorRecord>) -> Result<()> {
    for record in &records {
        index.remove(record.id);
    }
    if records.is_empty() {
        return Ok(());
    }
    let ids = records.iter().map(|record| record.id).collect::<Vec<_>>();
    let vectors = records
        .iter()
        .flat_map(|record| record.vector.iter().copied())
        .collect::<Vec<_>>();
    index
        .add_with_ids(&vectors, &ids)
        .map_err(|error| RetrievalError::Dense(error.to_string()))
}

fn actor_search(
    index: &IdMapIndex,
    vector: &[f32],
    allowed_ids: Option<&[u64]>,
    limit: usize,
) -> Result<Vec<ScoredId>> {
    if index.is_empty() {
        return Ok(Vec::new());
    }
    let filtered = allowed_ids.map(|ids| {
        ids.iter()
            .copied()
            .filter(|id| index.contains(*id))
            .collect::<Vec<_>>()
    });
    if filtered.as_ref().is_some_and(Vec::is_empty) {
        return Ok(Vec::new());
    }
    let effective = filtered.as_ref().map_or(index.len(), Vec::len).min(limit);
    if effective == 0 {
        return Ok(Vec::new());
    }
    let (scores, ids) = index.search_with_allowlist(vector, effective, filtered.as_deref());
    Ok(ids
        .into_iter()
        .zip(scores)
        .map(|(id, score)| ScoredId { id, score })
        .collect())
}

fn replace_all(state: &mut ActorState, records: Vec<VectorRecord>, revision: u64) -> Result<()> {
    let mut index = IdMapIndex::new(state.manifest.dimensions, state.manifest.bit_width)
        .map_err(|error| RetrievalError::Dense(error.to_string()))?;
    actor_upsert(&mut index, records)?;
    state.index = index;
    write_checkpoint(state, revision)
}

fn write_checkpoint(state: &mut ActorState, revision: u64) -> Result<()> {
    std::fs::create_dir_all(&state.directory)?;
    let snapshot = format!("snapshot-{revision}-{}.tvim", Uuid::new_v4().simple());
    let snapshot_path = state.directory.join(&snapshot);
    state.index.write(&snapshot_path)?;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&snapshot_path)?
        .sync_all()?;

    let old_snapshot = state.manifest.snapshot.clone();
    let mut manifest = state.manifest.clone();
    manifest.revision = revision;
    manifest.record_count = state.index.len();
    manifest.snapshot = snapshot;
    let temporary = state
        .directory
        .join(format!(".manifest-{}.tmp", Uuid::new_v4().simple()));
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        serde_json::to_writer_pretty(&mut file, &manifest)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    let manifest_path = state.directory.join("manifest.json");
    replace_file(&temporary, &manifest_path)?;
    state.manifest = manifest;
    if !old_snapshot.is_empty() && old_snapshot != state.manifest.snapshot {
        let _ = std::fs::remove_file(state.directory.join(old_snapshot));
    }
    Ok(())
}

fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    if destination.exists() {
        let backup = destination.with_extension(format!("bak-{}", Uuid::new_v4().simple()));
        std::fs::rename(destination, &backup)?;
        match std::fs::rename(source, destination) {
            Ok(()) => {
                let _ = std::fs::remove_file(backup);
                return Ok(());
            }
            Err(error) => {
                let _ = std::fs::rename(&backup, destination);
                return Err(error);
            }
        }
    }
    std::fs::rename(source, destination)
}

fn load_or_empty(directory: &Path, stack: &KnowledgeStack) -> Result<(IdMapIndex, Manifest, bool)> {
    let empty_manifest = Manifest {
        version: MANIFEST_VERSION,
        stack_id: stack.id,
        embedding_model: stack.embedding_model.clone(),
        dimensions: stack.embedding_dimensions as usize,
        bit_width: BIT_WIDTH,
        chunker_hash: stack.chunker_hash.clone(),
        revision: 0,
        record_count: 0,
        snapshot: String::new(),
    };
    let manifest_path = directory.join("manifest.json");
    let Ok(contents) = std::fs::read(&manifest_path) else {
        return Ok((
            IdMapIndex::new(empty_manifest.dimensions, BIT_WIDTH)
                .map_err(|error| RetrievalError::Dense(error.to_string()))?,
            empty_manifest,
            false,
        ));
    };
    let Ok(manifest) = serde_json::from_slice::<Manifest>(&contents) else {
        return Ok((
            IdMapIndex::new(empty_manifest.dimensions, BIT_WIDTH)
                .map_err(|error| RetrievalError::Dense(error.to_string()))?,
            empty_manifest,
            false,
        ));
    };
    if manifest.version != MANIFEST_VERSION
        || manifest.stack_id != stack.id
        || manifest.embedding_model != stack.embedding_model
        || manifest.dimensions != stack.embedding_dimensions as usize
        || manifest.bit_width != BIT_WIDTH
        || manifest.chunker_hash != stack.chunker_hash
        || manifest.snapshot.is_empty()
    {
        return Ok((
            IdMapIndex::new(empty_manifest.dimensions, BIT_WIDTH)
                .map_err(|error| RetrievalError::Dense(error.to_string()))?,
            empty_manifest,
            false,
        ));
    }
    let snapshot_path = directory.join(&manifest.snapshot);
    match IdMapIndex::load(snapshot_path) {
        Ok(index)
            if index.dim() == manifest.dimensions
                && index.bit_width() == manifest.bit_width
                && index.len() == manifest.record_count =>
        {
            Ok((index, manifest, true))
        }
        _ => Ok((
            IdMapIndex::new(empty_manifest.dimensions, BIT_WIDTH)
                .map_err(|error| RetrievalError::Dense(error.to_string()))?,
            empty_manifest,
            false,
        )),
    }
}

fn validate_records(records: &[VectorRecord], dimensions: usize) -> Result<()> {
    let mut ids = BTreeSet::new();
    for record in records {
        if record.id == 0 || !ids.insert(record.id) {
            return Err(RetrievalError::InvalidInput(
                "dense vector IDs must be unique and nonzero".to_string(),
            ));
        }
        if record.vector.len() != dimensions || !record.vector.iter().all(|value| value.is_finite())
        {
            return Err(RetrievalError::InvalidInput(format!(
                "each vector must contain {dimensions} finite dimensions"
            )));
        }
    }
    Ok(())
}

fn revisions_are_contiguous(
    previous: u64,
    target: u64,
    journal: &[northbridge_storage::DenseJournalEntry],
) -> bool {
    let revisions = journal
        .iter()
        .map(|entry| entry.revision)
        .collect::<BTreeSet<_>>();
    revisions == ((previous + 1)..=target).collect::<BTreeSet<_>>()
}

fn status_from_state(state: &ActorState) -> DenseIndexStatus {
    DenseIndexStatus {
        stack_id: state.manifest.stack_id,
        dimensions: state.manifest.dimensions,
        bit_width: state.manifest.bit_width,
        record_count: state.index.len(),
        revision: state.manifest.revision,
        snapshot: (!state.manifest.snapshot.is_empty()).then(|| state.manifest.snapshot.clone()),
    }
}

async fn receive<T>(receiver: oneshot::Receiver<Result<T>>) -> Result<T> {
    receiver.await.map_err(|_| RetrievalError::WorkerStopped)?
}
