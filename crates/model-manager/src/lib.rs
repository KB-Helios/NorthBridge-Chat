use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use hf_hub::{
    HFClient, HFError,
    progress::{DownloadEvent, ProgressEvent, ProgressHandler},
    split_id,
};
use northbridge_domain::{ManagedModel, ManagedModelSource, ManagedModelStatus};
use northbridge_storage::{Database, StorageError, now_ms};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEFAULT_REVISION: &str = "main";
const DISK_SAFETY_MARGIN_BYTES: u64 = 512 * 1024 * 1024;
const MANIFEST_FILENAME: &str = ".northbridge-model.json";

pub type Result<T> = std::result::Result<T, ModelManagerError>;

#[derive(Debug, thiserror::Error)]
pub enum ModelManagerError {
    #[error("invalid model request: {0}")]
    InvalidInput(String),
    #[error("model operation conflict: {0}")]
    Conflict(String),
    #[error("managed model was not found")]
    NotFound,
    #[error("model download was cancelled")]
    Cancelled,
    #[error(
        "insufficient disk space: need {required_bytes} bytes but only {available_bytes} are available"
    )]
    InsufficientDiskSpace {
        required_bytes: u64,
        available_bytes: u64,
    },
    #[error("model integrity validation failed: {0}")]
    Integrity(String),
    #[error("Hugging Face operation failed: {0}")]
    Hub(#[from] HFError),
    #[error("model storage operation failed: {0}")]
    Storage(#[from] StorageError),
    #[error("model filesystem operation failed: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("model worker failed: {0}")]
    Worker(String),
}

#[derive(Debug, Clone)]
pub struct ModelManagerConfig {
    pub root_directory: PathBuf,
    pub hub_endpoint: Option<String>,
}

impl ModelManagerConfig {
    #[must_use]
    pub fn local(root_directory: PathBuf) -> Self {
        Self {
            root_directory,
            hub_endpoint: None,
        }
    }
}

#[derive(Clone)]
pub struct ModelManager {
    inner: Arc<ModelManagerInner>,
}

struct ModelManagerInner {
    database: Database,
    root_directory: PathBuf,
    downloads_directory: PathBuf,
    staging_directory: PathBuf,
    hub: HFClient,
    progress: Mutex<HashMap<Uuid, Arc<DownloadProgress>>>,
    jobs: AsyncMutex<HashMap<Uuid, CancellationToken>>,
}

#[derive(Debug, Clone)]
pub struct ImportModelRequest {
    pub path: PathBuf,
    pub model_id: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadModelRequest {
    pub model_id: String,
    pub revision: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct DownloadSnapshot {
    bytes_downloaded: u64,
    total_bytes: Option<u64>,
    file_count: Option<u64>,
}

#[derive(Debug, Default)]
struct DownloadProgress {
    state: Mutex<DownloadProgressState>,
}

#[derive(Debug, Default)]
struct DownloadProgressState {
    total_files: Option<u64>,
    total_bytes: Option<u64>,
    files: BTreeMap<String, u64>,
    aggregate_bytes: u64,
    complete: bool,
}

impl DownloadProgress {
    fn seed(&self, total_bytes: Option<u64>, total_files: Option<u64>) {
        let mut state = lock(&self.state);
        state.total_bytes = total_bytes.or(state.total_bytes);
        state.total_files = total_files.or(state.total_files);
    }

    fn snapshot(&self) -> DownloadSnapshot {
        let state = lock(&self.state);
        let file_bytes = state.files.values().copied().sum::<u64>();
        let mut bytes_downloaded = file_bytes.max(state.aggregate_bytes);
        if state.complete {
            bytes_downloaded = state.total_bytes.unwrap_or(bytes_downloaded);
        }
        DownloadSnapshot {
            bytes_downloaded,
            total_bytes: state.total_bytes,
            file_count: state.total_files,
        }
    }
}

impl ProgressHandler for DownloadProgress {
    fn on_progress(&self, event: &ProgressEvent) {
        let ProgressEvent::Download(event) = event else {
            return;
        };
        let mut state = lock(&self.state);
        match event {
            DownloadEvent::Start {
                total_files,
                total_bytes,
            } => {
                state.total_files = u64::try_from(*total_files).ok();
                state.total_bytes = Some(*total_bytes);
            }
            DownloadEvent::Progress { files } => {
                for file in files {
                    state
                        .files
                        .insert(file.filename.clone(), file.bytes_completed);
                }
            }
            DownloadEvent::AggregateProgress {
                bytes_completed,
                total_bytes,
                ..
            } => {
                state.aggregate_bytes = *bytes_completed;
                state.total_bytes = state.total_bytes.or(Some(*total_bytes));
            }
            DownloadEvent::Complete => state.complete = true,
        }
    }
}

impl ModelManager {
    pub async fn open(database: Database, config: ModelManagerConfig) -> Result<Self> {
        if !config.root_directory.is_absolute() {
            return Err(ModelManagerError::InvalidInput(
                "model root directory must be absolute".to_string(),
            ));
        }
        let downloads_directory = config.root_directory.join("downloads");
        let staging_directory = config.root_directory.join("staging");
        let hub_cache = config.root_directory.join("huggingface").join("hub");
        for directory in [
            &config.root_directory,
            &downloads_directory,
            &staging_directory,
            &hub_cache,
        ] {
            tokio::fs::create_dir_all(directory).await?;
        }
        let root_directory = tokio::fs::canonicalize(&config.root_directory).await?;
        let mut builder = HFClient::builder().cache_dir(hub_cache);
        if let Some(endpoint) = config.hub_endpoint {
            builder = builder.endpoint(endpoint);
        }
        let hub = builder.build()?;
        database.recover_interrupted_model_downloads().await?;
        Ok(Self {
            inner: Arc::new(ModelManagerInner {
                database,
                downloads_directory,
                staging_directory,
                root_directory,
                hub,
                progress: Mutex::new(HashMap::new()),
                jobs: AsyncMutex::new(HashMap::new()),
            }),
        })
    }

    pub async fn list(&self) -> Result<Vec<ManagedModel>> {
        let mut models = self.inner.database.managed_models().await?;
        let progress = lock(&self.inner.progress);
        for model in &mut models {
            if let Some(tracker) = progress.get(&model.id) {
                apply_progress(model, tracker.snapshot());
            }
        }
        Ok(models)
    }

    pub async fn get(&self, id: Uuid) -> Result<ManagedModel> {
        let mut model = self
            .inner
            .database
            .managed_model(id)
            .await
            .map_err(map_not_found)?;
        if let Some(tracker) = lock(&self.inner.progress).get(&id) {
            apply_progress(&mut model, tracker.snapshot());
        }
        Ok(model)
    }

    pub async fn by_model_id(&self, model_id: &str) -> Result<Option<ManagedModel>> {
        let mut model = self
            .inner
            .database
            .managed_model_by_model_id(model_id)
            .await?;
        if let Some(model) = &mut model
            && let Some(tracker) = lock(&self.inner.progress).get(&model.id)
        {
            apply_progress(model, tracker.snapshot());
        }
        Ok(model)
    }

    pub async fn start_download(&self, request: DownloadModelRequest) -> Result<ManagedModel> {
        let model_id = validate_hugging_face_id(&request.model_id)?;
        let revision = validate_revision(request.revision.as_deref().unwrap_or(DEFAULT_REVISION))?;
        let existing = self
            .inner
            .database
            .managed_model_by_model_id(&model_id)
            .await?;
        if let Some(existing) = &existing {
            if existing.status.is_ready() {
                return Ok(existing.clone());
            }
            if self.inner.jobs.lock().await.contains_key(&existing.id) {
                return Err(ModelManagerError::Conflict(
                    "this model already has an active download".to_string(),
                ));
            }
        }

        let id = existing
            .as_ref()
            .map(|model| model.id)
            .unwrap_or_else(Uuid::now_v7);
        let display_name = normalize_display_name(
            request.display_name.as_deref(),
            model_id.rsplit('/').next().unwrap_or(&model_id),
        )?;
        let final_directory = self.inner.downloads_directory.join(id.to_string());
        let timestamp = now_ms();
        let model = ManagedModel {
            id,
            model_id: model_id.clone(),
            display_name,
            source: ManagedModelSource::HuggingFace,
            source_reference: model_id,
            revision: Some(revision),
            local_path: final_directory.to_string_lossy().into_owned(),
            status: ManagedModelStatus::Queued,
            bytes_downloaded: 0,
            total_bytes: None,
            file_count: None,
            manifest_sha256: None,
            last_error: None,
            created_at_ms: existing
                .as_ref()
                .map_or(timestamp, |existing| existing.created_at_ms),
            updated_at_ms: timestamp,
        };
        self.inner.database.upsert_managed_model(&model).await?;

        let tracker = Arc::new(DownloadProgress::default());
        let cancellation = CancellationToken::new();
        lock(&self.inner.progress).insert(id, Arc::clone(&tracker));
        self.inner
            .jobs
            .lock()
            .await
            .insert(id, cancellation.clone());
        let manager = self.clone();
        let task_model = model.clone();
        tokio::spawn(async move {
            manager
                .drive_download(task_model, tracker, cancellation)
                .await;
            manager.inner.jobs.lock().await.remove(&id);
        });
        Ok(model)
    }

    pub async fn import(&self, request: ImportModelRequest) -> Result<ManagedModel> {
        let path = tokio::fs::canonicalize(&request.path)
            .await
            .map_err(|error| {
                ModelManagerError::InvalidInput(format!(
                    "model import path {} is unavailable: {error}",
                    request.path.display()
                ))
            })?;
        if !tokio::fs::metadata(&path).await?.is_dir() {
            return Err(ModelManagerError::InvalidInput(
                "model import path must be a directory".to_string(),
            ));
        }
        let inspection_path = path.clone();
        let inspection = tokio::task::spawn_blocking(move || inspect_model(&inspection_path))
            .await
            .map_err(|error| ModelManagerError::Worker(error.to_string()))??;
        let default_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("Imported model");
        let display_name = normalize_display_name(request.display_name.as_deref(), default_name)?;
        let model_id = match request.model_id {
            Some(model_id) => validate_catalog_model_id(&model_id)?,
            None => format!(
                "imported/{}-{}",
                slug(default_name),
                &inspection.sha256[..12]
            ),
        };
        if let Some(existing) = self
            .inner
            .database
            .managed_model_by_model_id(&model_id)
            .await?
        {
            if existing.source_reference == path.to_string_lossy() {
                return Ok(existing);
            }
            return Err(ModelManagerError::Conflict(format!(
                "model ID {model_id} is already registered"
            )));
        }
        let timestamp = now_ms();
        let model = ManagedModel {
            id: Uuid::now_v7(),
            model_id,
            display_name,
            source: ManagedModelSource::Imported,
            source_reference: path.to_string_lossy().into_owned(),
            revision: None,
            local_path: path.to_string_lossy().into_owned(),
            status: ManagedModelStatus::Imported,
            bytes_downloaded: inspection.total_bytes,
            total_bytes: Some(inspection.total_bytes),
            file_count: Some(inspection.file_count),
            manifest_sha256: Some(inspection.sha256),
            last_error: None,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        };
        self.inner.database.upsert_managed_model(&model).await?;
        Ok(model)
    }

    pub async fn runtime_reference(&self, model_id: &str) -> Result<Option<PathBuf>> {
        let Some(model) = self.by_model_id(model_id).await? else {
            return Ok(None);
        };
        if !model.status.is_ready() {
            return Err(ModelManagerError::Conflict(format!(
                "model {model_id} is not ready; current state is {}",
                model.status.as_str()
            )));
        }
        let path = PathBuf::from(&model.local_path);
        if !tokio::fs::try_exists(&path).await? {
            return Err(ModelManagerError::Integrity(format!(
                "registered model path is missing: {}",
                path.display()
            )));
        }
        Ok(Some(path))
    }

    pub async fn cancel_download(&self, id: Uuid) -> Result<ManagedModel> {
        let jobs = self.inner.jobs.lock().await;
        let cancellation = jobs.get(&id).ok_or_else(|| {
            ModelManagerError::Conflict("the model does not have an active download".to_string())
        })?;
        cancellation.cancel();
        drop(jobs);
        self.get(id).await
    }

    pub async fn remove(&self, id: Uuid) -> Result<()> {
        if self.inner.jobs.lock().await.contains_key(&id) {
            return Err(ModelManagerError::Conflict(
                "cancel the active model download before removing it".to_string(),
            ));
        }
        let model = self.get(id).await?;
        if model.source == ManagedModelSource::HuggingFace {
            let path = PathBuf::from(&model.local_path);
            if tokio::fs::try_exists(&path).await? {
                ensure_managed_download_path(&self.inner.downloads_directory, &path).await?;
                tokio::fs::remove_dir_all(&path).await?;
            }
            let staging = self.inner.staging_directory.join(id.to_string());
            if tokio::fs::try_exists(&staging).await? {
                ensure_managed_download_path(&self.inner.staging_directory, &staging).await?;
                tokio::fs::remove_dir_all(staging).await?;
            }
        }
        self.inner.database.delete_managed_model(id).await?;
        lock(&self.inner.progress).remove(&id);
        Ok(())
    }

    pub async fn shutdown(&self, timeout: Duration) -> bool {
        {
            let jobs = self.inner.jobs.lock().await;
            for cancellation in jobs.values() {
                cancellation.cancel();
            }
        }
        let deadline = Instant::now() + timeout;
        loop {
            if self.inner.jobs.lock().await.is_empty() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn drive_download(
        &self,
        mut model: ManagedModel,
        tracker: Arc<DownloadProgress>,
        cancellation: CancellationToken,
    ) {
        model.status = ManagedModelStatus::Downloading;
        model.updated_at_ms = now_ms();
        let _ = self.inner.database.upsert_managed_model(&model).await;
        let result = self
            .download_model(&model, Arc::clone(&tracker), cancellation)
            .await;
        let snapshot = tracker.snapshot();
        apply_progress(&mut model, snapshot);
        model.updated_at_ms = now_ms();
        match result {
            Ok(inspection) => {
                model.status = ManagedModelStatus::Installed;
                model.local_path = inspection.path.to_string_lossy().into_owned();
                model.bytes_downloaded = inspection.total_bytes;
                model.total_bytes = Some(inspection.total_bytes);
                model.file_count = Some(inspection.file_count);
                model.manifest_sha256 = Some(inspection.sha256);
                model.last_error = None;
            }
            Err(ModelManagerError::Cancelled) => {
                model.status = ManagedModelStatus::Cancelled;
                model.last_error =
                    Some("download cancelled; partial files were kept for resume".to_string());
            }
            Err(error) => {
                tracing::error!(model_id = %model.model_id, %error, "model download failed");
                model.status = ManagedModelStatus::Failed;
                model.last_error = Some(error.to_string().chars().take(2048).collect());
            }
        }
        let _ = self.inner.database.upsert_managed_model(&model).await;
        lock(&self.inner.progress).remove(&model.id);
    }

    async fn download_model(
        &self,
        model: &ManagedModel,
        tracker: Arc<DownloadProgress>,
        cancellation: CancellationToken,
    ) -> Result<InstalledInspection> {
        let final_directory = PathBuf::from(&model.local_path);
        if tokio::fs::try_exists(&final_directory).await? {
            let inspection_path = final_directory.clone();
            let inspection = tokio::task::spawn_blocking(move || inspect_model(&inspection_path))
                .await
                .map_err(|error| ModelManagerError::Worker(error.to_string()))??;
            return Ok(inspection.with_path(final_directory));
        }

        let revision = model.revision.as_deref().unwrap_or(DEFAULT_REVISION);
        let (owner, name) = split_id(&model.source_reference);
        let repository = self.inner.hub.model(owner.to_string(), name.to_string());
        let info = repository
            .info()
            .revision(revision.to_string())
            .send()
            .await?;
        let total_bytes = info.used_storage.or_else(|| {
            info.siblings.as_ref().map(|siblings| {
                siblings
                    .iter()
                    .filter_map(|sibling| sibling.size)
                    .sum::<u64>()
            })
        });
        let file_count = info
            .siblings
            .as_ref()
            .and_then(|siblings| u64::try_from(siblings.len()).ok());
        tracker.seed(total_bytes, file_count);
        if let Some(total_bytes) = total_bytes {
            check_disk_space(&self.inner.root_directory, total_bytes)?;
        }
        let snapshot = tracker.snapshot();
        let _ = self
            .inner
            .database
            .update_managed_model_progress(
                model.id,
                snapshot.bytes_downloaded,
                snapshot.total_bytes,
                snapshot.file_count,
            )
            .await;

        let staging = self.inner.staging_directory.join(model.id.to_string());
        tokio::fs::create_dir_all(&staging).await?;
        let download = repository
            .snapshot_download()
            .revision(revision.to_string())
            .local_dir(staging.clone())
            .max_workers(4)
            .progress(Arc::clone(&tracker))
            .send();
        tokio::pin!(download);
        let mut progress_tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return Err(ModelManagerError::Cancelled),
                result = &mut download => {
                    result?;
                    break;
                }
                _ = progress_tick.tick() => {
                    let snapshot = tracker.snapshot();
                    let _ = self.inner.database.update_managed_model_progress(
                        model.id,
                        snapshot.bytes_downloaded,
                        snapshot.total_bytes,
                        snapshot.file_count,
                    ).await;
                }
            }
        }

        let inspection_path = staging.clone();
        let inspection = tokio::task::spawn_blocking(move || inspect_model(&inspection_path))
            .await
            .map_err(|error| ModelManagerError::Worker(error.to_string()))??;
        let manifest = DownloadManifest {
            schema_version: 1,
            model_id: model.model_id.clone(),
            revision: revision.to_string(),
            sha256: inspection.sha256.clone(),
            total_bytes: inspection.total_bytes,
            file_count: inspection.file_count,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            ModelManagerError::Integrity(format!("failed to serialize model manifest: {error}"))
        })?;
        tokio::fs::write(staging.join(MANIFEST_FILENAME), manifest_bytes).await?;
        tokio::fs::rename(&staging, &final_directory).await?;
        Ok(inspection.with_path(final_directory))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadManifest {
    schema_version: u32,
    model_id: String,
    revision: String,
    sha256: String,
    total_bytes: u64,
    file_count: u64,
}

#[derive(Debug)]
struct ModelInspection {
    sha256: String,
    total_bytes: u64,
    file_count: u64,
}

impl ModelInspection {
    fn with_path(self, path: PathBuf) -> InstalledInspection {
        InstalledInspection {
            path,
            sha256: self.sha256,
            total_bytes: self.total_bytes,
            file_count: self.file_count,
        }
    }
}

#[derive(Debug)]
struct InstalledInspection {
    path: PathBuf,
    sha256: String,
    total_bytes: u64,
    file_count: u64,
}

fn inspect_model(root: &Path) -> Result<ModelInspection> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path.strip_prefix(root).map_err(|error| {
                ModelManagerError::Integrity(format!("model path escaped its root: {error}"))
            })?;
            if relative
                .components()
                .next()
                .is_some_and(|component| component.as_os_str() == ".cache")
            {
                continue;
            }
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file()
                && relative
                    .file_name()
                    .is_none_or(|name| name != MANIFEST_FILENAME)
            {
                files.push(path);
            }
        }
    }
    files.sort();
    let has_config = files.iter().any(|path| has_filename(path, "config.json"));
    let has_tokenizer = files.iter().any(|path| {
        matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some("tokenizer.json" | "tokenizer.model" | "tokenizer_config.json")
        )
    });
    let has_plain_weights = files.iter().any(|path| {
        matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("safetensors" | "bin")
        )
    });
    let has_self_describing_weights = files.iter().any(|path| {
        matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("gguf" | "ggml")
        )
    });
    if !has_plain_weights && !has_self_describing_weights {
        return Err(ModelManagerError::Integrity(
            "model directory has no safetensors, bin, GGUF, or GGML weights".to_string(),
        ));
    }
    if has_plain_weights && (!has_config || !has_tokenizer) {
        return Err(ModelManagerError::Integrity(
            "plain model weights require config.json and tokenizer metadata".to_string(),
        ));
    }

    let mut hasher = Sha256::new();
    let mut total_bytes = 0_u64;
    for path in &files {
        let relative = path
            .strip_prefix(root)
            .expect("collected path is beneath root");
        let normalized = relative.to_string_lossy().replace('\\', "/");
        hasher.update(normalized.as_bytes());
        hasher.update([0]);
        let mut file = File::open(path)?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            total_bytes = total_bytes.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        }
        hasher.update([0xff]);
    }
    Ok(ModelInspection {
        sha256: format!("{:x}", hasher.finalize()),
        total_bytes,
        file_count: u64::try_from(files.len()).unwrap_or(u64::MAX),
    })
}

fn validate_hugging_face_id(value: &str) -> Result<String> {
    let value = validate_catalog_model_id(value)?;
    if value.starts_with("imported/") {
        return Err(ModelManagerError::InvalidInput(
            "Hugging Face model IDs cannot use the reserved imported/ namespace".to_string(),
        ));
    }
    Ok(value)
}

fn validate_catalog_model_id(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 256 {
        return Err(ModelManagerError::InvalidInput(
            "model ID must contain between 1 and 256 characters".to_string(),
        ));
    }
    if value.contains("..")
        || value.starts_with('/')
        || value.ends_with('/')
        || value.split('/').any(str::is_empty)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
    {
        return Err(ModelManagerError::InvalidInput(
            "model ID contains unsupported or unsafe characters".to_string(),
        ));
    }
    Ok(value.to_string())
}

fn validate_revision(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 128
        || value.contains("..")
        || value.starts_with('/')
        || value.ends_with('/')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
    {
        return Err(ModelManagerError::InvalidInput(
            "model revision contains unsupported or unsafe characters".to_string(),
        ));
    }
    Ok(value.to_string())
}

fn normalize_display_name(value: Option<&str>, fallback: &str) -> Result<String> {
    let value = value.unwrap_or(fallback).trim();
    if value.is_empty() || value.chars().count() > 120 {
        return Err(ModelManagerError::InvalidInput(
            "model display name must contain between 1 and 120 characters".to_string(),
        ));
    }
    Ok(value.to_string())
}

fn slug(value: &str) -> String {
    let slug = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    slug.trim_matches('-').chars().take(48).collect::<String>()
}

fn has_filename(path: &Path, filename: &str) -> bool {
    path.file_name().is_some_and(|name| name == filename)
}

fn check_disk_space(directory: &Path, model_bytes: u64) -> Result<()> {
    let available_bytes = fs2::available_space(directory)?;
    let required_bytes = model_bytes.saturating_add(DISK_SAFETY_MARGIN_BYTES);
    if available_bytes < required_bytes {
        return Err(ModelManagerError::InsufficientDiskSpace {
            required_bytes,
            available_bytes,
        });
    }
    Ok(())
}

async fn ensure_managed_download_path(root: &Path, candidate: &Path) -> Result<()> {
    let root = tokio::fs::canonicalize(root).await?;
    let candidate = tokio::fs::canonicalize(candidate).await?;
    if candidate == root || !candidate.starts_with(&root) {
        return Err(ModelManagerError::Integrity(format!(
            "refusing to remove unmanaged model path {}",
            candidate.display()
        )));
    }
    Ok(())
}

fn apply_progress(model: &mut ManagedModel, snapshot: DownloadSnapshot) {
    model.bytes_downloaded = snapshot.bytes_downloaded;
    model.total_bytes = snapshot.total_bytes.or(model.total_bytes);
    model.file_count = snapshot.file_count.or(model.file_count);
}

fn map_not_found(error: StorageError) -> ModelManagerError {
    match error {
        StorageError::NotFound { .. } => ModelManagerError::NotFound,
        error => ModelManagerError::Storage(error),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use northbridge_domain::{ManagedModelSource, ManagedModelStatus};
    use tempfile::TempDir;

    use super::{
        ImportModelRequest, ModelManager, ModelManagerConfig, validate_hugging_face_id,
        validate_revision,
    };
    use northbridge_storage::Database;

    #[test]
    fn validates_hugging_face_ids_and_revisions_without_path_traversal() {
        assert_eq!(
            validate_hugging_face_id("HuggingFaceTB/SmolLM2-135M-Instruct").unwrap(),
            "HuggingFaceTB/SmolLM2-135M-Instruct"
        );
        assert!(validate_hugging_face_id("../../escape").is_err());
        assert!(validate_hugging_face_id("imported/local").is_err());
        assert!(validate_revision("refs/pr/12").is_ok());
        assert!(validate_revision("../main").is_err());
    }

    #[tokio::test]
    async fn imports_resolves_and_unregisters_without_deleting_the_source() {
        let directory = TempDir::new().unwrap();
        let database = Database::connect(&directory.path().join("models.sqlite"))
            .await
            .unwrap();
        let manager = ModelManager::open(
            database,
            ModelManagerConfig::local(directory.path().join("managed")),
        )
        .await
        .unwrap();
        let imported = directory.path().join("tiny-model");
        create_valid_model(&imported);

        let model = manager
            .import(ImportModelRequest {
                path: imported.clone(),
                model_id: None,
                display_name: Some("Tiny imported model".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(model.source, ManagedModelSource::Imported);
        assert_eq!(model.status, ManagedModelStatus::Imported);
        assert_eq!(model.total_bytes, Some(6));
        assert_eq!(manager.list().await.unwrap().len(), 1);
        assert_eq!(
            manager.runtime_reference(&model.model_id).await.unwrap(),
            Some(std::fs::canonicalize(&imported).unwrap())
        );

        manager.remove(model.id).await.unwrap();
        assert!(imported.exists());
        assert!(manager.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_imports_without_runnable_model_metadata() {
        let directory = TempDir::new().unwrap();
        let database = Database::connect(&directory.path().join("models.sqlite"))
            .await
            .unwrap();
        let manager = ModelManager::open(
            database,
            ModelManagerConfig::local(directory.path().join("managed")),
        )
        .await
        .unwrap();
        let invalid = directory.path().join("invalid-model");
        std::fs::create_dir_all(&invalid).unwrap();
        std::fs::write(invalid.join("README.md"), "not a model").unwrap();
        assert!(
            manager
                .import(ImportModelRequest {
                    path: invalid,
                    model_id: None,
                    display_name: None,
                })
                .await
                .is_err()
        );
    }

    fn create_valid_model(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join("config.json"), "{}").unwrap();
        std::fs::write(path.join("tokenizer.json"), "{}").unwrap();
        std::fs::write(path.join("model.safetensors"), "xx").unwrap();
    }
}
