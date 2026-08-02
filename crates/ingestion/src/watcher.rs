use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use northbridge_domain::{KnowledgeWatchKind, KnowledgeWatchRoot};
use northbridge_storage::NewKnowledgeWatchRoot;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{
    IngestionError, IngestionService, Result, is_supported_document,
    service::source_uri_without_canonicalization,
};

#[derive(Clone)]
pub struct KnowledgeWatchManager {
    inner: Arc<KnowledgeWatchManagerInner>,
}

struct KnowledgeWatchManagerInner {
    ingestion: IngestionService,
    watcher: Mutex<RecommendedWatcher>,
    registered: RwLock<HashMap<Uuid, KnowledgeWatchRoot>>,
    shutdown: CancellationToken,
}

impl KnowledgeWatchManager {
    pub async fn open(ingestion: IngestionService) -> Result<Self> {
        let (sender, receiver) = mpsc::unbounded_channel::<notify::Result<Event>>();
        let watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })?;
        let manager = Self {
            inner: Arc::new(KnowledgeWatchManagerInner {
                ingestion,
                watcher: Mutex::new(watcher),
                registered: RwLock::new(HashMap::new()),
                shutdown: CancellationToken::new(),
            }),
        };
        let event_manager = manager.clone();
        tokio::spawn(async move { event_manager.event_loop(receiver).await });
        let roots = manager
            .inner
            .ingestion
            .database()
            .list_knowledge_watch_roots(None)
            .await?;
        for root in roots {
            if let Err(error) = manager.register_existing(root.clone()).await {
                manager
                    .inner
                    .ingestion
                    .database()
                    .update_knowledge_watch_scan(root.id, Some(&error.to_string()))
                    .await?;
            }
        }
        Ok(manager)
    }

    pub async fn add_root(
        &self,
        stack_id: Uuid,
        workspace_id: Uuid,
        path: impl AsRef<Path>,
        kind: KnowledgeWatchKind,
        recursive: bool,
    ) -> Result<KnowledgeWatchRoot> {
        let path = canonical_directory(path.as_ref()).await?;
        let root = self
            .inner
            .ingestion
            .database()
            .create_knowledge_watch_root(NewKnowledgeWatchRoot {
                stack_id,
                workspace_id,
                kind,
                path: path.to_string_lossy().into_owned(),
                recursive,
            })
            .await?;
        if let Err(error) = self.register_existing(root.clone()).await {
            let _ = self
                .inner
                .ingestion
                .database()
                .delete_knowledge_watch_root(root.id)
                .await;
            return Err(error);
        }
        let scan_manager = self.clone();
        let scan_root = root.clone();
        tokio::spawn(async move {
            if let Err(error) = scan_manager.scan_root(scan_root.clone()).await {
                let _ = scan_manager
                    .inner
                    .ingestion
                    .database()
                    .update_knowledge_watch_scan(scan_root.id, Some(&error.to_string()))
                    .await;
            }
        });
        Ok(root)
    }

    pub async fn remove_root(&self, id: Uuid) -> Result<bool> {
        let root = self
            .inner
            .registered
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        if let Some(root) = root {
            let path = PathBuf::from(&root.path);
            let still_used = self
                .inner
                .registered
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .any(|registered| registered.path == root.path);
            if !still_used {
                let _ = self
                    .inner
                    .watcher
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .unwatch(&path);
            }
        }
        self.inner
            .ingestion
            .database()
            .delete_knowledge_watch_root(id)
            .await
            .map_err(Into::into)
    }

    pub async fn rescan(&self, id: Uuid) -> Result<KnowledgeWatchRoot> {
        let root = self
            .inner
            .ingestion
            .database()
            .knowledge_watch_root(id)
            .await?;
        self.scan_root(root.clone()).await?;
        self.inner
            .ingestion
            .database()
            .knowledge_watch_root(id)
            .await
            .map_err(Into::into)
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
    }

    async fn register_existing(&self, root: KnowledgeWatchRoot) -> Result<()> {
        let path = PathBuf::from(&root.path);
        if !path.is_absolute() || !path.is_dir() {
            return Err(IngestionError::InvalidInput(format!(
                "watch root is not an existing absolute directory: {}",
                path.display()
            )));
        }
        let mode = if root.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        self.inner
            .watcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .watch(&path, mode)?;
        self.inner
            .registered
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(root.id, root);
        Ok(())
    }

    async fn scan_root(&self, root: KnowledgeWatchRoot) -> Result<()> {
        let path = PathBuf::from(&root.path);
        let recursive = root.recursive;
        let kind = root.kind;
        let files = tokio::task::spawn_blocking(move || {
            let mut walk = WalkDir::new(path).follow_links(false);
            if !recursive {
                walk = walk.max_depth(1);
            }
            walk.into_iter()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.file_type().is_file())
                .map(|entry| entry.into_path())
                .filter(|path| path_allowed(path, kind))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| IngestionError::Parse(format!("folder scanner panicked: {error}")))?;
        for file in files {
            self.inner
                .ingestion
                .start_file(root.stack_id, file, root.kind.source_kind())
                .await?;
        }
        self.inner
            .ingestion
            .database()
            .update_knowledge_watch_scan(root.id, None)
            .await?;
        Ok(())
    }

    async fn event_loop(&self, mut receiver: mpsc::UnboundedReceiver<notify::Result<Event>>) {
        let mut pending = HashMap::<PathBuf, Instant>::new();
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                () = self.inner.shutdown.cancelled() => break,
                event = receiver.recv() => {
                    let Some(event) = event else { break; };
                    match event {
                        Ok(event) => {
                            for path in event.paths {
                                pending.insert(path, Instant::now());
                            }
                        }
                        Err(error) => tracing::warn!(%error, "knowledge folder watcher reported an error"),
                    }
                }
                _ = interval.tick() => {
                    let ready = pending
                        .iter()
                        .filter(|(_, seen)| seen.elapsed() >= Duration::from_millis(500))
                        .map(|(path, _)| path.clone())
                        .collect::<Vec<_>>();
                    for path in ready {
                        pending.remove(&path);
                        if let Err(error) = self.process_path(path).await {
                            tracing::warn!(%error, "knowledge watch event could not be processed");
                        }
                    }
                }
            }
        }
    }

    async fn process_path(&self, path: PathBuf) -> Result<()> {
        let root = most_specific_root(
            &self
                .inner
                .registered
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            &path,
        );
        let Some(root) = root else {
            return Ok(());
        };
        if path.is_file() {
            if path_allowed(&path, root.kind) {
                self.inner
                    .ingestion
                    .start_file(root.stack_id, path, root.kind.source_kind())
                    .await?;
            }
        } else if let Some(uri) = source_uri_without_canonicalization(&path)
            && self
                .inner
                .ingestion
                .database()
                .delete_knowledge_document_by_source(root.stack_id, &uri)
                .await?
        {
            self.inner
                .ingestion
                .retrieval()
                .synchronize_stack(root.stack_id)
                .await?;
        }
        Ok(())
    }
}

fn most_specific_root(
    roots: &HashMap<Uuid, KnowledgeWatchRoot>,
    path: &Path,
) -> Option<KnowledgeWatchRoot> {
    roots
        .values()
        .filter(|root| path.starts_with(&root.path))
        .max_by_key(|root| Path::new(&root.path).components().count())
        .cloned()
}

fn path_allowed(path: &Path, kind: KnowledgeWatchKind) -> bool {
    if path.components().any(|component| {
        component
            .as_os_str()
            .to_string_lossy()
            .eq_ignore_ascii_case(".obsidian")
    }) {
        return false;
    }
    if kind == KnowledgeWatchKind::Obsidian {
        return path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md"));
    }
    is_supported_document(path)
}

async fn canonical_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(IngestionError::InvalidInput(
            "watch root path must be absolute".to_string(),
        ));
    }
    let canonical = tokio::fs::canonicalize(path).await?;
    if !canonical.is_dir() {
        return Err(IngestionError::InvalidInput(
            "watch root path must identify a directory".to_string(),
        ));
    }
    Ok(canonical)
}
