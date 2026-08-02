use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use northbridge_ai_gateway::{LocalAiRuntime, LocalAiRuntimeFactory, ModelAlias};
use northbridge_ingestion::{IngestionConfig, IngestionService, KnowledgeWatchManager};
use northbridge_model_manager::ModelManager;
use northbridge_retrieval::{
    EmbeddingProvider, KnowledgeRetrieval, RetrievalConfig, RetrievalError,
};
use northbridge_storage::Database;
use thiserror::Error;
use tokio::sync::Mutex;

pub const EMBEDDING_MODEL_ALIAS: &str = "embedding-default";
pub const DEFAULT_EMBEDDING_MODEL: &str = "Qwen/Qwen3-Embedding-0.6B";
pub const DEFAULT_EMBEDDING_DIMENSIONS: u32 = 1024;

#[derive(Debug, Clone)]
pub struct KnowledgeRuntimeConfig {
    pub blob_directory: PathBuf,
    pub index_directory: PathBuf,
    pub default_embedding_model: String,
    pub default_embedding_dimensions: u32,
}

impl KnowledgeRuntimeConfig {
    #[must_use]
    pub fn local(data_directory: impl Into<PathBuf>) -> Self {
        let root = data_directory.into().join("knowledge");
        Self {
            blob_directory: root.join("blobs"),
            index_directory: root.join("indexes"),
            default_embedding_model: DEFAULT_EMBEDDING_MODEL.to_string(),
            default_embedding_dimensions: DEFAULT_EMBEDDING_DIMENSIONS,
        }
    }

    fn validate(&self) -> Result<(), KnowledgeStartupError> {
        if !self.blob_directory.is_absolute() || !self.index_directory.is_absolute() {
            return Err(KnowledgeStartupError::InvalidConfig(
                "knowledge blob and index directories must be absolute".to_string(),
            ));
        }
        if self.default_embedding_model.trim().is_empty()
            || self.default_embedding_model.len() > 512
        {
            return Err(KnowledgeStartupError::InvalidConfig(
                "default embedding model must be between 1 and 512 characters".to_string(),
            ));
        }
        if !(8..=4096).contains(&self.default_embedding_dimensions)
            || !self.default_embedding_dimensions.is_multiple_of(8)
        {
            return Err(KnowledgeStartupError::InvalidConfig(
                "default embedding dimensions must be a multiple of eight between 8 and 4,096"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum KnowledgeStartupError {
    #[error("invalid knowledge runtime configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    Ingestion(#[from] northbridge_ingestion::IngestionError),
    #[error(transparent)]
    Retrieval(#[from] northbridge_retrieval::RetrievalError),
}

#[derive(Clone)]
pub struct KnowledgeServices {
    pub(crate) retrieval: KnowledgeRetrieval,
    pub(crate) ingestion: IngestionService,
    pub(crate) watchers: KnowledgeWatchManager,
    embeddings: Option<Arc<GatewayEmbeddingProvider>>,
    pub(crate) config: KnowledgeRuntimeConfig,
}

impl KnowledgeServices {
    pub async fn open(
        database: Database,
        config: KnowledgeRuntimeConfig,
        ai_factory: Option<LocalAiRuntimeFactory>,
        model_manager: Option<ModelManager>,
    ) -> Result<Self, KnowledgeStartupError> {
        let embeddings = Arc::new(GatewayEmbeddingProvider {
            factory: ai_factory,
            model_manager,
            runtimes: Mutex::new(HashMap::new()),
        });
        let provider: Arc<dyn EmbeddingProvider> = embeddings.clone();
        Self::open_with_provider(database, config, provider, Some(embeddings)).await
    }

    #[cfg(test)]
    pub(crate) async fn open_for_test(
        database: Database,
        config: KnowledgeRuntimeConfig,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<Self, KnowledgeStartupError> {
        Self::open_with_provider(database, config, provider, None).await
    }

    async fn open_with_provider(
        database: Database,
        config: KnowledgeRuntimeConfig,
        provider: Arc<dyn EmbeddingProvider>,
        embeddings: Option<Arc<GatewayEmbeddingProvider>>,
    ) -> Result<Self, KnowledgeStartupError> {
        config.validate()?;
        let retrieval = KnowledgeRetrieval::open(
            database.clone(),
            RetrievalConfig::local(config.index_directory.clone()),
            provider,
        )
        .await?;
        let ingestion = IngestionService::open(
            database,
            retrieval.clone(),
            IngestionConfig::local(config.blob_directory.clone()),
        )
        .await?;
        let watchers = KnowledgeWatchManager::open(ingestion.clone()).await?;
        Ok(Self {
            retrieval,
            ingestion,
            watchers,
            embeddings,
            config,
        })
    }

    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.watchers.shutdown();
        let ingestion_stopped = self.ingestion.shutdown(timeout).await;
        let embeddings_stopped = match &self.embeddings {
            Some(embeddings) => embeddings.shutdown().await,
            None => true,
        };
        ingestion_stopped && embeddings_stopped
    }
}

#[derive(Clone, Default)]
pub(crate) struct KnowledgeRuntimeRegistry {
    inner: Arc<std::sync::RwLock<Option<KnowledgeServices>>>,
}

impl KnowledgeRuntimeRegistry {
    pub(crate) fn current(&self) -> Option<KnowledgeServices> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn replace(&self, services: KnowledgeServices) {
        *self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(services);
    }
}

struct GatewayEmbeddingProvider {
    factory: Option<LocalAiRuntimeFactory>,
    model_manager: Option<ModelManager>,
    runtimes: Mutex<HashMap<String, LocalAiRuntime>>,
}

impl GatewayEmbeddingProvider {
    async fn runtime_for(
        &self,
        physical_model: &str,
    ) -> northbridge_retrieval::Result<LocalAiRuntime> {
        let mut runtimes = self.runtimes.lock().await;
        if let Some(runtime) = runtimes.get(physical_model) {
            return Ok(runtime.clone());
        }
        let mut factory = self.factory.clone().ok_or_else(|| {
            RetrievalError::Embedding(
                "the verified agentgateway/mistral.rs bundle is unavailable".to_string(),
            )
        })?;
        factory.model_alias = ModelAlias::local_embedding(EMBEDDING_MODEL_ALIAS)
            .map_err(|error| RetrievalError::Embedding(error.to_string()))?;
        let runtime_reference = match &self.model_manager {
            Some(manager) => manager
                .runtime_reference(physical_model)
                .await
                .map_err(|error| RetrievalError::Embedding(error.to_string()))?
                .map_or_else(|| physical_model.into(), std::path::PathBuf::into_os_string),
            None => physical_model.into(),
        };
        let runtime = factory
            .prepare(runtime_reference)
            .await
            .map_err(|error| RetrievalError::Embedding(error.to_string()))?;
        runtimes.insert(physical_model.to_string(), runtime.clone());
        Ok(runtime)
    }

    async fn shutdown(&self) -> bool {
        let runtimes = std::mem::take(&mut *self.runtimes.lock().await);
        let mut clean = true;
        for (model, runtime) in runtimes {
            if let Err(error) = runtime.shutdown().await {
                tracing::error!(%model, %error, "failed to shut down embedding runtime");
                clean = false;
            }
        }
        clean
    }
}

#[async_trait]
impl EmbeddingProvider for GatewayEmbeddingProvider {
    async fn embed(
        &self,
        physical_model: &str,
        expected_dimensions: usize,
        inputs: &[String],
    ) -> northbridge_retrieval::Result<Vec<Vec<f32>>> {
        let runtime = self.runtime_for(physical_model).await?;
        let response = runtime
            .embeddings(inputs)
            .await
            .map_err(|error| RetrievalError::Embedding(error.to_string()))?;
        let vectors = response
            .data
            .into_iter()
            .map(|item| item.embedding)
            .collect::<Vec<_>>();
        if vectors
            .iter()
            .any(|vector| vector.len() != expected_dimensions)
        {
            let actual = vectors.first().map_or(0, Vec::len);
            return Err(RetrievalError::Embedding(format!(
                "embedding model {physical_model:?} returned {actual} dimensions; the stack requires {expected_dimensions}"
            )));
        }
        Ok(vectors)
    }
}
