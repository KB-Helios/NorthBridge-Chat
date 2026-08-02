use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use northbridge_domain::{KnowledgeSearchHit, KnowledgeSearchResult, KnowledgeStack};
use northbridge_storage::{Database, KnowledgeChunkRecord};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    DenseIndex, DenseIndexSync, EmbeddingProvider, Result, RetrievalError, ScoredId,
    TurboVecDenseIndex,
};

const RRF_K: f64 = 60.0;
const MAX_QUERY_CHARACTERS: usize = 16_384;
const MAX_CACHED_QUERY_EMBEDDINGS: usize = 256;

#[derive(Debug, Clone)]
pub struct RetrievalConfig {
    pub index_directory: PathBuf,
    pub candidate_limit: u32,
}

impl RetrievalConfig {
    #[must_use]
    pub fn local(index_directory: impl Into<PathBuf>) -> Self {
        Self {
            index_directory: index_directory.into(),
            candidate_limit: 40,
        }
    }

    fn validate(&self) -> Result<()> {
        if !self.index_directory.is_absolute() {
            return Err(RetrievalError::InvalidInput(
                "knowledge index directory must be absolute".to_string(),
            ));
        }
        if !(10..=200).contains(&self.candidate_limit) {
            return Err(RetrievalError::InvalidInput(
                "retrieval candidate limit must be between 10 and 200".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct KnowledgeRetrieval {
    inner: Arc<KnowledgeRetrievalInner>,
}

struct KnowledgeRetrievalInner {
    database: Database,
    config: RetrievalConfig,
    embeddings: Arc<dyn EmbeddingProvider>,
    indexes: Mutex<HashMap<Uuid, Arc<TurboVecDenseIndex>>>,
    query_cache: Mutex<QueryEmbeddingCache>,
}

#[derive(Default)]
struct QueryEmbeddingCache {
    entries: HashMap<(String, u32, String), Vec<f32>>,
    order: VecDeque<(String, u32, String)>,
}

#[derive(Default)]
struct FusedCandidate {
    lexical_rank: Option<u32>,
    dense_rank: Option<u32>,
    score: f64,
}

impl KnowledgeRetrieval {
    pub async fn open(
        database: Database,
        config: RetrievalConfig,
        embeddings: Arc<dyn EmbeddingProvider>,
    ) -> Result<Self> {
        config.validate()?;
        tokio::fs::create_dir_all(&config.index_directory).await?;
        Ok(Self {
            inner: Arc::new(KnowledgeRetrievalInner {
                database,
                config,
                embeddings,
                indexes: Mutex::new(HashMap::new()),
                query_cache: Mutex::new(QueryEmbeddingCache::default()),
            }),
        })
    }

    #[must_use]
    pub fn database(&self) -> &Database {
        &self.inner.database
    }

    pub async fn synchronize_stack(&self, stack_id: Uuid) -> Result<crate::DenseIndexStatus> {
        let stack = self.inner.database.knowledge_stack(stack_id).await?;
        self.index_for(&stack).await?.synchronize().await
    }

    pub async fn rebuild_stack(&self, stack_id: Uuid) -> Result<crate::DenseIndexStatus> {
        let stack = self.inner.database.knowledge_stack(stack_id).await?;
        let index = self.index_for(&stack).await?;
        index.rebuild().await?;
        index.status().await
    }

    pub async fn forget_stack(&self, stack_id: Uuid) -> Result<()> {
        self.inner.indexes.lock().await.remove(&stack_id);
        let path = self.inner.config.index_directory.join(stack_id.to_string());
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn embed_chunks(
        &self,
        stack: &KnowledgeStack,
        chunks: &[String],
    ) -> Result<Vec<Vec<f32>>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let mut embeddings = self
            .inner
            .embeddings
            .embed(
                &stack.embedding_model,
                stack.embedding_dimensions as usize,
                chunks,
            )
            .await?;
        validate_embeddings(
            chunks.len(),
            stack.embedding_dimensions as usize,
            &embeddings,
        )?;
        normalize_embeddings(&mut embeddings)?;
        Ok(embeddings)
    }

    pub async fn search(
        &self,
        workspace_id: Uuid,
        stack_ids: &[Uuid],
        query: &str,
        limit: u32,
        generation_id: Option<Uuid>,
    ) -> Result<KnowledgeSearchResult> {
        let query = query.trim();
        if query.is_empty() || query.chars().count() > MAX_QUERY_CHARACTERS {
            return Err(RetrievalError::InvalidInput(format!(
                "retrieval query must be between 1 and {MAX_QUERY_CHARACTERS} characters"
            )));
        }
        if stack_ids.is_empty() || stack_ids.len() > 8 {
            return Err(RetrievalError::InvalidInput(
                "retrieval requires between one and eight knowledge stacks".to_string(),
            ));
        }
        if !(1..=50).contains(&limit) {
            return Err(RetrievalError::InvalidInput(
                "retrieval result limit must be between 1 and 50".to_string(),
            ));
        }
        let unique = stack_ids.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != stack_ids.len() {
            return Err(RetrievalError::InvalidInput(
                "knowledge stack IDs must be unique".to_string(),
            ));
        }

        let started = Instant::now();
        let mut fused = BTreeMap::<u64, FusedCandidate>::new();
        let mut lexical_candidate_count = 0_u32;
        let mut dense_candidate_count = 0_u32;
        let mut models = BTreeSet::new();
        for stack_id in stack_ids {
            let stack = self.inner.database.knowledge_stack(*stack_id).await?;
            if stack.workspace_id != workspace_id {
                return Err(RetrievalError::InvalidInput(
                    "knowledge stack does not belong to the requested workspace".to_string(),
                ));
            }
            models.insert(stack.embedding_model.clone());
            let index = self.index_for(&stack).await?;
            index.synchronize().await?;
            let lexical_future = async {
                self.inner
                    .database
                    .search_knowledge_chunks_lexical(
                        workspace_id,
                        *stack_id,
                        query,
                        self.inner.config.candidate_limit,
                    )
                    .await
                    .map_err(RetrievalError::from)
            };
            let allowed_future = async {
                self.inner
                    .database
                    .allowed_knowledge_dense_ids(workspace_id, *stack_id)
                    .await
                    .map_err(RetrievalError::from)
            };
            let embedding_future = self.query_embedding(&stack, query);
            let (lexical, allowed, embedding) =
                tokio::try_join!(lexical_future, allowed_future, embedding_future)?;
            let dense = if allowed.is_empty() {
                Vec::new()
            } else {
                index
                    .search(
                        &embedding,
                        Some(&allowed),
                        self.inner.config.candidate_limit as usize,
                    )
                    .await?
            };
            lexical_candidate_count = lexical_candidate_count
                .saturating_add(u32::try_from(lexical.len()).unwrap_or(u32::MAX));
            dense_candidate_count = dense_candidate_count
                .saturating_add(u32::try_from(dense.len()).unwrap_or(u32::MAX));
            fuse_rankings(&mut fused, &lexical, &dense);
        }

        let mut ranked = fused.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|(left_id, left), (right_id, right)| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left_id.cmp(right_id))
        });
        ranked.truncate(limit as usize);
        let ids = ranked.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let records = self
            .inner
            .database
            .knowledge_chunk_records(&ids)
            .await?
            .into_iter()
            .map(|record| (record.chunk.dense_id, record))
            .collect::<HashMap<_, _>>();
        let hits = ranked
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, (dense_id, candidate))| {
                records
                    .get(&dense_id)
                    .map(|record| hit_from_record(ordinal, record, dense_id, candidate))
            })
            .collect::<Vec<_>>();
        let result = KnowledgeSearchResult {
            trace_id: Uuid::now_v7(),
            query: query.to_string(),
            stack_ids: stack_ids.to_vec(),
            embedding_models: models.into_iter().collect(),
            lexical_candidate_count,
            dense_candidate_count,
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            hits,
        };
        self.inner
            .database
            .save_retrieval_trace(workspace_id, generation_id, &result)
            .await?;
        Ok(result)
    }

    async fn index_for(&self, stack: &KnowledgeStack) -> Result<Arc<TurboVecDenseIndex>> {
        if let Some(index) = self.inner.indexes.lock().await.get(&stack.id).cloned() {
            return Ok(index);
        }
        let opened = Arc::new(
            TurboVecDenseIndex::open(
                self.inner.database.clone(),
                &self.inner.config.index_directory,
                stack,
            )
            .await?,
        );
        let mut indexes = self.inner.indexes.lock().await;
        Ok(indexes.entry(stack.id).or_insert(opened).clone())
    }

    async fn query_embedding(&self, stack: &KnowledgeStack, query: &str) -> Result<Vec<f32>> {
        let query_hash = hex_sha256(query.as_bytes());
        let key = (
            stack.embedding_model.clone(),
            stack.embedding_dimensions,
            query_hash,
        );
        if let Some(vector) = self
            .inner
            .query_cache
            .lock()
            .await
            .entries
            .get(&key)
            .cloned()
        {
            return Ok(vector);
        }
        let mut embeddings = self
            .inner
            .embeddings
            .embed(
                &stack.embedding_model,
                stack.embedding_dimensions as usize,
                &[query.to_string()],
            )
            .await?;
        validate_embeddings(1, stack.embedding_dimensions as usize, &embeddings)?;
        normalize_embeddings(&mut embeddings)?;
        let vector = embeddings.remove(0);
        let mut cache = self.inner.query_cache.lock().await;
        if cache.entries.len() >= MAX_CACHED_QUERY_EMBEDDINGS
            && let Some(oldest) = cache.order.pop_front()
        {
            cache.entries.remove(&oldest);
        }
        cache.order.push_back(key.clone());
        cache.entries.insert(key, vector.clone());
        Ok(vector)
    }
}

pub fn pack_citation_context(result: &KnowledgeSearchResult, max_characters: usize) -> String {
    let mut context = String::from(
        "Use the following local knowledge sources when relevant. Cite factual claims with the exact citation labels shown (for example [K1]). Do not invent citations.\n\n",
    );
    for hit in &result.hits {
        let header = format!(
            "{} {} ({})\n",
            hit.citation,
            sanitize_context(&hit.title),
            sanitize_context(&hit.source_uri)
        );
        if context
            .chars()
            .count()
            .saturating_add(header.chars().count())
            >= max_characters
        {
            break;
        }
        context.push_str(&header);
        let remaining = max_characters.saturating_sub(context.chars().count());
        let body = hit
            .snippet
            .chars()
            .take(remaining.saturating_sub(2))
            .collect::<String>();
        context.push_str(&body);
        context.push_str("\n\n");
    }
    context
}

fn fuse_rankings(
    fused: &mut BTreeMap<u64, FusedCandidate>,
    lexical: &[northbridge_storage::KnowledgeLexicalHit],
    dense: &[ScoredId],
) {
    for (index, hit) in lexical.iter().enumerate() {
        let rank = u32::try_from(index + 1).unwrap_or(u32::MAX);
        let candidate = fused.entry(hit.dense_id).or_default();
        candidate.lexical_rank = Some(rank);
        candidate.score += 1.0 / (RRF_K + f64::from(rank));
    }
    for (index, hit) in dense.iter().enumerate() {
        let rank = u32::try_from(index + 1).unwrap_or(u32::MAX);
        let candidate = fused.entry(hit.id).or_default();
        candidate.dense_rank = Some(rank);
        candidate.score += 1.0 / (RRF_K + f64::from(rank));
    }
}

fn hit_from_record(
    ordinal: usize,
    record: &KnowledgeChunkRecord,
    dense_id: u64,
    candidate: FusedCandidate,
) -> KnowledgeSearchHit {
    KnowledgeSearchHit {
        citation: format!("[K{}]", ordinal + 1),
        dense_id,
        chunk_id: record.chunk.id,
        document_id: record.chunk.document_id,
        stack_id: record.chunk.stack_id,
        title: record.title.clone(),
        source_uri: record.source_uri.clone(),
        snippet: record.chunk.content.chars().take(1600).collect(),
        start_offset: record.chunk.start_offset,
        end_offset: record.chunk.end_offset,
        lexical_rank: candidate.lexical_rank,
        dense_rank: candidate.dense_rank,
        fused_score: candidate.score,
    }
}

fn validate_embeddings(count: usize, dimensions: usize, vectors: &[Vec<f32>]) -> Result<()> {
    if vectors.len() != count {
        return Err(RetrievalError::Embedding(format!(
            "provider returned {} vectors for {count} inputs",
            vectors.len()
        )));
    }
    if vectors
        .iter()
        .any(|vector| vector.len() != dimensions || !vector.iter().all(|value| value.is_finite()))
    {
        return Err(RetrievalError::Embedding(format!(
            "provider returned a non-finite vector or a dimension other than {dimensions}"
        )));
    }
    Ok(())
}

fn normalize_embeddings(vectors: &mut [Vec<f32>]) -> Result<()> {
    for vector in vectors {
        let norm = vector
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt();
        if !norm.is_finite() || norm <= f64::EPSILON {
            return Err(RetrievalError::Embedding(
                "provider returned a zero-length or non-finite embedding".to_string(),
            ));
        }
        let inverse = (1.0 / norm) as f32;
        for value in vector {
            *value *= inverse;
        }
    }
    Ok(())
}

fn sanitize_context(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[allow(dead_code)]
fn _assert_path_is_send(_: &Path) {}
