CREATE TABLE knowledge_stacks (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    description TEXT NOT NULL DEFAULT '',
    embedding_model TEXT NOT NULL CHECK (length(trim(embedding_model)) BETWEEN 1 AND 512),
    embedding_dimensions INTEGER NOT NULL CHECK (
        embedding_dimensions BETWEEN 8 AND 4096 AND embedding_dimensions % 8 = 0
    ),
    chunk_size INTEGER NOT NULL CHECK (chunk_size BETWEEN 256 AND 8192),
    chunk_overlap INTEGER NOT NULL CHECK (chunk_overlap >= 0 AND chunk_overlap < chunk_size),
    chunker_hash TEXT NOT NULL CHECK (
        length(chunker_hash) = 64 AND chunker_hash NOT GLOB '*[^0-9a-f]*'
    ),
    index_revision INTEGER NOT NULL DEFAULT 0 CHECK (index_revision >= 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (workspace_id, name COLLATE NOCASE)
) STRICT;

CREATE INDEX knowledge_stacks_workspace_updated_idx
    ON knowledge_stacks (workspace_id, updated_at_ms DESC, id DESC);

CREATE TABLE knowledge_documents (
    id TEXT PRIMARY KEY NOT NULL,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('file', 'folder', 'obsidian')),
    source_uri TEXT NOT NULL CHECK (length(trim(source_uri)) > 0),
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    mime_type TEXT NOT NULL CHECK (length(trim(mime_type)) > 0),
    content_sha256 TEXT NOT NULL CHECK (
        length(content_sha256) = 64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    blob_sha256 TEXT NOT NULL CHECK (
        length(blob_sha256) = 64 AND blob_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
    status TEXT NOT NULL CHECK (status IN ('indexing', 'ready', 'failed')),
    last_error TEXT,
    source_modified_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (stack_id, source_uri)
) STRICT;

CREATE INDEX knowledge_documents_stack_updated_idx
    ON knowledge_documents (stack_id, updated_at_ms DESC, id DESC);

CREATE TABLE document_chunks (
    dense_id INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    document_id TEXT NOT NULL REFERENCES knowledge_documents(id) ON DELETE CASCADE,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    content TEXT NOT NULL CHECK (length(content) > 0),
    content_sha256 TEXT NOT NULL CHECK (
        length(content_sha256) = 64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    character_count INTEGER NOT NULL CHECK (character_count > 0),
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset INTEGER NOT NULL CHECK (end_offset > start_offset),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (document_id, ordinal)
) STRICT;

CREATE INDEX document_chunks_stack_dense_idx
    ON document_chunks (stack_id, dense_id);

CREATE VIRTUAL TABLE document_chunks_fts USING fts5(
    dense_id UNINDEXED,
    chunk_id UNINDEXED,
    document_id UNINDEXED,
    stack_id UNINDEXED,
    workspace_id UNINDEXED,
    title,
    content,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER document_chunks_fts_insert
AFTER INSERT ON document_chunks
BEGIN
    INSERT INTO document_chunks_fts (
        dense_id, chunk_id, document_id, stack_id, workspace_id, title, content
    )
    SELECT NEW.dense_id, NEW.id, NEW.document_id, NEW.stack_id, NEW.workspace_id,
           knowledge_documents.title, NEW.content
    FROM knowledge_documents
    WHERE knowledge_documents.id = NEW.document_id;
END;

CREATE TRIGGER document_chunks_fts_update
AFTER UPDATE OF content, document_id ON document_chunks
BEGIN
    DELETE FROM document_chunks_fts WHERE dense_id = OLD.dense_id;
    INSERT INTO document_chunks_fts (
        dense_id, chunk_id, document_id, stack_id, workspace_id, title, content
    )
    SELECT NEW.dense_id, NEW.id, NEW.document_id, NEW.stack_id, NEW.workspace_id,
           knowledge_documents.title, NEW.content
    FROM knowledge_documents
    WHERE knowledge_documents.id = NEW.document_id;
END;

CREATE TRIGGER document_chunks_fts_document_title_update
AFTER UPDATE OF title ON knowledge_documents
BEGIN
    UPDATE document_chunks_fts
    SET title = NEW.title
    WHERE document_id = NEW.id;
END;

CREATE TRIGGER document_chunks_fts_delete
AFTER DELETE ON document_chunks
BEGIN
    DELETE FROM document_chunks_fts WHERE dense_id = OLD.dense_id;
END;

CREATE TABLE embeddings (
    chunk_dense_id INTEGER PRIMARY KEY REFERENCES document_chunks(dense_id) ON DELETE CASCADE,
    model TEXT NOT NULL CHECK (length(trim(model)) BETWEEN 1 AND 512),
    dimensions INTEGER NOT NULL CHECK (dimensions BETWEEN 8 AND 4096 AND dimensions % 8 = 0),
    vector BLOB NOT NULL,
    vector_sha256 TEXT NOT NULL CHECK (
        length(vector_sha256) = 64 AND vector_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (length(vector) = dimensions * 4)
) STRICT;

CREATE TABLE dense_index_journal (
    sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision > 0),
    chunk_dense_id INTEGER NOT NULL CHECK (chunk_dense_id > 0),
    operation TEXT NOT NULL CHECK (operation IN ('upsert', 'remove')),
    vector_sha256 TEXT,
    created_at_ms INTEGER NOT NULL,
    applied_at_ms INTEGER,
    UNIQUE (stack_id, revision, chunk_dense_id, operation)
) STRICT;

CREATE INDEX dense_index_journal_pending_idx
    ON dense_index_journal (stack_id, revision, sequence_number)
    WHERE applied_at_ms IS NULL;

CREATE TABLE ingestion_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE CASCADE,
    source_uri TEXT NOT NULL CHECK (length(trim(source_uri)) > 0),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('file', 'folder', 'obsidian')),
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    progress_current INTEGER NOT NULL DEFAULT 0 CHECK (progress_current >= 0),
    progress_total INTEGER CHECK (progress_total IS NULL OR progress_total >= 0),
    document_id TEXT REFERENCES knowledge_documents(id) ON DELETE SET NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX ingestion_jobs_stack_updated_idx
    ON ingestion_jobs (stack_id, updated_at_ms DESC, id DESC);

CREATE TABLE knowledge_watch_roots (
    id TEXT PRIMARY KEY NOT NULL,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('folder', 'obsidian')),
    path TEXT NOT NULL CHECK (length(trim(path)) > 0),
    recursive INTEGER NOT NULL DEFAULT 1 CHECK (recursive IN (0, 1)),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    last_scan_at_ms INTEGER,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (stack_id, path)
) STRICT;

CREATE INDEX knowledge_watch_roots_enabled_idx
    ON knowledge_watch_roots (enabled, stack_id, id);

CREATE TABLE retrieval_traces (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    generation_id TEXT REFERENCES generation_runs(id) ON DELETE SET NULL,
    query TEXT NOT NULL CHECK (length(trim(query)) > 0),
    stack_ids_json TEXT NOT NULL CHECK (json_valid(stack_ids_json)),
    embedding_model TEXT,
    lexical_candidate_count INTEGER NOT NULL CHECK (lexical_candidate_count >= 0),
    dense_candidate_count INTEGER NOT NULL CHECK (dense_candidate_count >= 0),
    result_count INTEGER NOT NULL CHECK (result_count >= 0),
    elapsed_ms INTEGER NOT NULL CHECK (elapsed_ms >= 0),
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX retrieval_traces_generation_idx
    ON retrieval_traces (generation_id, created_at_ms DESC)
    WHERE generation_id IS NOT NULL;

CREATE TABLE retrieval_trace_hits (
    trace_id TEXT NOT NULL REFERENCES retrieval_traces(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    dense_id INTEGER NOT NULL CHECK (dense_id > 0),
    chunk_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    stack_id TEXT NOT NULL,
    title TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    snippet TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    lexical_rank INTEGER,
    dense_rank INTEGER,
    fused_score REAL NOT NULL,
    PRIMARY KEY (trace_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE generation_knowledge_stacks (
    generation_id TEXT NOT NULL REFERENCES generation_runs(id) ON DELETE CASCADE,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (generation_id, stack_id),
    UNIQUE (generation_id, ordinal)
) STRICT, WITHOUT ROWID;
