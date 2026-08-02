CREATE TABLE managed_models (
    id TEXT PRIMARY KEY NOT NULL,
    model_id TEXT NOT NULL UNIQUE CHECK (length(trim(model_id)) > 0),
    display_name TEXT NOT NULL CHECK (length(trim(display_name)) > 0),
    source TEXT NOT NULL CHECK (source IN ('hugging_face', 'imported')),
    source_reference TEXT NOT NULL CHECK (length(trim(source_reference)) > 0),
    revision TEXT,
    local_path TEXT NOT NULL CHECK (length(trim(local_path)) > 0),
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'downloading', 'installed', 'imported', 'failed', 'cancelled')
    ),
    bytes_downloaded INTEGER NOT NULL DEFAULT 0 CHECK (bytes_downloaded >= 0),
    total_bytes INTEGER CHECK (total_bytes IS NULL OR total_bytes >= 0),
    file_count INTEGER CHECK (file_count IS NULL OR file_count >= 0),
    manifest_sha256 TEXT CHECK (
        manifest_sha256 IS NULL OR (
            length(manifest_sha256) = 64 AND manifest_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX managed_models_status_updated_idx
    ON managed_models (status, updated_at_ms DESC, id DESC);
