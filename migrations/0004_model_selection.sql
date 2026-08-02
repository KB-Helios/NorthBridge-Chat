ALTER TABLE generation_runs
    ADD COLUMN provider TEXT CHECK (
        provider IS NULL OR provider IN ('local_mistral_rs')
    );

ALTER TABLE generation_runs
    ADD COLUMN resolved_model_id TEXT CHECK (
        resolved_model_id IS NULL OR length(trim(resolved_model_id)) BETWEEN 1 AND 512
    );

CREATE INDEX generation_runs_model_idx
    ON generation_runs (provider, resolved_model_id, created_at_ms DESC)
    WHERE provider IS NOT NULL AND resolved_model_id IS NOT NULL;
