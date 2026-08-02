ALTER TABLE generation_runs
    ADD COLUMN input_message_id TEXT REFERENCES messages(id) ON DELETE RESTRICT;

ALTER TABLE generation_runs
    ADD COLUMN output_message_id TEXT REFERENCES messages(id) ON DELETE SET NULL;

CREATE INDEX generation_runs_status_updated_idx
    ON generation_runs (status, updated_at_ms, id)
    WHERE status IN ('queued', 'running', 'waiting_for_approval');
