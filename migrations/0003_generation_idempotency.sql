ALTER TABLE generation_runs
    ADD COLUMN client_request_id TEXT;

CREATE UNIQUE INDEX generation_runs_client_request_idx
    ON generation_runs (conversation_id, client_request_id)
    WHERE client_request_id IS NOT NULL;
