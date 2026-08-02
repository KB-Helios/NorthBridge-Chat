ALTER TABLE generation_runs
    ADD COLUMN load_profile_json TEXT CHECK (
        load_profile_json IS NULL OR json_valid(load_profile_json)
    );
