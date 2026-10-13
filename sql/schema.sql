CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS curated.stroke_model_ready (
    id BIGINT PRIMARY KEY,
    gender TEXT NOT NULL,
    age DOUBLE PRECISION NOT NULL,
    hypertension SMALLINT NOT NULL
        CHECK (hypertension IN (0, 1)),
    heart_disease SMALLINT NOT NULL
        CHECK (heart_disease IN (0, 1)),
    "Residence_type" TEXT NOT NULL,
    avg_glucose_level DOUBLE PRECISION NOT NULL
        CHECK (avg_glucose_level >= 0),
    bmi DOUBLE PRECISION NOT NULL
        CHECK (bmi >= 0),
    smoking_status TEXT NOT NULL,
    stroke SMALLINT NOT NULL
        CHECK (stroke IN (0, 1)),
    gender_score_risk SMALLINT NOT NULL,
    age_score_risk SMALLINT NOT NULL,
    glucose_score_risk SMALLINT NOT NULL,
    bmi_score_risk SMALLINT NOT NULL,
    smoking_score_risk SMALLINT NOT NULL,
    residence_score_risk SMALLINT NOT NULL,
    "VAAF" DOUBLE PRECISION NOT NULL,
    "TGI" DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.partition_loads (
    load_id BIGSERIAL PRIMARY KEY,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    partition_column TEXT NOT NULL,
    partition_value TEXT NOT NULL,
    source_rows INTEGER NOT NULL,
    table_rows_after INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at_utc TIMESTAMPTZ,
    run_mode TEXT,
    status TEXT NOT NULL,
    raw_rows INTEGER,
    staging_rows INTEGER,
    curated_rows INTEGER,
    error_type TEXT,
    error_message TEXT
);