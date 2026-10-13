# DSS150P Lab Activity #4 Runbook

## Purpose

This runbook describes how to set up, execute, validate, restart, troubleshoot, and recover the reproducible stroke predictive analytics pipeline.

## 1. Prerequisites

Required tools:

- Git
- Python 3.9+ for the local pipeline
- Docker Desktop / Docker Engine with Compose
- Internet access only for the initial clone/image/package installation

Repository:

`https://github.com/Sophia-A04/DSS150P_REPRO_ABAD_SOPHIA`

## 2. Clean Setup

```bash
git clone https://github.com/Sophia-A04/DSS150P_REPRO_ABAD_SOPHIA.git
cd DSS150P_REPRO_ABAD_SOPHIA

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements.txt

cp .env.example .env
```

Do not commit `.env`.

## 3. Start PostgreSQL

```bash
docker compose up -d postgres
docker compose ps
```

Expected service state:

```text
dss150p-repro-postgres ... healthy
```

If the database is not healthy:

```bash
docker compose logs postgres --tail=100
```

## 4. Verify Source Before Processing

```bash
python -m src.cli verify-source
```

Expected:

- 5,110 rows
- 12 columns
- SHA-256 equals the manifest
- status `PASS`

If verification fails, do not continue. Restore the canonical source or update the manifest only after intentionally reviewing a legitimate source change.

## 5. Full Ingestion

```bash
python -m src.cli ingest-full
```

Behavior:

- first valid execution creates the raw full snapshot;
- rerun with the same source checksum returns `SKIPPED`;
- a conflicting existing raw snapshot causes a failure rather than silent overwrite.

Audit evidence:

`metadata/ingestion_audit.csv`

## 6. Incremental / Checkpoint Demonstration

Because the source dataset has no natural update timestamp, incremental ingestion is explicitly simulated with deterministic source-row batches.

```bash
python -m src.cli ingest-incremental --max-batches 2
python -m src.cli ingest-incremental --all
```

Checkpoint:

`metadata/ingestion_checkpoint.json`

When all batches are complete, another run returns:

```text
status: SKIPPED
message: No remaining batches.
```

### Restart Behavior

If a run stops between batches, rerun the same incremental command. The persisted checkpoint records the last completed batch. Existing batch files are checksum-verified before reuse.

## 7. Transform Data

```bash
python -m src.cli transform-all
```

Expected core results:

- staging rows: 3,265
- quarantine rows: 1,845
- curated rows: 3,265
- missing BMI in staging: 0
- duplicate IDs in staging/curated: 0

Artifacts:

- `data/staging/stroke_cleaned.csv`
- `data/quarantine/rejected_records.csv`
- `data/curated/stroke_model_ready.csv`

## 8. Storage and PostgreSQL

```bash
python -m src.cli storage-all
```

Expected:

- CSV, JSONL, and Parquet all contain 3,265 logical rows;
- all three file formats have the same logical hash;
- PostgreSQL contains 3,265 rows and 3,265 distinct IDs.

Rerun:

```bash
python -m src.cli load-postgres
```

The table must remain at 3,265 rows.

### Selected Partition

```bash
python -m src.cli load-partition --age-score-risk 1
```

Expected selected partition rows: 1,779.

The complete table still remains at 3,265 unique IDs because loading is rerun-safe.

## 9. Train and Evaluate

```bash
python -m src.cli train
python -m src.cli evaluate
```

Expected evaluation status:

`PASS`

Primary result:

- ROC-AUC: approximately 0.6716448
- expected baseline: 0.6716
- tolerance: 0.01

Artifacts:

- `metadata/split_manifest.json`
- `outputs/predictions.csv`
- `outputs/metrics.json`
- local regenerated model artifact under `outputs/model/`

## 10. Validation and Tests

```bash
python -m src.cli validate-all
python -m pytest -c ./pytest.ini -v
```

Expected:

- overall validation: `PASS`
- tests: `5 passed`

## 11. Benchmark

```bash
python -m src.cli benchmark --repeats 5
```

Evidence:

- `docs/benchmark_results.csv`
- `docs/benchmark_context.json`

The benchmark verifies full and filtered logical hashes across CSV, JSONL, Parquet, and PostgreSQL.

## 12. Start Airflow

```bash
docker compose up -d --build airflow
docker compose ps
```

Check imports:

```bash
docker compose exec airflow airflow dags list-import-errors
```

Expected:

```text
No data found
```

List tasks:

```bash
docker compose exec airflow airflow tasks list \
dss150p_predictive_repro_pipeline --tree
```

## 13. Trigger a Normal Airflow Run

```bash
docker compose exec airflow airflow dags unpause \
dss150p_predictive_repro_pipeline

docker compose exec airflow airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id manual_success
```

Inspect:

```bash
docker compose exec airflow airflow dags list-runs \
-d dss150p_predictive_repro_pipeline
```

## 14. Failure Injection

The DAG supports a controlled failure parameter for demonstration.

```bash
docker compose exec airflow airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id manual_failure \
--conf '{"inject_failure": true}'
```

The `validate_raw` task intentionally raises a `ValueError`. The configured task retries once after 10 seconds. After the final failed attempt, downstream tasks are not executed successfully.

Evidence is appended to:

- `metadata/pipeline_events.jsonl`
- `metadata/pipeline_runs.jsonl`

## 15. Recovery

Correct the runtime parameter by triggering a new run without failure injection:

```bash
docker compose exec airflow airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id manual_recovery \
--conf '{"inject_failure": false}'
```

A successful recovery should complete end-to-end without creating duplicate PostgreSQL logical records.

## 16. Existing Demonstration Evidence

The committed evidence includes:

```text
lab4_success    SUCCESS
lab4_failure    FAILED
lab4_recovery   SUCCESS
```

The failed task record includes the deliberate failure message and a final `try_number` of 2.

## 17. Common Problems

### `empty compose file`

Cause: `docker-compose.yml` is empty or invalid.

Fix:

```bash
docker compose config
```

Correct the Compose file until this command succeeds.

### PostgreSQL port already allocated

Check:

```bash
docker ps
lsof -i :5432
```

Stop the conflicting service or change the host-side port mapping.

### `No module named ...`

Confirm the virtual environment is active:

```bash
which python
pip install -r requirements.txt
```

### Pytest collects 0 tests

Always run from the repository root using:

```bash
python -m pytest -c ./pytest.ini -v
```

### Airflow DAG import error

```bash
docker compose exec airflow airflow dags list-import-errors
docker compose logs airflow --tail=100
```

### Source checksum mismatch

Do not bypass the check. Compare the source file with `metadata/source_manifest.yml`. Treat an unexpected source change as a new data version requiring review.

## 18. Stop Services

```bash
docker compose down
```

To preserve PostgreSQL/Airflow volume data, do not add `-v`.

To fully reset container volumes only when intentionally starting over:

```bash
docker compose down -v
```

## 19. Clean Verification Sequence

Before submission or demonstration:

```bash
python -m src.cli verify-source
python -m src.cli validate-all
python -m src.cli evaluate
python -m src.cli storage-verify
python -m pytest -c ./pytest.ini -v
docker compose exec airflow airflow dags list-import-errors
git status
```

The repository should be clean after any intentionally generated runtime files are either restored or intentionally committed.
