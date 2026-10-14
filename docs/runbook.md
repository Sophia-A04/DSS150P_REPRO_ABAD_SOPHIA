# DSS150P Lab Activity #4 Runbook

## Purpose

This runbook describes how to set up, execute, validate, reset, troubleshoot, and recover the DSS150P reproducible stroke predictive analytics pipeline.

The pipeline reconstructs the previous predictive analytics workflow as an individual, modular, testable, observable, and reproducible data-engineering implementation.

---

## 1. Prerequisites

Required tools:

- Git
- Python 3.9 or later
- Docker Desktop or Docker Engine with Docker Compose
- Internet access for the initial repository clone, Python package installation, and Docker image installation

Repository:

`https://github.com/Sophia-A04/DSS150P_REPRO_ABAD_SOPHIA`

---

## 2. Clean Setup

Clone the repository:

```bash
git clone https://github.com/Sophia-A04/DSS150P_REPRO_ABAD_SOPHIA.git
cd DSS150P_REPRO_ABAD_SOPHIA
```

Create and activate the Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the pinned dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Create the local environment file:

```bash
cp .env.example .env
```

Do not commit `.env`.

---

## 3. Start PostgreSQL

Start the PostgreSQL service:

```bash
docker compose up -d postgres
docker compose ps
```

Expected database state:

```text
healthy
```

If PostgreSQL is not healthy, inspect the logs:

```bash
docker compose logs postgres --tail=100
```

---

## 4. Generated-State Reset

The repository contains selected generated artifacts as reproducibility evidence.

A reviewer who wants to reproduce the pipeline from a clean generated state can first preview what will be removed:

```bash
python -m src.cli reset-generated --dry-run
```

This does not delete anything.

Reset generated file state:

```bash
python -m src.cli reset-generated
```

To also reset the project-owned PostgreSQL tables:

```bash
python -m src.cli reset-generated --database
```

The reset preserves the canonical project definitions, including:

```text
data/source/
metadata/source_manifest.yml
metadata/data_contract.yml
metadata/expected_metrics.json
source code
tests
documentation
```

---

## 5. Verify the Canonical Source

Run:

```bash
python -m src.cli verify-source
```

Expected source identity:

```text
rows:    5110
columns: 12
status:  PASS
```

Expected SHA-256:

```text
144ea5366832bb5432645c0e49fbb951aadf4ee1e75e73d2e15d3dc0841525bd
```

The canonical source is:

`data/source/Stroke_Prediction_Dataset.csv`

The source manifest is:

`metadata/source_manifest.yml`

If source verification fails, do not bypass the check.

---

## 6. Full Ingestion

Run:

```bash
python -m src.cli ingest-full
```

On the first valid execution, the command creates:

`data/raw/full/Stroke_Prediction_Dataset.csv`

If the same source has already been ingested, the command safely returns:

```text
status: SKIPPED
```

An existing raw snapshot with conflicting contents must fail rather than being silently overwritten.

Ingestion audit evidence is stored in:

`metadata/ingestion_audit.csv`

---

## 7. Incremental Ingestion

The canonical source is static and has no natural event timestamp.

Incremental processing is therefore an explicit laboratory simulation using deterministic source-row batches.

Process only the first two batches:

```bash
python -m src.cli ingest-incremental --max-batches 2
```

Process all remaining batches:

```bash
python -m src.cli ingest-incremental --all
```

Checkpoint state is stored in:

`metadata/ingestion_checkpoint.json`

The completed incremental dataset contains:

```text
11 batches
5110 rows
5110 unique IDs
```

After all batches have already been ingested, another run returns:

```text
status: SKIPPED
message: No remaining batches.
```

---

## 8. Interrupted-Ingestion Recovery Demonstration

The repository includes an automated recovery demonstration using the real incremental-ingestion implementation.

Run:

```bash
python -m src.cli demo-interrupted-ingest
```

The demonstration performs this sequence:

```text
1. Process batches 1 and 2 normally.
2. Write batch 3.
3. Simulate interruption before checkpoint 3 becomes durable.
4. Confirm the checkpoint remains at batch 2.
5. Restart incremental ingestion.
6. Resume at batch 3.
7. Finish all 11 batches.
8. Verify final uniqueness and duplicate counts.
```

Expected final result:

```text
status: PASS
rows: 5110
unique IDs: 5110
duplicate IDs: 0
last completed batch: 11
```

Saved evidence:

`docs/interrupted_run_log.json`

This demonstration runs against temporary generated state and does not damage the committed canonical source.

---

## 9. Data Contract

The machine-readable contract is defined in:

`metadata/data_contract.yml`

The contract enforcement implementation is:

`src/contract.py`

The contract checks include:

- required columns;
- unexpected columns;
- primary-key validity;
- declared data types;
- integer requirements;
- allowed values;
- minimum and maximum values; and
- row-level contract violations.

Validate the actual ingested raw layer:

```bash
python -m src.cli validate-raw
```

For the canonical dataset, the expected result is:

```text
status: PASS
contract_invalid_rows: 0
requires_quarantine: false
```

Structural violations such as a missing required column fail validation.

Invalid row values are rejected from the valid staging population and can be represented in quarantine with explicit rejection reasons.

---

## 10. Controlled Contract-Failure Demonstrations

The failure demonstrations operate on temporary corrupted copies.

They do not modify the canonical source or permanent raw snapshot.

### Invalid binary value

Run:

```bash
python -m src.cli demo-contract-failure \
  --scenario invalid-binary
```

The demonstration temporarily changes:

```text
hypertension = 7
```

Expected result:

```text
status: FAILED
error_type: ControlledDataQualityFailure
```

The rejection reason includes:

```text
hypertension: value_not_allowed
```

The command intentionally exits as a failure because successfully detecting the corrupted value is the purpose of the demonstration.

### Missing required column

Run:

```bash
python -m src.cli demo-contract-failure \
  --scenario missing-column
```

The demonstration temporarily removes:

```text
hypertension
```

Expected result:

```text
status: FAILED
error_type: ControlledDataQualityFailure
```

This also represents an intentional successful detection of bad input.

---

## 11. Transform Data

Run the complete transformation:

```bash
python -m src.cli transform-all
```

Expected canonical results:

```text
source rows:            5110
staging rows:           3265
quarantined rows:       1845
contract-invalid rows:  0
curated rows:           3265
missing BMI:            0
duplicate IDs:          0
status:                 PASS
```

Artifacts:

```text
data/staging/stroke_cleaned.csv
data/quarantine/rejected_records.csv
data/curated/stroke_model_ready.csv
```

The canonical 1,845 quarantined records are excluded by the documented modeling-age range of 35 through 90 years.

They are not contract-corrupted records.

---

## 12. Storage Representations

Run:

```bash
python -m src.cli storage-all
```

The same 3,265 curated logical records are represented as:

```text
CSV
JSON Lines
Parquet
PostgreSQL
```

File outputs are stored under:

`outputs/storage/`

Logical row-set hashes are used to verify equivalence between file formats.

Verify storage equivalence independently:

```bash
python -m src.cli storage-verify
```

Expected result:

```text
status: PASS
rows: 3265
```

---

## 13. PostgreSQL

The database schema is defined in:

```text
sql/schema.sql
```

The UPSERT statement is defined in:

```text
sql/transformations.sql
```

`src/storage.py` executes these SQL files rather than embedding the table definitions directly in Python.

Load the curated data:

```bash
python -m src.cli load-postgres
```

Expected state:

```text
table rows:   3265
distinct IDs: 3265
status:       PASS
```

Run the same command again:

```bash
python -m src.cli load-postgres
```

The database should remain:

```text
3265 rows
3265 distinct IDs
```

This demonstrates rerun-safe loading using the primary key and UPSERT behavior.

---

## 14. Partition Demonstration

Create the partitioned representation:

```bash
python -m src.cli create-partitions
```

The curated data is partitioned by:

`age_score_risk`

Expected partition sizes:

```text
age_score_risk=0: 1486 rows
age_score_risk=1: 1779 rows
```

Load one selected partition:

```bash
python -m src.cli load-partition \
  --age-score-risk 1
```

Expected selected-partition size:

```text
1779 rows
```

Because loading is rerun-safe, the complete PostgreSQL table remains at 3,265 unique records.

---

## 15. Train the Model

Run:

```bash
python -m src.cli train
```

The implementation reproduces the baseline logistic-regression experiment using deterministic controls.

The train/test split contains:

```text
train rows: 2612
test rows:   653
```

Split identity is persisted in:

`metadata/split_manifest.json`

The trained model artifact is generated under:

`outputs/model/`

---

## 16. Evaluate Predictive Reproduction

Run:

```bash
python -m src.cli evaluate
```

Expected evaluation status:

```text
PASS
```

Primary reproduced ROC-AUC:

```text
0.6716448168671442
```

Expected baseline:

```text
0.6716
```

Tolerance:

```text
0.01
```

Additional baseline comparisons include accuracy and recall.

Machine-readable expected metrics:

`metadata/expected_metrics.json`

Regenerated metrics:

`outputs/metrics.json`

Predictions:

`outputs/predictions.csv`

---

## 17. Validation

Run the complete validation suite:

```bash
python -m src.cli validate-all
```

Validation covers:

- canonical source identity;
- raw-layer integrity;
- data-contract enforcement;
- package versions;
- required repository files;
- staging assertions;
- curated assertions;
- storage equivalence; and
- PostgreSQL connectivity and row state.

Expected final result:

```text
status: PASS
```

---

## 18. Automated Tests

Run:

```bash
python -m pytest -c ./pytest.ini -q
```

Current verified result:

```text
8 passed
```

The test suite includes coverage for:

- source integrity;
- deterministic feature preparation;
- logical hashing;
- contract rejection of invalid binary values;
- quarantine behavior;
- storage equivalence;
- PostgreSQL rerun safety;
- selected-partition loading; and
- interrupted-ingestion recovery.

---

## 19. Storage Benchmark

Run:

```bash
python -m src.cli benchmark --repeats 5
```

The benchmark repeatedly measures:

- write time;
- complete read time; and
- filtered read/query time.

It also verifies the complete and filtered logical row sets rather than relying only on row counts.

Evidence:

```text
docs/benchmark_results.csv
docs/benchmark_context.json
docs/benchmark_report.md
```

The reported timing values describe the tested machine and environment only.

They are not universal storage-format rankings.

---

## 20. Start Apache Airflow

Build and start PostgreSQL and Airflow:

```bash
docker compose up -d --build postgres airflow
```

Check service state:

```bash
docker compose ps
```

Check DAG import errors:

```bash
docker compose exec airflow \
airflow dags list-import-errors
```

Expected:

```text
No data found
```

List the DAG tasks:

```bash
docker compose exec airflow \
airflow tasks list \
dss150p_predictive_repro_pipeline \
--tree
```

The task chain is:

```text
ingest_source
    |
    v
validate_raw
    |
    v
transform_staging
    |
    v
build_curated
    |
    v
load_postgres
    |
    v
validate_curated
    |
    v
train_model
    |
    v
evaluate_model
    |
    v
reproducibility_check
    |
    v
publish_outputs
```

The DAG configuration includes:

```text
retries: 1
retry delay: 10 seconds
task execution timeout: 10 minutes
DAG timeout: 30 minutes
max_active_runs: 1
max_active_tasks: 2
```

---

## 21. Normal Airflow Run

Unpause the DAG:

```bash
docker compose exec airflow \
airflow dags unpause \
dss150p_predictive_repro_pipeline
```

Trigger a normal run:

```bash
docker compose exec airflow \
airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id lab4_normal
```

Inspect the result:

```bash
docker compose exec airflow \
airflow dags list-runs \
-d dss150p_predictive_repro_pipeline
```

A normal run should complete with:

```text
success
```

---

## 22. Airflow Contract-Failure Demonstration

Trigger a genuine data-contract failure:

```bash
docker compose exec airflow \
airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id lab4_contract_failure_final \
--conf '{"failure_scenario":"invalid-binary"}'
```

The DAG:

1. performs ingestion;
2. reaches `validate_raw`;
3. creates a temporary corrupted raw copy;
4. introduces an invalid `hypertension=7`;
5. detects the contract violation; and
6. fails the validation task.

The canonical source and permanent raw data remain unchanged.

Expected DAG state:

```text
failed
```

The failed task should report a contract-driven error rather than an arbitrary synthetic runtime exception.

---

## 23. Airflow Recovery

Trigger a corrected run using canonical data:

```bash
docker compose exec airflow \
airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id lab4_contract_recovery_final \
--conf '{"failure_scenario":"none"}'
```

Inspect the runs:

```bash
docker compose exec airflow \
airflow dags list-runs \
-d dss150p_predictive_repro_pipeline
```

Expected logical sequence:

```text
contract-failure run     -> failed
contract-recovery run    -> success
```

The successful recovery must still leave PostgreSQL with the expected unique logical records rather than duplicated rows.

---

## 24. Structured Airflow Evidence

Task-level events are stored in:

`metadata/pipeline_events.jsonl`

DAG-level run summaries are stored in:

`metadata/pipeline_runs.jsonl`

Task event records include information such as:

```text
pipeline_run_id
dag_id
task_id
status
try_number
duration_seconds
error_type
error_message
timestamp_utc
```

These files provide persisted orchestration evidence in addition to the Airflow UI and CLI output.

---

## 25. Common Problems

### `python: command not found`

Activate the virtual environment first:

```bash
source .venv/bin/activate
```

Then verify:

```bash
which python
python --version
```

---

### `No module named ...`

Confirm the environment is active and reinstall requirements:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

---

### Pytest collects zero tests

Run from the repository root:

```bash
python -m pytest -c ./pytest.ini -q
```

---

### PostgreSQL is not healthy

Inspect:

```bash
docker compose ps
docker compose logs postgres --tail=100
```

---

### PostgreSQL port 5432 is already allocated

Inspect:

```bash
docker ps
lsof -i :5432
```

Stop the conflicting process or container before starting the project PostgreSQL service.

---

### Airflow DAG import error

Run:

```bash
docker compose exec airflow \
airflow dags list-import-errors

docker compose logs airflow --tail=100
```

---

### Source checksum mismatch

Do not bypass the checksum.

Compare:

```text
data/source/Stroke_Prediction_Dataset.csv
metadata/source_manifest.yml
```

An unexpected checksum change should be treated as a new data version requiring review.

---

### Incremental ingestion reports no remaining batches

This is expected after all 11 batches have already been processed.

To demonstrate ingestion from clean generated state, use:

```bash
python -m src.cli reset-generated
```

and rerun the ingestion commands.

---

## 26. Stop Services

Stop containers while preserving their volume state:

```bash
docker compose down
```

To intentionally remove container volumes:

```bash
docker compose down -v
```

Use `-v` only when deliberately starting over.

---

## 27. Final Verification

Before submission or demonstration, run:

```bash
python -m src.cli verify-source
python -m src.cli validate-all
python -m src.cli evaluate
python -m src.cli storage-verify
python -m pytest -c ./pytest.ini -q

docker compose exec airflow \
airflow dags list-import-errors

git diff --check
git status --short
```

Target results:

```text
verify-source: PASS
validate-all: PASS
evaluate: PASS
storage-verify: PASS
pytest: 8 passed
Airflow import errors: No data found
git diff --check: no output
```

Any intentional documentation or evidence changes should be reviewed before they are committed.