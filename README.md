# DSS150P Reproducible Predictive Analytics Project

**Student:** Sophia Abad  
**Course:** DSS150P - Fundamentals of Data Engineering  
**Laboratory Activity:** Integrated Individual Laboratory Activity #4

## Project Overview

This repository reconstructs and productionizes the previous DSS141P Predictive Analytics project:

**Strengthening Philippine Stroke Care: A Machine Learning-Based Stroke Risk Assessment Focusing on Glucose Level, Hypertension, Heart Disease, BMI, and Smoking Status Factors**

The original project was primarily notebook-based. This individual DSS150P implementation converts it into a modular, rerunnable, testable, observable, and reproducible data-engineering pipeline.

The implementation preserves the original dataset and predictive objective while independently implementing source verification, ingestion, transformation, storage, validation, orchestration, and predictive reproduction.

## Canonical Dataset

Dataset: **Nnaodeh - Stroke Prediction Dataset**

Source: `https://huggingface.co/datasets/Nnaodeh/Stroke_Prediction_Dataset`

Canonical repository copy:

`data/source/Stroke_Prediction_Dataset.csv`

Source dimensions:

- 5,110 rows
- 12 columns

SHA-256:

`144ea5366832bb5432645c0e49fbb951aadf4ee1e75e73d2e15d3dc0841525bd`

The source copy is treated as immutable. Cleaning and model-specific transformations occur only downstream.

## Architecture Summary

```text
Source CSV + Manifest
        |
        v
Source Verification
        |
        v
Full / Incremental Raw Ingestion
        |
        v
Staging Cleaning + Quarantine
        |
        v
Curated Feature Engineering
        |
        +-----------------------------+
        |                             |
        v                             v
CSV / JSONL / Parquet            PostgreSQL
        |                             |
        +-------------+---------------+
                      |
                      v
            Deterministic Model Input
                      |
                      v
          Baseline Logistic Regression
                      |
                      v
         Predictions + Metrics + Model
                      |
                      v
           Reproducibility Comparison
```

Apache Airflow orchestrates the reusable Python modules instead of embedding transformation logic directly in the DAG.

## Repository Structure

```text
DSS150P_REPRO_ABAD_SOPHIA/
├── README.md
├── .gitignore
├── .env.example
├── requirements.txt
├── pytest.ini
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yml
├── config/
│   └── settings.yml
├── data/
│   ├── source/
│   ├── raw/
│   ├── staging/
│   ├── curated/
│   ├── partitioned/
│   └── quarantine/
├── metadata/
│   ├── source_profile.md
│   ├── source_manifest.yml
│   ├── data_contract.yml
│   ├── expected_metrics.json
│   ├── split_manifest.json
│   ├── ingestion_checkpoint.json
│   ├── ingestion_audit.csv
│   ├── pipeline_events.jsonl
│   └── pipeline_runs.jsonl
├── src/
│   ├── config.py
│   ├── ingest.py
│   ├── transform.py
│   ├── storage.py
│   ├── train.py
│   ├── evaluate.py
│   ├── validate.py
│   ├── benchmark.py
│   ├── observe.py
│   └── cli.py
├── sql/
│   ├── schema.sql
│   └── transformations.sql
├── dags/
│   └── predictive_repro_pipeline.py
├── tests/
│   ├── test_unit.py
│   └── test_integration.py
├── outputs/
│   ├── metrics.json
│   ├── predictions.csv
│   ├── storage/
│   └── benchmark/
└── docs/
    ├── architecture.png
    ├── lineage.png
    ├── runbook.md
    ├── benchmark_report.md
    ├── benchmark_results.csv
    ├── benchmark_context.json
    └── reproducibility_report.md
```

## Clean-Machine Setup

Clone the repository and enter it:

```bash
git clone https://github.com/Sophia-A04/DSS150P_REPRO_ABAD_SOPHIA.git
cd DSS150P_REPRO_ABAD_SOPHIA
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Create the local environment file:

```bash
cp .env.example .env
```

The committed `.env.example` contains placeholders only. Real credentials must never be committed.

## Start PostgreSQL

```bash
docker compose up -d postgres
docker compose ps
```

The PostgreSQL service should report `healthy`.

## Verify Source Identity

```bash
python -m src.cli verify-source
```

Expected core result:

```text
rows: 5110
columns: 12
status: PASS
```

## Full Ingestion

```bash
python -m src.cli ingest-full
```

If an identical raw snapshot already exists, a rerun intentionally returns `SKIPPED` instead of creating a duplicate.

## Incremental / Checkpoint Demonstration

The source is static and has no natural event/update timestamp, so incremental processing is simulated using deterministic source-row batches.

```bash
python -m src.cli ingest-incremental --max-batches 2
python -m src.cli ingest-incremental --all
```

Checkpoint state is persisted in:

`metadata/ingestion_checkpoint.json`

After every source batch is processed, an additional incremental run returns `SKIPPED` with no remaining batches.

## Transformations

```bash
python -m src.cli transform-all
```

Observed core results:

```text
source rows:        5110
staging rows:       3265
quarantined rows:   1845
curated rows:       3265
missing BMI:        0
duplicate IDs:      0
status:             PASS
```

Staging contains cleaning, eligibility handling, imputation, and quarantine behavior. Curated data contains model-oriented engineered features.

## Storage and PostgreSQL

```bash
python -m src.cli storage-all
```

The same 3,265 curated logical rows are represented as:

- CSV
- JSON Lines
- Parquet
- PostgreSQL

Cross-format logical hashes verify equivalence.

Rerun the PostgreSQL load:

```bash
python -m src.cli load-postgres
```

Expected:

```text
table_rows: 3265
distinct_ids: 3265
status: PASS
```

Rerun safety is implemented with a primary key and PostgreSQL UPSERT behavior.

## Partition Demonstration

The curated dataset is partitioned by `age_score_risk`.

Observed partitions:

```text
age_score_risk=0: 1486 rows
age_score_risk=1: 1779 rows
```

Load a selected partition independently:

```bash
python -m src.cli load-partition --age-score-risk 1
```

The complete PostgreSQL table remains at 3,265 unique logical records.

## Predictive Reproduction

Train:

```bash
python -m src.cli train
```

Evaluate:

```bash
python -m src.cli evaluate
```

Deterministic controls include:

- `random_state=42`
- stratified 80/20 train/test split
- persisted split manifest
- `StandardScaler`
- baseline Logistic Regression
- `class_weight="balanced"`
- pinned Python dependencies

Observed split:

```text
training rows: 2612
test rows:      653
```

### Historical vs Reproduced Metrics

| Metric | Historical Baseline | Reproduced | Absolute Difference | Tolerance | Result |
|---|---:|---:|---:|---:|---|
| ROC-AUC | 0.6716 | 0.6716448 | 0.0000448 | 0.01 | PASS |
| Accuracy | 0.6815 | 0.6814701 | 0.0000299 | 0.01 | PASS |
| Recall | 0.5714 | 0.5714286 | 0.0000286 | 0.01 | PASS |

Machine-readable expected values:

`metadata/expected_metrics.json`

Machine-readable reproduced values:

`outputs/metrics.json`

## Validation

```bash
python -m src.cli validate-all
```

Expected final result:

```text
status: PASS
```

## Automated Tests

```bash
python -m pytest -c ./pytest.ini -v
```

Current result:

```text
5 passed
```

The tests cover source integrity, deterministic feature preparation, logical hashing, storage equivalence, PostgreSQL idempotency, and selected-partition loading.

## Storage Benchmark

```bash
python -m src.cli benchmark --repeats 5
```

Observed benchmark:

| Storage | Size (bytes) | Median Write (s) | Median Full Read (s) | Median Filtered Read (s) |
|---|---:|---:|---:|---:|
| CSV | 253,279 | 0.0081805 | 0.0020265 | 0.0037926 |
| JSONL | 1,101,963 | 0.0062749 | 0.0121160 | 0.0147380 |
| Parquet | 90,335 | 0.0031610 | 0.0011086 | 0.0014997 |
| PostgreSQL | 1,728,512 | 0.0778098 | 0.0203078 | 0.0099002 |

These results are specific to the tested machine and workload. They are not universal rankings.

Detailed evidence:

- `docs/benchmark_results.csv`
- `docs/benchmark_context.json`
- `docs/benchmark_report.md`

## Apache Airflow

Build and start:

```bash
docker compose up -d --build airflow
```

Check DAG import errors:

```bash
docker compose exec airflow airflow dags list-import-errors
```

Expected:

```text
No data found
```

DAG:

`dss150p_predictive_repro_pipeline`

Task sequence:

```text
ingest_source
    ↓
validate_raw
    ↓
transform_staging
    ↓
build_curated
    ↓
load_postgres
    ↓
validate_curated
    ↓
train_model
    ↓
evaluate_model
    ↓
reproducibility_check
    ↓
publish_outputs
```

The DAG includes retries, retry delay, execution timeout, DAG-level timeout, `max_active_runs=1`, `max_active_tasks=2`, runtime parameters, structured callbacks, and persisted run records.

## Failure and Recovery Evidence

Observed Airflow sequence:

```text
lab4_success    SUCCESS
lab4_failure    FAILED
lab4_recovery   SUCCESS
```

The intentional failure used:

```json
{"inject_failure": true}
```

The corrected recovery run used:

```json
{"inject_failure": false}
```

Structured evidence is stored in:

- `metadata/pipeline_events.jsonl`
- `metadata/pipeline_runs.jsonl`

## Documentation

- `docs/architecture.png`
- `docs/lineage.png`
- `docs/runbook.md`
- `docs/benchmark_report.md`
- `docs/reproducibility_report.md`

## Known Limitations

The source dataset is static and lacks a natural update timestamp, so the incremental path is an explicit laboratory simulation using deterministic source-row batches.

The project reproduces the documented baseline Logistic Regression rather than every experimental model from the previous group project.

Performance measurements depend on the machine, filesystem cache, Docker overhead, database state, and other runtime conditions.

The dataset is strongly class-imbalanced, so accuracy alone is not sufficient for evaluating predictive quality.
