# DSS150P Reproducible Predictive Analytics Project

**Student:** Sophia Abad  
**Course:** DSS150P - Fundamentals of Data Engineering  
**Laboratory Activity:** Integrated Individual Laboratory Activity #4

## Project Overview

This repository reconstructs and productionizes the previous DSS141P Predictive Analytics project:

**Strengthening Philippine Stroke Care: A Machine Learning-Based Stroke Risk Assessment Focusing on Glucose Level, Hypertension, Heart Disease, BMI, and Smoking Status Factors**

The previous predictive analytics work was completed as a group project. This repository does not claim sole authorship of the historical research question, preprocessing decisions, or baseline predictive experiment.

This DSS150P Lab Activity #4 repository is Sophia Abad's individual data-engineering reconstruction of that prior work. It converts the earlier notebook-oriented workflow into a modular, rerunnable, testable, observable, and reproducible pipeline.

The implementation preserves the original dataset and predictive objective while independently implementing:

- source verification;
- full and incremental ingestion;
- checkpoint recovery;
- contract enforcement;
- quarantine handling;
- staging and curated transformations;
- multiple storage representations;
- PostgreSQL loading;
- validation;
- automated testing;
- benchmarking;
- Airflow orchestration;
- failure and recovery demonstrations; and
- predictive reproduction.

## Canonical Dataset

Dataset: **Nnaodeh - Stroke Prediction Dataset**

Source: [Nnaodeh - Stroke Prediction Dataset](https://huggingface.co/datasets/Nnaodeh/Stroke_Prediction_Dataset)

Canonical repository copy:

`data/source/Stroke_Prediction_Dataset.csv`

Source dimensions:

```text
rows:    5110
columns: 12
```

SHA-256:

```text
144ea5366832bb5432645c0e49fbb951aadf4ee1e75e73d2e15d3dc0841525bd
```

The source copy is treated as immutable. Cleaning, contract enforcement, eligibility handling, quarantine, and model-specific transformations occur only downstream.

## Architecture Summary

```text
Canonical Source CSV
        |
        v
Source Manifest + SHA-256 Verification
        |
        v
Full / Incremental Raw Ingestion
        |
        v
Raw Data-Contract Validation
        |
        +---- invalid structure ----> FAIL
        |
        v
Staging Contract Enforcement
        |
        +---- invalid rows ---------> Quarantine
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

Architecture and lineage diagrams:

- `docs/architecture.png`
- `docs/lineage.png`

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
│   ├── contract.py
│   ├── ingest.py
│   ├── transform.py
│   ├── storage.py
│   ├── train.py
│   ├── evaluate.py
│   ├── validate.py
│   ├── benchmark.py
│   ├── observe.py
│   ├── failure_demo.py
│   ├── recovery_demo.py
│   ├── reset.py
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
│   ├── model/
│   ├── storage/
│   └── benchmark/
└── docs/
    ├── architecture.png
    ├── lineage.png
    ├── runbook.md
    ├── benchmark_report.md
    ├── benchmark_results.csv
    ├── benchmark_context.json
    ├── interrupted_run_log.json
    └── reproducibility_report.md
```

## Clean-Machine Setup

Clone the repository:

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

Database definitions are stored externally in:

```text
sql/schema.sql
sql/transformations.sql
```

`src/storage.py` executes these SQL files rather than embedding the schema and UPSERT definitions directly in Python.

## Fresh Reproduction from Generated-State Reset

The repository contains selected generated artifacts as reproducibility evidence.

A reviewer who wants to reproduce the pipeline from an intentionally clean generated state can first inspect what would be removed:

```bash
python -m src.cli reset-generated --dry-run
```

This command does not modify anything.

To reset generated file state:

```bash
python -m src.cli reset-generated
```

To also reset the project-owned PostgreSQL tables:

```bash
python -m src.cli reset-generated --database
```

The reset preserves the canonical source and project definitions, including:

```text
data/source/
metadata/source_manifest.yml
metadata/data_contract.yml
metadata/expected_metrics.json
source code
tests
documentation
```

After resetting, rerun the pipeline using the commands below.

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

A first run creates the raw snapshot.

If an identical raw snapshot already exists, a rerun intentionally returns:

```text
status: SKIPPED
```

instead of creating another copy.

## Incremental Ingestion

The source is static and has no natural event or update timestamp.

Incremental processing is therefore an explicit laboratory simulation using deterministic source-row batches.

```bash
python -m src.cli ingest-incremental --max-batches 2
python -m src.cli ingest-incremental --all
```

Checkpoint state:

`metadata/ingestion_checkpoint.json`

After all 11 batches are completed, an additional run returns:

```text
status: SKIPPED
message: No remaining batches.
```

## Interrupted-Run Recovery Demonstration

The repository includes a controlled recovery demonstration using the real incremental-ingestion implementation.

Run:

```bash
python -m src.cli demo-interrupted-ingest
```

The demonstration performs the following sequence:

```text
1. Processes batches 1 and 2.
2. Writes batch 3.
3. Simulates interruption before checkpoint 3 becomes durable.
4. Verifies that the checkpoint remains at batch 2.
5. Reruns incremental ingestion.
6. Resumes from batch 3.
7. Finishes all 11 batches.
8. Verifies 5,110 rows, 5,110 unique IDs, and 0 duplicate IDs.
```

Saved evidence:

`docs/interrupted_run_log.json`

Observed final recovery state:

```text
checkpoint before interruption: 2
checkpoint after interruption:  2
batch 3 file written:            true
final batch:                     11
final rows:                      5110
unique IDs:                      5110
duplicate IDs:                   0
status:                          PASS
```

## Enforced Data Contract

The source contract is defined in:

`metadata/data_contract.yml`

The contract is loaded and enforced by:

`src/contract.py`

The implementation checks:

- required columns;
- unexpected columns;
- primary-key validity;
- declared data types;
- integer requirements;
- allowed values;
- minimum and maximum values; and
- row-level validity.

Structural violations such as missing required columns fail validation.

Invalid row values are excluded from staging and written to quarantine with explicit rejection reasons.

Examples include:

```text
hypertension not in {0, 1}
heart_disease not in {0, 1}
stroke not in {0, 1}
negative numeric values where prohibited
invalid categorical values
```

Validate the actual ingested raw layer:

```bash
python -m src.cli validate-raw
```

For the canonical dataset:

```text
status: PASS
contract_invalid_rows: 0
requires_quarantine: false
```

## Real Contract-Failure Demonstration

The repository contains controlled failure demonstrations that operate only on temporary corrupted copies.

The canonical source and raw snapshot are not modified.

### Invalid Binary Value

Run:

```bash
python -m src.cli demo-contract-failure \
  --scenario invalid-binary
```

This injects the temporary invalid value:

```text
hypertension = 7
```

Expected result:

```text
status: FAILED
error_type: ControlledDataQualityFailure
reason: hypertension: value_not_allowed
```

A `FAILED` result is expected because the data-quality control correctly detected the violation.

### Missing Required Column

Run:

```bash
python -m src.cli demo-contract-failure \
  --scenario missing-column
```

Expected result:

```text
status: FAILED
error_type: ControlledDataQualityFailure
missing required source column: hypertension
```

Again, this failure is intentional and demonstrates contract enforcement.

## Transformations

Run:

```bash
python -m src.cli transform-all
```

Observed canonical results:

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

The canonical 1,845 quarantined records are excluded by the documented modeling-age scope rather than contract corruption.

If true contract-invalid rows are introduced, their rejection reason is recorded in:

`data/quarantine/rejected_records.csv`

## Storage and PostgreSQL

Run:

```bash
python -m src.cli storage-all
```

The same 3,265 curated logical rows are represented as:

- CSV;
- JSON Lines;
- Parquet; and
- PostgreSQL.

Cross-format logical hashes verify row-set equivalence.

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

Rerun safety is implemented using the patient `id` primary key and PostgreSQL UPSERT logic defined in:

`sql/transformations.sql`

## Partition Demonstration

The curated dataset is partitioned by:

`age_score_risk`

This partition was selected because the engineered age-risk cohort is deterministic, low-cardinality, relevant to the analytical population, and produces independently loadable subsets without changing the canonical source.

Observed partitions:

```text
age_score_risk=0: 1486 rows
age_score_risk=1: 1779 rows
```

Load one partition independently:

```bash
python -m src.cli load-partition \
  --age-score-risk 1
```

The complete PostgreSQL table remains at 3,265 unique logical records because the load is rerun-safe.

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

```text
random_state=42
stratified 80/20 train/test split
persisted split manifest
StandardScaler
baseline Logistic Regression
class_weight="balanced"
pinned Python dependencies
```

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

Expected values:

`metadata/expected_metrics.json`

Reproduced values:

`outputs/metrics.json`

## Validation

Run the complete validation suite:

```bash
python -m src.cli validate-all
```

The validation covers:

- source identity;
- actual raw-layer checksum;
- data-contract enforcement;
- required package versions;
- required non-empty repository files;
- staging and curated data-quality assertions;
- storage equivalence; and
- PostgreSQL connectivity and row state.

Expected final result:

```text
status: PASS
```

## Automated Tests

Run:

```bash
python -m pytest -c ./pytest.ini -q
```

Current verified result:

```text
8 passed
```

The automated suite covers:

- source integrity;
- deterministic feature preparation;
- logical hashing;
- contract rejection of invalid binary values;
- quarantine of invalid records;
- storage equivalence;
- PostgreSQL rerun safety;
- selected-partition loading; and
- interrupted-ingestion recovery.

## Storage Benchmark

Run:

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

These results describe this machine and workload only and are not universal storage-format rankings.

Detailed evidence:

```text
docs/benchmark_results.csv
docs/benchmark_context.json
docs/benchmark_report.md
```

## Apache Airflow

Build and start:

```bash
docker compose up -d --build postgres airflow
```

Check imports:

```bash
docker compose exec airflow \
airflow dags list-import-errors
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

The DAG includes:

- retries;
- retry delay;
- task execution timeout;
- DAG-level timeout;
- `max_active_runs=1`;
- `max_active_tasks=2`;
- runtime parameters;
- structured success/failure callbacks; and
- persisted task and run records.

## Airflow Contract Failure and Recovery

Trigger a genuine contract-driven failure:

```bash
docker compose exec airflow \
airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id lab4_contract_failure \
--conf '{"failure_scenario":"invalid-binary"}'
```

The `validate_raw` task uses a temporary corrupted dataset and detects an invalid contract value.

The canonical repository data remains unchanged.

Observed:

```text
lab4_contract_failure    FAILED
```

Trigger recovery using canonical data:

```bash
docker compose exec airflow \
airflow dags trigger \
dss150p_predictive_repro_pipeline \
--run-id lab4_contract_recovery \
--conf '{"failure_scenario":"none"}'
```

Observed:

```text
lab4_contract_recovery   SUCCESS
```

Structured orchestration evidence:

```text
metadata/pipeline_events.jsonl
metadata/pipeline_runs.jsonl
```

## Engineering Gap Summary

| Previous Notebook-Oriented Limitation | DSS150P Engineering Control |
|---|---|
| Environment assumptions were implicit | Pinned dependencies, Docker, and `.env.example` |
| Source identity was not machine-verified | SHA-256 source manifest |
| Raw and derived data were not clearly separated | Source, raw, staging, curated, and quarantine layers |
| Data-quality rules were mostly procedural | Enforced YAML data contract |
| Invalid rows could silently continue | Contract-driven quarantine or validation failure |
| Incremental recovery was absent | Durable checkpoint plus interrupted-run demonstration |
| Database reruns could risk duplication | Primary key plus UPSERT |
| SQL logic was embedded in application code | External `schema.sql` and `transformations.sql` |
| Train/test identity was not independently persisted | Split-ID manifest and hashes |
| Pipeline execution relied on notebook order | Reusable Python modules, CLI, and Airflow |
| Failure behavior was not observable | Structured task and run event logs |
| Storage comparisons could compare unlike data | Full and filtered logical hashes |
| Timing could depend on one observation | Repeated benchmark with median and standard deviation |
| Reproduction required local manual knowledge | README, runbook, reset command, tests, and validation |

## Attribution

The historical DSS141P predictive analytics project, including its research topic and baseline experiment, originated from the prior group work.

This DSS150P repository is an individual engineering reconstruction by **Sophia Abad**.

Its individual contribution is the reproducibility and data-engineering implementation, including:

- source verification;
- data layers;
- ingestion;
- checkpoint recovery;
- data-contract enforcement;
- quarantine handling;
- storage engineering;
- PostgreSQL loading;
- validation;
- testing;
- benchmarking;
- Airflow orchestration;
- recovery controls; and
- documentation.

The repository therefore distinguishes the **historical group predictive work** from the **individual Lab Activity #4 engineering implementation**.

## Documentation

Primary supporting documents:

```text
docs/architecture.png
docs/lineage.png
docs/runbook.md
docs/benchmark_report.md
docs/reproducibility_report.md
docs/interrupted_run_log.json
```

## Known Limitations

The source dataset is static and lacks a natural update timestamp, so incremental ingestion is an explicit deterministic laboratory simulation rather than a business event-time feed.

The project reproduces the documented baseline Logistic Regression rather than every experimental model from the previous predictive analytics project.

Timing measurements depend on hardware, operating system, filesystem cache, database state, Docker overhead, and other runtime conditions.

The dataset is strongly class-imbalanced, so accuracy alone is insufficient for assessing predictive quality.

The historical baseline numbers are reproduced from the repository's documented prior-project values. Verification against the original previous-term notebook or final paper would require those original artifacts.

## Final Verification

Before submission, run:

```bash
python -m src.cli verify-source
python -m src.cli validate-raw
python -m src.cli validate-all
python -m src.cli evaluate
python -m src.cli storage-verify
python -m pytest -c ./pytest.ini -q
```

Check Airflow:

```bash
docker compose exec airflow \
airflow dags list-import-errors
```

Check Git formatting and status:

```bash
git diff --check
git status
```

Verified current state:

```text
pytest:         8 passed
validate-all:   PASS
git diff check: clean
```