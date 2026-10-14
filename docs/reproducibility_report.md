# Reproducibility Report

## 1. Scope

This report documents the individual DSS150P Laboratory Activity #4 reconstruction of a previous predictive analytics project on stroke risk.

The objective was not to preserve the previous notebook exactly as it originally existed. The objective was to preserve the meaning of the source data and predictive experiment while rebuilding the workflow as a reproducible data-engineering pipeline with explicit source identity, ingestion, data contracts, transformation layers, storage controls, testing, orchestration, recovery behavior, and predictive verification.

## 2. Historical Project and Attribution

Previous project:

**Strengthening Philippine Stroke Care: A Machine Learning-Based Stroke Risk Assessment Focusing on Glucose Level, Hypertension, Heart Disease, BMI, and Smoking Status Factors**

The historical predictive analytics project was completed as group work.

This DSS150P repository is an individual engineering reconstruction by **Sophia Abad**.

The historical group work provided the research topic, baseline predictive objective, and documented baseline results.

The individual DSS150P implementation adds the reproducibility and data-engineering controls, including:

- source identity verification;
- full and incremental ingestion;
- checkpoint recovery;
- data-contract enforcement;
- quarantine handling;
- staging and curated data layers;
- external SQL definitions;
- PostgreSQL rerun safety;
- storage equivalence;
- automated tests;
- benchmarking;
- Airflow orchestration;
- structured failure logging;
- reproducibility validation; and
- operational documentation.

## 3. Canonical Source

Canonical source file:

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

The source copy is treated as immutable.

Its identity is controlled by:

`metadata/source_manifest.yml`

The pipeline verifies the source before ingestion and also checks that the full raw snapshot matches the verified source checksum.

## 4. Predictive Reproduction Target

Primary reproduced model:

**Baseline Logistic Regression**

Documented historical metrics:

```text
ROC-AUC:  0.6716
Accuracy: 0.6815
Recall:   0.5714
```

Machine-readable expected values:

`metadata/expected_metrics.json`

## 5. Reproducible Pipeline Design

The implementation separates responsibilities into reusable modules:

- `src/config.py` - repository configuration and path resolution
- `src/contract.py` - data-contract loading and enforcement
- `src/ingest.py` - source verification, full ingestion, incremental ingestion, checkpointing, and audit
- `src/transform.py` - deterministic staging, quarantine, and curated transformations
- `src/storage.py` - storage formats, logical equivalence, partitioning, PostgreSQL loading, and SQL execution
- `src/train.py` - deterministic model training and split persistence
- `src/evaluate.py` - predictions, metrics, and baseline comparison
- `src/validate.py` - raw, environment, package, file, data-quality, storage, and database validation
- `src/benchmark.py` - repeated storage benchmarking
- `src/observe.py` - structured Airflow task and DAG event records
- `src/failure_demo.py` - genuine contract-failure demonstrations
- `src/recovery_demo.py` - interrupted incremental-ingestion demonstration
- `src/reset.py` - safe generated-state reset
- `src/cli.py` - unified command-line interface
- `dags/predictive_repro_pipeline.py` - thin Airflow orchestration layer

The Airflow DAG delegates pipeline work to the same reusable modules used by the CLI.

## 6. Data Lineage

```text
Hugging Face source
        |
        v
data/source/Stroke_Prediction_Dataset.csv
        |
        | SHA-256 + manifest verification
        v
data/raw/full/
        |
        +-----------------------------+
        |                             |
        v                             v
raw contract validation       incremental raw batches
        |                             |
        |                             v
        |                   checkpoint + recovery
        |
        v
data/staging/stroke_cleaned.csv
        |
        +---- invalid / excluded ----> data/quarantine/rejected_records.csv
        |
        v
data/curated/stroke_model_ready.csv
        |
        +--> CSV
        |
        +--> JSONL
        |
        +--> Parquet
        |
        +--> PostgreSQL
        |
        +--> deterministic model training
                    |
                    +--> metadata/split_manifest.json
                    +--> outputs/model/
                    +--> outputs/predictions.csv
                    +--> outputs/metrics.json
```

Architecture and lineage diagrams are stored in:

```text
docs/architecture.png
docs/lineage.png
```

## 7. Full Ingestion and Rerun Safety

Full ingestion first verifies the canonical source checksum.

The raw snapshot is written without modifying the canonical source.

If an identical raw snapshot already exists, the pipeline returns:

```text
status: SKIPPED
```

instead of creating duplicate data.

A conflicting raw snapshot causes failure rather than silent overwrite.

This makes repeated execution safe and explicit.

## 8. Incremental Ingestion Strategy

The dataset has no natural business update timestamp.

The pipeline therefore does not invent one.

Incremental ingestion is explicitly implemented as a laboratory simulation using deterministic source-row batches.

Checkpoint state is stored in:

`metadata/ingestion_checkpoint.json`

The checkpoint tracks completion by batch number rather than pretending that the generated sequence represents business event time.

The canonical run contains:

```text
total batches: 11
total rows:    5110
```

After all batches are complete, another incremental run returns:

```text
status: SKIPPED
message: No remaining batches.
```

## 9. Interrupted-Run Recovery

A dedicated recovery demonstration exercises the real incremental-ingestion implementation.

Command:

```bash
python -m src.cli demo-interrupted-ingest
```

The demonstration performs the following sequence:

```text
1. Process batches 1 and 2 normally.
2. Write batch 3.
3. Interrupt execution before checkpoint 3 becomes durable.
4. Confirm checkpoint remains at batch 2.
5. Rerun incremental ingestion.
6. Resume from batch 3.
7. Complete through batch 11.
8. Verify final row and ID counts.
```

Observed recovery evidence:

```text
checkpoint before interruption: 2
checkpoint after interruption:  2
batch 3 file exists:             true
last completed batch:            11
rows:                            5110
unique IDs:                      5110
duplicate IDs:                   0
status:                          PASS
```

Saved evidence:

`docs/interrupted_run_log.json`

This demonstrates that a written-but-uncheckpointed batch does not cause duplicate logical rows after recovery.

## 10. Data Contract Enforcement

The declared source contract is stored in:

`metadata/data_contract.yml`

The implementation is in:

`src/contract.py`

The contract validates:

- expected columns;
- missing required columns;
- unexpected columns;
- primary-key validity;
- uniqueness requirements;
- numeric types;
- integer requirements;
- string types;
- nullability;
- allowed categorical or binary values;
- minimum values; and
- maximum values.

Structural violations such as a missing required column stop processing.

Row-level violations are assigned explicit rejection reasons and prevented from entering staging/modeling.

For example:

```text
hypertension must be in {0, 1}
heart_disease must be in {0, 1}
stroke must be in {0, 1}
```

The canonical source produces:

```text
contract_invalid_rows: 0
status: PASS
```

## 11. Genuine Invalid-Condition Demonstration

The original failure demonstration was replaced with a real data-quality failure.

Command:

```bash
python -m src.cli demo-contract-failure \
  --scenario invalid-binary
```

The demonstration creates a temporary copy in which:

```text
hypertension = 7
```

The contract correctly detects:

```text
hypertension: value_not_allowed
```

and returns:

```text
error_type: ControlledDataQualityFailure
status: FAILED
```

A second scenario removes the required `hypertension` column:

```bash
python -m src.cli demo-contract-failure \
  --scenario missing-column
```

This also fails through genuine schema validation.

The canonical source and raw snapshot are not modified by either demonstration.

## 12. Staging and Quarantine

The staging layer performs deterministic source cleaning and modeling-population eligibility handling.

Canonical results:

```text
source rows:            5110
staging rows:           3265
quarantined rows:       1845
contract-invalid rows:  0
```

The canonical 1,845 rejected rows result from the documented modeling-age scope rather than invalid clinical binary values.

Quarantine output:

`data/quarantine/rejected_records.csv`

Each rejected row includes a `rejection_reason`.

Invalid contract rows, if introduced, are therefore traceable rather than silently discarded.

## 13. Curated Data

Curated output:

`data/curated/stroke_model_ready.csv`

Canonical curated rows:

```text
3265
```

Observed target distribution:

```text
stroke = 0: 3019
stroke = 1: 246
```

The curated layer contains the model-oriented engineered variables used for the reproduced predictive experiment.

It remains distinct from the canonical source and raw layers.

## 14. Externalized SQL

Database definitions are stored in:

```text
sql/schema.sql
sql/transformations.sql
```

`src/storage.py` loads and executes these files rather than keeping the complete schema and UPSERT logic embedded only inside Python.

This makes the database implementation separately inspectable and directly addresses the requirement that SQL artifacts be meaningful project components.

## 15. PostgreSQL Rerun Safety

The curated PostgreSQL table uses patient `id` as the logical primary key.

Repeated loads use UPSERT behavior.

Observed after reruns:

```text
table rows:   3265
distinct IDs: 3265
status:       PASS
```

Therefore, retrying or rerunning the load does not create duplicate logical records.

## 16. Storage Equivalence

The curated dataset is represented as:

- CSV;
- JSON Lines;
- Parquet; and
- PostgreSQL.

Full logical hash:

```text
ee5df2e0884ce360a85252cf9bf1bb5a062e24a3f4ffc631682ee358562b44a8
```

Filtered workload:

```text
stroke == 1
```

Filtered logical hash:

```text
233774545d2a6a25b7cd071f1ea452289274316fb1d47d2a7e9a54c443d5aac1
```

All tested representations match both the canonical full-row hash and filtered-row hash.

Therefore, the storage benchmark compares equivalent logical data rather than merely matching row counts.

## 17. Partition Design

The curated dataset is partitioned by:

`age_score_risk`

Observed partitions:

```text
age_score_risk=0: 1486 rows
age_score_risk=1: 1779 rows
```

The partition column was selected because it is:

- deterministic;
- derived consistently from the modeling-age population;
- low-cardinality;
- relevant to the analytical risk grouping; and
- suitable for demonstrating independent subset loading.

The partitioning strategy is a laboratory demonstration rather than a claim that `age_score_risk` is universally optimal for large-scale production partitioning.

Selected-partition loading is tested independently and remains rerun-safe.

## 18. Model Determinism

The reproduced model uses:

```text
random_state=42
stratified 80/20 train/test split
persisted split IDs
StandardScaler
baseline Logistic Regression
class_weight="balanced"
pinned package versions
```

Observed split:

```text
training rows: 2612
test rows:      653
```

The split IDs are stored in:

`metadata/split_manifest.json`

This makes the train/test population independently inspectable.

## 19. Predictive Result Consistency

| Metric | Expected | Reproduced | Absolute Difference | Tolerance | Result |
|---|---:|---:|---:|---:|---|
| ROC-AUC | 0.6716 | 0.6716448169 | 0.0000448169 | 0.01 | PASS |
| Accuracy | 0.6815 | 0.6814701378 | 0.0000298622 | 0.01 | PASS |
| Recall | 0.5714 | 0.5714285714 | 0.0000285714 | 0.01 | PASS |

The reproduced Logistic Regression therefore satisfies the declared metric-tolerance rule.

## 20. Validation

The complete validation command is:

```bash
python -m src.cli validate-all
```

It verifies:

- canonical source identity;
- actual raw-layer checksum;
- raw-layer data contract;
- required package versions;
- required non-empty project files;
- staging data quality;
- quarantine integrity;
- curated data quality;
- storage equivalence;
- PostgreSQL connectivity;
- PostgreSQL row count; and
- PostgreSQL distinct-ID count.

Current verified result:

```text
status: PASS
```

## 21. Automated Tests

Command:

```bash
python -m pytest -c ./pytest.ini -q
```

Current verified result:

```text
8 passed
```

The suite covers:

- source integrity;
- row-order-independent logical hashing;
- deterministic model feature preparation;
- contract detection of invalid binary values;
- quarantine behavior for an invalid record;
- full storage equivalence;
- PostgreSQL rerun safety;
- selected-partition loading; and
- interrupted incremental-ingestion recovery.

## 22. Airflow Orchestration

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

Operational controls include:

```text
retries:             1
retry delay:         10 seconds
task timeout:        10 minutes
max_active_runs:     1
max_active_tasks:    2
DAG run timeout:     30 minutes
catchup:             false
```

The DAG remains thin and delegates processing to reusable modules.

## 23. Airflow Failure and Recovery

A real contract-driven Airflow failure was demonstrated using:

```json
{"failure_scenario":"invalid-binary"}
```

Observed run:

```text
lab4_contract_failure_final    FAILED
```

The `validate_raw` task fails because the temporary demonstration dataset contains a genuine invalid contract value.

A subsequent canonical run uses:

```json
{"failure_scenario":"none"}
```

Observed run:

```text
lab4_contract_recovery_final   SUCCESS
```

Structured task and DAG evidence is stored in:

```text
metadata/pipeline_events.jsonl
metadata/pipeline_runs.jsonl
```

This is stronger than an arbitrary manually raised exception because the failure is produced by an actual data-quality control.

## 24. Fresh-Clone Reproduction

The repository contains selected generated artifacts as reproducibility evidence.

To prevent a fresh clone from appearing permanently "already done," a safe reset command is available.

Inspect without deleting:

```bash
python -m src.cli reset-generated --dry-run
```

Reset generated files:

```bash
python -m src.cli reset-generated
```

Reset generated files and project-owned PostgreSQL tables:

```bash
python -m src.cli reset-generated --database
```

The reset preserves the canonical source, source manifest, data contract, expected metrics, source code, tests, and documentation.

This gives a reviewer an explicit route from repository clone to regenerated pipeline state.

## 25. Storage Benchmark Summary

The same 3,265-row logical dataset is benchmarked across CSV, JSONL, Parquet, and PostgreSQL.

Each representation is measured repeatedly for:

- write or UPSERT;
- full read; and
- filtered read/query.

All representations are verified against full and filtered logical hashes during the benchmark.

For the current small local analytical workload, Parquet produced the smallest representation and lowest median times.

This result is explicitly machine-specific and should not be generalized to every workload.

See:

`docs/benchmark_report.md`

## 26. Engineering Gap Summary

| Previous Notebook-Oriented Limitation | DSS150P Engineering Control |
|---|---|
| Environment assumptions were implicit | Pinned dependencies, Docker, `.env.example` |
| Source identity was not machine-verified | SHA-256 source manifest |
| Source and derived files were not clearly separated | Source, raw, staging, curated, quarantine layers |
| Data-quality rules were procedural | Executable YAML data contract |
| Invalid records could silently continue | Contract-driven quarantine or failure |
| Incremental recovery was absent | Durable checkpoint plus interruption/recovery demonstration |
| Fresh clone could appear already processed | Safe `reset-generated` command |
| Database reruns could duplicate records | Primary key plus UPSERT |
| SQL logic was not externally inspectable | `schema.sql` and `transformations.sql` |
| Train/test identity was implicit | Persisted split IDs and hashes |
| Workflow depended on execution order | Reusable modules, CLI, and Airflow DAG |
| Failures were weakly observable | Structured JSONL task/run records |
| Storage comparisons could use unequal data | Full and filtered logical hashes |
| Timing could rely on one observation | Repeated benchmark plus median and standard deviation |
| Operational reproduction required manual knowledge | README, runbook, reset command, validation, and tests |

## 27. Analysis and Reflection

### 27.1 What prevented immediate reproduction of the previous predictive project?

The previous work was primarily notebook-oriented. Reproduction depended on implicit execution order, local environment state, locally available files, and manually remembered steps.

The reconstructed pipeline makes those assumptions explicit through configuration, pinned dependencies, source manifests, data layers, tests, validation, and orchestration.

### 27.2 What is the authoritative source?

The canonical source file identified by the manifest and checksum is the source of truth.

Roles are separated as follows:

- **Source:** provenance anchor and immutable data truth.
- **Raw:** operational ingestion snapshot.
- **Staging:** cleaned and contract-controlled analytical population.
- **Quarantine:** traceable rejected or excluded records.
- **Curated:** model-ready analytical representation.
- **Model/output files:** derived products, never source truth.

### 27.3 How are duplicates prevented?

Patient `id` is treated as the logical primary key.

PostgreSQL uses primary-key UPSERT behavior.

Incremental ingestion uses deterministic batch identity and persisted checkpoints.

Repeated runs therefore preserve logical uniqueness.

### 27.4 How is incremental ingestion simulated without inventing a timestamp?

The source contains no natural event/update timestamp.

Incremental ingestion therefore uses deterministic source-row batches and a batch-number checkpoint.

The generated checkpoint is explicitly operational state and not represented as a business timestamp.

### 27.5 Which transformations belong in staging versus curated?

Staging contains source cleaning, contract enforcement, eligibility logic, imputation, and quarantine decisions.

Curated contains model-oriented feature engineering.

This separates source/data-quality semantics from predictive-feature construction.

### 27.6 Which storage format performed best?

For this small local benchmark, Parquet had the smallest file size and lowest median write/read timings.

The conclusion is workload-specific and should not be generalized because scale, hardware, cache state, indexes, concurrency, compression, network conditions, and workload design can change the result.

### 27.7 What happens if Airflow retries after PostgreSQL already committed?

The same logical load can be replayed.

Because PostgreSQL uses `id` as the primary key and performs UPSERT, the replay updates or reconfirms existing logical rows instead of appending duplicates.

### 27.8 How is retry safety implemented?

Retry safety is supported by:

- checksum-based full-ingestion skipping;
- durable incremental checkpoints;
- existing batch-file verification;
- atomic checkpoint replacement;
- deterministic transformations;
- PostgreSQL primary-key UPSERT; and
- repeatable model split persistence.

### 27.9 Which data-quality checks are most important for protecting model accuracy?

Important checks include:

- source checksum;
- schema validity;
- binary-domain checks;
- modeling-population row counts;
- target-class counts;
- persisted split identity; and
- final predictive metric comparison.

Together these can detect both structural corruption and silent behavioral changes.

### 27.10 How do we know the same logical test population is used?

The train/test split is deterministic and the patient IDs are persisted in:

`metadata/split_manifest.json`

The split manifest includes row counts and hashes that identify the logical train/test sets.

### 27.11 What nondeterminism remains?

Possible environment-level variation remains from:

- floating-point implementations;
- operating-system differences;
- low-level numerical libraries;
- filesystem caching;
- Docker scheduling;
- PostgreSQL execution state;
- Airflow-generated timestamps; and
- benchmark timing noise.

Fixed seeds, pinned dependencies, split persistence, and tolerance-based comparison reduce these effects.

### 27.12 How should a metric mismatch be diagnosed?

Diagnose in order:

```text
1. source checksum
2. raw checksum
3. contract validation
4. staging row count
5. quarantine reasons
6. curated logical hash
7. train/test split IDs
8. dependency versions
9. model parameters
10. reproduced metrics
```

The first stage that diverges identifies the most likely cause.

### 27.13 What should be monitored in production?

Useful monitoring would include:

- source version/checksum;
- schema changes;
- ingestion checkpoint lag;
- quarantine rate;
- missingness;
- domain violations;
- duplicate IDs;
- class distribution;
- task duration;
- retries and failures;
- PostgreSQL row counts;
- prediction distribution;
- metric drift when labels become available; and
- repeated recovery events.

### 27.14 What would become a bottleneck at 100x scale?

Likely bottlenecks include:

- row-oriented Python processing;
- repeated local file reads;
- row-wise PostgreSQL UPSERT;
- single-host execution; and
- local storage throughput.

At greater scale, bulk loading, staged database merges, scalable columnar processing, and distributed/parallel execution would be considered.

### 27.15 What would change for sensitive or regulated data?

The public repository would no longer contain raw or record-level derived data.

The design would require controlled storage, encryption, least-privilege access, secret management, audit logging, retention policies, restricted networking, and de-identification or tokenization where appropriate.

## 28. Acceptance Evidence Summary

| Acceptance Area | Evidence |
|---|---|
| Canonical source identity | `metadata/source_manifest.yml` |
| Source checksum | SHA-256 verification |
| Full ingestion | raw snapshot plus ingestion audit |
| Incremental ingestion | deterministic batches plus checkpoint |
| Interrupted recovery | `docs/interrupted_run_log.json` |
| Data contract | `metadata/data_contract.yml`, `src/contract.py` |
| Invalid-row handling | quarantine rejection reasons |
| Genuine failure detection | `src/failure_demo.py` |
| Staging/curated determinism | repeatable transformations and validation |
| PostgreSQL rerun safety | primary key plus UPSERT |
| External SQL | `sql/schema.sql`, `sql/transformations.sql` |
| Storage equivalence | identical logical hashes |
| Selected partition | independent `age_score_risk=1` load |
| Model determinism | persisted split manifest |
| Reproduced metrics | `outputs/metrics.json` |
| Metric tolerance | expected vs actual PASS |
| Automated tests | `8 passed` |
| Airflow orchestration | valid 10-task DAG |
| Real Airflow failure | `lab4_contract_failure_final` |
| Airflow recovery | `lab4_contract_recovery_final` |
| Structured run records | `pipeline_events.jsonl`, `pipeline_runs.jsonl` |
| Fresh-clone reset | `reset-generated` |
| Benchmark evidence | results CSV, context JSON, benchmark report |
| Documentation | README, runbook, reports, diagrams |

## 29. Known Limitations

The dataset is static and contains no natural business update timestamp. Incremental ingestion is therefore a deterministic laboratory simulation.

The project reproduces the documented Logistic Regression baseline rather than every model explored in the historical project.

Benchmark results are machine-specific.

The dataset is class-imbalanced, so accuracy alone is not sufficient to judge model usefulness.

Verification of the historical baseline directly against the original previous-term notebook or paper requires those original artifacts.

## 30. Conclusion

The final repository is not simply a copy of a previous notebook.

It is an independently structured reproducible data-engineering implementation with:

- explicit source identity;
- contract enforcement;
- deterministic transformations;
- traceable quarantine;
- restartable incremental ingestion;
- interrupted-run recovery evidence;
- rerun-safe storage;
- external SQL;
- automated validation;
- automated testing;
- repeated storage benchmarking;
- Airflow orchestration;
- genuine data-quality failure handling; and
- predictive metric reproduction.

The reproduced Logistic Regression result matches the documented historical baseline within the declared tolerance while providing substantially stronger engineering controls for repeatability, observability, and recovery.