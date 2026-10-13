# Reproducibility Report

## 1. Scope

This report documents the individual DSS150P Lab Activity #4 reconstruction of the prior predictive analytics project on stroke risk.

The goal was not to preserve a notebook exactly as it existed. The goal was to preserve the **meaning of the data and predictive experiment** while rebuilding the workflow as a reproducible data-engineering implementation with explicit source identity, transformations, storage, orchestration, testing, and recovery behavior.

## 2. Historical Project and Reproduction Target

Previous project:

**Strengthening Philippine Stroke Care: A Machine Learning-Based Stroke Risk Assessment Focusing on Glucose Level, Hypertension, Heart Disease, BMI, and Smoking Status Factors**

Canonical source:

`data/source/Stroke_Prediction_Dataset.csv`

Source identity:

- rows: 5,110
- columns: 12
- SHA-256: `144ea5366832bb5432645c0e49fbb951aadf4ee1e75e73d2e15d3dc0841525bd`

Primary reproduced model:

**Baseline Logistic Regression**

Historical metrics:

- ROC-AUC: 0.6716
- Accuracy: 0.6815
- Recall: 0.5714

## 3. Reproducible Pipeline Design

The implementation separates responsibilities into reusable modules:

- `src/ingest.py` - source integrity, full ingestion, incremental/checkpoint simulation, audit
- `src/transform.py` - deterministic staging and curated transformations
- `src/storage.py` - storage formats, logical equivalence, partitions, PostgreSQL UPSERT
- `src/train.py` - deterministic model training and split manifest
- `src/evaluate.py` - predictions, metrics, tolerance comparison
- `src/validate.py` - environment, file, data-quality, storage, and database checks
- `src/observe.py` - structured Airflow task/run records
- `src/benchmark.py` - repeated storage benchmark
- `src/cli.py` - command-line interface
- `dags/predictive_repro_pipeline.py` - thin Airflow orchestration layer

## 4. Data Lineage Summary

```text
Hugging Face source
      |
      v
data/source/Stroke_Prediction_Dataset.csv
      |
      | checksum + schema verification
      v
data/raw/full + data/raw/incremental
      |
      v
data/staging/stroke_cleaned.csv
      | \
      |  \--> data/quarantine/rejected_records.csv
      v
data/curated/stroke_model_ready.csv
      |
      +--> CSV / JSONL / Parquet
      |
      +--> PostgreSQL curated.stroke_model_ready
      |
      +--> model training
                |
                +--> metadata/split_manifest.json
                +--> outputs/model/
                +--> outputs/predictions.csv
                +--> outputs/metrics.json
```

## 5. Ingestion and Rerun Safety

### Full ingestion

The source checksum is verified before ingestion. A full raw snapshot is copied atomically. If the same snapshot already exists with the same checksum, the run is skipped. If a conflicting file exists, the pipeline fails instead of overwriting it.

### Incremental demonstration

The source has no natural update timestamp. The pipeline therefore does **not** invent one. Instead, the source is split into deterministic row-order batches and progress is recorded in `metadata/ingestion_checkpoint.json`.

The checkpoint strategy is explicitly labeled as a laboratory simulation.

### Auditability

`metadata/ingestion_audit.csv` records run IDs, UTC timestamps, mode, status, sequence ranges, row counts, batch numbers, paths, hashes, and messages.

## 6. Transformations

The staging layer performs data cleaning and eligibility handling while preserving rejected records in quarantine.

Observed result:

- source: 5,110 rows
- staging: 3,265 rows
- quarantine: 1,845 rows

The curated layer produces the model-ready dataset and engineered risk variables.

Observed curated target counts:

- stroke = 0: 3,019
- stroke = 1: 246

## 7. Storage Reproducibility

The curated dataset is written to CSV, JSONL, Parquet, and PostgreSQL.

Full logical hash:

`ee5df2e0884ce360a85252cf9bf1bb5a062e24a3f4ffc631682ee358562b44a8`

All file formats and PostgreSQL match this same logical row set.

PostgreSQL loading is rerun-safe through the `id` primary key and UPSERT behavior. Repeated full loads remain at:

- table rows: 3,265
- distinct IDs: 3,265

Selected partition loading is also independently rerunnable.

## 8. Model Determinism

The model reproduction uses:

- deterministic random seed: 42
- stratified 80/20 split
- persisted training/test IDs
- fixed feature preparation
- `StandardScaler`
- baseline Logistic Regression
- `class_weight="balanced"`
- pinned dependency versions

Split sizes:

- training: 2,612 rows
- test: 653 rows

The test IDs are persisted in `metadata/split_manifest.json`, allowing the same evaluation population to be reconstructed.

## 9. Predictive Result Consistency

| Metric | Expected | Reproduced | Absolute Difference | Tolerance | Result |
|---|---:|---:|---:|---:|---|
| ROC-AUC | 0.6716 | 0.6716448169 | 0.0000448169 | 0.01 | PASS |
| Accuracy | 0.6815 | 0.6814701378 | 0.0000298622 | 0.01 | PASS |
| Recall | 0.5714 | 0.5714285714 | 0.0000285714 | 0.01 | PASS |

The reproduced predictive result therefore satisfies the declared tolerance rule.

## 10. Testing and Validation

`python -m src.cli validate-all` verifies:

- source identity;
- required dependencies;
- required repository files;
- data-quality assertions;
- storage equivalence;
- PostgreSQL connectivity and row state.

Automated tests:

```text
5 passed
```

Tests include:

- source integrity;
- row-order-independent logical hashing;
- deterministic feature preparation;
- full storage equivalence;
- PostgreSQL rerun safety;
- selected-partition loading.

## 11. Airflow Orchestration

DAG:

`dss150p_predictive_repro_pipeline`

Sequence:

```text
ingest_source
→ validate_raw
→ transform_staging
→ build_curated
→ load_postgres
→ validate_curated
→ train_model
→ evaluate_model
→ reproducibility_check
→ publish_outputs
```

Operational controls:

- retries: 1
- retry delay: 10 seconds
- task execution timeout: 10 minutes
- `max_active_runs=1`
- `max_active_tasks=2`
- DAG run timeout: 30 minutes
- `catchup=False`
- runtime parameters for ingestion mode and failure injection

The Airflow DAG remains thin and delegates work to the reusable modules.

## 12. Failure and Recovery Demonstration

Observed sequence:

```text
lab4_success    SUCCESS
lab4_failure    FAILED
lab4_recovery   SUCCESS
```

The failed run intentionally used:

```json
{"inject_failure": true}
```

The `validate_raw` task retried and ultimately logged a structured `ValueError` with final `try_number: 2`.

The recovery run disabled the injected failure and completed successfully.

Evidence:

- `metadata/pipeline_events.jsonl`
- `metadata/pipeline_runs.jsonl`

## 13. Storage Benchmark Summary

The same logical dataset was benchmarked using five repeated writes, full reads, and filtered reads/queries.

Parquet was the smallest and fastest representation for this specific small analytical workload.

The result is not generalized to all workloads. PostgreSQL offers transactional and constraint behavior that a pure file benchmark does not measure.

See:

`docs/benchmark_report.md`

## 14. Deviations from the Prior Group Project

The reproduction intentionally differs from the previous project in engineering structure:

1. The workflow is modular Python rather than notebook-only.
2. Source identity is controlled by checksum and manifest.
3. Raw, staging, curated, quarantine, partitioned, and output layers are explicit.
4. Incremental processing is an explicit deterministic simulation because the source lacks a natural timestamp.
5. PostgreSQL loading is rerun-safe.
6. Model split IDs and expected metrics are machine-readable.
7. Airflow orchestration and structured failure/recovery evidence are included.
8. Storage equivalence is proven with logical hashes.
9. Storage benchmarks use repeated measurements instead of a single timing.
10. Automated validation and tests are included.

These changes are implementation improvements, not changes to the intended predictive question.

# 15. Required Analysis and Reflection Questions

## 15.1 What aspects of the previous predictive analytics project prevented another person from reproducing it immediately?

The previous work was primarily notebook/project-file oriented rather than an engineered pipeline. Reproduction therefore depended on implicit execution order, local environment state, manually available files, and undocumented operational steps. It also lacked an explicit immutable source checksum, externalized configuration, raw/staging/curated data layers, checkpointed ingestion, rerun-safe database loading, automated tests, and orchestrated failure/recovery evidence. Lab Activity #4 makes those assumptions explicit.

## 15.2 Which artifact is the true authoritative source: the original file, the raw ingested copy, the curated dataset, or the model-ready dataset? Explain the role of each.

The authoritative data source is the canonical original source file identified by `metadata/source_manifest.yml` and its SHA-256 checksum.

- **Source file:** source truth and provenance anchor.
- **Raw ingested copy:** immutable operational snapshot used by the pipeline.
- **Staging:** cleaned and eligibility-controlled representation; rejected rows remain traceable through quarantine.
- **Curated/model-ready dataset:** authoritative input for the reproduced predictive experiment, but not a replacement for source truth.
- **Model artifact:** derived output and therefore never the authoritative data source.

## 15.3 What identifier or rule did you use to prevent duplicates, and what assumption makes that rule valid?

Patient `id` is the logical primary key. The source profile verifies 5,110 unique IDs, and the curated PostgreSQL table declares `id` as its primary key. UPSERT uses `ON CONFLICT (id)` so retries update/reconfirm the logical record instead of adding a duplicate. This is valid only under the assumption that the dataset's `id` consistently identifies one logical patient record.

## 15.4 If your dataset has no natural update timestamp, how did you simulate incremental ingestion without pretending that the generated checkpoint is a real business timestamp?

The incremental path uses deterministic source-row sequence batches, not fabricated timestamps. The checkpoint records the source checksum, configured batch size, total rows, total batches, completed batch metadata, and last completed batch. It is explicitly labeled `strategy: immutable_source_row_sequence` and `laboratory_simulation: true`. This demonstrates checkpoint/resume behavior without misrepresenting sequence numbers as business event time.

## 15.5 Which transformations belong in staging versus curated, and why?

Staging contains source cleaning and eligibility operations needed to create a valid, consistent analytical population: category cleanup, age eligibility, deterministic BMI handling, duplicate prevention, and quarantine.

Curated contains downstream model-oriented feature engineering such as risk scores and engineered variables. This separation keeps source-cleaning semantics distinct from features created specifically for analytical/model use and makes each layer independently inspectable.

## 15.6 Which storage format performed best for your workload? Why should your result not be generalized to every data-engineering workload?

Parquet performed best in this benchmark: it was the smallest representation and had the lowest median write, full-read, and filtered-read times.

This result should not be generalized because the benchmark is small, local, single-user, and machine-specific. Different row counts, schemas, compression, filters, concurrency, network latency, database indexes, and access patterns can change the result. PostgreSQL also provides transactional and constraint capabilities that file timing alone does not measure.

## 15.7 What happens if Airflow retries your load task after the database committed but the task process did not receive confirmation?

The task may replay the same database load. A non-idempotent insert could create duplicates or fail on repeated records. In this implementation, replaying the load is safe for the curated table because `id` is the primary key and the operation uses UPSERT. The logical table therefore remains at 3,265 unique rows.

## 15.8 How does your design make that retry safe?

Retry safety is implemented at several layers:

- PostgreSQL uses primary-key UPSERT by `id`.
- Full ingestion checks the destination checksum and skips an identical snapshot.
- Incremental ingestion persists completed batches in a checkpoint.
- Existing incremental batch files are checksum-verified.
- JSON checkpoint writes use a temporary file followed by atomic replacement.
- Transformations are deterministic and overwrite reproducible derived outputs rather than append uncontrolled duplicates.

## 15.9 Which data-quality check is most likely to detect a change that would silently damage model accuracy?

Among the current data-quality assertions, the exact curated row/class-count checks are particularly useful because a change in filtering or target population can alter the model without producing an obvious schema error. Source checksum/schema verification catches unapproved source changes. The strongest final safeguard is the model reproducibility comparison in `outputs/metrics.json`, which detects a behavior change even if structural checks still pass.

## 15.10 How do you know that your training and test rows are the same logical rows used in your baseline result?

The split is deterministic with `random_state=42` and stratification, and the resulting training and test patient IDs are persisted in `metadata/split_manifest.json`. The manifest includes row counts and SHA-256 hashes for the train/test ID sets. Evaluation reconstructs the test population from those persisted IDs instead of performing a new uncontrolled split.

## 15.11 What sources of nondeterminism remain in your model or environment?

Remaining nondeterminism includes floating-point/library implementation differences across platforms, low-level numerical libraries, filesystem cache effects, Docker/database scheduling, benchmark timing noise, and Airflow-generated timestamps/run metadata. Dependency pinning, fixed split IDs, fixed random state, and tolerance-based metric comparison reduce but do not eliminate all environment-level variation.

## 15.12 If the reproduced metric differs from the original, how can you determine whether the cause is data, preprocessing, split logic, dependencies, model parameters, or nondeterministic execution?

Diagnose stage by stage:

1. verify the source checksum and schema;
2. compare raw/staging/curated row counts and logical hashes;
3. verify transformation configuration and engineered columns;
4. compare persisted train/test IDs and split hashes;
5. verify pinned package versions;
6. compare serialized model configuration and feature columns;
7. rerun training/evaluation under the same environment;
8. compare machine-readable expected versus actual metrics.

This isolates where the first reproducibility divergence appears.

## 15.13 What would you monitor if this pipeline ran every day in production?

I would monitor source checksum/version, schema changes, ingestion lag/checkpoint progress, row counts, quarantine rate, missingness, category/domain violations, duplicate IDs, class distribution, task duration, retry/failure rate, database row counts, storage growth, prediction distribution, metric drift when labels are available, Airflow timeout/SLA behavior, and repeated recovery events.

## 15.14 What part of your current architecture would become the first bottleneck if the dataset grew by 100x?

The current row-oriented PostgreSQL UPSERT and single-process local processing would become likely bottlenecks before they are problematic at the current scale. The benchmark already shows PostgreSQL writes as the slowest write path. At much larger scale I would replace row-wise loading with bulk loading/staging-table merge techniques, use more scalable columnar processing, and move from a sequential local executor to an execution model appropriate for parallel workloads.

## 15.15 What would you change if the dataset became sensitive or regulated data?

I would stop committing raw or record-level derived data to a public repository. Source manifests would reference controlled storage instead. I would use encryption in transit and at rest, least-privilege access, private networking, secret management, audited access, retention/deletion rules, restricted logs, de-identification or tokenization where appropriate, and organization-approved environments. Public repository artifacts would be limited to code, schemas, synthetic examples, non-sensitive metadata, and reproducibility instructions.

## 16. Acceptance Evidence Summary

| Acceptance Area | Evidence |
|---|---|
| Repository/setup | Git history, README, pinned requirements, Docker files |
| Source identity | `source_manifest.yml`, SHA-256 validation |
| No machine-specific paths required | path resolution from repository/configuration |
| No committed secrets | `.gitignore`, `.env.example` |
| Full ingestion | raw snapshot + ingestion audit |
| Incremental ingestion | checkpoint + deterministic batches |
| Deterministic staging/curated | repeatable transformations and validation |
| PostgreSQL rerun safety | primary key + UPSERT + integration test |
| Storage equivalence | identical logical hashes |
| Selected partition | independent `age_score_risk=1` load |
| Airflow | valid DAG, explicit dependency chain |
| Recoverable failure | `lab4_failure` followed by `lab4_recovery` |
| Invalid-condition detection | source/data validators + deliberate Airflow failure |
| Second-run duplicate safety | table remains 3,265 distinct IDs |
| Regenerated metrics | `train` + `evaluate` from curated data |
| Tolerance rule | all required metrics PASS |
| Documentation | README, runbook, benchmark report, reproducibility report, diagrams |

## 17. Conclusion

The final repository is not merely a copy of the previous notebook project. It is an independently structured reproducible implementation with explicit source identity, deterministic transformations, rerun-safe storage, repeatable model evaluation, tests, Airflow orchestration, failure/recovery evidence, and machine-readable acceptance checks.

The reproduced Logistic Regression result matches the historical baseline within the documented tolerance, while the surrounding engineering controls make the result explainable and repeatable.
