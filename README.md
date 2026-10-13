# DSS150P Reproducible Predictive Analytics Project

**Student:** Sophia Abad  
**Course:** DSS150P - Fundamentals of Data Engineering  
**Laboratory Activity:** Integrated Individual Laboratory Activity #4

## Project

Reproduction and data-engineering modularization of the previous Predictive Analytics project:

**Strengthening Philippine Stroke Care: A Machine Learning-Based Stroke Risk Assessment Focusing on Glucose Level, Hypertension, Heart Disease, BMI, and Smoking Status Factors**

## Objective

This repository converts the previous notebook-based predictive analytics project into a reproducible data-engineering workflow.

The workflow is designed to:

1. Preserve and verify the original source dataset.
2. Ingest data into a controlled raw layer.
3. Clean, validate, deduplicate, and transform data into staging.
4. Produce a curated model-ready dataset.
5. Train and evaluate a deterministic predictive model.
6. Store and compare data using multiple storage representations.
7. Support rerunnable PostgreSQL loading.
8. Orchestrate the workflow using Apache Airflow.
9. Validate data quality, reproducibility, and idempotency.
10. Reproduce predictive results within a documented tolerance.

## Dataset Source

Original dataset:

**Nnaodeh - Stroke Prediction Dataset**

Hugging Face:

`https://huggingface.co/datasets/Nnaodeh/Stroke_Prediction_Dataset`

The immutable source dataset will be stored in:

`data/source/`

Its integrity will be recorded using a SHA-256 checksum in:

`metadata/source_manifest.yml`

## Repository Structure

```text
config/              Configuration
data/source/         Immutable original source
data/raw/            Ingested source snapshots or batches
data/staging/        Cleaned, typed, deduplicated data
data/curated/        Model-ready dataset
data/partitioned/    Partitioned storage representation
data/quarantine/     Invalid or rejected records
metadata/            Profiles, contracts, manifests, expected metrics
src/                 Reusable Python pipeline modules
sql/                 PostgreSQL schema and SQL transformations
dags/                Apache Airflow DAG
tests/               Unit and integration tests
outputs/             Metrics, predictions, and model outputs
docs/                Architecture, lineage, runbook, benchmarks, report