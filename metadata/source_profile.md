# Source Dataset Profile

## Dataset

**Name:** Stroke Prediction Dataset  
**Source:** Nnaodeh / Hugging Face  
**Canonical file:** `data/source/Stroke_Prediction_Dataset.csv`

## Integrity

- SHA-256: `144ea5366832bb5432645c0e49fbb951aadf4ee1e75e73d2e15d3dc0841525bd`
- File size: 327,451 bytes
- Rows: 5,110
- Columns: 12
- Exact duplicate rows: 0
- Unique `id` values: 5,110

## Schema Profile

| Column | Source Type | Missing | Observed Values / Range |
|---|---|---:|---|
| `id` | integer | 0 | 67 to 72,940; 5,110 unique |
| `gender` | string | 0 | Female, Male, Other |
| `age` | float | 0 | 0.08 to 82.0 |
| `hypertension` | integer | 0 | 0, 1 |
| `heart_disease` | integer | 0 | 0, 1 |
| `ever_married` | string | 0 | Yes, No |
| `work_type` | string | 0 | Private, Self-employed, children, Govt_job, Never_worked |
| `Residence_type` | string | 0 | Urban, Rural |
| `avg_glucose_level` | float | 0 | 55.12 to 271.74 |
| `bmi` | float | 201 | 10.3 to 97.6 among non-null records |
| `smoking_status` | string | 0 | never smoked, Unknown, formerly smoked, smokes |
| `stroke` | integer | 0 | 0, 1 |

## Target Distribution

- No stroke (`0`): 4,861 records, approximately 95.13%
- Stroke (`1`): 249 records, approximately 4.87%

The source is strongly class-imbalanced.

## Missing Values

The only source column with missing values is `bmi`.

- Missing BMI values: 201
- Non-missing BMI values: 4,909

The immutable source is not repaired in place. Missing-value handling occurs in downstream staging.

## Source-Layer Policy

The pipeline does not:

- edit the source CSV in place;
- silently repair source values;
- overwrite missing values in the source layer; or
- alter source records to satisfy model requirements.

Cleaning, eligibility filtering, feature engineering, and model preparation occur downstream.

## Provenance, Availability, and Privacy

Public source repository:

`https://huggingface.co/datasets/Nnaodeh/Stroke_Prediction_Dataset`

The exact original retrieval date from the previous group project was not recorded. For Lab Activity #4, source identity is controlled through the committed manifest and SHA-256 checksum.

No credentials are required to access the referenced public source. If a future version contains sensitive or regulated information, the raw dataset should not be committed to a public repository; the manifest should instead reference an approved controlled location with appropriate access controls.
