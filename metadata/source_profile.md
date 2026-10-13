cat > metadata/source_profile.md <<'EOF'
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

## Categorical Distribution Notes

### Gender

- Female: 2,994
- Male: 2,115
- Other: 1

### Work Type

- Private: 2,925
- Self-employed: 819
- children: 687
- Govt_job: 657
- Never_worked: 22

### Residence Type

- Urban: 2,596
- Rural: 2,514

### Smoking Status

- never smoked: 1,892
- Unknown: 1,544
- formerly smoked: 885
- smokes: 789

## Target Distribution

Target column: `stroke`

- No stroke (`0`): 4,861 records — approximately 95.13%
- Stroke (`1`): 249 records — approximately 4.87%

The source dataset is therefore strongly class-imbalanced.

## Missing-Value Findings

The only source column with missing values is `bmi`:

- Missing BMI values: 201
- Non-missing BMI values: 4,909

Missing BMI values are preserved in the immutable source layer. Any handling of these records must occur in staging or later pipeline stages and must be deterministic and documented.

## Source-Layer Policy

The file under `data/source/` is treated as immutable source truth.

The pipeline must not:

- edit the source CSV in place;
- silently repair source values;
- overwrite missing values in the source layer; or
- modify source records to satisfy model requirements.

Cleaning, eligibility filtering, feature engineering, encoding, and model preparation occur in downstream layers.
EOF