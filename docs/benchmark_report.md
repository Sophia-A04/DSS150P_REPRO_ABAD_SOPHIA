# Storage Benchmark Report

## Objective

The storage experiment compares CSV, JSON Lines, Parquet, and PostgreSQL using the same 3,265-row curated logical dataset.

The benchmark was designed to avoid two weak comparisons:

1. timing different logical datasets; and
2. relying on only one timing observation.

Every representation is verified against canonical full-row and filtered-row logical hashes.

## Workloads

Each storage representation was tested five times for:

1. write / UPSERT;
2. full read; and
3. filtered read/query where `stroke == 1`.

Expected logical counts:

- full dataset: 3,265 rows
- filtered dataset: 246 rows

Reported statistics include median and standard deviation.

## Environment

From `docs/benchmark_context.json`:

- Python: 3.9.6
- Platform: macOS ARM64
- repeats: 5
- PostgreSQL write operation: rerun-safe UPSERT plus commit

Performance results are machine-specific and should not be interpreted as universal rankings.

## Results

| Storage | Size (bytes) | Median Write (s) | Write SD (s) | Median Full Read (s) | Full Read SD (s) | Median Filtered Read (s) | Filtered Read SD (s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| CSV | 253,279 | 0.0081805 | 0.0004186 | 0.0020265 | 0.0001541 | 0.0037926 | 0.0008140 |
| JSONL | 1,101,963 | 0.0062749 | 0.0002704 | 0.0121160 | 0.0012118 | 0.0147380 | 0.0006512 |
| Parquet | 90,335 | 0.0031610 | 0.0191428 | 0.0011086 | 0.0274120 | 0.0014997 | 0.0004570 |
| PostgreSQL | 1,728,512 | 0.0778098 | 0.0271070 | 0.0203078 | 0.0059504 | 0.0099002 | 0.0067678 |

## Logical Equivalence

All four representations produced the same full logical hash:

`ee5df2e0884ce360a85252cf9bf1bb5a062e24a3f4ffc631682ee358562b44a8`

All four also produced the same filtered logical hash for `stroke == 1`:

`233774545d2a6a25b7cd071f1ea452289274316fb1d47d2a7e9a54c443d5aac1`

Therefore, the benchmark compares equivalent logical data rather than merely similar row counts.

## Interpretation

For this dataset and this machine, Parquet produced the smallest file and the lowest median write, full-read, and filtered-read timings. This is consistent with a compact columnar representation being effective for analytical workloads.

CSV was larger than Parquet but remained simple, portable, and fast for a small dataset.

JSONL was the largest file representation and had the slowest file-based read timings in this benchmark. Its advantage is flexible line-oriented interchange rather than compact analytical storage.

PostgreSQL had the slowest median write because the tested operation intentionally includes rerun-safe UPSERT behavior and a database transaction commit. It also carries database metadata/index overhead not represented by a plain data file. Its value is not raw file speed; it provides constraints, primary-key enforcement, querying, concurrency, and durable transactional loading.

## Why the Result Should Not Be Generalized

The benchmark uses only 3,265 curated rows. Results may change with:

- much larger datasets;
- wider tables;
- different filters;
- compression configuration;
- database indexing;
- concurrent users;
- networked databases;
- warm versus cold filesystem cache;
- different hardware and operating systems.

The correct conclusion is therefore workload-specific: **Parquet performed best for this small analytical benchmark, while PostgreSQL provides operational database capabilities not captured by file-format timing alone.**

## Reproducibility

Run:

```bash
python -m src.cli benchmark --repeats 5
```

Machine-readable evidence:

- `docs/benchmark_results.csv`
- `docs/benchmark_context.json`
