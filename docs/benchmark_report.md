# Storage Benchmark Report

## 1. Objective

This benchmark compares four storage representations using the same curated logical dataset:

- CSV
- JSON Lines
- Parquet
- PostgreSQL

The benchmark was designed to avoid two weak comparisons:

1. comparing different logical datasets; and
2. relying on only one timing observation.

Every representation is verified against canonical full-row and filtered-row logical hashes.

## 2. Benchmark Dataset

Canonical benchmark input:

`data/curated/stroke_model_ready.csv`

Expected full dataset:

```text
rows: 3265
```

Filtered workload:

```text
stroke == 1
```

Expected filtered dataset:

```text
rows: 246
```

The benchmark therefore compares the same analytical population across every storage representation.

## 3. Workloads

Each representation is measured for:

1. write or UPSERT;
2. full read; and
3. filtered read/query.

The benchmark is run with:

```text
5 repetitions
```

for each operation.

The reported summary includes:

- median;
- mean internally;
- standard deviation;
- minimum internally; and
- maximum internally.

The report focuses on median and standard deviation because one timing observation can be strongly affected by cache state, system load, database scheduling, or filesystem behavior.

## 4. Cold/Warm Run Policy

The benchmark does **not** execute and discard a separate warm-up repetition.

All five measured repetitions are included in the reported statistics.

Therefore, the first measured repetition may include additional cold-cache, initialization, filesystem, or database effects.

This is intentional and should be considered when interpreting the standard deviation.

The benchmark should be understood as a descriptive measurement of the observed local environment rather than a controlled microbenchmark that completely isolates cache effects.

## 5. Environment

Recorded benchmark context:

```text
Python:   3.9.6
Platform: macOS ARM64
Repeats:  5
```

PostgreSQL write timing measures:

```text
rerun-safe UPSERT + transaction commit
```

Performance values are machine- and environment-specific.

They should not be treated as universal rankings.

## 6. Results

| Storage | Size (bytes) | Median Write (s) | Write SD (s) | Median Full Read (s) | Full Read SD (s) | Median Filtered Read (s) | Filtered Read SD (s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| CSV | 253,279 | 0.0081805 | 0.0004186 | 0.0020265 | 0.0001541 | 0.0037926 | 0.0008140 |
| JSONL | 1,101,963 | 0.0062749 | 0.0002704 | 0.0121160 | 0.0012118 | 0.0147380 | 0.0006512 |
| Parquet | 90,335 | 0.0031610 | 0.0191428 | 0.0011086 | 0.0274120 | 0.0014997 | 0.0004570 |
| PostgreSQL | 1,728,512 | 0.0778098 | 0.0271070 | 0.0203078 | 0.0059504 | 0.0099002 | 0.0067678 |

## 7. Full-Row Logical Equivalence

Expected full logical hash:

```text
ee5df2e0884ce360a85252cf9bf1bb5a062e24a3f4ffc631682ee358562b44a8
```

The following representations all produced the same full logical hash:

```text
CSV
JSONL
Parquet
PostgreSQL
```

Expected full row count:

```text
3265
```

All representations matched this count.

This means the benchmark is not comparing storage formats that happen to have similar sizes or schemas; it is comparing the same logical row set.

## 8. Filtered Logical Equivalence

Filtered workload:

```text
stroke == 1
```

Expected filtered row count:

```text
246
```

Expected filtered logical hash:

```text
233774545d2a6a25b7cd071f1ea452289274316fb1d47d2a7e9a54c443d5aac1
```

CSV, JSONL, Parquet, and PostgreSQL all matched this filtered logical hash.

Therefore, filtered-read/query timing also compares equivalent logical results.

## 9. Interpretation by Storage Type

### CSV

CSV used:

```text
253,279 bytes
```

It remained simple and portable and performed adequately for the small dataset.

CSV's strengths in this project are interoperability and readability rather than schema enforcement or advanced analytical filtering.

### JSON Lines

JSONL used:

```text
1,101,963 bytes
```

It was the largest file representation.

Its file-based read timings were also slower than CSV and Parquet in this workload.

Its main advantage is flexible line-oriented interchange rather than compact analytical storage.

### Parquet

Parquet used:

```text
90,335 bytes
```

It was the smallest file representation.

It also had the lowest median:

```text
write time
full-read time
filtered-read time
```

for this benchmark.

Parquet's columnar representation is well suited to analytical access patterns, but the observed timing values are specific to this small local dataset and environment.

The comparatively large standard deviations on some Parquet measurements also reinforce why a single timing observation should not be used as the conclusion.

### PostgreSQL

PostgreSQL relation size was:

```text
1,728,512 bytes
```

This value is not directly equivalent to plain file size because it includes database storage structures and overhead.

PostgreSQL produced the slowest median write in the benchmark.

That operation intentionally measures:

```text
primary-key-aware UPSERT
+
transaction commit
```

rather than an uncontrolled append.

PostgreSQL therefore provides behavior that the file formats do not provide directly, including:

- primary-key enforcement;
- constraints;
- rerun-safe writes;
- transactions;
- query execution;
- indexing capability;
- concurrent access; and
- persistent database management.

Its value should not be reduced to raw write speed.

## 10. Partition Design Context

The pipeline separately demonstrates partitioned storage using:

`age_score_risk`

Observed partition sizes:

```text
age_score_risk=0: 1486 rows
age_score_risk=1: 1779 rows
```

This column was selected because it is:

- deterministic;
- low-cardinality;
- analytically relevant;
- already part of the curated feature set; and
- suitable for demonstrating independent subset loading.

The partition choice is not presented as a universal production optimization.

At significantly larger scale, partition design should instead be based on actual query patterns, cardinality, pruning effectiveness, storage engine behavior, and expected data growth.

## 11. Why the Benchmark Should Not Be Generalized

The benchmark contains only:

```text
3265 rows
```

Results can change significantly with:

- millions or billions of rows;
- wider schemas;
- different compression codecs;
- different query predicates;
- different Parquet row-group sizing;
- different CSV parsers;
- networked databases;
- database indexes;
- database vacuum/analyze state;
- concurrent users;
- different Docker configurations;
- local filesystem cache;
- memory pressure;
- CPU architecture;
- storage hardware;
- operating system;
- Python version; and
- package versions.

The correct conclusion is therefore workload-specific:

**Parquet performed best for this small local analytical benchmark, while PostgreSQL provides operational database guarantees that raw file timing does not measure.**

## 12. Benchmark Reproducibility

Run:

```bash
python -m src.cli benchmark --repeats 5
```

Machine-readable results:

```text
docs/benchmark_results.csv
```

Benchmark environment/context:

```text
docs/benchmark_context.json
```

The implementation:

1. reads the same canonical curated dataset;
2. calculates the expected full logical hash;
3. calculates the expected filtered logical hash;
4. repeats each operation five times;
5. verifies each full read against the canonical hash;
6. verifies each filtered read/query against the filtered hash;
7. reports repeated timing statistics; and
8. writes machine-readable benchmark evidence.

## 13. Benchmark Limitations

This benchmark does not attempt to measure:

- high concurrency;
- distributed storage;
- cloud object storage;
- network latency;
- database replication;
- index design alternatives;
- very large datasets;
- sustained throughput;
- multi-user contention;
- transaction isolation overhead across workloads; or
- long-running production behavior.

It is intentionally scoped to the DSS150P reproducibility exercise.

## 14. Conclusion

The benchmark demonstrates two engineering principles.

First, storage-performance comparisons should use equivalent logical data.

Second, repeated measurements are more defensible than a single timing result.

For the tested environment:

```text
Parquet:
- smallest file representation
- lowest median write time
- lowest median full-read time
- lowest median filtered-read time
```

PostgreSQL was slower for writes but provided transactional, constraint, primary-key, and rerun-safety behavior that the file formats do not provide directly.

The benchmark therefore supports a workload-specific storage discussion rather than a universal ranking of storage technologies.