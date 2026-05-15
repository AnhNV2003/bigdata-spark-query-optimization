# Slides Content

## Slide 1 - Cover

- Distributed Query Optimization on Columnar Storage
- Big Data and Processing - IT5427E
- Dataset: NYC Yellow Taxi Trip Records 2025
- Technology stack: Apache Spark SQL, MinIO, Python, Jupyter
- Group 13
- Nguyen Viet Minh
- Truong Gia Bach
- Ngo Viet Anh

## Slide 2 - Problem Statement

- Large analytical datasets are expensive to query when physical layout does not match workload patterns
- Spark SQL queries can waste time on:
  - reading unnecessary columns
  - scanning irrelevant files
  - shuffling data across executors
  - sorting large intermediate data
  - spilling data to disk
- Taxi trips are modeled as trajectory segments:
  - origin zone
  - destination zone
  - pickup/dropoff time
  - distance and fare attributes
- Main question:
  - How do storage format, data layout, and join strategy affect distributed query performance?
- Optimization directions:
  - columnar file formats
  - time partitioning
  - origin-zone hash bucketing
  - join strategy and skew handling

## Slide 3 - System Architecture

- Data source: NYC TLC monthly taxi trip files
- Storage layer: MinIO S3-compatible object storage
- Compute layer: Apache Spark SQL
- Processing access: Spark reads/writes through S3A paths
- Normalized logical table: `trips_clean`
- Benchmark layouts:
  - raw Parquet
  - raw ORC
  - raw Avro
  - year/month partitioned Parquet
  - origin-zone hash-bucketed Parquet
  - taxi zone dimension table
- Output artifacts:
  - CSV benchmark metrics
  - physical query plans
  - generated charts
  - validation reports

## Slide 4 - Data Pipeline

- Download NYC Yellow Taxi monthly files
- Upload raw files to MinIO bucket
- Normalize schema differences across monthly files
- Build common `trips_clean` schema
- Generate benchmark datasets in multiple formats and layouts
- Execute Spark SQL benchmark query groups
- Collect runtime, rows returned, shuffle, spill, and plan signals
- Generate evidence tables and visualizations under `results/evidence/`
- Refresh summary documents from generated CSV evidence

## Slide 5 - Dataset and Benchmark Protocol

- Dataset profile:
  - 48,722,573 trips
  - time range: 2025-01-01 to 2025-12-31
  - 262 origin zones
  - 263 destination zones
  - 55,538 distinct routes
  - top origin zone: 237, Upper East Side South
  - top origin-zone volume: 2,125,560 trips, 4.36% of all trips
- Benchmark query groups:
  - format: projection, aggregation, route ranking, origin-destination volume
  - partition: monthly count, monthly zone flow, monthly OD volume
  - bucketing: origin-zone lookup and origin-zone join
  - join/skew: SortMerge, broadcast, salted join, AQE on/off
- Main metrics:
  - median runtime
  - shuffle read/write
  - disk spill
  - physical-plan evidence
  - generated charts and validation status

## Slide 6 - Compare Performance Across Formats

- Compared formats:
  - Parquet
  - ORC
  - Avro
- Benchmark queries:
  - Q01 trajectory projection scan
  - Q03 vendor-origin zone aggregation
  - Q06 route ranking window
  - Q07 origin-destination volume
- Key results:
  - average Parquet vs Avro speedup: 11.6x
  - average ORC vs Avro speedup: 13.3x
- Example runtimes:
  - Q01: Parquet 0.975s, ORC 0.719s, Avro 9.761s
  - Q07: Parquet 0.471s, ORC 0.520s, Avro 9.894s
- Interpretation:
  - Parquet and ORC are columnar and read selected columns efficiently
  - Avro is row-oriented and must deserialize full records
  - columnar formats are better for scan-heavy analytical workloads
- Evidence:
  - `results/evidence/format_summary.csv`
  - `results/evidence/charts/format_runtime.png`

## Slide 7 - Optimizing Partition

- Layout strategy:
  - partition Parquet data by `trip_year` and `trip_month`
- Target workload:
  - time-window analytics
  - monthly filtering
  - daily zone flow
  - monthly origin-destination volume
- Final partition evidence:
  - query filters align with `trip_year = 2025` and `trip_month = 1`
  - Spark scans 1 of 12 monthly partition directories
  - physical plan contains `PartitionFilters`
- Runtime results:
  - QP01 monthly count: 0.091s raw, 0.054s partitioned, 40.4% faster
  - QP02 monthly zone flow: 0.142s raw, 0.060s partitioned, 58.2% faster
  - QP03 monthly OD volume: 0.236s raw, 0.082s partitioned, 65.2% faster
- Interpretation:
  - partitioning helps when query filters match partition columns
  - partitioning is less useful when runtime is dominated by shuffle, sort, or window operations
- Evidence:
  - `results/evidence/partition_month_summary.csv`
  - `results/evidence/charts/partition_month_runtime.png`

## Slide 8 - Optimizing Bucketing

- Layout strategy:
  - compute `origin_zone_bucket = PMOD(HASH(origin_zone_id), 16)`
  - write records into origin-zone hash-bucket directories
- Target workload:
  - repeated lookup by origin zone
  - joins filtered by origin zone
  - spatial-key query acceleration
- Runtime results:
  - Q11 origin-zone lookup: 1.238s raw, 0.585s bucketed, 52.8% faster
  - Q12 origin-zone join: 0.937s raw, 0.483s bucketed, 48.4% faster
- Interpretation:
  - bucketed layout reduces scan scope for zone-targeted queries
  - performance gain is lower than theoretical 16x because Spark still has task scheduling, metadata, filtering, aggregation, and join overhead
  - this is directory-level data skipping, not Spark catalog `bucketBy` shuffle elimination
- Evidence:
  - `results/evidence/bucketing_summary.csv`
  - `results/evidence/charts/bucketing_runtime.png`
  - `results/evidence/charts/origin_bucket_distribution.png`

## Slide 9 - Data Skew

- Skew source:
  - taxi activity is concentrated in popular zones
  - top origin zone 237 has 2,125,560 trips
  - skewed keys can overload one shuffle partition during distributed joins
- Join strategy results:
  - SortMerge join: 4.982s runtime, 25.850 MB shuffle read, 731.200 MB spill
  - Broadcast join: 1.146s runtime, 0.910 MB shuffle read, 0 MB spill
  - Broadcast is 4.3x faster than SortMerge for the 265-row taxi-zone dimension
- Non-broadcast skew evidence:
  - broadcast disabled
  - SortMerge join retained
  - AQE off/on comparison included
  - salting reduces left-key skew ratio from 8.869 to 1.378
  - skew distribution improves by about 6.4x
- Interpretation:
  - broadcast is best for small dimension joins
  - SortMerge can cause shuffle and disk spill
  - salting helps redistribute hot keys when broadcast is not possible
  - salting has overhead and is not always faster for small local runs
- Evidence:
  - `results/evidence/join_summary.csv`
  - `results/evidence/join_skew_aqe_summary.csv`
  - `results/evidence/join_skew_large_summary.csv`
  - `results/evidence/charts/join_strategy_metrics.png`
  - `results/evidence/charts/large_skew_distribution.png`

## Slide 10 - Conclusion

- Main findings:
  - Parquet and ORC are much faster than Avro for analytical trajectory queries
  - partition pruning reduces scan scope for time-window queries
  - origin-zone hash bucketing accelerates spatial-key lookup and join workloads
  - broadcast join eliminates shuffle and spill for small dimension tables
  - AQE and salting are useful tools for non-broadcast skewed joins
- Practical recommendations:
  - use columnar formats for analytics
  - partition by common time filters
  - bucket by frequent spatial lookup keys
  - broadcast small dimensions
  - apply salting only when skew cost exceeds salting overhead
- Project outputs:
  - generated benchmark evidence in `results/evidence/`
  - generated charts in `results/evidence/charts/`
  - terminal demo command: `bash src/demo_topic5.sh --presentation-mode`
  - validation report shows all checks pass
- Final message:
  - scalable Spark analytics requires workload-aware storage layout and query planning
