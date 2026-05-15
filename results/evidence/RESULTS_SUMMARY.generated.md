# Topic 5 Results Summary

Generated at: `2026-05-15 00:19:04 UTC`

This file is generated from the CSV files under `results/`. Do not hand-edit benchmark claims without regenerating evidence.

## Dataset Profile

- Rows profiled: `48722573`
- Pickup time range: `2025-01-01 00:00:00` to `2025-12-31 23:59:59`
- Distinct origin zones: `262`
- Distinct destination zones: `263`
- Distinct routes: `55538`
- Top origin zone: `237` Upper East Side South with `2125560` trips (4.36%).
- Top route: `237->236` with `302261` trips.
- Hash bucket imbalance: max/min trip count ratio `33.02x`.

## 1. Format Benchmark

| query_id | query_name                     | Parquet | ORC   | Avro   | Parquet_vs_Avro | ORC_vs_Avro |
| -------- | ------------------------------ | ------- | ----- | ------ | --------------- | ----------- |
| Q01      | trajectory_projection_scan     | 0.975   | 0.719 | 9.761  | 10.0x           | 13.6x       |
| Q03      | vendor_origin_zone_aggregation | 1.212   | 0.955 | 10.206 | 8.4x            | 10.7x       |
| Q06      | route_ranking_window           | 0.811   | 0.822 | 10.248 | 12.6x           | 12.5x       |
| Q07      | origin_destination_volume      | 0.471   | 0.520 | 9.894  | 21.0x           | 19.0x       |

Average Parquet vs Avro speedup: `11.6x`.
Average ORC vs Avro speedup: `13.3x`.

Interpretation: Parquet and ORC satisfy the columnar-storage requirement. Avro is much slower because it is row-oriented and cannot use the same vectorized columnar scan path.

## 2. Partition Benchmark

### 2.1 Historical Full-Year Partition Context

| query_id | query_name                    | raw_sec | partitioned_sec | uses_partition_pruning | partitions_scanned | result       |
| -------- | ----------------------------- | ------- | --------------- | ---------------------- | ------------------ | ------------ |
| Q02      | time_filter_partition_pruning | 0.526   | 0.365           | True                   |                    | 30.7% faster |
| Q05      | daily_zone_flow_summary       | 0.539   | 0.823           | True                   |                    | 52.8% slower |
| Q06      | route_ranking_window          | 0.838   | 1.510           | True                   |                    | 80.2% slower |
| Q07      | origin_destination_volume     | 0.508   | 0.690           | True                   |                    | 35.7% slower |

Interpretation: Spark partition pruning is present in the historical physical plans, but these full-year CSVs are mixed. They should be used as context, not as the final partition-optimization evidence.

### 2.2 One-Month Partition Pruning Evidence

| query_id | query_name            | raw_sec | partitioned_sec | uses_partition_pruning | partitions_scanned | result       |
| -------- | --------------------- | ------- | --------------- | ---------------------- | ------------------ | ------------ |
| QP01     | monthly_count_pruning | 0.091   | 0.054           | True                   | 1/12               | 40.4% faster |
| QP02     | monthly_zone_flow     | 0.142   | 0.060           | True                   | 1/12               | 58.2% faster |
| QP03     | monthly_od_volume     | 0.236   | 0.082           | True                   | 1/12               | 65.2% faster |

Interpretation: The one-month benchmark aligns timestamp filters with `trip_year = 2025` and `trip_month = 1`, and records `partitions_scanned` so data skipping is directly auditable.

## 3. Hash-Bucketed Layout Benchmark

| query_id | query_name                  | raw_sec | bucketed_sec | uses_partition_pruning | partitions_scanned | result       |
| -------- | --------------------------- | ------- | ------------ | ---------------------- | ------------------ | ------------ |
| Q11      | bucketed_origin_zone_lookup | 1.238   | 0.585        | True                   |                    | 52.8% faster |
| Q12      | bucketed_origin_zone_join   | 0.937   | 0.483        | True                   |                    | 48.4% faster |

Interpretation: The hash-bucketed `origin_zone_bucket` directory layout improves targeted spatial-key lookup/join queries. This demonstrates data skipping, although it is directory partitioning by hash bucket rather than Spark catalog `bucketBy` shuffle elimination.

## 4. Join and Skew Benchmark

### 4.1 Small Dimension Join Strategy Context

| query_id | strategy         | median_runtime_sec | shuffle_read_mb | shuffle_write_mb | spill_mb | uses_broadcast_join |
| -------- | ---------------- | ------------------ | --------------- | ---------------- | -------- | ------------------- |
| Q04      | Skew aggregation | 1.139              | 0.490           | 0.490            | 0.000    | False               |
| Q08      | SortMerge join   | 4.982              | 25.850          | 25.850           | 731.200  | False               |
| Q09      | Broadcast join   | 1.146              | 0.910           | 0.910            | 0.000    | True                |
| Q10      | Salted join      | 3.675              | 0.910           | 0.910            | 0.000    | True                |

Interpretation: Broadcast join is best for the small `taxi_zone_dim`. This is useful context but not sufficient alone to prove skew mitigation for non-broadcast distributed joins.

### 4.2 AQE On/Off Skew Join Evidence

| query_id | query_name                  | strategy          | adaptive_enabled | median_runtime_sec | best_runtime_sec | rows_returned | left_rows | right_rows | hot_key_left_rows | hot_key_right_rows | salt_count | left_skew_ratio_before | left_skew_ratio_after | uses_adaptive_plan | uses_broadcast_join | uses_sort_merge_join |
| -------- | --------------------------- | ----------------- | ---------------- | ------------------ | ---------------- | ------------- | --------- | ---------- | ----------------- | ------------------ | ---------- | ---------------------- | --------------------- | ------------------ | ------------------- | -------------------- |
| AQE_OFF  | sortmerge_skew_join_aqe_off | SortMerge AQE off | False            | 0.149              | 0.140            | 20            | 180000    | 12000      | 60000             | 80                 | 8          | 8.869                  | 1.378                 | False              | False               | True                 |
| AQE_ON   | sortmerge_skew_join_aqe_on  | SortMerge AQE on  | True             | 0.146              | 0.142            | 20            | 180000    | 12000      | 60000             | 80                 | 8          | 8.869                  | 1.378                 | True               | False               | True                 |

Interpretation: AQE on/off runs isolate Spark adaptive planning for the same broadcast-disabled skewed fact-to-fact join.

### 4.3 Large Non-Broadcast Skew Join Evidence

| query_id | query_name                | strategy                | adaptive_enabled | median_runtime_sec | best_runtime_sec | rows_returned | left_rows | right_rows | hot_key_left_rows | hot_key_right_rows | salt_count | left_skew_ratio_before | left_skew_ratio_after | uses_adaptive_plan | uses_broadcast_join | uses_sort_merge_join |
| -------- | ------------------------- | ----------------------- | ---------------- | ------------------ | ---------------- | ------------- | --------- | ---------- | ----------------- | ------------------ | ---------- | ---------------------- | --------------------- | ------------------ | ------------------- | -------------------- |
| JL01     | large_sortmerge_skew_join | SortMerge non-broadcast | True             | 0.155              | 0.136            | 20            | 180000    | 12000      | 60000             | 80                 | 8          | 8.869                  | 1.378                 | True               | False               | True                 |
| JL02     | large_salted_skew_join    | Salted non-broadcast    | True             | 0.351              | 0.325            | 20            | 180000    | 12000      | 60000             | 80                 | 8          | 8.869                  | 1.378                 | True               | False               | True                 |

Interpretation: The large supplemental join disables broadcast and compares a normal SortMerge join with a salted join. The `left_skew_ratio_before` and `left_skew_ratio_after` columns show how salting redistributes a hot join key across shuffle partitions.

## 5. Topic 5 Requirement Audit

| Requirement                             | Status    | Evidence                                                                                                                                                                                                |
| --------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Compare Parquet, ORC, Avro              | Fulfilled | Format CSVs, runtime chart, physical plans with Parquet/ORC batched scans and Avro non-batched scan.                                                                                                    |
| Optimize partitioning for data skipping | Fulfilled | One-month partition evidence reports scanned partition directories and PartitionFilters. Historical full-year CSVs remain mixed, so they are treated as context rather than the primary pruning result. |
| Optimize bucketing for data skipping    | Fulfilled | Hash-bucket directory pruning improves targeted origin-zone lookup and join queries.                                                                                                                    |
| Address data skew in distributed joins  | Fulfilled | Small-dimension joins are compared, plus supplemental AQE on/off and large non-broadcast fact-to-fact SortMerge vs salted skew joins.                                                                   |
| Quantitative evaluation with charts     | Fulfilled | Generated markdown summaries, CSV tables, and PNG charts under results/evidence.                                                                                                                        |

## 6. Generated Charts

- `results/evidence/charts/format_runtime.png`
- `results/evidence/charts/partition_month_runtime.png`
- `results/evidence/charts/partition_runtime.png`
- `results/evidence/charts/join_skew_aqe_runtime.png`
- `results/evidence/charts/large_skew_join_runtime.png`
- `results/evidence/charts/large_skew_distribution.png`
- `results/evidence/charts/bucketing_runtime.png`
- `results/evidence/charts/join_strategy_metrics.png`
- `results/evidence/charts/origin_bucket_distribution.png`
- `results/evidence/charts/top_origin_zones.png`

## 7. Recommended Work Status

| Recommended Work                                       | Status    | Evidence                                                                                      |
| ------------------------------------------------------ | --------- | --------------------------------------------------------------------------------------------- |
| One-month partition rerun with scanned partition count | Fulfilled | results/partition_month/*.csv                                                                 |
| AQE on/off join-skew comparison                        | Fulfilled | results/join_skew_aqe/trajectory_join_skew_aqe_benchmark.csv                                  |
| Large non-broadcast fact-to-fact skew join             | Fulfilled | results/join_skew_large/trajectory_large_skew_join_benchmark.csv                              |
| Generated presentation numbers from evidence CSVs      | Fulfilled | docs/RESULTS_SUMMARY.md and results/evidence/*.csv are generated by src/generate_evidence.py. |

All previously blocking Topic 5 evidence gaps are now represented by generated result files. Optional next work is to rerun the same supplemental scripts on the intended Linux/Spark/MinIO cluster for larger-scale timings.
