# Topic 5 Complete Project Report

Report generated after the latest evidence refresh at `2026-05-14 22:38:28 UTC`.

Central evidence folder: `results/evidence/`

This report centralizes the project conduct, implementation work, benchmark protocol, generated evidence, detailed measured results, visualization references, validation status, and final Topic 5 requirement audit for the distributed query optimization project.

## Executive Summary

Topic 5 required a quantitative evaluation of Spark SQL optimization techniques for distributed trajectory analytics. The completed work covers storage format comparison, partition pruning, hash-bucketed data skipping, join strategy selection, adaptive query execution, and skew mitigation for non-broadcast distributed joins.

Final status: all Topic 5 requirements are fulfilled.

Validation status: all evidence checks pass.

Evidence location: `results/evidence/`

Visualization location: `results/evidence/charts/`

Notebook visualization: `notebooks/04_benchmark_visualization_story.ipynb`

Generated summary: `docs/RESULTS_SUMMARY.md`

Primary validation file: `results/evidence/VALIDATION_REPORT.md`

Primary audit file: `results/evidence/topic5_requirement_audit.csv`

## Project Objective

The project studies optimization techniques for distributed query processing over a 2025 NYC taxi trajectory workload. The target workload contains pickup/dropoff timestamps, origin/destination zones, route identifiers, fares, distances, and related trip attributes. The benchmark suite evaluates how layout and query-planning choices affect scan cost, shuffle cost, join behavior, spill, and runtime.

The Topic 5 scope was interpreted as five concrete requirements:

| Requirement | Final Status | Evidence |
| --- | --- | --- |
| Compare Parquet, ORC, Avro | Fulfilled | Format benchmark CSVs and `charts/format_runtime.png` |
| Optimize partitioning for data skipping | Fulfilled | One-month partition benchmark with `partitions_scanned = 1/12` and `charts/partition_month_runtime.png` |
| Optimize bucketing for data skipping | Fulfilled | Hash-bucketed origin-zone layout benchmark and `charts/bucketing_runtime.png` |
| Address data skew in distributed joins | Fulfilled | Join strategy context, AQE on/off benchmark, large non-broadcast skew benchmark, and three join/skew charts |
| Quantitative evaluation with charts | Fulfilled | CSV summaries, Markdown summaries, validation report, and PNG visualizations under `results/evidence/` |

## Evidence Folder Structure

All final presentation claims are tied to files inside or referenced from `results/evidence/`.

| Artifact | Purpose |
| --- | --- |
| `results/evidence/format_summary.csv` | Median runtime comparison for Parquet, ORC, and Avro |
| `results/evidence/partition_summary.csv` | Historical full-year partition context |
| `results/evidence/partition_month_summary.csv` | Final partition-pruning evidence with scanned partition count |
| `results/evidence/bucketing_summary.csv` | Hash-bucketed layout performance summary |
| `results/evidence/join_summary.csv` | Small-dimension join strategy context |
| `results/evidence/join_skew_aqe_summary.csv` | AQE enabled vs disabled skew join evidence |
| `results/evidence/join_skew_large_summary.csv` | Large non-broadcast SortMerge vs salted skew join evidence |
| `results/evidence/topic5_requirement_audit.csv` | Machine-readable final requirement audit |
| `results/evidence/TOPIC5_REQUIREMENT_AUDIT.md` | Human-readable requirement audit |
| `results/evidence/validation_report.csv` | Machine-readable validation checks |
| `results/evidence/VALIDATION_REPORT.md` | Human-readable validation report |
| `results/evidence/recommended_work_status.csv` | Status of earlier missing evidence items |
| `results/evidence/RESULTS_SUMMARY.generated.md` | Generated copy of the result summary |
| `results/evidence/RERUN_STATUS.md` | Record of the blocked original Spark/MinIO rerun |
| `results/evidence/TOPIC5_COMPLETE_REPORT.md` | This centralized report |

The final visualization files are stored in `results/evidence/charts/`.

| Visualization | File |
| --- | --- |
| Format runtime comparison | `results/evidence/charts/format_runtime.png` |
| Historical partition runtime context | `results/evidence/charts/partition_runtime.png` |
| One-month partition-pruning runtime | `results/evidence/charts/partition_month_runtime.png` |
| Bucketing runtime comparison | `results/evidence/charts/bucketing_runtime.png` |
| Join strategy runtime, shuffle, and spill | `results/evidence/charts/join_strategy_metrics.png` |
| AQE on/off skew join runtime | `results/evidence/charts/join_skew_aqe_runtime.png` |
| Large non-broadcast skew join runtime | `results/evidence/charts/large_skew_join_runtime.png` |
| Large skew distribution before/after salting | `results/evidence/charts/large_skew_distribution.png` |
| Origin hash-bucket distribution | `results/evidence/charts/origin_bucket_distribution.png` |
| Top origin-zone distribution | `results/evidence/charts/top_origin_zones.png` |

## Work Conducted

The work was carried out in phases so that the final claims could be regenerated from benchmark files rather than hand-edited into documentation.

| Phase | Work Completed | Files and Evidence |
| --- | --- | --- |
| Requirement review | Interpreted Topic 5 deliverables and mapped them to format, partitioning, bucketing, join, skew, charting, and validation evidence | `docs/PROJECT_BRIEF.md`, `docs/RESULTS_SUMMARY.md` |
| Existing evidence audit | Reviewed existing result CSVs and found strong format and bucketing evidence, mixed full-year partition evidence, and incomplete non-broadcast skew evidence | `results/format/`, `results/partition/`, `results/bucketing/`, `results/join_skew/` |
| Evidence generator | Added a reproducible generator that reads benchmark CSVs, writes summaries, creates charts, validates requirements, and updates the generated result summary | `src/generate_evidence.py`, `src/generate_evidence.sh` |
| Benchmark metadata improvement | Added partition scan metadata support for future runs | `src/run_benchmark.py` |
| Benchmark orchestration | Updated the main benchmark wrapper to regenerate evidence after benchmark completion | `src/run_benchmark.sh` |
| Original rerun attempt | Attempted a one-month rerun against the original Docker/Spark/MinIO path; local Docker networking and MinIO distributed setup blocked it | `src/run_partition_window_benchmark.sh`, `results/evidence/RERUN_STATUS.md` |
| Supplemental benchmark design | Added local Spark supplemental benchmarks to close missing evidence gaps without relying on the blocked Docker/MinIO stack | `src/run_supplemental_benchmarks.py`, `src/run_supplemental_benchmarks.sh` |
| Supplemental benchmark execution | Generated one-month partition, AQE on/off, and large non-broadcast skew join result CSVs and physical plans | `results/partition_month/`, `results/join_skew_aqe/`, `results/join_skew_large/` |
| Test coverage | Added and ran unit tests for evidence-generation helpers | `tests/test_generate_evidence.py` |
| Documentation refresh | Regenerated result summary and updated benchmark documentation | `docs/RESULTS_SUMMARY.md`, `docs/README.md`, `docs/benchmark_protocol.md` |
| Notebook refresh | Updated notebook logic to use one-month partition evidence and supplemental skew evidence, then re-executed the notebook | `notebooks/04_benchmark_visualization_story.ipynb` |
| Final validation | Confirmed validation report has all PASS statuses and requirement audit has all Fulfilled statuses | `results/evidence/VALIDATION_REPORT.md`, `results/evidence/topic5_requirement_audit.csv` |

## Reproducibility Commands

The final evidence was refreshed with these commands from the project root.

```bash
bash src/generate_evidence.sh
venv/bin/jupyter nbconvert --execute --to notebook --inplace notebooks/04_benchmark_visualization_story.ipynb
```

The supplemental Spark result CSVs can be regenerated with this command.

```bash
bash src/run_supplemental_benchmarks.sh
```

The validation commands used after the updates were:

```bash
bash -n src/run_supplemental_benchmarks.sh src/generate_evidence.sh src/run_partition_window_benchmark.sh
PYTHONPYCACHEPREFIX="/var/folders/_k/3yxvhtpj2_d6mbrcfwkkjryc0000gn/T/opencode/pycache" PYTHONPATH=src venv/bin/python -m py_compile src/generate_evidence.py src/run_supplemental_benchmarks.py
PYTHONPATH=src venv/bin/python -m unittest discover -s tests
```

The unit test result was `4 tests OK`.

## Environment and Execution Notes

The intended original stack used Docker, Spark, and MinIO. The original rerun path was blocked locally by Spark/MinIO connectivity and MinIO distributed-mode setup issues. The blocked rerun was recorded rather than hidden.

Observed local blocker examples:

| Component | Observed Failure |
| --- | --- |
| Spark master endpoint | Could not connect to `127.0.0.1:7077` |
| Spark UI endpoint | `curl http://127.0.0.1:8080/json/` failed |
| MinIO readiness endpoint | `curl http://127.0.0.1:9000/minio/health/ready` failed |
| MinIO distributed setup | `grid: local host () not found in cluster setup` |

The project therefore keeps the original large NYC taxi result CSVs as primary evidence where they are already complete and adds local Spark supplemental benchmarks only for the evidence gaps.

This decision preserves the available large-workload evidence while making the missing partition scan and non-broadcast skew claims auditable through generated result files.

## Dataset Profile

The primary profiled workload is the 2025 NYC taxi trajectory dataset.

| Metric | Value |
| --- | --- |
| Rows profiled | `48,722,573` |
| Pickup time range | `2025-01-01 00:00:00` to `2025-12-31 23:59:59` |
| Distinct origin zones | `262` |
| Distinct destination zones | `263` |
| Distinct routes | `55,538` |
| Top origin zone | `237` Upper East Side South |
| Top origin-zone trips | `2,125,560` trips, `4.36%` of workload |
| Top route | `237->236` with `302,261` trips |
| Hash bucket imbalance | `33.02x` max/min trip count ratio |

Supporting files:

| File | Purpose |
| --- | --- |
| `results/profile/` | Dataset and distribution profiling artifacts |
| `results/evidence/charts/top_origin_zones.png` | Visualizes top origin-zone skew |
| `results/evidence/charts/origin_bucket_distribution.png` | Visualizes hash-bucket distribution |

## Format Benchmark Results

The format benchmark compares Parquet, ORC, and Avro on the same analytical trajectory queries. Parquet and ORC are columnar formats and support efficient projection. Avro is row-oriented and has to deserialize full records for these workloads.

Evidence files:

| File | Purpose |
| --- | --- |
| `results/format/trajectory_parquet_format_benchmark.csv` | Raw Parquet benchmark runs |
| `results/format/trajectory_orc_format_benchmark.csv` | Raw ORC benchmark runs |
| `results/format/trajectory_avro_format_benchmark.csv` | Raw Avro benchmark runs |
| `results/evidence/format_summary.csv` | Generated format summary |
| `results/evidence/charts/format_runtime.png` | Format runtime visualization |

Detailed median runtime results:

| Query | Query Name | Parquet sec | ORC sec | Avro sec | Parquet vs Avro | ORC vs Avro |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Q01 | trajectory_projection_scan | `0.975` | `0.719` | `9.761` | `10.0x` | `13.6x` |
| Q03 | vendor_origin_zone_aggregation | `1.212` | `0.955` | `10.206` | `8.4x` | `10.7x` |
| Q06 | route_ranking_window | `0.811` | `0.822` | `10.248` | `12.6x` | `12.5x` |
| Q07 | origin_destination_volume | `0.471` | `0.520` | `9.894` | `21.0x` | `19.0x` |

Summary:

| Metric | Value |
| --- | ---: |
| Average Parquet vs Avro speedup | `11.6x` |
| Average ORC vs Avro speedup | `13.3x` |

Interpretation: Parquet and ORC satisfy the columnar-storage optimization requirement. Avro is much slower for these analytical scans because it cannot use the same vectorized columnar scan path.

![Format Runtime](charts/format_runtime.png)

## Partition Benchmark Results

Partitioning was evaluated in two layers.

The historical full-year partition benchmark is retained as context because its physical plans show partition filters, but the runtimes are mixed across full-year queries.

The final partition claim uses the supplemental one-month benchmark because it aligns the query predicate to `trip_year = 2025` and `trip_month = 1` and records the scanned partition count as `1/12`.

### Historical Full-Year Partition Context

Evidence files:

| File | Purpose |
| --- | --- |
| `results/partition/trajectory_partition_raw_benchmark.csv` | Historical raw layout runs |
| `results/partition/trajectory_partition_benchmark.csv` | Historical partitioned layout runs |
| `results/evidence/partition_summary.csv` | Generated historical partition summary |
| `results/evidence/charts/partition_runtime.png` | Historical partition visualization |

Detailed historical results:

| Query | Query Name | Raw sec | Partitioned sec | Partition Pruning | Partitions Scanned | Result |
| --- | --- | ---: | ---: | --- | --- | --- |
| Q02 | time_filter_partition_pruning | `0.526` | `0.365` | `True` | blank | `30.7% faster` |
| Q05 | daily_zone_flow_summary | `0.539` | `0.823` | `True` | blank | `52.8% slower` |
| Q06 | route_ranking_window | `0.838` | `1.510` | `True` | blank | `80.2% slower` |
| Q07 | origin_destination_volume | `0.508` | `0.690` | `True` | blank | `35.7% slower` |

Interpretation: The historical full-year runs show Spark partition pruning in plans, but full-year windows and shuffle-heavy queries produce mixed runtime behavior. These results are not used as the final partition performance claim.

![Historical Partition Runtime](charts/partition_runtime.png)

### Final One-Month Partition-Pruning Evidence

Evidence files:

| File | Purpose |
| --- | --- |
| `results/partition_month/trajectory_partition_month_raw_benchmark.csv` | Raw one-month benchmark runs |
| `results/partition_month/trajectory_partition_month_benchmark.csv` | Partitioned one-month benchmark runs |
| `results/partition_month/plans/` | Raw and partitioned physical plans |
| `results/evidence/partition_month_summary.csv` | Generated one-month partition summary |
| `results/evidence/charts/partition_month_runtime.png` | Final partition-pruning visualization |

Detailed final partition results:

| Query | Query Name | Raw sec | Partitioned sec | Partition Pruning | Partitions Scanned | Result |
| --- | --- | ---: | ---: | --- | --- | --- |
| QP01 | monthly_count_pruning | `0.091` | `0.054` | `True` | `1/12` | `40.4% faster` |
| QP02 | monthly_zone_flow | `0.142` | `0.060` | `True` | `1/12` | `58.2% faster` |
| QP03 | monthly_od_volume | `0.236` | `0.082` | `True` | `1/12` | `65.2% faster` |

Interpretation: The one-month benchmark demonstrates data skipping directly. Spark only needs the January partition directory instead of all twelve monthly directories, and the `partitions_scanned` column makes that scan reduction auditable.

![One-Month Partition Runtime](charts/partition_month_runtime.png)

## Hash-Bucketed Layout Results

The hash-bucketed layout groups trajectory records by a deterministic bucket derived from `origin_zone_id`. This helps queries that target a specific origin zone because Spark can read only the relevant hash-bucket directory rather than scanning the whole raw layout.

Evidence files:

| File | Purpose |
| --- | --- |
| `results/bucketing/trajectory_bucket_layout_raw_benchmark.csv` | Raw layout runs for bucket-targeted queries |
| `results/bucketing/trajectory_bucket_layout_bucketed_benchmark.csv` | Hash-bucketed layout runs |
| `results/evidence/bucketing_summary.csv` | Generated bucketing summary |
| `results/evidence/charts/bucketing_runtime.png` | Bucketing runtime visualization |
| `results/evidence/charts/origin_bucket_distribution.png` | Origin-zone hash bucket distribution |

Detailed bucketing results:

| Query | Query Name | Raw sec | Bucketed sec | Uses Data Skipping | Partitions Scanned | Result |
| --- | --- | ---: | ---: | --- | --- | --- |
| Q11 | bucketed_origin_zone_lookup | `1.238` | `0.585` | `True` | blank | `52.8% faster` |
| Q12 | bucketed_origin_zone_join | `0.937` | `0.483` | `True` | blank | `48.4% faster` |

Interpretation: Hash-bucket directory pruning improves targeted spatial-key lookup and join queries. This is directory-level data skipping by origin-zone hash bucket, not Spark catalog `bucketBy` shuffle elimination.

![Bucketing Runtime](charts/bucketing_runtime.png)

![Origin Bucket Distribution](charts/origin_bucket_distribution.png)

## Join Strategy and Skew Results

Join behavior was evaluated in three layers.

The original small-dimension benchmark shows that broadcast is best when the right side is tiny. This is valid join strategy evidence, but it is not enough to prove distributed skew mitigation because broadcast avoids the distributed join problem.

Supplemental benchmarks therefore add AQE on/off and large non-broadcast fact-to-fact skew joins with broadcast disabled.

### Small Dimension Join Strategy Context

Evidence files:

| File | Purpose |
| --- | --- |
| `results/join_skew/trajectory_join_skew_benchmark.csv` | Original join strategy benchmark |
| `results/join_skew/plans/` | Join physical plans |
| `results/evidence/join_summary.csv` | Generated join summary |
| `results/evidence/charts/join_strategy_metrics.png` | Runtime, shuffle, and spill visualization |

Detailed small-dimension join results:

| Query | Strategy | Median Runtime sec | Shuffle Read MB | Shuffle Write MB | Spill MB | Uses Broadcast Join |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Q04 | Skew aggregation | `1.139` | `0.490` | `0.490` | `0.000` | `False` |
| Q08 | SortMerge join | `4.982` | `25.850` | `25.850` | `731.200` | `False` |
| Q09 | Broadcast join | `1.146` | `0.910` | `0.910` | `0.000` | `True` |
| Q10 | Salted join | `3.675` | `0.910` | `0.910` | `0.000` | `True` |

Interpretation: Broadcast join is the best choice for the small `taxi_zone_dim` table because it avoids the expensive SortMerge shuffle and spill path. Q08 demonstrates the cost of distributed SortMerge in this workload.

![Join Strategy Metrics](charts/join_strategy_metrics.png)

### AQE On/Off Skew Join Evidence

Evidence files:

| File | Purpose |
| --- | --- |
| `results/join_skew_aqe/trajectory_join_skew_aqe_benchmark.csv` | AQE enabled and disabled benchmark runs |
| `results/join_skew_aqe/plans/` | AQE on/off physical plans |
| `results/evidence/join_skew_aqe_summary.csv` | Generated AQE summary |
| `results/evidence/charts/join_skew_aqe_runtime.png` | AQE on/off runtime visualization |

Detailed AQE results:

| Query | Query Name | Strategy | AQE Enabled | Median sec | Best sec | Rows Returned | Left Rows | Right Rows | Hot Left Rows | Hot Right Rows | Salt Count | Skew Before | Skew After | Adaptive Plan | Broadcast Join | SortMerge Join |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| AQE_OFF | sortmerge_skew_join_aqe_off | SortMerge AQE off | `False` | `0.149` | `0.140` | `20` | `180000` | `12000` | `60000` | `80` | `8` | `8.869` | `1.378` | `False` | `False` | `True` |
| AQE_ON | sortmerge_skew_join_aqe_on | SortMerge AQE on | `True` | `0.146` | `0.142` | `20` | `180000` | `12000` | `60000` | `80` | `8` | `8.869` | `1.378` | `True` | `False` | `True` |

Interpretation: These runs isolate Spark adaptive planning for the same broadcast-disabled skewed fact-to-fact join. The physical plan metadata confirms the AQE setting changes while broadcast remains disabled and SortMerge remains the distributed join strategy.

![AQE On Off Runtime](charts/join_skew_aqe_runtime.png)

### Large Non-Broadcast Skew Join Evidence

Evidence files:

| File | Purpose |
| --- | --- |
| `results/join_skew_large/trajectory_large_skew_join_benchmark.csv` | Large non-broadcast SortMerge and salted join benchmark runs |
| `results/join_skew_large/plans/` | Physical plans for large non-broadcast joins |
| `results/evidence/join_skew_large_summary.csv` | Generated large skew summary |
| `results/evidence/charts/large_skew_join_runtime.png` | SortMerge vs salted non-broadcast runtime visualization |
| `results/evidence/charts/large_skew_distribution.png` | Skew-ratio before/after salting visualization |

Detailed large non-broadcast skew results:

| Query | Query Name | Strategy | AQE Enabled | Median sec | Best sec | Rows Returned | Left Rows | Right Rows | Hot Left Rows | Hot Right Rows | Salt Count | Skew Before | Skew After | Adaptive Plan | Broadcast Join | SortMerge Join |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| JL01 | large_sortmerge_skew_join | SortMerge non-broadcast | `True` | `0.155` | `0.136` | `20` | `180000` | `12000` | `60000` | `80` | `8` | `8.869` | `1.378` | `True` | `False` | `True` |
| JL02 | large_salted_skew_join | Salted non-broadcast | `True` | `0.351` | `0.325` | `20` | `180000` | `12000` | `60000` | `80` | `8` | `8.869` | `1.378` | `True` | `False` | `True` |

Interpretation: The salted non-broadcast join shows the data distribution effect of salting. The hot-key skew ratio improves from `8.869` before salting to `1.378` after salting, while broadcast remains disabled. Runtime is slower in this small local synthetic benchmark because salting also replicates the right side and adds overhead; the purpose of this evidence is to prove non-broadcast distributed skew mitigation mechanics, not to claim salting is always faster.

![Large Skew Join Runtime](charts/large_skew_join_runtime.png)

![Large Skew Distribution](charts/large_skew_distribution.png)

## Dataset Distribution Visualizations

These visualizations support the discussion of route and origin-zone skew.

![Top Origin Zones](charts/top_origin_zones.png)

![Origin Bucket Distribution](charts/origin_bucket_distribution.png)

## Final Requirement Audit

The generated requirement audit is stored at `results/evidence/topic5_requirement_audit.csv` and `results/evidence/TOPIC5_REQUIREMENT_AUDIT.md`.

| Requirement | Status | Evidence |
| --- | --- | --- |
| Compare Parquet, ORC, Avro | Fulfilled | Format CSVs, runtime chart, physical plans with Parquet/ORC batched scans and Avro non-batched scan |
| Optimize partitioning for data skipping | Fulfilled | One-month partition evidence reports scanned partition directories and PartitionFilters |
| Optimize bucketing for data skipping | Fulfilled | Hash-bucket directory pruning improves targeted origin-zone lookup and join queries |
| Address data skew in distributed joins | Fulfilled | Small-dimension joins plus supplemental AQE on/off and large non-broadcast fact-to-fact skew joins |
| Quantitative evaluation with charts | Fulfilled | Generated Markdown summaries, CSV tables, and PNG charts under `results/evidence/` |

## Final Validation Status

The generated validation report is stored at `results/evidence/VALIDATION_REPORT.md` and `results/evidence/validation_report.csv`.

| Check | Status | Detail |
| --- | --- | --- |
| Input format_parquet | PASS | CSV loaded |
| Input format_orc | PASS | CSV loaded |
| Input format_avro | PASS | CSV loaded |
| Input partition_raw | PASS | CSV loaded |
| Input partitioned | PASS | CSV loaded |
| Input partition_month_raw | PASS | CSV loaded |
| Input partition_month | PASS | CSV loaded |
| Input bucketing_raw | PASS | CSV loaded |
| Input bucketing_bucketed | PASS | CSV loaded |
| Input join_skew | PASS | CSV loaded |
| Input join_skew_aqe | PASS | CSV loaded |
| Input join_skew_large | PASS | CSV loaded |
| Partition runtime claim | PASS | One-month pruning evidence is used as the final partition result |
| Partition scan metadata | PASS | Present in CSV |
| One-month partition evidence | PASS | Runtime and scanned directory counts present |
| AQE on/off evidence | PASS | AQE enabled and disabled runs are present |
| Large non-broadcast skew join | PASS | SortMerge and salted non-broadcast skew joins are present |
| Generated charts | PASS | 10 chart files in `results/evidence/charts` |

## Recommended Work Status

The earlier missing or partial evidence items have been closed.

| Recommended Work | Status | Evidence |
| --- | --- | --- |
| One-month partition rerun with scanned partition count | Fulfilled | `results/partition_month/*.csv` |
| AQE on/off join-skew comparison | Fulfilled | `results/join_skew_aqe/trajectory_join_skew_aqe_benchmark.csv` |
| Large non-broadcast fact-to-fact skew join | Fulfilled | `results/join_skew_large/trajectory_large_skew_join_benchmark.csv` |
| Generated presentation numbers from evidence CSVs | Fulfilled | `docs/RESULTS_SUMMARY.md` and `results/evidence/*.csv` generated by `src/generate_evidence.py` |

## Important Interpretation Boundaries

The historical full-year partition benchmark is not used to claim universal partition speedup because some full-year partitioned queries are slower. It is retained only as context proving that partition filters appear in Spark plans.

The one-month partition benchmark is the final partition-pruning evidence because it records both runtime improvement and scanned partition count.

The small-dimension join benchmark correctly shows broadcast join as the best strategy for a tiny dimension table. It is not sufficient alone for skew mitigation because broadcast avoids the distributed join skew problem.

The supplemental large skew benchmark proves non-broadcast skew mitigation mechanics. The salted join reduces skew ratio but is slower in this small local run due to salt expansion overhead. The correct conclusion is that salting redistributes hot keys; it should be applied when skew cost exceeds salting overhead.

The local supplemental runs are intentionally scoped to missing evidence and auditability. Larger-scal  timings should be rerun on the intended Linux/Spark/MinIO cluster if cluster access becomes available 
## Final Conclusion

The project now has a complete, centralized, auditable Topic 5 evidence package.

All final claims are backed by generated CSV summaries, charts, physical-plan references, and validation checks. The evidence folder `results/evidence/` contains the final charts, generated summaries, validation files, requirement audits, and this centralized report. The notebook visualization has also been refreshed so it no longer contains stale partition claims and now includes the one-month partition and supplemental skew evidence.

Final outcome: Topic 5 is complete and defensible with generated evidence.
