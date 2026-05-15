# Topic 5 Terminal Demo Guide

Demo target: maximum 3 minutes.

Demo style: terminal-first. The screen should mostly show scripts running, Spark stages, terminal output, PASS checks, and final benchmark numbers. Do not spend the demo opening many images or scrolling reports.

## One Command

Run from the project root:

```bash
bash src/demo_topic5.sh --presentation-mode
```

Fast rehearsal without pauses:

```bash
bash src/demo_topic5.sh
```

Debug rehearsal with pauses disabled but presentation-mode logic enabled:

```bash
bash src/demo_topic5.sh --presentation-mode --pause-scale 0
```

## What The Command Does

The command is designed to demonstrate a real running process in terminal.

| Step | What Runs | What It Proves |
| --- | --- | --- |
| 0 | `generate_evidence.py` refreshes `results/evidence/` | Results are generated from CSV artifacts, not hand-written |
| 1 | Starts local Spark and creates a compact trajectory workload | The project can run Spark SQL code live |
| 2 | Writes Parquet, ORC, year/month partitioned, and origin-zone bucketed layouts | Shows the same storage/layout ideas as the full project |
| 3 | Runs Parquet and ORC projection/aggregation queries | Demonstrates live columnar reads |
| 4 | Runs raw vs partitioned month queries and checks `PartitionFilters` | Demonstrates partition pruning/data skipping |
| 5 | Runs raw vs bucketed origin-zone queries and checks `PartitionFilters` | Demonstrates bucket directory pruning/data skipping |
| 6 | Runs non-broadcast SortMerge and salted joins | Demonstrates distributed join and skew-handling mechanics |
| 7 | Prints official generated evidence from the full benchmark | Gives final project numbers for format, partition, bucketing, join, skew, audit, validation |

The live Spark workload is intentionally compact so it fits in the presentation. The final measured project results are printed from `results/evidence/*.csv`.

## Exact 3-Minute Demo Flow

### 0:00-0:15: Start Command

Screen: terminal at repository root.

Run:

```bash
bash src/demo_topic5.sh --presentation-mode
```

Say:

> This is our product demo for Topic 5: Distributed Query Optimization on Columnar Storage. I will run one script that refreshes generated evidence, starts Spark locally, runs compact optimization queries, checks physical plans, and prints the official benchmark results.

### 0:15-0:40: Evidence Refresh + Spark Setup

Terminal will show:

```text
[demo] Refreshing generated evidence from benchmark CSV files...
Evidence written to: .../results/evidence
Charts written to: .../results/evidence/charts
1/5 LIVE SPARK SETUP: BUILD MINI TRAJECTORY WORKLOAD
Creating local Spark session and 240,000 synthetic trajectory rows...
```

Say:

> First, the script regenerates the evidence folder from benchmark CSVs. Then it starts Spark and builds a compact trajectory workload with the same fields we use in the project: year, month, origin zone, destination zone, route key, bucket key, distance, and amount.

### 0:40-1:05: Layout Creation

Terminal will show Spark stages and then:

```text
Wrote Parquet, ORC, year/month partitions, and origin-zone bucket folders
```

Say:

> The script writes the same layout families required by Topic 5: Parquet, ORC, time partitioning by year and month, and hash-bucket directory layout by origin zone. This proves the implementation is not only a static report; it runs the storage-layout preparation logic live.

### 1:05-1:35: Format + Data Skipping Queries

Terminal will show:

```text
2/5 LIVE FORMAT CHECK: COLUMNAR READS
running Parquet projection/aggregation ...
running ORC projection/aggregation ...
3/5 LIVE DATA SKIPPING: PARTITIONING AND BUCKETING
running raw month query ...
running partitioned month query ...
physical plan PartitionFilters found: True
running raw origin-zone query ...
running bucketed origin-zone query ...
bucket plan PartitionFilters found: True
```

Say:

> Now Spark executes actual SQL. The format step reads columnar Parquet and ORC. The partition step compares raw and partitioned month queries and confirms `PartitionFilters` in the physical plan. The bucket step filters by origin-zone bucket and also confirms directory pruning in the plan.

### 1:35-2:15: Join/Skew Demo

Terminal will show:

```text
4/5 LIVE JOIN/SKEW CHECK: NON-BROADCAST SORTMERGE AND SALTING
measured hot-key skew ratio before/after salting: ... -> ...
running non-broadcast SortMerge join ...
running salted non-broadcast join ...
SortMergeJoin found: True
BroadcastHashJoin absent: True
AdaptiveSparkPlan found: True
```

Say:

> This part demonstrates the join/skew section. Broadcast is disabled, so the plan must use a distributed SortMerge join. The script creates a hot key, measures skew before and after salting, runs both joins, and checks that the salted join is still non-broadcast, uses SortMerge, and is under AdaptiveSparkPlan.

### 2:15-2:50: Official Full Benchmark Results

Terminal will show:

```text
5/5 OFFICIAL GENERATED EVIDENCE: FULL PROJECT RESULTS
FORMAT      Parquet 11.6x faster than Avro; ORC 13.3x faster than Avro
PARTITION   one-month pruning scans 1/12 partitions; 40.4-65.2% faster
BUCKETING   origin-zone hash layout is 48.4-52.8% faster
JOIN        broadcast is 4.3x faster than SortMerge; spill 731.2 MB -> 0.0 MB
SKEW        non-broadcast salting reduces skew ratio 8.869 -> 1.378
AUDIT       Fulfilled
VALIDATION  PASS
CHARTS      10 PNG files in results/evidence/charts
```

Say:

> These are the official generated results from our benchmark evidence. They cover the full project scope: format comparison, partition pruning, bucketing data skipping, join strategy, skew handling, quantitative evaluation, charts, and validation.

### 2:50-3:00: Close

Terminal will show:

```text
Demo completed in ...s
```

Say:

> Final status: the benchmark evidence is complete. The evidence is centralized in `results/evidence`, and the demo proves both implementation and evaluation through running code and generated terminal output.

## Why This Demo Works For The Presentation

| Rubric Item | Demo Evidence |
| --- | --- |
| System Analysis and Design | Shows Spark SQL + object-storage style layouts and workload-aware optimization choices |
| Technical Implementation | Runs Python, Spark SQL, generated evidence code, and physical-plan checks in terminal |
| Optimization and Research Depth | Demonstrates format, partitioning, bucketing, join strategy, AQE, and salting |
| Evaluation and Testing | Prints quantitative official results, PASS validation, Fulfilled audit, and chart count |
| Report and Presentation | One command is stable enough for live or pre-recorded delivery |

## Backup Commands For Q&A

Regenerate only evidence:

```bash
bash src/generate_evidence.sh
```

Run supplemental Spark evidence again:

```bash
bash src/run_supplemental_benchmarks.sh
bash src/generate_evidence.sh
```

Run the full cluster benchmark on a healthy Spark/MinIO environment:

```bash
bash src/prepare_benchmark_datasets.sh
bash src/run_benchmark.sh
```

Open centralized evidence after the demo if the examiner asks:

```bash
open results/evidence/TOPIC5_COMPLETE_REPORT.md
open results/evidence/charts
```

## Important Speaking Notes

The compact live Spark run is for demonstration and physical-plan proof. The official final numbers come from the generated evidence CSVs under `results/evidence/`, which are based on the project benchmark outputs.

If asked why not run the full Spark/MinIO benchmark live, answer:

> The full benchmark is the real pipeline but it is too long and environment-sensitive for a 3-minute presentation. This terminal demo runs a compact Spark workload live and then prints the official generated benchmark evidence, so it is both reliable and faithful to the project.
