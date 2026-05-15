from __future__ import annotations

import unittest

import pandas as pd

from generate_evidence import build_layout_comparison, delta_label, percent_faster, speedup_ratio


class EvidenceGenerationTests(unittest.TestCase):
    def test_percent_faster_and_speedup_ratio(self) -> None:
        self.assertAlmostEqual(percent_faster(10.0, 7.5), 25.0)
        self.assertAlmostEqual(percent_faster(10.0, 12.5), -25.0)
        self.assertAlmostEqual(speedup_ratio(10.0, 2.0), 5.0)
        self.assertEqual(delta_label(10.0, 7.5), "25.0% faster")
        self.assertEqual(delta_label(10.0, 12.5), "25.0% slower")

    def test_build_layout_comparison_preserves_mixed_runtime_results(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "query_id": "Q01",
                    "query_name": "fast_query",
                    "median_runtime_sec": 10.0,
                    "best_runtime_sec": 9.0,
                    "rows_returned": 1,
                    "stages": 1,
                    "tasks": 1,
                    "shuffle_read_mb": 0.0,
                    "shuffle_write_mb": 0.0,
                    "spill_mb": 0.0,
                    "uses_broadcast_join": False,
                    "uses_partition_pruning": False,
                    "partitions_scanned": "",
                },
                {
                    "query_id": "Q02",
                    "query_name": "slow_query",
                    "median_runtime_sec": 10.0,
                    "best_runtime_sec": 9.0,
                    "rows_returned": 1,
                    "stages": 1,
                    "tasks": 1,
                    "shuffle_read_mb": 0.0,
                    "shuffle_write_mb": 0.0,
                    "spill_mb": 0.0,
                    "uses_broadcast_join": False,
                    "uses_partition_pruning": False,
                    "partitions_scanned": "",
                },
            ]
        )
        optimized = raw.copy()
        optimized.loc[optimized["query_id"] == "Q01", "median_runtime_sec"] = 5.0
        optimized.loc[optimized["query_id"] == "Q02", "median_runtime_sec"] = 20.0
        optimized["uses_partition_pruning"] = True
        optimized["partitions_scanned"] = "1/12"

        result = build_layout_comparison(raw, optimized, "partitioned")

        labels = dict(zip(result["query_id"], result["result"], strict=False))
        self.assertEqual(labels["Q01"], "50.0% faster")
        self.assertEqual(labels["Q02"], "100.0% slower")


if __name__ == "__main__":
    unittest.main()
