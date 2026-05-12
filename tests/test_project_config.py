from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from project_config import load_env_file


class ProjectConfigEnvTests(unittest.TestCase):
    def test_load_env_file_exports_values_without_overriding_existing_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            env_path = Path(tmp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "NODE1_IP=10.0.0.1",
                        "SPARK_MASTER=spark://spark-master.test:7077",
                        "MINIO_ENDPOINT=http://minio.test:9000",
                        "EXISTING_VALUE=from-file",
                        "QUOTED_VALUE=\"quoted text\"",
                        "EMPTY_VALUE=",
                        "# ignored comment",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"EXISTING_VALUE": "from-env"}, clear=True):
                loaded = load_env_file(env_path)

                self.assertTrue(loaded)
                self.assertEqual(os.environ["NODE1_IP"], "10.0.0.1")
                self.assertEqual(os.environ["SPARK_MASTER"], "spark://spark-master.test:7077")
                self.assertEqual(os.environ["MINIO_ENDPOINT"], "http://minio.test:9000")
                self.assertEqual(os.environ["EXISTING_VALUE"], "from-env")
                self.assertEqual(os.environ["QUOTED_VALUE"], "quoted text")
                self.assertEqual(os.environ["EMPTY_VALUE"], "")

    def test_load_env_file_returns_false_when_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict(os.environ, {}, clear=True):
                self.assertFalse(load_env_file(Path(tmp_dir) / ".env"))


if __name__ == "__main__":
    unittest.main()
