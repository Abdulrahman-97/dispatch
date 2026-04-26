from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "python_callable.py"


def load_runner_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("dispatch_python_callable", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load python_callable.py")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PythonCallableTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = load_runner_module()
        self.fake_module = types.ModuleType("fake_dispatch_jobs")
        self.fake_module.success = self.success
        self.fake_module.not_json = self.not_json
        sys.modules["fake_dispatch_jobs"] = self.fake_module

    def tearDown(self) -> None:
        sys.modules.pop("fake_dispatch_jobs", None)

    def test_runs_allowlisted_callable(self) -> None:
        result = self.runner.run_callable(
            {
                "callable": "demo_success",
                "kwargs": {"partition_date": "2026-04-24"},
            },
            allowlist={"demo_success": "fake_dispatch_jobs:success"},
        )

        self.assertEqual(
            result,
            {
                "contract_version": 1,
                "partition_date": "2026-04-24",
                "status": "success",
            },
        )

    def test_rejects_unallowlisted_alias(self) -> None:
        with self.assertRaisesRegex(
            self.runner.CallableRunnerError,
            "callable alias is not allowed",
        ):
            self.runner.run_callable(
                {"callable": "os_system", "kwargs": {}},
                allowlist={"demo_success": "fake_dispatch_jobs:success"},
            )

    def test_rejects_non_object_kwargs(self) -> None:
        with self.assertRaisesRegex(self.runner.CallableRunnerError, "kwargs"):
            self.runner.run_callable(
                {"callable": "demo_success", "kwargs": []},
                allowlist={"demo_success": "fake_dispatch_jobs:success"},
            )

    def test_requires_json_serializable_dict_result(self) -> None:
        with self.assertRaisesRegex(self.runner.CallableRunnerError, "JSON serializable"):
            self.runner.run_callable(
                {"callable": "demo_not_json", "kwargs": {}},
                allowlist={"demo_not_json": "fake_dispatch_jobs:not_json"},
            )

    @staticmethod
    def success(partition_date: str) -> dict[str, Any]:
        return {
            "contract_version": 1,
            "partition_date": partition_date,
            "status": "success",
        }

    @staticmethod
    def not_json() -> dict[str, Any]:
        return {"bad": object()}


if __name__ == "__main__":
    unittest.main()
