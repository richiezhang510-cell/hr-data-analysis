from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture(scope="session", autouse=True)
def activate_test_dataset(tmp_path_factory: pytest.TempPathFactory) -> None:
    import salary_reporting

    runtime_dir = tmp_path_factory.mktemp("salary_runtime")
    salary_reporting.DB_PATH = runtime_dir / "salary_analysis.db"
    salary_reporting.UPLOADS_DIR = runtime_dir / "uploads"
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    salary_reporting.init_database()
    salary_reporting.activate_dataset(ROOT_DIR / "test_multi_bu.csv")
