from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from salary_schema import get_schema


ROOT_DIR = Path(__file__).resolve().parents[1]
FULL_GENERATOR = ROOT_DIR / "demo" / "generate_hr_data.py"


def test_no_active_data_source_blocks_analysis(tmp_path, monkeypatch):
    import salary_reporting

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "no_source.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    salary_reporting.init_database()

    meta = salary_reporting.metadata()
    assert meta["row_count"] == 0
    assert meta["period_start"] == ""
    assert meta["period_end"] == ""
    assert meta["data_source"]["ready"] is False

    with pytest.raises(ValueError, match="请先导入兼容当前宽表结构的真实 CSV 数据"):
        salary_reporting.ensure_data_source_ready()


def test_upload_activates_dataset(tmp_path, monkeypatch):
    import app as app_module
    import salary_reporting

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "upload_mode.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)
    salary_reporting.init_database()

    csv_bytes = (ROOT_DIR / "test_薪酬数据_宽表.csv").read_bytes()

    with TestClient(app_module.app) as client:
        response = client.post("/api/upload", files={"file": ("real_dataset.csv", csv_bytes, "text/csv")})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["filename"] == "real_dataset.csv"
    assert payload["data_source"]["ready"] is True
    assert payload["data_source"]["filename"].endswith("real_dataset.csv")
    assert payload["row_count"] > 0

    meta = salary_reporting.metadata()
    assert meta["data_source"]["ready"] is True
    assert meta["row_count"] == payload["row_count"]
    assert meta["period_start"] != ""
    assert meta["period_end"] != ""


def test_upload_validation_failure_does_not_activate_dataset(tmp_path, monkeypatch):
    import app as app_module
    import salary_reporting

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "invalid_upload.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)
    salary_reporting.init_database()

    invalid_csv = "foo,bar\n1,2\n".encode("utf-8")

    with TestClient(app_module.app) as client:
        response = client.post("/api/upload", files={"file": ("invalid.csv", invalid_csv, "text/csv")})

    assert response.status_code == 400
    assert "字段" in response.json()["detail"]

    meta = salary_reporting.metadata()
    assert meta["data_source"]["ready"] is False
    assert meta["row_count"] == 0


def test_upload_full_schema_dataset(tmp_path, monkeypatch):
    import app as app_module
    import salary_reporting

    output_path = tmp_path / "pingan_full_small.csv"
    subprocess.run(
        [
            sys.executable,
            str(FULL_GENERATOR),
            "--employees",
            "60",
            "--start",
            "2024-12",
            "--end",
            "2025-01",
            "--seed",
            "20260323",
            "--output",
            str(output_path),
        ],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "full_upload.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)
    try:
        salary_reporting.init_database()

        with TestClient(app_module.app) as client:
            response = client.post("/api/upload", files={"file": ("pingan_full_small.csv", output_path.read_bytes(), "text/csv")})

        assert response.status_code == 200
        payload = response.json()
        assert payload["data_source"]["ready"] is True
        assert payload["data_source"]["schema_id"] == "pingan_full"

        meta = salary_reporting.metadata()
        assert "底薪/基本工资" in meta["subjects"]
        assert "住房公积金(公司)" in meta["subjects"]
        assert meta["row_count"] == 120
    finally:
        salary_reporting.configure_schema(get_schema("legacy_simple"))


def test_activate_local_data_source_endpoint(tmp_path, monkeypatch):
    import app as app_module
    import salary_reporting

    output_path = tmp_path / "pingan_full_small.csv"
    subprocess.run(
        [
            sys.executable,
            str(FULL_GENERATOR),
            "--employees",
            "40",
            "--start",
            "2024-12",
            "--end",
            "2025-01",
            "--seed",
            "20260323",
            "--output",
            str(output_path),
        ],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "local_activate.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)

    try:
        salary_reporting.init_database()
        with TestClient(app_module.app) as client:
            response = client.post("/api/data-source/activate-local", json={"path": str(output_path)})
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["data_source"]["schema_id"] == "pingan_full"
        assert payload["data_source"]["row_count"] == 80
    finally:
        salary_reporting.configure_schema(get_schema("legacy_simple"))


def test_upload_unmatched_headers_returns_inference_draft(tmp_path, monkeypatch):
    import app as app_module
    import salary_reporting

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "infer_upload.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)
    salary_reporting.init_database()

    csv_bytes = "\n".join(
        [
            "年月,工号,组织单元,补偿金",
            "2024-12,E001,华东区,12000",
            "2025-01,E001,华东区,15000",
        ]
    ).encode("utf-8")

    with TestClient(app_module.app) as client:
        response = client.post("/api/upload", files={"file": ("hetero.csv", csv_bytes, "text/csv")})

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "inference_required"
    assert payload["draft"]["subject_columns"] == ["经济补偿金"]
    assert "BU" in payload["draft"]["dimension_columns"]
    assert payload["draft"]["period"]["period_column"] == "年月"


def test_activate_inferred_dataset_from_draft(tmp_path, monkeypatch):
    import app as app_module
    import salary_reporting

    monkeypatch.setattr(salary_reporting, "DB_PATH", tmp_path / "infer_activate.db")
    monkeypatch.setattr(salary_reporting, "UPLOADS_DIR", tmp_path / "uploads")
    salary_reporting.UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)
    salary_reporting.init_database()

    csv_path = tmp_path / "hetero.csv"
    csv_path.write_text(
        "\n".join(
            [
                "年月,工号,组织单元,职级,补偿金",
                "2024-12,E001,华东区,B1,12000",
                "2025-01,E001,华东区,B1,15000",
                "2024-12,E002,华南区,B2,9000",
            ]
        ),
        encoding="utf-8",
    )

    draft = salary_reporting.infer_schema_draft(csv_path)
    assert draft["mode"] == "inference_required"

    with TestClient(app_module.app) as client:
        response = client.post(
            "/api/data-source/activate-inferred",
            json={"path": str(csv_path), "manifest": draft["draft"]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data_source"]["ready"] is True
    assert payload["data_source"]["schema_mode"] == "inferred"

    meta = salary_reporting.metadata()
    assert meta["schema_mode"] == "inferred"
    assert meta["subjects"] == ["经济补偿金"]
    assert meta["capabilities"]["supports_trend_analysis"] is True
