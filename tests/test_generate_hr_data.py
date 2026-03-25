from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

from salary_schema import get_schema


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT_DIR / "demo" / "generate_hr_data.py"
FULL_SCHEMA = get_schema("pingan_full")


def test_generate_hr_data_small_sample(tmp_path):
    output_path = tmp_path / "hr_data_small.csv"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--employees",
            "120",
            "--start",
            "2024-12",
            "--end",
            "2025-02",
            "--seed",
            "20260322",
            "--output",
            str(output_path),
        ],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "总行数=360" in result.stdout.replace(",", "")
    assert output_path.exists()

    with output_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == FULL_SCHEMA.wide_columns
        rows = list(reader)

    assert len(rows) == 360
    assert len({row["员工ID"] for row in rows}) == 120
    assert len({(row["统计年度"], row["统计月份"]) for row in rows}) == 3
    assert {row["级别"] for row in rows}.issubset({"O", "A", "B", "C"})
    assert {row["职能序列"] for row in rows}.issubset({"M序列", "P序列", "T序列"})
    assert all(row["员工UM"] for row in rows)
    assert all(row["统计月"] for row in rows)
    assert all(row["部门"] for row in rows)
    assert all(row["年龄"] for row in rows)
    assert all(row["司龄"] for row in rows)


def test_generate_hr_data_business_consistency(tmp_path):
    output_path = tmp_path / "hr_data_consistency.csv"
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--employees",
            "300",
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

    with output_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    level_salary: dict[str, list[float]] = {"O": [], "A": [], "B": [], "C": []}
    for row in rows:
        level_salary[row["级别"]].append(float(row["底薪/基本工资"] or 0))

    avg_c = sum(level_salary["C"]) / max(len(level_salary["C"]), 1)
    avg_o = sum(level_salary["O"]) / max(len(level_salary["O"]), 1)
    assert avg_o > avg_c

    regular = [float(row["养老保险扣款"] or 0) for row in rows if row["养老保险扣款"]]
    backfill = [float(row["养老保险扣款补缴"] or 0) for row in rows if row["养老保险扣款补缴"]]
    assert regular
    assert len(backfill) < len(regular)

    december_bonuses = [float(row["年终奖"] or 0) for row in rows if row["统计月份"] == "12" and row["年终奖"]]
    january_bonuses = [float(row["年终奖"] or 0) for row in rows if row["统计月份"] == "1" and row["年终奖"]]
    assert len(december_bonuses) >= len(january_bonuses)
