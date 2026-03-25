#!/usr/bin/env python3
"""Generate a large salary wide-table CSV from the provided sample schema."""

from __future__ import annotations

import argparse
import csv
import math
import random
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from statistics import mean


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = str(SCRIPT_DIR / "薪酬数据_宽表样例.csv")
DEFAULT_OUTPUT = str(SCRIPT_DIR / "generated_salary_wide_legacy.csv")
DEFAULT_EMPLOYEES = 82_000
DEFAULT_SEED = 20260312

BU_CONFIG = [
    {"name": "平安集团", "prefix": "PJT", "weight": 0.08, "salary_factor": 1.00, "bonus_factor": 1.00},
    {"name": "平安科技", "prefix": "PAKJ", "weight": 0.14, "salary_factor": 1.12, "bonus_factor": 1.08},
    {"name": "平安产险", "prefix": "PACX", "weight": 0.15, "salary_factor": 1.03, "bonus_factor": 1.10},
    {"name": "平安寿险", "prefix": "PASX", "weight": 0.19, "salary_factor": 1.05, "bonus_factor": 1.16},
    {"name": "平安健康险", "prefix": "PAJKX", "weight": 0.09, "salary_factor": 0.98, "bonus_factor": 1.04},
    {"name": "平安陆控", "prefix": "PALK", "weight": 0.11, "salary_factor": 1.07, "bonus_factor": 1.06},
    {"name": "平安银行", "prefix": "PAYH", "weight": 0.16, "salary_factor": 1.10, "bonus_factor": 1.12},
    {"name": "平安好医生", "prefix": "PAHYS", "weight": 0.08, "salary_factor": 0.97, "bonus_factor": 1.02},
]

FIELDS = [
    "统计年度",
    "统计月份",
    "BU",
    "员工ID",
    "职能",
    "绩效分位",
    "级别",
    "司龄分箱",
    "年龄分箱",
    "底薪",
    "基本工资调整",
    "内勤绩效",
    "岗位津贴",
    "倒班津贴",
    "特招津贴",
    "加班费",
    "经济补偿金",
    "签约金",
    "降温取暖费",
    "配偶补贴",
    "借调补贴",
]

CATEGORY_FIELDS = ["BU", "职能", "绩效分位", "级别", "司龄分箱", "年龄分箱"]
NUMERIC_FIELDS = [
    "底薪",
    "基本工资调整",
    "内勤绩效",
    "岗位津贴",
    "倒班津贴",
    "特招津贴",
    "加班费",
    "经济补偿金",
    "签约金",
    "降温取暖费",
    "配偶补贴",
    "借调补贴",
]

LEVEL_BASE_SALARY = {
    "CD类员工": (4_800, 7_000),
    "B类": (6_500, 9_500),
    "A类领导": (8_800, 12_500),
    "O类领导": (11_800, 14_800),
}

PERFORMANCE_MULTIPLIER = {
    "前10%": 1.45,
    "前20%": 1.32,
    "前30%": 1.22,
    "前40%": 1.15,
    "前50%": 1.08,
    "前60%": 1.00,
    "前70%": 0.92,
    "后20%": 0.82,
    "后30%": 0.74,
}

TENURE_SALARY_MULTIPLIER = {
    "1年以下": 0.92,
    "1-3": 0.98,
    "3-5": 1.05,
    "5-8": 1.10,
    "8-10": 1.16,
    "10年以上": 1.22,
}

AGE_SALARY_MULTIPLIER = {
    "25以下": 0.95,
    "25-30": 1.00,
    "30-35": 1.05,
    "35-40": 1.10,
    "40-45": 1.14,
    "45+": 1.18,
}

FUNCTION_OVERTIME_MULTIPLIER = {
    "技术": 1.35,
    "运营": 1.25,
    "客服": 1.30,
    "销售": 1.10,
    "产品": 1.08,
    "风控": 1.00,
    "法务": 0.95,
    "财务": 0.92,
    "市场": 0.98,
    "行政": 0.85,
    "人力": 0.88,
}

FUNCTION_POST_MULTIPLIER = {
    "销售": 1.35,
    "技术": 1.20,
    "产品": 1.18,
    "法务": 1.05,
    "风控": 1.05,
    "运营": 1.00,
    "财务": 0.96,
    "市场": 1.10,
    "客服": 1.08,
    "行政": 0.88,
    "人力": 0.92,
}

SHIFT_ELIGIBLE_FUNCTIONS = {"运营", "客服", "技术"}
SPECIAL_HIRE_FUNCTIONS = {"技术", "产品", "销售"}


@dataclass(frozen=True)
class SampleStats:
    fields: list[str]
    sample_rows: list[dict[str, str]]
    weighted_choices: dict[str, tuple[list[str], list[float]]]
    numeric_avg: dict[str, float]
    numeric_min: dict[str, int]
    numeric_max: dict[str, int]


@dataclass(frozen=True)
class EmployeeProfile:
    employee_id: str
    bu: str
    bu_prefix: str
    function: str
    performance: str
    level: str
    tenure: str
    age_band: str
    base_salary: int
    salary_adjustment_base: int
    performance_base: int
    post_allowance_base: int
    overtime_base: int
    compensation_base: int
    spouse_allowance_base: int
    shift_eligible: bool
    special_hire_type: str
    special_hire_base: int
    signing_bonus_month: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a salary wide-table CSV for 2025-01 to 2027-01."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Sample CSV path.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path.")
    parser.add_argument(
        "--employees",
        type=int,
        default=DEFAULT_EMPLOYEES,
        help="Number of employees in the fixed employee pool.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Random seed for reproducible generation.",
    )
    return parser.parse_args()


def read_sample_stats(path: Path) -> SampleStats:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = reader.fieldnames or []

    if fields != FIELDS:
        raise ValueError(f"Unexpected columns in {path}: {fields}")
    if not rows:
        raise ValueError(f"No sample rows found in {path}")

    weighted_choices: dict[str, tuple[list[str], list[float]]] = {}
    for field in CATEGORY_FIELDS:
        counter = Counter(row[field] for row in rows)
        weighted_choices[field] = (list(counter.keys()), list(counter.values()))

    numeric_avg = {
        field: mean(int(row[field]) for row in rows)
        for field in NUMERIC_FIELDS
    }
    numeric_min = {
        field: min(int(row[field]) for row in rows)
        for field in NUMERIC_FIELDS
    }
    numeric_max = {
        field: max(int(row[field]) for row in rows)
        for field in NUMERIC_FIELDS
    }

    return SampleStats(
        fields=fields,
        sample_rows=rows,
        weighted_choices=weighted_choices,
        numeric_avg=numeric_avg,
        numeric_min=numeric_min,
        numeric_max=numeric_max,
    )


def pick_weighted(rng: random.Random, values: list[str], weights: list[float]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def bounded_int(value: float, floor: int = 0) -> int:
    return max(floor, int(round(value)))


def month_sequence(start_year: int, start_month: int, end_year: int, end_month: int) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        month += 1
        if month == 13:
            year += 1
            month = 1
    return months


def build_employee_profiles(
    employees: int, sample_stats: SampleStats, rng: random.Random
) -> list[EmployeeProfile]:
    function_values, function_weights = sample_stats.weighted_choices["职能"]
    perf_values, perf_weights = sample_stats.weighted_choices["绩效分位"]
    level_values, level_weights = sample_stats.weighted_choices["级别"]
    tenure_values, tenure_weights = sample_stats.weighted_choices["司龄分箱"]
    age_values, age_weights = sample_stats.weighted_choices["年龄分箱"]

    sample_adjustment_avg = sample_stats.numeric_avg["基本工资调整"]
    sample_post_avg = sample_stats.numeric_avg["岗位津贴"]
    sample_overtime_avg = sample_stats.numeric_avg["加班费"]
    sample_spouse_avg = sample_stats.numeric_avg["配偶补贴"]
    bu_names = [item["name"] for item in BU_CONFIG]
    bu_weights = [item["weight"] for item in BU_CONFIG]
    bu_lookup = {item["name"]: item for item in BU_CONFIG}

    profiles: list[EmployeeProfile] = []
    for idx in range(1, employees + 1):
        bu = pick_weighted(rng, bu_names, bu_weights)
        bu_meta = bu_lookup[bu]
        function = pick_weighted(rng, function_values, function_weights)
        performance = pick_weighted(rng, perf_values, perf_weights)
        level = pick_weighted(rng, level_values, level_weights)
        tenure = pick_weighted(rng, tenure_values, tenure_weights)
        age_band = pick_weighted(rng, age_values, age_weights)

        salary_low, salary_high = LEVEL_BASE_SALARY[level]
        salary_anchor = rng.randint(salary_low, salary_high)
        base_salary = bounded_int(
            salary_anchor
            * bu_meta["salary_factor"]
            * TENURE_SALARY_MULTIPLIER.get(tenure, 1.0)
            * AGE_SALARY_MULTIPLIER.get(age_band, 1.0)
            * rng.uniform(0.96, 1.04),
            floor=sample_stats.numeric_min["底薪"],
        )

        performance_multiplier = PERFORMANCE_MULTIPLIER.get(performance, 1.0)
        post_multiplier = FUNCTION_POST_MULTIPLIER.get(function, 1.0)
        overtime_multiplier = FUNCTION_OVERTIME_MULTIPLIER.get(function, 1.0)

        salary_adjustment_base = bounded_int(
            max(sample_adjustment_avg, base_salary * 0.038)
            * performance_multiplier
            * rng.uniform(0.75, 1.20)
        )
        performance_base = bounded_int(
            base_salary
            * 0.31
            * performance_multiplier
            * bu_meta["bonus_factor"]
            * rng.uniform(0.90, 1.12)
        )
        post_allowance_base = bounded_int(
            sample_post_avg
            * post_multiplier
            * (1.08 if level in {"A类领导", "O类领导"} else 1.0)
            * rng.uniform(0.72, 1.30)
        )
        overtime_base = bounded_int(
            sample_overtime_avg
            * overtime_multiplier
            * (0.92 if level in {"A类领导", "O类领导"} else 1.0)
            * rng.uniform(0.70, 1.35)
        )
        compensation_base = bounded_int(
            (base_salary + performance_base + post_allowance_base)
            * bu_meta["bonus_factor"]
            * rng.uniform(1.05, 3.10)
        )

        has_spouse_allowance = rng.random() < 0.78
        spouse_allowance_base = (
            bounded_int(sample_spouse_avg * rng.uniform(0.40, 2.60))
            if has_spouse_allowance
            else 0
        )

        shift_eligible = function in SHIFT_ELIGIBLE_FUNCTIONS and rng.random() < 0.33

        special_hire_type = "none"
        special_hire_base = 0
        if function in SPECIAL_HIRE_FUNCTIONS and rng.random() < 0.21:
            special_hire_type = "recurring" if rng.random() < 0.62 else "phase"
            special_hire_base = bounded_int(
                sample_stats.numeric_avg["特招津贴"] * rng.uniform(0.70, 1.45)
            )

        signing_bonus_month = 0
        if rng.random() < 0.025:
            signing_bonus_month = rng.randint(1, 3)

        profiles.append(
            EmployeeProfile(
                employee_id=f"{bu_meta['prefix']}-EMP-{idx:06d}",
                bu=bu,
                bu_prefix=bu_meta["prefix"],
                function=function,
                performance=performance,
                level=level,
                tenure=tenure,
                age_band=age_band,
                base_salary=base_salary,
                salary_adjustment_base=salary_adjustment_base,
                performance_base=performance_base,
                post_allowance_base=post_allowance_base,
                overtime_base=overtime_base,
                compensation_base=compensation_base,
                spouse_allowance_base=spouse_allowance_base,
                shift_eligible=shift_eligible,
                special_hire_type=special_hire_type,
                special_hire_base=special_hire_base,
                signing_bonus_month=signing_bonus_month,
            )
        )
    return profiles


def month_wave(month_index: int, period: float, amplitude: float, phase_shift: float = 0.0) -> float:
    angle = (month_index / period) * math.tau + phase_shift
    return 1.0 + amplitude * math.sin(angle)


def build_month_row(
    profile: EmployeeProfile,
    year: int,
    month: int,
    month_index: int,
    rng: random.Random,
) -> dict[str, str]:
    base_wave = month_wave(month_index, period=12.0, amplitude=0.020, phase_shift=0.25)
    perf_wave = month_wave(month_index, period=6.0, amplitude=0.045, phase_shift=0.9)
    overtime_wave = month_wave(month_index, period=4.0, amplitude=0.085, phase_shift=1.5)
    comp_wave = month_wave(month_index, period=8.0, amplitude=0.12, phase_shift=0.6)

    base_salary = bounded_int(profile.base_salary * base_wave * rng.uniform(0.985, 1.015))
    salary_adjustment = bounded_int(
        profile.salary_adjustment_base * perf_wave * rng.uniform(0.90, 1.10)
    )
    performance_pay = bounded_int(
        profile.performance_base * perf_wave * rng.uniform(0.90, 1.12)
    )
    post_allowance = bounded_int(
        profile.post_allowance_base * month_wave(month_index, 10.0, 0.03, 0.1) * rng.uniform(0.93, 1.08)
    )
    overtime_pay = bounded_int(
        profile.overtime_base * overtime_wave * rng.uniform(0.78, 1.25)
    )

    shift_allowance = 0
    if profile.shift_eligible:
        shift_allowance = 600 if rng.random() < 0.42 else 0

    special_hire_allowance = 0
    if profile.special_hire_type == "recurring":
        if rng.random() < 0.72:
            special_hire_allowance = bounded_int(
                profile.special_hire_base * rng.uniform(0.82, 1.16)
            )
    elif profile.special_hire_type == "phase":
        if 1 <= month_index <= 6 and rng.random() < 0.55:
            special_hire_allowance = bounded_int(
                profile.special_hire_base * rng.uniform(0.85, 1.18)
            )

    compensation = bounded_int(
        profile.compensation_base * comp_wave * rng.uniform(0.82, 1.18)
    )
    signing_bonus = 0
    if profile.signing_bonus_month and month_index == profile.signing_bonus_month:
        signing_bonus = bounded_int(rng.uniform(4_000, 12_000))

    spouse_allowance = 0
    if profile.spouse_allowance_base and rng.random() < 0.96:
        spouse_allowance = bounded_int(
            profile.spouse_allowance_base * rng.uniform(0.94, 1.06)
        )

    return {
        "统计年度": str(year),
        "统计月份": str(month),
        "BU": profile.bu,
        "员工ID": profile.employee_id,
        "职能": profile.function,
        "绩效分位": profile.performance,
        "级别": profile.level,
        "司龄分箱": profile.tenure,
        "年龄分箱": profile.age_band,
        "底薪": str(base_salary),
        "基本工资调整": str(salary_adjustment),
        "内勤绩效": str(performance_pay),
        "岗位津贴": str(post_allowance),
        "倒班津贴": str(shift_allowance),
        "特招津贴": str(special_hire_allowance),
        "加班费": str(overtime_pay),
        "经济补偿金": str(compensation),
        "签约金": str(signing_bonus),
        "降温取暖费": "300",
        "配偶补贴": str(spouse_allowance),
        "借调补贴": "0",
    }


def write_dataset(
    output_path: Path,
    profiles: list[EmployeeProfile],
    rng: random.Random,
) -> int:
    months = month_sequence(2025, 1, 2027, 1)
    total_rows = 0
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for month_index, (year, month) in enumerate(months, start=1):
            for profile in profiles:
                writer.writerow(build_month_row(profile, year, month, month_index, rng))
                total_rows += 1
    return total_rows


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    sample_stats = read_sample_stats(input_path)
    rng = random.Random(args.seed)
    profiles = build_employee_profiles(args.employees, sample_stats, rng)
    total_rows = write_dataset(output_path, profiles, rng)
    print(
        f"Generated {total_rows} rows for {args.employees} employees into {output_path}"
    )


if __name__ == "__main__":
    main()
