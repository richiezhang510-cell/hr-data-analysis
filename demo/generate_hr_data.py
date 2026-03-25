#!/usr/bin/env python3
"""Generate a Ping An style full salary wide-table dataset."""

from __future__ import annotations

import argparse
import csv
import random
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from salary_schema import get_schema


SCRIPT_DIR = Path(__file__).resolve().parent
FULL_SCHEMA = get_schema("pingan_full")
DEFAULT_OUTPUT = SCRIPT_DIR / "薪酬数据_宽表_202412_202512_平安仿真_1999998行.csv"
DEFAULT_EMPLOYEES = 153_846
DEFAULT_START = "2024-12"
DEFAULT_END = "2025-12"
DEFAULT_SEED = 20260323

LEVEL_ORDER = ["C", "B", "A", "O"]
PERFORMANCE_BANDS = ["前10%", "前20%", "前30%", "前40%", "前50%", "前60%", "前70%", "后20%", "后30%"]

BU_CONFIG = [
    {"name": "平安寿险", "prefix": "PASX", "weight": 0.21, "salary_factor": 1.06, "bonus_factor": 1.20},
    {"name": "平安银行", "prefix": "PAYH", "weight": 0.18, "salary_factor": 1.09, "bonus_factor": 1.08},
    {"name": "平安科技", "prefix": "PAKJ", "weight": 0.16, "salary_factor": 1.18, "bonus_factor": 1.05},
    {"name": "平安产险", "prefix": "PACX", "weight": 0.14, "salary_factor": 1.03, "bonus_factor": 1.12},
    {"name": "平安陆控", "prefix": "PALK", "weight": 0.10, "salary_factor": 1.05, "bonus_factor": 1.04},
    {"name": "平安健康险", "prefix": "PAJK", "weight": 0.07, "salary_factor": 0.99, "bonus_factor": 1.03},
    {"name": "平安集团", "prefix": "PJT", "weight": 0.08, "salary_factor": 1.12, "bonus_factor": 1.00},
    {"name": "平安好医生", "prefix": "PAHYS", "weight": 0.06, "salary_factor": 0.97, "bonus_factor": 1.01},
]

DEPARTMENTS = {
    "平安寿险": ["个险销售", "银保业务", "团险业务", "运营支持", "精算财务", "风控合规", "人力行政"],
    "平安银行": ["零售金融", "公司金融", "普惠金融", "风险管理", "运营管理", "科技产品", "综合管理"],
    "平安科技": ["研发工程", "数据智能", "产品设计", "项目交付", "平台运营", "财务法务", "人力行政"],
    "平安产险": ["车险业务", "非车业务", "理赔服务", "客户运营", "风控管理", "财务管理", "综合管理"],
    "平安陆控": ["产品运营", "科技研发", "风险策略", "业务支持", "资金财务", "综合管理"],
    "平安健康险": ["产品精算", "运营客服", "渠道销售", "风控合规", "医学管理", "综合管理"],
    "平安集团": ["战略企划", "财务管理", "法务合规", "人力资源", "品牌市场", "科技赋能"],
    "平安好医生": ["互联网产品", "研发工程", "用户运营", "医生服务", "市场商务", "综合管理"],
}

DEPARTMENT_SEQUENCE_WEIGHTS = {
    "销售": {"M序列": 0.16, "P序列": 0.76, "T序列": 0.08},
    "业务": {"M序列": 0.18, "P序列": 0.70, "T序列": 0.12},
    "运营": {"M序列": 0.18, "P序列": 0.66, "T序列": 0.16},
    "研发": {"M序列": 0.10, "P序列": 0.15, "T序列": 0.75},
    "科技": {"M序列": 0.10, "P序列": 0.18, "T序列": 0.72},
    "产品": {"M序列": 0.12, "P序列": 0.70, "T序列": 0.18},
    "风控": {"M序列": 0.16, "P序列": 0.70, "T序列": 0.14},
    "财务": {"M序列": 0.15, "P序列": 0.75, "T序列": 0.10},
    "法务": {"M序列": 0.18, "P序列": 0.76, "T序列": 0.06},
    "综合": {"M序列": 0.22, "P序列": 0.70, "T序列": 0.08},
}

LEVEL_WEIGHTS_BY_SEQUENCE = {
    "M序列": [0.18, 0.42, 0.28, 0.12],
    "P序列": [0.58, 0.27, 0.12, 0.03],
    "T序列": [0.52, 0.30, 0.14, 0.04],
}

BASE_SALARY_RANGES = {
    "C": (6500, 11500),
    "B": (11500, 22000),
    "A": (21000, 42000),
    "O": (38000, 88000),
}

PERF_BONUS_MULTIPLIER = {
    "前10%": 1.55,
    "前20%": 1.38,
    "前30%": 1.24,
    "前40%": 1.12,
    "前50%": 1.00,
    "前60%": 0.92,
    "前70%": 0.84,
    "后20%": 0.68,
    "后30%": 0.54,
}


@dataclass(frozen=True)
class EmployeeProfile:
    employee_id: str
    employee_um: str
    bu: str
    bu_prefix: str
    department: str
    function_name: str
    sequence: str
    level: str
    performance_band: str
    age_start: int
    tenure_start: float
    social_base: float
    house_fund_base: float
    basic_salary: float
    manager: bool
    core_sales: bool
    tech_role: bool
    family_support: bool
    mortgage_holder: bool
    rent_holder: bool
    child_education: bool
    elder_support: bool
    degree_support: bool
    infant_care: bool
    pension_support: bool
    high_value_talent: bool
    shift_role: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate full Ping An style salary wide-table CSV.")
    parser.add_argument("--employees", type=int, default=DEFAULT_EMPLOYEES, help="Fixed employee pool size.")
    parser.add_argument("--start", default=DEFAULT_START, help="Start period in YYYY-MM.")
    parser.add_argument("--end", default=DEFAULT_END, help="End period in YYYY-MM.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path.")
    return parser.parse_args()


def parse_period(period: str) -> tuple[int, int]:
    year_str, month_str = period.split("-")
    year = int(year_str)
    month = int(month_str)
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {period}")
    return year, month


def iter_periods(start: str, end: str) -> list[tuple[int, int]]:
    start_year, start_month = parse_period(start)
    end_year, end_month = parse_period(end)
    cursor_year, cursor_month = start_year, start_month
    periods: list[tuple[int, int]] = []
    while (cursor_year, cursor_month) <= (end_year, end_month):
        periods.append((cursor_year, cursor_month))
        cursor_month += 1
        if cursor_month > 12:
            cursor_year += 1
            cursor_month = 1
    return periods


def weighted_choice(rng: random.Random, values: list[str], weights: list[float]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def classify_department(name: str) -> str:
    if any(keyword in name for keyword in ["销售", "业务", "渠道", "零售", "公司金融", "普惠"]):
        return "销售"
    if any(keyword in name for keyword in ["研发", "科技", "数据", "工程"]):
        return "研发"
    if "产品" in name:
        return "产品"
    if any(keyword in name for keyword in ["运营", "客服", "服务", "支持", "理赔"]):
        return "运营"
    if any(keyword in name for keyword in ["风控", "风险", "合规"]):
        return "风控"
    if any(keyword in name for keyword in ["财务", "精算", "资金"]):
        return "财务"
    if "法务" in name:
        return "法务"
    return "综合"


def age_band(age: int) -> str:
    if age < 26:
        return "25以下"
    if age <= 30:
        return "25-30"
    if age <= 35:
        return "30-35"
    if age <= 40:
        return "35-40"
    if age <= 45:
        return "40-45"
    return "45+"


def tenure_band(tenure: float) -> str:
    if tenure < 1:
        return "1年以下"
    if tenure < 3:
        return "1-3"
    if tenure < 5:
        return "3-5"
    if tenure < 8:
        return "5-8"
    if tenure < 10:
        return "8-10"
    return "10年以上"


def fmt_money(value: float | None) -> str:
    if value is None:
        return ""
    if abs(value) < 0.005:
        return ""
    return f"{value:.2f}"


def tax_amount(taxable_income: float) -> float:
    brackets = [
        (3000, 0.03, 0),
        (12000, 0.10, 210),
        (25000, 0.20, 1410),
        (35000, 0.25, 2660),
        (55000, 0.30, 4410),
        (80000, 0.35, 7160),
        (float("inf"), 0.45, 15160),
    ]
    for upper, rate, quick in brackets:
        if taxable_income <= upper:
            return max(taxable_income * rate - quick, 0.0)
    return 0.0


def assign_sequence(rng: random.Random, department: str) -> str:
    kind = classify_department(department)
    config = DEPARTMENT_SEQUENCE_WEIGHTS[kind]
    return weighted_choice(rng, list(config.keys()), list(config.values()))


def build_employee_profiles(employee_count: int, rng: random.Random) -> list[EmployeeProfile]:
    profiles: list[EmployeeProfile] = []
    bu_names = [item["name"] for item in BU_CONFIG]
    bu_weights = [item["weight"] for item in BU_CONFIG]
    bu_lookup = {item["name"]: item for item in BU_CONFIG}

    for index in range(1, employee_count + 1):
        bu = weighted_choice(rng, bu_names, bu_weights)
        bu_info = bu_lookup[bu]
        department = rng.choice(DEPARTMENTS[bu])
        function_name = classify_department(department)
        sequence = assign_sequence(rng, department)
        level = weighted_choice(rng, LEVEL_ORDER, LEVEL_WEIGHTS_BY_SEQUENCE[sequence])
        performance_band = weighted_choice(
            rng,
            PERFORMANCE_BANDS,
            [0.08, 0.12, 0.15, 0.15, 0.14, 0.12, 0.10, 0.08, 0.06],
        )
        age_start = rng.randint(22, 54)
        max_tenure = max(min(age_start - 21, 25), 0)
        tenure_start = round(rng.uniform(0.2, max(max_tenure, 1.0)), 1)

        low, high = BASE_SALARY_RANGES[level]
        sequence_factor = {"M序列": 1.10, "P序列": 1.00, "T序列": 1.12}[sequence]
        salary = rng.uniform(low, high) * bu_info["salary_factor"] * sequence_factor
        if function_name == "研发":
            salary *= 1.08
        if function_name == "销售":
            salary *= 0.95
        if performance_band in {"前10%", "前20%"}:
            salary *= 1.05

        social_base = min(max(salary * rng.uniform(0.78, 1.04), 5000), 38082)
        house_fund_base = min(max(salary * rng.uniform(0.80, 1.08), 3500), 42000)
        profiles.append(
            EmployeeProfile(
                employee_id=f"{bu_info['prefix']}-EMP-{index:06d}",
                employee_um=f"UM{index:08d}",
                bu=bu,
                bu_prefix=bu_info["prefix"],
                department=department,
                function_name=function_name,
                sequence=sequence,
                level=level,
                performance_band=performance_band,
                age_start=age_start,
                tenure_start=tenure_start,
                social_base=round(social_base, 2),
                house_fund_base=round(house_fund_base, 2),
                basic_salary=round(salary, 2),
                manager=sequence == "M序列" or level in {"A", "O"},
                core_sales=function_name == "销售",
                tech_role=sequence == "T序列",
                family_support=rng.random() < 0.14,
                mortgage_holder=rng.random() < 0.18,
                rent_holder=rng.random() < 0.32,
                child_education=rng.random() < 0.22,
                elder_support=rng.random() < 0.17,
                degree_support=rng.random() < 0.08,
                infant_care=rng.random() < 0.10,
                pension_support=rng.random() < 0.12,
                high_value_talent=(level in {"A", "O"}) or (sequence == "T序列" and rng.random() < 0.18),
                shift_role=function_name == "运营" and rng.random() < 0.28,
            )
        )
    return profiles


def set_amount(row: dict[str, str], column: str, amount: float | None) -> None:
    row[column] = fmt_money(amount)


def month_serial(year: int, month: int) -> int:
    return year * 12 + month


def generate_row(profile: EmployeeProfile, year: int, month: int, rng: random.Random) -> dict[str, str]:
    row = {column: "" for column in FULL_SCHEMA.wide_columns}
    months_since_start = month_serial(year, month) - month_serial(2024, 12)
    age = profile.age_start + (months_since_start // 12)
    tenure = round(profile.tenure_start + months_since_start / 12, 1)
    bonus_factor = PERF_BONUS_MULTIPLIER[profile.performance_band]
    salary_growth = 1.0 + 0.008 * months_since_start
    base_salary = round(profile.basic_salary * salary_growth, 2)
    taxable_deductions = 0.0

    row["统计年度"] = str(year)
    row["统计月份"] = str(month)
    row["统计月"] = f"{year}-{month:02d}"
    row["员工ID"] = profile.employee_id
    row["员工UM"] = profile.employee_um
    row["BU"] = profile.bu
    row["部门"] = profile.department
    row["职能序列"] = profile.sequence
    row["职能"] = profile.function_name
    row["级别"] = profile.level
    row["去年绩效排名"] = profile.performance_band
    row["绩效分位"] = profile.performance_band
    row["年龄"] = str(age)
    row["年龄分箱"] = age_band(age)
    row["司龄"] = f"{tenure:.1f}"
    row["司龄分箱"] = tenure_band(tenure)

    set_amount(row, "底薪/基本工资", base_salary)
    set_amount(row, "基本工资调整", base_salary * 0.02 if month in {1, 4, 7, 10} and profile.level != "C" else None)
    set_amount(row, "岗位津贴", base_salary * (0.08 if profile.level in {"A", "O"} else 0.03))
    set_amount(row, "岗位/技能津贴", base_salary * (0.05 if profile.tech_role else 0.02))
    set_amount(row, "级别津贴", {"C": 300, "B": 1200, "A": 3200, "O": 8500}[profile.level])
    set_amount(row, "年资津贴", min(tenure * 80, 1800))
    set_amount(row, "内勤绩效", base_salary * 0.18 * bonus_factor if not profile.core_sales else None)
    set_amount(row, "月度绩效", base_salary * 0.12 * bonus_factor * rng.uniform(0.8, 1.2))
    set_amount(row, "内勤绩效奖金", base_salary * 0.09 * bonus_factor if profile.sequence != "P序列" else None)
    set_amount(row, "内勤绩效调整", base_salary * 0.03 * rng.uniform(-0.8, 1.1) if month in {3, 6, 9, 12} else None)
    set_amount(row, "特殊岗位津贴", base_salary * 0.04 if profile.tech_role or profile.shift_role else None)
    set_amount(row, "倒班津贴", rng.uniform(200, 1000) if profile.shift_role else None)
    set_amount(row, "加班费", base_salary * rng.uniform(0.01, 0.08) if profile.tech_role or profile.shift_role else None)
    set_amount(row, "特招津贴", base_salary * 0.10 if profile.high_value_talent and month <= 6 and rng.random() < 0.08 else None)
    set_amount(row, "考勤奖", rng.uniform(200, 1200) if rng.random() < 0.38 else None)
    set_amount(row, "过节费", rng.uniform(500, 3000) if month in {1, 2, 9} else None)
    set_amount(row, "降温取暖费", rng.uniform(300, 1200) if month in {1, 2, 7, 8, 12} else None)
    set_amount(row, "通讯补贴", rng.uniform(100, 500) if profile.level in {"B", "A", "O"} else None)
    set_amount(row, "交通补贴", rng.uniform(200, 1500))
    set_amount(row, "误餐补贴", rng.uniform(120, 500) if profile.shift_role or profile.tech_role else None)
    set_amount(row, "配偶补贴", rng.uniform(500, 1800) if profile.family_support else None)
    set_amount(row, "长期出差补助", rng.uniform(1200, 5000) if rng.random() < 0.04 else None)
    set_amount(row, "异地任职津贴", rng.uniform(1500, 6000) if rng.random() < 0.03 else None)
    set_amount(row, "学历津贴", rng.uniform(300, 1500) if profile.degree_support else None)
    set_amount(row, "借调补贴", rng.uniform(1000, 8000) if rng.random() < 0.02 else None)
    set_amount(row, "实习津贴", rng.uniform(1800, 3500) if age <= 24 and tenure < 1.0 and rng.random() < 0.08 else None)
    set_amount(row, "计件薪", rng.uniform(300, 2200) if profile.core_sales and rng.random() < 0.35 else None)

    if profile.core_sales:
        set_amount(row, "零售业务提奖", base_salary * rng.uniform(0.12, 0.60) * bonus_factor)
        set_amount(row, "公司业务提奖", base_salary * rng.uniform(0.05, 0.28) * bonus_factor if "公司" in profile.department else None)
        set_amount(row, "普惠业务提奖", base_salary * rng.uniform(0.03, 0.16) * bonus_factor if "普惠" in profile.department else None)
        set_amount(row, "同业业务提奖", base_salary * rng.uniform(0.02, 0.12) * bonus_factor if "银行" in profile.bu else None)
        set_amount(row, "特管业务提奖", base_salary * rng.uniform(0.02, 0.10) * bonus_factor if rng.random() < 0.12 else None)
        set_amount(row, "月度业务提奖", base_salary * rng.uniform(0.08, 0.40) * bonus_factor)
        set_amount(row, "非月度业务提奖", base_salary * rng.uniform(0.12, 0.50) * bonus_factor if month in {6, 12} else None)
        set_amount(row, "团队长提奖", base_salary * rng.uniform(0.05, 0.22) * bonus_factor if profile.manager else None)
        set_amount(row, "展业津贴", base_salary * rng.uniform(0.03, 0.18))
        set_amount(row, "业务浮动奖金", base_salary * rng.uniform(0.08, 0.35) * bonus_factor)
        set_amount(row, "业务推动激励", rng.uniform(500, 5000) if rng.random() < 0.18 else None)
        set_amount(row, "增员激励", rng.uniform(1000, 8000) if rng.random() < 0.08 else None)

    if "寿险" in profile.bu:
        set_amount(row, "养老险短险产品绩效(实发)", base_salary * rng.uniform(0.05, 0.24) * bonus_factor if rng.random() < 0.42 else None)
        set_amount(row, "养老险短险产品绩效(计提)", base_salary * rng.uniform(0.03, 0.18) * bonus_factor if rng.random() < 0.34 else None)
        set_amount(row, "养老险长险产品绩效(实发)", base_salary * rng.uniform(0.08, 0.35) * bonus_factor if rng.random() < 0.48 else None)
        set_amount(row, "养老险长险产品绩效(计提)", base_salary * rng.uniform(0.05, 0.22) * bonus_factor if rng.random() < 0.39 else None)
        set_amount(row, "养老险直投业务绩效(实发)", base_salary * rng.uniform(0.03, 0.15) * bonus_factor if rng.random() < 0.18 else None)
        set_amount(row, "养老险直投业务绩效(计提)", base_salary * rng.uniform(0.02, 0.12) * bonus_factor if rng.random() < 0.16 else None)
        set_amount(row, "团险长险产品绩效(实发)", base_salary * rng.uniform(0.04, 0.20) * bonus_factor if rng.random() < 0.22 else None)
        set_amount(row, "团险长险产品绩效(计提)", base_salary * rng.uniform(0.03, 0.15) * bonus_factor if rng.random() < 0.18 else None)
        set_amount(row, "团险短险产品绩效(实发)", base_salary * rng.uniform(0.04, 0.18) * bonus_factor if rng.random() < 0.22 else None)
        set_amount(row, "团险短险产品绩效(计提)", base_salary * rng.uniform(0.03, 0.12) * bonus_factor if rng.random() < 0.18 else None)
        set_amount(row, "年金产品综合开拓绩效", base_salary * rng.uniform(0.03, 0.14) * bonus_factor if rng.random() < 0.18 else None)
        set_amount(row, "企业年金产品绩效", base_salary * rng.uniform(0.03, 0.14) * bonus_factor if rng.random() < 0.14 else None)
        set_amount(row, "年金基础", rng.uniform(500, 2500) if profile.level in {"A", "O"} else None)

    if "产险" in profile.bu:
        set_amount(row, "产险产品综合开拓绩效", base_salary * rng.uniform(0.06, 0.30) * bonus_factor if rng.random() < 0.45 else None)

    if "健康险" in profile.bu:
        set_amount(row, "健康险产品综合开拓绩效", base_salary * rng.uniform(0.04, 0.18) * bonus_factor if rng.random() < 0.33 else None)

    if "好医生" in profile.bu:
        set_amount(row, "健康互联(好医生)综合开拓绩效", base_salary * rng.uniform(0.03, 0.16) * bonus_factor if rng.random() < 0.35 else None)

    if "科技" in profile.bu or profile.tech_role:
        set_amount(row, "中期激励奖金", base_salary * rng.uniform(0.25, 1.20) if month in {6, 12} and profile.high_value_talent else None)
        set_amount(row, "职务发明奖励", rng.uniform(2000, 50000) if rng.random() < 0.03 else None)
        set_amount(row, "内部推荐奖", rng.uniform(1500, 12000) if rng.random() < 0.05 else None)

    set_amount(row, "综合业务贡献绩效", base_salary * rng.uniform(0.03, 0.25) * bonus_factor if rng.random() < 0.24 else None)
    set_amount(row, "外勤管理绩效", base_salary * rng.uniform(0.05, 0.24) * bonus_factor if profile.manager and profile.core_sales else None)
    set_amount(row, "内勤其他奖金", base_salary * rng.uniform(0.02, 0.10) if rng.random() < 0.18 else None)
    set_amount(row, "外勤其他奖金", base_salary * rng.uniform(0.03, 0.18) if profile.core_sales and rng.random() < 0.12 else None)
    set_amount(row, "机构奖金", base_salary * rng.uniform(0.05, 0.30) if month in {6, 12} and rng.random() < 0.22 else None)
    set_amount(row, "总行其他奖金", base_salary * rng.uniform(0.05, 0.24) if "集团" in profile.bu and month in {6, 12} else None)
    set_amount(row, "公司特殊奖励奖金", base_salary * rng.uniform(0.10, 0.80) if rng.random() < 0.02 else None)
    set_amount(row, "全年一次性奖金", base_salary * rng.uniform(1.0, 5.5) * bonus_factor if month == 12 else None)
    set_amount(row, "年终奖", base_salary * rng.uniform(0.8, 4.2) * bonus_factor if month == 12 else None)
    set_amount(row, "递延年终奖", base_salary * rng.uniform(0.4, 2.5) * bonus_factor if month == 1 and profile.level in {"A", "O"} else None)
    set_amount(row, "年底绩效奖", base_salary * rng.uniform(0.5, 2.4) * bonus_factor if month == 12 else None)
    set_amount(row, "奖金预发", base_salary * rng.uniform(0.2, 0.8) if month in {3, 9} and rng.random() < 0.08 else None)
    set_amount(row, "蓄水奖金", base_salary * rng.uniform(0.1, 0.6) if month in {5, 11} and rng.random() < 0.10 else None)

    for column in [
        "外勤年中/年终奖(直投)",
        "外勤年中/年终奖(资管)",
        "外勤年中/年终奖(年金)",
        "外勤年中/年终奖(长险)",
        "外勤年中/年终奖(短险)",
        "外勤年中/年终奖(产险)",
        "外勤年中/年终奖",
    ]:
        set_amount(
            row,
            column,
            base_salary * rng.uniform(0.3, 1.8) * bonus_factor if profile.core_sales and month in {6, 12} and rng.random() < 0.24 else None,
        )

    set_amount(row, "签约金", base_salary * rng.uniform(0.4, 2.5) if profile.high_value_talent and months_since_start == 0 else None)
    set_amount(row, "经济补偿金", base_salary * rng.uniform(2.0, 8.0) if rng.random() < 0.002 else None)
    set_amount(row, "竞业补偿金", base_salary * rng.uniform(0.5, 2.0) if rng.random() < 0.0015 else None)
    set_amount(row, "年休假未休补偿金", rng.uniform(1000, 12000) if month in {6, 12} and rng.random() < 0.05 else None)
    set_amount(row, "医疗补助(非工伤)", rng.uniform(500, 8000) if rng.random() < 0.015 else None)
    set_amount(row, "医疗补助(工伤)", rng.uniform(1500, 30000) if rng.random() < 0.001 else None)
    set_amount(row, "独生子女费", 50.00 if rng.random() < 0.04 else None)
    set_amount(row, "女工保健费", rng.uniform(50, 200) if rng.random() < 0.08 else None)
    set_amount(row, "托儿费", rng.uniform(200, 1000) if profile.infant_care and rng.random() < 0.08 else None)
    set_amount(row, "教师课酬", rng.uniform(300, 4000) if rng.random() < 0.01 else None)
    set_amount(row, "调查奖金", rng.uniform(800, 5000) if rng.random() < 0.01 else None)
    set_amount(row, "辅导专员奖金", rng.uniform(1000, 6000) if rng.random() < 0.01 else None)
    set_amount(row, "前线经理奖金", rng.uniform(2000, 10000) if profile.manager and rng.random() < 0.02 else None)
    set_amount(row, "机构班子成员奖", rng.uniform(8000, 30000) if profile.level in {"A", "O"} and rng.random() < 0.01 else None)
    set_amount(row, "年金奖励缴费(公司)", base_salary * 0.03 if profile.level in {"A", "O"} else None)
    set_amount(row, "年金扣款(个人)", base_salary * 0.02 if profile.level in {"A", "O"} and rng.random() < 0.75 else None)

    pension_personal = round(profile.social_base * 0.08, 2)
    medical_personal = round(profile.social_base * 0.02 + 3, 2)
    unemployment_personal = round(profile.social_base * 0.005, 2)
    housing_personal = round(profile.house_fund_base * 0.12, 2)
    pension_company = round(profile.social_base * 0.16, 2)
    medical_company = round(profile.social_base * 0.07, 2)
    workinjury_company = round(profile.social_base * 0.0025, 2)
    maternity_company = round(profile.social_base * 0.0045, 2)
    unemployment_company = round(profile.social_base * 0.005, 2)
    disability_company = round(profile.social_base * 0.006, 2)
    housing_company = round(profile.house_fund_base * 0.12, 2)
    set_amount(row, "养老保险扣款", pension_personal)
    set_amount(row, "医疗保险扣款", medical_personal)
    set_amount(row, "失业保险扣款", unemployment_personal)
    set_amount(row, "住房公积金扣款", housing_personal)
    set_amount(row, "养老保险(公司)", pension_company)
    set_amount(row, "医疗保险(公司)", medical_company)
    set_amount(row, "工伤保险(公司)", workinjury_company)
    set_amount(row, "生育保险(公司)", maternity_company)
    set_amount(row, "失业保险(公司)", unemployment_company)
    set_amount(row, "残疾保障(公司)", disability_company)
    set_amount(row, "住房公积金(公司)", housing_company)
    set_amount(row, "计提工会费", base_salary * 0.02)
    set_amount(row, "计提教育经费", base_salary * 0.015)
    set_amount(row, "代扣工会费", base_salary * 0.005)

    if rng.random() < 0.015:
        set_amount(row, "养老保险扣款补缴", pension_personal * rng.uniform(0.5, 2.5))
        set_amount(row, "医疗保险扣款补缴", medical_personal * rng.uniform(0.5, 2.5))
        set_amount(row, "失业保险扣款补缴", unemployment_personal * rng.uniform(0.5, 2.5))
        set_amount(row, "住房公积金扣款补缴", housing_personal * rng.uniform(0.5, 2.5))
        set_amount(row, "养老保险(公司)补缴", pension_company * rng.uniform(0.5, 2.5))
        set_amount(row, "医疗保险(公司)补缴", medical_company * rng.uniform(0.5, 2.5))
        set_amount(row, "失业保险(公司)补缴", unemployment_company * rng.uniform(0.5, 2.5))
        set_amount(row, "住房公积金(公司)补缴", housing_company * rng.uniform(0.5, 2.5))
        set_amount(row, "其他社保补缴(公司)", rng.uniform(100, 800))
        set_amount(row, "其他社保扣款补缴", rng.uniform(50, 600))

    set_amount(row, "补充住房津贴", rng.uniform(500, 5000) if profile.level in {"A", "O"} else None)
    set_amount(row, "商业保险", rng.uniform(60, 400) if rng.random() < 0.60 else None)
    set_amount(row, "商业保险税前扣款", rng.uniform(80, 600) if rng.random() < 0.18 else None)
    set_amount(row, "商业保险免税额", rng.uniform(50, 200) if row["商业保险税前扣款"] else None)
    set_amount(row, "其他社保(公司)", rng.uniform(50, 400) if rng.random() < 0.10 else None)
    set_amount(row, "其他社保扣款", rng.uniform(20, 300) if rng.random() < 0.08 else None)

    set_amount(row, "代扣房款", rng.uniform(800, 6000) if profile.mortgage_holder else None)
    set_amount(row, "代扣房租", rng.uniform(600, 4500) if profile.rent_holder else None)
    set_amount(row, "公益捐款", rng.uniform(10, 500) if rng.random() < 0.03 else None)
    set_amount(row, "处罚扣款", rng.uniform(50, 3000) if rng.random() < 0.01 else None)
    set_amount(row, "病假扣款", rng.uniform(80, 4000) if rng.random() < 0.05 else None)
    set_amount(row, "事假扣款", rng.uniform(80, 3500) if rng.random() < 0.04 else None)
    set_amount(row, "旷工扣款", rng.uniform(200, 6000) if rng.random() < 0.005 else None)
    set_amount(row, "迟到款", rng.uniform(30, 300) if rng.random() < 0.06 else None)
    set_amount(row, "早退扣款", rng.uniform(30, 300) if rng.random() < 0.03 else None)
    set_amount(row, "中间外出扣款", rng.uniform(20, 200) if rng.random() < 0.02 else None)
    set_amount(row, "考勤扣款调整", rng.uniform(-500, 500) if rng.random() < 0.03 else None)
    set_amount(row, "漏打卡扣款", rng.uniform(20, 150) if rng.random() < 0.04 else None)
    set_amount(row, "其他代扣款", rng.uniform(10, 800) if rng.random() < 0.05 else None)
    set_amount(row, "税后扣款", rng.uniform(50, 500) if rng.random() < 0.02 else None)
    set_amount(row, "手工扣税", rng.uniform(100, 3000) if rng.random() < 0.01 else None)
    set_amount(row, "扣税不发薪(费用)", rng.uniform(100, 2000) if rng.random() < 0.01 else None)
    set_amount(row, "扣税不发薪(福利)", rng.uniform(100, 1500) if rng.random() < 0.01 else None)
    set_amount(row, "扣税不发薪(其他)", rng.uniform(100, 1500) if rng.random() < 0.01 else None)
    set_amount(row, "其他税前扣款", rng.uniform(100, 1500) if rng.random() < 0.03 else None)
    set_amount(row, "其他税前收入", rng.uniform(200, 3000) if rng.random() < 0.04 else None)
    set_amount(row, "其他税后收入", rng.uniform(100, 1500) if rng.random() < 0.03 else None)
    set_amount(row, "其他津贴", rng.uniform(100, 2000) if rng.random() < 0.15 else None)
    set_amount(row, "其他福利", rng.uniform(100, 3000) if rng.random() < 0.12 else None)
    set_amount(row, "综合福利保障(公司)", rng.uniform(200, 2500) if rng.random() < 0.55 else None)
    set_amount(row, "商外补贴", rng.uniform(100, 1800) if rng.random() < 0.06 else None)

    if profile.child_education:
        set_amount(row, "累计子女教育专项附加扣除", 1000.00)
    if profile.elder_support:
        set_amount(row, "累计赡养老人专项附加扣除", 2000.00)
    if profile.degree_support:
        set_amount(row, "累计继续学历教育专项附加扣除", 400.00)
    if profile.rent_holder:
        set_amount(row, "累计住房租金专项附加扣除", 1500.00)
    if profile.mortgage_holder:
        set_amount(row, "累计住房贷款利息专项附加扣除", 1000.00)
    if profile.infant_care:
        set_amount(row, "累计婴幼儿照护费专项附加扣除", 1000.00)
    if profile.pension_support:
        set_amount(row, "累计个人养老金扣除", rng.uniform(200, 1000))
    set_amount(row, "年度免税额累计值", 5000.00 * month)

    taxable_deductions += pension_personal + medical_personal + unemployment_personal + housing_personal
    taxable_deductions += float(row["累计子女教育专项附加扣除"] or 0)
    taxable_deductions += float(row["累计赡养老人专项附加扣除"] or 0)
    taxable_deductions += float(row["累计继续学历教育专项附加扣除"] or 0)
    taxable_deductions += float(row["累计住房贷款利息专项附加扣除"] or 0)
    taxable_deductions += float(row["累计住房租金专项附加扣除"] or 0)
    taxable_deductions += float(row["累计婴幼儿照护费专项附加扣除"] or 0)
    taxable_deductions += float(row["累计个人养老金扣除"] or 0)

    gross_taxable = 0.0
    for column in [
        "底薪/基本工资",
        "基本工资调整",
        "岗位津贴",
        "岗位/技能津贴",
        "级别津贴",
        "年资津贴",
        "内勤绩效",
        "月度绩效",
        "内勤绩效奖金",
        "综合业务贡献绩效",
        "业务浮动奖金",
        "月度业务提奖",
        "零售业务提奖",
        "公司业务提奖",
        "普惠业务提奖",
        "同业业务提奖",
        "特管业务提奖",
        "年终奖",
        "全年一次性奖金",
        "年底绩效奖",
        "签约金",
        "奖金预发",
        "其他税前收入",
    ]:
        gross_taxable += float(row[column] or 0)

    monthly_taxable = max(gross_taxable - 5000 - taxable_deductions, 0.0)
    tax = tax_amount(monthly_taxable)
    set_amount(row, "代扣个人所得税", tax)
    set_amount(row, "代扣个税", tax if rng.random() < 0.35 else None)
    set_amount(row, "个税调整", rng.uniform(-500, 500) if month in {1, 12} and rng.random() < 0.05 else None)

    set_amount(row, "政府奖励税前返还", rng.uniform(1000, 10000) if rng.random() < 0.005 else None)
    set_amount(row, "政府奖励税后返还", rng.uniform(800, 8000) if rng.random() < 0.005 else None)
    set_amount(row, "纳税主体变更", rng.uniform(-1000, 1000) if month in {1, 7} and rng.random() < 0.002 else None)

    return row


def write_dataset(output_path: Path, employees: int, periods: list[tuple[int, int]], seed: int) -> int:
    rng = random.Random(seed)
    profiles = build_employee_profiles(employees, rng)
    row_count = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FULL_SCHEMA.wide_columns)
        writer.writeheader()
        for year, month in periods:
            for profile in profiles:
                writer.writerow(generate_row(profile, year, month, rng))
                row_count += 1
    return row_count


def main() -> None:
    args = parse_args()
    periods = iter_periods(args.start, args.end)
    output_path = Path(args.output).expanduser().resolve()
    row_count = write_dataset(output_path, args.employees, periods, args.seed)
    print(
        f"已生成中国平安风格宽表：{output_path}\n"
        f"schema={FULL_SCHEMA.schema_id} | 员工数={args.employees:,} | 期间数={len(periods)} | 总行数={row_count:,}"
    )


if __name__ == "__main__":
    main()
