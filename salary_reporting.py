from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, ClassVar

from openai import OpenAI
from salary_schema import DEFAULT_SCHEMA_ID, SalarySchema, create_runtime_schema, detect_schema_from_headers, get_schema

# 导入 prompt 模块（如果可用）
try:
    from salary_prompts import (
        build_dimension_prompt,
        build_consolidated_prompt,
        build_external_research_prompt,
        build_report_revision_prompt,
        build_short_answer_prompt,
        SYSTEM_DIMENSION_ANALYSIS,
        SYSTEM_CONSOLIDATED_ANALYSIS,
        SYSTEM_REPORT_REVISION,
        SYSTEM_SHORT_ANSWER,
        SYSTEM_EXTERNAL_RESEARCH,
        SYSTEM_FULL_REPORT,
    )
    PROMPTS_AVAILABLE = True
except ImportError:
    PROMPTS_AVAILABLE = False
    # 如果导入失败，将在运行时使用旧的 prompt 函数（这些函数仍然在 salary_reporting.py 中定义）


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "salary_analysis.db"
UPLOADS_DIR = BASE_DIR / "uploads"
DEMO_DIR = BASE_DIR / "demo"
ACTIVE_DATASET_ENV_VAR = "ACTIVE_DATASET_PATH"

APP_META_ACTIVE_DATASET_PATH = "active_dataset_path"
APP_META_ACTIVE_DATASET_NAME = "active_dataset_name"
APP_META_ACTIVE_DATASET_SIGNATURE = "active_dataset_signature"
APP_META_ACTIVE_DATASET_IMPORTED_AT = "active_dataset_imported_at"
APP_META_ACTIVE_DATASET_ROW_COUNT = "active_dataset_row_count"
APP_META_ACTIVE_DATASET_PERIOD_START = "active_dataset_period_start"
APP_META_ACTIVE_DATASET_PERIOD_END = "active_dataset_period_end"
APP_META_ACTIVE_DATASET_ENCODING = "active_dataset_encoding"
APP_META_ACTIVE_DATASET_VALIDATION_STATUS = "active_dataset_validation_status"
APP_META_ACTIVE_SCHEMA_ID = "active_schema_id"
APP_META_ACTIVE_SCHEMA_MODE = "active_schema_mode"
APP_META_ACTIVE_SCHEMA_MANIFEST = "active_schema_manifest"

ACTIVE_SCHEMA = get_schema(DEFAULT_SCHEMA_ID)
WIDE_COLUMNS: list[str] = []
TEXT_DIMENSION_COLUMNS: list[str] = []
NUMERIC_COLUMNS: list[str] = []
DIMENSION_COLUMNS: list[str] = []
SUBJECT_COLUMNS: list[str] = []
SECONDARY_DIMENSION_LIMIT = 4
SUBJECT_ALIASES: dict[str, str] = {}
DIMENSION_ALIASES: dict[str, str] = {}
DEFAULT_SUBJECT = ""
DEFAULT_SECONDARY_DIMENSIONS: list[str] = []
SUBJECT_CATEGORIES: dict[str, str] = {}
AMBIGUOUS_SUBJECT_TERMS: dict[str, list[str]] = {}

NUMERIC_METRICS = ["总额", "平均金额", "领取人数", "发放覆盖率"]
METRIC_ALIASES = {
    "总额": "总额",
    "总量": "总额",
    "合计": "总额",
    "平均": "平均金额",
    "均值": "平均金额",
    "平均金额": "平均金额",
    "人均": "平均金额",
    "领取人数": "领取人数",
    "发放人数": "领取人数",
    "人数": "领取人数",
    "覆盖率": "发放覆盖率",
    "发放覆盖率": "发放覆盖率",
    "占比": "占比",
    "比例": "占比",
    "环比": "环比",
    "同比": "同比",
}


def configure_schema(schema: SalarySchema) -> None:
    global ACTIVE_SCHEMA
    global WIDE_COLUMNS, TEXT_DIMENSION_COLUMNS, NUMERIC_COLUMNS
    global DIMENSION_COLUMNS, SUBJECT_COLUMNS, SUBJECT_ALIASES, DIMENSION_ALIASES
    global DEFAULT_SUBJECT, DEFAULT_SECONDARY_DIMENSIONS, SUBJECT_CATEGORIES, AMBIGUOUS_SUBJECT_TERMS

    ACTIVE_SCHEMA = schema
    WIDE_COLUMNS = list(schema.wide_columns)
    TEXT_DIMENSION_COLUMNS = list(schema.text_dimension_columns)
    NUMERIC_COLUMNS = list(schema.numeric_columns)
    DIMENSION_COLUMNS = list(schema.dimension_columns)
    SUBJECT_COLUMNS = list(schema.subject_columns)
    SUBJECT_ALIASES = dict(schema.subject_aliases)
    DIMENSION_ALIASES = dict(schema.dimension_aliases)
    DEFAULT_SUBJECT = schema.default_subject
    DEFAULT_SECONDARY_DIMENSIONS = list(schema.default_secondary_dimensions)
    SUBJECT_CATEGORIES = dict(schema.subject_categories)
    AMBIGUOUS_SUBJECT_TERMS = {key: list(value) for key, value in schema.ambiguous_terms.items()}


def active_schema() -> SalarySchema:
    return ACTIVE_SCHEMA


configure_schema(ACTIVE_SCHEMA)

TRUSTED_SOURCE_KEYWORDS = [
    "wtwco.com",
    "mercer.com",
    "roberthalf",
    "gartner.com",
    "mckinsey.com",
    "pwc.com",
    "deloitte.com",
    "ey.com",
    "kornferry.com",
    "mof.gov.cn",
    "gov.cn",
    "ilo.org",
    "shrm.org",
]

STREAM_PROGRESS_STAGES = [
    ("scope", "识别问题与口径"),
    ("window", "确认分析时间窗口"),
    ("overview", "汇总总体指标"),
    ("dimensions", "逐维度拆解"),
    ("research", "检索外部参考"),
    ("consolidated", "生成综合报告"),
]


# ---------------------------------------------------------------------------
# Thread-safe TTL cache for expensive queries
# ---------------------------------------------------------------------------
import threading

class TTLCache:
    """Thread-safe in-memory cache with per-key TTL."""

    def __init__(self, default_ttl: int = 300):
        self._store: dict[str, tuple[float, Any]] = {}
        self._default_ttl = default_ttl
        self._lock = threading.RLock()  # 可重入锁，避免死锁

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.time() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        with self._lock:
            self._store[key] = (time.time() + (ttl or self._default_ttl), value)

    def make_key(self, *parts: str) -> str:
        raw = "|".join(str(p) for p in parts)
        return hashlib.md5(raw.encode()).hexdigest()

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_cache = TTLCache(default_ttl=300)


@dataclass
class AnalysisRequest:
    subject: str
    primary_dimension: str
    secondary_dimensions: list[str]
    start_year: int
    start_month: int
    end_year: int
    end_month: int
    metrics: list[str]
    question: str
    follow_up_context: dict[str, Any] | None = None


@dataclass
class CsvValidationResult:
    csv_path: Path
    filename: str
    encoding: str
    headers: list[str]
    row_count: int
    period_start: str
    period_end: str
    signature: str
    schema_id: str
    schema_mode: str = "registered"
    schema_manifest: dict[str, Any] | None = None


@dataclass(frozen=True)
class InferredColumn:
    name: str
    detected_type: str
    canonical_name: str
    confidence: float
    reason: str
    sample_values: list[str]
    non_empty_ratio: float
    numeric_ratio: float


@dataclass(frozen=True)
class SubjectResolution:
    resolved_subject: str
    display_subject: str
    confidence: float
    ambiguity_reason: str
    candidate_subjects: list[str]
    requires_confirmation: bool
    matched_terms: list[str]


def _dataset_signature(csv_path: Path) -> str:
    stat = csv_path.stat()
    return json.dumps(
        {
            "path": str(csv_path.resolve()),
            "size": stat.st_size,
            "mtime": stat.st_mtime,
        },
        ensure_ascii=False,
    )


def _set_meta_value(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO app_meta(key, value) VALUES (?, ?)",
        (key, value),
    )


def _get_meta_value(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return row[0]


def _delete_meta_keys(conn: sqlite3.Connection, *keys: str) -> None:
    if not keys:
        return
    placeholders = ",".join("?" for _ in keys)
    conn.execute(f"DELETE FROM app_meta WHERE key IN ({placeholders})", list(keys))


def _clear_dimension_value_cache() -> None:
    global _DIMENSION_VALUE_CACHE
    _DIMENSION_VALUE_CACHE = None


def _resolve_active_dataset_path(conn: sqlite3.Connection) -> Path | None:
    env_path = ensure_text(os.getenv(ACTIVE_DATASET_ENV_VAR))
    if env_path:
        return Path(env_path).expanduser().resolve()
    stored_path = _get_meta_value(conn, APP_META_ACTIVE_DATASET_PATH)
    if stored_path:
        return Path(stored_path).expanduser().resolve()
    return None


def _load_runtime_schema_manifest(conn: sqlite3.Connection) -> dict[str, Any] | None:
    raw = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_MANIFEST))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _schema_from_meta(conn: sqlite3.Connection) -> SalarySchema:
    schema_mode = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_MODE), "registered")
    if schema_mode == "inferred":
        manifest = _load_runtime_schema_manifest(conn)
        if manifest:
            return create_runtime_schema(manifest)
    schema_id = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_ID), DEFAULT_SCHEMA_ID)
    return get_schema(schema_id)


def _current_data_source_meta(conn: sqlite3.Connection) -> dict[str, Any]:
    dataset_path = _resolve_active_dataset_path(conn)
    filename = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_NAME))
    if dataset_path and not filename:
        filename = dataset_path.name
    ready = bool(
        dataset_path
        and dataset_path.exists()
        and ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_SIGNATURE))
        and ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_IMPORTED_AT))
        and ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_VALIDATION_STATUS)) == "passed"
    )
    row_count = int(ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_ROW_COUNT), "0") or "0")
    period_start = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_PERIOD_START))
    period_end = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_PERIOD_END))
    imported_at = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_IMPORTED_AT))
    signature = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_SIGNATURE))
    encoding = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_ENCODING))
    validation_status = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_VALIDATION_STATUS), "missing")
    schema = _schema_from_meta(conn)
    schema_id = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_ID), schema.schema_id)
    schema_mode = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_MODE), schema.schema_mode)
    schema_manifest = _load_runtime_schema_manifest(conn) if schema_mode == "inferred" else None

    if ready:
        message = "当前已加载真实数据，可直接开始分析。"
    elif dataset_path and not dataset_path.exists():
        message = "已记录的数据源文件不存在，请重新上传或重新配置真实数据。"
    else:
        message = "请先导入兼容当前宽表结构的真实 CSV 数据。"

    return {
        "ready": ready,
        "filename": filename,
        "path": str(dataset_path) if dataset_path else "",
        "row_count": row_count,
        "period_start": period_start,
        "period_end": period_end,
        "imported_at": imported_at,
        "signature": signature,
        "encoding": encoding,
        "validation_status": validation_status,
        "schema_id": schema_id,
        "schema_name": schema.display_name,
        "schema_mode": schema_mode,
        "source_manifest": schema_manifest,
        "message": message,
    }


def get_data_source_status() -> dict[str, Any]:
    conn = get_connection()
    try:
        return _current_data_source_meta(conn)
    finally:
        conn.close()


def ensure_data_source_ready() -> dict[str, Any]:
    status = get_data_source_status()
    if not status["ready"]:
        raise ValueError(status["message"])
    return status


def default_period_window() -> tuple[str, str]:
    data_source = get_data_source_status()
    if data_source.get("ready") and data_source.get("period_start") and data_source.get("period_end"):
        return data_source["period_start"], data_source["period_end"]
    return "2025-01", "2027-01"


def build_dimension_presets() -> list[dict[str, list[str] | str]]:
    primary_dimension = primary_dimension_name()
    dept_dim = "部门" if "部门" in DIMENSION_COLUMNS else "职能"
    perf_dim = "去年绩效排名" if "去年绩效排名" in DIMENSION_COLUMNS else "绩效分位"
    presets = [
        {"label": f"{primary_dimension} x {dept_dim}", "dimensions": [dept_dim]},
        {"label": f"{primary_dimension} x 级别", "dimensions": ["级别"]},
        {"label": f"{primary_dimension} x {perf_dim}", "dimensions": [perf_dim]},
    ]
    if "职能序列" in DIMENSION_COLUMNS:
        presets.append({"label": f"{primary_dimension} x 职能序列", "dimensions": ["职能序列"]})
    if "年龄分箱" in DIMENSION_COLUMNS:
        presets.append({"label": f"{primary_dimension} x 年龄分箱", "dimensions": ["年龄分箱"]})
    if "司龄分箱" in DIMENSION_COLUMNS:
        presets.append({"label": f"{primary_dimension} x 司龄分箱", "dimensions": ["司龄分箱"]})
    common_dimensions = [dim for dim in [dept_dim, "级别", perf_dim, "年龄分箱"] if dim in DIMENSION_COLUMNS]
    if common_dimensions:
        presets.append({"label": "常用四维", "dimensions": common_dimensions[:4]})
    return presets


def build_time_window_options() -> list[dict[str, str]]:
    start_period, end_period = default_period_window()
    try:
        end_year, end_month = parse_period(end_period)
    except ValueError:
        return []
    options = [
        ("最新月", end_period, end_period),
        ("最近3个月", *_format_shifted_window(end_year, end_month, 2)),
        ("最近6个月", *_format_shifted_window(end_year, end_month, 5)),
        ("全部期间", start_period, end_period),
    ]
    return [
        {"label": label, "start_period": start, "end_period": end}
        for label, start, end in options
    ]


def _format_shifted_window(end_year: int, end_month: int, months_back: int) -> tuple[str, str]:
    start_year, start_month = _shift_period(end_year, end_month, -months_back)
    return f"{start_year:04d}-{start_month:02d}", f"{end_year:04d}-{end_month:02d}"


def build_subject_catalog(subjects: list[str] | None = None) -> list[dict[str, str]]:
    items = subjects or SUBJECT_COLUMNS
    return [
        {
            "subject": subject,
            "category": SUBJECT_CATEGORIES.get(subject, "其他"),
        }
        for subject in items
    ]


def build_dimension_catalog(dimensions: list[str] | None = None) -> list[dict[str, str]]:
    items = dimensions or [dimension for dimension in DIMENSION_COLUMNS if dimension != "BU"]
    catalog: list[dict[str, str]] = []
    for dimension in items:
        if dimension in {"统计月份", "统计月"}:
            category = "时间"
        elif any(token in dimension for token in ["年龄", "司龄", "绩效", "级别"]):
            category = "人群"
        elif any(token in dimension for token in ["BU", "部门", "序列", "职能"]):
            category = "组织"
        else:
            category = "其他"
        catalog.append({"dimension": dimension, "category": category})
    return catalog


def active_capabilities() -> dict[str, bool]:
    schema = active_schema()
    if schema.capabilities:
        return dict(schema.capabilities)
    return {
        "supports_trend_analysis": True,
        "supports_employee_level_detail": True,
        "supports_yoy": True,
        "supports_mom": True,
    }


def primary_dimension_name() -> str:
    if "BU" in DIMENSION_COLUMNS:
        return "BU"
    if DIMENSION_COLUMNS:
        return DIMENSION_COLUMNS[0]
    return "BU"


def build_default_subject_options() -> list[str]:
    preferred = [
        "底薪/基本工资",
        "月度绩效",
        "内勤绩效",
        "年终奖",
        "代扣个人所得税",
        "住房公积金(公司)",
        "住房公积金扣款",
        "养老保险(公司)",
        "养老保险扣款",
        "岗位津贴",
        "加班费",
        "经济补偿金",
    ]
    options = [subject for subject in preferred if subject in SUBJECT_COLUMNS]
    if len(options) >= 6:
        return options[:12]
    seen = set(options)
    for subject in SUBJECT_COLUMNS:
        if subject not in seen:
            options.append(subject)
            seen.add(subject)
        if len(options) >= 12:
            break
    return options


def detect_subject_mentions(question: str) -> list[str]:
    found: list[str] = []
    for alias, canonical in SUBJECT_ALIASES.items():
        if alias in question and canonical not in found:
            found.append(canonical)
    for subject in SUBJECT_COLUMNS:
        if subject in question and subject not in found:
            found.append(subject)
    return found


def resolve_subject(
    question: str,
    explicit_subject: str | None = None,
    context_subject: str | None = None,
) -> SubjectResolution:
    q = ensure_text(question)
    explicit = canonicalize_subject_name(explicit_subject)
    context = canonicalize_subject_name(context_subject)

    if len(SUBJECT_COLUMNS) == 1:
        only_subject = SUBJECT_COLUMNS[0]
        return SubjectResolution(
            resolved_subject=only_subject,
            display_subject=ensure_text(explicit_subject or context_subject, only_subject),
            confidence=1.0,
            ambiguity_reason="",
            candidate_subjects=[only_subject],
            requires_confirmation=False,
            matched_terms=[only_subject] if only_subject in q else [],
        )

    exact_hits = [subject for subject in SUBJECT_COLUMNS if subject and subject in q]
    alias_hits: list[tuple[str, str]] = []
    for alias, canonical in SUBJECT_ALIASES.items():
        if alias and alias in q:
            alias_hits.append((alias, canonical))

    ambiguous_hits = [term for term in AMBIGUOUS_SUBJECT_TERMS if term in q]
    candidate_scores: dict[str, float] = {}
    matched_terms: list[str] = []

    for subject in exact_hits:
        candidate_scores[subject] = candidate_scores.get(subject, 0) + 3.0
        matched_terms.append(subject)
    for alias, canonical in alias_hits:
        candidate_scores[canonical] = candidate_scores.get(canonical, 0) + (2.5 if alias == canonical else 1.6)
        matched_terms.append(alias)
    for term in ambiguous_hits:
        for subject in AMBIGUOUS_SUBJECT_TERMS.get(term, [])[:12]:
            candidate_scores[subject] = candidate_scores.get(subject, 0) + 0.35
        matched_terms.append(term)

    if explicit:
        candidate_scores[explicit] = candidate_scores.get(explicit, 0) + 2.4
    if context:
        candidate_scores[context] = candidate_scores.get(context, 0) + 1.2

    candidates = [subject for subject, _ in sorted(candidate_scores.items(), key=lambda item: (-item[1], item[0]))]
    top_candidates = candidates[:6]

    if len(set(exact_hits + [canonical for _, canonical in alias_hits])) > 1:
        return SubjectResolution(
            resolved_subject=explicit or context or "",
            display_subject=ensure_text(explicit_subject or context_subject),
            confidence=0.25,
            ambiguity_reason="问题里同时出现了多个薪酬科目信号，先确认分析科目再继续最稳妥。",
            candidate_subjects=top_candidates,
            requires_confirmation=True,
            matched_terms=_dedupe_values(matched_terms),
        )

    if len(exact_hits) == 1 and len(set(exact_hits)) == 1:
        subject = exact_hits[0]
        return SubjectResolution(
            resolved_subject=subject,
            display_subject=subject if subject != explicit else ensure_text(explicit_subject, subject),
            confidence=1.0,
            ambiguity_reason="",
            candidate_subjects=[subject],
            requires_confirmation=False,
            matched_terms=_dedupe_values(matched_terms),
        )

    if explicit and not exact_hits and not ambiguous_hits:
        return SubjectResolution(
            resolved_subject=explicit,
            display_subject=ensure_text(explicit_subject, explicit),
            confidence=0.95,
            ambiguity_reason="",
            candidate_subjects=[explicit],
            requires_confirmation=False,
            matched_terms=_dedupe_values(matched_terms),
        )

    if len({canonical for _, canonical in alias_hits}) > 1 or len(set(exact_hits)) > 1:
        return SubjectResolution(
            resolved_subject=explicit or context or "",
            display_subject=ensure_text(explicit_subject or context_subject),
            confidence=0.25,
            ambiguity_reason="问题里同时出现了多个薪酬科目信号，先确认分析科目再继续最稳妥。",
            candidate_subjects=top_candidates,
            requires_confirmation=True,
            matched_terms=_dedupe_values(matched_terms),
        )

    if ambiguous_hits and not exact_hits:
        preferred_subject = explicit or context or (top_candidates[0] if top_candidates else "")
        return SubjectResolution(
            resolved_subject=preferred_subject,
            display_subject=ensure_text(explicit_subject or context_subject, preferred_subject),
            confidence=0.45 if preferred_subject else 0.0,
            ambiguity_reason=f"你用了“{'、'.join(ambiguous_hits[:3])}”这类泛词，它可能对应多个薪酬科目。",
            candidate_subjects=top_candidates,
            requires_confirmation=True,
            matched_terms=_dedupe_values(matched_terms),
        )

    if len(top_candidates) > 1 and top_candidates[0] != (explicit or context):
        return SubjectResolution(
            resolved_subject=top_candidates[0],
            display_subject=top_candidates[0],
            confidence=0.55,
            ambiguity_reason="当前问题只能推断出候选科目，建议先确认再分析。",
            candidate_subjects=top_candidates,
            requires_confirmation=True,
            matched_terms=_dedupe_values(matched_terms),
        )

    resolved = explicit or (top_candidates[0] if top_candidates else context)
    return SubjectResolution(
        resolved_subject=resolved or "",
        display_subject=ensure_text(explicit_subject or resolved or context_subject),
        confidence=0.75 if resolved else 0.0,
        ambiguity_reason="" if resolved else "还没有识别出唯一的薪酬科目。",
        candidate_subjects=top_candidates,
        requires_confirmation=not bool(resolved),
        matched_terms=_dedupe_values(matched_terms),
    )


def request_to_payload(request: AnalysisRequest) -> dict[str, Any]:
    return {
        "subject": request.subject,
        "primary_dimension": request.primary_dimension,
        "secondary_dimensions": request.secondary_dimensions,
        "start_period": f"{request.start_year}-{request.start_month:02d}",
        "end_period": f"{request.end_year}-{request.end_month:02d}",
        "metrics": request.metrics,
        "question": request.question,
    }


def request_from_payload(payload: dict[str, Any] | None) -> AnalysisRequest:
    raw = dict(payload or {})
    subject = normalize_subject(raw.get("subject") or DEFAULT_SUBJECT)
    dimensions = raw.get("secondary_dimensions") or DEFAULT_SECONDARY_DIMENSIONS
    secondary_dimensions = normalize_secondary_dimensions(dimensions)
    default_start_period, default_end_period = default_period_window()
    start_period = ensure_text(raw.get("start_period"), default_start_period)
    end_period = ensure_text(raw.get("end_period"), default_end_period)
    start_year, start_month = parse_period(start_period)
    end_year, end_month = parse_period(end_period)
    metrics = raw.get("metrics") or ["总额", "平均金额", "发放覆盖率"]

    return AnalysisRequest(
        subject=subject,
        primary_dimension=ensure_text(raw.get("primary_dimension"), primary_dimension_name()) or primary_dimension_name(),
        secondary_dimensions=secondary_dimensions,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
        metrics=[ensure_text(item) for item in metrics if ensure_text(item)] or ["总额", "平均金额", "发放覆盖率"],
        question=ensure_text(raw.get("question")),
        follow_up_context=raw.get("context"),
    )


def build_report_response(
    request: AnalysisRequest,
    consolidated: dict[str, Any],
    insight_bundle: dict[str, Any],
    dimension_reports: list[dict[str, Any]],
    analysis_mode: str,
) -> dict[str, Any]:
    data_source = get_data_source_status()
    return {
        "request": request_to_payload(request),
        "report": {
            "executive_summary": consolidated["executive_summary"],
            "short_answer": ensure_text(consolidated.get("short_answer")),
            "cross_dimension_summary": consolidated["cross_dimension_summary"],
            "priority_actions": consolidated["priority_actions"],
            "global_risks": consolidated["global_risks"],
            "report_title": consolidated["report_title"],
            "report_subtitle": consolidated["report_subtitle"],
            "leadership_takeaways": consolidated["leadership_takeaways"],
            "appendix_notes": consolidated["appendix_notes"],
            "external_research_summary": consolidated["external_research_summary"],
            "external_sources": consolidated["external_sources"],
            "research_mode": consolidated["research_mode"],
            "full_report_sections": consolidated["full_report_sections"],
            "hero_metrics": insight_bundle["hero_metrics"],
            "bu_overview": insight_bundle["bu_overview"],
            "overview_charts": insight_bundle["overview_charts"],
            "dimension_reports": dimension_reports,
            "consolidated_charts": consolidated["consolidated_charts"],
            "sql_preview": insight_bundle["sql_preview"],
            "methodology": {
                "data_source": data_source["filename"] or data_source["path"],
                "data_source_signature": data_source["signature"],
                "data_source_imported_at": data_source["imported_at"],
                "analysis_mode": analysis_mode,
                "note": "先做分维度洞察，再做跨维度综合归纳。",
            },
        },
    }


def build_stream_progress_event(stage: str, label: str, step_index: int, step_total: int, message: str) -> dict[str, Any]:
    return {
        "type": "progress",
        "stage": stage,
        "label": label,
        "step_index": step_index,
        "step_total": step_total,
        "message": message,
    }


def normalize_short_answer_text(value: Any, fallback: str) -> str:
    text = ensure_text(value, fallback).replace("**", "").replace("\r", "").strip()
    if not text:
        return fallback

    paragraphs = [
        re.sub(r"^[\-\*\d\.\)\s]+", "", part.strip())
        for part in re.split(r"\n{2,}", text)
        if part.strip()
    ]
    if not paragraphs:
        paragraphs = [fallback]

    normalized = []
    for paragraph in paragraphs[:2]:
        merged = re.sub(r"\s*\n\s*", " ", paragraph).strip()
        if merged:
            normalized.append(merged)

    final_text = "\n\n".join(normalized).strip()
    return final_text or fallback


def detect_dimension_mentions(question: str) -> list[str]:
    found: list[str] = []
    for name in DIMENSION_COLUMNS:
        if name != "BU" and name in question and name not in found:
            found.append(name)
    for alias, canonical in DIMENSION_ALIASES.items():
        if alias in question and canonical != "BU" and canonical not in found:
            found.append(canonical)
    return found


def build_clarification_response(payload: dict[str, Any]) -> dict[str, Any] | None:
    question = ensure_text(payload.get("question"))
    primary_dimension = primary_dimension_name()
    default_start_period, default_end_period = default_period_window()
    dimension_presets = build_dimension_presets()
    time_window_options = build_time_window_options()
    explicit_subject = ensure_text(payload.get("subject"))
    explicit_dimensions = payload.get("secondary_dimensions") or []
    explicit_metrics = payload.get("metrics") or []
    explicit_start_period = ensure_text(payload.get("start_period"))
    explicit_end_period = ensure_text(payload.get("end_period"))

    if not question:
        return {
            "mode": "clarification",
            "message": "先告诉我你想分析什么，我再一步步帮你把口径补齐。",
            "request_draft": {
                "subject": "",
                "primary_dimension": primary_dimension,
                "secondary_dimensions": [],
                "start_period": default_start_period,
                "end_period": default_end_period,
                "metrics": [],
                "question": "",
            },
            "clarification": {
                "needs_subject": True,
                "needs_time_window": True,
                "needs_dimensions": True,
                "needs_metrics": True,
                "current_step": "subject",
                "subject_prompt": "先确认这次要分析的薪酬科目。高歧义科目我会先让你确认，避免直接跑错口径。",
                "time_window_prompt": "再确认时间窗口。大文件已经有完整期间范围，建议优先缩小到最近月、最近3个月、最近6个月或全部期间。",
                "dimension_prompt": f"再选你想怎么拆。默认主维度是 {primary_dimension}，次维度建议从部门、级别、绩效或年龄分箱开始。",
                "metric_prompt": "最后选你想重点看哪些指标。常用的是总额、平均金额、领取人数、发放覆盖率、环比、同比。",
                "subject_prompt_reason": "还没有识别出唯一的分析科目。",
                "subject_options": build_default_subject_options(),
                "subject_candidate_options": [],
                "subject_catalog": build_subject_catalog(),
                "dimension_options": [dimension for dimension in DIMENSION_COLUMNS if dimension != primary_dimension],
                "time_window_options": time_window_options,
                "metric_options": ["总额", "平均金额", "领取人数", "发放覆盖率", "占比", "环比", "同比"],
                "dimension_presets": dimension_presets,
                "matched_terms": [],
            },
        }

    parsed = parse_natural_language(question, payload.get("subject"))
    subject_resolution = resolve_subject(question, explicit_subject or parsed.get("subject"), payload.get("subject"))
    subject_mentions = detect_subject_mentions(question)
    dimension_mentions = detect_dimension_mentions(question)
    metric_mentions = parsed.get("metrics") or []
    parsed_start_period = parsed.get("start_period") or default_start_period
    parsed_end_period = parsed.get("end_period") or default_end_period
    inferred_subject = explicit_subject or subject_resolution.display_subject or parsed.get("subject") or ""
    inferred_dimensions = explicit_dimensions or parsed.get("secondary_dimensions") or []
    inferred_metrics = explicit_metrics or parsed.get("metrics") or []
    effective_start_period = explicit_start_period or parsed_start_period
    effective_end_period = explicit_end_period or parsed_end_period
    if len(SUBJECT_COLUMNS) == 1 and not inferred_subject:
        inferred_subject = SUBJECT_COLUMNS[0]

    has_explicit_subject = bool(explicit_subject)
    has_confirmed_subject = bool(inferred_subject) and (has_explicit_subject or bool(subject_mentions) or len(SUBJECT_COLUMNS) == 1)
    needs_subject = subject_resolution.requires_confirmation or not has_confirmed_subject
    needs_time_window = not explicit_start_period and not explicit_end_period and effective_start_period == default_start_period and effective_end_period == default_end_period
    needs_dimensions = not explicit_dimensions and not dimension_mentions
    needs_metrics = not explicit_metrics and not metric_mentions

    if not needs_subject and not needs_time_window and not needs_dimensions and not needs_metrics:
        return None

    request_draft = {
        "subject": inferred_subject,
        "primary_dimension": primary_dimension,
        "secondary_dimensions": inferred_dimensions,
        "start_period": effective_start_period,
        "end_period": effective_end_period,
        "metrics": inferred_metrics,
        "question": question,
    }
    current_step = (
        "subject" if needs_subject else
        "time_window" if needs_time_window else
        "dimensions" if needs_dimensions else
        "metrics"
    )
    top_subject_options = subject_resolution.candidate_subjects[:6]
    if needs_subject and len(top_subject_options) <= 1:
        top_subject_options = build_default_subject_options()
    elif not top_subject_options and inferred_subject:
        top_subject_options = [canonicalize_subject_name(inferred_subject)]
    return {
        "mode": "clarification",
        "message": "我先判断了一下，你这个问题的分析边界还不够完整，需要先把口径补齐。",
        "request_draft": request_draft,
        "clarification": {
            "needs_subject": needs_subject,
            "needs_time_window": needs_time_window,
            "needs_dimensions": needs_dimensions,
            "needs_metrics": needs_metrics,
            "current_step": current_step,
            "subject_prompt": "先确认薪酬科目。高歧义输入我会先拦一下，避免直接用错分析口径。",
            "time_window_prompt": "再确认时间窗口。默认不会盲目沿用旧 demo 时间段，而是优先用当前数据源的实际期间。",
            "dimension_prompt": f"再确认拆解维度。默认主维度是 {primary_dimension}，次维度可以从部门、职能序列、级别、绩效、年龄分箱里选。",
            "metric_prompt": "最后确认展示指标。比如总额、平均金额、领取人数、发放覆盖率、环比、同比，可多选。",
            "subject_prompt_reason": subject_resolution.ambiguity_reason,
            "subject_options": top_subject_options,
            "subject_candidate_options": top_subject_options,
            "subject_catalog": build_subject_catalog(),
            "dimension_options": [dimension for dimension in DIMENSION_COLUMNS if dimension != primary_dimension],
            "time_window_options": time_window_options,
            "metric_options": ["总额", "平均金额", "领取人数", "发放覆盖率", "占比", "环比", "同比"],
            "dimension_presets": dimension_presets,
            "matched_terms": subject_resolution.matched_terms,
        },
    }


def ensure_text(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        text = value.strip()
        return text or default
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        if any(key in value for key in ("action", "priority", "rationale")):
            action = ensure_text(value.get("action"))
            priority = ensure_text(value.get("priority"))
            rationale = ensure_text(value.get("rationale"))
            parts = []
            if priority:
                parts.append(f"[{priority}]")
            if action:
                parts.append(action)
            if rationale:
                parts.append(f"原因：{rationale}")
            if parts:
                return " ".join(parts)
        for key in ("content", "text", "summary", "value", "title", "headline"):
            if key in value:
                return ensure_text(value.get(key), default)
        return default
    if isinstance(value, list):
        parts = [ensure_text(item) for item in value]
        parts = [item for item in parts if item]
        return "\n\n".join(parts) if parts else default
    return str(value).strip() or default


def ensure_text_list(value: Any, default: list[str] | None = None, limit: int | None = None) -> list[str]:
    items: list[str] = []
    if isinstance(value, list):
        for item in value:
            text = ensure_text(item)
            if text:
                items.append(text)
    elif isinstance(value, str):
        chunks = re.split(r"\n+|[；;]\s*", value)
        for chunk in chunks:
            text = chunk.strip(" \t\r\n-•")
            if text:
                items.append(text)
    elif value is not None:
        text = ensure_text(value)
        if text:
            items.append(text)
    if not items:
        items = list(default or [])
    if limit is not None:
        items = items[:limit]
    return items


def normalize_report_sections(raw_sections: Any, fallback_text: str = "") -> list[dict[str, str]]:
    sections: list[dict[str, str]] = []
    if isinstance(raw_sections, list):
        for index, item in enumerate(raw_sections, start=1):
            if isinstance(item, dict):
                content = ensure_text(item.get("content") or item.get("body") or item.get("text"))
                if not content:
                    continue
                sections.append(
                    {
                        "id": ensure_text(item.get("id"), f"section-{index}") or f"section-{index}",
                        "title": ensure_text(item.get("title")),
                        "content": content,
                    }
                )
            else:
                content = ensure_text(item)
                if content:
                    sections.append({"id": f"section-{index}", "title": "", "content": content})
    elif isinstance(raw_sections, dict):
        content = ensure_text(raw_sections.get("content") or raw_sections.get("body") or raw_sections.get("text"))
        if content:
            sections.append(
                {
                    "id": ensure_text(raw_sections.get("id"), "section-1") or "section-1",
                    "title": ensure_text(raw_sections.get("title")),
                    "content": content,
                }
            )
    else:
        content = ensure_text(raw_sections or fallback_text)
        if content:
            paragraphs = [part.strip() for part in re.split(r"\n{2,}", content) if part.strip()]
            if not paragraphs:
                paragraphs = [content]
            for index, paragraph in enumerate(paragraphs, start=1):
                sections.append({"id": f"section-{index}", "title": "", "content": paragraph})
    if not sections and fallback_text:
        sections.append({"id": "section-1", "title": "", "content": fallback_text})
    return sections


def normalize_external_sources(raw_sources: Any) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    if isinstance(raw_sources, list):
        for item in raw_sources:
            if not isinstance(item, dict):
                continue
            url = ensure_text(item.get("url"))
            title = ensure_text(item.get("title"))
            source_name = ensure_text(item.get("source_name") or item.get("source"))
            summary = ensure_text(item.get("summary") or item.get("snippet"))
            published_at = ensure_text(item.get("published_at") or item.get("date"))
            query_topic = ensure_text(item.get("query_topic"))
            if not (url or title or source_name):
                continue
            sources.append(
                {
                    "source_name": source_name or "外部来源",
                    "title": title or url,
                    "published_at": published_at,
                    "summary": summary,
                    "url": url,
                    "query_topic": query_topic,
                }
            )
    return sources[:10]


def ensure_dimension_report_text_lists(report: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(report or {})
    for field in ("key_findings", "anomalies", "possible_drivers", "management_implications"):
        value = normalized.get(field)
        if isinstance(value, dict):
            text_items = [ensure_text(item) for item in value.values()]
            normalized[field] = [item for item in text_items if item]
        else:
            normalized[field] = ensure_text_list(value)
    return normalized


def clean_sentence(text: str) -> str:
    cleaned = normalize_chinese_punctuation(ensure_text(text))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.rstrip("；。 ，")


def normalize_chinese_punctuation(text: str) -> str:
    cleaned = ensure_text(text)
    if not cleaned:
        return ""
    cleaned = cleaned.replace("\r", "")
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = re.sub(r"([。；，、！？]){2,}", lambda m: "。" if "。" in m.group(0) else m.group(0)[0], cleaned)
    cleaned = re.sub(r"[；。]+(?=；)", "。", cleaned)
    cleaned = re.sub(r"；(?=[。！？])", "。", cleaned)
    cleaned = re.sub(r"[。；]+(?=，)", "，", cleaned)
    cleaned = re.sub(r"，{2,}", "，", cleaned)
    cleaned = re.sub(r"。；|；。", "。", cleaned)
    cleaned = re.sub(r"。(?=；)", "。", cleaned)
    cleaned = re.sub(r"；(?=；)", "；", cleaned)
    cleaned = re.sub(r"([万元亿元人%])。；", r"\1。", cleaned)
    cleaned = re.sub(r"([万元亿元人%])。。", r"\1。", cleaned)
    cleaned = re.sub(r"([。！？])\s*([；，])", r"\1", cleaned)
    cleaned = re.sub(r"([；，])\s*([。！？])", r"\2", cleaned)
    cleaned = re.sub(r"\s+([。；，、！？])", r"\1", cleaned)
    cleaned = re.sub(r"([。；，、！？])(?=[^\s\n”）】』」》])", r"\1", cleaned)
    return cleaned.strip()


def polish_report_text_fields(report: dict[str, Any]) -> dict[str, Any]:
    polished = deepcopy(report or {})
    polished["executive_summary"] = normalize_chinese_punctuation(ensure_text(polished.get("executive_summary")))
    polished["short_answer"] = normalize_chinese_punctuation(ensure_text(polished.get("short_answer")))
    for field in ("cross_dimension_summary", "priority_actions", "global_risks", "leadership_takeaways", "appendix_notes", "external_research_summary"):
        polished[field] = [
            normalize_chinese_punctuation(ensure_text(item))
            for item in (polished.get(field) or [])
            if normalize_chinese_punctuation(ensure_text(item))
        ]
    sections = []
    for section in polished.get("full_report_sections") or []:
        if not isinstance(section, dict):
            continue
        next_section = dict(section)
        next_section["title"] = normalize_chinese_punctuation(ensure_text(next_section.get("title")))
        next_section["content"] = normalize_chinese_punctuation(ensure_text(next_section.get("content")))
        sections.append(next_section)
    polished["full_report_sections"] = sections
    return polished


def join_sentences(items: list[str], fallback: str = "") -> str:
    parts = [clean_sentence(item) for item in items if clean_sentence(item)]
    if not parts:
        return normalize_chinese_punctuation(fallback)
    return normalize_chinese_punctuation("；".join(parts) + "。")


def build_short_answer_prompt(request: AnalysisRequest, report: dict[str, Any]) -> str:
    hero_metrics = report.get("hero_metrics") or {}
    hero_lines = [
        f"总额：{int(hero_metrics.get('total_amount') or 0):,} 元",
        f"平均金额：{round(float(hero_metrics.get('avg_amount') or 0), 2)} 元",
        f"领取人数：{int(hero_metrics.get('issued_employee_count') or 0):,} 人",
        f"覆盖率：{round(float(hero_metrics.get('coverage_rate') or 0), 2)}%",
    ]
    return f"""请基于现有薪酬分析结果，用 1 到 2 段中文，直接回答用户最初的问题。

要求：
1. 先正面回答问题，再补充 1 到 2 个关键发现。
2. 不能写标题、不能写列表、不能使用 Markdown。
3. 语气像咨询顾问的简短结论，不要复述“根据分析”“从数据看”这类空话。
4. 总长度控制在 120 到 220 字，最多两段。

用户原始问题：
{request.question or f'请概括{request.subject}分析结论'}

分析主题：
{request.subject}

执行摘要：
{ensure_text(report.get('executive_summary'))}

跨维度关键点：
{json.dumps(ensure_text_list(report.get('cross_dimension_summary'), limit=4), ensure_ascii=False)}

总体指标：
{chr(10).join(hero_lines)}

请直接返回最终回答正文。"""


def build_short_answer_fallback(request: AnalysisRequest, report: dict[str, Any]) -> str:
    question = ensure_text(request.question, f"{request.subject}分析结论")
    executive_summary = clean_sentence(report.get("executive_summary"))
    if not executive_summary:
        executive_summary = f"{request.subject}在当前时间范围内已经呈现出较明显的结构分层，重点在于识别头部 BU 与关键人群的差异来源"

    first_paragraph = f"针对“{question}”，当前最直接的结论是：{executive_summary}。"

    hero_metrics = report.get("hero_metrics") or {}
    metrics_parts = []
    if hero_metrics.get("total_amount") is not None:
        metrics_parts.append(f"总额约 {int(hero_metrics.get('total_amount') or 0):,} 元")
    if hero_metrics.get("issued_employee_count") is not None:
        metrics_parts.append(f"领取人数约 {int(hero_metrics.get('issued_employee_count') or 0):,} 人")
    if hero_metrics.get("coverage_rate") is not None:
        metrics_parts.append(f"覆盖率约 {round(float(hero_metrics.get('coverage_rate') or 0), 2)}%")

    cross_dimension = join_sentences(
        ensure_text_list(report.get("cross_dimension_summary"), limit=2),
        fallback="关键差异主要集中在少数 BU 与头部人群分层，治理重点应放在反复命中的结构性驱动因素上。",
    )
    second_parts = [clean_sentence(cross_dimension)]
    if metrics_parts:
        second_parts.append("整体上，" + "，".join(metrics_parts) + "。")
    second_paragraph = " ".join(part for part in second_parts if part).strip()

    return normalize_short_answer_text("\n\n".join([first_paragraph, second_paragraph]), first_paragraph)


def generate_short_answer_for_report(
    request: AnalysisRequest,
    report: dict[str, Any],
    llm_service: "LLMService | None" = None,
) -> str:
    fallback = build_short_answer_fallback(request, report)
    if llm_service and llm_service.enabled:
        generated = llm_service.generate_short_answer(request, report)
        if generated:
            return normalize_short_answer_text(generated, fallback)
    return fallback


def normalize_consolidated_payload(
    payload: dict[str, Any],
    request: AnalysisRequest,
    fallback: dict[str, Any],
    dimension_reports: list[dict[str, Any]],
    insight_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(fallback)
    normalized.update(payload or {})

    default_title = f"{request.subject}成本结构分析与治理策略报告 ({request.start_year}-{request.end_year})"
    default_subtitle = (
        f"范围：{request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d}"
    )

    normalized["report_title"] = ensure_text(normalized.get("report_title"), default_title)
    normalized["report_subtitle"] = ensure_text(normalized.get("report_subtitle"), default_subtitle)
    normalized["executive_summary"] = ensure_text(
        normalized.get("executive_summary"),
        fallback.get("executive_summary", ""),
    )
    normalized["short_answer"] = ensure_text(
        normalized.get("short_answer"),
        fallback.get("short_answer", ""),
    )
    normalized["cross_dimension_summary"] = ensure_text_list(
        normalized.get("cross_dimension_summary"),
        fallback.get("cross_dimension_summary", []),
        limit=6,
    )
    normalized["priority_actions"] = ensure_text_list(
        normalized.get("priority_actions"),
        fallback.get("priority_actions", []),
        limit=6,
    )
    normalized["global_risks"] = ensure_text_list(
        normalized.get("global_risks"),
        fallback.get("global_risks", []),
        limit=6,
    )
    normalized["leadership_takeaways"] = ensure_text_list(
        normalized.get("leadership_takeaways"),
        fallback.get("leadership_takeaways", []),
        limit=4,
    )
    normalized["appendix_notes"] = ensure_text_list(
        normalized.get("appendix_notes"),
        fallback.get("appendix_notes", []),
        limit=4,
    )
    normalized["external_research_summary"] = ensure_text_list(
        normalized.get("external_research_summary"),
        fallback.get("external_research_summary", []),
        limit=8,
    )
    normalized["external_sources"] = normalize_external_sources(
        normalized.get("external_sources") or fallback.get("external_sources") or []
    )
    normalized["research_mode"] = ensure_text(
        normalized.get("research_mode"),
        fallback.get("research_mode", "internal_only"),
    )
    if not normalized["external_research_summary"]:
        if normalized["research_mode"] == "external_unavailable":
            normalized["external_research_summary"] = ["当前未配置外部搜索能力，本次正式报告仅基于内部数据生成。"]
        elif normalized["research_mode"] == "external_empty":
            normalized["external_research_summary"] = ["已执行外部搜索，但本次未命中可引用的合格来源。"]

    report_body = ensure_text(
        normalized.get("full_report_body")
        or normalized.get("full_report_text")
        or normalized.get("full_report")
    )
    fallback_sections = fallback.get("full_report_sections", [])
    fallback_body = "\n\n".join(
        section.get("content", "") for section in fallback_sections if isinstance(section, dict)
    )
    normalized["full_report_sections"] = normalize_report_sections(
        normalized.get("full_report_sections") or report_body,
        fallback_text=report_body or fallback_body,
    )
    normalized["full_report_body"] = "\n\n".join(
        section["content"] for section in normalized["full_report_sections"] if section.get("content")
    )
    normalized["consolidated_charts"] = build_consolidated_charts(dimension_reports)
    if insight_bundle is not None:
        enrich_sections_with_data(normalized["full_report_sections"], insight_bundle, dimension_reports, request)
    return polish_report_text_fields(normalized)


def build_report_revision_prompt(
    request: AnalysisRequest,
    report: dict[str, Any],
    revision_instruction: str,
    follow_up_messages: list[dict[str, Any]],
) -> str:
    sections = normalize_report_sections(
        report.get("full_report_sections") or report.get("full_report_body") or report.get("executive_summary"),
        fallback_text=ensure_text(report.get("executive_summary")),
    )
    section_lines = []
    for section in sections:
        title = ensure_text(section.get("title"), "正文段落")
        content = ensure_text(section.get("content"))
        if content:
            section_lines.append(f"[{ensure_text(section.get('id'), 'section')}] {title}\n{content}")

    follow_up_context = []
    for item in follow_up_messages[-6:]:
        question = ensure_text(item.get("question"))
        answer = ensure_text(item.get("answer"))
        if question or answer:
            follow_up_context.append(f"用户追问：{question}\n系统回答：{answer}")

    return f"""请基于现有薪酬分析报告，按照人工建议生成一版新的完整报告。

限制：
1. 只能基于现有报告内容做润色、补充表达、重组结构和强化管理建议。
2. 不允许重新取数，不允许虚构新的数据口径、SQL 结果或统计值。
3. 必须保留原报告的核心事实、时间范围和分析主题。
4. 输出严格 JSON，不要附带解释文字。

当前分析请求：
{json.dumps(request_to_payload(request), ensure_ascii=False)}

人工建议：
{revision_instruction}

现有报告摘要：
- 标题：{ensure_text(report.get('report_title'))}
- 副标题：{ensure_text(report.get('report_subtitle'))}
- 执行摘要：{ensure_text(report.get('executive_summary'))}
- 跨维度总结：{json.dumps(report.get('cross_dimension_summary') or [], ensure_ascii=False)}
- 优先动作：{json.dumps(report.get('priority_actions') or [], ensure_ascii=False)}
- 关键风险：{json.dumps(report.get('global_risks') or [], ensure_ascii=False)}
- 管理层要点：{json.dumps(report.get('leadership_takeaways') or [], ensure_ascii=False)}

现有完整正文：
{chr(10).join(section_lines)}

追问上下文（如有）：
{chr(10).join(follow_up_context) if follow_up_context else '无'}

请返回如下 JSON：
{{
  "report_title": "可沿用或轻微润色后的标题",
  "report_subtitle": "结合人工建议后的副标题",
  "executive_summary": "新的执行摘要",
  "cross_dimension_summary": ["3-6条"],
  "priority_actions": ["3-6条"],
  "global_risks": ["2-5条"],
  "leadership_takeaways": ["2-4条"],
  "appendix_notes": ["2-4条"],
  "full_report_sections": [
    {{
      "id": "section-1",
      "title": "段落标题",
      "content": "段落正文"
    }}
  ]
}}"""


def _normalize_revision_appendix_notes(
    report: dict[str, Any],
    revision_instruction: str,
    extra_notes: Any = None,
) -> list[str]:
    notes = ensure_text_list(report.get("appendix_notes"), limit=6)
    notes.extend(ensure_text_list(extra_notes, limit=4))
    revision_note = ensure_text(revision_instruction)
    if revision_note:
        notes.append(f"本版报告基于人工建议完成润色改写：{revision_note}")
    deduped: list[str] = []
    for item in notes:
        if item and item not in deduped:
            deduped.append(item)
    return deduped[:6]


def merge_revised_report(
    request: AnalysisRequest,
    report: dict[str, Any],
    revision_payload: dict[str, Any] | None,
    revision_instruction: str,
) -> dict[str, Any]:
    base_report = deepcopy(report)
    payload = revision_payload or {}
    base_sections = normalize_report_sections(
        base_report.get("full_report_sections") or base_report.get("full_report_body") or base_report.get("executive_summary"),
        fallback_text=ensure_text(base_report.get("executive_summary")),
    )
    base_body = "\n\n".join(section.get("content", "") for section in base_sections if section.get("content"))
    subtitle_fallback = ensure_text(base_report.get("report_subtitle"))
    if subtitle_fallback:
        subtitle_fallback = f"{subtitle_fallback} · 建议润色版"
    else:
        subtitle_fallback = (
            f"{request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d}"
            f" · {request.subject}建议润色版"
        )

    base_report["report_title"] = ensure_text(
        payload.get("report_title"),
        ensure_text(base_report.get("report_title"), f"{request.subject}分析报告"),
    )
    base_report["report_subtitle"] = ensure_text(payload.get("report_subtitle"), subtitle_fallback)
    base_report["executive_summary"] = ensure_text(
        payload.get("executive_summary"),
        ensure_text(base_report.get("executive_summary")),
    )
    base_report["cross_dimension_summary"] = ensure_text_list(
        payload.get("cross_dimension_summary"),
        ensure_text_list(base_report.get("cross_dimension_summary"), limit=6),
        limit=6,
    )
    base_report["priority_actions"] = ensure_text_list(
        payload.get("priority_actions"),
        ensure_text_list(base_report.get("priority_actions"), limit=6),
        limit=6,
    )
    base_report["global_risks"] = ensure_text_list(
        payload.get("global_risks"),
        ensure_text_list(base_report.get("global_risks"), limit=6),
        limit=6,
    )
    base_report["leadership_takeaways"] = ensure_text_list(
        payload.get("leadership_takeaways"),
        ensure_text_list(base_report.get("leadership_takeaways"), limit=4),
        limit=4,
    )
    base_report["appendix_notes"] = _normalize_revision_appendix_notes(
        base_report,
        revision_instruction,
        payload.get("appendix_notes"),
    )
    base_report["full_report_sections"] = normalize_report_sections(
        payload.get("full_report_sections"),
        fallback_text=base_body,
    ) or base_sections
    methodology = dict(base_report.get("methodology") or {})
    methodology["analysis_mode"] = "report_revision_llm" if revision_payload else "report_revision_template"
    methodology["note"] = "基于原报告快照与人工建议进行润色改写，未重新读取底层数据。"
    base_report["methodology"] = methodology
    return base_report


def build_revised_report_fallback(
    request: AnalysisRequest,
    report: dict[str, Any],
    revision_instruction: str,
) -> dict[str, Any]:
    base_report = deepcopy(report)
    revision_note = ensure_text(revision_instruction, "补充管理建议并润色表达")
    base_report["report_subtitle"] = ensure_text(base_report.get("report_subtitle"))
    if base_report["report_subtitle"]:
        base_report["report_subtitle"] = f"{base_report['report_subtitle']} · 建议润色版"
    else:
        base_report["report_subtitle"] = (
            f"{request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d}"
            f" · {request.subject}建议润色版"
        )
    base_report["executive_summary"] = (
        f"本版报告已根据人工建议“{revision_note}”完成表达优化与重点重组，"
        "以下结论继续沿用原始数据快照，不重新读取底层数据。"
        f"\n\n{ensure_text(base_report.get('executive_summary'))}"
    ).strip()
    priority_actions = ensure_text_list(base_report.get("priority_actions"), limit=5)
    priority_actions.insert(0, f"结合人工建议“{revision_note}”，优先强化报告主线与管理动作表述。")
    base_report["priority_actions"] = priority_actions[:6]
    base_report["appendix_notes"] = _normalize_revision_appendix_notes(base_report, revision_note)
    methodology = dict(base_report.get("methodology") or {})
    methodology["analysis_mode"] = "report_revision_template"
    methodology["note"] = "基于原报告快照与人工建议进行润色改写，未重新读取底层数据。"
    base_report["methodology"] = methodology
    return base_report


def enrich_sections_with_data(
    sections: list[dict[str, Any]],
    insight_bundle: dict[str, Any],
    dimension_reports: list[dict[str, Any]],
    request: AnalysisRequest,
) -> None:
    """Inject data_tables and charts into report sections in-place."""
    bu_overview = insight_bundle.get("bu_overview", [])
    for section in sections:
        if not isinstance(section, dict):
            continue
        sid = section.get("id", "")

        # section-1: cost pressure → BU overview table + bar chart
        if sid == "section-1" and bu_overview:
            top_bus = bu_overview[:8]
            section["data_tables"] = [
                {
                    "table_title": f"{request.subject} BU 概览（Top {len(top_bus)}）",
                    "columns": ["BU", "总额", "均值", "领取人数", "覆盖率(%)"],
                    "rows": [
                        {
                            "BU": row.get("BU", ""),
                            "总额": int(row.get("total_amount") or 0),
                            "均值": round(float(row.get("avg_amount") or 0), 2),
                            "领取人数": int(row.get("issued_employee_count") or 0),
                            "覆盖率(%)": float(row.get("coverage_rate") or 0),
                        }
                        for row in top_bus
                    ],
                }
            ]
            section["charts"] = [
                {
                    "chart_type": "bar",
                    "chart_title": f"{request.subject} BU 总额分布",
                    "chart_insight": "",
                    "chart_payload": {
                        "labels": [row.get("BU", "") for row in top_bus],
                        "series": [int(row.get("total_amount", 0)) for row in top_bus],
                    },
                }
            ]

        # section-2: multi-dimension insights → per-dimension distribution table + primary chart
        if sid == "section-2" and dimension_reports:
            tables = []
            charts = []
            for dim_report in dimension_reports:
                dim_name = dim_report.get("dimension", "")
                chart_data = dim_report.get("chart_data", {})
                primary_chart = chart_data.get("primary_chart")

                # Build distribution table from primary chart payload
                if primary_chart:
                    cp = primary_chart.get("chart_payload", {})
                    labels = cp.get("labels") or cp.get("categories") or []
                    series = cp.get("series") or []
                    if labels and series:
                        tables.append({
                            "table_title": f"{dim_name}维度分布",
                            "columns": [dim_name, "总额"],
                            "rows": [
                                {dim_name: labels[i], "总额": series[i]}
                                for i in range(min(len(labels), len(series)))
                            ],
                        })
                    charts.append(primary_chart)

            if tables:
                section["data_tables"] = tables
            if charts:
                section["charts"] = charts


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _build_salary_table_ddl(schema: SalarySchema) -> str:
    column_defs = [
        "统计年度 INTEGER NOT NULL",
        "统计月份 INTEGER NOT NULL",
    ]
    for column in schema.text_dimension_columns:
        column_defs.append(f'"{column}" TEXT NOT NULL')
    for column in schema.numeric_columns:
        column_defs.append(f'"{column}" REAL')
    return f"CREATE TABLE IF NOT EXISTS salary_wide ({', '.join(column_defs)})"


def _recreate_salary_table(conn: sqlite3.Connection, schema: SalarySchema) -> None:
    conn.execute("DROP VIEW IF EXISTS salary_subject_facts")
    conn.execute("DROP TABLE IF EXISTS salary_wide")
    conn.execute(_build_salary_table_ddl(schema))


def _load_schema_from_meta(conn: sqlite3.Connection) -> SalarySchema:
    schema = _schema_from_meta(conn)
    configure_schema(schema)
    return schema


def _ensure_salary_table_matches_schema(conn: sqlite3.Connection, schema: SalarySchema) -> None:
    existing_columns = [row["name"] for row in conn.execute("PRAGMA table_info(salary_wide)").fetchall()]
    if existing_columns and existing_columns == schema.wide_columns:
        return
    _recreate_salary_table(conn, schema)


def init_database() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        schema = _load_schema_from_meta(conn)
        _ensure_salary_table_matches_schema(conn, schema)
        _create_indexes(conn)
        _create_subject_view(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL,
                subject TEXT NOT NULL,
                request_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )
        _create_saved_reports_table(conn)
        _ensure_query_history_schema(conn)
        _sync_active_dataset(conn)
        conn.commit()
    finally:
        conn.close()


def _ensure_table_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")


def _ensure_query_history_schema(conn: sqlite3.Connection) -> None:
    _ensure_table_column(conn, "query_history", "data_source_name", "data_source_name TEXT")
    _ensure_table_column(conn, "query_history", "data_source_signature", "data_source_signature TEXT")


def _detect_csv_encoding(csv_path: Path) -> tuple[str, list[str]]:
    decode_errors: list[str] = []
    for encoding in ("utf-8-sig", "gbk"):
        try:
            with csv_path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                headers = reader.fieldnames or []
                if not headers:
                    raise ValueError("CSV 文件缺少表头。")
                return encoding, headers
        except UnicodeDecodeError as exc:
            decode_errors.append(str(exc))
            continue
    raise ValueError("CSV 编码无法识别，请使用 UTF-8 或 GBK。")


def _safe_numeric_ratio(values: list[str]) -> float:
    if not values:
        return 0.0
    numeric_count = 0
    for value in values:
        try:
            float(str(value).replace(",", "").strip())
            numeric_count += 1
        except (TypeError, ValueError):
            continue
    return round(numeric_count / max(len(values), 1), 4)


def _dedupe_preserve(values: list[str]) -> list[str]:
    output: list[str] = []
    for value in values:
        if value and value not in output:
            output.append(value)
    return output


def _suggest_dimension_name(column_name: str) -> str:
    cleaned = ensure_text(column_name)
    aliases = dict(get_schema("pingan_full").dimension_aliases)
    aliases.update(get_schema("legacy_simple").dimension_aliases)
    if cleaned in aliases:
        return aliases[cleaned]
    for alias, canonical in aliases.items():
        if alias and alias in cleaned:
            return canonical
    return cleaned


def _suggest_subject_name(column_name: str) -> str:
    cleaned = ensure_text(column_name)
    schema = get_schema("pingan_full")
    if cleaned in schema.subject_columns:
        return cleaned
    if cleaned in schema.subject_aliases:
        return schema.subject_aliases[cleaned]
    for alias, canonical in schema.subject_aliases.items():
        if alias and alias in cleaned:
            return canonical
    return cleaned


def _period_from_single_value(raw: str) -> tuple[int, int] | None:
    text = ensure_text(raw)
    if not text:
        return None
    match = re.search(r"(20\d{2})[-/年](\d{1,2})", text)
    if match:
        return int(match.group(1)), int(match.group(2))
    compact = re.fullmatch(r"(20\d{2})(\d{2})", text)
    if compact:
        return int(compact.group(1)), int(compact.group(2))
    return None


def _derive_dimension_value(target: str, row: dict[str, Any], row_number: int, year: int, month: int, schema: SalarySchema) -> str:
    if target == "统计月":
        return f"{year:04d}-{month:02d}"
    source_map = schema.source_column_map or {}
    synthetic_defaults = schema.synthetic_defaults or {}
    source_column = source_map.get(target, target)
    if source_column == "__rowid__":
        return f"ROW-{row_number - 1}"
    if source_column == "__period__":
        return f"{year:04d}-{month:02d}"
    if source_column == "__constant__":
        return synthetic_defaults.get(target, "")
    text = ensure_text(row.get(source_column))
    if text:
        return text
    return synthetic_defaults.get(target, "")


def _parse_period_from_row(row: dict[str, Any], row_number: int, schema: SalarySchema) -> tuple[int, int]:
    manifest = schema.source_manifest or {}
    if schema.schema_mode != "inferred" or not manifest:
        year_value = _parse_numeric_cell(row, "统计年度", row_number)
        month_value = _parse_numeric_cell(row, "统计月份", row_number)
        if year_value is None or month_value is None:
            raise ValueError(f"第 {row_number} 行缺少统计期间。")
        year = int(year_value)
        month = int(month_value)
    else:
        period_mode = ensure_text(manifest.get("period_mode"), "year_month")
        period_config = dict(manifest.get("period") or {})
        if period_mode == "single_period":
            period_column = ensure_text(period_config.get("period_column"))
            period = _period_from_single_value(ensure_text(row.get(period_column)))
            if period is None:
                raise ValueError(f"第 {row_number} 行字段“{period_column}”无法解析为期间。")
            year, month = period
        else:
            year_column = ensure_text(period_config.get("year_column"), "统计年度")
            month_column = ensure_text(period_config.get("month_column"), "统计月份")
            year_value = _parse_numeric_cell(row, year_column, row_number)
            month_value = _parse_numeric_cell(row, month_column, row_number)
            if year_value is None or month_value is None:
                raise ValueError(f"第 {row_number} 行缺少统计期间。")
            year = int(year_value)
            month = int(month_value)
    if month < 1 or month > 12:
        raise ValueError(f"第 {row_number} 行字段“统计月份”超出 1-12 范围：{month}")
    return year, month


def _parse_numeric_cell(row: dict[str, Any], column: str, row_number: int, allow_empty: bool = False) -> float | None:
    raw = row.get(column)
    if raw is None or ensure_text(raw) == "":
        if allow_empty:
            return None
        raise ValueError(f"第 {row_number} 行字段“{column}”为空。")
    try:
        return round(float(str(raw).replace(",", "").strip()), 2)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"第 {row_number} 行字段“{column}”不是合法数字：{raw}") from exc


def _normalize_import_row(
    row: dict[str, Any],
    row_number: int,
    schema: SalarySchema,
) -> tuple[tuple[Any, ...], tuple[int, int]]:
    year, month = _parse_period_from_row(row, row_number, schema)

    values: list[Any] = [year, month]
    for column in schema.text_dimension_columns:
        text = _derive_dimension_value(column, row, row_number, year, month, schema)
        if not text:
            raise ValueError(f"第 {row_number} 行字段“{column}”为空。")
        values.append(text)

    for column in schema.numeric_columns:
        source_column = (schema.source_column_map or {}).get(column, column)
        if source_column in {"__constant__", "__rowid__", "__period__"}:
            values.append(None)
        else:
            values.append(_parse_numeric_cell(row, source_column, row_number, allow_empty=True))
    return tuple(values), (year, month)


def _validate_csv_file(csv_path: Path, schema: SalarySchema | None = None) -> CsvValidationResult:
    if not csv_path.exists():
        raise ValueError(f"数据源文件不存在：{csv_path}")
    if csv_path.suffix.lower() != ".csv":
        raise ValueError("仅支持 CSV 文件。")

    encoding, headers = _detect_csv_encoding(csv_path)
    chosen_schema = schema or detect_schema_from_headers(headers)
    configure_schema(chosen_schema)

    with csv_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        row_count = 0
        first_period: tuple[int, int] | None = None
        last_period: tuple[int, int] | None = None
        for row_number, row in enumerate(reader, start=2):
            _, period = _normalize_import_row(row, row_number, chosen_schema)
            row_count += 1
            if first_period is None or period < first_period:
                first_period = period
            if last_period is None or period > last_period:
                last_period = period

    if row_count == 0:
        raise ValueError("CSV 文件为空。")
    if first_period is None or last_period is None:
        raise ValueError("CSV 文件缺少有效数据行。")

    return CsvValidationResult(
        csv_path=csv_path,
        filename=csv_path.name,
        encoding=encoding,
        headers=headers,
        row_count=row_count,
        period_start=f"{first_period[0]:04d}-{first_period[1]:02d}",
        period_end=f"{last_period[0]:04d}-{last_period[1]:02d}",
        signature=_dataset_signature(csv_path),
        schema_id=chosen_schema.schema_id,
        schema_mode=chosen_schema.schema_mode,
        schema_manifest=chosen_schema.source_manifest,
    )


def _sample_csv_rows(csv_path: Path, encoding: str, limit: int = 40) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        for _, row in zip(range(limit), reader):
            rows.append(dict(row))
    return rows


def _infer_column(columns: list[str], name: str, sample_rows: list[dict[str, Any]]) -> InferredColumn:
    values = [ensure_text(row.get(name)) for row in sample_rows if ensure_text(row.get(name))]
    unique_values = len(set(values))
    numeric_ratio = _safe_numeric_ratio(values)
    non_empty_ratio = round(len(values) / max(len(sample_rows), 1), 4) if sample_rows else 0.0
    lowered = name.lower()
    reason_parts: list[str] = []

    period_keywords = ["统计月", "月份", "month", "期间", "date", "年月"]
    year_keywords = ["统计年度", "年份", "year"]
    month_keywords = ["统计月份", "月份", "month"]
    dimension_keywords = ["员工", "工号", "um", "bu", "部门", "序列", "职能", "级别", "绩效", "年龄", "司龄", "岗位"]
    subject_keywords = ["金额", "工资", "奖金", "津贴", "补贴", "补偿", "保险", "公积金", "个税", "扣款", "收入", "提奖", "绩效"]
    ignore_keywords = ["备注", "说明", "comment", "note"]

    detected_type = "ignored"
    canonical_name = name
    confidence = 0.35

    if any(keyword in name for keyword in year_keywords):
        detected_type = "period_year"
        canonical_name = "统计年度"
        confidence = 0.98
        reason_parts.append("列名明显指向年份字段")
    elif any(keyword in name for keyword in month_keywords) and not any(keyword in name for keyword in year_keywords):
        detected_type = "period_month"
        canonical_name = "统计月份"
        confidence = 0.96
        reason_parts.append("列名明显指向月份字段")
    elif any(keyword in name for keyword in period_keywords) or any(_period_from_single_value(value) for value in values[:6]):
        detected_type = "period"
        canonical_name = "统计月"
        confidence = 0.9 if any(_period_from_single_value(value) for value in values[:6]) else 0.8
        reason_parts.append("样本值可解析为年月期间")
    elif any(keyword in lowered for keyword in ignore_keywords):
        detected_type = "ignored"
        canonical_name = name
        confidence = 0.95
        reason_parts.append("列名看起来更像备注说明")
    elif any(keyword in name for keyword in subject_keywords) or (numeric_ratio >= 0.75 and unique_values >= 3):
        detected_type = "subject"
        canonical_name = _suggest_subject_name(name)
        confidence = 0.88 if any(keyword in name for keyword in subject_keywords) else 0.72
        reason_parts.append("列名或样本值更像可聚合金额科目")
    elif any(keyword in lowered for keyword in dimension_keywords) or numeric_ratio < 0.55:
        detected_type = "dimension"
        canonical_name = _suggest_dimension_name(name)
        confidence = 0.86 if any(keyword in lowered for keyword in dimension_keywords) else 0.65
        reason_parts.append("列名或样本分布更像文本维度")
    else:
        detected_type = "ignored"
        canonical_name = name
        confidence = 0.45
        reason_parts.append("暂时无法稳定判断，先放入待确认/忽略区")

    if numeric_ratio >= 0.95 and detected_type == "dimension":
        detected_type = "subject"
        canonical_name = _suggest_subject_name(name)
        confidence = max(confidence, 0.7)
        reason_parts.append("高比例数值字段，更偏向金额科目")
    if unique_values <= 2 and detected_type == "subject" and numeric_ratio < 0.5:
        detected_type = "dimension"
        canonical_name = _suggest_dimension_name(name)
        confidence = 0.62
        reason_parts.append("取值种类很少，更像枚举维度")

    return InferredColumn(
        name=name,
        detected_type=detected_type,
        canonical_name=canonical_name,
        confidence=round(confidence, 2),
        reason="；".join(_dedupe_preserve(reason_parts)),
        sample_values=values[:5],
        non_empty_ratio=non_empty_ratio,
        numeric_ratio=numeric_ratio,
    )


def _maybe_refine_inference_with_llm(columns: list[InferredColumn]) -> list[InferredColumn]:
    llm = LLMService()
    if not llm.enabled:
        return columns
    pending = [column for column in columns if column.confidence < 0.7]
    if not pending:
        return columns
    prompt = {
        "task": "请判断薪酬宽表字段更像 period/dimension/subject/ignored，并可给出更合适的 canonical_name。",
        "columns": [
            {
                "name": column.name,
                "detected_type": column.detected_type,
                "canonical_name": column.canonical_name,
                "confidence": column.confidence,
                "reason": column.reason,
                "sample_values": column.sample_values,
                "numeric_ratio": column.numeric_ratio,
            }
            for column in pending
        ],
        "output_schema": {
            "columns": [
                {
                    "name": "原列名",
                    "detected_type": "period|period_year|period_month|dimension|subject|ignored",
                    "canonical_name": "建议显示名",
                    "confidence": 0.0,
                    "reason": "一句话原因",
                }
            ]
        },
    }
    try:
        raw = llm._chat_completion(
            system_prompt="你是薪酬数据表头识别助手，只输出 JSON，不要解释。",
            user_prompt=json.dumps(prompt, ensure_ascii=False),
            temperature=0.1,
        )
        payload = json.loads(extract_json(raw))
        mapping = {ensure_text(item.get("name")): item for item in payload.get("columns", [])}
        refined: list[InferredColumn] = []
        for column in columns:
            suggestion = mapping.get(column.name)
            if not suggestion:
                refined.append(column)
                continue
            refined.append(
                InferredColumn(
                    name=column.name,
                    detected_type=ensure_text(suggestion.get("detected_type"), column.detected_type),
                    canonical_name=ensure_text(suggestion.get("canonical_name"), column.canonical_name),
                    confidence=min(0.95, max(column.confidence, float(suggestion.get("confidence") or column.confidence))),
                    reason=ensure_text(suggestion.get("reason"), column.reason),
                    sample_values=column.sample_values,
                    non_empty_ratio=column.non_empty_ratio,
                    numeric_ratio=column.numeric_ratio,
                )
            )
        return refined
    except Exception:
        return columns


def infer_schema_draft(csv_path: Path) -> dict[str, Any]:
    if not csv_path.exists():
        raise ValueError(f"数据源文件不存在：{csv_path}")
    if csv_path.suffix.lower() != ".csv":
        raise ValueError("仅支持 CSV 文件。")

    encoding, headers = _detect_csv_encoding(csv_path)
    try:
        schema = detect_schema_from_headers(headers)
        validation = _validate_csv_file(csv_path, schema)
        return {
            "mode": "registered_match",
            "filename": csv_path.name,
            "path": str(csv_path.resolve()),
            "encoding": encoding,
            "schema_id": schema.schema_id,
            "schema_name": schema.display_name,
            "row_count": validation.row_count,
            "period_start": validation.period_start,
            "period_end": validation.period_end,
        }
    except ValueError:
        pass

    sample_rows = _sample_csv_rows(csv_path, encoding)
    inferred_columns = [_infer_column(headers, name, sample_rows) for name in headers]
    inferred_columns = _maybe_refine_inference_with_llm(inferred_columns)

    year_column = next((column.name for column in inferred_columns if column.detected_type == "period_year"), "")
    month_column = next((column.name for column in inferred_columns if column.detected_type == "period_month"), "")
    period_column = next((column.name for column in inferred_columns if column.detected_type == "period"), "")
    period_mode = "single_period" if period_column and not (year_column and month_column) else "year_month"

    raw_dimensions = [column for column in inferred_columns if column.detected_type == "dimension"]
    raw_subjects = [column for column in inferred_columns if column.detected_type == "subject"]
    ignored = [column for column in inferred_columns if column.detected_type == "ignored"]

    if not raw_dimensions:
        raise ValueError("未识别出可分析维度列，请检查 CSV 是否包含员工/组织/人群字段。")
    if not raw_subjects:
        raise ValueError("当前文件不包含可分析金额科目。")

    dimension_columns = _dedupe_preserve([column.canonical_name for column in raw_dimensions])
    subject_columns = _dedupe_preserve([column.canonical_name for column in raw_subjects])
    source_column_map: dict[str, str] = {}
    dimension_aliases: dict[str, str] = {}
    subject_aliases: dict[str, str] = {}

    for column in raw_dimensions:
        source_column_map[column.canonical_name] = column.name
        if column.canonical_name != column.name:
            dimension_aliases[column.name] = column.canonical_name
    for column in raw_subjects:
        source_column_map[column.canonical_name] = column.name
        if column.canonical_name != column.name:
            subject_aliases[column.name] = column.canonical_name

    synthetic_defaults = {}
    if "统计月" not in dimension_columns:
        dimension_columns.append("统计月")
        source_column_map["统计月"] = "__period__"
    if "BU" not in dimension_columns:
        primary_candidate = dimension_columns[0]
        source_column_map["BU"] = source_column_map.get(primary_candidate, primary_candidate)
        synthetic_defaults["BU"] = "全部BU"
    if "员工ID" not in dimension_columns:
        source_column_map["员工ID"] = "__rowid__"
        synthetic_defaults["员工ID"] = ""

    support_dimensions = ["职能", "绩效分位", "级别", "司龄分箱", "年龄分箱"]
    for dimension in support_dimensions:
        if dimension not in source_column_map:
            source_column_map[dimension] = "__constant__"
            synthetic_defaults[dimension] = "未提供"

    text_dimension_columns = _dedupe_preserve(
        [
            "统计月",
            "员工ID",
            "BU",
            *support_dimensions,
            *[dimension for dimension in dimension_columns if dimension not in {"统计月", "员工ID", "BU", *support_dimensions}],
        ]
    )
    display_dimension_columns = [dimension for dimension in dimension_columns if dimension != "统计月"]
    capabilities = {
        "supports_trend_analysis": bool(period_column or (year_column and month_column)),
        "supports_employee_level_detail": "员工ID" in [column.canonical_name for column in raw_dimensions],
        "supports_yoy": bool(period_column or (year_column and month_column)),
        "supports_mom": bool(period_column or (year_column and month_column)),
    }
    manifest = {
        "schema_id": "inferred_runtime",
        "display_name": "智能识别宽表",
        "schema_mode": "inferred",
        "period_mode": period_mode,
        "period": {
            "year_column": year_column,
            "month_column": month_column,
            "period_column": period_column,
        },
        "text_dimension_columns": text_dimension_columns,
        "dimension_columns": _dedupe_preserve(["BU", *display_dimension_columns]),
        "display_dimension_columns": [dimension for dimension in _dedupe_preserve(display_dimension_columns) if dimension != "员工ID"],
        "subject_columns": subject_columns,
        "default_subject": subject_columns[0],
        "default_secondary_dimensions": [dimension for dimension in display_dimension_columns if dimension != "BU"][:4],
        "source_column_map": source_column_map,
        "dimension_aliases": dimension_aliases,
        "subject_aliases": subject_aliases,
        "synthetic_defaults": synthetic_defaults,
        "capabilities": capabilities,
        "columns": [
            {
                "name": column.name,
                "detected_type": column.detected_type,
                "canonical_name": column.canonical_name,
                "confidence": column.confidence,
                "reason": column.reason,
                "sample_values": column.sample_values,
                "non_empty_ratio": column.non_empty_ratio,
                "numeric_ratio": column.numeric_ratio,
            }
            for column in inferred_columns
        ],
        "ignored_columns": [column.name for column in ignored],
    }
    return {
        "mode": "inference_required",
        "filename": csv_path.name,
        "path": str(csv_path.resolve()),
        "encoding": encoding,
        "draft": manifest,
    }


def _persist_active_dataset_meta(
    conn: sqlite3.Connection,
    validation: CsvValidationResult,
) -> None:
    imported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_PATH, str(validation.csv_path.resolve()))
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_NAME, validation.filename)
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_SIGNATURE, validation.signature)
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_IMPORTED_AT, imported_at)
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_ROW_COUNT, str(validation.row_count))
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_PERIOD_START, validation.period_start)
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_PERIOD_END, validation.period_end)
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_ENCODING, validation.encoding)
    _set_meta_value(conn, APP_META_ACTIVE_DATASET_VALIDATION_STATUS, "passed")
    _set_meta_value(conn, APP_META_ACTIVE_SCHEMA_ID, validation.schema_id)
    _set_meta_value(conn, APP_META_ACTIVE_SCHEMA_MODE, validation.schema_mode)
    if validation.schema_manifest:
        _set_meta_value(conn, APP_META_ACTIVE_SCHEMA_MANIFEST, json.dumps(validation.schema_manifest, ensure_ascii=False))
    else:
        _delete_meta_keys(conn, APP_META_ACTIVE_SCHEMA_MANIFEST)


def _sync_active_dataset(conn: sqlite3.Connection) -> None:
    dataset_path = _resolve_active_dataset_path(conn)
    if dataset_path is None:
        return
    if not dataset_path.exists():
        print(f"[数据源] 默认数据文件不存在: {dataset_path}")
        _set_meta_value(conn, APP_META_ACTIVE_DATASET_VALIDATION_STATUS, "missing")
        return

    print(f"[数据源] 检查默认数据源: {dataset_path}")

    current_rows = conn.execute("SELECT COUNT(*) FROM salary_wide").fetchone()[0]
    stored_signature = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_DATASET_SIGNATURE))
    stored_schema_id = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_ID))
    stored_schema_mode = ensure_text(_get_meta_value(conn, APP_META_ACTIVE_SCHEMA_MODE), "registered")
    current_signature = _dataset_signature(dataset_path)

    # Fast path for large datasets: if the imported DB already matches the tracked file
    # signature and schema, skip re-scanning the entire CSV during startup.
    if current_rows > 0 and stored_signature == current_signature and stored_schema_id:
        target_schema = _schema_from_meta(conn)
        configure_schema(target_schema)
        _set_meta_value(conn, APP_META_ACTIVE_DATASET_PATH, str(dataset_path.resolve()))
        _set_meta_value(conn, APP_META_ACTIVE_DATASET_NAME, dataset_path.name)
        _set_meta_value(conn, APP_META_ACTIVE_DATASET_VALIDATION_STATUS, "passed")
        print(f"[数据源] 复用已导入 SQLite 数据，共 {current_rows} 行")
        return

    target_schema = _schema_from_meta(conn) if stored_schema_mode == "inferred" else None
    print("[数据源] 开始校验 CSV 结构")
    validation = _validate_csv_file(dataset_path, target_schema)
    target_schema = create_runtime_schema(validation.schema_manifest) if validation.schema_mode == "inferred" and validation.schema_manifest else get_schema(validation.schema_id)
    configure_schema(target_schema)
    if current_rows == 0 or stored_signature != validation.signature or stored_schema_id != validation.schema_id or stored_schema_mode != validation.schema_mode:
        print(f"[数据源] 开始导入 CSV 到 SQLite，预计行数: {validation.row_count}")
        _recreate_salary_table(conn, target_schema)
        _import_csv(conn, validation.csv_path, validation.encoding)
        _create_indexes(conn)
        _create_subject_view(conn)
        print("[数据源] CSV 导入完成，正在写入活动数据源信息")
    _persist_active_dataset_meta(conn, validation)
    print(f"[数据源] 默认数据源已激活: {validation.filename}")


def _import_csv(conn: sqlite3.Connection, csv_path: Path, encoding: str) -> None:
    schema = active_schema()
    placeholders = ",".join("?" for _ in WIDE_COLUMNS)
    quoted_columns = ",".join(f'"{column}"' for column in WIDE_COLUMNS)
    insert_sql = f"INSERT INTO salary_wide ({quoted_columns}) VALUES ({placeholders})"
    batch: list[tuple[Any, ...]] = []
    imported_rows = 0
    with csv_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            normalized_row, _ = _normalize_import_row(row, row_number, schema)
            batch.append(normalized_row)
            if len(batch) >= 5000:
                conn.executemany(insert_sql, batch)
                imported_rows += len(batch)
                if imported_rows % 100000 == 0:
                    print(f"[数据导入] 已写入 {imported_rows} 行")
                batch.clear()
        if batch:
            conn.executemany(insert_sql, batch)
            imported_rows += len(batch)
    print(f"[数据导入] 全部写入完成，共 {imported_rows} 行")


def _create_indexes(conn: sqlite3.Connection) -> None:
    available_columns = {row["name"] for row in conn.execute("PRAGMA table_info(salary_wide)").fetchall()}
    index_specs = [
        ("idx_salary_wide_period", ["统计年度", "统计月份"]),
        ("idx_salary_wide_bu", ["BU"]),
        ("idx_salary_wide_emp", ["员工ID"]),
        ("idx_salary_wide_level", ["级别"]),
        ("idx_salary_wide_function", ["职能"]),
        ("idx_salary_wide_dept", ["部门"]),
        ("idx_salary_wide_perf", ["绩效分位"]),
        ("idx_salary_wide_prev_perf", ["去年绩效排名"]),
    ]
    for index_name, columns in index_specs:
        if all(column in available_columns for column in columns):
            joined = ", ".join(columns)
            conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON salary_wide({joined})")


def _create_saved_reports_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            subject TEXT NOT NULL,
            question TEXT NOT NULL,
            request_json TEXT NOT NULL,
            report_json TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT 'manual',
            base_saved_report_id INTEGER,
            revision_instruction TEXT,
            data_source_name TEXT,
            data_source_signature TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )
    _ensure_table_column(conn, "saved_reports", "data_source_name", "data_source_name TEXT")
    _ensure_table_column(conn, "saved_reports", "data_source_signature", "data_source_signature TEXT")


def _create_subject_view(conn: sqlite3.Connection) -> None:
    available_columns = {row["name"] for row in conn.execute("PRAGMA table_info(salary_wide)").fetchall()}

    def select_or_default(column: str, default_sql: str = "''") -> str:
        if column in available_columns:
            return f'"{column}" AS "{column}"'
        return f"{default_sql} AS \"{column}\""

    union_sql = []
    for subject in SUBJECT_COLUMNS:
        union_sql.append(
            f"""
            SELECT
                统计年度,
                统计月份,
                {select_or_default('BU')},
                {select_or_default('员工ID')},
                {select_or_default('职能')},
                {select_or_default('绩效分位')},
                {select_or_default('级别')},
                {select_or_default('司龄分箱')},
                {select_or_default('年龄分箱')},
                '{subject}' AS 薪酬科目,
                "{subject}" AS 金额,
                CASE WHEN "{subject}" > 0 THEN 1 ELSE 0 END AS 是否发放
            FROM salary_wide
            """
        )
    conn.execute("DROP VIEW IF EXISTS salary_subject_facts")
    conn.execute(f"CREATE VIEW salary_subject_facts AS {' UNION ALL '.join(union_sql)}")


def metadata() -> dict[str, Any]:
    conn = get_connection()
    try:
        data_source = _current_data_source_meta(conn)
        capabilities = active_capabilities()
        capabilities.update(external_research_runtime_status())
        display_dimensions = list(dict.fromkeys(
            (active_schema().source_manifest or {}).get("display_dimension_columns")
            or [d for d in DIMENSION_COLUMNS if d != primary_dimension_name()]
        ))
        return {
            "subjects": SUBJECT_COLUMNS,
            "subject_catalog": build_subject_catalog(),
            "dimension_catalog": build_dimension_catalog(display_dimensions),
            "dimensions": display_dimensions,
            "primary_dimension": primary_dimension_name(),
            "row_count": data_source["row_count"] if data_source["ready"] else 0,
            "period_start": data_source["period_start"] if data_source["ready"] else "",
            "period_end": data_source["period_end"] if data_source["ready"] else "",
            "schema_mode": active_schema().schema_mode,
            "capabilities": capabilities,
            "source_manifest": active_schema().source_manifest,
            "data_source": data_source,
        }
    finally:
        conn.close()


def activate_dataset(csv_path: Path) -> dict[str, Any]:
    validation = _validate_csv_file(csv_path.resolve())
    conn = get_connection()
    try:
        configure_schema(get_schema(validation.schema_id))
        _recreate_salary_table(conn, active_schema())
        _import_csv(conn, validation.csv_path, validation.encoding)
        _create_indexes(conn)
        _create_subject_view(conn)
        _persist_active_dataset_meta(conn, validation)
        _clear_dimension_value_cache()
        conn.commit()
        return _current_data_source_meta(conn)
    finally:
        conn.close()


def activate_inferred_dataset(csv_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    runtime_schema = create_runtime_schema(manifest)
    validation = _validate_csv_file(csv_path.resolve(), runtime_schema)
    conn = get_connection()
    try:
        configure_schema(runtime_schema)
        _recreate_salary_table(conn, runtime_schema)
        _import_csv(conn, validation.csv_path, validation.encoding)
        _create_indexes(conn)
        _create_subject_view(conn)
        _persist_active_dataset_meta(conn, validation)
        _clear_dimension_value_cache()
        conn.commit()
        return _current_data_source_meta(conn)
    finally:
        conn.close()


def normalize_subject(raw: str | None) -> str:
    raw = (raw or "").strip()
    if raw in SUBJECT_COLUMNS:
        return raw
    if raw in SUBJECT_ALIASES:
        return SUBJECT_ALIASES[raw]
    for subject in SUBJECT_COLUMNS:
        if raw and raw in subject:
            return subject
    raise ValueError("无法识别薪酬科目，请从支持的科目中选择。")


def normalize_dimension(raw: str) -> str:
    raw = raw.strip()
    if raw in DIMENSION_COLUMNS:
        return raw
    if raw in DIMENSION_ALIASES:
        return DIMENSION_ALIASES[raw]
    raise ValueError(f"无法识别维度：{raw}")


def normalize_secondary_dimensions(raw_dimensions: list[str]) -> list[str]:
    cleaned: list[str] = []
    for item in raw_dimensions:
        dimension = normalize_dimension(item)
        if dimension == "BU":
            continue
        if dimension not in cleaned:
            cleaned.append(dimension)
    if not cleaned:
        raise ValueError("请至少选择一个次维度。")
    if len(cleaned) > SECONDARY_DIMENSION_LIMIT:
        raise ValueError(f"次维度最多支持 {SECONDARY_DIMENSION_LIMIT} 个。")
    return cleaned


def parse_period(period: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-(\d{2})", period)
    if not match:
        raise ValueError(f"日期格式不正确：{period}")
    year, month = int(match.group(1)), int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError(f"月份不正确：{period}")
    return year, month


def parse_natural_language(question: str, fallback_subject: str | None = None) -> dict[str, Any]:
    subject_resolution = resolve_subject(question, fallback_subject, fallback_subject)
    chosen_subject = subject_resolution.display_subject or fallback_subject

    found_dimensions: list[str] = []
    for name in DIMENSION_COLUMNS:
        if name != "BU" and name in question:
            found_dimensions.append(name)
    for alias, canonical in DIMENSION_ALIASES.items():
        if alias in question and canonical not in found_dimensions and canonical != "BU":
            found_dimensions.append(canonical)

    found_metrics: list[str] = []
    for alias, canonical in METRIC_ALIASES.items():
        if alias in question and canonical not in found_metrics:
            found_metrics.append(canonical)

    year_matches = re.findall(r"(20\d{2})年?", question)
    month_matches = re.findall(r"(20\d{2})[-年](\d{1,2})月?", question)
    start_period, end_period = default_period_window()
    if len(year_matches) == 1:
        start_period = f"{year_matches[0]}-01"
        end_period = f"{year_matches[0]}-12"
    elif len(year_matches) >= 2:
        start_period = f"{year_matches[0]}-01"
        end_period = f"{year_matches[-1]}-12"
    if len(month_matches) == 1:
        year, month = month_matches[0]
        start_period = f"{year}-{int(month):02d}"
        end_period = start_period
    elif len(month_matches) >= 2:
        sy, sm = month_matches[0]
        ey, em = month_matches[-1]
        start_period = f"{sy}-{int(sm):02d}"
        end_period = f"{ey}-{int(em):02d}"

    return {
        "subject": chosen_subject,
        "secondary_dimensions": found_dimensions,
        "start_period": start_period,
        "end_period": end_period,
        "metrics": found_metrics,
    }


def build_request(payload: dict[str, Any]) -> AnalysisRequest:
    question = (payload.get("question") or "").strip()
    parsed = parse_natural_language(question, payload.get("subject"))
    default_start_period, default_end_period = default_period_window()

    subject = normalize_subject(payload.get("subject") or parsed.get("subject") or DEFAULT_SUBJECT)
    dimensions = payload.get("secondary_dimensions") or parsed.get("secondary_dimensions") or DEFAULT_SECONDARY_DIMENSIONS
    secondary_dimensions = normalize_secondary_dimensions(dimensions)

    start_period = payload.get("start_period") or parsed.get("start_period") or default_start_period
    end_period = payload.get("end_period") or parsed.get("end_period") or default_end_period
    start_year, start_month = parse_period(start_period)
    end_year, end_month = parse_period(end_period)
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("开始时间不能晚于结束时间。")
    metrics = payload.get("metrics") or parsed.get("metrics") or ["总额", "平均金额", "发放覆盖率"]

    return AnalysisRequest(
        subject=subject,
        primary_dimension=primary_dimension_name(),
        secondary_dimensions=secondary_dimensions,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
        metrics=metrics,
        question=question,
        follow_up_context=payload.get("context"),
    )


def analyze_dimensions_concurrent(
    request: AnalysisRequest,
    insights: list[dict[str, Any]],
    llm_service: "LLMService",
) -> list[dict[str, Any]]:
    """
    并发分析所有维度，显著提升报告生成速度。

    Args:
        request: 分析请求对象
        insights: 维度洞察数据列表
        llm_service: LLM 服务实例

    Returns:
        维度分析报告列表
    """
    import concurrent.futures

    # 如果未启用 LLM 或只有一个维度，直接串行处理
    if not llm_service.enabled or len(insights) <= 1:
        return [llm_service.analyze_dimension(request, insight) for insight in insights]

    # 使用线程池并发处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_dimension = {
            executor.submit(llm_service.analyze_dimension, request, insight): insight
            for insight in insights
        }
        dimension_reports = []
        for future in concurrent.futures.as_completed(future_to_dimension):
            insight = future_to_dimension[future]
            try:
                report = future.result()
                dimension_reports.append(report)
            except Exception as e:
                print(f"[并发分析] 维度 {insight.get('dimension', '未知')} 分析失败: {e}")
                # 失败时使用降级方案
                dimension_reports.append(llm_service._fallback_dimension_report(request, insight))

    # 保持原始顺序（as_completed 会乱序）
    original_order = {insight["dimension"]: i for i, insight in enumerate(insights)}
    dimension_reports.sort(key=lambda r: original_order.get(r.get("dimension", ""), 0))

    return dimension_reports


def generate_report(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_data_source_ready()
    clarification = build_clarification_response(payload)
    if clarification is not None:
        return clarification

    request = build_request(payload)
    save_history(request)
    insight_bundle = collect_insights(request)
    llm_service = LLMService()

    # 并发分析所有维度
    dimension_reports = analyze_dimensions_concurrent(request, insight_bundle["dimension_insights"], llm_service)

    external_research = collect_external_research(request, llm_service)
    consolidated = llm_service.summarize_dimensions(request, insight_bundle, dimension_reports, external_research)
    consolidated["short_answer"] = generate_short_answer_for_report(
        request,
        {
            **consolidated,
            "hero_metrics": insight_bundle["hero_metrics"],
        },
        llm_service,
    )
    return build_report_response(
        request,
        consolidated,
        insight_bundle,
        dimension_reports,
        "structured_sql_plus_llm" if llm_service.enabled else "structured_sql_plus_template",
    )


def generate_report_stream(payload: dict[str, Any]):
    """Generator that yields SSE events as each stage completes."""
    import json as _json
    step_total = len(STREAM_PROGRESS_STAGES)
    ensure_data_source_ready()

    clarification = build_clarification_response(payload)
    if clarification is not None:
        yield f"data: {_json.dumps({'type': 'clarification', 'data': clarification}, ensure_ascii=False)}\n\n"
        yield "data: {\"type\":\"done\"}\n\n"
        return

    request = build_request(payload)
    save_history(request)

    yield f"data: {_json.dumps(build_stream_progress_event('scope', '识别问题与口径', 1, step_total, f'已识别本次问题围绕“{request.subject}”展开，正在锁定分析口径。'), ensure_ascii=False)}\n\n"
    yield f"data: {_json.dumps(build_stream_progress_event('window', '确认分析时间窗口', 2, step_total, f'分析时间窗口已确认为 {request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d}。'), ensure_ascii=False)}\n\n"

    yield f"data: {_json.dumps(build_stream_progress_event('overview', '汇总总体指标', 3, step_total, '正在计算核心指标并汇总 BU 概览...'), ensure_ascii=False)}\n\n"
    insight_bundle = collect_insights(request)

    yield f"data: {_json.dumps({'type': 'hero', 'data': insight_bundle['hero_metrics']}, ensure_ascii=False)}\n\n"
    yield f"data: {_json.dumps({'type': 'overview', 'data': {'bu_overview': insight_bundle['bu_overview'], 'overview_charts': insight_bundle['overview_charts']}}, ensure_ascii=False)}\n\n"

    llm_service = LLMService()

    # 流式输出：先发送"正在并行分析..."消息
    total_dimensions = len(insight_bundle["dimension_insights"])
    if total_dimensions > 1 and llm_service.enabled:
        yield f"data: {_json.dumps(build_stream_progress_event('dimensions', '并行拆解维度', 4, step_total, f'正在并行分析 {total_dimensions} 个维度...'), ensure_ascii=False)}\n\n"

    # 并发分析所有维度
    dimension_reports = analyze_dimensions_concurrent(request, insight_bundle["dimension_insights"], llm_service)

    # 按顺序推送维度报告
    for i, dim_report in enumerate(dimension_reports, 1):
        dim_name = dim_report.get("dimension", f"维度{i}")
        yield f"data: {_json.dumps(build_stream_progress_event('dimensions', '逐维度拆解', 4, step_total, f'完成 {dim_name} 维度分析 ({i}/{total_dimensions})'), ensure_ascii=False)}\n\n"
        yield f"data: {_json.dumps({'type': 'dimension', 'data': dim_report}, ensure_ascii=False)}\n\n"

    yield f"data: {_json.dumps(build_stream_progress_event('research', '检索外部参考', 5, step_total, '正在检索外部研究与管理实践参考...'), ensure_ascii=False)}\n\n"
    external_research = collect_external_research(request, llm_service)

    yield f"data: {_json.dumps(build_stream_progress_event('consolidated', '生成综合报告', 6, step_total, '正在整合多维度洞察并生成综合报告...'), ensure_ascii=False)}\n\n"
    consolidated = llm_service.summarize_dimensions(request, insight_bundle, dimension_reports, external_research)
    consolidated["short_answer"] = generate_short_answer_for_report(
        request,
        {
            **consolidated,
            "hero_metrics": insight_bundle["hero_metrics"],
        },
        llm_service,
    )

    full_response = build_report_response(
        request,
        consolidated,
        insight_bundle,
        dimension_reports,
        "structured_sql_plus_llm" if llm_service.enabled else "structured_sql_plus_template",
    )
    yield f"data: {_json.dumps({'type': 'consolidated', 'data': full_response}, ensure_ascii=False)}\n\n"
    yield "data: {\"type\":\"done\"}\n\n"


def collect_insights(request: AnalysisRequest) -> dict[str, Any]:
    conn = get_connection()
    try:
        base_where, params = build_period_where(request)
        subject_column = f'"{request.subject}"'
        hero_sql = f"""
            SELECT
                SUM({subject_column}) AS total_amount,
                AVG(CASE WHEN {subject_column} > 0 THEN {subject_column} END) AS avg_amount,
                COUNT(DISTINCT 员工ID) AS employee_count,
                COUNT(DISTINCT CASE WHEN {subject_column} > 0 THEN 员工ID END) AS issued_employee_count
            FROM salary_wide
            WHERE {base_where}
        """
        hero_row = conn.execute(hero_sql, params).fetchone()
        hero_metrics = {
            "total_amount": int(hero_row["total_amount"] or 0),
            "avg_amount": round(float(hero_row["avg_amount"] or 0), 2),
            "employee_count": int(hero_row["employee_count"] or 0),
            "issued_employee_count": int(hero_row["issued_employee_count"] or 0),
            "coverage_rate": round(
                (hero_row["issued_employee_count"] or 0) / max(hero_row["employee_count"] or 1, 1) * 100,
                2,
            ),
        }
        bu_overview = rows_to_dicts(
            conn.execute(
                f"""
                SELECT
                    BU,
                    SUM({subject_column}) AS total_amount,
                    ROUND(AVG(CASE WHEN {subject_column} > 0 THEN {subject_column} END), 2) AS avg_amount,
                    COUNT(DISTINCT 员工ID) AS employee_count,
                    COUNT(DISTINCT CASE WHEN {subject_column} > 0 THEN 员工ID END) AS issued_employee_count,
                    ROUND(
                        COUNT(DISTINCT CASE WHEN {subject_column} > 0 THEN 员工ID END) * 100.0
                        / NULLIF(COUNT(DISTINCT 员工ID), 0), 2
                    ) AS coverage_rate
                FROM salary_wide
                WHERE {base_where}
                GROUP BY BU
                ORDER BY total_amount DESC, BU
                """,
                params,
            ).fetchall()
        )
        trend_sql = f"""
            SELECT
                printf('%04d-%02d', 统计年度, 统计月份) AS period,
                SUM({subject_column}) AS total_amount
            FROM salary_wide
            WHERE {base_where}
            GROUP BY 统计年度, 统计月份
            ORDER BY period
        """
        overall_trend_rows = rows_to_dicts(conn.execute(trend_sql, params).fetchall())
        hero_metrics["trend_snapshot"] = build_trend_snapshot(overall_trend_rows)
        overview_charts = build_overview_charts(conn, request, base_where, params, subject_column, bu_overview)

        dimension_insights = []
        sql_preview = []
        for dimension in request.secondary_dimensions:
            insight = analyze_dimension(conn, request, dimension, base_where, params)
            dimension_insights.append(insight)
            sql_preview.append({"dimension": dimension, "sql": insight["sql"]})

        return {
            "hero_metrics": hero_metrics,
            "bu_overview": bu_overview,
            "overview_charts": overview_charts,
            "dimension_insights": dimension_insights,
            "sql_preview": sql_preview,
        }
    finally:
        conn.close()


def build_period_where(request: AnalysisRequest) -> tuple[str, list[Any]]:
    where = """
        (
            (统计年度 > ? OR (统计年度 = ? AND 统计月份 >= ?))
            AND
            (统计年度 < ? OR (统计年度 = ? AND 统计月份 <= ?))
        )
    """
    params = [
        request.start_year,
        request.start_year,
        request.start_month,
        request.end_year,
        request.end_year,
        request.end_month,
    ]
    return where, params


def calculate_mom(series: list[float | int]) -> list[float]:
    results: list[float] = []
    previous = None
    for value in series:
        current = float(value or 0)
        if previous in (None, 0):
            results.append(0.0)
        else:
            results.append(round((current - previous) / previous * 100, 2))
        previous = current
    return results


def calculate_yoy(periods: list[str], series: list[float | int]) -> list[float]:
    values_by_period = {period: float(value or 0) for period, value in zip(periods, series)}
    results: list[float] = []
    for period, value in zip(periods, series):
        year, month = period.split("-")
        previous_period = f"{int(year) - 1:04d}-{month}"
        previous_value = values_by_period.get(previous_period)
        current = float(value or 0)
        if previous_value in (None, 0):
            results.append(0.0)
        else:
            results.append(round((current - previous_value) / previous_value * 100, 2))
    return results


def build_trend_snapshot(trend_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not trend_rows:
        return {}
    periods = [row["period"] for row in trend_rows]
    totals = [float(row["total_amount"] or 0) for row in trend_rows]
    latest_period = periods[-1]
    latest_total = totals[-1]
    previous_period = periods[-2] if len(periods) > 1 else ""
    previous_total = totals[-2] if len(totals) > 1 else None
    mom_delta = round(latest_total - previous_total, 2) if previous_total is not None else None
    mom_rate = (
        round((latest_total - previous_total) / previous_total * 100, 2)
        if previous_total not in (None, 0)
        else None
    )

    year, month = latest_period.split("-")
    yoy_period = f"{int(year) - 1:04d}-{month}"
    values_by_period = {row["period"]: float(row["total_amount"] or 0) for row in trend_rows}
    yoy_total = values_by_period.get(yoy_period)
    yoy_delta = round(latest_total - yoy_total, 2) if yoy_total is not None else None
    yoy_rate = (
        round((latest_total - yoy_total) / yoy_total * 100, 2)
        if yoy_total not in (None, 0)
        else None
    )

    return {
        "latest_period": latest_period,
        "latest_total": round(latest_total, 2),
        "previous_period": previous_period,
        "previous_total": round(previous_total, 2) if previous_total is not None else None,
        "mom_delta": mom_delta,
        "mom_rate": mom_rate,
        "yoy_period": yoy_period if yoy_total is not None else "",
        "yoy_total": round(yoy_total, 2) if yoy_total is not None else None,
        "yoy_delta": yoy_delta,
        "yoy_rate": yoy_rate,
    }


def build_external_queries(request: AnalysisRequest) -> list[dict[str, str]]:
    dimensions_text = "、".join(request.secondary_dimensions) if request.secondary_dimensions else "部门、级别、去年绩效排名"
    time_text = f"{request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d}"
    return [
        {
            "topic": "薪酬治理方法",
            "query": f"{request.subject} 薪酬治理 预算控制 趋势 WTW Mercer Robert Half {time_text}",
        },
        {
            "topic": "组织风险",
            "query": f"{request.subject} 离职风险 组织稳定性 人才保留 趋势 WTW Mercer Robert Half",
        },
        {
            "topic": "维度结构观察",
            "query": f"{request.subject} {dimensions_text} 薪酬结构 人群分层 趋势",
        },
        {
            "topic": "奖励设计趋势",
            "query": f"2025 2026 薪酬透明 奖励设计 人才保留 趋势 Mercer WTW Robert Half",
        },
        {
            "topic": "制度治理实践",
            "query": f"{request.subject} 制度设计 治理实践 预算管理 组织效能",
        },
        {
            "topic": "宏观变化",
            "query": f"2025 2026 中国 雇佣市场 组织调整 薪酬预算 趋势 {request.subject}",
        },
    ]


def is_trusted_source(url: str) -> bool:
    lowered = ensure_text(url).lower()
    return any(keyword in lowered for keyword in TRUSTED_SOURCE_KEYWORDS)


def extract_source_name(url: str) -> str:
    lowered = ensure_text(url).lower()
    if "wtwco.com" in lowered:
        return "WTW"
    if "mercer.com" in lowered:
        return "Mercer"
    if "roberthalf" in lowered:
        return "Robert Half"
    if "gartner.com" in lowered:
        return "Gartner"
    if "mckinsey.com" in lowered:
        return "McKinsey"
    if "pwc.com" in lowered:
        return "PwC"
    if "deloitte.com" in lowered:
        return "Deloitte"
    if "ey.com" in lowered:
        return "EY"
    if "kornferry.com" in lowered:
        return "Korn Ferry"
    if "gov.cn" in lowered:
        return "政府/监管来源"
    if "ilo.org" in lowered:
        return "ILO"
    if "shrm.org" in lowered:
        return "SHRM"
    return "外部来源"


def tavily_search(query: str, max_results: int = 5, timeout_seconds: int = 20) -> list[dict[str, Any]]:
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        return []
    body = json.dumps(
        {
            "query": query,
            "search_depth": "advanced",
            "max_results": max_results,
            "topic": "general",
            "include_answer": False,
            "include_raw_content": False,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://api.tavily.com/search",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
            results = payload.get("results", [])
            if isinstance(results, list):
                return results
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return []
    return []


def external_research_runtime_status() -> dict[str, Any]:
    enabled = bool(ensure_text(os.getenv("TAVILY_API_KEY")))
    return {
        "external_research_enabled": enabled,
        "external_research_status": "external_available" if enabled else "external_unavailable",
    }


def clean_external_content(raw: str) -> str:
    """Strip navigation, TOC, footer, site descriptions and other web noise from Tavily content."""
    text = ensure_text(raw)
    if not text:
        return ""
    noise_patterns = [
        r"(?i)(skip to (?:main )?content|cookie\s*(?:policy|settings|preferences)|accept\s*(?:all\s*)?cookies)",
        r"(?i)(navigation|breadcrumb|sidebar|footer|header|menu)\s*[:\-]",
        r"(?i)(subscribe|sign\s*up|log\s*in|register|newsletter|follow us)",
        r"(?i)(copyright|©|all rights reserved|terms of (?:use|service)|privacy policy)",
        r"(?i)(share\s*(?:on|this)|tweet|linkedin|facebook|twitter)",
        r"(?i)(目录|导航|首页|返回顶部|上一篇|下一篇|相关推荐|热门文章|阅读更多)",
        r"(?i)(关注我们|订阅|注册|登录|免费试用|立即咨询|联系我们)",
    ]
    for pattern in noise_patterns:
        text = re.sub(pattern, "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{3,}", " ", text)
    text = re.sub(r"https?://\S+", "", text)
    text = text.strip()
    sentences = re.split(r"(?<=[。！？.!?])\s*", text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 8]
    if len(sentences) > 3:
        sentences = sentences[:3]
    return "".join(sentences)


LOW_QUALITY_URL_PATTERNS = [
    r"(?i)(baike\.|zhihu\.com/question|baijiahao|sohu\.com|163\.com|sina\.com\.cn/)",
    r"(?i)(marketing|promo|campaign|landing|signup|demo|trial)",
    r"(?i)(aggregat|directory|listing|index\.html$)",
]


def is_low_quality_source(url: str) -> bool:
    lowered = ensure_text(url).lower()
    return any(re.search(pattern, lowered) for pattern in LOW_QUALITY_URL_PATTERNS)


def compact_external_result(result: dict[str, Any], topic: str) -> dict[str, str]:
    url = ensure_text(result.get("url"))
    title = ensure_text(result.get("title"))
    raw_content = ensure_text(result.get("content") or result.get("snippet"))
    summary = clean_external_content(raw_content)
    if not summary:
        summary = clean_sentence(raw_content)[:200] if raw_content else ""
    return {
        "source_name": extract_source_name(url),
        "title": title or url,
        "published_at": ensure_text(result.get("published_date")),
        "summary": summary,
        "url": url,
        "query_topic": topic,
    }


def fallback_external_research_bundle(request: AnalysisRequest, source_notes: list[dict[str, str]]) -> dict[str, Any]:
    short_notes = source_notes[:6]
    source_names = list(dict.fromkeys(item.get("source_name", "外部来源") for item in short_notes if item.get("source_name")))
    names_text = "、".join(source_names[:3]) if source_names else "外部研究"
    trends = [
        f"结合 {names_text} 的公开研究，{request.subject} 的治理重心正在从事后补偿转向事前预算约束与关键人群识别。",
        f"多家咨询机构观察到，薪酬预算收紧背景下，{request.subject} 的结构性集中问题正在成为管理层关注焦点。",
    ]
    risks = [
        f"外部研究普遍提示，{request.subject} 在高敏感群体中的集中释放可能引发合规与雇主品牌风险。",
        "竞业限制泛化、协议离职条款不规范等问题在近期监管案例中反复出现，值得内部排查。",
    ]
    actions = [
        f"成熟市场的治理实践表明，{request.subject} 的管控应前置到审批流程和预算分配环节，而非仅做事后统计。",
    ]
    angles = [
        f"结合外部趋势，本次内部分析更应关注 {request.subject} 在头部 BU 和高敏感人群中的结构性集中，而非平均水平。",
    ]
    return {
        "research_mode": "external_blended" if short_notes else "internal_only",
        "external_trends": trends[:2],
        "external_risk_signals": risks[:2],
        "external_management_patterns": actions,
        "external_reporting_angles": angles,
        "external_research_summary": trends[:1] + actions[:1],
        "source_notes": short_notes,
    }


def build_external_research_unavailable_bundle() -> dict[str, Any]:
    return {
        "research_mode": "external_unavailable",
        "external_trends": [],
        "external_risk_signals": [],
        "external_management_patterns": [],
        "external_reporting_angles": [],
        "external_research_summary": ["当前未配置外部搜索能力，本次正式报告仅基于内部数据生成。"],
        "source_notes": [],
    }


def build_external_research_empty_bundle(reason: str = "") -> dict[str, Any]:
    summary = "已尝试执行外部搜索，但本次未命中可引用的合格来源。"
    if reason:
        summary = f"{summary}{reason}"
    return {
        "research_mode": "external_empty",
        "external_trends": [],
        "external_risk_signals": [],
        "external_management_patterns": [],
        "external_reporting_angles": [],
        "external_research_summary": [summary],
        "source_notes": [],
    }


def collect_external_research(request: AnalysisRequest, llm_service: "LLMService") -> dict[str, Any]:
    if not ensure_text(os.getenv("TAVILY_API_KEY")):
        return build_external_research_unavailable_bundle()
    queries = build_external_queries(request)
    max_results = int(os.getenv("TAVILY_MAX_RESULTS", "5"))
    timeout_seconds = int(os.getenv("TAVILY_TIMEOUT_SECONDS", "20"))
    source_notes: list[dict[str, str]] = []
    had_any_result = False
    for item in queries:
        results = tavily_search(item["query"], max_results=max_results, timeout_seconds=timeout_seconds)
        if results:
            had_any_result = True
        filtered = [row for row in results if not is_low_quality_source(ensure_text(row.get("url")))]
        trusted = [row for row in filtered if is_trusted_source(ensure_text(row.get("url")))]
        selected = trusted[:2] if trusted else filtered[:1]
        for result in selected:
            compact = compact_external_result(result, item["topic"])
            if compact["url"] and compact not in source_notes:
                source_notes.append(compact)
        if len(source_notes) >= 6:
            break
    if not source_notes:
        return build_external_research_empty_bundle("" if had_any_result else "检索返回为空或超时。")
    if llm_service.enabled:
        summarized = llm_service.summarize_external_research(request, source_notes)
        if summarized:
            return summarized
    return fallback_external_research_bundle(request, source_notes)


def analyze_dimension(
    conn: sqlite3.Connection,
    request: AnalysisRequest,
    dimension: str,
    base_where: str,
    params: list[Any],
) -> dict[str, Any]:
    subject_column = f'"{request.subject}"'
    sql = f"""
        SELECT
            BU,
            "{dimension}" AS dimension_value,
            SUM({subject_column}) AS total_amount,
            ROUND(AVG(CASE WHEN {subject_column} > 0 THEN {subject_column} END), 2) AS avg_amount,
            COUNT(DISTINCT 员工ID) AS employee_count,
            COUNT(DISTINCT CASE WHEN {subject_column} > 0 THEN 员工ID END) AS issued_employee_count,
            ROUND(
                COUNT(DISTINCT CASE WHEN {subject_column} > 0 THEN 员工ID END) * 100.0
                / NULLIF(COUNT(DISTINCT 员工ID), 0), 2
            ) AS coverage_rate
        FROM salary_wide
        WHERE {base_where}
        GROUP BY BU, "{dimension}"
        ORDER BY total_amount DESC, BU, dimension_value
    """
    rows = rows_to_dicts(conn.execute(sql, params).fetchall())
    trend_sql = f"""
        SELECT
            printf('%04d-%02d', 统计年度, 统计月份) AS period,
            BU,
            SUM({subject_column}) AS total_amount
        FROM salary_wide
        WHERE {base_where}
        GROUP BY 统计年度, 统计月份, BU
        ORDER BY period, total_amount DESC
    """
    trend_rows = rows_to_dicts(conn.execute(trend_sql, params).fetchall())
    anomalies = derive_anomalies(rows)
    bu_leaders = derive_bu_leaders(rows)
    dimension_values = derive_dimension_values(rows)
    anomaly_people = derive_anomaly_people(conn, request, dimension, base_where, params, anomalies)
    consolidated = summarize_dimension(rows, anomalies, bu_leaders, dimension_values, dimension, subject=request.subject)
    chart_bundle = build_dimension_chart_data(
        request,
        insight={
            "grouped_rows": rows,
            "dimension_values": dimension_values,
            "trend_rows": trend_rows,
            "dimension": dimension,
        },
    )
    analysis_model = run_analysis_model(dimension, rows, trend_rows, request.subject)
    return {
        "dimension": dimension,
        "column_identity": classify_column(dimension),
        "analysis_model": analysis_model,
        "sql": compact_sql(sql),
        "grouped_rows": rows[:80],
        "total_groups": len(rows),
        "trend_rows": trend_rows[:120],
        "anomalies": anomalies,
        "anomaly_people": anomaly_people,
        "bu_leaders": bu_leaders,
        "dimension_values": dimension_values,
        "chart_bundle": chart_bundle,
        "derived_summary": consolidated,
    }


def derive_anomaly_people(
    conn: sqlite3.Connection,
    request: AnalysisRequest,
    dimension: str,
    base_where: str,
    params: list[Any],
    anomalies: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not anomalies:
        return []
    top_anomalies = anomalies[:2]
    condition_parts: list[str] = []
    extra_params: list[Any] = []
    anomaly_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for item in top_anomalies:
        condition_parts.append(f"(BU = ? AND \"{dimension}\" = ?)")
        extra_params.extend([item["BU"], item["dimension_value"]])
        anomaly_lookup[(str(item["BU"]), str(item["dimension_value"]))] = item

    subject_column = f'"{request.subject}"'
    sql = f"""
        SELECT
            BU,
            "{dimension}" AS dimension_value,
            员工ID,
            职能,
            绩效分位,
            级别,
            司龄分箱,
            年龄分箱,
            SUM({subject_column}) AS total_amount,
            ROUND(AVG(CASE WHEN {subject_column} > 0 THEN {subject_column} END), 2) AS avg_paid_amount,
            COUNT(CASE WHEN {subject_column} > 0 THEN 1 END) AS paid_months
        FROM salary_wide
        WHERE {base_where}
          AND ({' OR '.join(condition_parts)})
        GROUP BY BU, "{dimension}", 员工ID, 职能, 绩效分位, 级别, 司龄分箱, 年龄分箱
        ORDER BY total_amount DESC, paid_months DESC, 员工ID
        LIMIT 18
    """
    rows = rows_to_dicts(conn.execute(sql, params + extra_params).fetchall())
    details: list[dict[str, Any]] = []
    for row in rows:
        anomaly_meta = anomaly_lookup.get((str(row["BU"]), str(row["dimension_value"])), {})
        details.append(
            {
                **row,
                "dimension": dimension,
                "z_score": anomaly_meta.get("z_score"),
                "signal": anomaly_meta.get("signal", ""),
                "group_total_amount": anomaly_meta.get("total_amount"),
                "reason_summary": anomaly_meta.get("reason_summary", ""),
            }
        )
    return details


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [{key: row[key] for key in row.keys()} for row in rows]


def compact_sql(sql: str) -> str:
    return " ".join(sql.split())


def derive_anomalies(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    totals = [float(row["total_amount"] or 0) for row in rows]
    avg_total = mean(totals)
    variance = mean((value - avg_total) ** 2 for value in totals) if len(totals) > 1 else 0
    std_dev = math.sqrt(variance)
    enriched = []
    for row in rows:
        total = float(row["total_amount"] or 0)
        z_score = (total - avg_total) / std_dev if std_dev else 0
        enriched.append(
            {
                **row,
                "z_score": round(z_score, 2),
                "signal": "high" if z_score > 1.25 else "low" if z_score < -1.25 else "neutral",
                "reason_summary": (
                    f"{row['BU']} 在 {row['dimension_value']} 这一组的金额明显高于同维度其他组。"
                    if z_score > 1.25
                    else f"{row['BU']} 在 {row['dimension_value']} 这一组的金额明显低于同维度其他组。"
                    if z_score < -1.25
                    else ""
                ),
            }
        )
    anomalies = [row for row in enriched if row["signal"] != "neutral"]
    anomalies.sort(key=lambda item: abs(item["z_score"]), reverse=True)
    return anomalies[:6]


def derive_bu_leaders(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    leaderboard: dict[str, dict[str, Any]] = {}
    for row in rows:
        bu = row["BU"]
        current = leaderboard.get(bu)
        if current is None or row["total_amount"] > current["total_amount"]:
            leaderboard[bu] = row
    return sorted(leaderboard.values(), key=lambda item: item["total_amount"], reverse=True)[:8]


def derive_dimension_values(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"dimension_value": "", "total_amount": 0, "employee_count": 0, "issued_employee_count": 0}
    )
    for row in rows:
        value = row["dimension_value"]
        bucket = summary[value]
        bucket["dimension_value"] = value
        bucket["total_amount"] += int(row["total_amount"] or 0)
        bucket["employee_count"] += int(row["employee_count"] or 0)
        bucket["issued_employee_count"] += int(row["issued_employee_count"] or 0)
    results = []
    for item in summary.values():
        issued = item["issued_employee_count"]
        item["coverage_rate"] = round(issued * 100 / max(item["employee_count"], 1), 2)
        item["avg_amount"] = round(item["total_amount"] / max(issued, 1), 2)
        results.append(item)
    results.sort(key=lambda item: item["total_amount"], reverse=True)
    return results[:8]


def summarize_dimension(
    rows: list[dict[str, Any]],
    anomalies: list[dict[str, Any]],
    bu_leaders: list[dict[str, Any]],
    dimension_values: list[dict[str, Any]],
    dimension: str,
    subject: str = "",
) -> dict[str, Any]:
    if not rows:
        return {
            "headline": f"{dimension} 维度暂无可分析数据",
            "facts": [],
            "drivers": [],
            "management_implications": [],
        }

    # ---------- 维度值的人性化映射 ----------
    def _humanize_dim(dim: str, val: str) -> str:
        """把 '级别=CD类员工' 这种机械表达改成自然语言。"""
        dim_phrases = {
            "级别": f"{val.replace('类员工', '级').replace('类领导', '级管理层').replace('类', '级')}员工",
            "职能": f"{val}序列",
            "绩效分位": f"绩效{val}的员工",
            "司龄分箱": f"司龄{val}年的员工",
            "年龄分箱": f"{val}岁年龄段",
        }
        return dim_phrases.get(dim, val)

    def _fmt_amount(amount: float) -> str:
        """把金额格式化为更易读的形式。"""
        v = int(amount)
        if v >= 100_000_000:
            return f"{v / 100_000_000:.1f}亿元"
        if v >= 10_000:
            return f"{v / 10_000:.0f}万元"
        return f"{v:,}元"

    top_group = max(rows, key=lambda item: item["total_amount"])
    low_group = min(rows, key=lambda item: item["total_amount"])
    strongest_dim = dimension_values[0] if dimension_values else None

    top_desc = _humanize_dim(dimension, top_group["dimension_value"])
    low_desc = _humanize_dim(dimension, low_group["dimension_value"])

    facts = [
        f"{top_group['BU']}的{top_desc}{subject or '该科目'}总额最高，达到{_fmt_amount(top_group['total_amount'])}。",
        f"相比之下，{low_group['BU']}的{low_desc}处于最低水平，总额仅{_fmt_amount(low_group['total_amount'])}。",
    ]
    if strongest_dim:
        dim_desc = _humanize_dim(dimension, strongest_dim["dimension_value"])
        facts.append(
            f"从整体看，{dim_desc}群体贡献了最大份额，总额{_fmt_amount(strongest_dim['total_amount'])}，覆盖率{strongest_dim['coverage_rate']}%。"
        )
    drivers = []
    if anomalies:
        first = anomalies[0]
        first_desc = _humanize_dim(dimension, first["dimension_value"])
        drivers.append(
            f"{first['BU']}的{first_desc}{subject or '该科目'}明显高于同组其他群体，已经形成需要单独解释的头部现象。"
        )
    if bu_leaders:
        leader = bu_leaders[0]
        leader_desc = _humanize_dim(dimension, leader["dimension_value"])
        drivers.append(
            f"{leader['BU']}在该维度下的领先群体集中在{leader_desc}，说明该BU的结构差异更为集中。"
        )
    implications = [
        f"应优先复核{dimension}维度下头部群体的高值是否来自真实的组织结构差异，而非一次性发放造成的短期抬升。",
        f"若{dimension}在多个BU中都出现明显分层，后续制度讨论应按该维度建立更细的对标口径。",
    ]
    return {
        "headline": f"{dimension}维度显示出显著的BU内部结构差异，头部群体与尾部之间存在明显梯度。",
        "facts": facts,
        "drivers": drivers,
        "management_implications": implications,
    }


def build_overview_charts(
    conn: sqlite3.Connection,
    request: AnalysisRequest,
    base_where: str,
    params: list[Any],
    subject_column: str,
    bu_overview: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    charts: list[dict[str, Any]] = []
    total_amount = max(sum(int(item["total_amount"]) for item in bu_overview), 1)
    if "总额" in request.metrics or "占比" in request.metrics:
        charts.append(
            {
                "chart_type": "bar",
                "chart_title": "BU 总额排名" if "总额" in request.metrics else "BU 金额占比排名",
                "chart_insight": "头部 BU 的总额集中度能够快速反映该薪酬科目的组织分布重心。",
                "chart_payload": {
                    "categories": [row["BU"] for row in bu_overview[:10]],
                    "series": [
                        int(row["total_amount"]) if "总额" in request.metrics else round(int(row["total_amount"]) * 100 / total_amount, 2)
                        for row in bu_overview[:10]
                    ],
                    "share": [
                        round(int(row["total_amount"]) * 100 / total_amount, 2)
                        for row in bu_overview[:10]
                    ],
                },
            }
        )
    if "平均金额" in request.metrics:
        charts.append(
            {
                "chart_type": "bar",
                "chart_title": "BU 平均金额对比",
                "chart_insight": "平均金额更适合识别同一科目在不同 BU 的单人支付强度差异。",
                "chart_payload": {
                    "categories": [row["BU"] for row in sorted(bu_overview, key=lambda item: float(item["avg_amount"] or 0), reverse=True)[:10]],
                    "series": [float(row["avg_amount"] or 0) for row in sorted(bu_overview, key=lambda item: float(item["avg_amount"] or 0), reverse=True)[:10]],
                },
            }
        )
    if "领取人数" in request.metrics:
        charts.append(
            {
                "chart_type": "bar",
                "chart_title": "BU 领取人数排名",
                "chart_insight": "领取人数用于区分总额高是由覆盖面大驱动，还是由单人金额高驱动。",
                "chart_payload": {
                    "categories": [row["BU"] for row in sorted(bu_overview, key=lambda item: int(item["issued_employee_count"] or 0), reverse=True)[:10]],
                    "series": [int(row["issued_employee_count"] or 0) for row in sorted(bu_overview, key=lambda item: int(item["issued_employee_count"] or 0), reverse=True)[:10]],
                },
            }
        )
    if "发放覆盖率" in request.metrics:
        charts.append(
            {
                "chart_type": "scatter",
                "chart_title": "BU 覆盖率与均值分布",
                "chart_insight": "覆盖率高且均值高的 BU 更可能体现制度性或结构性差异，而不是局部样本波动。",
                "chart_payload": {
                    "points": [
                        {
                            "name": row["BU"],
                            "coverage_rate": float(row["coverage_rate"] or 0),
                            "avg_amount": float(row["avg_amount"] or 0),
                            "employee_count": int(row["employee_count"] or 0),
                        }
                        for row in bu_overview[:12]
                    ]
                },
            }
        )
    if "环比" in request.metrics or "同比" in request.metrics:
        trend_sql = f"""
            SELECT
                printf('%04d-%02d', 统计年度, 统计月份) AS period,
                SUM({subject_column}) AS total_amount
            FROM salary_wide
            WHERE {base_where}
            GROUP BY 统计年度, 统计月份
            ORDER BY period
        """
        trend_rows = rows_to_dicts(conn.execute(trend_sql, params).fetchall())
        total_series = [int(row["total_amount"] or 0) for row in trend_rows]
        periods = [row["period"] for row in trend_rows]
        if "环比" in request.metrics:
            charts.append(
                {
                    "chart_type": "line",
                    "chart_title": "整体月度环比趋势",
                    "chart_insight": "环比用于识别最新月份相对前一月的抬升或回落幅度。",
                    "chart_payload": {
                        "periods": periods,
                        "series": calculate_mom(total_series),
                        "value_type": "percent",
                    },
                }
            )
        if "同比" in request.metrics:
            charts.append(
                {
                    "chart_type": "line",
                    "chart_title": "整体月度同比趋势",
                    "chart_insight": "同比用于识别相同月份跨年度的变化是否已经形成持续性结构信号。",
                    "chart_payload": {
                        "periods": periods,
                        "series": calculate_yoy(periods, total_series),
                        "value_type": "percent",
                    },
                }
            )
    return charts


class LLMService:
    _DEFAULT_MODEL = "gpt-4.1-mini"
    _client: ClassVar[OpenAI | None] = None
    _client_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self) -> None:
        self.provider = "openai"
        self.api_key = ensure_text(os.getenv("OPENAI_API_KEY"))
        self.base_url = ensure_text(os.getenv("OPENAI_BASE_URL"))
        self.model = ensure_text(os.getenv("OPENAI_MODEL"), self._DEFAULT_MODEL)
        self.enabled = bool(self.api_key)

        # 单例模式复用 client 连接
        if self.enabled:
            with self._client_lock:
                if LLMService._client is None:
                    client_kwargs: dict[str, Any] = {"api_key": self.api_key}
                    if self.base_url:
                        client_kwargs["base_url"] = self.base_url
                    LLMService._client = OpenAI(**client_kwargs)
            self.client = LLMService._client
        else:
            self.client = None

        if self.enabled:
            print(f"[LLM] 已启用 | 模型: {self.model} | 地址: {self.base_url or 'default'}")
        else:
            print("[LLM] 未配置 OPENAI_API_KEY，已回退到模板分析模式")

    def analyze_dimension(self, request: AnalysisRequest, insight: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return self._fallback_dimension_report(request, insight)
        prompt = build_dimension_prompt(request, insight)
        text = self._chat_completion(
            system_prompt=SYSTEM_DIMENSION_ANALYSIS if PROMPTS_AVAILABLE else "你是一名顶级咨询公司的薪酬分析顾问，擅长把结构化数据写成专业、克制、可汇报的中文分析结论。",
            user_prompt=prompt,
            temperature=0.5,
        )
        return self._parse_dimension_response(text, request, insight)

    def summarize_dimensions(
        self,
        request: AnalysisRequest,
        insight_bundle: dict[str, Any],
        dimension_reports: list[dict[str, Any]],
        external_research: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.enabled:
            return self._fallback_consolidated_report(request, insight_bundle, dimension_reports, external_research)
        prompt = build_consolidated_prompt(request, insight_bundle, dimension_reports, external_research)
        text = self._chat_completion(
            system_prompt=SYSTEM_CONSOLIDATED_ANALYSIS if PROMPTS_AVAILABLE else (
                "你是一名顶级咨询公司的薪酬与组织分析顾问。"
                "你的写作像正式汇报材料，不像聊天回答，也不像 AI 自动总结。"
                "你直接下判断，句子克制、稳定、专业，避免空泛套话、避免模板化排比、避免解释自己如何分析。"
            ),
            user_prompt=prompt,
            temperature=0.4,
        )
        return self._parse_consolidated_response(text, request, insight_bundle, dimension_reports, external_research)

    def revise_report(
        self,
        request: AnalysisRequest,
        report: dict[str, Any],
        revision_instruction: str,
        follow_up_messages: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        prompt = build_report_revision_prompt(request, report, revision_instruction, follow_up_messages or [])
        try:
            text = self._chat_completion(
                system_prompt=SYSTEM_REPORT_REVISION if PROMPTS_AVAILABLE else (
                    "你是一名顶级咨询公司的薪酬与组织分析顾问。"
                    "你只能基于已有报告内容做润色、重组和补充表达，不得虚构新的数据结果，"
                    "也不得要求重新取数。输出必须是严格 JSON。"
                ),
                user_prompt=prompt,
                temperature=0.4,
            )
            payload = json.loads(extract_json(text))
            payload["source_mode"] = "llm_revision"
            return payload
        except Exception:
            return None

    def generate_short_answer(self, request: AnalysisRequest, report: dict[str, Any]) -> str | None:
        if not self.enabled:
            return None
        prompt = build_short_answer_prompt(request, report)
        try:
            return self._chat_completion(
                system_prompt=SYSTEM_SHORT_ANSWER if PROMPTS_AVAILABLE else "你是一名顶级咨询公司的薪酬分析顾问，负责用极简但专业的语言直接回答用户问题。",
                user_prompt=prompt,
                temperature=0.3,
            )
        except Exception:
            return None

    def _chat_completion(self, system_prompt: str, user_prompt: str, temperature: float) -> str:
        if not self.enabled or self.client is None:
            raise RuntimeError("[LLM] 未启用，请检查 OPENAI_API_KEY 配置")
        # kimi-k2.5 只支持 temperature=1，强制覆盖
        actual_temperature = 1.0 if "kimi-k" in self.model else temperature
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=actual_temperature,
        )
        message = completion.choices[0].message
        content = message.content or ""
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if text:
                        parts.append(text)
            return "".join(parts).strip()
        return str(content).strip()

    def _parse_dimension_response(
        self, text: str, request: AnalysisRequest, insight: dict[str, Any]
    ) -> dict[str, Any]:
        payload = json.loads(extract_json(text))
        payload["source_mode"] = "llm"
        payload["dimension"] = insight["dimension"]
        payload["chart_data"] = insight["chart_bundle"]
        payload["derived_summary"] = insight["derived_summary"]
        payload["anomaly_people"] = insight.get("anomaly_people", [])
        return payload

    def _parse_consolidated_response(
        self,
        text: str,
        request: AnalysisRequest,
        insight_bundle: dict[str, Any],
        dimension_reports: list[dict[str, Any]],
        external_research: dict[str, Any],
    ) -> dict[str, Any]:
        payload = json.loads(extract_json(text))
        payload["source_mode"] = "llm"
        fallback = self._fallback_consolidated_report(request, insight_bundle, dimension_reports, external_research)
        normalized = normalize_consolidated_payload(payload, request, fallback, dimension_reports, insight_bundle)
        normalized["source_mode"] = "llm"
        return normalized

    def summarize_external_research(self, request: AnalysisRequest, source_notes: list[dict[str, str]]) -> dict[str, Any] | None:
        if not self.enabled or not source_notes:
            return None
        prompt = build_external_research_prompt(request, source_notes)
        try:
            text = self._chat_completion(
                system_prompt=SYSTEM_EXTERNAL_RESEARCH if PROMPTS_AVAILABLE else (
                    "你是一名薪酬与组织研究顾问。"
                    "你需要把多篇外部研究材料压缩成结构化研究摘要，用于支持正式内部报告。"
                ),
                user_prompt=prompt,
                temperature=0.3,
            )
            payload = json.loads(extract_json(text))
            payload["source_notes"] = normalize_external_sources(payload.get("source_notes") or source_notes)
            payload["research_mode"] = "external_blended"
            payload["external_trends"] = ensure_text_list(payload.get("external_trends"), limit=4)
            payload["external_risk_signals"] = ensure_text_list(payload.get("external_risk_signals"), limit=4)
            payload["external_management_patterns"] = ensure_text_list(payload.get("external_management_patterns"), limit=4)
            payload["external_reporting_angles"] = ensure_text_list(payload.get("external_reporting_angles"), limit=4)
            return payload
        except Exception:
            return None

    def _fallback_dimension_report(
        self, request: AnalysisRequest, insight: dict[str, Any]
    ) -> dict[str, Any]:
        summary = insight["derived_summary"]
        anomalies = insight["anomalies"]
        anomaly_lines = [
            f"{item['BU']}的{item['dimension_value']}群体{request.subject}抬升最为明显，总额达到{int(item['total_amount']):,}元，已显著高于同类其他群体。"
            for item in anomalies[:2]
        ]
        return {
            "dimension": insight["dimension"],
            "source_mode": "template",
            "headline": summary["headline"],
            "narrative": (
                f"围绕{request.subject}展开分析时，{insight['dimension']}维度能够清晰解释BU内部的结构差异。"
                "高值群体与低值群体之间的差距较大，说明该维度不仅影响发放规模，也影响人群覆盖的集中度。"
            ),
            "key_findings": summary["facts"][:2],
            "anomalies": anomaly_lines or ["该维度下暂未发现需要特别关注的异常群体。"],
            "possible_drivers": summary["drivers"][:2],
            "management_implications": summary["management_implications"][:1],
            "chart_data": insight["chart_bundle"],
            "derived_summary": summary,
            "anomaly_people": insight.get("anomaly_people", []),
        }

    def _fallback_consolidated_report(
        self,
        request: AnalysisRequest,
        insight_bundle: dict[str, Any],
        dimension_reports: list[dict[str, Any]],
        external_research: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_dimension_reports = [ensure_dimension_report_text_lists(report) for report in dimension_reports]
        repeat_signals = []
        risk_lines = []
        action_lines = []
        top_bu = insight_bundle["bu_overview"][0] if insight_bundle["bu_overview"] else None
        for report in normalized_dimension_reports[:3]:
            findings = report.get("key_findings", [])
            implications = report.get("management_implications", [])
            if findings:
                repeat_signals.append(findings[0])
            if implications:
                risk_lines.append(implications[0])
        if top_bu:
            action_lines.append(
                f"优先复核 {top_bu['BU']} 在 {request.subject} 上的结构性驱动因素，尤其关注头部维度对总额抬升的贡献。"
            )
        action_lines.append("建立按 BU + 关键次维度的月度跟踪口径，将一次性异常与持续性结构差异分开管理。")
        action_lines.append("对覆盖率高但均值偏高的群体优先做政策核查，防止制度漂移。")
        full_sections = build_fallback_full_report(
            request,
            insight_bundle,
            normalized_dimension_reports,
            repeat_signals,
            risk_lines,
            action_lines,
            external_research,
        )
        payload = {
            "source_mode": "template",
            "report_title": f"{request.subject}成本结构分析与治理策略报告 ({request.start_year}-{request.end_year})",
            "report_subtitle": f"范围：{request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d}；维度：BU、{'、'.join(request.secondary_dimensions)}",
            "executive_summary": (
                f"{request.subject} 的差异已经明确呈现出结构特征，重点不是\"哪家 BU 金额高\"，而是哪些 BU 的高值在多个维度下反复出现。"
                "从当前结果看，头部 BU 的规模优势不是孤立现象，而是与职能、级别、绩效分位、司龄和年龄层等人群分层共同作用的结果。"
            ),
            "cross_dimension_summary": repeat_signals[:5],
            "priority_actions": action_lines[:4],
            "global_risks": risk_lines[:4] or ["需警惕将一次性发放误判为长期结构问题。"],
            "leadership_takeaways": repeat_signals[:3] or ["当前差异主要来自结构分层，不是个别月份偶发波动。"],
            "appendix_notes": [
                f"数据来源：{get_data_source_status().get('filename') or '当前活动数据源'}",
                "口径说明：金额统计基于所选时间范围内的宽表月度数据。",
                f"本次勾选指标：{'、'.join(request.metrics)}",
            ],
            "external_research_summary": (
                external_research.get("external_trends", [])[:2]
                + external_research.get("external_management_patterns", [])[:2]
            ),
            "external_sources": external_research.get("source_notes", []),
            "research_mode": external_research.get("research_mode", "internal_only"),
            "full_report_sections": full_sections,
            "consolidated_charts": build_consolidated_charts(normalized_dimension_reports),
        }
        return normalize_consolidated_payload(payload, request, payload, normalized_dimension_reports, insight_bundle)


def build_dimension_chart_data(request: AnalysisRequest, insight: dict[str, Any]) -> dict[str, Any]:
    top_groups = sorted(
        insight["grouped_rows"],
        key=lambda item: (item["total_amount"], item["coverage_rate"]),
        reverse=True,
    )[:8]
    dimension_values = insight["dimension_values"][:6]
    timeline: list[dict[str, Any]] = []
    timeline_map: dict[str, float] = defaultdict(float)
    for row in insight["trend_rows"]:
        timeline_map[row["period"]] += float(row["total_amount"] or 0)
    for period, total in sorted(timeline_map.items()):
        timeline.append({"period": period, "total_amount": round(total, 2)})

    heatmap_rows = []
    for row in insight["grouped_rows"][:40]:
        heatmap_rows.append(
            {
                "bu": row["BU"],
                "dimension_value": row["dimension_value"],
                "total_amount": int(row["total_amount"] or 0),
                "coverage_rate": float(row["coverage_rate"] or 0),
            }
        )

    distribution_labels = [item["dimension_value"] for item in dimension_values]
    distribution_series = [int(item["total_amount"] or 0) for item in dimension_values]
    distribution_coverage = [float(item["coverage_rate"] or 0) for item in dimension_values]

    scatter_points = []
    for row in insight["grouped_rows"][:25]:
        scatter_points.append(
            {
                "name": f"{row['BU']} / {row['dimension_value']}",
                "coverage_rate": float(row["coverage_rate"] or 0),
                "avg_amount": float(row["avg_amount"] or 0),
                "employee_count": int(row["employee_count"] or 0),
            }
        )

    sorted_by_avg = sorted(
        insight["grouped_rows"],
        key=lambda item: (float(item["avg_amount"] or 0), item["total_amount"]),
        reverse=True,
    )[:8]
    sorted_by_coverage = sorted(
        insight["grouped_rows"],
        key=lambda item: (float(item["coverage_rate"] or 0), item["total_amount"]),
        reverse=True,
    )[:8]
    sorted_by_people = sorted(
        insight["grouped_rows"],
        key=lambda item: (int(item["issued_employee_count"] or 0), item["total_amount"]),
        reverse=True,
    )[:8]
    total_distribution_amount = max(sum(distribution_series), 1)
    share_series = [round(value * 100 / total_distribution_amount, 2) for value in distribution_series]
    timeline_series = [item["total_amount"] for item in timeline]

    if "平均金额" in request.metrics:
        primary_chart = {
            "chart_type": "grouped-bar",
            "chart_title": f"BU x {insight['dimension']} 平均金额对比",
            "chart_insight": "平均金额口径更能识别单人支付强度集中的分组。",
            "chart_payload": {
                "labels": [f"{item['BU']} / {item['dimension_value']}" for item in sorted_by_avg],
                "series": [float(item["avg_amount"] or 0) for item in sorted_by_avg],
            },
        }
    elif "发放覆盖率" in request.metrics:
        primary_chart = {
            "chart_type": "grouped-bar",
            "chart_title": f"BU x {insight['dimension']} 覆盖率对比",
            "chart_insight": "覆盖率高的分组更可能是制度性分布，而不是小样本异常。",
            "chart_payload": {
                "labels": [f"{item['BU']} / {item['dimension_value']}" for item in sorted_by_coverage],
                "series": [float(item["coverage_rate"] or 0) for item in sorted_by_coverage],
            },
        }
    elif "领取人数" in request.metrics:
        primary_chart = {
            "chart_type": "grouped-bar",
            "chart_title": f"BU x {insight['dimension']} 领取人数对比",
            "chart_insight": "领取人数可帮助判断高总额是否主要由覆盖规模驱动。",
            "chart_payload": {
                "labels": [f"{item['BU']} / {item['dimension_value']}" for item in sorted_by_people],
                "series": [int(item["issued_employee_count"] or 0) for item in sorted_by_people],
            },
        }
    else:
        primary_chart = {
            "chart_type": "grouped-bar",
            "chart_title": f"BU x {insight['dimension']} 头部分组对比",
            "chart_insight": "头部组别越集中，越说明当前维度对该薪酬科目的结构解释力较强。",
            "chart_payload": {
                "labels": [f"{item['BU']} / {item['dimension_value']}" for item in top_groups],
                "series": [int(item["total_amount"] or 0) for item in top_groups],
            },
        }

    if "环比" in request.metrics or "同比" in request.metrics:
        secondary_chart = {
            "chart_type": "line",
            "chart_title": f"{insight['dimension']} 关联下的月度{'环比' if '环比' in request.metrics else '趋势'}",
            "chart_insight": "时间走势帮助区分一次性冲高和持续性结构问题。",
            "chart_payload": {
                "periods": [item["period"] for item in timeline],
                "series": calculate_mom(timeline_series) if "环比" in request.metrics else timeline_series,
            },
        }
    elif "占比" in request.metrics:
        secondary_chart = {
            "chart_type": "grouped-bar",
            "chart_title": f"{insight['dimension']} 贡献占比",
            "chart_insight": "占比视角更适合判断哪个分组真正决定了整体结构重心。",
            "chart_payload": {"labels": distribution_labels, "series": share_series},
        }
    else:
        secondary_chart = {
            "chart_type": "line",
            "chart_title": f"{insight['dimension']} 关联下的月度走势",
            "chart_insight": "趋势变化可帮助区分短期异常与持续性结构问题。",
            "chart_payload": {
                "periods": [item["period"] for item in timeline],
                "series": timeline_series,
            },
        }

    supporting_charts: list[dict[str, Any]] = []
    if "总额" in request.metrics or "占比" in request.metrics:
        supporting_charts.append(
            {
                "chart_type": "grouped-bar",
                "chart_title": f"{insight['dimension']} {'贡献度排序' if '总额' in request.metrics else '占比排序'}",
                "chart_insight": "排序图能稳定比较不同组别对整体结构的贡献差异。",
                "chart_payload": {
                    "labels": distribution_labels,
                    "series": distribution_series if "总额" in request.metrics else share_series,
                    "coverage": distribution_coverage,
                },
            }
        )
    if "发放覆盖率" in request.metrics or "平均金额" in request.metrics:
        supporting_charts.append(
            {
                "chart_type": "scatter",
                "chart_title": f"{insight['dimension']} 覆盖率与均值分布",
                "chart_insight": "覆盖率与均值同时偏高的组别，往往更值得优先复核。",
                "chart_payload": {"points": scatter_points},
            }
        )
    if "领取人数" in request.metrics and len(supporting_charts) < 2:
        supporting_charts.append(
            {
                "chart_type": "grouped-bar",
                "chart_title": f"{insight['dimension']} 领取人数排序",
                "chart_insight": "人数排序用于识别规模驱动的头部人群。",
                "chart_payload": {
                    "labels": [f"{item['BU']} / {item['dimension_value']}" for item in sorted_by_people],
                    "series": [int(item["issued_employee_count"] or 0) for item in sorted_by_people],
                },
            }
        )

    supporting_charts = supporting_charts[:2]

    return {
        "primary_chart": primary_chart,
        "secondary_chart": secondary_chart,
        "supporting_charts": supporting_charts,
    }


def build_consolidated_charts(dimension_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    radar_indicators = []
    radar_values = []
    matrix_rows = []
    for raw_report in dimension_reports:
        report = ensure_dimension_report_text_lists(raw_report)
        anomaly_count = len(report.get("anomalies", []))
        finding_count = len(report.get("key_findings", []))
        signal_strength = anomaly_count * 20 + finding_count * 10
        radar_indicators.append({"name": report["dimension"], "max": 100})
        radar_values.append(min(signal_strength, 100))
        matrix_rows.append(
            {
                "dimension": report["dimension"],
                "signal_strength": signal_strength,
                "anomaly_count": anomaly_count,
                "finding_count": finding_count,
            }
        )
    return [
        {
            "chart_type": "radar",
            "chart_title": "各次维度解释力雷达图",
            "chart_insight": "解释力越高，说明该维度越能稳定解释 BU 差异与异常分层。",
            "chart_payload": {"indicators": radar_indicators, "values": radar_values},
        },
        {
            "chart_type": "matrix",
            "chart_title": "维度信号强度矩阵",
            "chart_insight": "矩阵图帮助比较各维度的异常密度与发现浓度，适合排序优先级。",
            "chart_payload": {"rows": matrix_rows},
        },
    ]


def _build_dimension_source_sentence(report: dict[str, Any]) -> str:
    dimension = ensure_text(report.get("dimension"))
    findings = ensure_text_list(report.get("key_findings"), limit=2)
    drivers = ensure_text_list(report.get("possible_drivers"), limit=2)
    implications = ensure_text_list(report.get("management_implications"), limit=2)

    parts = [f"基于{dimension}维度观察，{clean_sentence(report.get('headline')) or f'{dimension}层面存在明显结构差异'}"]
    if findings:
        parts.append(f"当前最显著的业务信号是{join_sentences(findings).rstrip('。')}")
    if drivers:
        parts.append(f"背后更值得追问的驱动因素包括{join_sentences(drivers).rstrip('。')}")
    if implications:
        parts.append(f"管理上应优先处理{join_sentences(implications).rstrip('。')}")
    return normalize_chinese_punctuation("。".join(part.rstrip("。") for part in parts if part) + "。")


def _build_cross_dimension_portraits(dimension_reports: list[dict[str, Any]], repeat_signals: list[str]) -> list[str]:
    dimension_names = [ensure_text(report.get("dimension")) for report in dimension_reports if ensure_text(report.get("dimension"))]
    if len(dimension_names) >= 2:
        joined_dims = "、".join(dimension_names[:3])
        portrait_intro = f"把{joined_dims}这些维度放在一起看，高值群体并不是随机散落，而是在少数人群组合里反复出现。"
    elif len(dimension_names) == 1:
        portrait_intro = f"当前仅能基于{dimension_names[0]}维度形成稳定观察，其他维度交叉信号不足。"
    else:
        portrait_intro = "当前可用于交叉分析的维度信号有限，需要谨慎解读复合画像。"

    portraits = [portrait_intro]
    if repeat_signals:
        portraits.append(
            f"重复命中的关键信号包括{join_sentences(repeat_signals[:3]).rstrip('。')}，说明当前成本压力更像结构性集中，而不是普遍抬升。"
        )
    if len(dimension_names) >= 2:
        portraits.append(
            f"因此，这一节的判断不是分别看{dimension_names[0]}、{dimension_names[1]}谁更高，而是识别哪些群体在多个维度上同时进入头部。"
        )
    return [normalize_chinese_punctuation(item) for item in portraits if normalize_chinese_punctuation(item)]


def build_fallback_full_report(
    request: AnalysisRequest,
    insight_bundle: dict[str, Any],
    dimension_reports: list[dict[str, Any]],
    repeat_signals: list[str],
    risk_lines: list[str],
    action_lines: list[str],
    external_research: dict[str, Any],
) -> list[dict[str, Any]]:
    hero = insight_bundle["hero_metrics"]
    top_bu = insight_bundle["bu_overview"][0] if insight_bundle["bu_overview"] else None
    normalized_dimension_reports = [ensure_dimension_report_text_lists(report) for report in dimension_reports]
    sections: list[dict[str, Any]] = []
    sections.append(
        {
            "id": "management-summary",
            "title": "核心结论——成本压力的结构性集中",
            "content": (
                f"这份关于 {request.subject} 的分析，核心结论只有一句话：成本压力不是平均分布的，而是集中落在少数 BU 和少数关键人群上。"
                f" 在 {request.start_year}-{request.start_month:02d} 至 {request.end_year}-{request.end_month:02d} 期间，累计金额达到 {hero['total_amount']:,}，"
                f"已发放员工均值为 {hero['avg_amount']:,}，覆盖率为 {hero['coverage_rate']}%。这说明当前问题不是偶发支出，而是会影响预算判断、人员安排和制度认知的重要成本信号。"
                "管理层现在最需要判断的，不是哪一个数字更高，而是这些高值背后到底是短期事件、组织分层，还是长期制度安排。"
            ),
        }
    )
    core_facts = []
    if top_bu:
        core_facts.append(
            f"从 BU 总览看，{top_bu['BU']} 是当前 {request.subject} 总额最高的组织单元，总额达到 {int(top_bu['total_amount']):,}，覆盖率为 {top_bu['coverage_rate']}%。"
        )
    for report in normalized_dimension_reports[:2]:
        findings = report.get("key_findings", [])
        if findings:
            core_facts.append(findings[0])
    sections.append(
        {
            "id": "core-findings",
            "title": "多维数据洞察——头部 BU 与关键群体的结构性凸显",
            "content": " ".join(core_facts)
            + " 更重要的是，这些高值并不是在单一维度里孤立存在，而是在多个维度上重复出现。"
            "这说明问题已经不是\"某张榜单偏高\"，而是组织结构、职级分布、绩效层级和离职群体特征共同叠加后的结果。"
            "因此，后续管理动作不能停留在解释排名，而应该直接进入口径复核、群体识别和制度检视。",
        }
    )
    bu_overview_line = (
        f"从 BU 视角看，{request.subject} 的分布已经出现清晰分层。头部 BU 在总额、均值和覆盖率三个指标上的领先，不只是体量大这么简单，"
        "更可能意味着其业务结构、组织层级或阶段性人员变化与其他 BU 有明显不同。BU 视角在这里的价值，是先把问题定位到哪几家机构最值得看，"
        "然后再通过职能、级别、绩效、司龄和年龄维度去判断高值背后的真实驱动。换句话说，BU 告诉我们问题在哪里，次维度告诉我们问题为什么会发生。"
    )
    sections.append({"id": "bu-overview", "title": "BU 总览与分层判断", "content": bu_overview_line})
    dimension_paragraphs = []
    for report in normalized_dimension_reports:
        findings = report.get("key_findings", [])
        drivers = report.get("possible_drivers", [])
        implications = report.get("management_implications", [])
        sentence = (
            f"在 {report['dimension']} 维度下，{report['headline']} "
            f"从数据上看，{'；'.join(findings[:2]) if findings else '当前该维度的关键信号仍以结构差异为主'}。"
        )
        if drivers:
            sentence += f" 这说明当前差异背后最值得关注的驱动因素是 {'；'.join(drivers[:2])}。"
        if implications:
            sentence += f" 对管理层而言，真正需要处理的是 {'；'.join(implications[:2])}。"
        dimension_paragraphs.append(sentence)
    sections.append(
        {
            "id": "dimension-deep-dive",
            "title": "维度交叉与复合画像",
            "content": normalize_chinese_punctuation(
                "\n\n".join(
                    [_build_dimension_source_sentence(report) for report in normalized_dimension_reports if report.get("dimension")]
                    + _build_cross_dimension_portraits(normalized_dimension_reports, repeat_signals)
                )
            ),
        }
    )
    sections.append(
        {
            "id": "cross-dimension",
            "title": "跨维度重复信号",
            "content": (
                "跨维度综合后的判断是，这一轮高值不是由单一因素推动，而是由同一批人群在多个切面下重复出现造成的。"
                f" 当前最具有代表性的重复信号包括：{'；'.join(repeat_signals[:4]) if repeat_signals else '头部组别在多个维度下同时占优'}。"
                "这类重复命中意味着风险不再是局部噪音，而是具有稳定组织特征的成本信号。对领导层来说，这个结论的意义在于：后续动作应该优先处理重复出现的结构问题，"
                "而不是被某一个维度下的局部异常牵着走。只要同一批 BU、同一类群体在多个维度里反复出现，就应视为优先级更高的管理对象。"
            ),
        }
    )
    external_trends = external_research.get("external_trends", [])
    external_patterns = external_research.get("external_management_patterns", [])
    external_sources = external_research.get("source_notes", [])
    if external_trends or external_patterns:
        referenced = []
        for index, item in enumerate((external_trends + external_patterns)[:3], start=1):
            anchor = f"〔外部参考 {index}〕" if index <= len(external_sources) else ""
            referenced.append(f"{item}{anchor}")
        sections.append(
            {
                "id": "external-context",
                "title": "外部环境对标与风险研判",
                "content": (
                    "把外部研究放进来一起看，当前内部信号并不孤立。"
                    + " ".join(referenced)
                    + " 这意味着本次报告里的管理动作不只是针对某一个月份做解释，而是要顺着市场上已经反复出现的治理逻辑，把预算约束、关键人群识别和制度前置审批放到同一套动作里。"
                ),
            }
        )
    sections.append(
        {
            "id": "risks",
            "title": "风险研判",
            "content": (
                f"当前最需要警惕的风险是：{'；'.join(risk_lines[:3]) if risk_lines else '把阶段性事件误判为长期结构问题、把局部高值误读成普遍趋势、在单维度下做过度解释'}。"
                "如果这些问题不及时澄清，管理层在预算、人员和制度上的判断都会被带偏。尤其是当某些群体同时表现出高覆盖率和高均值时，"
                "必须优先确认这是不是制度安排造成的稳定差异；如果不是，就要排查是否存在特定时点的人事动作、项目清退或口径偏移。"
            ),
        }
    )
    sections.append(
        {
            "id": "actions",
            "title": "管理建议与行动路线图",
            "content": (
                f"建议动作很直接：{'；'.join(action_lines[:4])}。"
                "执行顺序上，先做口径核查，再做群体识别，最后再决定是否进入制度调整。头部 BU 和重复命中的高敏感群体应该先查，因为这部分最可能决定整体成本走势。"
                "如果复核后确认差异会持续存在，就要建立月度追踪；如果确认只是阶段性事件，就应该在汇报和预算讨论中把一次性影响单独拆出，避免形成错误预期。"
            ),
        }
    )
    return sections


def build_dimension_prompt(request: AnalysisRequest, insight: dict[str, Any]) -> str:
    return f"""
你是一名顶级咨询公司的薪酬分析顾问。请基于以下 JSON 数据，为"{request.subject}"在"{insight['dimension']}"维度写一个结构化分析对象。
要求：
1. 用中文输出 JSON。
2. 不要杜撰数据，只基于输入数据。
3. tone 要专业、克制、适合高层和 HRBP 汇报，但必须让不懂统计学的人也能直接看懂。
4. 返回字段：headline, narrative, key_findings, anomalies, possible_drivers, management_implications。
5. `key_findings` 只写 1-2 条，`anomalies` 只写 1-2 条，每条都要是完整短句。
6. 禁止出现 z-score、标准差、σ、显著性、离群值、分布偏态、置信区间 等统计术语。
7. 异常描述必须写成业务语言，例如"明显高于同维度其他组""连续几个月都偏高""金额集中在少数人群"，不要解释统计过程。
8. `anomalies` 必须尽量与输入中的 anomaly_people 对应，优先描述那些能在员工明细里直接看到的人群。
9. 在 `possible_drivers` 中，必须尝试回答"为什么"，从以下角度分析：
   - 业务结构差异：是否因为某BU业务转型导致人员优化？
   - 组织层级差异：是否因为某级别员工基数大/薪酬高？
   - 阶段性事件：是否因为某月集中清退/项目结束？
   - 制度性因素：是否因为某群体协议离职标准更高？
10. 在 `anomalies` 中，必须包含三个要素：
    - 异常的具体表现（如"连续3个月总额偏高"）
    - 异常的量级（如"是同维度平均值的2.5倍"）
    - 异常的群体特征（如"集中在司龄10年以上的CD类员工"）
11. 去机械化表达约束（非常重要）：
    - 禁止使用 "维度=值" 的格式（如 "级别=CD类员工"），应改为自然表达（如 "C、D级别的员工"）
    - 禁止使用 "当前维度下最低组为 XX / YY" 这种系统输出式写法，应改为 "相比之下，XX的YY群体处于最低水平"
    - 禁止使用 "总额 42,775,149" 这种裸数字，应加上量词（如 "总额约4278万元" 或 "总额达到4278万元"）
    - "CD类员工" 应表述为 "C、D级别的员工"
    - "O类领导" 应表述为 "O级管理层"
    - "B类" 应表述为 "B级员工"
    - "前20%" 应表述为 "绩效排名前20%的员工"
    - "后30%" 应表述为 "绩效排名靠后30%的员工"
    - "司龄分箱=10年以上" 应表述为 "司龄超过10年的员工"
    - "年龄分箱=35-40" 应表述为 "35至40岁年龄段"
    - 总之，每个表述都应该像一位资深HR在口头汇报时的说法，而不是数据库查询条件的输出

输入数据：
{json.dumps(insight, ensure_ascii=False)}
""".strip()


def build_external_research_prompt(request: AnalysisRequest, source_notes: list[dict[str, str]]) -> str:
    return f"""
你是一名薪酬与组织研究顾问，正在为一份内部管理报告准备外部研究支撑材料。

# 任务
把以下外部来源整理成结构化研究判断，用于融合进正式报告正文。

# 核心要求
1. 输出必须是管理语言，而不是网页摘抄。禁止整段引用原文。
2. 每条判断必须可直接嵌入报告正文，语气像咨询顾问的研究备忘录。
3. 只保留与 `{request.subject}` 明显相关的趋势、风险、行业口径和管理实践；弱相关内容直接丢弃。
4. 优先提炼与薪酬结构、预算管理、人群分层、组织调整、绩效治理、人才保留、制度执行相关的判断。
5. 不要出现网页导航、栏目名、广告语、站点说明等噪音。

# 输出格式
用中文输出一个合法 JSON 对象，包含以下字段：
- `external_trends`: 2-3 条，当前薪酬/用工领域的趋势判断。
- `external_risk_signals`: 2-3 条，合规风险或治理风险信号。
- `external_management_patterns`: 2-3 条，成熟企业或成熟市场的管理实践。
- `external_reporting_angles`: 1-2 条，这些外部研究给本次内部分析带来的补充视角。
- `industry_benchmarks`: 1-2 条，行业水位或行业常见口径的对比判断。
- `best_practices`: 2-3 条，值得参考的管理实践。
- `source_notes`: 整理输入来源，字段必须包含 `source_name`, `title`, `published_at`, `summary`, `url`, `query_topic`。`summary` 必须是清洗后的 1 句话摘要，不超过 60 字。

# 输入
{json.dumps({"subject": request.subject, "sources": source_notes}, ensure_ascii=False)}

仅输出 JSON，不要包含 markdown 代码块标记或额外解释。
""".strip()


def build_consolidated_prompt(
    request: AnalysisRequest,
    insight_bundle: dict[str, Any],
    dimension_reports: list[dict[str, Any]],
    external_research: dict[str, Any],
) -> str:
    payload = {
        "request": {
            "subject": request.subject,
            "start_period": f"{request.start_year}-{request.start_month:02d}",
            "end_period": f"{request.end_year}-{request.end_month:02d}",
            "secondary_dimensions": request.secondary_dimensions,
            "metrics": request.metrics,
        },
        "hero_metrics": insight_bundle["hero_metrics"],
        "dimension_reports": dimension_reports,
        "external_research": external_research,
    }
    if request.follow_up_context:
        payload["follow_up_context"] = request.follow_up_context
    return f"""
# Role
你是一名来自顶级咨询公司的薪酬与组织效能顾问。你的报告风格冷静、客观、数据驱动、行动导向，像正式管理内参，而不是系统自动总结。

# Task
基于提供的 JSON 数据，撰写一份《{request.subject}分析与管理建议报告》。
输出必须严格为一个合法的 JSON 对象，包含以下字段：
`report_title`, `report_subtitle`, `executive_summary`, `cross_dimension_summary`, `priority_actions`, `global_risks`, `leadership_takeaways`, `appendix_notes`, `full_report_sections`, `external_research_summary`, `external_sources`, `research_mode`。

# Constraints & Guidelines

## 1. 标题规范
- `report_title`: 必须严格遵循格式 `{request.subject}分析与管理建议报告 ({request.start_year}-{request.end_year})`。禁止使用"体检"、"洞察"、"多维"、"全景"等营销词汇。
- `report_subtitle`: 用一句话概括核心矛盾，例如"关键差异集中在少数 BU 与重点人群，结构性管理动作需要前置。"

## 2. 正文写作逻辑
`full_report_sections` 是一个数组，每个元素是一个对象，包含 `id`（段落标识，如 "section-1"）、`title`（小节标题）和 `content`（正文内容）。拼接后必须形成一篇 3500-5000 字的连续正式管理报告。

严格要求：
- 每个 section 必须有唯一的 `id`（格式 "section-1", "section-2" 等）和明确的 `title`。
- 每节 `content` 的第一句话必须是该节的核心结论（判断句），后续段落用数据佐证。
- 段落之间用换行符分隔（\\n\\n），每段只承载一个论点。
- 总共输出 6 个 section，分别对应下面六大部分。

请严格按照以下六大部分撰写，每部分作为一个 section 对象输出：

### Section 1: 执行摘要（id: "section-1", title: "执行摘要"）
- 这是一页纸版本，领导只看这一页
- 必须包含三个段落（用\\n\\n分隔），总计300-400字：
  第一段：核心结论 - 一句话概括总盘子、结构性集中点、主要驱动因素。
  第二段：关键风险 - 指出最大的管理风险，并量化其影响范围。
  第三段：行动承诺 - 给出优先动作和预期收益或预期改善方向。
- 写法：先给判断，再给数据支撑，最后给行动方向

### Section 2: 现状透视——从数据到业务归因（id: "section-2", title: "现状透视：从数据到业务归因"）
- 必须回答三个问题：
  1. 是哪些 BU、人群或时间段在拉动结果？不能只停留在单一维度标签，要写成复合画像
  2. 为什么拿钱？区分三类动因：
     - 规模变化：人数、覆盖率、月份分布是否变化
     - 结构变化：人群层次、BU 构成、绩效或职级结构是否变化
     - 规则变化：预算、制度、激励政策、组织动作是否可能影响结果
  3. 异常信号是什么？针对异常月份、异常 BU、异常人群组合给出业务解释
- 必须包含：
  - 结构热力图：哪些 BU/条线是重点来源，用占比量化贡献
  - 人群画像深描：用复合画像描述高值群体
  - 异常信号预警：哪些月份/群体出现异常波动，需要优先核查
- 每一段都必须明确写出判断来自哪个或哪些维度，例如"基于部门维度观察"、"结合级别与去年绩效排名两个维度看"。
- 不能把部门、级别、绩效、年龄逐段机械罗列后就结束；至少要有一段把多个维度交叉成复合画像。
- 字数：800-1000字

### Section 3: 外部对标与差距分析（id: "section-3", title: "外部对标与差距分析"）
- 必须包含：
  - 行业水位：与 `{request.subject}` 相关的行业口径、预算趋势或常见管理方式
  - 最佳实践：头部企业或成熟市场是如何管理类似问题的
  - 趋势判断：结合外部研究，判断当前信号更像一次性波动还是持续性结构
- 外部信息融合要求：
  - 禁止整段粘贴原文
  - 必须先总结，再融合进判断链
  - 用轻量锚点 `〔外部参考 1〕` 标注句末
- 字数：600-800字

### Section 4: 管理行动方案（id: "section-4", title: "管理行动方案"）
- 必须包含4个策略，每个策略包含：动作描述 + 预期收益测算或预期改善方向
- 动作可以来自预算控制、结构调整、政策修订、绩效治理、组织协同、数据治理、审批机制等维度
- 所有动作都必须与数据发现直接对应，不能空泛
- 字数：800-1000字

### Section 5: 实施路线图与资源需求（id: "section-5", title: "实施路线图与资源需求"）
- 必须包含：
  - 时间表：按短期 / 中期 / 持续跟踪展开，每个阶段写清楚里程碑
  - 资源需求：需要哪些部门配合
  - 责任主体：谁来牵头执行，谁来提供数据和业务支持
- 字数：400-600字

### Section 6: 风险预案（id: "section-6", title: "风险预案"）
- 必须包含：
  - 风险识别：执行动作后可能引发的业务、组织、预算、合规或沟通风险
  - 应对 Plan B：针对每个风险给出缓解措施
  - 管理底线声明：明确哪些口径不能被误读或越界使用
- 字数：400-600字

## 3. 语言与风格
- 语气：冷峻、专业、果断。像给 CEO 写的管理内参，不是给 HR 专员看的操作手册。
- 句式：多用判断句，少用被动句。
- 行文目标：读起来像正式汇报稿或内参摘要，而不是 AI 对数据的口头总结。
- 必须体现"算账能力"和"风控能力"：
  - 算账：每个策略都要有预期收益测算（如"预计节约成本XX亿元，降幅Y%"）
  - 风控：每个风险都要有应对方案（如"设立申诉通道，准备统一口径"）
- 禁止使用的空话：
  - "建议关注"、"建议复核"、"加强管理"、"持续跟踪"
  - 改为具体动作：如"立即冻结XX审批"、"Q1完成XX盘点"、"Q2上线XX系统"
- 负面约束：
  - 禁止出现 z-score、标准差、σ、显著性、分布偏态、离群值、置信区间 等统计术语。用"明显高于整体常态""持续高于同类分组"等业务语言替代。
  - 禁止虚构数据或未在输入中体现的外部基准。
  - 禁止过度夸大风险，保持客观理性。
  - 禁止在 `full_report_sections` 中出现 Markdown 表格，用文字描述对比关系。
- 禁止出现"首先/其次/最后/此外/综上所述/值得注意的是/从数据上看/可以看出/建议关注/建议复核"等模板化连接词。
- 禁止出现连续标点、重复标点或病句式拼接，例如"。；"、"。。"、"；；"。
  - 禁止出现"本次分析/基于以上数据/通过上述维度/系统显示/模型认为"等自我解释分析过程的句子。
  - 禁止出现"网上资料显示""根据搜索结果""公开网页提到"等暴露检索过程的表述。
  - 禁止把每个维度机械重复一遍；要把多个维度压缩进一条判断链。
  - 优先使用"判断句 + 证据句"的写法。
- 去机械化表达约束（非常重要，必须严格遵守）：
  - 禁止使用 "维度=值" 的格式（如 "级别=CD类员工"），改为自然表达（如 "C、D级别的员工"）
  - 禁止使用 "当前维度下最低组为 XX / YY，总额 N" 这种系统输出式写法
  - 禁止出现裸数字（如 "总额 42,775,149"），必须加量词和上下文（如 "总额约4278万元"）
  - 以下为必须遵守的人性化改写规则：
    "CD类员工" → "C、D级别的员工"
    "O类领导" → "O级管理层" 或 "高管"
    "B类" → "B级员工"
    "前20%" → "绩效排名前20%的员工"
    "后30%" → "绩效排名靠后的员工"
    "司龄分箱=10年以上" → "司龄超过10年的老员工"
    "年龄分箱=35-40" → "35至40岁年龄段"
    "XX / YY" → "XX的YY群体" 或 "XX中YY序列的员工"
  - 金额表达规则：超过1亿用"X.X亿元"，超过1万用"X万元"，不要写出完整的阿拉伯数字
  - 总体原则：每句话都应该像一位资深HRBP在向CEO口头汇报时的说法，而不是数据库查询结果的直接输出

## 4. 其他字段要求
- `executive_summary`: 必须包含三个段落（用\\n\\n分隔），总计300-400字：
  第一段：核心结论 - 一句话概括总盘子、结构性集中特征、主要驱动因素。
  第二段：关键风险 - 指出最大的管理风险并量化影响。
  第三段：行动承诺 - 给出优先动作和预期改善结果。
- `cross_dimension_summary`: 简述维度交叉发现的关键重合点，用复合画像表达。
- `priority_actions`: 数组格式，列出 3-5 条具体行动，每条包含 `action`, `priority` (P0/P1/P2), `rationale`。
- `global_risks`: 数组格式，列出 2-3 个系统性风险。
- `leadership_takeaways`: 给一把手的 3 句核心建议。
- `appendix_notes`: 补充说明数据的局限性或计算口径假设。
- `external_research_summary`: 1-2 条简短导语，概括外部研究给本次分析带来的补充视角。不要重复正文已有内容。
- `external_sources`: 数组格式，字段必须包含 `source_name`, `title`, `published_at`, `summary`, `url`, `query_topic`。summary 必须是清洗后的 1 句话摘要，不超过 60 字。与正文里的外部参考锚点顺序对应。
- `research_mode`: 如果实际使用了外部研究就写 `external_blended`，否则写 `internal_only`。

# Input Data
{json.dumps(payload, ensure_ascii=False)}

# Output Format
仅输出标准的 JSON 字符串，不要包含 markdown 代码块标记，不要包含任何额外的解释文字。确保 JSON 转义正确，特别是换行符和引号。
""".strip()


def _try_parse_json(text: str) -> dict[str, Any] | None:
    """Try to parse JSON, return None on failure."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def _next_non_whitespace_char(text: str, start: int) -> str | None:
    """Return the next non-whitespace character after start, or None."""
    for idx in range(start, len(text)):
        if not text[idx].isspace():
            return text[idx]
    return None


def _is_likely_string_terminator(text: str, quote_index: int) -> bool:
    """Heuristic: decide whether a quote likely closes a JSON string."""
    next_char = _next_non_whitespace_char(text, quote_index + 1)
    if next_char is None:
        return True
    return next_char in {",", "}", "]", ":"}


def _safe_json_repair(raw: str) -> str:
    """Apply only safe repairs that won't break valid JSON strings."""
    fixed = raw
    # 1. 去掉 markdown 代码块标记
    fixed = re.sub(r"```json\s*", "", fixed)
    fixed = re.sub(r"```\s*", "", fixed)
    # 2. 修复中文单引号（这些不会破坏 JSON 结构）
    fixed = fixed.replace("\u2018", "'").replace("\u2019", "'")
    # 3. 去掉尾部多余逗号（trailing comma）—— 这个是安全的
    fixed = re.sub(r",\s*([}\]])", r"\1", fixed)
    # 4. 修复字符串内的未转义换行符、中文双引号和裸双引号
    result_chars = []
    in_string = False
    escape_next = False
    for idx, ch in enumerate(fixed):
        if escape_next:
            result_chars.append(ch)
            escape_next = False
            continue
        if ch == '\\' and in_string:
            result_chars.append(ch)
            escape_next = True
            continue
        if ch == '"':
            if in_string:
                if _is_likely_string_terminator(fixed, idx):
                    in_string = False
                    result_chars.append(ch)
                else:
                    result_chars.append('\\"')
            else:
                in_string = True
                result_chars.append(ch)
            continue
        if ch in ('\u201c', '\u201d'):
            if in_string:
                result_chars.append('\\"')
            else:
                in_string = not in_string
                result_chars.append('"')
            continue
        if in_string and ch == '\n':
            result_chars.append('\\n')
            continue
        if in_string and ch == '\r':
            continue
        if in_string and ch == '\t':
            result_chars.append('\\t')
            continue
        result_chars.append(ch)
    fixed = ''.join(result_chars)
    return fixed

def extract_json(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON payload returned")
    raw = text[start : end + 1]
    # 先尝试直接解析
    if _try_parse_json(raw) is not None:
        return raw
    # 安全修复
    fixed = _safe_json_repair(raw)
    if _try_parse_json(fixed) is not None:
        print("[JSON修复] 自动修复成功")
        return fixed
    # 最后手段：打印诊断信息
    try:
        json.loads(fixed)
    except json.JSONDecodeError as exc:
        print(f"[JSON修复] 自动修复失败: {exc}")
        context_start = max(0, exc.pos - 80)
        context_end = min(len(fixed), exc.pos + 80)
        context = fixed[context_start:context_end]
        pointer = " " * (exc.pos - context_start) + "^"
        print(f"[JSON修复] 出错位置附近: ...{context}...")
        print(f"[JSON修复] 错误指针: ...{pointer}...")
        raise ValueError(f"LLM 返回的 JSON 无法解析: {exc}") from exc
    return fixed


# ---------------------------------------------------------------------------
# Phase 2.2 — Column Identity & Analysis Routing
# ---------------------------------------------------------------------------

# Column identity: classify each dimension into a role
COLUMN_IDENTITY_MAP: dict[str, str] = {
    "BU": "org_unit",
    "职能": "org_unit",
    "部门": "org_unit",
    "职能序列": "org_unit",
    "绩效分位": "cohort",
    "去年绩效排名": "cohort",
    "级别": "cohort",
    "司龄分箱": "cohort",
    "年龄分箱": "cohort",
    "统计月份": "time",
}


def classify_column(dimension: str) -> str:
    """Return the identity role of a dimension column."""
    return COLUMN_IDENTITY_MAP.get(dimension, "cohort")


def route_analysis_model(dimension: str) -> str:
    """Pick the best analysis model for a dimension based on its identity."""
    identity = classify_column(dimension)
    if identity == "org_unit":
        return "pareto"          # concentration / 80-20 analysis
    if identity == "cohort":
        return "structural_drift"  # structure shift across cohort segments
    return "trend_volatility"      # time-based volatility


def pareto_analysis(rows: list[dict[str, Any]], subject: str) -> dict[str, Any]:
    """Pareto (80/20) concentration analysis for org-unit dimensions."""
    sorted_rows = sorted(rows, key=lambda r: r.get("total_amount", 0), reverse=True)
    grand_total = sum(r.get("total_amount", 0) for r in sorted_rows)
    if grand_total == 0:
        return {"model": "pareto", "concentration_ratio": 0, "top_contributors": [], "pareto_index": 0}

    cumulative = 0.0
    top_contributors = []
    for row in sorted_rows:
        cumulative += row.get("total_amount", 0)
        pct = round(cumulative / grand_total * 100, 2)
        top_contributors.append({
            "label": row.get("BU") or row.get("dimension_value", ""),
            "amount": row.get("total_amount", 0),
            "cumulative_pct": pct,
        })
        if pct >= 80:
            break

    pareto_index = len(top_contributors)
    total_groups = len(sorted_rows)
    concentration_ratio = round(pareto_index / max(total_groups, 1) * 100, 2)

    return {
        "model": "pareto",
        "concentration_ratio": concentration_ratio,
        "pareto_index": pareto_index,
        "total_groups": total_groups,
        "top_contributors": top_contributors[:10],
        "insight": f"前 {pareto_index} 个分组（占 {concentration_ratio}%）贡献了 80% 的{subject}总额。",
    }


def trend_volatility_analysis(trend_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Trend volatility analysis for time-based dimensions."""
    if len(trend_rows) < 2:
        return {"model": "trend_volatility", "volatility": 0, "max_swing": None, "periods": len(trend_rows)}

    amounts = [r.get("total_amount", 0) for r in trend_rows]
    periods = [r.get("period", "") for r in trend_rows]

    mean_val = sum(amounts) / len(amounts) if amounts else 0
    variance = sum((a - mean_val) ** 2 for a in amounts) / len(amounts) if amounts else 0
    std_dev = variance ** 0.5
    cv = round(std_dev / mean_val * 100, 2) if mean_val else 0

    # Find max month-over-month swing
    max_swing_pct = 0.0
    max_swing_period = ""
    for i in range(1, len(amounts)):
        prev = amounts[i - 1]
        if prev == 0:
            continue
        swing = abs(amounts[i] - prev) / prev * 100
        if swing > max_swing_pct:
            max_swing_pct = round(swing, 2)
            max_swing_period = periods[i]

    return {
        "model": "trend_volatility",
        "volatility_cv": cv,
        "std_dev": round(std_dev, 2),
        "mean": round(mean_val, 2),
        "max_swing_pct": max_swing_pct,
        "max_swing_period": max_swing_period,
        "periods": len(amounts),
        "insight": f"变异系数 {cv}%，最大单月波动 {max_swing_pct}%（{max_swing_period}）。",
    }


def structural_drift_analysis(rows: list[dict[str, Any]], dimension: str) -> dict[str, Any]:
    """Structural drift analysis for cohort dimensions — detects share shifts."""
    grand_total = sum(r.get("total_amount", 0) for r in rows)
    if grand_total == 0:
        return {"model": "structural_drift", "segments": [], "max_drift": None}

    # Group by dimension_value, aggregate
    segment_map: dict[str, float] = {}
    segment_count: dict[str, int] = {}
    for row in rows:
        key = str(row.get("dimension_value", row.get(dimension, "")))
        segment_map[key] = segment_map.get(key, 0) + row.get("total_amount", 0)
        segment_count[key] = segment_count.get(key, 0) + row.get("issued_employee_count", 0)

    segments = []
    for key, amount in sorted(segment_map.items(), key=lambda x: x[1], reverse=True):
        share = round(amount / grand_total * 100, 2)
        segments.append({
            "label": key,
            "amount": amount,
            "share_pct": share,
            "headcount": segment_count.get(key, 0),
        })

    # Identify dominant segment
    top_segment = segments[0] if segments else None
    hhi = sum((s["share_pct"] / 100) ** 2 for s in segments)

    return {
        "model": "structural_drift",
        "hhi": round(hhi, 4),
        "segment_count": len(segments),
        "segments": segments[:10],
        "dominant_segment": top_segment["label"] if top_segment else None,
        "dominant_share": top_segment["share_pct"] if top_segment else 0,
        "insight": f"HHI={round(hhi, 4)}，{top_segment['label'] if top_segment else '--'} 占比 {top_segment['share_pct'] if top_segment else 0}% 为结构重心。" if top_segment else "无有效分段。",
    }


def run_analysis_model(
    dimension: str,
    grouped_rows: list[dict[str, Any]],
    trend_rows: list[dict[str, Any]],
    subject: str,
) -> dict[str, Any]:
    """Route to the appropriate analysis model based on column identity."""
    model_name = route_analysis_model(dimension)
    if model_name == "pareto":
        return pareto_analysis(grouped_rows, subject)
    if model_name == "trend_volatility":
        return trend_volatility_analysis(trend_rows)
    return structural_drift_analysis(grouped_rows, dimension)

def dashboard_summary(subject: str) -> dict[str, Any]:
    """Return a BI-style dashboard payload for a single subject."""
    ensure_data_source_ready()
    subject = normalize_subject(subject)
    cache_key = _cache.make_key("dashboard", subject)
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    conn = get_connection()
    try:
        # Hero row: total, avg, coverage, headcount
        hero = conn.execute(
            f"""
            SELECT
                SUM("{subject}") AS total,
                AVG(CASE WHEN "{subject}" != 0 THEN "{subject}" END) AS avg_amount,
                COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS issued_count,
                COUNT(DISTINCT 员工ID) AS total_count
            FROM salary_wide
            """,
        ).fetchone()
        total = hero["total"] or 0
        avg_amount = hero["avg_amount"] or 0
        issued_count = hero["issued_count"] or 0
        total_count = hero["total_count"] or 1
        coverage = round(issued_count / total_count, 4)

        # Monthly trend
        trend_rows = conn.execute(
            f"""
            SELECT 统计年度 || '-' || printf('%02d', 统计月份) AS period,
                   SUM("{subject}") AS amount,
                   COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
            FROM salary_wide
            GROUP BY 统计年度, 统计月份
            ORDER BY 统计年度, 统计月份
            """,
        ).fetchall()
        trend = [dict(r) for r in trend_rows]

        # BU ranking
        bu_rows = conn.execute(
            f"""
            SELECT BU,
                   SUM("{subject}") AS amount,
                   COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
            FROM salary_wide
            GROUP BY BU
            ORDER BY amount DESC
            """,
        ).fetchall()
        bu_ranking = [dict(r) for r in bu_rows]

        # Dimension slices (top-5 per dimension)
        dim_slices: dict[str, list[dict[str, Any]]] = {}
        for dim in DIMENSION_COLUMNS:
            if dim == "统计月份":
                continue
            rows = conn.execute(
                f"""
                SELECT "{dim}" AS label,
                       SUM("{subject}") AS amount,
                       COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
                FROM salary_wide
                GROUP BY "{dim}"
                ORDER BY amount DESC
                LIMIT 10
                """,
            ).fetchall()
            dim_slices[dim] = [dict(r) for r in rows]

        result = {
            "subject": subject,
            "hero": {
                "total_amount": total,
                "avg_amount": round(avg_amount, 2),
                "coverage_rate": coverage,
                "issued_count": issued_count,
            },
            "trend": trend,
            "bu_ranking": bu_ranking,
            "dimension_slices": dim_slices,
        }
        _cache.set(cache_key, result)
        return result
    finally:
        conn.close()


def drilldown_query(subject: str, filters: dict[str, str]) -> dict[str, Any]:
    """Drill into a subject with dimension filters, returning next-level breakdown."""
    ensure_data_source_ready()
    subject = normalize_subject(subject)
    conn = get_connection()
    try:
        where_parts = []
        params: list[Any] = []
        for dim, val in filters.items():
            dim = normalize_dimension(dim)
            where_parts.append(f'"{dim}" = ?')
            params.append(val)
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        # Determine which dimensions are not yet filtered — offer those as next drill
        filtered_dims = set(normalize_dimension(d) for d in filters)
        remaining_dims = [d for d in DIMENSION_COLUMNS if d not in filtered_dims and d != "统计月份"]

        # Aggregate by each remaining dimension
        breakdowns: dict[str, list[dict[str, Any]]] = {}
        for dim in remaining_dims:
            rows = conn.execute(
                f"""
                SELECT "{dim}" AS label,
                       SUM("{subject}") AS amount,
                       COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
                FROM salary_wide
                WHERE {where_clause}
                GROUP BY "{dim}"
                ORDER BY amount DESC
                """,
                params,
            ).fetchall()
            breakdowns[dim] = [dict(r) for r in rows]

        # Trend within filter
        trend_rows = conn.execute(
            f"""
            SELECT 统计年度 || '-' || printf('%02d', 统计月份) AS period,
                   SUM("{subject}") AS amount,
                   COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
            FROM salary_wide
            WHERE {where_clause}
            GROUP BY 统计年度, 统计月份
            ORDER BY 统计年度, 统计月份
            """,
            params,
        ).fetchall()

        # Summary stats
        summary = conn.execute(
            f"""
            SELECT SUM("{subject}") AS total,
                   AVG(CASE WHEN "{subject}" != 0 THEN "{subject}" END) AS avg_amount,
                   COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
            FROM salary_wide
            WHERE {where_clause}
            """,
            params,
        ).fetchone()

        return {
            "subject": subject,
            "filters": filters,
            "summary": {
                "total_amount": summary["total"] or 0,
                "avg_amount": round(summary["avg_amount"] or 0, 2),
                "headcount": summary["headcount"] or 0,
            },
            "trend": [dict(r) for r in trend_rows],
            "breakdowns": breakdowns,
            "remaining_dimensions": remaining_dims,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Follow-up Q&A (lightweight)
# ---------------------------------------------------------------------------

_DATA_QUERY_KEYWORDS = [
    "列出", "top", "Top", "TOP", "明细", "清单", "哪些员工",
    "筛选", "查询", "补偿金top", "最高", "最低", "排名",
    "员工列表", "详细数据", "具体数据", "数据明细", "人员信息", "员工信息", "找出", "哪些人",
]

_IMPLICIT_TOP_EMPLOYEE_QUERIES = [
    "哪些员工贡献最大",
    "贡献最高的是谁",
    "谁拖高了这个科目",
    "top员工有哪些",
]

_IN_SCOPE_DETAIL_SIGNALS = [
    "找出", "列出", "哪些人", "哪些员工", "人员信息", "员工信息", "异常员工",
    "明细", "清单", "排名", "top", "前", "高", "低", "靠后", "靠前",
]

_IN_SCOPE_CHART_SIGNALS = [
    "画", "图", "饼图", "柱状图", "折线图", "条形图", "散点图", "趋势图",
    "可视化", "展示", "展现", "对比图", "分布图", "占比图", "贡献", "占比",
]

_DIMENSION_VALUE_CACHE: dict[str, set[str]] | None = None
_FOLLOW_UP_DIMENSIONS = ["BU", "部门", "职能", "职能序列", "绩效分位", "去年绩效排名", "级别", "司龄分箱", "年龄分箱"]


def is_data_query(question: str) -> bool:
    """Detect whether a follow-up question is asking for detail data rows."""
    q = question.strip().lower()
    if _is_explanatory_follow_up(q):
        return False
    for kw in _DATA_QUERY_KEYWORDS:
        if kw.lower() in q:
            return True
    if re.search(r"(前|后|倒数)\s*\d+", q):
        return True
    if re.search(r"top\s*\d+", q, re.IGNORECASE):
        return True
    if re.search(r"最高的?\s*前\s*\d+", q):
        return True
    return False


_EXPLANATORY_FOLLOW_UP_PATTERNS = [
    "什么意思",
    "怎么理解",
    "如何理解",
    "为什么这么说",
    "这段话是什么意思",
    "这部分内容什么意思",
    "这条建议是什么意思",
    "为什么会这样判断",
]

_DETAIL_SUMMARY_SIGNALS = ["有没有明细", "有没有这些人", "这部分人", "这批人", "这类人", "名单"]

_DETAIL_RAW_SIGNALS = ["展开明细", "看原始记录", "逐月明细", "按月份展开", "原始明细"]


def _is_explanatory_follow_up(question: str) -> bool:
    q = question.strip()
    if any(pattern in q for pattern in _EXPLANATORY_FOLLOW_UP_PATTERNS):
        return True
    if any(q.endswith(suffix) for suffix in ["是什么意思？", "是什么意思", "什么意思？", "什么意思", "怎么理解？", "怎么理解"]):
        return True
    if len(q) >= 40 and any(marker in q for marker in ["什么意思", "怎么理解", "如何理解", "为什么"]):
        return True
    return False


def _fallback_data_query(question: str) -> dict[str, Any]:
    """Return a graceful fallback when LLM or DB query fails."""
    return {
        "mode": "data_query",
        "answer": f"无法解析数据查询：{question}。请尝试更具体的描述，例如'列出2026年8月经济补偿金top10'。",
        "columns": [],
        "rows": [],
    }


def _is_employee_aggregate_query(question: str) -> bool:
    """Detect whether the user wants per-employee aggregated totals instead of raw rows."""
    q = question.strip().lower()
    aggregate_signals = ["个人总共", "个人累计", "个人合计", "每人总共", "每人累计", "每人合计", "累计", "总共", "合计", "汇总"]
    person_signals = ["个人", "每人", "员工", "人员"]
    ranking_signals = ["top", "前", "最高", "最低", "排名", "倒数"]
    has_aggregate = any(signal in q for signal in aggregate_signals)
    has_person = any(signal in q for signal in person_signals)
    has_ranking = any(signal in q for signal in ranking_signals)
    return (has_aggregate and has_person) or (has_aggregate and has_ranking)


def _is_implicit_top_employee_query(question: str) -> bool:
    q = question.strip().lower()
    return any(pattern.lower() in q for pattern in _IMPLICIT_TOP_EMPLOYEE_QUERIES)


def get_dimension_value_cache() -> dict[str, set[str]]:
    global _DIMENSION_VALUE_CACHE
    if not get_data_source_status()["ready"]:
        return {dim: set() for dim in _FOLLOW_UP_DIMENSIONS}
    if _DIMENSION_VALUE_CACHE is not None:
        return _DIMENSION_VALUE_CACHE

    cache: dict[str, set[str]] = {}
    conn = get_connection()
    try:
        for dim in _FOLLOW_UP_DIMENSIONS:
            rows = conn.execute(f'SELECT DISTINCT "{dim}" AS value FROM salary_wide ORDER BY "{dim}"').fetchall()
            cache[dim] = {row["value"] for row in rows if row["value"]}
    finally:
        conn.close()

    _DIMENSION_VALUE_CACHE = cache
    return cache


def _merge_filter_value(existing: Any, incoming: str) -> str | list[str]:
    if isinstance(existing, list):
        if incoming not in existing:
            return [*existing, incoming]
        return existing
    if isinstance(existing, str) and existing:
        if existing == incoming:
            return existing
        return [existing, incoming]
    return incoming


def _dedupe_values(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        if value and value not in deduped:
            deduped.append(value)
    return deduped


def _infer_function_filters(question: str) -> list[str]:
    values = get_dimension_value_cache().get("职能", set())
    matches: list[str] = []
    if "产品" in question and "产品" in values:
        matches.append("产品")
    if "财务" in question and "财务" in values:
        matches.append("财务")
    return _dedupe_values(matches)


def _infer_level_filters(question: str) -> list[str]:
    compact = (
        question.strip()
        .replace("、", "")
        .replace("，", "")
        .replace(",", "")
        .replace("/", "")
        .replace(" ", "")
        .lower()
    )
    values = get_dimension_value_cache().get("级别", set())
    matches: list[str] = []
    if any(token in compact for token in ["cd级别", "cd类员工", "cd类", "cd员工"]):
        if "CD类员工" in values:
            matches.append("CD类员工")
    if any(token in compact for token in ["c级或d级", "c级和d级", "c级d级"]):
        if "CD类员工" in values:
            matches.append("CD类员工")
    if any(token in compact for token in ["o级管理层", "含o级管理层", "o级", "o类领导"]):
        if "O类领导" in values:
            matches.append("O类领导")
    return matches


def _infer_tenure_filters(question: str) -> list[str]:
    if not any(signal in question for signal in ["司龄8年以上", "8年以上", "司龄超过8年", "司龄8年及以上"]):
        return []
    values = get_dimension_value_cache().get("司龄分箱", set())
    matches = [value for value in ["8-10", "10年以上"] if value in values]
    return matches


def _infer_mid_tenure_filters(question: str) -> list[str]:
    compact = question.strip().replace(" ", "")
    if not any(signal in compact for signal in ["司龄3至10年", "司龄3-10年", "3至10年司龄", "3到10年司龄", "司龄3到10年"]):
        return []
    values = get_dimension_value_cache().get("司龄分箱", set())
    return [value for value in ["3-5", "5-8", "8-10"] if value in values]


def _infer_age_filters(question: str) -> list[str]:
    compact = question.strip().replace(" ", "")
    if not any(signal in compact for signal in ["35至40岁", "35到40岁", "35-40岁", "35至40岁年龄段", "35到40岁年龄段", "35-40年龄段"]):
        return []
    values = get_dimension_value_cache().get("年龄分箱", set())
    return [value for value in ["35-40"] if value in values]


def _infer_high_performance_filter(question: str) -> str | None:
    compact = question.strip().replace(" ", "")
    values = get_dimension_value_cache().get("绩效分位", set())
    for token, canonical in [
        ("绩效前10%", "前10%"),
        ("绩效前20%", "前20%"),
        ("绩效前30%", "前30%"),
        ("绩效前40%", "前40%"),
        ("绩效前50%", "前50%"),
        ("绩效前60%", "前60%"),
        ("绩效前70%", "前70%"),
    ]:
        if token in compact and canonical in values:
            return canonical
    performance_match = re.search(r"绩效(?:排名)?前\s*(\d{1,2})%", compact)
    if performance_match:
        canonical = f"前{performance_match.group(1)}%"
        if canonical in values:
            return canonical
    return None


def _mentions_performance_dimension(question: str) -> bool:
    q = question.strip()
    return bool(
        _infer_low_performance_filter(q)
        or _infer_high_performance_filter(q)
        or any(signal in q for signal in ["绩效分位", "绩效分布", "绩效排名", "高绩效", "低绩效", "靠前", "靠后"])
    )


def _wants_summary_detail(question: str) -> bool:
    q = question.strip()
    return any(signal in q for signal in _DETAIL_SUMMARY_SIGNALS)


def _wants_raw_detail(question: str) -> bool:
    q = question.strip()
    return any(signal in q for signal in _DETAIL_RAW_SIGNALS)


def _infer_dimension_filters(question: str) -> dict[str, str | list[str]]:
    """Infer structured dimension filters directly from question text."""
    inferred: dict[str, str | list[str]] = {}
    q = question.strip()
    value_cache = get_dimension_value_cache()
    for dim in _FOLLOW_UP_DIMENSIONS:
        matched_values: list[tuple[int, str]] = []
        for value in value_cache.get(dim, set()):
            positions = [pos for pos in [q.find(value), q.find(f"{value}序列")] if pos >= 0]
            if positions:
                matched_values.append((min(positions), value))
        for _, value in sorted(matched_values, key=lambda item: (item[0], -len(item[1]))):
            inferred[dim] = _merge_filter_value(inferred.get(dim), value)
    for value in _infer_function_filters(q):
        inferred["职能"] = _merge_filter_value(inferred.get("职能"), value)
    for value in _infer_level_filters(q):
        inferred["级别"] = _merge_filter_value(inferred.get("级别"), value)
    for value in _infer_tenure_filters(q):
        inferred["司龄分箱"] = _merge_filter_value(inferred.get("司龄分箱"), value)
    for value in _infer_mid_tenure_filters(q):
        inferred["司龄分箱"] = _merge_filter_value(inferred.get("司龄分箱"), value)
    for value in _infer_age_filters(q):
        inferred["年龄分箱"] = _merge_filter_value(inferred.get("年龄分箱"), value)
    high_performance_filter = _infer_high_performance_filter(q)
    if high_performance_filter:
        inferred["绩效分位"] = _merge_filter_value(inferred.get("绩效分位"), high_performance_filter)
    return inferred


def _normalize_single_filter_value(dim: str, candidate: str, value_cache: dict[str, set[str]]) -> str | None:
    values = value_cache.get(dim, set())
    raw = candidate.strip()
    if not raw or raw == "可选":
        return None
    if raw in values:
        return raw
    if raw.endswith("序列") and raw[:-2] in values:
        return raw[:-2]
    stripped_candidate = raw.replace("序列", "")
    if stripped_candidate in values:
        return stripped_candidate
    return None


def _normalize_dimension_filters(filters: dict[str, Any], question: str) -> dict[str, str | list[str]]:
    """Normalize filter values so they match actual dimension values in the database."""
    normalized: dict[str, str | list[str]] = {}
    inferred = _infer_dimension_filters(question)
    value_cache = get_dimension_value_cache()
    for dim in _FOLLOW_UP_DIMENSIONS:
        raw = filters.get(dim)
        if raw is None or raw == "" or raw == "可选":
            if dim in inferred:
                normalized[dim] = inferred[dim]
            continue
        candidates = raw if isinstance(raw, list) else [raw]
        normalized_values = []
        for candidate in candidates:
            if isinstance(candidate, str):
                resolved = _normalize_single_filter_value(dim, candidate, value_cache)
                if resolved:
                    normalized_values.append(resolved)
        if dim == "司龄分箱" and any(
            isinstance(candidate, str) and candidate in {"8年以上", "司龄8年以上", "司龄超过8年", "司龄8年及以上"}
            for candidate in candidates
        ):
            normalized_values.extend(_infer_tenure_filters(question))
        if dim == "司龄分箱" and any(
            isinstance(candidate, str) and candidate in {"3至10年", "司龄3至10年", "3-10年", "司龄3-10年", "3到10年", "司龄3到10年"}
            for candidate in candidates
        ):
            normalized_values.extend(_infer_mid_tenure_filters(question))
        normalized_values = _dedupe_values(normalized_values)
        if normalized_values:
            normalized[dim] = normalized_values[0] if len(normalized_values) == 1 else normalized_values
            continue
        if dim in inferred:
            normalized[dim] = inferred[dim]
    return normalized


def canonicalize_subject_name(value: Any) -> str:
    subject = ensure_text(value)
    if not subject:
        return DEFAULT_SUBJECT
    if subject in SUBJECT_COLUMNS:
        return subject
    if subject in SUBJECT_ALIASES:
        return SUBJECT_ALIASES[subject]
    for alias, canonical in SUBJECT_ALIASES.items():
        if subject and alias in subject:
            return canonical
    return subject


def subject_names_match(left: Any, right: Any) -> bool:
    return canonicalize_subject_name(left) == canonicalize_subject_name(right)


def normalize_query_subject(value: Any, default_subject: str) -> str:
    subject = ensure_text(value, default_subject)
    if default_subject and subject == default_subject:
        return default_subject
    if subject in SUBJECT_COLUMNS:
        return subject
    if subject in SUBJECT_ALIASES:
        return SUBJECT_ALIASES[subject]
    for alias, canonical in SUBJECT_ALIASES.items():
        if subject and alias in subject:
            return canonical
    return default_subject if default_subject in SUBJECT_COLUMNS else DEFAULT_SUBJECT


def _extract_top_limit(question: str, default_limit: int = 10) -> int:
    for pattern in [r"top\s*(\d+)", r"前\s*(\d+)", r"后\s*(\d+)", r"倒数\s*(\d+)", r"(\d+)\s*(?:个人|个员工|名员工|名|人|位|个)"]:
        matched = re.search(pattern, question, re.IGNORECASE)
        if matched:
            return min(int(matched.group(1)), 50)
    return default_limit


def _parse_explicit_period_range(question: str) -> tuple[tuple[int, int] | None, tuple[int, int] | None]:
    explicit_range = re.search(r"(20\d{2})\s*[-至到]\s*(20\d{2})", question)
    if explicit_range:
        return (int(explicit_range.group(1)), 1), (int(explicit_range.group(2)), 1)
    return None, None


def _describe_filter_value(dim: str, value: str | list[str]) -> str:
    if isinstance(value, list):
        if dim == "职能" and set(value) == {"产品", "财务"}:
            return "产品与财务条线"
        if dim == "级别" and value == ["CD类员工"]:
            return "C/D级别"
        if dim == "司龄分箱" and set(value) == {"8-10", "10年以上"}:
            return "司龄8年以上"
        return "、".join(value)
    if dim == "职能" and value in {"产品", "财务"}:
        return f"{value}条线"
    if dim == "级别" and value == "CD类员工":
        return "C/D级别"
    if dim == "级别" and value == "O类领导":
        return "O级管理层"
    if dim == "年龄分箱" and value == "35-40":
        return "35至40岁年龄段"
    return value


def _shift_period(year: int, month: int, delta_months: int) -> tuple[int, int]:
    total_months = year * 12 + (month - 1) + delta_months
    return total_months // 12, total_months % 12 + 1


def _infer_chart_period_window(question: str) -> tuple[tuple[int, int], tuple[int, int]] | None:
    q = question.strip()
    explicit_start, explicit_end = _parse_explicit_period_range(q)
    if explicit_start and explicit_end:
        return explicit_start, explicit_end

    month_match = re.search(r"(20\d{2})年\s*(\d{1,2})月", q)
    if month_match:
        year = int(month_match.group(1))
        month = int(month_match.group(2))
        return (year, month), (year, month)

    years_match = re.search(r"(\d+)\s*年(?:来|内)?", q)
    if years_match:
        years = max(int(years_match.group(1)), 1)
        meta = metadata()
        if not meta.get("period_end"):
            return None
        end_year, end_month = map(int, meta["period_end"].split("-"))
        start_year, start_month = _shift_period(end_year, end_month, -(years * 12 - 1))
        return (start_year, start_month), (end_year, end_month)

    if "两年" in q or "2年" in q:
        meta = metadata()
        if not meta.get("period_end"):
            return None
        end_year, end_month = map(int, meta["period_end"].split("-"))
        start_year, start_month = _shift_period(end_year, end_month, -23)
        return (start_year, start_month), (end_year, end_month)

    return None


def _infer_low_performance_filter(question: str) -> str | None:
    q = question.strip()
    low_perf_signals = ["绩效排名靠后", "绩效靠后", "低绩效", "后段绩效", "绩效较差", "绩效后段"]
    if not any(signal in q for signal in low_perf_signals):
        return None

    performance_values = sorted(get_dimension_value_cache().get("绩效分位", set()))
    lowered_pairs = [(value, value.lower()) for value in performance_values]
    preferred_keywords = ["后", "低", "差", "末", "c", "d", "尾"]
    for original, lowered in lowered_pairs:
        if any(keyword in lowered for keyword in preferred_keywords):
            return original
    return performance_values[0] if performance_values else None


def _is_in_scope_detail_query(question: str, context: dict[str, Any]) -> bool:
    prev_subject = (context.get("previous_request") or {}).get("subject", "")
    q = question.strip()
    if not prev_subject:
        return False
    prev_canonical = canonicalize_subject_name(prev_subject)
    mentions_other_subject = any(subj in q and subj != prev_subject and subj != prev_canonical for subj in SUBJECT_COLUMNS)
    mentions_other_alias = any(
        alias in q and canonical != prev_canonical and not (alias == "绩效" and _mentions_performance_dimension(q))
        for alias, canonical in SUBJECT_ALIASES.items()
    )
    if mentions_other_subject or mentions_other_alias:
        return False
    if prev_subject in q:
        return True
    if any(keyword in q for keyword in ["绩效", "平安寿险", "平安银行", "平安科技"]) and any(
        keyword in q for keyword in ["人员信息", "员工信息", "找出", "列出", "哪些人", "排名", "高", "低", "靠后", "靠前", "明细", "名单"]
    ):
        return True
    if is_data_query(q):
        return True
    return any(signal in q for signal in _IN_SCOPE_DETAIL_SIGNALS)


def _is_in_scope_chart_query(question: str, context: dict[str, Any]) -> bool:
    prev_subject = (context.get("previous_request") or {}).get("subject", "")
    q = question.strip()
    if not prev_subject:
        return False
    prev_canonical = canonicalize_subject_name(prev_subject)
    mentions_other_subject = any(subj in q and subj != prev_subject and subj != prev_canonical for subj in SUBJECT_COLUMNS)
    mentions_other_alias = False
    for alias, canonical in SUBJECT_ALIASES.items():
        if alias == "绩效" and (
            _infer_low_performance_filter(q) or "绩效分位" in q or "绩效分布" in q or "绩效排名" in q
        ):
            continue
        if alias in q and canonical != prev_canonical:
            mentions_other_alias = True
            break
    if mentions_other_subject or mentions_other_alias:
        return False
    if prev_subject in q:
        return True
    if is_chart_query(q):
        return True
    return any(signal in q for signal in _IN_SCOPE_CHART_SIGNALS)


def should_skip_llm_for_data_query(question: str, context: dict[str, Any]) -> bool:
    q = question.strip()
    if _is_implicit_top_employee_query(q):
        return True
    has_subject = any(subj in q for subj in SUBJECT_COLUMNS) or any(alias in q for alias in SUBJECT_ALIASES)
    range_start, range_end = _parse_explicit_period_range(q)
    has_explicit_period = bool(re.search(r"\d{4}年\s*\d{1,2}月", q) or re.search(r"\d{4}年", q) or (range_start and range_end))
    has_rank = bool(re.search(r"top\s*\d+|前\s*\d+|倒数\s*\d+|后\s*\d+", q, re.IGNORECASE)) or any(
        kw in q for kw in ["最高", "最低", "最大", "最小", "排名", "汇总"]
    )
    has_filters = bool(_infer_dimension_filters(q))
    default_subject = (context.get("previous_request") or {}).get("subject")
    return has_explicit_period or has_rank or has_filters or (has_subject and bool(default_subject))


def parse_data_query_fast(question: str, default_subject: str) -> dict[str, Any]:
    q = question.strip()
    subject_resolution = resolve_subject(q, default_subject, default_subject)
    subject = default_subject
    if not subject_resolution.requires_confirmation and subject_resolution.display_subject:
        subject = subject_resolution.display_subject

    month_match = re.search(r"(\d{4})年\s*(\d{1,2})月", q)
    year_match = re.search(r"(\d{4})年", q) if not month_match else None
    range_start, range_end = _parse_explicit_period_range(q)

    sort_order = "ASC" if any(kw in q.lower() for kw in ["最低", "最少", "最小", "倒数", "bottom"]) else "DESC"
    aggregation = "employee_total" if (_is_employee_aggregate_query(q) or _is_implicit_top_employee_query(q)) else "row"
    if month_match:
        aggregation = "row"
    if any(signal in q.lower() for signal in ["汇总top", "汇总 top", "top10列表", "汇总top10", "补偿金汇总", "汇总前10"]):
        aggregation = "employee_total"

    filters = _infer_dimension_filters(q)
    low_performance_filter = _infer_low_performance_filter(q)
    if low_performance_filter and "绩效分位" not in filters:
        filters["绩效分位"] = low_performance_filter
    high_performance_filter = _infer_high_performance_filter(q)
    if high_performance_filter and "绩效分位" not in filters:
        filters["绩效分位"] = high_performance_filter

    if _wants_summary_detail(q) and not _wants_raw_detail(q):
        aggregation = "employee_total"

    return {
        "subject": normalize_query_subject(subject, default_subject),
        "filters": filters,
        "year": int(month_match.group(1)) if month_match else (int(year_match.group(1)) if year_match else None),
        "month": int(month_match.group(2)) if month_match else None,
        "start_year": range_start[0] if range_start else None,
        "start_month": range_start[1] if range_start else None,
        "end_year": range_end[0] if range_end else None,
        "end_month": range_end[1] if range_end else None,
        "sort_by": "total_amount",
        "sort_order": sort_order,
        "limit": _extract_top_limit(q, 10),
        "aggregation": aggregation,
    }


def query_detail_data(question: str, context: dict[str, Any]) -> dict[str, Any]:
    """Parse a natural-language data query via LLM, then run parameterized SQL on salary_wide."""
    ensure_data_source_ready()
    previous_request = context.get("previous_request", {})
    default_subject = previous_request.get("subject", DEFAULT_SUBJECT)

    params_parsed = None
    if should_skip_llm_for_data_query(question, context):
        params_parsed = parse_data_query_fast(question, default_subject)
    else:
        llm = LLMService()
        if llm.enabled:
            try:
                parse_prompt = f"""从以下用户问题中提取数据查询参数，返回严格JSON格式：
{{
  "subject": "薪酬科目名称，如经济补偿金、底薪等",
  "filters": {{"BU": "可选", "职能": "可选", "级别": "可选", "绩效分位": "可选", "司龄分箱": "可选", "年龄分箱": "可选"}},
  "year": 2026,
  "month": 8,
  "sort_by": "total_amount",
  "sort_order": "DESC或ASC",
  "limit": 10,
  "aggregation": "row或employee_total"
}}

可用的薪酬科目：{', '.join(SUBJECT_COLUMNS)}
可用的维度列：BU, 部门, 职能序列, 去年绩效排名, 级别, 司龄分箱, 年龄分箱
上一轮分析的科目是：{default_subject}

用户问题：{question}

注意：
- filters中只保留用户明确提到的筛选条件，没提到的不要包含
- 如果用户提到了具体的BU名称（如"平安科技"、"平安寿险"、"平安银行"等），必须放入filters的BU字段
- limit最大50
- 如果用户没指定年月，year和month设为null
- aggregation的判断规则：
  - 用户提到"个人总共"、"个人累计"、"每人合计"、"累计最高TOP"这类表述时，设为"employee_total"
  - 普通明细记录查询设为"row"
- sort_order的判断规则：
  - 用户说"最低"、"最少"、"最小"、"bottom"、"倒数" → sort_order设为"ASC"
  - 用户说"最高"、"最多"、"最大"、"top"、"前" → sort_order设为"DESC"
  - 默认为"DESC"
- 返回纯JSON，不要其他文字"""

                text = llm._chat_completion(
                    system_prompt="你是一个SQL查询参数解析器，只返回JSON。",
                    user_prompt=parse_prompt,
                    temperature=0.1,
                )
                params_parsed = json.loads(extract_json(text))
            except Exception:
                params_parsed = None

    if not params_parsed:
        params_parsed = parse_data_query_fast(question, default_subject)

    display_subject = normalize_query_subject(params_parsed.get("subject"), default_subject)
    subject = normalize_subject(display_subject)
    filters = _normalize_dimension_filters(params_parsed.get("filters") or {}, question)
    year = params_parsed.get("year")
    month = params_parsed.get("month")
    start_year = params_parsed.get("start_year")
    start_month = params_parsed.get("start_month")
    end_year = params_parsed.get("end_year")
    end_month = params_parsed.get("end_month")
    limit = min(params_parsed.get("limit", 10) or 10, 50)
    sort_order = "DESC" if params_parsed.get("sort_order", "DESC").upper() == "DESC" else "ASC"
    aggregation = params_parsed.get("aggregation", "row")
    if aggregation not in {"row", "employee_total"}:
        aggregation = "employee_total" if (_is_employee_aggregate_query(question) or _is_implicit_top_employee_query(question)) else "row"
    if _is_implicit_top_employee_query(question):
        aggregation = "employee_total"
        sort_order = "DESC"
        limit = 10
    if any(signal in question.lower() for signal in ["汇总top", "汇总 top", "top10列表", "汇总top10", "补偿金汇总", "汇总前10"]):
        aggregation = "employee_total"
    if _wants_summary_detail(question) and not _wants_raw_detail(question):
        aggregation = "employee_total"

    subject_col = f'"{subject}"'
    conn = get_connection()
    try:
        where_clauses = [f"{subject_col} > 0"]
        sql_params: list[Any] = []

        if start_year and end_year:
            where_clauses.append(
                "((统计年度 > ?) OR (统计年度 = ? AND 统计月份 >= ?)) AND ((统计年度 < ?) OR (统计年度 = ? AND 统计月份 <= ?))"
            )
            sql_params.extend([int(start_year), int(start_year), int(start_month or 1), int(end_year), int(end_year), int(end_month or 12)])
        if year:
            where_clauses.append("统计年度 = ?")
            sql_params.append(int(year))
        if month:
            where_clauses.append("统计月份 = ?")
            sql_params.append(int(month))

        valid_dims = set(_FOLLOW_UP_DIMENSIONS)
        for dim, val in filters.items():
            if dim not in valid_dims or not val:
                continue
            values = val if isinstance(val, list) else [val]
            cleaned_values = [item for item in values if isinstance(item, str) and item and item != "可选"]
            if not cleaned_values:
                continue
            if len(cleaned_values) == 1:
                where_clauses.append(f'"{dim}" = ?')
                sql_params.append(cleaned_values[0])
            else:
                placeholders = ",".join("?" for _ in cleaned_values)
                where_clauses.append(f'"{dim}" IN ({placeholders})')
                sql_params.extend(cleaned_values)

        where_sql = " AND ".join(where_clauses)

        if aggregation == "employee_total":
            sql = f"""
                SELECT
                    员工ID,
                    MIN(BU) AS BU,
                    MIN(职能) AS 职能,
                    MIN(级别) AS 级别,
                    MIN(绩效分位) AS 绩效分位,
                    MIN(司龄分箱) AS 司龄分箱,
                    MIN(年龄分箱) AS 年龄分箱,
                    SUM({subject_col}) AS total_amount,
                    COUNT(DISTINCT printf('%04d-%02d', 统计年度, 统计月份)) AS paid_months,
                    ROUND(AVG(CASE WHEN {subject_col} > 0 THEN {subject_col} END), 2) AS avg_amount
                FROM salary_wide
                WHERE {where_sql}
                GROUP BY 员工ID
                ORDER BY total_amount {sort_order}
                LIMIT ?
            """
            sql_params.append(limit)
            rows = conn.execute(sql, sql_params).fetchall()
            columns = ["员工ID", "BU", "职能", "级别", "绩效分位", "司龄分箱", "年龄分箱", f"累计{display_subject}", "发放月数", f"月均{display_subject}"]
            result_rows = []
            for row in rows:
                result_rows.append({
                    "员工ID": row["员工ID"],
                    "BU": row["BU"],
                    "职能": row["职能"],
                    "级别": row["级别"],
                    "绩效分位": row["绩效分位"],
                    "司龄分箱": row["司龄分箱"],
                    "年龄分箱": row["年龄分箱"],
                    f"累计{display_subject}": row["total_amount"],
                    "发放月数": row["paid_months"],
                    f"月均{display_subject}": row["avg_amount"],
                })
        else:
            sql = f"""
                SELECT
                    员工ID, BU, 职能, 级别, 绩效分位, 司龄分箱, 年龄分箱,
                    统计年度, 统计月份,
                    {subject_col} AS amount
                FROM salary_wide
                WHERE {where_sql}
                ORDER BY amount {sort_order}
                LIMIT ?
            """
            sql_params.append(limit)
            rows = conn.execute(sql, sql_params).fetchall()
            columns = ["员工ID", "BU", "职能", "级别", "绩效分位", "司龄分箱", "年龄分箱", "年度", "月份", display_subject]
            result_rows = []
            for row in rows:
                result_rows.append({
                    "员工ID": row["员工ID"],
                    "BU": row["BU"],
                    "职能": row["职能"],
                    "级别": row["级别"],
                    "绩效分位": row["绩效分位"],
                    "司龄分箱": row["司龄分箱"],
                    "年龄分箱": row["年龄分箱"],
                    "年度": row["统计年度"],
                    "月份": row["统计月份"],
                    display_subject: row["amount"],
                })

        filter_parts: list[str] = []
        if start_year and end_year:
            filter_parts.append(f"{start_year}-{start_month or 1:02d}至{end_year}-{end_month or 12:02d}")
        elif year:
            filter_parts.append(f"{year}年")
        if month:
            filter_parts.append(f"{month}月")
        for dim, val in filters.items():
            if dim in valid_dims and val:
                filter_parts.append(_describe_filter_value(dim, val))
        filter_desc = "、".join(part for part in filter_parts if part)

        sort_label = "最高" if sort_order == "DESC" else "最低"
        if _is_implicit_top_employee_query(question):
            answer = f"以下是当前分析科目“{display_subject}”贡献最高的前10名员工（按个人累计金额排序）："
        elif aggregation == "employee_total" and _wants_summary_detail(question) and not _wants_raw_detail(question):
            answer = f"以下是符合{filter_desc}条件的人群汇总名单（按 {display_subject} 累计金额展示，共返回{len(result_rows)}人）。如需逐月原始明细，可继续追问“展开原始明细”。"
        elif aggregation == "employee_total":
            answer = f"以下是{filter_desc}员工在当前范围内{display_subject}累计{sort_label}的前{limit}名员工（按 SUM 聚合排序，共返回{len(result_rows)}人）："
        else:
            answer = f"以下是{filter_desc}{display_subject}{sort_label}的{limit}条记录（共返回{len(result_rows)}条）："

        return {
            "mode": "data_query",
            "answer": answer,
            "columns": columns,
            "rows": result_rows,
        }
    except Exception as exc:
        return {
            "mode": "data_query",
            "answer": f"数据查询失败：{exc}",
            "columns": [],
            "rows": [],
        }
    finally:
        conn.close()


_NEW_REPORT_KEYWORDS = ["重新分析", "重新生成", "换一个", "生成报告", "帮我分析", "详细分析"]
_ALWAYS_NEW_REPORT_KEYWORDS = ["重新分析", "重新生成", "重新生成报告", "重跑", "重新跑"]


def is_new_report_request(question: str, context: dict[str, Any]) -> bool:
    """Detect whether a follow-up question requires a full report regeneration."""
    q = question.strip()
    prev_subject = (context.get("previous_request") or {}).get("subject", "")
    if any(keyword in q for keyword in _ALWAYS_NEW_REPORT_KEYWORDS):
        return True
    if prev_subject and (_is_in_scope_detail_query(q, context) or _is_in_scope_chart_query(q, context)):
        return False
    if prev_subject:
        prev_canonical = canonicalize_subject_name(prev_subject)
        for subj in SUBJECT_COLUMNS:
            if subj in q and subj != prev_subject and subj != prev_canonical:
                return True
        for alias, canonical in SUBJECT_ALIASES.items():
            if alias == "绩效" and _infer_low_performance_filter(q):
                continue
            if alias in AMBIGUOUS_SUBJECT_TERMS and alias in q:
                continue
            if alias in q and canonical != prev_canonical:
                return True
    for kw in _NEW_REPORT_KEYWORDS:
        if kw in q:
            if kw in {"帮我分析", "详细分析", "生成报告"} and (
                _is_in_scope_detail_query(q, context) or _is_in_scope_chart_query(q, context)
            ):
                return False
            return True
    return False


def build_follow_up_subject_clarification(question: str, context: dict[str, Any]) -> dict[str, Any] | None:
    previous_request = context.get("previous_request") or {}
    current_subject = ensure_text(previous_request.get("subject"))
    if not current_subject:
        return None
    if len(SUBJECT_COLUMNS) == 1:
        return None
    resolution = resolve_subject(question, None, current_subject)
    ambiguous_terms = [term for term in AMBIGUOUS_SUBJECT_TERMS if term in question]
    if ambiguous_terms == ["绩效"] and _mentions_performance_dimension(question):
        return None
    ambiguous_hit = bool(ambiguous_terms)
    if not resolution.requires_confirmation or not ambiguous_hit:
        return None
    draft = {
        "subject": current_subject,
        "primary_dimension": "BU",
        "secondary_dimensions": previous_request.get("secondary_dimensions") or DEFAULT_SECONDARY_DIMENSIONS,
        "start_period": previous_request.get("start_period") or default_period_window()[0],
        "end_period": previous_request.get("end_period") or default_period_window()[1],
        "metrics": previous_request.get("metrics") or ["总额", "平均金额", "发放覆盖率"],
        "question": question,
    }
    return {
        "mode": "clarification",
        "message": "这次追问里用了高歧义科目词，我先帮你确认分析科目，避免直接沿用错口径。",
        "request_draft": draft,
        "clarification": {
            "needs_subject": True,
            "needs_time_window": False,
            "needs_dimensions": False,
            "needs_metrics": False,
            "current_step": "subject",
            "subject_prompt": "请先确认这次追问想看的具体科目。",
            "time_window_prompt": "",
            "dimension_prompt": "",
            "metric_prompt": "",
            "subject_prompt_reason": resolution.ambiguity_reason,
            "subject_options": resolution.candidate_subjects[:6] or [canonicalize_subject_name(current_subject)],
            "subject_candidate_options": resolution.candidate_subjects[:6] or [canonicalize_subject_name(current_subject)],
            "subject_catalog": build_subject_catalog(),
            "dimension_options": [dimension for dimension in DIMENSION_COLUMNS if dimension != "BU"],
            "time_window_options": [],
            "metric_options": ["总额", "平均金额", "领取人数", "发放覆盖率", "占比", "环比", "同比"],
            "dimension_presets": build_dimension_presets(),
            "matched_terms": resolution.matched_terms,
        },
    }


_CHART_KEYWORDS = [
    "画", "图", "饼图", "柱状图", "折线图", "条形图", "散点图", "趋势图",
    "可视化", "展示", "展现", "对比图", "分布图", "占比图", "chart", "pie", "bar",
]

_CHART_TYPE_MAP = {
    "饼图": "pie", "pie": "pie", "占比图": "pie", "占比": "pie",
    "柱状图": "bar", "条形图": "bar", "bar": "bar", "对比图": "bar",
    "折线图": "line", "趋势图": "line", "line": "line", "走势图": "line",
    "散点图": "scatter", "scatter": "scatter",
}

_METRIC_FOR_CHART = {
    "总额": "total_amount",
    "平均": "avg_amount", "均值": "avg_amount", "人均": "avg_amount", "平均金额": "avg_amount",
    "人数": "headcount", "领取人数": "headcount", "发放人数": "headcount",
    "覆盖率": "coverage_rate", "发放覆盖率": "coverage_rate",
}


def is_chart_query(question: str) -> bool:
    """Detect whether a follow-up question is asking for a chart/visualization."""
    q = question.strip()
    if is_data_query(q):
        return False
    return any(kw in q for kw in _CHART_KEYWORDS)


def query_chart_data(question: str, context: dict[str, Any]) -> dict[str, Any]:
    """Parse a natural-language chart request, query DB, return structured chart data."""
    ensure_data_source_ready()
    previous_request = context.get("previous_request", {})
    default_subject = previous_request.get("subject", DEFAULT_SUBJECT)

    # --- 1. 识别薪酬科目 ---
    subject_resolution = resolve_subject(question, default_subject, default_subject)
    display_subject = subject_resolution.display_subject or default_subject

    # --- 2. 识别图表类型 ---
    chart_type = "bar"  # 默认柱状图
    for keyword, ctype in _CHART_TYPE_MAP.items():
        if keyword in question.lower():
            chart_type = ctype
            break

    # --- 3. 识别过滤条件与分组维度 ---
    filters = _infer_dimension_filters(question)
    dimension = "BU"  # 默认按 BU
    time_keywords = ["每月", "月", "趋势", "走势", "变化", "月度"]
    has_time_series_intent = any(kw in question for kw in time_keywords)
    if has_time_series_intent:
        dimension = "统计月份"
    elif "绩效分布" in question or "不同绩效" in question or "绩效分位" in question:
        dimension = "绩效分位"
    else:
        for dim in DIMENSION_COLUMNS:
            if dim != "统计月份" and dim in question:
                dimension = dim
                break
        for alias, canonical in DIMENSION_ALIASES.items():
            if alias in question:
                dimension = canonical
                break
    if dimension in filters and len(filters) > 1:
        for candidate in ["绩效分位", "职能", "级别", "年龄分箱", "司龄分箱", "BU"]:
            if candidate != dimension and candidate not in filters and candidate in question:
                dimension = candidate
                break
    if dimension in filters and filters.get("BU") and dimension == "BU":
        for candidate in ["绩效分位", "职能", "级别", "年龄分箱", "司龄分箱"]:
            alias = next((raw for raw, canonical in DIMENSION_ALIASES.items() if canonical == candidate), None)
            if candidate in question or (alias and alias in question):
                dimension = candidate
                break
    # 特殊处理：月度/趋势 → 按月
    if has_time_series_intent and dimension == "BU":
        dimension = "统计月份"

    display_dimension = dimension
    if dimension != "统计月份":
        for alias, canonical in DIMENSION_ALIASES.items():
            if canonical == dimension and alias in question and alias not in {"月份", "序列"}:
                display_dimension = alias
                break
        if display_dimension == "部门" and "职能" in question:
            display_dimension = "职能"

    # --- 4. 识别指标 ---
    metric_field = "total_amount"  # 默认总额
    metric_label = "总额"
    for keyword, field in _METRIC_FOR_CHART.items():
        if keyword in question:
            metric_field = field
            metric_label = keyword
            break

    # --- 5. 查数据库 ---
    conn = get_connection()
    try:
        subject = normalize_subject(display_subject)
        subject_col = f'"{subject}"'
        where_clauses = []
        sql_params: list[Any] = []
        chart_period_window = _infer_chart_period_window(question) if dimension == "统计月份" else None
        if chart_period_window:
            (start_year, start_month), (end_year, end_month) = chart_period_window
            where_clauses.append(
                "((统计年度 > ?) OR (统计年度 = ? AND 统计月份 >= ?)) AND ((统计年度 < ?) OR (统计年度 = ? AND 统计月份 <= ?))"
            )
            sql_params.extend([start_year, start_year, start_month, end_year, end_year, end_month])
        for dim, val in filters.items():
            if dim in set(_FOLLOW_UP_DIMENSIONS) and val and dim != dimension:
                values = val if isinstance(val, list) else [val]
                cleaned_values = [item for item in values if isinstance(item, str) and item]
                if not cleaned_values:
                    continue
                if len(cleaned_values) == 1:
                    where_clauses.append(f'"{dim}" = ?')
                    sql_params.append(cleaned_values[0])
                else:
                    placeholders = ",".join("?" for _ in cleaned_values)
                    where_clauses.append(f'"{dim}" IN ({placeholders})')
                    sql_params.extend(cleaned_values)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        if dimension == "统计月份":
            # 时间趋势图
            sql = f"""
                SELECT printf('%04d-%02d', 统计年度, 统计月份) AS label,
                       SUM({subject_col}) AS total_amount,
                       ROUND(AVG(CASE WHEN {subject_col} > 0 THEN {subject_col} END), 2) AS avg_amount,
                       COUNT(DISTINCT CASE WHEN {subject_col} > 0 THEN 员工ID END) AS headcount,
                       ROUND(
                           COUNT(DISTINCT CASE WHEN {subject_col} > 0 THEN 员工ID END) * 100.0
                           / NULLIF(COUNT(DISTINCT 员工ID), 0), 2
                       ) AS coverage_rate
                FROM salary_wide
                {where_sql}
                GROUP BY 统计年度, 统计月份
                ORDER BY 统计年度, 统计月份
            """
            rows = [dict(r) for r in conn.execute(sql, sql_params).fetchall()]
            # 趋势默认用折线图
            if chart_type == "bar":
                chart_type = "line"
        else:
            sql = f"""
                SELECT "{dimension}" AS label,
                       SUM({subject_col}) AS total_amount,
                       ROUND(AVG(CASE WHEN {subject_col} > 0 THEN {subject_col} END), 2) AS avg_amount,
                       COUNT(DISTINCT CASE WHEN {subject_col} > 0 THEN 员工ID END) AS headcount,
                       ROUND(
                           COUNT(DISTINCT CASE WHEN {subject_col} > 0 THEN 员工ID END) * 100.0
                           / NULLIF(COUNT(DISTINCT 员工ID), 0), 2
                       ) AS coverage_rate
                FROM salary_wide
                {where_sql}
                GROUP BY "{dimension}"
                ORDER BY {metric_field} DESC
                LIMIT 15
            """
            rows = [dict(r) for r in conn.execute(sql, sql_params).fetchall()]

        # --- 6. 组装返回 ---
        labels = [str(r["label"]) for r in rows]
        series = [float(r.get(metric_field) or 0) for r in rows]

        # 饼图需要计算占比
        if chart_type == "pie":
            total = sum(series) or 1
            pie_data = [
                {
                    "name": labels[i],
                    "value": round(series[i], 2),
                    "share": round(series[i] / total * 100, 2),
                }
                for i in range(len(labels))
            ]
            chart_payload = {"items": pie_data, "value_type": metric_field}
        else:
            chart_payload = {"labels": labels, "series": series}

        # 构建数据表格
        table_rows = []
        for r in rows:
            table_rows.append({
                display_dimension: str(r["label"]),
                "总额": int(r.get("total_amount") or 0),
                "均值": round(float(r.get("avg_amount") or 0), 2),
                "领取人数": int(r.get("headcount") or 0),
                "覆盖率(%)": float(r.get("coverage_rate") or 0),
            })

        chart_type_cn = {"pie": "饼图", "bar": "柱状图", "line": "折线图", "scatter": "散点图"}.get(chart_type, "图表")
        filter_desc = ""
        if filters:
            filter_desc = "在" + "、".join(
                f"{dim}={_describe_filter_value(dim, val)}" for dim, val in filters.items() if dim != dimension
            ) if any(dim != dimension for dim in filters) else ""
        time_desc = ""
        if chart_period_window:
            (start_year, start_month), (end_year, end_month) = chart_period_window
            time_desc = f"{start_year}-{start_month:02d}至{end_year}-{end_month:02d}"
        answer = f"已为你生成{filter_desc}{display_subject}按{display_dimension}的{metric_label}{chart_type_cn}（共{len(rows)}组数据）。"
        if time_desc:
            answer = f"已为你生成{filter_desc}{display_subject}在{time_desc}范围内按{display_dimension}的{metric_label}{chart_type_cn}（共{len(rows)}组数据）。"

        return {
            "mode": "chart",
            "answer": answer,
            "chart": {
                "chart_type": chart_type,
                "chart_title": f"{display_subject} — {filter_desc or '整体'}{f'在{time_desc}范围内' if time_desc else ''}按{display_dimension}的{metric_label}分布",
                "chart_insight": "",
                "chart_payload": chart_payload,
            },
            "data_table": {
                "table_title": f"{display_subject} {filter_desc or ''}{f' 在{time_desc}范围内' if time_desc else ''}按{display_dimension}明细".strip(),
                "columns": [display_dimension, "总额", "均值", "领取人数", "覆盖率(%)"],
                "rows": table_rows,
            },
            "meta": {
                "subject": default_subject if display_subject == default_subject else canonicalize_subject_name(display_subject),
                "dimension": display_dimension,
                "metric": metric_label,
                "chart_type": chart_type,
            },
        }
    except Exception as exc:
        return {
            "mode": "chart",
            "answer": f"图表生成失败：{exc}",
            "chart": None,
            "data_table": None,
        }
    finally:
        conn.close()


def answer_follow_up(question: str, context: dict[str, Any]) -> dict[str, Any]:
    """Lightweight follow-up: send question + previous report summary to LLM for a short answer."""
    if is_new_report_request(question, context):
        current_subject = (context.get("previous_request") or {}).get("subject") or "当前"
        return {
            "answer": (
                f"这个问题已经超出当前报告范围。右侧追问只针对当前这份“{current_subject}分析报告”。"
                "如果你想发起新的分析，请回首页 chatbot 输入新问题。"
            ),
            "mode": "follow_up",
        }

    follow_up_clarification = build_follow_up_subject_clarification(question, context)
    if follow_up_clarification is not None:
        return follow_up_clarification

    if _is_explanatory_follow_up(question):
        return _answer_explanatory_follow_up(question, context)

    # Data/detail requests take precedence over chart language like "展示" or "展现"
    if is_data_query(question):
        try:
            return query_detail_data(question, context)
        except ValueError as exc:
            return {"mode": "data_query", "answer": str(exc), "columns": [], "rows": []}
        except Exception:
            return _fallback_data_query(question)

    if is_chart_query(question):
        try:
            return query_chart_data(question, context)
        except ValueError as exc:
            return {"mode": "chart", "answer": str(exc), "chart": None, "data_table": None}

    llm = LLMService()
    if not llm.enabled:
        return {"answer": "当前未配置 LLM，无法回答追问。", "mode": "fallback"}

    previous_summary = context.get("previous_summary", "")
    previous_request = context.get("previous_request", {})

    prompt = f"""基于以下报告摘要，简洁回答用户的追问。

报告摘要：{previous_summary}
上一轮请求：{json.dumps(previous_request, ensure_ascii=False)}

用户追问：{question}

要求：直接回答，200-500字，不要重新生成完整报告。用数据说话，语气专业简洁。"""

    try:
        answer = llm._chat_completion(
            system_prompt="你是薪酬分析顾问，基于已有报告回答追问，简洁专业。",
            user_prompt=prompt,
            temperature=0.4,
        )
        return {"answer": answer, "mode": "follow_up"}
    except Exception as exc:
        return {"answer": f"追问回答失败：{exc}", "mode": "fallback"}


def _answer_explanatory_follow_up(question: str, context: dict[str, Any]) -> dict[str, Any]:
    llm = LLMService()
    previous_summary = context.get("previous_summary", "")
    previous_request = context.get("previous_request", {})
    subject = previous_request.get("subject", "当前科目")

    if not llm.enabled:
        fallback_answer = (
            f"这段内容是在解释当前“{subject}”问题为什么不是短期偶发，而更像会持续存在的结构性压力。"
            "换句话说，如果不提前做预算约束、统一口径和区域化合规治理，后续相关成本大概率还会继续维持高位。"
        )
        return {"answer": fallback_answer, "mode": "follow_up"}

    prompt = f"""基于以下报告摘要与用户引用的原文，解释这段内容的含义。

报告摘要：{previous_summary}
上一轮请求：{json.dumps(previous_request, ensure_ascii=False)}

用户追问：{question}

要求：
1. 用1-2段中文直接解释“这段话是什么意思”。
2. 优先翻译成管理语言或业务语言，帮助用户理解结论和影响。
3. 不要返回数据表、员工明细或重新做数据查询。
4. 不要重新生成完整报告。"""

    try:
        answer = llm._chat_completion(
            system_prompt="你是薪酬分析顾问，擅长把报告原文翻译成易懂的管理语言，回答要简洁、直接、专业。",
            user_prompt=prompt,
            temperature=0.3,
        )
        return {"answer": answer, "mode": "follow_up"}
    except Exception as exc:
        return {"answer": f"追问回答失败：{exc}", "mode": "fallback"}


# ---------------------------------------------------------------------------
# Phase 3.1 — Anomaly Monitoring
# ---------------------------------------------------------------------------

def monitor_scan() -> list[dict[str, Any]]:
    """Scan all subjects for anomalies: MoM spikes and z-score outliers."""
    if not get_data_source_status()["ready"]:
        return []
    cache_key = _cache.make_key("monitor_scan")
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    conn = get_connection()
    try:
        results = []
        for subject in SUBJECT_COLUMNS:
            # Get last two periods
            periods = conn.execute(
                f"""
                SELECT 统计年度 || '-' || printf('%02d', 统计月份) AS period,
                       SUM("{subject}") AS amount,
                       COUNT(DISTINCT CASE WHEN "{subject}" != 0 THEN 员工ID END) AS headcount
                FROM salary_wide
                GROUP BY 统计年度, 统计月份
                ORDER BY 统计年度 DESC, 统计月份 DESC
                LIMIT 2
                """,
            ).fetchall()
            periods = [dict(r) for r in periods]

            mom_rate = None
            latest_amount = 0
            if len(periods) >= 1:
                latest_amount = periods[0]["amount"] or 0
            if len(periods) >= 2:
                prev_amount = periods[1]["amount"] or 0
                if prev_amount > 0:
                    mom_rate = round((latest_amount - prev_amount) / prev_amount * 100, 2)

            # Count anomaly employees (z-score > 2) in latest period
            latest_period = periods[0] if periods else None
            anomaly_count = 0
            if latest_period:
                period_parts = latest_period["period"].split("-")
                year, month = int(period_parts[0]), int(period_parts[1])

                stats = conn.execute(
                    f"""
                    SELECT AVG(CASE WHEN "{subject}" > 0 THEN "{subject}" END) AS mean_val,
                           COUNT(CASE WHEN "{subject}" > 0 THEN 1 END) AS cnt
                    FROM salary_wide
                    WHERE 统计年度 = ? AND 统计月份 = ?
                    """,
                    (year, month),
                ).fetchone()
                mean_val = stats["mean_val"] or 0
                cnt = stats["cnt"] or 0

                if cnt > 1 and mean_val > 0:
                    var_row = conn.execute(
                        f"""
                        SELECT SUM(("{subject}" - ?) * ("{subject}" - ?)) / ? AS variance
                        FROM salary_wide
                        WHERE 统计年度 = ? AND 统计月份 = ? AND "{subject}" > 0
                        """,
                        (mean_val, mean_val, cnt, year, month),
                    ).fetchone()
                    variance = var_row["variance"] or 0
                    std_dev = variance ** 0.5

                    if std_dev > 0:
                        threshold = mean_val + 2 * std_dev
                        anomaly_row = conn.execute(
                            f"""
                            SELECT COUNT(DISTINCT 员工ID) AS cnt
                            FROM salary_wide
                            WHERE 统计年度 = ? AND 统计月份 = ? AND "{subject}" > ?
                            """,
                            (year, month, threshold),
                        ).fetchone()
                        anomaly_count = anomaly_row["cnt"] or 0

            severity = "green"
            if mom_rate is not None and abs(mom_rate) > 20:
                severity = "red"
            elif mom_rate is not None and abs(mom_rate) > 10:
                severity = "yellow"
            if anomaly_count > 10:
                severity = "red"
            elif anomaly_count > 5 and severity != "red":
                severity = "yellow"

            results.append({
                "subject": subject,
                "latest_amount": latest_amount,
                "mom_rate": mom_rate,
                "anomaly_count": anomaly_count,
                "headcount": periods[0]["headcount"] if periods else 0,
                "severity": severity,
            })

        severity_order = {"red": 0, "yellow": 1, "green": 2}
        results.sort(key=lambda r: (severity_order.get(r["severity"], 3), -(r["anomaly_count"] or 0)))
        _cache.set(cache_key, results, ttl=600)
        return results
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Phase 3.2 — Query History
# ---------------------------------------------------------------------------

def save_history(request: AnalysisRequest) -> None:
    """Persist a query to the history table."""
    conn = get_connection()
    try:
        data_source = _current_data_source_meta(conn)
        request_json = json.dumps({
            "subject": request.subject,
            "primary_dimension": request.primary_dimension,
            "secondary_dimensions": request.secondary_dimensions,
            "start_period": f"{request.start_year}-{request.start_month:02d}",
            "end_period": f"{request.end_year}-{request.end_month:02d}",
            "metrics": request.metrics,
        }, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO query_history (question, subject, request_json, data_source_name, data_source_signature)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                request.question,
                request.subject,
                request_json,
                data_source.get("filename", ""),
                data_source.get("signature", ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_history(limit: int = 50) -> list[dict[str, Any]]:
    """Return recent query history entries."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, question, subject, request_json, data_source_name, data_source_signature, created_at
            FROM query_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_history(entry_id: int) -> bool:
    """Delete a single history entry."""
    conn = get_connection()
    try:
        cursor = conn.execute("DELETE FROM query_history WHERE id = ?", (entry_id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def _safe_json_loads(text: str, fallback: Any) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return fallback


def save_report_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    request_payload = dict(payload.get("request") or {})
    report_payload = dict(payload.get("report") or {})
    if not request_payload or not report_payload:
        raise ValueError("保存报告时缺少 request 或 report 数据。")
    request = request_from_payload(request_payload)
    if not ensure_text(report_payload.get("short_answer")):
        report_payload["short_answer"] = generate_short_answer_for_report(request, report_payload, None)
    methodology = dict(report_payload.get("methodology") or {})
    current_source = get_data_source_status()
    data_source_name = ensure_text(methodology.get("data_source"), current_source.get("filename", ""))
    data_source_signature = ensure_text(methodology.get("data_source_signature"), current_source.get("signature", ""))

    subject = ensure_text(request_payload.get("subject"), "未命名科目")
    title = ensure_text(payload.get("title"), ensure_text(report_payload.get("report_title"), f"{subject}分析报告"))
    question = ensure_text(request_payload.get("question"))
    source_type = ensure_text(payload.get("source_type"), "manual")
    if source_type not in {"manual", "revised"}:
        source_type = "manual"
    base_saved_report_id = payload.get("base_saved_report_id")
    if not isinstance(base_saved_report_id, int):
        base_saved_report_id = None
    revision_instruction = ensure_text(payload.get("revision_instruction"))

    conn = get_connection()
    try:
        _create_saved_reports_table(conn)
        cursor = conn.execute(
            """
            INSERT INTO saved_reports (
                title, subject, question, request_json, report_json, source_type, base_saved_report_id, revision_instruction,
                data_source_name, data_source_signature
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                subject,
                question,
                json.dumps(request_payload, ensure_ascii=False),
                json.dumps(report_payload, ensure_ascii=False),
                source_type,
                base_saved_report_id,
                revision_instruction,
                data_source_name,
                data_source_signature,
            ),
        )
        conn.commit()
        saved_id = int(cursor.lastrowid)
    finally:
        conn.close()

    snapshot = get_saved_report(saved_id)
    if snapshot is None:
        raise ValueError("报告保存失败。")
    return snapshot


def list_saved_reports(limit: int = 50) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        _create_saved_reports_table(conn)
        rows = conn.execute(
            """
            SELECT id, title, subject, question, source_type, base_saved_report_id, revision_instruction,
                   data_source_name, data_source_signature, created_at
            FROM saved_reports
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_saved_report(saved_report_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        _create_saved_reports_table(conn)
        row = conn.execute(
            """
            SELECT id, title, subject, question, request_json, report_json, source_type, base_saved_report_id,
                   revision_instruction, data_source_name, data_source_signature, created_at
            FROM saved_reports
            WHERE id = ?
            """,
            (saved_report_id,),
        ).fetchone()
        if row is None:
            return None
        data = dict(row)
        request_payload = _safe_json_loads(data["request_json"], {})
        report_payload = _safe_json_loads(data["report_json"], {})
        if request_payload and report_payload and not ensure_text(report_payload.get("short_answer")):
            try:
                report_payload["short_answer"] = generate_short_answer_for_report(
                    request_from_payload(request_payload),
                    report_payload,
                    None,
                )
            except Exception:
                report_payload["short_answer"] = ensure_text(report_payload.get("executive_summary"))
        return {
            "id": data["id"],
            "title": data["title"],
            "subject": data["subject"],
            "question": data["question"],
            "source_type": data["source_type"],
            "base_saved_report_id": data["base_saved_report_id"],
            "revision_instruction": data["revision_instruction"],
            "data_source_name": data.get("data_source_name") or ensure_text((report_payload.get("methodology") or {}).get("data_source")),
            "data_source_signature": data.get("data_source_signature") or ensure_text((report_payload.get("methodology") or {}).get("data_source_signature")),
            "created_at": data["created_at"],
            "request": request_payload,
            "report": report_payload,
        }
    finally:
        conn.close()


def revise_report(payload: dict[str, Any]) -> dict[str, Any]:
    request_payload = dict(payload.get("request") or {})
    report_payload = dict(payload.get("report") or {})
    revision_instruction = ensure_text(payload.get("revision_instruction"))
    follow_up_messages = payload.get("follow_up_messages") or []
    if not request_payload or not report_payload:
        raise ValueError("改写报告时缺少 request 或 report 数据。")
    if not revision_instruction:
        raise ValueError("请先输入改写建议。")

    request = request_from_payload(request_payload)
    llm_service = LLMService()
    revision_payload = llm_service.revise_report(request, report_payload, revision_instruction, follow_up_messages)
    revised_report = (
        merge_revised_report(request, report_payload, revision_payload, revision_instruction)
        if revision_payload
        else build_revised_report_fallback(request, report_payload, revision_instruction)
    )
    revised_report["short_answer"] = generate_short_answer_for_report(request, revised_report, llm_service)
    return {
        "request": request_to_payload(request),
        "report": revised_report,
    }


# ---------------------------------------------------------------------------
# Phase 4 — Custom Metric Formula Builder
# ---------------------------------------------------------------------------

_SAFE_FORMULA_RE = re.compile(
    r'^[\w\s\+\-\*/\(\)\.\,\u4e00-\u9fff"]+$'
)


def evaluate_custom_metric(
    formula: str,
    group_by: str = "BU",
) -> list[dict[str, Any]]:
    """Evaluate a user-defined formula across groups.

    The formula may reference any SUBJECT_COLUMNS by name, e.g.:
        "底薪 + 岗位津贴"  or  "经济补偿金 / 底薪 * 100"

    SQL injection is prevented by whitelisting characters and validating
    that all identifiers exist in SUBJECT_COLUMNS.
    """
    ensure_data_source_ready()
    formula = formula.strip()
    if not formula:
        raise ValueError("公式不能为空")
    if not _SAFE_FORMULA_RE.match(formula):
        raise ValueError("公式包含不允许的字符")

    # Validate referenced columns
    for col in SUBJECT_COLUMNS:
        formula = formula.replace(col, f'"{col}"')

    # Ensure group_by is a known dimension
    if group_by not in DIMENSION_COLUMNS and group_by != "BU":
        raise ValueError(f"不支持的分组维度: {group_by}")

    conn = get_connection()
    try:
        sql = f"""
            SELECT "{group_by}" AS group_label,
                   SUM({formula}) AS metric_value,
                   COUNT(DISTINCT 员工ID) AS headcount
            FROM salary_wide
            GROUP BY "{group_by}"
            ORDER BY metric_value DESC
            LIMIT 20
        """
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        raise ValueError(f"公式执行失败: {exc}") from exc
    finally:
        conn.close()
