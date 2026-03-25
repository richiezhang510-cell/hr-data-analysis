from __future__ import annotations

import hashlib
import hmac
import os
import shutil
import time
from pathlib import Path
from typing import Any

# 自动加载项目根目录下的 .env 文件（如存在则生效，不存在则跳过）
def _load_simple_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
except ImportError:
    _load_simple_env(Path(__file__).resolve().parent / ".env")

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from salary_reporting import (
    activate_inferred_dataset,
    activate_dataset,
    answer_follow_up,
    delete_history,
    ensure_data_source_ready,
    evaluate_custom_metric,
    generate_report,
    generate_report_stream,
    get_data_source_status,
    get_saved_report,
    infer_schema_draft,
    init_database,
    list_history,
    list_saved_reports,
    metadata,
    monitor_scan,
    revise_report,
    save_report_snapshot,
)


APP_TITLE = "Comp Insight Studio"
BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIST_DIR = BASE_DIR / "frontend" / "dist"
UPLOADS_DIR = BASE_DIR / "uploads"
DEMO_DIR = BASE_DIR / "demo"

# JWT_SECRET 不再提供默认值！生产环境必须配置
JWT_SECRET = os.environ.get("JWT_SECRET", "")
if not JWT_SECRET and os.environ.get("JWT_SECRET_REQUIRED", "false").lower() == "true":
    raise RuntimeError("JWT_SECRET_REQUIRED=true 但未配置 JWT_SECRET 环境变量")

AUTH_ENABLED = os.environ.get("AUTH_ENABLED", "false").lower() == "true"

# 数据目录白名单（防止路径遍历攻击）
_ALLOWED_DATA_DIRS_ENV = os.environ.get("ALLOWED_DATA_DIRS", "")
ALLOWED_DATA_DIRS = [
    UPLOADS_DIR,
    DEMO_DIR,
    *(Path(d).expanduser().resolve() for d in _ALLOWED_DATA_DIRS_ENV.split(",") if d.strip()),
]


def _safe_resolve_path(raw_path: str, allow_absolute: bool = False) -> Path:
    """
    安全解析路径，防止目录遍历攻击。

    Args:
        raw_path: 原始路径字符串
        allow_absolute: 是否允许绝对路径（默认不允许）

    Returns:
        解析后的绝对路径

    Raises:
        HTTPException: 路径不安全或不在白名单内
    """
    # 基本验证
    if not raw_path:
        raise HTTPException(status_code=400, detail="路径不能为空")

    # 解析路径
    try:
        resolved = Path(raw_path).expanduser().resolve()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"路径解析失败: {e}")

    # 检查是否在允许的目录白名单内
    is_allowed = False
    for allowed_dir in ALLOWED_DATA_DIRS:
        try:
            resolved.relative_to(allowed_dir)
            is_allowed = True
            break
        except ValueError:
            continue

    if not is_allowed:
        allowed_str = ", ".join(str(d) for d in ALLOWED_DATA_DIRS)
        raise HTTPException(
            status_code=403,
            detail=f"路径不在允许的数据目录白名单中。允许的目录：{allowed_str}"
        )

    return resolved

app = FastAPI(title=APP_TITLE)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
if (FRONTEND_DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST_DIR / "assets"), name="frontend-assets")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.on_event("startup")
def startup() -> None:
    init_database()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    if (FRONTEND_DIST_DIR / "index.html").exists():
        return FileResponse(FRONTEND_DIST_DIR / "index.html")
    meta = metadata()
    initial_state = default_initial_state(meta)
    context = build_template_context(request, initial_state, meta)
    return templates.TemplateResponse("index.html", context)


@app.get("/legacy", response_class=HTMLResponse)
async def legacy_index(request: Request) -> HTMLResponse:
    meta = metadata()
    initial_state = default_initial_state(meta)
    context = build_template_context(request, initial_state, meta)
    return templates.TemplateResponse("index.html", context)


@app.post("/analyze", response_class=HTMLResponse)
async def analyze(request: Request) -> HTMLResponse:
    form = await request.form()
    payload = {
        "subject": form.get("subject"),
        "secondary_dimensions": form.getlist("secondary_dimensions"),
        "start_period": form.get("start_month"),
        "end_period": form.get("end_month"),
        "metrics": form.getlist("metrics"),
        "question": form.get("prompt"),
    }
    metrics = form.getlist("metrics")
    try:
        response = generate_report(payload)
    except ValueError as exc:
        meta = metadata()
        initial_state = default_initial_state(meta)
        initial_state["loading"] = False
        initial_state["filters"].update(
            {
                "subject": payload.get("subject"),
                "secondary_dimensions": payload.get("secondary_dimensions") or [],
                "start_month": payload.get("start_period"),
                "end_month": payload.get("end_period"),
                "prompt": payload.get("question") or "",
                "metrics": metrics or ["总额", "平均金额", "发放覆盖率"],
            }
        )
        initial_state["executive_summary"] = {
            "title": "当前筛选条件无法生成报告",
            "narrative": str(exc),
            "priority_callout": "请调整科目、日期或次维度后重试。",
        }
        context = build_template_context(request, initial_state, meta)
        return templates.TemplateResponse("index.html", context, status_code=400)

    meta = metadata()
    initial_state = build_initial_state(response, metrics)
    context = build_template_context(request, initial_state, meta)
    return templates.TemplateResponse("index.html", context)


@app.post("/api/report")
async def api_report(request: Request) -> JSONResponse:
    payload = await request.json()
    try:
        response = generate_report(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(response)


@app.post("/api/report/stream")
async def api_report_stream(request: Request) -> StreamingResponse:
    payload = await request.json()
    try:
        ensure_data_source_ready()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StreamingResponse(
        generate_report_stream(payload),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/follow-up")
async def api_follow_up(request: Request) -> JSONResponse:
    body = await request.json()
    question = body.get("question", "")
    context = body.get("context", {})

    result = answer_follow_up(question, context)
    return JSONResponse(result)


@app.get("/api/monitor")
async def api_monitor() -> JSONResponse:
    return JSONResponse(monitor_scan())


@app.get("/api/history")
async def api_history() -> JSONResponse:
    return JSONResponse(list_history())


@app.delete("/api/history/{entry_id}")
async def api_delete_history(entry_id: int) -> JSONResponse:
    ok = delete_history(entry_id)
    if not ok:
        raise HTTPException(status_code=404, detail="记录不存在")
    return JSONResponse({"ok": True})


@app.get("/api/metadata")
async def api_metadata() -> JSONResponse:
    return JSONResponse(metadata())


@app.get("/api/data-source")
async def api_data_source() -> JSONResponse:
    return JSONResponse(get_data_source_status())


@app.post("/api/data-source/activate-local")
async def api_activate_local_data_source(request: Request) -> JSONResponse:
    body = await request.json()
    raw_path = body.get("path", "")
    if not raw_path:
        raise HTTPException(status_code=400, detail="请提供本地 CSV 路径")
    try:
        # 使用安全的路径解析
        resolved_path = _safe_resolve_path(raw_path)
        draft = infer_schema_draft(resolved_path)
        if draft["mode"] == "registered_match":
            data_source = activate_dataset(resolved_path)
            return JSONResponse({"ok": True, "mode": "activated", "data_source": data_source})
        return JSONResponse({"ok": True, "mode": "inference_required", **draft})
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/data-source/infer-schema")
async def api_infer_schema(request: Request) -> JSONResponse:
    body = await request.json()
    raw_path = body.get("path", "")
    if not raw_path:
        raise HTTPException(status_code=400, detail="请提供本地 CSV 路径")
    try:
        # 使用安全的路径解析
        resolved_path = _safe_resolve_path(raw_path)
        draft = infer_schema_draft(resolved_path)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(draft)


@app.post("/api/data-source/activate-inferred")
async def api_activate_inferred_data_source(request: Request) -> JSONResponse:
    body = await request.json()
    raw_path = body.get("path", "")
    manifest = body.get("manifest") or {}
    if not raw_path:
        raise HTTPException(status_code=400, detail="请提供待激活的 CSV 路径")
    if not manifest:
        raise HTTPException(status_code=400, detail="请提供确认后的字段映射")
    try:
        # 使用安全的路径解析
        resolved_path = _safe_resolve_path(raw_path)
        data_source = activate_inferred_dataset(resolved_path, manifest)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"ok": True, "mode": "activated", "data_source": data_source})


@app.post("/api/saved-reports")
async def api_save_report(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        snapshot = save_report_snapshot(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(snapshot)


@app.get("/api/saved-reports")
async def api_saved_reports() -> JSONResponse:
    return JSONResponse(list_saved_reports())


@app.get("/api/saved-reports/{saved_report_id}")
async def api_saved_report_detail(saved_report_id: int) -> JSONResponse:
    snapshot = get_saved_report(saved_report_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="保存的报告不存在")
    return JSONResponse(snapshot)


@app.post("/api/report/revise")
async def api_revise_report(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        response = revise_report(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(response)


@app.get("/health")
async def health() -> JSONResponse:
    meta = metadata()
    return JSONResponse(
        {
            "status": "ok",
            "rows": meta["row_count"],
            "data_source_ready": meta.get("data_source", {}).get("ready", False),
        }
    )


# ---------------------------------------------------------------------------
# Auth helpers (simple HMAC-based token, opt-in via AUTH_ENABLED=true)
# ---------------------------------------------------------------------------

def _create_token(user: str) -> str:
    payload = f"{user}|{int(time.time()) + 86400}"
    sig = hmac.new(JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}|{sig}"


def _verify_token(token: str) -> str | None:
    parts = token.split("|")
    if len(parts) != 3:
        return None
    user, expires_str, sig = parts
    expected = hmac.new(JWT_SECRET.encode(), f"{user}|{expires_str}".encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    if int(expires_str) < int(time.time()):
        return None
    return user


@app.post("/api/auth/login")
async def api_login(request: Request) -> JSONResponse:
    body = await request.json()
    username = body.get("username", "")
    password = body.get("password", "")
    configured_password = os.environ.get("ADMIN_PASSWORD")
    if not configured_password:
        raise HTTPException(status_code=503, detail="未配置管理员密码，登录能力已关闭")
    if username == "admin" and password == configured_password:
        token = _create_token(username)
        return JSONResponse({"token": token, "user": username})
    raise HTTPException(status_code=401, detail="用户名或密码错误")


# ---------------------------------------------------------------------------
# CSV Upload
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)) -> JSONResponse:
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="仅支持 CSV 文件")

    # 安全处理文件名（防止路径遍历）
    safe_name = Path(file.filename).name
    if safe_name != file.filename or "/" in safe_name or "\\" in safe_name:
        raise HTTPException(status_code=400, detail="文件名不能包含路径分隔符")

    upload_dir = BASE_DIR / "uploads"
    upload_dir.mkdir(exist_ok=True)
    timestamped_name = f"{int(time.time())}_{safe_name}"
    dest = upload_dir / timestamped_name
    try:
        with dest.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)
        draft = infer_schema_draft(dest)
        if draft["mode"] == "registered_match":
            data_source = activate_dataset(dest)
            return JSONResponse(
                {
                    "ok": True,
                    "mode": "activated",
                    "filename": safe_name,
                    "stored_filename": timestamped_name,
                    "row_count": data_source["row_count"],
                    "period_start": data_source["period_start"],
                    "period_end": data_source["period_end"],
                    "data_source": data_source,
                }
            )
        return JSONResponse(
            {
                "ok": True,
                "mode": "inference_required",
                "filename": safe_name,
                "stored_filename": timestamped_name,
                **draft,
            }
        )
    except ValueError as exc:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/custom-metric")
async def api_custom_metric(request: Request) -> JSONResponse:
    body = await request.json()
    formula = body.get("formula", "")
    group_by = body.get("group_by", "BU")
    try:
        result = evaluate_custom_metric(formula, group_by)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(result)


def build_template_context(request: Request, initial_state: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "request": request,
        "page_title": APP_TITLE,
        "form_action": "/analyze",
        "subject_options": meta["subjects"],
        "secondary_dimension_options": meta["dimensions"],
        "metric_options": ["总额", "平均金额", "领取人数", "发放覆盖率", "占比", "环比", "同比"],
        "initial_state": initial_state,
    }


def default_initial_state(meta: dict[str, Any]) -> dict[str, Any]:
    data_source = meta.get("data_source", {})
    data_ready = bool(data_source.get("ready"))
    return {
        "loading": False,
        "filters": {
            "subject": "",
            "primary_dimension": "BU",
            "secondary_dimensions": [],
            "start_month": meta["period_start"],
            "end_month": meta["period_end"],
            "time_range_label": f"{meta['period_start']} 至 {meta['period_end']}" if data_ready else "待导入真实数据",
            "metrics": [],
            "prompt": "",
        },
        "executive_summary": {
            "title": "请先导入真实数据后再开始分析。" if not data_ready else "确认分析口径后，系统会基于当前活动数据源生成正式管理报告。",
            "narrative": (
                data_source.get("message", "请上传兼容当前宽表结构的真实 CSV 数据。")
                if not data_ready
                else "系统会基于当前活动数据源，围绕 BU 与多个次维度拆解规模、结构、覆盖率与异常信号，再汇总成正式管理判断。"
            ),
            "priority_callout": (
                "上传成功后，系统会自动导入、激活并刷新时间范围。"
                if not data_ready
                else "建议先用 2-3 个次维度生成首份报告，便于快速形成管理判断。"
            ),
        },
        "overview_cards": [
            {
                "label": "数据源状态",
                "context": "当前模式",
                "value": "已激活" if data_ready else "待导入",
                "insight": data_source.get("filename", "尚未选择真实 CSV 数据。"),
            },
            {
                "label": "数据规模",
                "context": "活动数据源",
                "value": f"{meta['row_count']:,} 行" if data_ready else "--",
                "insight": f"覆盖 {meta['period_start']} 至 {meta['period_end']} 的月度薪酬记录。" if data_ready else "导入后将展示真实数据的时间范围和记录规模。",
            },
            {
                "label": "主维度",
                "context": "固定分析轴",
                "value": "BU",
                "insight": "正式模式下所有分析都以 BU 为主轴，再叠加多次维度拆解。",
            },
            {
                "label": "次维度上限",
                "context": "单次分析",
                "value": "4 个",
                "insight": "兼顾报告深度、生成速度与可读性。",
            },
        ],
        "overview_charts": [],
        "dimension_reports": [],
        "consolidated_summary": {},
        "full_report": {
            "title": "完整分析报告",
            "subtitle": "生成后，这里会直接输出完整正文。",
            "body_paragraphs": [],
        },
    }


def extract_report_paragraphs(report: dict[str, Any]) -> list[str]:
    paragraphs: list[str] = []
    sections = report.get("full_report_sections")
    if isinstance(sections, list):
        for item in sections:
            if isinstance(item, dict):
                content = item.get("content")
                if isinstance(content, str) and content.strip():
                    paragraphs.append(content.strip())
            elif isinstance(item, str) and item.strip():
                paragraphs.append(item.strip())
    elif isinstance(sections, str) and sections.strip():
        paragraphs.append(sections.strip())

    body = report.get("full_report_body")
    if not paragraphs and isinstance(body, str) and body.strip():
        paragraphs = [part.strip() for part in body.split("\n\n") if part.strip()] or [body.strip()]

    if not paragraphs:
        summary = report.get("executive_summary")
        if isinstance(summary, str) and summary.strip():
            paragraphs = [summary.strip()]
    return paragraphs


def build_initial_state(response: dict[str, Any], selected_metrics: list[str]) -> dict[str, Any]:
    request = response["request"]
    report = response["report"]
    hero = report["hero_metrics"]
    trend_snapshot = hero.get("trend_snapshot", {})
    top_bu = report["bu_overview"][0] if report["bu_overview"] else None

    active_metrics = request.get("metrics") or selected_metrics or ["总额", "平均金额", "发放覆盖率"]
    overview_cards = []
    def trend_direction(value: float | int | None) -> str:
        if value is None:
            return "flat"
        if value > 0:
            return "up"
        if value < 0:
            return "down"
        return "flat"

    metric_card_builders = {
        "总额": {
            "label": "总额",
            "context": request["subject"],
            "value": f"¥ {hero['total_amount']:,}",
            "insight": "反映当前时间范围内该薪酬科目的总体支出规模。",
        },
        "平均金额": {
            "label": "平均金额",
            "context": "已发放员工",
            "value": f"¥ {hero['avg_amount']:,}",
            "insight": "仅按该科目实际领取员工计算，更适合观察口径差异。",
        },
        "领取人数": {
            "label": "领取人数",
            "context": "实际发放员工",
            "value": f"{hero['issued_employee_count']:,}",
            "insight": "用于区分总额变化是由覆盖范围扩大还是单人金额抬升驱动。",
        },
        "发放覆盖率": {
            "label": "发放覆盖率",
            "context": "领取员工 / 总员工",
            "value": f"{hero['coverage_rate']}%",
            "insight": "覆盖率越高，越可能代表制度性发放而非局部例外。",
        },
        "占比": {
            "label": "科目占比视角",
            "context": "结构重心",
            "value": top_bu["BU"] if top_bu else "--",
            "insight": "占比用于识别真正决定整体结构重心的头部 BU。",
        },
        "环比": {
            "label": "最新环比",
            "context": f"{trend_snapshot.get('latest_period', '--')} vs {trend_snapshot.get('previous_period', '--')}",
            "value": f"{trend_snapshot.get('mom_rate', 0):+.2f}%" if trend_snapshot.get("mom_rate") is not None else "--",
            "insight": (
                f"金额变化 ¥ {trend_snapshot.get('mom_delta', 0):+,.0f}"
                if trend_snapshot.get("mom_delta") is not None
                else "当前选择范围内缺少上月可比数据。"
            ),
            "trend_direction": trend_direction(trend_snapshot.get("mom_rate")),
            "trend_icon": "↑" if trend_direction(trend_snapshot.get("mom_rate")) == "up" else "↓" if trend_direction(trend_snapshot.get("mom_rate")) == "down" else "→",
        },
        "同比": {
            "label": "最新同比",
            "context": f"{trend_snapshot.get('latest_period', '--')} vs {trend_snapshot.get('yoy_period', '--')}",
            "value": f"{trend_snapshot.get('yoy_rate', 0):+.2f}%" if trend_snapshot.get("yoy_rate") is not None else "--",
            "insight": (
                f"金额变化 ¥ {trend_snapshot.get('yoy_delta', 0):+,.0f}"
                if trend_snapshot.get("yoy_delta") is not None
                else "当前选择范围内缺少去年同月可比数据。"
            ),
            "trend_direction": trend_direction(trend_snapshot.get("yoy_rate")),
            "trend_icon": "↑" if trend_direction(trend_snapshot.get("yoy_rate")) == "up" else "↓" if trend_direction(trend_snapshot.get("yoy_rate")) == "down" else "→",
        },
    }
    for metric in active_metrics:
        card = metric_card_builders.get(metric)
        if card:
            overview_cards.append(card)
    if top_bu and "总额" in active_metrics:
        overview_cards.append(
            {
                "label": "头部 BU",
                "context": "总额最高",
                "value": top_bu["BU"],
                "insight": f"总额 ¥ {int(top_bu['total_amount']):,}，覆盖率 {top_bu['coverage_rate']}%。",
            }
        )

    dimension_reports = []
    for item in report["dimension_reports"]:
        dimension_reports.append(
            {
                "dimension_name": item["dimension"],
                "title": f"按{item['dimension']}拆解",
                "headline": item["headline"],
                "key_findings": item["key_findings"],
                "anomalies": item["anomalies"],
                "possible_drivers": item["possible_drivers"],
                "management_implications": item["management_implications"],
                "chart_data": item["chart_data"],
            }
        )

    consolidated_summary = {
        "title": "跨维度综合判断",
        "summary": report["executive_summary"],
        "signal_pills": ["结构判断", "组织风险", "优先动作"],
        "chart_data": report["consolidated_charts"],
    }

    return {
        "loading": False,
        "filters": {
            "subject": request["subject"],
            "primary_dimension": request["primary_dimension"],
            "secondary_dimensions": request["secondary_dimensions"],
            "start_month": request["start_period"],
            "end_month": request["end_period"],
            "time_range_label": f"{request['start_period']} 至 {request['end_period']}",
            "metrics": active_metrics,
            "prompt": request["question"],
        },
        "executive_summary": {
            "title": report["report_title"],
            "narrative": report["executive_summary"],
            "priority_callout": report["priority_actions"][0] if report["priority_actions"] else "优先聚焦跨维度重复出现的异常 BU。",
        },
        "overview_cards": overview_cards,
        "overview_charts": report["overview_charts"],
        "dimension_reports": dimension_reports,
        "consolidated_summary": consolidated_summary,
        "full_report": {
            "title": report["report_title"],
            "subtitle": report["report_subtitle"],
            "body_paragraphs": extract_report_paragraphs(report),
        },
    }
