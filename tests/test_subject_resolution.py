from __future__ import annotations

from salary_reporting import (
    build_clarification_response,
    build_follow_up_subject_clarification,
    build_subject_catalog,
    configure_schema,
    resolve_subject,
)
from salary_schema import create_runtime_schema, get_schema


def test_unique_subject_full_name_passes_directly():
    configure_schema(get_schema("pingan_full"))
    result = resolve_subject("请分析底薪/基本工资", None, None)
    assert result.requires_confirmation is False
    assert result.resolved_subject == "底薪/基本工资"


def test_single_alias_without_ambiguity_can_pass():
    configure_schema(get_schema("pingan_full"))
    result = resolve_subject("帮我看看补偿金", "经济补偿金", "经济补偿金")
    assert result.requires_confirmation is False
    assert result.resolved_subject == "经济补偿金"


def test_ambiguous_generic_subject_requires_confirmation():
    configure_schema(get_schema("pingan_full"))
    result = resolve_subject("帮我看看绩效", None, None)
    assert result.requires_confirmation is True
    assert "绩效" in result.ambiguity_reason
    assert result.candidate_subjects


def test_multiple_subject_mentions_require_confirmation():
    configure_schema(get_schema("pingan_full"))
    result = resolve_subject("帮我对比底薪和经济补偿金", None, None)
    assert result.requires_confirmation is True
    assert len(result.candidate_subjects) >= 2


def test_clarification_response_prioritizes_subject_step_for_ambiguous_query():
    configure_schema(get_schema("pingan_full"))
    payload = build_clarification_response({"question": "帮我分析绩效"})
    assert payload is not None
    assert payload["mode"] == "clarification"
    assert payload["clarification"]["current_step"] == "subject"
    assert payload["clarification"]["needs_subject"] is True
    assert payload["clarification"]["subject_candidate_options"]
    assert payload["clarification"]["subject_catalog"] == build_subject_catalog()


def test_explicit_subject_selection_does_not_reenter_subject_clarification():
    configure_schema(get_schema("pingan_full"))
    payload = build_clarification_response(
        {
            "question": "帮我分析一下",
            "subject": "底薪/基本工资",
            "secondary_dimensions": ["部门", "级别", "去年绩效排名", "年龄分箱"],
            "metrics": ["总额", "平均金额"],
            "start_period": "2024-12",
            "end_period": "2025-12",
        }
    )
    assert payload is None


def test_generic_question_shows_multiple_subject_options_instead_of_only_default():
    configure_schema(get_schema("pingan_full"))
    payload = build_clarification_response({"question": "帮我分析一下"})
    assert payload is not None
    assert payload["clarification"]["needs_subject"] is True
    assert len(payload["clarification"]["subject_options"]) > 1
    assert "底薪/基本工资" in payload["clarification"]["subject_options"]
    assert "月度绩效" in payload["clarification"]["subject_options"]


def test_single_subject_runtime_schema_skips_subject_confirmation():
    runtime_schema = create_runtime_schema(
        {
            "schema_id": "inferred_runtime",
            "display_name": "测试异构宽表",
            "text_dimension_columns": ["统计月", "员工ID", "BU", "职能", "绩效分位", "级别", "司龄分箱", "年龄分箱", "部门"],
            "dimension_columns": ["BU", "部门", "统计月份"],
            "display_dimension_columns": ["部门"],
            "subject_columns": ["经济补偿金"],
            "default_subject": "经济补偿金",
            "default_secondary_dimensions": ["部门"],
            "source_column_map": {
                "统计月": "__period__",
                "员工ID": "工号",
                "BU": "组织单元",
                "职能": "__constant__",
                "绩效分位": "__constant__",
                "级别": "__constant__",
                "司龄分箱": "__constant__",
                "年龄分箱": "__constant__",
                "部门": "组织单元",
                "经济补偿金": "补偿金",
            },
            "synthetic_defaults": {
                "职能": "未提供",
                "绩效分位": "未提供",
                "级别": "未提供",
                "司龄分箱": "未提供",
                "年龄分箱": "未提供",
            },
            "capabilities": {
                "supports_trend_analysis": True,
                "supports_employee_level_detail": True,
            },
        }
    )
    configure_schema(runtime_schema)
    payload = build_clarification_response({"question": "帮我分析一下", "start_period": "2024-12", "end_period": "2025-01", "secondary_dimensions": ["部门"], "metrics": ["总额"]})
    assert payload is None


def test_follow_up_subject_clarification_returns_full_subject_catalog():
    configure_schema(get_schema("pingan_full"))
    clarification = build_follow_up_subject_clarification(
        "换个科目，我想看绩效",
        {
            "previous_request": {
                "subject": "底薪/基本工资",
                "secondary_dimensions": ["部门"],
                "start_period": "2024-12",
                "end_period": "2025-12",
                "metrics": ["总额"],
            }
        },
    )
    assert clarification is not None
    assert clarification["clarification"]["needs_subject"] is True
    assert clarification["clarification"]["subject_catalog"] == build_subject_catalog()


def test_single_subject_runtime_schema_skips_follow_up_subject_clarification():
    runtime_schema = create_runtime_schema(
        {
            "schema_id": "inferred_runtime",
            "display_name": "测试异构宽表",
            "text_dimension_columns": ["统计月", "员工ID", "BU", "职能", "绩效分位", "级别", "司龄分箱", "年龄分箱", "部门"],
            "dimension_columns": ["BU", "部门", "统计月份"],
            "display_dimension_columns": ["部门"],
            "subject_columns": ["经济补偿金"],
            "default_subject": "经济补偿金",
            "default_secondary_dimensions": ["部门"],
            "source_column_map": {
                "统计月": "__period__",
                "员工ID": "工号",
                "BU": "组织单元",
                "职能": "__constant__",
                "绩效分位": "__constant__",
                "级别": "__constant__",
                "司龄分箱": "__constant__",
                "年龄分箱": "__constant__",
                "部门": "组织单元",
                "经济补偿金": "补偿金",
            },
            "synthetic_defaults": {
                "职能": "未提供",
                "绩效分位": "未提供",
                "级别": "未提供",
                "司龄分箱": "未提供",
                "年龄分箱": "未提供",
            },
            "capabilities": {
                "supports_trend_analysis": True,
                "supports_employee_level_detail": True,
            },
        }
    )
    configure_schema(runtime_schema)
    clarification = build_follow_up_subject_clarification(
        "看一下补偿金",
        {
            "previous_request": {
                "subject": "经济补偿金",
                "secondary_dimensions": ["部门"],
                "start_period": "2024-12",
                "end_period": "2025-01",
                "metrics": ["总额"],
            }
        },
    )
    assert clarification is None
