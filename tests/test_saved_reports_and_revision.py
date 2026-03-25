import unittest
from unittest.mock import patch

from salary_reporting import get_saved_report, init_database, list_saved_reports, revise_report, save_report_snapshot


def sample_request() -> dict:
    return {
        "subject": "底薪",
        "primary_dimension": "BU",
        "secondary_dimensions": ["职能", "级别", "绩效分位", "年龄分箱"],
        "start_period": "2026-01",
        "end_period": "2026-12",
        "metrics": ["总额", "平均金额", "领取人数"],
        "question": "帮我分析底薪结构",
    }


def sample_report() -> dict:
    return {
        "executive_summary": "底薪在不同 BU 和人群层级之间存在明显结构分层。",
        "cross_dimension_summary": ["产品与高职级群体重复命中高值。"],
        "priority_actions": ["优先核查头部 BU 的固薪结构。"],
        "global_risks": ["高固薪沉淀可能侵蚀调薪预算。"],
        "report_title": "底薪结构分析报告",
        "report_subtitle": "范围：2026-01 至 2026-12",
        "leadership_takeaways": ["差异背后是结构问题而非单点波动。"],
        "appendix_notes": ["数据快照来自原始宽表。"],
        "external_research_summary": ["外部实践普遍强调固浮比治理。"],
        "external_sources": [],
        "research_mode": "internal_only",
        "full_report_sections": [
            {
                "id": "section-1",
                "title": "总体判断",
                "content": "当前底薪差异已形成较强的结构性信号，值得优先治理。",
            },
            {
                "id": "section-2",
                "title": "治理建议",
                "content": "建议先从头部 BU 与关键岗位做口径复核，再逐步推动薪酬结构优化。",
            },
        ],
        "hero_metrics": {
            "total_amount": 123456789,
            "avg_amount": 9520.35,
            "employee_count": 1000,
            "issued_employee_count": 1000,
            "coverage_rate": 100.0,
        },
        "bu_overview": [],
        "overview_charts": [],
        "dimension_reports": [],
        "consolidated_charts": [],
        "sql_preview": [],
        "methodology": {
            "data_source": "test_薪酬数据_宽表.csv",
            "analysis_mode": "structured_sql_plus_llm",
            "note": "先做分维度洞察，再做跨维度综合归纳。",
        },
    }


class SavedReportsAndRevisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_database()

    def test_save_and_load_report_snapshot(self):
        snapshot = save_report_snapshot(
            {
                "request": sample_request(),
                "report": sample_report(),
                "source_type": "manual",
            }
        )
        self.assertIsInstance(snapshot["id"], int)
        detail = get_saved_report(snapshot["id"])
        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail["request"]["subject"], "底薪")
        self.assertEqual(detail["report"]["report_title"], "底薪结构分析报告")
        self.assertTrue(detail["report"]["short_answer"])
        self.assertTrue(detail["data_source_name"])
        self.assertTrue(detail["data_source_signature"])
        saved_ids = [item["id"] for item in list_saved_reports(limit=20)]
        self.assertIn(snapshot["id"], saved_ids)
        saved_summary = next(item for item in list_saved_reports(limit=20) if item["id"] == snapshot["id"])
        self.assertTrue(saved_summary["data_source_name"])
        self.assertTrue(saved_summary["data_source_signature"])

    @patch("salary_reporting.collect_insights", side_effect=AssertionError("revise_report should not re-read data"))
    @patch("salary_reporting.LLMService.generate_short_answer", return_value="这份底薪报告最值得关注的是高固薪群体的结构风险。建议先优先治理高底薪低绩效人群。")
    @patch(
        "salary_reporting.LLMService.revise_report",
        return_value={
            "executive_summary": "新版报告强调高固薪群体的治理优先级。",
            "report_subtitle": "范围：2026-01 至 2026-12 · 建议润色版",
            "priority_actions": ["优先聚焦高固薪低绩效群体。"],
            "full_report_sections": [
                {
                    "id": "section-1",
                    "title": "新版判断",
                    "content": "本版根据人工建议重组了报告主线，但保留原始数据结论。",
                }
            ],
        },
    )
    def test_revise_report_uses_existing_snapshot_only(self, _mock_llm_revise, _mock_short_answer, _mock_collect):
        response = revise_report(
            {
                "request": sample_request(),
                "report": sample_report(),
                "revision_instruction": "请更突出高底薪低绩效群体的治理建议。",
                "follow_up_messages": [{"question": "哪些人风险最大？", "answer": "主要集中在部分高职级群体。"}],
            }
        )
        self.assertEqual(response["request"]["subject"], "底薪")
        self.assertEqual(response["report"]["executive_summary"], "新版报告强调高固薪群体的治理优先级。")
        self.assertEqual(response["report"]["hero_metrics"]["total_amount"], 123456789)
        self.assertEqual(response["report"]["full_report_sections"][0]["title"], "新版判断")
        self.assertIn("高固薪群体", response["report"]["short_answer"])

    @patch("salary_reporting.collect_insights", side_effect=AssertionError("revise_report should not re-read data"))
    @patch("salary_reporting.LLMService.generate_short_answer", return_value=None)
    @patch("salary_reporting.LLMService.revise_report", return_value=None)
    def test_revise_report_falls_back_when_llm_revision_unavailable(self, _mock_llm_revise, _mock_short_answer, _mock_collect):
        response = revise_report(
            {
                "request": sample_request(),
                "report": sample_report(),
                "revision_instruction": "请改成更适合管理层汇报的风格。",
            }
        )
        self.assertIn("建议润色版", response["report"]["report_subtitle"])
        self.assertIn("不重新读取底层数据", response["report"]["executive_summary"])
        self.assertEqual(response["report"]["hero_metrics"]["total_amount"], 123456789)
        self.assertIn("帮我分析底薪结构", response["report"]["short_answer"])


if __name__ == "__main__":
    unittest.main()
