import unittest
from unittest.mock import patch

from salary_reporting import (
    AnalysisRequest,
    LLMService,
    build_external_research_empty_bundle,
    build_external_research_unavailable_bundle,
    build_fallback_full_report,
    collect_external_research,
    ensure_dimension_report_text_lists,
    normalize_chinese_punctuation,
)


class FallbackDimensionReportNormalizationTests(unittest.TestCase):
    def setUp(self):
        self.request = AnalysisRequest(
            subject="经济补偿金",
            primary_dimension="BU",
            secondary_dimensions=["职能", "级别"],
            start_year=2026,
            start_month=1,
            end_year=2026,
            end_month=12,
            metrics=["总额", "平均金额"],
            question="测试问题",
        )
        self.insight_bundle = {
            "hero_metrics": {
                "total_amount": 1000000,
                "avg_amount": 50000,
                "coverage_rate": 12.5,
            },
            "bu_overview": [
                {
                    "BU": "平安寿险",
                    "total_amount": 600000,
                    "coverage_rate": 18.2,
                }
            ],
        }
        self.external_research = {}

    def test_normalizes_non_list_fields_into_text_lists(self):
        report = {
            "dimension": "职能",
            "key_findings": {"title": "产品职能显著偏高"},
            "anomalies": None,
            "possible_drivers": {"reason": "项目补偿集中在核心产品团队"},
            "management_implications": "需要优先复核头部岗位补偿口径",
        }
        normalized = ensure_dimension_report_text_lists(report)
        self.assertIsInstance(normalized["key_findings"], list)
        self.assertIsInstance(normalized["possible_drivers"], list)
        self.assertIsInstance(normalized["management_implications"], list)
        self.assertEqual(normalized["anomalies"], [])
        self.assertTrue(normalized["possible_drivers"])

    def test_build_fallback_full_report_tolerates_dirty_dimension_fields(self):
        dimension_reports = [
            {
                "dimension": "职能",
                "headline": "产品序列补偿显著抬升",
                "key_findings": {"content": "产品职能的补偿金额集中在少数BU"},
                "possible_drivers": {"reason": "项目结束与组织调整叠加"},
                "management_implications": "优先复核产品序列补偿政策",
                "anomalies": None,
            },
            {
                "dimension": "级别",
                "headline": "高职级群体均值偏高",
                "key_findings": "O级与A级群体均值显著高于其他层级",
                "possible_drivers": None,
                "management_implications": {"value": "控制异常个案固薪外溢"},
                "anomalies": [],
            },
        ]
        sections = build_fallback_full_report(
            self.request,
            self.insight_bundle,
            dimension_reports,
            repeat_signals=["产品职能重复命中"],
            risk_lines=["高职级群体的补偿均值持续偏高"],
            action_lines=["优先核查产品序列补偿规则"],
            external_research=self.external_research,
        )
        self.assertTrue(sections)
        self.assertTrue(any(section.get("content") for section in sections))

    def test_fallback_consolidated_report_tolerates_dirty_dimension_fields(self):
        dimension_reports = [
            {
                "dimension": "职能",
                "headline": "产品职能补偿偏高",
                "key_findings": {"content": "产品职能在多个BU重复命中"},
                "possible_drivers": {"reason": "组织调整叠加项目收尾"},
                "management_implications": {"content": "优先复核该群体补偿规则"},
                "anomalies": {"content": "局部群体异常抬升"},
            }
        ]
        payload = LLMService()._fallback_consolidated_report(
            self.request,
            self.insight_bundle,
            dimension_reports,
            self.external_research,
        )
        self.assertIn("full_report_sections", payload)
        self.assertTrue(payload["full_report_sections"])

    def test_normalize_chinese_punctuation_collapses_duplicate_marks(self):
        text = "达到1457万元。；相比之下，另一个群体仅225万元。。。"
        normalized = normalize_chinese_punctuation(text)
        self.assertNotIn("。；", normalized)
        self.assertNotIn("。。", normalized)
        self.assertIn("相比之下", normalized)

    def test_fallback_cross_dimension_section_mentions_dimension_sources(self):
        dimension_reports = [
            {
                "dimension": "部门",
                "headline": "头部条线差异明显",
                "key_findings": ["平安科技的数据智能条线补偿金额明显偏高。"],
                "possible_drivers": ["项目调整与组织优化叠加。"],
                "management_implications": ["优先核查数据智能条线的补偿规则。"],
                "anomalies": [],
            },
            {
                "dimension": "级别",
                "headline": "中高职级群体集中度偏高",
                "key_findings": ["B级员工在多个BU重复进入头部。"],
                "possible_drivers": ["中层骨干群体的补偿标准明显更高。"],
                "management_implications": ["把中高职级群体纳入重点复核名单。"],
                "anomalies": [],
            },
        ]
        sections = build_fallback_full_report(
            self.request,
            self.insight_bundle,
            dimension_reports,
            repeat_signals=["数据智能条线与B级员工重复命中高值。"],
            risk_lines=["高敏感群体补偿标准可能被一次性事件放大。"],
            action_lines=["优先复核数据智能条线和B级员工的补偿口径。"],
            external_research=self.external_research,
        )
        target = next(section for section in sections if section["id"] == "dimension-deep-dive")
        self.assertIn("基于部门维度观察", target["content"])
        self.assertIn("基于级别维度观察", target["content"])
        self.assertIn("把部门、级别这些维度放在一起看", target["content"])

    @patch.dict("os.environ", {}, clear=False)
    def test_collect_external_research_marks_unavailable_without_tavily_key(self):
        bundle = collect_external_research(self.request, LLMService())
        self.assertEqual(bundle["research_mode"], "external_unavailable")
        self.assertTrue(bundle["external_research_summary"])

    @patch.dict("os.environ", {"TAVILY_API_KEY": "dummy-key"}, clear=False)
    @patch("salary_reporting.tavily_search", return_value=[])
    def test_collect_external_research_marks_empty_when_search_returns_nothing(self, _mock_search):
        bundle = collect_external_research(self.request, LLMService())
        self.assertEqual(bundle["research_mode"], "external_empty")
        self.assertTrue(bundle["external_research_summary"])

    def test_external_research_bundles_have_explanatory_summary(self):
        self.assertEqual(
            build_external_research_unavailable_bundle()["research_mode"],
            "external_unavailable",
        )
        self.assertEqual(
            build_external_research_empty_bundle()["research_mode"],
            "external_empty",
        )


if __name__ == "__main__":
    unittest.main()
