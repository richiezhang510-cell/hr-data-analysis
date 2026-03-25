"""
LLM 并发维度分析测试

测试并发分析的正确性、顺序保持和降级逻辑。

运行方式：
    python -m pytest tests/test_llm_concurrency.py -v
"""
import threading
import time
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestConcurrentDimensionAnalysis:
    """测试并发维度分析"""

    def _make_request(self):
        from salary_reporting import AnalysisRequest
        return AnalysisRequest(
            subject="基本工资",
            primary_dimension="BU",
            secondary_dimensions=["级别", "部门", "绩效分位"],
            start_year=2024,
            start_month=1,
            end_year=2024,
            end_month=12,
            metrics=["总额", "平均金额"],
            question="",
        )

    def _make_insights(self, dimensions=("级别", "部门", "绩效分位")):
        return [
            {
                "dimension": dim,
                "derived_summary": {
                    "headline": f"{dim} 维度标题",
                    "facts": [f"{dim} 事实1", f"{dim} 事实2"],
                    "drivers": [f"{dim} 驱动因素"],
                    "management_implications": [f"{dim} 管理建议"],
                },
                "chart_bundle": {},
                "anomalies": [],
                "anomaly_people": [],
                "grouped_rows": [],
                "dimension_values": [],
                "trend_rows": [],
                "sql": "",
            }
            for dim in dimensions
        ]

    def test_function_exists(self):
        """测试并发分析函数存在"""
        from salary_reporting import analyze_dimensions_concurrent
        assert callable(analyze_dimensions_concurrent)

    def test_result_count_matches_input(self):
        """测试结果数量与输入维度数量一致"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService
        request = self._make_request()
        insights = self._make_insights(["级别", "部门"])
        llm = LLMService()
        reports = analyze_dimensions_concurrent(request, insights, llm)
        assert len(reports) == 2

    def test_order_preserved(self):
        """测试输出顺序与输入顺序一致"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService
        dims = ["级别", "部门", "绩效分位"]
        request = self._make_request()
        insights = self._make_insights(dims)
        llm = LLMService()
        reports = analyze_dimensions_concurrent(request, insights, llm)
        for i, dim in enumerate(dims):
            assert reports[i].get("dimension") == dim, (
                f"顺序不一致：期望 {dim}，得到 {reports[i].get('dimension')}"
            )

    def test_empty_insights(self):
        """测试空维度列表"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService
        request = self._make_request()
        llm = LLMService()
        reports = analyze_dimensions_concurrent(request, [], llm)
        assert reports == []

    def test_single_insight_serial_path(self):
        """单维度应走串行路径，不出错"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService
        request = self._make_request()
        insights = self._make_insights(["级别"])
        llm = LLMService()
        reports = analyze_dimensions_concurrent(request, insights, llm)
        assert len(reports) == 1
        assert reports[0]["dimension"] == "级别"

    def test_fallback_mode_no_api_key(self):
        """无 API Key 时走降级模板模式"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService
        request = self._make_request()
        insights = self._make_insights(["级别", "部门"])
        llm = LLMService()
        if llm.enabled:
            pytest.skip("配置了 OPENAI_API_KEY，跳过降级模式测试")
        reports = analyze_dimensions_concurrent(request, insights, llm)
        assert all(r.get("source_mode") == "template" for r in reports)

    def test_thread_safety_of_concurrent_calls(self):
        """测试并发调用本身的线程安全性（模拟多个并发请求）"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService
        results = []
        errors = []

        def run_analysis():
            try:
                request = self._make_request()
                insights = self._make_insights(["级别"])
                llm = LLMService()
                reports = analyze_dimensions_concurrent(request, insights, llm)
                results.append(reports)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=run_analysis) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"并发调用失败: {errors}"
        assert len(results) == 5
