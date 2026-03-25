import unittest

from salary_reporting import (
    _infer_dimension_filters,
    _is_employee_aggregate_query,
    _normalize_dimension_filters,
    normalize_query_subject,
    parse_data_query_fast,
    query_detail_data,
    should_skip_llm_for_data_query,
)


class DetailQueryAggregationTests(unittest.TestCase):
    def test_detects_employee_total_query(self):
        question = "平安银行产品序列个人总共底薪最高TOP10"
        self.assertTrue(_is_employee_aggregate_query(question))


    def test_infers_product_sequence_as_function_filter(self):
        filters = _infer_dimension_filters("平安银行产品序列个人总共底薪最高TOP10")
        self.assertEqual(filters.get("BU"), "平安银行")
        self.assertEqual(filters.get("职能"), "产品")

    def test_normalizes_llm_style_sequence_value(self):
        filters = _normalize_dimension_filters({"BU": "平安银行", "职能": "产品序列"}, "平安银行产品序列个人总共底薪最高TOP10")
        self.assertEqual(filters.get("BU"), "平安银行")
        self.assertEqual(filters.get("职能"), "产品")

    def test_returns_employee_aggregated_rows(self):
        question = "平安银行产品序列个人总共底薪最高TOP10"
        result = query_detail_data(question, {"previous_request": {"subject": "底薪"}})
        self.assertEqual(result["mode"], "data_query")
        self.assertIn("累计底薪", result["columns"])
        self.assertIn("发放月数", result["columns"])
        self.assertIn("月均底薪", result["columns"])
        self.assertLessEqual(len(result["rows"]), 10)
        if result["rows"]:
            first = result["rows"][0]
            self.assertEqual(first["BU"], "平安银行")
            self.assertEqual(first["职能"], "产品")
            self.assertGreater(first["累计底薪"], 0)

    def test_generic_top_employee_question_uses_current_subject(self):
        question = "哪些员工贡献最大？"
        result = query_detail_data(question, {"previous_request": {"subject": "经济补偿金"}})
        self.assertEqual(result["mode"], "data_query")
        self.assertIn("累计经济补偿金", result["columns"])
        self.assertIn("贡献最高的前10名员工", result["answer"])

    def test_structured_question_skips_llm(self):
        question = "列出2026年8月经济补偿金top10"
        self.assertTrue(should_skip_llm_for_data_query(question, {"previous_request": {"subject": "经济补偿金"}}))
        parsed = parse_data_query_fast(question, "经济补偿金")
        self.assertEqual(parsed["year"], 2026)
        self.assertEqual(parsed["month"], 8)
        self.assertEqual(parsed["aggregation"], "row")

    def test_subject_none_falls_back_to_default_subject(self):
        self.assertEqual(normalize_query_subject(None, "经济补偿金"), "经济补偿金")

    def test_low_performance_high_salary_query_stays_on_current_subject(self):
        question = "列出平安寿险低绩效但底薪最高的前10人"
        parsed = parse_data_query_fast(question, "底薪")
        self.assertEqual(parsed["subject"], "底薪")
        self.assertEqual(parsed["filters"].get("BU"), "平安寿险")
        self.assertIn("绩效分位", parsed["filters"])

    def test_low_performance_high_salary_query_returns_rows(self):
        question = "列出平安寿险低绩效但底薪最高的前10人"
        result = query_detail_data(question, {"previous_request": {"subject": "底薪"}})
        self.assertEqual(result["mode"], "data_query")
        self.assertLessEqual(len(result["rows"]), 10)

    def test_multi_filter_summary_query_is_employee_aggregate(self):
        question = "帮我列出平安寿险产品与财务条线、C D级别、司龄8年以上老员工的补偿金金额在2025-2027两年内补偿金汇总TOP10列表"
        self.assertTrue(_is_employee_aggregate_query(question))
        parsed = parse_data_query_fast(question, "经济补偿金")
        self.assertEqual(parsed["aggregation"], "employee_total")
        self.assertEqual(parsed["start_year"], 2025)
        self.assertEqual(parsed["start_month"], 1)
        self.assertEqual(parsed["end_year"], 2027)
        self.assertEqual(parsed["end_month"], 1)
        self.assertEqual(parsed["subject"], "经济补偿金")
        self.assertEqual(parsed["filters"].get("BU"), "平安寿险")
        self.assertEqual(parsed["filters"].get("职能"), ["产品", "财务"])
        self.assertEqual(parsed["filters"].get("级别"), "CD类员工")
        self.assertEqual(parsed["filters"].get("司龄分箱"), ["8-10", "10年以上"])

    def test_multi_filter_summary_query_returns_aggregated_rows(self):
        question = "帮我列出平安寿险产品与财务条线、C D级别、司龄8年以上老员工的补偿金金额在2025-2027两年内补偿金汇总TOP10列表"
        result = query_detail_data(question, {"previous_request": {"subject": "经济补偿金"}})
        self.assertEqual(result["mode"], "data_query")
        self.assertIn("累计经济补偿金", result["columns"])
        self.assertIn("发放月数", result["columns"])
        self.assertIn("月均经济补偿金", result["columns"])
        self.assertNotIn("年度", result["columns"])
        self.assertNotIn("月份", result["columns"])
        self.assertIn("SUM 聚合排序", result["answer"])
        self.assertLessEqual(len(result["rows"]), 10)
        if result["rows"]:
            first = result["rows"][0]
            self.assertEqual(first["BU"], "平安寿险")
            self.assertIn(first["职能"], {"产品", "财务"})
            self.assertEqual(first["级别"], "CD类员工")
            self.assertIn(first["司龄分箱"], {"8-10", "10年以上"})
            self.assertGreater(first["累计经济补偿金"], 0)


if __name__ == '__main__':
    unittest.main()
