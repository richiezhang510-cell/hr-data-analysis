import unittest
from unittest.mock import patch

from salary_reporting import answer_follow_up, is_data_query, is_new_report_request, parse_data_query_fast


class FollowUpSemanticExamplesTests(unittest.TestCase):
    def test_same_subject_chart_examples(self):
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        cases = [
            {
                "name": "same_subject_chart_bu_filter_perf_dimension",
                "question": "帮我做个饼图，展示一下各个平安寿险不同人员绩效分布对于底薪的贡献",
                "expected_title_fragment": "绩效分位",
                "expected_answer_fragment": "BU=平安寿险",
                "expected_first_column": "绩效分位",
            },
            {
                "name": "same_subject_chart_bu_filter_function_dimension",
                "question": "帮我做个柱状图，看平安寿险不同职能对底薪的贡献",
                "expected_title_fragment": "职能",
                "expected_answer_fragment": "BU=平安寿险",
                "expected_first_column": "职能",
            },
            {
                "name": "same_subject_chart_bu_filter_level_dimension",
                "question": "画个饼图，展示平安银行不同级别对底薪的占比",
                "expected_title_fragment": "级别",
                "expected_answer_fragment": "BU=平安银行",
                "expected_first_column": "级别",
            },
            {
                "name": "same_subject_chart_monthly_trend",
                "question": "展示一下底薪的月度趋势",
                "expected_title_fragment": "统计月份",
                "expected_answer_fragment": "底薪",
                "expected_first_column": "统计月份",
            },
            {
                "name": "same_subject_chart_monthly_trend_with_bu_filter_and_avg_metric",
                "question": "用折线图表示平安科技人均补偿金2年来每月的变化",
                "context_subject": "经济补偿金",
                "expected_title_fragment": "统计月份",
                "expected_answer_fragment": "BU=平安科技",
                "expected_first_column": "统计月份",
            },
        ]

        for case in cases:
            with self.subTest(case["name"]):
                case_context = context
                if case.get("context_subject"):
                    case_context = {
                        **context,
                        "previous_request": {"subject": case["context_subject"]},
                    }
                self.assertFalse(is_new_report_request(case["question"], case_context))
                result = answer_follow_up(case["question"], case_context)
                self.assertEqual(result["mode"], "chart")
                expected_subject = "经济补偿金" if "补偿金" in case["question"] else "底薪"
                self.assertEqual(result["meta"]["subject"], expected_subject)
                self.assertIn(case["expected_title_fragment"], result["chart"]["chart_title"])
                self.assertIn(case["expected_answer_fragment"], result["answer"])
                self.assertEqual(result["data_table"]["columns"][0], case["expected_first_column"])

    def test_same_subject_detail_examples(self):
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        cases = [
            {
                "name": "same_subject_detail_low_perf_high_salary",
                "question": "请找出一些平安寿险绩效排名靠后而底薪很高的人员信息",
            },
            {
                "name": "same_subject_detail_low_perf_top10",
                "question": "列出平安寿险低绩效但底薪最高的前10人",
            },
            {
                "name": "same_subject_detail_generic_top_employees",
                "question": "哪些员工贡献最大？",
            },
        ]

        for case in cases:
            with self.subTest(case["name"]):
                self.assertFalse(is_new_report_request(case["question"], context))
                result = answer_follow_up(case["question"], context)
                self.assertEqual(result["mode"], "data_query")
                self.assertTrue(result["columns"])
                self.assertLessEqual(len(result["rows"]), 10)
                self.assertIn("底薪", result["answer"])

    def test_same_subject_complex_detail_filters_stay_in_scope(self):
        context = {
            "previous_request": {"subject": "经济补偿金"},
            "previous_summary": "经济补偿金存在明显的人群分层。",
        }
        question = "平安寿险产品或财务职能、C级或D级（含O级管理层）、35至40岁年龄段、司龄3至10年、绩效前20% 这部分人有没有明细"
        self.assertFalse(is_new_report_request(question, context))
        parsed = parse_data_query_fast(question, "经济补偿金")
        self.assertEqual(parsed["subject"], "经济补偿金")
        self.assertEqual(parsed["aggregation"], "employee_total")
        self.assertEqual(parsed["filters"].get("BU"), "平安寿险")
        self.assertEqual(parsed["filters"].get("职能"), ["产品", "财务"])
        self.assertIn("级别", parsed["filters"])
        self.assertEqual(parsed["filters"].get("年龄分箱"), "35-40")
        self.assertEqual(parsed["filters"].get("司龄分箱"), ["3-5", "5-8", "8-10"])
        self.assertEqual(parsed["filters"].get("绩效分位"), "前20%")

        result = answer_follow_up(question, context)
        self.assertEqual(result["mode"], "data_query")
        self.assertIn("累计经济补偿金", result["columns"])
        self.assertNotIn("年度", result["columns"])
        self.assertNotIn("月份", result["columns"])
        self.assertIn("汇总名单", result["answer"])

    def test_same_subject_complex_detail_can_expand_to_raw_rows(self):
        context = {
            "previous_request": {"subject": "经济补偿金"},
            "previous_summary": "经济补偿金存在明显的人群分层。",
        }
        question = "平安寿险产品或财务职能、C级或D级（含O级管理层）、35至40岁年龄段、司龄3至10年、绩效前20% 这部分人按月份展开原始明细"
        self.assertFalse(is_new_report_request(question, context))
        parsed = parse_data_query_fast(question, "经济补偿金")
        self.assertEqual(parsed["aggregation"], "row")
        result = answer_follow_up(question, context)
        self.assertEqual(result["mode"], "data_query")
        self.assertIn("年度", result["columns"])
        self.assertIn("月份", result["columns"])


    def test_out_of_scope_new_analysis_examples(self):
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        cases = [
            "帮我分析经济补偿金",
            "重新生成底薪报告",
            "换一个科目，看签约金",
        ]

        for question in cases:
            with self.subTest(question):
                self.assertTrue(is_new_report_request(question, context))
                result = answer_follow_up(question, context)
                self.assertEqual(result["mode"], "follow_up")
                self.assertIn("请回首页 chatbot", result["answer"])

    def test_same_subject_explanatory_example_stays_in_scope(self):
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        question = "为什么平安寿险的底薪更高？"
        self.assertFalse(is_new_report_request(question, context))

    def test_explanatory_follow_up_examples(self):
        context = {
            "previous_request": {"subject": "经济补偿金"},
            "previous_summary": "经济补偿金高位运行并非一次性事件，更像结构性压力。",
        }
        cases = [
            "这部分内容什么意思？",
            "这段话怎么理解？",
            "为什么说这不是一次性事件？",
            "这条建议意味着什么？",
            (
                "趋势判断显示，当前的高成本并非一次性事件，而是持续性结构压力的体现。"
                "随着寿险行业代理人改革深化与金融科技替代加速，中后台岗位优化将成为常态。"
                "若不能建立前置性的预算约束与标准统一机制，未来三年经济补偿金支出将维持高位运行。"
                "这部分内容什么意思？"
            ),
        ]

        for question in cases:
            with self.subTest(question):
                self.assertFalse(is_new_report_request(question, context))
                with patch("salary_reporting.LLMService") as mock_llm_cls:
                    mock_llm = mock_llm_cls.return_value
                    mock_llm.enabled = True
                    mock_llm._chat_completion.return_value = "这段话的意思是，当前成本压力更像长期结构问题，需要提前做好预算和合规治理。"
                    result = answer_follow_up(question, context)
                self.assertEqual(result["mode"], "follow_up")
                self.assertNotIn("columns", result)
                self.assertNotIn("rows", result)

    def test_explanatory_language_does_not_trigger_data_query(self):
        cases = [
            "若不能建立前置性预算约束与标准统一机制，这部分内容什么意思？",
            "这个判断成立的前提是什么？",
            "前文这段结论怎么理解？",
        ]

        for question in cases:
            with self.subTest(question):
                self.assertFalse(is_data_query(question))


if __name__ == "__main__":
    unittest.main()
