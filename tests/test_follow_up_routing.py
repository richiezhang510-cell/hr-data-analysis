import unittest

from salary_reporting import answer_follow_up, is_chart_query, is_data_query, is_new_report_request


class FollowUpRoutingTests(unittest.TestCase):
    def test_detail_request_with_display_language_prefers_data_query(self):
        question = "有没有明细数据展现平安寿险职能为产品补偿金前10名"
        self.assertTrue(is_data_query(question))
        self.assertFalse(is_chart_query(question))

    def test_explicit_chart_request_still_routes_to_chart(self):
        question = "请展示平安寿险按职能的经济补偿金柱状图"
        self.assertFalse(is_data_query(question))
        self.assertTrue(is_chart_query(question))

    def test_same_subject_detail_question_is_not_treated_as_new_report(self):
        question = "请找出一些平安寿险绩效排名靠后而底薪很高的人员信息"
        context = {"previous_request": {"subject": "底薪"}}
        self.assertFalse(is_new_report_request(question, context))

    def test_different_subject_still_requires_new_report(self):
        question = "帮我分析经济补偿金"
        context = {"previous_request": {"subject": "底薪"}}
        self.assertTrue(is_new_report_request(question, context))

    def test_follow_up_stays_in_scope_for_same_subject_detail_query(self):
        question = "请找出一些平安寿险绩效排名靠后而底薪很高的人员信息"
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        result = answer_follow_up(question, context)
        self.assertEqual(result["mode"], "data_query")

    def test_same_subject_chart_question_is_not_treated_as_new_report(self):
        question = "帮我做个饼图，展示一下各个平安寿险不同人员绩效分布对于底薪的贡献"
        context = {"previous_request": {"subject": "底薪"}}
        self.assertFalse(is_new_report_request(question, context))

    def test_follow_up_stays_in_scope_for_same_subject_chart_query(self):
        question = "帮我做个饼图，展示一下各个平安寿险不同人员绩效分布对于底薪的贡献"
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        result = answer_follow_up(question, context)
        self.assertEqual(result["mode"], "chart")
        self.assertIn("绩效分位", result["chart"]["chart_title"])
        self.assertIn("BU=平安寿险", result["answer"])

    def test_ambiguous_follow_up_subject_returns_clarification(self):
        question = "看一下绩效"
        context = {
            "previous_request": {"subject": "底薪"},
            "previous_summary": "底薪存在结构分层。",
        }
        result = answer_follow_up(question, context)
        self.assertEqual(result["mode"], "clarification")
        self.assertTrue(result["clarification"]["needs_subject"])


if __name__ == '__main__':
    unittest.main()
