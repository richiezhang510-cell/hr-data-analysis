import json
import unittest

from salary_reporting import _safe_json_repair, extract_json


class JsonRepairTests(unittest.TestCase):
    def test_repairs_bare_quotes_inside_string(self):
        raw = '{"executive_summary":"通过建立预算强控与标准化谈判体系，预计可降低非标支出15%-20%，节约年度成本2-3亿元；同步建立高龄高薪人员"软着陆"通道与PIP证据链标准化，将合规风险降至最低。"}'
        fixed = _safe_json_repair(raw)
        payload = json.loads(fixed)
        self.assertIn('"软着陆"', payload['executive_summary'])

    def test_repairs_chinese_quotes_and_newlines(self):
        raw = '{"full_report_sections":[{"id":"section-1","title":"执行摘要","content":"建立“劳动关系谈判专家组”后\n重点BU响应速度会提升"}],}'
        fixed = _safe_json_repair(raw)
        payload = json.loads(fixed)
        self.assertEqual(payload['full_report_sections'][0]['content'], '建立"劳动关系谈判专家组"后\n重点BU响应速度会提升')

    def test_keeps_valid_json_unchanged_semantically(self):
        raw = '{"report_title":"合法JSON","priority_actions":[{"action":"Q1完成盘点","priority":"P0","rationale":"锁定重点BU"}]}'
        fixed = _safe_json_repair(raw)
        self.assertEqual(json.loads(fixed), json.loads(raw))

    def test_extract_json_handles_wrapped_payload(self):
        text = '```json\n{"executive_summary":"针对高龄高薪人员"软着陆"方案，Q2上线预算管控系统。"}\n```'
        extracted = extract_json(text)
        payload = json.loads(extracted)
        self.assertIn('"软着陆"方案', payload['executive_summary'])


if __name__ == '__main__':
    unittest.main()
