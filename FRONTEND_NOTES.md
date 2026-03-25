# Frontend Notes

## Files
- `templates/index.html`
- `static/app.css`
- `static/app.js`

## Expected Server Data Shape

The template is designed for a server-rendered `initial_state` object with this approximate structure:

```json
{
  "loading": false,
  "filters": {
    "subject": "经济补偿金",
    "primary_dimension": "BU",
    "secondary_dimensions": ["职能", "级别", "绩效分位"],
    "start_month": "2025-01",
    "end_month": "2027-01",
    "time_range_label": "2025-01 至 2027-01",
    "metrics": ["总额", "平均金额", "发放覆盖率"],
    "prompt": "分析异常 BU 的潜在原因并给出建议"
  },
  "executive_summary": {
    "title": "一句话总论",
    "narrative": "摘要正文",
    "priority_callout": "优先处理建议"
  },
  "overview_cards": [
    {
      "label": "总额",
      "context": "全样本",
      "value": "¥ 2.31 亿",
      "insight": "一句话解释"
    }
  ],
  "dimension_reports": [
    {
      "dimension_name": "职能",
      "title": "按职能拆解",
      "headline": "一句话结论",
      "chart_title": "BU x 职能结构图",
      "chart_caption": "图表说明",
      "key_findings": ["...", "..."],
      "anomalies": ["..."],
      "possible_drivers": ["..."],
      "management_implications": ["..."]
    }
  ],
  "consolidated_summary": {
    "title": "跨维度综合判断",
    "summary": "综合总结",
    "signal_pills": ["重复信号", "结构性问题", "优先建议"],
    "chart_title": "解释力比较图",
    "chart_caption": "图表说明",
    "cross_dimension_signals": ["..."],
    "structural_judgements": ["..."],
    "priority_actions": ["...", "..."]
  }
}
```

## UX Notes
- The page is intentionally report-first rather than dashboard-first.
- Empty states are styled to still feel presentation-ready.
- `app.js` only handles light progressive enhancement so server rendering remains the source of truth.
