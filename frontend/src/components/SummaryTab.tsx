import { ArrowUpRight, ArrowDownRight, Minus } from "lucide-react"
import ReactECharts from "echarts-for-react"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import { formatMoneySmart, formatPercent, formatCompact } from "@/lib/formatters"
import { buildChartOption } from "@/lib/chart-builder"
import { useAppStore } from "@/store"

function getTrendMeta(value: number | null | undefined) {
  if (value == null) return { icon: Minus, className: "text-slate-400 bg-slate-800/60", label: "持平" }
  if (value > 0) return { icon: ArrowUpRight, className: "text-emerald-400 bg-emerald-500/10 shadow-[0_0_12px_rgba(52,211,153,0.15)]", label: "上升" }
  if (value < 0) return { icon: ArrowDownRight, className: "text-red-400 bg-red-500/10 shadow-[0_0_12px_rgba(248,113,113,0.15)]", label: "下降" }
  return { icon: Minus, className: "text-slate-400 bg-slate-800/60", label: "持平" }
}

function splitAnswerParagraphs(text: string | undefined) {
  return (text || "")
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean)
}

function formatImportedAt(value: string | undefined) {
  if (!value) return "未记录"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString("zh-CN", { hour12: false })
}

export default function SummaryTab() {
  const report = useAppStore((s) => s.report)
  const metadata = useAppStore((s) => s.metadata)

  const heroMetrics = report?.report.hero_metrics
  const trendSnapshot = heroMetrics?.trend_snapshot
  const recognizedRequest = report?.request
  const activeMetrics = recognizedRequest?.metrics || []

  const trendCards = [
    {
      key: "mom",
      title: "最新环比",
      visible: activeMetrics.includes("环比"),
      value: trendSnapshot?.mom_rate,
      delta: trendSnapshot?.mom_delta,
      context: `${trendSnapshot?.latest_period || "--"} vs ${trendSnapshot?.previous_period || "--"}`,
    },
    {
      key: "yoy",
      title: "最新同比",
      visible: activeMetrics.includes("同比"),
      value: trendSnapshot?.yoy_rate,
      delta: trendSnapshot?.yoy_delta,
      context: `${trendSnapshot?.latest_period || "--"} vs ${trendSnapshot?.yoy_period || "--"}`,
    },
  ].filter((item) => item.visible)

  const summaryCards = [
    {
      label: "总额",
      value: heroMetrics ? formatMoneySmart(heroMetrics.total_amount) : "--",
      hint: "当前时间范围内的总体支出规模",
    },
    {
      label: "平均金额",
      value: heroMetrics ? formatMoneySmart(heroMetrics.avg_amount) : "--",
      hint: "按实际发放员工口径计算",
    },
    {
      label: "发放覆盖率",
      value: heroMetrics ? formatPercent(heroMetrics.coverage_rate) : "--",
      hint: "用于识别制度性发放",
    },
    {
      label: "领取人数",
      value: heroMetrics?.issued_employee_count?.toLocaleString("zh-CN") || "--",
      hint: "用于识别规模驱动与金额驱动",
    },
  ]

  const overviewCharts = report?.report.overview_charts || []
  const shortAnswerParagraphs = splitAnswerParagraphs(report?.report.short_answer || report?.report.executive_summary)
  const methodology = report?.report.methodology

  return (
    <Card className="rounded-[32px]">
      <CardHeader className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="space-y-2">
          <CardTitle className="text-2xl">
            {report?.report.report_title || "等待生成结果"}
          </CardTitle>
          <CardDescription className="max-w-3xl leading-6">
            {report?.report.report_subtitle || `${metadata.period_start} - ${metadata.period_end}`}
          </CardDescription>
        </div>
        <div className="flex flex-wrap gap-2">
          {activeMetrics.map((metric) => (
            <Badge key={metric} variant="secondary" className="rounded-full px-3 py-1">
              {metric}
            </Badge>
          ))}
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {methodology ? (
          <Card className="rounded-[28px] border-[rgba(56,189,248,0.1)] bg-slate-800/40 shadow-none" data-testid="report-methodology-card">
            <CardContent className="flex flex-col gap-3 p-6 text-sm text-slate-400 md:flex-row md:items-center md:justify-between">
              <div>
                <div className="text-xs uppercase tracking-[0.16em] text-slate-500">报告数据快照</div>
                <div className="mt-2 text-base font-medium text-slate-100" data-testid="methodology-data-source">{methodology.data_source || "未标记数据源"}</div>
              </div>
              <div className="flex flex-wrap gap-4 text-xs text-slate-500">
                <span>导入时间：{formatImportedAt(methodology.data_source_imported_at)}</span>
                <span>签名：{methodology.data_source_signature || "未记录"}</span>
                <span>模式：{methodology.analysis_mode}</span>
              </div>
            </CardContent>
          </Card>
        ) : null}

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {summaryCards.map((item) => (
            <Card key={item.label} className="rounded-[28px] border-[rgba(56,189,248,0.1)] bg-slate-800/40 shadow-none">
              <CardContent className="p-6">
                <p className="text-sm text-slate-500">{item.label}</p>
                <div className="mt-3 text-3xl font-semibold tracking-tight text-white">
                  {item.value}
                </div>
                <p className="mt-2 text-sm text-slate-500">{item.hint}</p>
              </CardContent>
            </Card>
          ))}
        </div>

        {trendCards.length ? (
          <div className="grid gap-4 md:grid-cols-2">
            {trendCards.map((item) => {
              const trend = getTrendMeta(item.value)
              const Icon = trend.icon
              return (
                <Card key={item.key} className="rounded-[28px] border-[rgba(56,189,248,0.1)] bg-slate-800/40 shadow-none">
                  <CardContent className="flex items-center justify-between gap-4 p-6">
                    <div className="space-y-2">
                      <p className="text-sm text-slate-500">{item.title}</p>
                      <div className="text-3xl font-semibold tracking-tight text-white">
                        {item.value == null ? "--" : `${item.value >= 0 ? "+" : ""}${item.value.toFixed(2)}%`}
                      </div>
                      <div className="text-sm text-slate-500">{item.context}</div>
                      <div className="text-sm text-slate-400">
                        金额变化 {item.delta == null ? "--" : `¥ ${item.delta >= 0 ? "+" : ""}${formatCompact(item.delta)}`}
                      </div>
                    </div>
                    <div className={cn("flex h-14 w-14 items-center justify-center rounded-full", trend.className)}>
                      <Icon className="h-6 w-6" />
                    </div>
                  </CardContent>
                </Card>
              )
            })}
          </div>
        ) : null}

        <Card className="rounded-[28px] border-[rgba(56,189,248,0.1)] bg-slate-800/40 shadow-none" data-testid="ai-short-answer-card">
          <CardHeader>
            <CardTitle className="text-lg">AI 简短回答</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4 text-sm leading-7 text-slate-400">
            <div className="space-y-3" data-testid="ai-short-answer-content">
              {shortAnswerParagraphs.length ? (
                shortAnswerParagraphs.map((paragraph, index) => (
                  <p key={`${paragraph}-${index}`}>{paragraph}</p>
                ))
              ) : (
                <p>输入问题后，这里会输出一段简短回答。</p>
              )}
            </div>
            <div className="flex flex-wrap gap-2">
              {(report?.report.cross_dimension_summary || []).slice(0, 4).map((item, index) => (
                <Badge key={`${item}-${index}`} variant="secondary" className="rounded-full px-3 py-1">
                  {item}
                </Badge>
              ))}
            </div>
          </CardContent>
        </Card>

        {overviewCharts.length ? (
          <div className="grid gap-4 lg:grid-cols-2">
            {overviewCharts.map((chart) => (
              <Card key={chart.chart_title} className="rounded-[28px]">
                <CardHeader>
                  <CardTitle className="text-base">{chart.chart_title}</CardTitle>
                  <CardDescription>{chart.chart_insight}</CardDescription>
                </CardHeader>
                <CardContent className="pt-0">
                  <ReactECharts option={buildChartOption(chart)} style={{ height: 320 }} notMerge lazyUpdate />
                </CardContent>
              </Card>
            ))}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
