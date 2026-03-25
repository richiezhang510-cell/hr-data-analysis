import { useAppStore } from "@/store"
import { Card, CardContent } from "@/components/ui/card"
import { formatPublishedAt } from "@/lib/formatters"
import type { ReportResponse, ReportSection, InlineTable, ChartConfig } from "@/types"
import { useRef, useCallback, useMemo } from "react"
import ReactECharts from "echarts-for-react"
import { buildChartOption } from "@/lib/chart-builder"

type Section = ReportSection

function formatImportedAt(value: string | undefined) {
  if (!value) return "未记录"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString("zh-CN", { hour12: false })
}

function normalizeSections(raw: ReportResponse["report"]["full_report_sections"]): Section[] {
  return raw
    .map((item, i) => {
      if (typeof item === "string") {
        return { id: `section-${i + 1}`, title: "", content: item.trim() }
      }
      return {
        id: item.id || `section-${i + 1}`,
        title: (item.title || "").trim(),
        content: (item.content || "").trim(),
        data_tables: item.data_tables,
        charts: item.charts,
      }
    })
    .filter((s) => s.content)
}

function highlightLeadSentence(content: string) {
  const firstPeriod = content.search(/[。！？\n]/)
  if (firstPeriod <= 0 || firstPeriod > 200) return <p>{content}</p>
  const lead = content.slice(0, firstPeriod + 1)
  const rest = content.slice(firstPeriod + 1).trim()
  return (
    <>
      <p className="font-semibold text-slate-100">{lead}</p>
      {rest && <p className="mt-3">{rest}</p>}
    </>
  )
}

function InlineDataTable({ table }: { table: InlineTable }) {
  return (
    <div className="my-4 overflow-x-auto rounded-xl border border-slate-700/50 bg-slate-900/40">
      {table.table_title && (
        <div className="border-b border-slate-700/40 px-4 py-2 text-xs font-medium text-slate-400">
          {table.table_title}
        </div>
      )}
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-700/50">
            {table.columns.map((col) => (
              <th key={col} className="px-3 py-2 text-left text-xs font-medium text-slate-400 whitespace-nowrap">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.map((row, ri) => (
            <tr key={ri} className="border-b border-slate-800/40 hover:bg-slate-800/30">
              {table.columns.map((col) => (
                <td key={col} className="px-3 py-1.5 text-xs whitespace-nowrap text-slate-300">
                  {typeof row[col] === "number" ? row[col].toLocaleString() : row[col]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function InlineChart({ chart }: { chart: ChartConfig }) {
  const option = useMemo(() => buildChartOption(chart), [chart])
  return (
    <div className="my-4">
      {chart.chart_title && (
        <p className="mb-1 text-xs font-medium text-slate-400">{chart.chart_title}</p>
      )}
      <div className="rounded-xl border border-slate-700/50 bg-slate-900/40 p-3">
        <ReactECharts option={option} style={{ height: 240 }} />
      </div>
      {chart.chart_insight && (
        <p className="mt-1 text-xs text-slate-500">{chart.chart_insight}</p>
      )}
    </div>
  )
}

function PriorityBadge({ priority }: { priority: string }) {
  const colors: Record<string, string> = {
    P0: "bg-red-500/20 text-red-400 border-red-500/30",
    P1: "bg-amber-500/20 text-amber-400 border-amber-500/30",
    P2: "bg-blue-500/20 text-blue-400 border-blue-500/30",
  }
  return (
    <span className={`inline-block rounded-md border px-2 py-0.5 text-xs font-bold ${colors[priority] || colors.P2}`}>
      {priority}
    </span>
  )
}

export default function ReportTab() {
  const report = useAppStore((s) => s.report)
  const metadata = useAppStore((s) => s.metadata)
  const sectionRefs = useRef<Record<string, HTMLElement | null>>({})

  const scrollTo = useCallback((id: string) => {
    sectionRefs.current[id]?.scrollIntoView({ behavior: "smooth", block: "start" })
  }, [])

  const sections = report ? normalizeSections(report.report.full_report_sections) : []
  const externalSources = report?.report.external_sources || []
  const researchMode = report?.report.research_mode || "internal_only"
  const researchStatusLabel =
    researchMode === "external_blended"
      ? "已纳入外部研究"
      : researchMode === "external_unavailable"
        ? "未启用外部搜索"
        : researchMode === "external_empty"
          ? "已搜索但未命中来源"
          : "仅内部数据"
  const priorityActions = report?.report.priority_actions || []

  const parsedActions = priorityActions.map((item) => {
    if (typeof item === "string") return { action: item, priority: "P1", rationale: "" }
    return { action: item.action || "", priority: item.priority || "P1", rationale: item.rationale || "" }
  })

  return (
    <Card className="rounded-[32px]">
      <CardContent className="space-y-0 p-8 md:p-12">
        <article className="mx-auto max-w-3xl" data-testid="full-report-article">
          {/* Header */}
          <header className="mb-8 border-b border-[rgba(56,189,248,0.15)] pb-8">
            <h1 className="text-2xl font-bold leading-snug tracking-tight text-slate-100" data-testid="full-report-title">
              {report?.report.report_title || "完整分析报告"}
            </h1>
            <p className="mt-2 text-sm text-slate-500" data-testid="full-report-subtitle">
              {report?.report.report_subtitle || `${metadata.period_start} - ${metadata.period_end}`}
            </p>
            <p className="mt-1 text-xs text-slate-600">
              生成时间：{new Date().toLocaleDateString("zh-CN")}
            </p>
            {report?.report.methodology ? (
              <div className="mt-3 flex flex-wrap gap-3 text-xs text-slate-500">
                <span>数据源：{report.report.methodology.data_source || "未标记数据源"}</span>
                <span>导入时间：{formatImportedAt(report.report.methodology.data_source_imported_at)}</span>
                <span>签名：{report.report.methodology.data_source_signature || "未记录"}</span>
              </div>
            ) : null}
          </header>

          {/* Executive Summary Card */}
          {report?.report.executive_summary && (
            <div className="mb-8 rounded-2xl border border-neon-cyan/20 bg-neon-cyan/5 p-6" data-testid="executive-summary-card">
              <div className="mb-2 flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-neon-cyan">
                <span>核心结论</span>
              </div>
              <p className="text-[15px] leading-8 text-slate-200">
                {report.report.executive_summary}
              </p>
            </div>
          )}

          {/* Table of Contents */}
          {sections.length > 1 && (
            <nav className="mb-10 rounded-2xl border border-slate-700/50 bg-slate-900/40 p-5" data-testid="report-table-of-contents">
              <p className="mb-3 text-xs font-bold uppercase tracking-widest text-slate-500">目录</p>
              <ol className="space-y-1.5">
                {sections.map((section, i) => (
                  <li key={section.id}>
                    <button
                      type="button"
                      onClick={() => scrollTo(section.id)}
                      className="text-sm text-slate-400 transition-colors hover:text-neon-cyan"
                    >
                      {i + 1}. {section.title || `第${i + 1}节`}
                    </button>
                  </li>
                ))}
                {parsedActions.length > 0 && (
                  <li>
                    <button
                      type="button"
                      onClick={() => scrollTo("action-roadmap")}
                      className="text-sm text-slate-400 transition-colors hover:text-neon-cyan"
                    >
                      {sections.length + 1}. 行动路线图
                    </button>
                  </li>
                )}
              </ol>
            </nav>
          )}

          {/* Report Sections */}
          <div className="space-y-10 text-[15px] leading-8 text-slate-300">
            {sections.length ? (
              sections.map((section, i) => (
                <section
                  key={section.id}
                  ref={(el) => { sectionRefs.current[section.id] = el }}
                  className="scroll-mt-8"
                  data-testid="report-section"
                >
                  <h2 className="mb-1 text-lg font-bold text-slate-100">
                    <span className="mr-2 text-neon-cyan">{i + 1}.</span>
                    {section.title || `第${i + 1}节`}
                  </h2>
                  <div className="mb-4 h-px bg-gradient-to-r from-neon-cyan/30 to-transparent" />
                  <div className="space-y-3">
                    {section.content.split(/\n\n+/).map((para, pi) => (
                      <div key={pi}>
                        {pi === 0 ? highlightLeadSentence(para) : <p>{para}</p>}
                      </div>
                    ))}
                  </div>
                  {section.data_tables && section.data_tables.length > 0 && (
                    <div className="mt-4 space-y-3">
                      {section.data_tables.map((table, ti) => (
                        <InlineDataTable key={ti} table={table} />
                      ))}
                    </div>
                  )}
                  {section.charts && section.charts.length > 0 && (
                    <div className="mt-4 space-y-3">
                      {section.charts.map((chart, ci) => (
                        <InlineChart key={ci} chart={chart} />
                      ))}
                    </div>
                  )}
                </section>
              ))
            ) : (
              <p className="text-slate-500">生成后，这里会直接输出完整正文。</p>
            )}
          </div>

          {/* Action Roadmap */}
          {parsedActions.length > 0 && (
            <section
              ref={(el) => { sectionRefs.current["action-roadmap"] = el }}
              className="mt-12 scroll-mt-8"
              data-testid="action-roadmap-section"
            >
              <h2 className="mb-1 text-lg font-bold text-slate-100">
                <span className="mr-2 text-neon-cyan">{sections.length + 1}.</span>
                行动路线图
              </h2>
              <div className="mb-5 h-px bg-gradient-to-r from-neon-cyan/30 to-transparent" />
              <div className="relative space-y-4 border-l-2 border-slate-700/60 pl-6">
                {parsedActions.map((action, i) => (
                  <div key={i} className="relative">
                    <div className="absolute -left-[31px] top-1 h-3 w-3 rounded-full border-2 border-neon-cyan bg-slate-950" />
                    <div className="flex items-start gap-3">
                      <PriorityBadge priority={action.priority} />
                      <div>
                        <p className="text-sm font-medium text-slate-200">{action.action}</p>
                        {action.rationale && (
                          <p className="mt-1 text-xs text-slate-500">{action.rationale}</p>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Appendix & Sources */}
          <footer className="mt-12 border-t border-[rgba(56,189,248,0.15)] pt-8">
            <div className="flex items-center justify-between gap-3">
              <h3 className="text-sm font-bold uppercase tracking-[0.12em] text-slate-100">附录 & 数据来源</h3>
              <span className="text-xs text-slate-500">
                {researchStatusLabel}
              </span>
            </div>

            {/* Appendix Notes */}
            {(report?.report.appendix_notes || []).length > 0 && (
              <div className="mt-4 space-y-1 text-xs text-slate-500">
                {report?.report.appendix_notes.map((note, i) => (
                  <p key={i}>{note}</p>
                ))}
              </div>
            )}

            {/* Methodology */}
            {report?.report.methodology && (
              <div className="mt-4 text-xs text-slate-500">
                <p>数据来源：{report.report.methodology.data_source} · 分析模式：{report.report.methodology.analysis_mode}</p>
                {report.report.methodology.data_source_imported_at ? (
                  <p>导入时间：{formatImportedAt(report.report.methodology.data_source_imported_at)}</p>
                ) : null}
                {report.report.methodology.data_source_signature ? <p>数据签名：{report.report.methodology.data_source_signature}</p> : null}
                {report.report.methodology.note && <p>{report.report.methodology.note}</p>}
              </div>
            )}

            {(report?.report.external_research_summary || []).length > 0 && (
              <p className="mt-4 text-sm leading-7 text-slate-400">
                {report?.report.external_research_summary.slice(0, 1).join("")}
              </p>
            )}

            {externalSources.length ? (
              <ol className="mt-5 space-y-3 text-sm leading-6 text-slate-400">
                {externalSources.map((source, index) => (
                  <li key={`${source.url}-${index}`} className="flex items-baseline gap-2">
                    <span className="shrink-0 text-xs font-semibold text-slate-500">[{index + 1}]</span>
                    <span>
                      <span className="font-medium text-slate-300">{source.source_name}</span>
                      {source.published_at ? <span className="mx-1 text-slate-500">({formatPublishedAt(source.published_at)})</span> : null}
                      {source.url ? (
                        <a
                          href={source.url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-neon-indigo underline decoration-neon-indigo/30 underline-offset-2 hover:text-neon-indigo/80"
                        >
                          {source.title}
                        </a>
                      ) : (
                        <span>{source.title}</span>
                      )}
                      {source.summary ? <span className="text-slate-500"> — {source.summary}</span> : null}
                    </span>
                  </li>
                ))}
              </ol>
            ) : (
              <p className="mt-4 text-sm text-slate-500">
                {researchMode === "external_unavailable"
                  ? "当前未配置外部搜索能力，缺少的是 TAVILY_API_KEY，不是 OPENAI_API_KEY。"
                  : researchMode === "external_empty"
                    ? "已执行外部搜索，但本次未命中通过筛选的合格来源。"
                    : researchMode === "external_blended"
                      ? "本次已纳入外部研究，但没有可额外展示的来源条目。"
                      : "当前报告仅基于内部数据生成。"}
              </p>
            )}
          </footer>
        </article>
      </CardContent>
    </Card>
  )
}
