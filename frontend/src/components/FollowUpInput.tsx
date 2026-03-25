import { ChevronLeft, ChevronRight, GripVertical, Loader2, Send, Sparkles } from "lucide-react"
import { useMemo, useRef, useState } from "react"
import ReactECharts from "echarts-for-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { useAppStore } from "@/store"
import { buildChartOption } from "@/lib/chart-builder"
import type { ChartConfig, InlineTable } from "@/types"

const followUpSuggestions = [
  "能再按级别细分一下吗？",
  "哪些员工贡献最大？",
  "请把这份报告改成更适合管理层汇报的版本。",
  "环比变化的主要驱动因素是什么？",
]

function formatFollowUpAnswer(answer: string) {
  const cleaned = answer
    .replace(/\*\*/g, "")
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .trim()

  return cleaned
    .replace(/([：:])\s*/g, "$1\n")
    .replace(/([。！？])(?=[^\n])/g, "$1\n")
    .replace(/\n{3,}/g, "\n\n")
    .split(/\n{2,}|\n/)
    .map((part) => part.trim())
    .filter(Boolean)
}

function FollowUpParagraph({ text }: { text: string }) {
  const headingMatch = text.match(/^([^：:]{2,24}[：:])\s*(.*)$/)
  if (headingMatch) {
    return (
      <div className="rounded-[20px] border border-[rgba(56,189,248,0.08)] bg-slate-950/35 px-4 py-3">
        <div className="text-xs uppercase tracking-[0.14em] text-slate-500">{headingMatch[1].replace(/[：:]$/, "")}</div>
        <p className="mt-2 text-sm leading-7 text-slate-200">{headingMatch[2]}</p>
      </div>
    )
  }

  return <p className="text-sm leading-7 text-slate-200">{text}</p>
}

function FollowUpTable({ table }: { table: InlineTable }) {
  return (
    <div className="mt-4 overflow-hidden rounded-[22px] border border-slate-700/50 bg-slate-950/45">
      {table.table_title ? (
        <div className="border-b border-slate-700/40 px-4 py-3 text-xs font-medium uppercase tracking-[0.12em] text-slate-400">
          {table.table_title}
        </div>
      ) : null}
      <div className="max-h-[360px] overflow-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-slate-950/95">
            <tr className="border-b border-slate-700/60">
              {table.columns.map((col) => (
                <th key={col} className="whitespace-nowrap px-3 py-2.5 text-left font-medium text-slate-400">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {table.rows.map((row, ri) => (
              <tr key={ri} className="border-b border-slate-700/30 hover:bg-slate-700/15">
                {table.columns.map((col) => (
                  <td key={col} className="whitespace-nowrap px-3 py-2.5 text-slate-200">
                    {typeof row[col] === "number" ? row[col].toLocaleString() : row[col]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function FollowUpChart({ chart }: { chart: ChartConfig }) {
  const option = useMemo(() => buildChartOption(chart), [chart])
  return (
    <div className="mt-4">
      {chart.chart_title ? <p className="mb-2 text-sm font-medium text-slate-200">{chart.chart_title}</p> : null}
      <div className="rounded-[22px] border border-slate-700/50 bg-slate-950/45 p-4">
        <ReactECharts option={option} style={{ height: 360 }} notMerge lazyUpdate />
      </div>
      {chart.chart_insight ? <p className="mt-2 text-xs leading-6 text-slate-500">{chart.chart_insight}</p> : null}
    </div>
  )
}

export default function FollowUpInput() {
  const report = useAppStore((s) => s.report)
  const previousRequest = useAppStore((s) => s.previousRequest)
  const previousSummary = useAppStore((s) => s.previousSummary)
  const followUpMessages = useAppStore((s) => s.followUpMessages)
  const appendFollowUp = useAppStore((s) => s.appendFollowUp)
  const isFollowUpLoading = useAppStore((s) => s.isFollowUpLoading)
  const setIsFollowUpLoading = useAppStore((s) => s.setIsFollowUpLoading)
  const setError = useAppStore((s) => s.setError)
  const followUpDrawerOpen = useAppStore((s) => s.followUpDrawerOpen)
  const setFollowUpDrawerOpen = useAppStore((s) => s.setFollowUpDrawerOpen)
  const followUpDrawerWidth = useAppStore((s) => s.followUpDrawerWidth)
  const setFollowUpDrawerWidth = useAppStore((s) => s.setFollowUpDrawerWidth)
  const reviseCurrentReport = useAppStore((s) => s.reviseCurrentReport)
  const reportRevisionLoading = useAppStore((s) => s.reportRevisionLoading)

  const [followUp, setFollowUp] = useState("")
  const draggingRef = useRef(false)

  if (!report || !previousRequest) return null

  async function handleSubmit() {
    const q = followUp.trim()
    if (!q || !previousRequest || !previousSummary) return
    setFollowUp("")
    setIsFollowUpLoading(true)
    try {
      const res = await fetch("/api/follow-up", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: q,
          context: {
            previous_request: previousRequest,
            previous_summary: previousSummary,
          },
        }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => null)
        throw new Error(err?.detail || "追问请求失败")
      }
      const data = await res.json()
      if (data.mode === "data_query") {
        appendFollowUp({
          question: q,
          answer: data.answer || "查询完成",
          mode: "data_query",
          columns: data.columns || [],
          rows: data.rows || [],
        })
      } else if (data.mode === "chart") {
        appendFollowUp({
          question: q,
          answer: data.answer || "图表已生成",
          mode: "chart",
          chart: data.chart || null,
          data_table: data.data_table || null,
        })
      } else {
        appendFollowUp({ question: q, answer: data.answer || "无法回答", mode: "follow_up" })
      }
      setFollowUpDrawerOpen(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : "追问请求失败")
    } finally {
      setIsFollowUpLoading(false)
    }
  }

  async function handleReviseReport() {
    const instruction = followUp.trim()
    if (!instruction) return
    const nextReport = await reviseCurrentReport(instruction)
    if (!nextReport) return
    appendFollowUp({
      question: instruction,
      answer: `已基于你的建议生成新的完整报告，当前内容已切换为“${nextReport.request.subject}分析报告”的建议润色版。`,
      mode: "follow_up",
    })
    setFollowUp("")
    setFollowUpDrawerOpen(true)
  }

  function startDrag(clientX: number) {
    draggingRef.current = true
    const onMove = (event: MouseEvent) => {
      if (!draggingRef.current) return
      setFollowUpDrawerWidth(window.innerWidth - event.clientX)
    }
    const onTouchMove = (event: TouchEvent) => {
      if (!draggingRef.current) return
      setFollowUpDrawerWidth(window.innerWidth - event.touches[0].clientX)
    }
    const onUp = () => {
      draggingRef.current = false
      window.removeEventListener("mousemove", onMove)
      window.removeEventListener("mouseup", onUp)
      window.removeEventListener("touchmove", onTouchMove)
      window.removeEventListener("touchend", onUp)
    }

    setFollowUpDrawerWidth(window.innerWidth - clientX)
    window.addEventListener("mousemove", onMove)
    window.addEventListener("mouseup", onUp)
    window.addEventListener("touchmove", onTouchMove)
    window.addEventListener("touchend", onUp)
  }

  const busy = isFollowUpLoading || reportRevisionLoading

  return (
    <>
      <button
        type="button"
        onClick={() => setFollowUpDrawerOpen(!followUpDrawerOpen)}
        data-testid="follow-up-toggle"
        className="fixed right-0 top-1/2 z-40 -translate-y-1/2 rounded-l-2xl border border-r-0 border-[rgba(56,189,248,0.16)] bg-slate-950/90 px-3 py-5 text-xs font-medium tracking-[0.14em] text-slate-300 shadow-[0_10px_30px_rgba(2,6,23,0.45)] backdrop-blur-xl"
      >
        <div className="flex items-center gap-2 [writing-mode:vertical-rl]">
          {followUpDrawerOpen ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
          追问
        </div>
      </button>

      <aside
        data-testid="follow-up-drawer"
        className={`fixed right-0 top-0 z-30 h-screen border-l border-[rgba(56,189,248,0.1)] bg-[rgba(8,12,24,0.96)] shadow-[-16px_0_40px_rgba(2,6,23,0.45)] backdrop-blur-xl transition-transform duration-300 ${
          followUpDrawerOpen ? "translate-x-0" : "translate-x-full"
        }`}
        style={{ width: `${followUpDrawerWidth}px`, maxWidth: "100vw" }}
      >
        <div
          className="absolute left-0 top-0 hidden h-full w-4 -translate-x-1/2 cursor-col-resize items-center justify-center md:flex"
          onMouseDown={(event) => startDrag(event.clientX)}
          onTouchStart={(event) => startDrag(event.touches[0].clientX)}
        >
          <div className="flex h-16 w-4 items-center justify-center rounded-full bg-slate-900/80 text-slate-500">
            <GripVertical className="h-4 w-4" />
          </div>
        </div>

        <div className="flex h-full flex-col">
          <div className="border-b border-[rgba(56,189,248,0.08)] px-6 py-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-sm font-semibold text-slate-100">{`继续追问 · 当前仅针对「${report.request.subject}分析报告」`}</div>
                <div className="mt-2 text-sm text-slate-400">{report.report.report_title}</div>
                <div className="mt-1 text-xs leading-6 text-slate-500">
                  {report.report.report_subtitle || "当前追问区只补充解释、图表、明细或基于建议生成新报告，不会发起新的底层分析。"}
                </div>
              </div>
              <Button variant="ghost" size="sm" className="rounded-xl text-slate-400" onClick={() => setFollowUpDrawerOpen(false)}>
                收起
              </Button>
            </div>
          </div>

          <div className="flex-1 space-y-4 overflow-y-auto px-5 py-5" data-testid="follow-up-messages">
            {followUpMessages.length === 0 ? (
              <div className="rounded-[24px] border border-[rgba(56,189,248,0.08)] bg-slate-900/60 p-5 text-sm leading-7 text-slate-400">
                报告已生成。你可以继续问明细、图表、结构原因，也可以直接输入修改意见，然后点击“生成新报告”得到一版不重跑数据的建议润色版。
              </div>
            ) : null}

            {followUpMessages.map((msg, i) => (
              <div key={i} className="space-y-3">
                <div className="flex justify-end">
                  <div className="max-w-[92%] rounded-[22px] bg-neon-cyan/10 px-4 py-3 text-sm leading-7 text-slate-100">{msg.question}</div>
                </div>
                <div className="flex justify-start">
                  <div className="max-w-[96%] rounded-[24px] border border-[rgba(56,189,248,0.08)] bg-slate-900/75 px-5 py-4 text-sm leading-relaxed text-slate-300">
                    <div className="space-y-3">
                      {formatFollowUpAnswer(msg.answer).map((paragraph, index) => (
                        <FollowUpParagraph key={`${i}-${index}`} text={paragraph} />
                      ))}
                    </div>
                    {msg.mode === "chart" && msg.chart ? <FollowUpChart chart={msg.chart} /> : null}
                    {msg.mode === "chart" && msg.data_table ? <FollowUpTable table={msg.data_table} /> : null}
                    {msg.mode === "data_query" && msg.columns && msg.rows ? (
                      <FollowUpTable table={{ table_title: "查询结果", columns: msg.columns, rows: msg.rows }} />
                    ) : null}
                  </div>
                </div>
              </div>
            ))}

            {busy ? (
              <div className="flex justify-start">
                <div className="flex items-center gap-2 rounded-2xl bg-slate-800/60 px-4 py-2.5 text-sm text-slate-400">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  {reportRevisionLoading ? "正在基于建议生成新的完整报告..." : "正在处理当前追问..."}
                </div>
              </div>
            ) : null}
          </div>

          <div className="border-t border-[rgba(56,189,248,0.08)] px-5 py-5">
            <div className="mb-3 flex flex-wrap gap-2">
              {followUpSuggestions.map((suggestion) => (
                <button
                  key={suggestion}
                  type="button"
                  onClick={() => setFollowUp(suggestion)}
                  className="rounded-full border border-[rgba(56,189,248,0.15)] bg-slate-900/60 px-3 py-1.5 text-xs text-slate-400 transition-all hover:border-neon-cyan/40 hover:text-neon-cyan"
                >
                  {suggestion}
                </button>
              ))}
            </div>
            <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto_auto]">
              <Input
                className="rounded-2xl border-[rgba(56,189,248,0.12)] bg-slate-900/60"
                data-testid="follow-up-input"
                placeholder="基于当前报告继续追问，或输入建议后一键生成新报告"
                value={followUp}
                onChange={(e) => setFollowUp(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault()
                    void handleSubmit()
                  }
                }}
              />
              <Button
                variant="outline"
                className="rounded-2xl px-4"
                onClick={() => void handleReviseReport()}
                disabled={busy || !followUp.trim()}
                data-testid="revise-report-button"
              >
                {reportRevisionLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Sparkles className="mr-2 h-4 w-4" />}
                生成新报告
              </Button>
              <Button className="rounded-2xl px-4" onClick={() => void handleSubmit()} disabled={busy || !followUp.trim()} data-testid="send-follow-up-button">
                {isFollowUpLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Send className="mr-2 h-4 w-4" />}
                发送追问
              </Button>
            </div>
          </div>
        </div>
      </aside>
    </>
  )
}
