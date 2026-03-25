import { useEffect, useState } from "react"
import { BookmarkCheck, Clock, Eye, RotateCw, Sparkles, Trash2 } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useAppStore } from "@/store"
import type { HistoryEntry, SavedReportSummary } from "@/types"

function renderDataSourceLabel(value?: string) {
  return value?.trim() || "未标记数据源"
}

export default function HistoryPanel() {
  const setQuestion = useAppStore((s) => s.setQuestion)
  const submitReportStream = useAppStore((s) => s.submitReportStream)
  const selectedHistoryTab = useAppStore((s) => s.selectedHistoryTab)
  const setSelectedHistoryTab = useAppStore((s) => s.setSelectedHistoryTab)
  const savedReports = useAppStore((s) => s.savedReports)
  const savedReportsLoading = useAppStore((s) => s.savedReportsLoading)
  const fetchSavedReports = useAppStore((s) => s.fetchSavedReports)
  const openSavedReport = useAppStore((s) => s.openSavedReport)
  const currentSavedReportId = useAppStore((s) => s.currentSavedReportId)

  const [items, setItems] = useState<HistoryEntry[]>([])
  const [loading, setLoading] = useState(true)

  function fetchHistory() {
    setLoading(true)
    fetch("/api/history")
      .then((res) => res.json())
      .then((data: HistoryEntry[]) => setItems(data))
      .catch(() => setItems([]))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    fetchHistory()
    void fetchSavedReports()
  }, [fetchSavedReports])

  function handleReplay(item: HistoryEntry) {
    setQuestion(item.question)
    try {
      const req = JSON.parse(item.request_json) as {
        subject?: string
        secondary_dimensions?: string[]
        metrics?: string[]
      }
      void submitReportStream({
        subject: req.subject,
        secondary_dimensions: req.secondary_dimensions,
        metrics: req.metrics,
      })
    } catch {
      void submitReportStream()
    }
  }

  function handleDelete(id: number) {
    fetch(`/api/history/${id}`, { method: "DELETE" })
      .then(() => setItems((prev) => prev.filter((i) => i.id !== id)))
      .catch(() => {})
  }

  function renderSavedReports() {
    if (savedReportsLoading) {
      return <div className="py-12 text-center text-sm text-slate-500">正在读取已保存报告...</div>
    }
    if (savedReports.length === 0) {
      return <div className="py-12 text-center text-sm text-slate-500">还没有保存的报告，先在报告页点击“保存报告”。</div>
    }

    return (
      <div className="space-y-3">
        {savedReports.map((item: SavedReportSummary) => {
          const isCurrent = currentSavedReportId === item.id
          return (
            <div
              key={item.id}
              className={`flex items-center justify-between gap-4 rounded-[18px] border px-5 py-4 transition-colors ${
                isCurrent
                  ? "border-neon-cyan/30 bg-neon-cyan/8"
                  : "border-[rgba(56,189,248,0.1)] bg-slate-800/40 hover:bg-slate-800/60"
              }`}
            >
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <div className="truncate text-sm font-medium text-slate-100">{item.title}</div>
                  <Badge variant="secondary" className="rounded-full px-2 py-0 text-xs">
                    {item.subject}
                  </Badge>
                  <Badge variant="outline" className="rounded-full px-2 py-0 text-xs">
                    {item.source_type === "revised" ? "润色版" : "手动保存"}
                  </Badge>
                </div>
                <div className="mt-2 truncate text-sm text-slate-400">{item.question || "未填写原始问题"}</div>
                <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-500">
                  <span>{item.created_at}</span>
                  <span>数据源：{renderDataSourceLabel(item.data_source_name)}</span>
                  {item.revision_instruction ? <span className="truncate">建议：{item.revision_instruction}</span> : null}
                </div>
              </div>
              <div className="flex shrink-0 gap-2">
                <Button variant="ghost" size="sm" className="rounded-xl text-xs" onClick={() => void openSavedReport(item.id)}>
                  <Eye className="mr-1 h-3.5 w-3.5" />
                  查看
                </Button>
              </div>
            </div>
          )
        })}
      </div>
    )
  }

  function renderQueryHistory() {
    if (loading) {
      return <div className="py-12 text-center text-sm text-slate-500">加载中...</div>
    }
    if (items.length === 0) {
      return <div className="py-12 text-center text-sm text-slate-500">暂无查询历史</div>
    }
    return (
      <div className="space-y-2">
        {items.map((item) => (
          <div
            key={item.id}
            className="flex items-center justify-between gap-4 rounded-[16px] border border-[rgba(56,189,248,0.1)] bg-slate-800/40 px-5 py-3 transition-colors hover:bg-slate-800/60"
          >
            <div className="flex items-center gap-3 overflow-hidden">
              <Clock className="h-4 w-4 shrink-0 text-slate-500" />
              <div className="min-w-0">
                <div className="truncate text-sm text-slate-200">{item.question || "(无问题文本)"}</div>
                <div className="mt-1 flex flex-wrap gap-2 text-xs text-slate-500">
                  <span>{item.created_at}</span>
                  <span>数据源：{renderDataSourceLabel(item.data_source_name)}</span>
                </div>
              </div>
            </div>
            <div className="flex shrink-0 gap-2">
              <Button variant="ghost" size="sm" className="rounded-xl text-xs" onClick={() => handleReplay(item)}>
                重新分析
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="rounded-xl text-xs text-slate-500 hover:text-red-400"
                onClick={() => handleDelete(item.id)}
              >
                <Trash2 className="h-3 w-3" />
              </Button>
            </div>
          </div>
        ))}
      </div>
    )
  }

  return (
    <Card className="rounded-[32px]">
      <CardHeader className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="space-y-1">
          <CardTitle className="text-2xl">历史记录</CardTitle>
          <p className="text-sm text-slate-500">保存可直接查看快照，查询历史则继续保留“重新分析”的工作流。</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" className="rounded-2xl" onClick={() => void fetchSavedReports()}>
            <BookmarkCheck className="mr-2 h-4 w-4" />
            刷新已保存
          </Button>
          <Button variant="outline" className="rounded-2xl" onClick={fetchHistory}>
            <RotateCw className="mr-2 h-4 w-4" />
            刷新历史
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <Tabs value={selectedHistoryTab} onValueChange={(value) => setSelectedHistoryTab(value as "saved" | "queries")}>
          <TabsList className="mb-6 grid w-full grid-cols-2 rounded-[22px]">
            <TabsTrigger value="saved" className="rounded-2xl" data-testid="history-tab-saved">
              已保存报告
            </TabsTrigger>
            <TabsTrigger value="queries" className="rounded-2xl" data-testid="history-tab-queries">
              查询历史
            </TabsTrigger>
          </TabsList>
        </Tabs>

        <div className="rounded-[24px] border border-[rgba(56,189,248,0.08)] bg-slate-900/35 p-4" data-testid="history-panel-content">
          {selectedHistoryTab === "saved" ? renderSavedReports() : renderQueryHistory()}
        </div>

        <div className="mt-4 flex items-start gap-2 rounded-[18px] border border-dashed border-[rgba(56,189,248,0.12)] bg-slate-900/20 px-4 py-3 text-xs leading-6 text-slate-500">
          <Sparkles className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          已保存报告会直接恢复当时那份完整快照；查询历史仍然会重新走分析链路，适合复跑最新结果。
        </div>
      </CardContent>
    </Card>
  )
}
