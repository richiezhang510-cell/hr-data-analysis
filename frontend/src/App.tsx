import { useEffect } from "react"
import { AlertCircle, BookmarkCheck, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useAppStore } from "@/store"
import ChatLanding from "@/components/ChatLanding"
import SummaryTab from "@/components/SummaryTab"
import ReportTab from "@/components/ReportTab"
import ExportButton from "@/components/ExportButton"
import FollowUpInput from "@/components/FollowUpInput"
import HistoryPanel from "@/components/HistoryPanel"

function ViewSwitcher() {
  const appView = useAppStore((s) => s.appView)
  const setAppView = useAppStore((s) => s.setAppView)
  const report = useAppStore((s) => s.report)

  return (
    <div className="mb-6 flex items-center justify-center">
      <div className="inline-flex rounded-full border border-[rgba(56,189,248,0.12)] bg-slate-950/70 p-1 shadow-[0_12px_40px_rgba(2,6,23,0.35)] backdrop-blur-xl">
        <button
          type="button"
          onClick={() => setAppView("chat")}
          data-testid="switch-chat-view"
          className={`rounded-full px-4 py-2 text-sm transition-colors ${
            appView === "chat" ? "bg-neon-cyan/15 text-slate-100" : "text-slate-400 hover:text-slate-200"
          }`}
        >
          薪酬chatbot
        </button>
        <button
          type="button"
          onClick={() => report && setAppView("report")}
          disabled={!report}
          data-testid="switch-report-view"
          className={`rounded-full px-4 py-2 text-sm transition-colors ${
            appView === "report"
              ? "bg-neon-cyan/15 text-slate-100"
              : report
                ? "text-slate-400 hover:text-slate-200"
                : "cursor-not-allowed text-slate-600"
          }`}
        >
          当前报告
        </button>
      </div>
    </div>
  )
}

function ReportWorkspace() {
  const report = useAppStore((s) => s.report)
  const metadata = useAppStore((s) => s.metadata)
  const error = useAppStore((s) => s.error)
  const resultTab = useAppStore((s) => s.resultTab)
  const setResultTab = useAppStore((s) => s.setResultTab)
  const setAppView = useAppStore((s) => s.setAppView)
  const saveCurrentReport = useAppStore((s) => s.saveCurrentReport)
  const saveReportLoading = useAppStore((s) => s.saveReportLoading)
  const currentSavedReportId = useAppStore((s) => s.currentSavedReportId)
  const reportDataSource = report?.report.methodology?.data_source || metadata.data_source.filename
  const reportImportedAt = report?.report.methodology?.data_source_imported_at

  return (
    <div className="min-h-screen px-4 py-8 md:px-6 lg:px-8">
      <div className="mx-auto max-w-7xl">
        <ViewSwitcher />
        <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-start gap-4">
            <div>
              <h1 className="text-3xl font-semibold tracking-tight text-slate-100">
                <span data-testid="report-workspace-title">
                {report ? `${report.request.subject}分析报告` : "分析报告"}
                </span>
              </h1>
              <p className="mt-2 text-sm text-slate-400" data-testid="report-workspace-subtitle">
                {report?.report.report_subtitle || `${metadata.period_start} - ${metadata.period_end}`}
              </p>
              {reportDataSource ? (
                <p className="mt-2 text-xs text-slate-500" data-testid="report-data-source">
                  数据源：{reportDataSource}
                  {reportImportedAt ? ` · 导入于 ${new Date(reportImportedAt).toLocaleString("zh-CN", { hour12: false })}` : ""}
                </p>
              ) : null}
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Button
              variant={currentSavedReportId ? "secondary" : "outline"}
              className="rounded-2xl"
              onClick={() => void saveCurrentReport()}
              disabled={!report || saveReportLoading}
              data-testid="save-report-button"
            >
              {saveReportLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <BookmarkCheck className="mr-2 h-4 w-4" />}
              {currentSavedReportId ? "已保存" : "保存报告"}
            </Button>
            <Button variant="outline" className="rounded-2xl" onClick={() => setAppView("chat")} data-testid="new-question-button">
              新问题
            </Button>
            {resultTab === "report" ? <ExportButton /> : null}
          </div>
        </div>

        {error ? (
          <Card className="mb-6 rounded-[28px] border-red-500/30 bg-red-500/10">
            <CardContent className="flex items-start gap-3 p-5 text-sm text-red-400">
              <AlertCircle className="mt-0.5 h-4 w-4" />
              <div>{error}</div>
            </CardContent>
          </Card>
        ) : null}

        <Tabs value={resultTab} onValueChange={setResultTab} className="w-full">
          <TabsList className="h-auto w-full justify-start gap-2 rounded-[24px] p-2">
            <TabsTrigger value="summary" className="rounded-2xl" data-testid="tab-summary">
              Answer Summary
            </TabsTrigger>
            <TabsTrigger value="report" className="rounded-2xl" data-testid="tab-report">
              完整正文
            </TabsTrigger>
            <TabsTrigger value="history" className="rounded-2xl" data-testid="tab-history">
              历史记录
            </TabsTrigger>
          </TabsList>

          <TabsContent value="summary" className="mt-6">
            <SummaryTab />
          </TabsContent>
          <TabsContent value="report" className="mt-6">
            <ReportTab />
          </TabsContent>
          <TabsContent value="history" className="mt-6">
            <HistoryPanel />
          </TabsContent>
        </Tabs>
      </div>
      <FollowUpInput />
    </div>
  )
}

export default function App() {
  const fetchMetadata = useAppStore((s) => s.fetchMetadata)
  const appView = useAppStore((s) => s.appView)

  useEffect(() => {
    void fetchMetadata()
  }, [fetchMetadata])

  return appView === "report" ? (
    <ReportWorkspace />
  ) : (
    <div className="min-h-screen px-4 py-8 md:px-6 lg:px-8">
      <div className="mx-auto max-w-6xl">
        <ViewSwitcher />
      </div>
      <ChatLanding />
    </div>
  )
}
