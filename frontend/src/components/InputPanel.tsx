import { Loader2, Sparkle, Sparkles } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { useAppStore, promptSuggestions } from "@/store"
import { StreamingProgress } from "@/components/StreamingProgress"

export default function InputPanel() {
  const question = useAppStore((s) => s.question)
  const setQuestion = useAppStore((s) => s.setQuestion)
  const isLoading = useAppStore((s) => s.isLoading)
  const isStreaming = useAppStore((s) => s.isStreaming)
  const submitReport = useAppStore((s) => s.submitReportStream)
  const metadata = useAppStore((s) => s.metadata)
  const report = useAppStore((s) => s.report)
  const dataSourceReady = metadata.data_source.ready

  const recognizedRequest = report?.request
  const activeMetrics = recognizedRequest?.metrics || []
  const activeDimensions = recognizedRequest?.secondary_dimensions || []

  return (
    <div className="space-y-6">
      <div className="rounded-[32px] border border-[rgba(56,189,248,0.1)] bg-slate-900/60 p-4 md:p-5">
        <div className="flex items-start gap-3">
          <div className="mt-1 flex h-11 w-11 items-center justify-center rounded-2xl bg-gradient-to-br from-neon-cyan to-sky-500 text-slate-950">
            <Sparkle className="h-5 w-5" />
          </div>
          <div className="flex-1 space-y-4">
            <Textarea
              className="min-h-[150px] rounded-[24px] border-0 bg-slate-800/80 text-[15px] leading-7"
              data-testid="analysis-question-input"
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
              placeholder="输入你想问的薪酬问题"
            />
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex flex-wrap gap-2" data-testid="analysis-context-badges">
                <Badge variant="secondary" className="rounded-full px-3 py-1" data-testid="recognized-subject-badge">
                  {recognizedRequest?.subject || (dataSourceReady ? "待确认科目" : "请先导入真实数据")}
                </Badge>
                <Badge variant="secondary" className="rounded-full px-3 py-1" data-testid="primary-dimension-badge">{metadata.primary_dimension}</Badge>
                {activeDimensions.slice(0, 3).map((dimension) => (
                  <Badge key={dimension} variant="secondary" className="rounded-full px-3 py-1" data-testid="secondary-dimension-badge">
                    {dimension}
                  </Badge>
                ))}
              </div>
              <Button className="rounded-2xl px-6" onClick={() => void submitReport()} disabled={isLoading || !dataSourceReady} data-testid="analyze-button">
                {isLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Sparkles className="mr-2 h-4 w-4" />}
                分析
              </Button>
            </div>
            {isStreaming && <StreamingProgress />}
          </div>
        </div>
      </div>

      <div className="flex flex-wrap gap-3" data-testid="prompt-suggestions">
        {promptSuggestions.map((prompt) => (
          <button
            key={prompt}
            type="button"
            onClick={() => setQuestion(prompt)}
            data-testid="prompt-suggestion"
            className="rounded-full border border-[rgba(56,189,248,0.15)] bg-slate-900/60 px-4 py-2 text-left text-sm text-slate-400 transition-all hover:border-neon-cyan/40 hover:bg-[rgba(56,189,248,0.1)] hover:text-neon-cyan hover:shadow-glow"
          >
            {prompt}
          </button>
        ))}
      </div>
    </div>
  )
}
