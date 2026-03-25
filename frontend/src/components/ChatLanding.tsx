import { BookOpenText, BrainCircuit, CalendarRange, CheckCircle2, Database, Loader2, MessageSquareText, Sparkles, Upload } from "lucide-react"
import { useMemo, useRef, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useAppStore, promptSuggestions, DEFAULT_METRICS, DEFAULT_SECONDARY_DIMENSIONS } from "@/store"
import type {
  AnalysisStep,
  InferredColumn,
  InferredSchemaDraft,
  KnowledgeBaseItem,
  MetadataResponse,
  ReportTemplateOption,
  UploadActivationResponse,
  UploadInferenceResponse,
} from "@/types"

function buildPeriods(start: string, end: string) {
  const matchStart = start.match(/^(\d{4})-(\d{2})$/)
  const matchEnd = end.match(/^(\d{4})-(\d{2})$/)
  if (!matchStart || !matchEnd) return []

  let year = Number(matchStart[1])
  let month = Number(matchStart[2])
  const endYear = Number(matchEnd[1])
  const endMonth = Number(matchEnd[2])
  const periods: string[] = []

  while (year < endYear || (year === endYear && month <= endMonth)) {
    periods.push(`${year}-${String(month).padStart(2, "0")}`)
    month += 1
    if (month > 12) {
      month = 1
      year += 1
    }
  }

  return periods
}

function formatImportedAt(value: string) {
  if (!value) return "未导入"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString("zh-CN", { hour12: false })
}

type EditableInferredColumn = InferredColumn & {
  current_type: string
  current_name: string
}

function buildEditableDraft(draft: InferredSchemaDraft): EditableInferredColumn[] {
  return draft.columns.map((column) => ({
    ...column,
    current_type: column.detected_type,
    current_name: column.canonical_name,
  }))
}

function buildManifestFromEditableColumns(
  draft: InferredSchemaDraft,
  columns: EditableInferredColumn[],
): InferredSchemaDraft {
  const yearColumn = columns.find((column) => column.current_type === "period_year")?.name || draft.period.year_column || ""
  const monthColumn = columns.find((column) => column.current_type === "period_month")?.name || draft.period.month_column || ""
  const periodColumn = columns.find((column) => column.current_type === "period")?.name || draft.period.period_column || ""
  const periodMode = periodColumn && !(yearColumn && monthColumn) ? "single_period" : "year_month"

  const dimensions = columns.filter((column) => column.current_type === "dimension").map((column) => column.current_name.trim()).filter(Boolean)
  const subjects = columns.filter((column) => column.current_type === "subject").map((column) => column.current_name.trim()).filter(Boolean)
  const ignored = columns.filter((column) => column.current_type === "ignored").map((column) => column.name)

  const sourceColumnMap: Record<string, string> = {}
  const dimensionAliases: Record<string, string> = {}
  const subjectAliases: Record<string, string> = {}
  const syntheticDefaults = { ...(draft.synthetic_defaults || {}) }

  columns.forEach((column) => {
    const nextName = column.current_name.trim()
    if (!nextName || !["dimension", "subject"].includes(column.current_type)) return
    sourceColumnMap[nextName] = column.name
    if (column.current_type === "dimension" && nextName !== column.name) {
      dimensionAliases[column.name] = nextName
    }
    if (column.current_type === "subject" && nextName !== column.name) {
      subjectAliases[column.name] = nextName
    }
  })

  const supportDimensions = ["统计月", "员工ID", "BU", "职能", "绩效分位", "级别", "司龄分箱", "年龄分箱"]
  sourceColumnMap["统计月"] = "__period__"
  if (!sourceColumnMap["员工ID"]) {
    sourceColumnMap["员工ID"] = "__rowid__"
    syntheticDefaults["员工ID"] = ""
  }
  if (!sourceColumnMap["BU"]) {
    const primaryCandidate = dimensions.find((item) => item !== "员工ID" && item !== "统计月")
    if (primaryCandidate) {
      sourceColumnMap["BU"] = sourceColumnMap[primaryCandidate] || primaryCandidate
    } else {
      sourceColumnMap["BU"] = "__constant__"
      syntheticDefaults["BU"] = "全部BU"
    }
  }
  ;["职能", "绩效分位", "级别", "司龄分箱", "年龄分箱"].forEach((name) => {
    if (!sourceColumnMap[name]) {
      sourceColumnMap[name] = "__constant__"
      syntheticDefaults[name] = "未提供"
    }
  })

  const textDimensionColumns = Array.from(new Set([...supportDimensions, ...dimensions.filter((item) => !supportDimensions.includes(item))]))
  const displayDimensionColumns = Array.from(new Set(dimensions.filter((item) => !["统计月", "员工ID"].includes(item))))

  return {
    ...draft,
    period_mode: periodMode,
    period: {
      year_column: yearColumn,
      month_column: monthColumn,
      period_column: periodColumn,
    },
    text_dimension_columns: textDimensionColumns,
    dimension_columns: Array.from(new Set(["BU", ...displayDimensionColumns])),
    display_dimension_columns: displayDimensionColumns,
    subject_columns: Array.from(new Set(subjects)),
    default_subject: subjects[0] || draft.default_subject,
    default_secondary_dimensions: displayDimensionColumns.filter((item) => item !== "BU").slice(0, 4),
    source_column_map: sourceColumnMap,
    dimension_aliases: dimensionAliases,
    subject_aliases: subjectAliases,
    synthetic_defaults: syntheticDefaults,
    columns: columns.map((column) => ({
      ...column,
      detected_type: column.current_type,
      canonical_name: column.current_name.trim(),
    })),
    ignored_columns: ignored,
  }
}

function ChatBubble({ role, content }: { role: "user" | "assistant" | "status"; content: string }) {
  if (role === "status") {
    return (
      <div className="flex justify-center">
        <div className="inline-flex items-center gap-2 rounded-full border border-[rgba(56,189,248,0.12)] bg-slate-900/70 px-4 py-2 text-xs text-slate-400">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          {content}
        </div>
      </div>
    )
  }

  const isUser = role === "user"
  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
      <div
        className={`max-w-[85%] rounded-[24px] px-5 py-4 text-sm leading-7 ${
          isUser ? "bg-neon-cyan/12 text-slate-100" : "border border-[rgba(56,189,248,0.08)] bg-slate-900/70 text-slate-300"
        }`}
      >
        {content}
      </div>
    </div>
  )
}

function StepBadge({ step }: { step: AnalysisStep }) {
  const stateClass =
    step.status === "completed"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-200"
      : "border-sky-400/20 bg-sky-400/10 text-sky-100"

  return (
    <div className={`rounded-[22px] border px-4 py-3 ${stateClass}`}>
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.18em] text-slate-400">{`Step ${step.step_index}/${step.step_total}`}</div>
          <div className="mt-1 text-sm font-semibold">{step.label}</div>
        </div>
        {step.status === "completed" ? <CheckCircle2 className="h-4 w-4" /> : <Loader2 className="h-4 w-4 animate-spin" />}
      </div>
      <p className="mt-2 text-sm leading-6 text-slate-300">{step.message}</p>
    </div>
  )
}

function ThinkingPanel({ steps }: { steps: AnalysisStep[] }) {
  if (steps.length === 0) return null

  return (
    <div className="mb-5 rounded-[28px] border border-[rgba(56,189,248,0.12)] bg-slate-950/50 p-4" data-testid="analysis-thinking-panel">
      <div className="mb-4 flex items-center gap-3">
        <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-neon-cyan/12 text-neon-cyan">
          <BrainCircuit className="h-4 w-4" />
        </div>
        <div className="text-sm font-semibold text-slate-100" data-testid="analysis-thinking-title">分析思考过程</div>
      </div>
      <div className="space-y-3">
        {steps.map((step) => (
          <StepBadge key={`${step.stage}-${step.step_index}`} step={step} />
        ))}
      </div>
    </div>
  )
}

function KnowledgeBasePanel({ items }: { items: KnowledgeBaseItem[] }) {
  return (
    <div className="rounded-[28px] border border-[rgba(56,189,248,0.12)] bg-slate-900/55 p-5">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
            <Database className="h-4 w-4 text-neon-cyan" />
            专属知识库
          </div>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            这里先保留正式模式下的接入位。后续可以把真实历史报告、制度材料和专项分析结果沉淀进来，作为写报告参考。
          </p>
        </div>
        <Badge variant="secondary" className="rounded-full border border-[rgba(56,189,248,0.12)] bg-slate-950/70 text-slate-300">
          即将支持
        </Badge>
      </div>
      <div className="mt-4 space-y-3">
        {items.map((item) => (
          <div key={item.id} className="rounded-[22px] border border-[rgba(56,189,248,0.08)] bg-slate-950/60 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-sm font-medium text-slate-100">{item.title}</div>
              <span className="rounded-full border border-[rgba(56,189,248,0.1)] px-2 py-1 text-[11px] text-slate-400">{item.status}</span>
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">{item.description}</p>
            <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
              <span>{item.updated_at}</span>
              <button type="button" className="rounded-full border border-dashed border-[rgba(56,189,248,0.2)] px-3 py-1 text-slate-400">
                上传报告
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function TemplatePanel({
  options,
  selectedTemplateId,
  onSelect,
}: {
  options: ReportTemplateOption[]
  selectedTemplateId: string
  onSelect: (templateId: string) => void
}) {
  return (
    <div className="rounded-[28px] border border-[rgba(56,189,248,0.12)] bg-slate-900/55 p-5">
      <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
        <BookOpenText className="h-4 w-4 text-neon-cyan" />
        选择模版
      </div>
      <p className="mt-2 text-sm leading-6 text-slate-400">当前只影响首页展示和本地状态，不会改变后端分析逻辑。</p>
      <div className="mt-4 grid gap-3">
        {options.map((option) => {
          const selected = option.id === selectedTemplateId
          return (
            <button
              key={option.id}
              type="button"
              onClick={() => onSelect(option.id)}
              className={`rounded-[22px] border p-4 text-left transition-all ${
                selected
                  ? "border-neon-cyan/40 bg-neon-cyan/10 shadow-[0_12px_32px_rgba(14,165,233,0.12)]"
                  : "border-[rgba(56,189,248,0.08)] bg-slate-950/55 hover:border-[rgba(56,189,248,0.2)]"
              }`}
            >
              <div className={`h-1.5 w-16 rounded-full bg-gradient-to-r ${option.accent}`} />
              <div className="mt-3 flex items-center justify-between gap-3">
                <div className="text-sm font-medium text-slate-100">{option.name}</div>
                {selected ? <Badge className="rounded-full">当前选择</Badge> : null}
              </div>
              <p className="mt-2 text-sm leading-6 text-slate-400">{option.description}</p>
            </button>
          )
        })}
      </div>
    </div>
  )
}

export default function ChatLanding() {
  const setQuestion = useAppStore((s) => s.setQuestion)
  const setError = useAppStore((s) => s.setError)
  const metadata = useAppStore((s) => s.metadata)
  const fetchMetadata = useAppStore((s) => s.fetchMetadata)
  const resetForActivatedDataSource = useAppStore((s) => s.resetForActivatedDataSource)
  const landingMessages = useAppStore((s) => s.landingMessages)
  const clarification = useAppStore((s) => s.clarification)
  const selectedSubject = useAppStore((s) => s.selectedSubject)
  const setSelectedSubject = useAppStore((s) => s.setSelectedSubject)
  const selectedStartPeriod = useAppStore((s) => s.selectedStartPeriod)
  const setSelectedStartPeriod = useAppStore((s) => s.setSelectedStartPeriod)
  const selectedEndPeriod = useAppStore((s) => s.selectedEndPeriod)
  const setSelectedEndPeriod = useAppStore((s) => s.setSelectedEndPeriod)
  const beginChatQuestion = useAppStore((s) => s.beginChatQuestion)
  const acknowledgeSubjectSelection = useAppStore((s) => s.acknowledgeSubjectSelection)
  const submitReportStream = useAppStore((s) => s.submitReportStream)
  const isLoading = useAppStore((s) => s.isLoading)
  const error = useAppStore((s) => s.error)
  const analysisSteps = useAppStore((s) => s.analysisSteps)
  const knowledgeBaseItems = useAppStore((s) => s.knowledgeBaseItems)
  const selectedTemplateId = useAppStore((s) => s.selectedTemplateId)
  const setSelectedTemplateId = useAppStore((s) => s.setSelectedTemplateId)
  const reportTemplateOptions = useAppStore((s) => s.reportTemplateOptions)

  const [draft, setDraft] = useState("")
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [isUploading, setIsUploading] = useState(false)
  const [inferencePath, setInferencePath] = useState("")
  const [inferenceDraft, setInferenceDraft] = useState<InferredSchemaDraft | null>(null)
  const [editableColumns, setEditableColumns] = useState<EditableInferredColumn[]>([])
  const [isActivatingInference, setIsActivatingInference] = useState(false)
  const [libraryPanelOpen, setLibraryPanelOpen] = useState(false)
  const [libraryPanelTab, setLibraryPanelTab] = useState<"knowledge" | "template">("knowledge")
  const fileInputRef = useRef<HTMLInputElement | null>(null)

  const needsSubjectOnly = useMemo(() => {
    if (!clarification) return false
    return (
      clarification.clarification.needs_subject &&
      !clarification.clarification.needs_dimensions &&
      !clarification.clarification.needs_metrics
    )
  }, [clarification])

  const periodOptions = useMemo(() => buildPeriods(metadata.period_start, metadata.period_end), [metadata.period_end, metadata.period_start])
  const dataSourceReady = metadata.data_source.ready

  const visibleStartPeriod = selectedStartPeriod || metadata.period_start
  const visibleEndPeriod = selectedEndPeriod || metadata.period_end

  async function handleSubmit(nextQuestion?: string) {
    const q = (nextQuestion ?? draft).trim()
    if (!q || isLoading) return
    if (!dataSourceReady) {
      setError(metadata.data_source.message || "请先导入真实数据后再开始分析。")
      return
    }
    if (visibleStartPeriod > visibleEndPeriod) return
    beginChatQuestion(q)
    setQuestion(q)
    setDraft("")
    await submitReportStream({
      secondary_dimensions: DEFAULT_SECONDARY_DIMENSIONS,
      metrics: DEFAULT_METRICS,
    })
  }

  async function handleSubjectConfirm(subject: string) {
    if (!dataSourceReady) return
    setSelectedSubject(subject)
    acknowledgeSubjectSelection(subject)
    await submitReportStream({
      subject,
      secondary_dimensions: DEFAULT_SECONDARY_DIMENSIONS,
      metrics: DEFAULT_METRICS,
    })
  }

  function openLibraryPanel(tab: "knowledge" | "template") {
    if (libraryPanelOpen && libraryPanelTab === tab) {
      setLibraryPanelOpen(false)
      return
    }
    setLibraryPanelTab(tab)
    setLibraryPanelOpen(true)
  }

  async function handleUpload() {
    if (!selectedFile || isUploading) return
    setIsUploading(true)
    setError("")

    try {
      const formData = new FormData()
      formData.append("file", selectedFile)

      const response = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      })
      const payload = (await response.json().catch(() => null)) as UploadActivationResponse | UploadInferenceResponse | null
      if (!response.ok) {
        throw new Error((payload as { detail?: string } | null)?.detail || "上传失败")
      }

      if (payload?.mode === "inference_required") {
        setInferenceDraft(payload.draft)
        setEditableColumns(buildEditableDraft(payload.draft))
        setInferencePath(payload.path)
        return
      }

      const metaResponse = await fetch("/api/metadata")
      if (!metaResponse.ok) {
        throw new Error("数据源激活成功，但刷新元数据失败")
      }
      const nextMeta: MetadataResponse = await metaResponse.json()
      resetForActivatedDataSource(nextMeta)
      setDraft("")
      setSelectedFile(null)
      setInferenceDraft(null)
      setEditableColumns([])
      setInferencePath("")
      if (fileInputRef.current) {
        fileInputRef.current.value = ""
      }
      await fetchMetadata()
    } catch (uploadError) {
      setError(uploadError instanceof Error ? uploadError.message : "上传失败")
    } finally {
      setIsUploading(false)
    }
  }

  async function handleActivateInferred() {
    if (!inferenceDraft || !inferencePath || isActivatingInference) return
    const normalizedDraft = buildManifestFromEditableColumns(inferenceDraft, editableColumns)
    if (!normalizedDraft.subject_columns.length) {
      setError("至少需要保留 1 个金额科目才能导入。")
      return
    }
    const hasPeriod = !!normalizedDraft.period.period_column || (!!normalizedDraft.period.year_column && !!normalizedDraft.period.month_column)
    if (!hasPeriod) {
      setError("请先确认时间字段，至少要有单列期间或 年+月。")
      return
    }
    setIsActivatingInference(true)
    setError("")
    try {
      const response = await fetch("/api/data-source/activate-inferred", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          path: inferencePath,
          manifest: normalizedDraft,
        }),
      })
      const payload = await response.json().catch(() => null)
      if (!response.ok) {
        throw new Error(payload?.detail || "激活失败")
      }
      const metaResponse = await fetch("/api/metadata")
      if (!metaResponse.ok) {
        throw new Error("数据源激活成功，但刷新元数据失败")
      }
      const nextMeta: MetadataResponse = await metaResponse.json()
      resetForActivatedDataSource(nextMeta)
      setDraft("")
      setSelectedFile(null)
      setInferenceDraft(null)
      setEditableColumns([])
      setInferencePath("")
      if (fileInputRef.current) {
        fileInputRef.current.value = ""
      }
      await fetchMetadata()
    } catch (activationError) {
      setError(activationError instanceof Error ? activationError.message : "激活失败")
    } finally {
      setIsActivatingInference(false)
    }
  }

  return (
    <div className="mx-auto flex min-h-[calc(100vh-120px)] max-w-[1500px] flex-col">
      <div className="mb-8 flex items-center justify-between">
        <div>
          <Badge variant="default" className="rounded-full px-4 py-1 text-[11px] uppercase tracking-[0.2em]">
            Comp Insight Studio
          </Badge>
          <h1 className="mt-4 text-3xl font-semibold tracking-tight text-slate-100 md:text-4xl">
            薪酬chatbot
          </h1>
          <p className="mt-2 max-w-2xl text-sm leading-7 text-slate-400">
            默认运行在真实数据模式。先激活一份兼容当前宽表结构的 CSV，再围绕当前活动数据源生成正式分析报告。
          </p>
        </div>
        <div className="hidden min-w-[340px] rounded-[28px] border border-[rgba(56,189,248,0.12)] bg-slate-900/60 px-5 py-4 md:block">
          {dataSourceReady ? (
            <>
              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-slate-500">
                <CalendarRange className="h-3.5 w-3.5" />
                分析窗口
              </div>
              <div className="mt-3 grid grid-cols-[1fr_auto_1fr] items-center gap-3">
                <Select value={visibleStartPeriod} onValueChange={setSelectedStartPeriod}>
                  <SelectTrigger className="h-11 rounded-2xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100">
                    <SelectValue placeholder="开始时间" />
                  </SelectTrigger>
                  <SelectContent>
                    {periodOptions.map((period) => (
                      <SelectItem key={period} value={period}>
                        {period}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <span className="text-xs uppercase tracking-[0.16em] text-slate-500">to</span>
                <Select value={visibleEndPeriod} onValueChange={setSelectedEndPeriod}>
                  <SelectTrigger className="h-11 rounded-2xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100">
                    <SelectValue placeholder="结束时间" />
                  </SelectTrigger>
                  <SelectContent>
                    {periodOptions.map((period) => (
                      <SelectItem key={period} value={period}>
                        {period}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <p className="mt-3 text-xs text-slate-500">分析会严格使用当前活动数据源的时间范围。</p>
            </>
          ) : (
            <>
              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-slate-500">
                <Database className="h-3.5 w-3.5" />
                待激活
              </div>
              <div className="mt-3 text-sm leading-7 text-slate-300">
                当前暂无活动数据源。上传真实 CSV 后，系统会自动校验字段、导入 SQLite 并刷新可分析时间范围。
              </div>
            </>
          )}
        </div>
      </div>

      <div className="mb-6 rounded-[30px] border border-[rgba(56,189,248,0.12)] bg-slate-900/60 p-5">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" className="rounded-full border border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-300">
                {dataSourceReady ? "真实数据模式" : "待导入真实数据"}
              </Badge>
              {dataSourceReady ? (
                <Badge variant="outline" className="rounded-full border-emerald-400/20 text-emerald-300">
                  已激活
                </Badge>
              ) : null}
            </div>
            <div className="mt-3 text-lg font-semibold text-slate-100">
              {dataSourceReady ? metadata.data_source.filename || "当前活动数据源" : "请先上传一份真实宽表 CSV"}
            </div>
            <p className="mt-2 max-w-3xl text-sm leading-7 text-slate-400">
              {dataSourceReady
                ? "上传新文件后会立即完成字段校验、SQLite 导入和数据源切换，后续报告、追问和监控都会基于这份数据。"
                : "系统不会再默认加载演示样例。只有当真实 CSV 校验通过并被激活后，分析入口才会开放。"}
            </p>
            <div className="mt-3 rounded-[18px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 px-4 py-3 text-xs leading-6 text-slate-400">
              小文件可以直接上传；大文件更推荐保留在本地任意稳定目录，再通过本地路径激活。浏览器上传后的文件会保存到项目目录下的
              {" "}
              <span className="font-medium text-slate-200">uploads/</span>
              ，但正式数据并不要求你手工先放到这里。
            </div>
            <div className="mt-4 flex flex-wrap gap-3 text-xs text-slate-500">
              <span>记录数：{dataSourceReady ? metadata.data_source.row_count.toLocaleString("zh-CN") : "--"}</span>
              <span>时间范围：{dataSourceReady ? `${metadata.data_source.period_start} 至 ${metadata.data_source.period_end}` : "--"}</span>
              <span>导入时间：{dataSourceReady ? formatImportedAt(metadata.data_source.imported_at) : "未导入"}</span>
              {dataSourceReady && metadata.data_source.encoding ? <span>编码：{metadata.data_source.encoding}</span> : null}
              {dataSourceReady && metadata.data_source.path ? <span>来源路径：{metadata.data_source.path}</span> : null}
            </div>
          </div>
          <div className="w-full max-w-xl rounded-[24px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 p-4">
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,text/csv"
              className="hidden"
              onChange={(event) => setSelectedFile(event.target.files?.[0] || null)}
            />
            <div className="text-sm font-medium text-slate-100">上传并激活数据源</div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              支持 UTF-8 / UTF-8-SIG 编码的 CSV。系统会校验宽表字段完整性、空文件、关键列和数值列格式。
            </p>
            <div className="mt-3 rounded-[18px] border border-dashed border-[rgba(56,189,248,0.12)] px-4 py-3 text-xs leading-6 text-slate-500">
              小文件适合直接上传。1GB 级或更大的正式数据，建议继续放在你本机原目录，再走“本地路径激活”，不要把浏览器上传当成大文件主入口。
            </div>
            <div className="mt-4 flex flex-col gap-3 sm:flex-row">
              <Button
                type="button"
                variant="outline"
                className="rounded-2xl"
                onClick={() => fileInputRef.current?.click()}
                disabled={isUploading}
              >
                <Upload className="mr-2 h-4 w-4" />
                选择 CSV
              </Button>
              <Button type="button" className="rounded-2xl" onClick={() => void handleUpload()} disabled={!selectedFile || isUploading}>
                {isUploading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Database className="mr-2 h-4 w-4" />}
                上传并激活
              </Button>
            </div>
            <div className="mt-3 rounded-[18px] border border-dashed border-[rgba(56,189,248,0.12)] px-4 py-3 text-xs leading-6 text-slate-500">
              {selectedFile ? `待上传文件：${selectedFile.name}` : "尚未选择文件。"}
            </div>
            {inferenceDraft ? (
              <div className="mt-4 rounded-[22px] border border-amber-400/20 bg-amber-400/5 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium text-slate-100">字段识别确认</div>
                    <p className="mt-1 text-xs leading-6 text-slate-400">
                      这份 CSV 没有完全命中固定模板，系统已经先做了表头识别。你可以调整哪些列是时间、维度、科目或忽略，再正式激活。
                    </p>
                  </div>
                  <Badge variant="outline" className="rounded-full border-amber-400/20 text-amber-300">
                    半自动导入
                  </Badge>
                </div>

                <div className="mt-4 grid gap-4 lg:grid-cols-2">
                  <div className="rounded-[18px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 p-4">
                    <div className="text-sm font-medium text-slate-100">期间字段</div>
                    <div className="mt-3 grid gap-3">
                      <div>
                        <div className="mb-1 text-xs text-slate-400">单列期间</div>
                        <select
                          className="h-10 w-full rounded-xl border border-[rgba(56,189,248,0.12)] bg-slate-950 px-3 text-sm text-slate-100"
                          value={editableColumns.find((column) => column.current_type === "period")?.name || ""}
                          onChange={(event) => {
                            const value = event.target.value
                            setEditableColumns((current) =>
                              current.map((column) => {
                                if (column.current_type === "period") {
                                  return { ...column, current_type: "ignored" }
                                }
                                if (column.name === value) {
                                  return { ...column, current_type: "period" }
                                }
                                return column
                              }),
                            )
                          }}
                        >
                          <option value="">未使用</option>
                          {editableColumns.map((column) => (
                            <option key={`period-${column.name}`} value={column.name}>
                              {column.name}
                            </option>
                          ))}
                        </select>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                        <div>
                          <div className="mb-1 text-xs text-slate-400">年份列</div>
                          <select
                            className="h-10 w-full rounded-xl border border-[rgba(56,189,248,0.12)] bg-slate-950 px-3 text-sm text-slate-100"
                            value={editableColumns.find((column) => column.current_type === "period_year")?.name || ""}
                            onChange={(event) => {
                              const value = event.target.value
                              setEditableColumns((current) =>
                                current.map((column) => {
                                  if (column.current_type === "period_year") {
                                    return { ...column, current_type: "ignored" }
                                  }
                                  if (column.name === value) {
                                    return { ...column, current_type: "period_year" }
                                  }
                                  return column
                                }),
                              )
                            }}
                          >
                            <option value="">未使用</option>
                            {editableColumns.map((column) => (
                              <option key={`year-${column.name}`} value={column.name}>
                                {column.name}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div>
                          <div className="mb-1 text-xs text-slate-400">月份列</div>
                          <select
                            className="h-10 w-full rounded-xl border border-[rgba(56,189,248,0.12)] bg-slate-950 px-3 text-sm text-slate-100"
                            value={editableColumns.find((column) => column.current_type === "period_month")?.name || ""}
                            onChange={(event) => {
                              const value = event.target.value
                              setEditableColumns((current) =>
                                current.map((column) => {
                                  if (column.current_type === "period_month") {
                                    return { ...column, current_type: "ignored" }
                                  }
                                  if (column.name === value) {
                                    return { ...column, current_type: "period_month" }
                                  }
                                  return column
                                }),
                              )
                            }}
                          >
                            <option value="">未使用</option>
                            {editableColumns.map((column) => (
                              <option key={`month-${column.name}`} value={column.name}>
                                {column.name}
                              </option>
                            ))}
                          </select>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-[18px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 p-4">
                    <div className="text-sm font-medium text-slate-100">能力边界</div>
                    <div className="mt-3 space-y-2 text-xs leading-6 text-slate-400">
                      <div>趋势分析：{inferenceDraft.capabilities.supports_trend_analysis ? "支持" : "不支持"}</div>
                      <div>员工明细：{inferenceDraft.capabilities.supports_employee_level_detail ? "支持" : "受限"}</div>
                      <div>已识别科目：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).subject_columns.join("、") || "未确认"}</div>
                      <div>已识别维度：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).display_dimension_columns.join("、") || "未确认"}</div>
                    </div>
                  </div>
                </div>

                <div className="mt-4 grid gap-4 lg:grid-cols-3">
                  <div className="rounded-[18px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 p-4 lg:col-span-1">
                    <div className="text-sm font-medium text-slate-100">维度字段</div>
                    <div className="mt-3 space-y-3">
                      {editableColumns.filter((column) => column.current_type === "dimension").map((column) => (
                        <div key={`dimension-${column.name}`} className="rounded-xl border border-[rgba(56,189,248,0.08)] p-3">
                          <div className="text-xs text-slate-500">{column.name}</div>
                          <Input
                            value={column.current_name}
                            onChange={(event) => setEditableColumns((current) => current.map((item) => item.name === column.name ? { ...item, current_name: event.target.value } : item))}
                            className="mt-2 h-10 rounded-xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100"
                          />
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="rounded-[18px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 p-4 lg:col-span-1">
                    <div className="text-sm font-medium text-slate-100">科目字段</div>
                    <div className="mt-3 space-y-3">
                      {editableColumns.filter((column) => column.current_type === "subject").map((column) => (
                        <div key={`subject-${column.name}`} className="rounded-xl border border-[rgba(56,189,248,0.08)] p-3">
                          <div className="text-xs text-slate-500">{column.name}</div>
                          <Input
                            value={column.current_name}
                            onChange={(event) => setEditableColumns((current) => current.map((item) => item.name === column.name ? { ...item, current_name: event.target.value } : item))}
                            className="mt-2 h-10 rounded-xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100"
                          />
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="rounded-[18px] border border-[rgba(56,189,248,0.08)] bg-slate-950/50 p-4 lg:col-span-1">
                    <div className="text-sm font-medium text-slate-100">全部字段</div>
                    <div className="mt-3 max-h-[380px] space-y-3 overflow-auto pr-1">
                      {editableColumns.map((column) => (
                        <div key={column.name} className="rounded-xl border border-[rgba(56,189,248,0.08)] p-3">
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <div className="text-sm font-medium text-slate-100">{column.name}</div>
                              <div className="mt-1 text-xs leading-5 text-slate-500">{column.reason}</div>
                              {column.sample_values.length ? (
                                <div className="mt-1 text-[11px] text-slate-500">样本：{column.sample_values.join("、")}</div>
                              ) : null}
                            </div>
                            <Badge variant="outline" className="rounded-full border-[rgba(56,189,248,0.12)] text-slate-300">
                              {Math.round(column.confidence * 100)}%
                            </Badge>
                          </div>
                          <div className="mt-3 grid grid-cols-2 gap-3">
                            <select
                              className="h-10 rounded-xl border border-[rgba(56,189,248,0.12)] bg-slate-950 px-3 text-sm text-slate-100"
                              value={column.current_type}
                              onChange={(event) => setEditableColumns((current) => current.map((item) => item.name === column.name ? { ...item, current_type: event.target.value } : item))}
                            >
                              <option value="dimension">维度</option>
                              <option value="subject">科目</option>
                              <option value="ignored">忽略</option>
                              <option value="period">单列期间</option>
                              <option value="period_year">年份</option>
                              <option value="period_month">月份</option>
                            </select>
                            <Input
                              value={column.current_name}
                              onChange={(event) => setEditableColumns((current) => current.map((item) => item.name === column.name ? { ...item, current_name: event.target.value } : item))}
                              className="h-10 rounded-xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100"
                              placeholder="显示名 / 标准别名"
                            />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                <div className="mt-4 flex items-center justify-end gap-3">
                  <Button
                    type="button"
                    variant="outline"
                    className="rounded-2xl"
                    onClick={() => {
                      setInferenceDraft(null)
                      setEditableColumns([])
                      setInferencePath("")
                    }}
                  >
                    取消
                  </Button>
                  <Button type="button" className="rounded-2xl" onClick={() => void handleActivateInferred()} disabled={isActivatingInference}>
                    {isActivatingInference ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Sparkles className="mr-2 h-4 w-4" />}
                    确认字段并激活
                  </Button>
                </div>
              </div>
            ) : null}
          </div>
        </div>
        {inferenceDraft ? (
          <div className="mt-5 rounded-[24px] border border-[rgba(56,189,248,0.16)] bg-slate-950/55 p-5">
            <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
              <div>
                <div className="text-sm font-semibold text-slate-100">字段识别确认</div>
                <div className="mt-1 text-sm leading-6 text-slate-400">
                  系统已经自动识别了表头分类。你可以微调“时间 / 维度 / 科目 / 忽略”，确认后再正式导入。
                </div>
              </div>
              <div className="flex flex-wrap gap-2 text-xs text-slate-400">
                <span>候选维度：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).display_dimension_columns.length}</span>
                <span>候选科目：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).subject_columns.length}</span>
              </div>
            </div>

            <div className="mt-4 grid gap-4 lg:grid-cols-4">
              <div className="rounded-[20px] border border-[rgba(56,189,248,0.08)] bg-slate-900/60 p-4">
                <div className="text-sm font-medium text-slate-100">期间字段</div>
                <div className="mt-2 space-y-2 text-xs text-slate-400">
                  <div>年字段：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).period.year_column || "未指定"}</div>
                  <div>月字段：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).period.month_column || "未指定"}</div>
                  <div>期间字段：{buildManifestFromEditableColumns(inferenceDraft, editableColumns).period.period_column || "未指定"}</div>
                </div>
              </div>
              <div className="rounded-[20px] border border-[rgba(56,189,248,0.08)] bg-slate-900/60 p-4">
                <div className="text-sm font-medium text-slate-100">维度字段</div>
                <div className="mt-2 text-xs text-slate-400">
                  {buildManifestFromEditableColumns(inferenceDraft, editableColumns).display_dimension_columns.join("、") || "暂无"}
                </div>
              </div>
              <div className="rounded-[20px] border border-[rgba(56,189,248,0.08)] bg-slate-900/60 p-4">
                <div className="text-sm font-medium text-slate-100">科目字段</div>
                <div className="mt-2 text-xs text-slate-400">
                  {buildManifestFromEditableColumns(inferenceDraft, editableColumns).subject_columns.join("、") || "暂无"}
                </div>
              </div>
              <div className="rounded-[20px] border border-[rgba(56,189,248,0.08)] bg-slate-900/60 p-4">
                <div className="text-sm font-medium text-slate-100">分析能力</div>
                <div className="mt-2 space-y-2 text-xs text-slate-400">
                  <div>{buildManifestFromEditableColumns(inferenceDraft, editableColumns).capabilities.supports_trend_analysis ? "支持趋势分析" : "仅支持截面分析"}</div>
                  <div>{buildManifestFromEditableColumns(inferenceDraft, editableColumns).capabilities.supports_employee_level_detail ? "支持员工明细" : "不支持员工级明细"}</div>
                </div>
              </div>
            </div>

            <div className="mt-4 space-y-3">
              {editableColumns.map((column) => (
                <div key={column.name} className="rounded-[20px] border border-[rgba(56,189,248,0.08)] bg-slate-900/45 p-4">
                  <div className="grid gap-3 lg:grid-cols-[1.2fr_0.8fr_1fr_1.4fr]">
                    <div>
                      <div className="text-sm font-medium text-slate-100">{column.name}</div>
                      <div className="mt-1 text-xs text-slate-500">
                        置信度 {Math.round(column.confidence * 100)}% · {column.reason}
                      </div>
                      {column.sample_values.length ? (
                        <div className="mt-2 text-xs text-slate-400">样本值：{column.sample_values.join("、")}</div>
                      ) : null}
                    </div>
                    <div>
                      <div className="mb-1 text-xs text-slate-500">字段分类</div>
                      <select
                        value={column.current_type}
                        onChange={(event) =>
                          setEditableColumns((current) =>
                            current.map((item) => (item.name === column.name ? { ...item, current_type: event.target.value } : item)),
                          )
                        }
                        className="h-10 w-full rounded-xl border border-[rgba(56,189,248,0.12)] bg-slate-950/70 px-3 text-sm text-slate-100"
                      >
                        <option value="period_year">期间-年</option>
                        <option value="period_month">期间-月</option>
                        <option value="period">期间-单列</option>
                        <option value="dimension">维度</option>
                        <option value="subject">科目</option>
                        <option value="ignored">忽略</option>
                      </select>
                    </div>
                    <div>
                      <div className="mb-1 text-xs text-slate-500">显示名称</div>
                      <Input
                        value={column.current_name}
                        onChange={(event) =>
                          setEditableColumns((current) =>
                            current.map((item) => (item.name === column.name ? { ...item, current_name: event.target.value } : item)),
                          )
                        }
                        className="h-10 rounded-xl border-[rgba(56,189,248,0.12)] bg-slate-950/70 text-slate-100"
                      />
                    </div>
                    <div className="text-xs text-slate-400">
                      <div>非空率：{Math.round(column.non_empty_ratio * 100)}%</div>
                      <div className="mt-1">数值占比：{Math.round(column.numeric_ratio * 100)}%</div>
                      <div className="mt-1">当前建议：{column.current_type} / {column.current_name || "未命名"}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:justify-end">
              <Button
                type="button"
                variant="outline"
                className="rounded-2xl"
                onClick={() => {
                  setInferenceDraft(null)
                  setEditableColumns([])
                  setInferencePath("")
                }}
              >
                取消本次确认
              </Button>
              <Button
                type="button"
                className="rounded-2xl"
                onClick={() => void handleActivateInferred()}
                disabled={isActivatingInference}
              >
                {isActivatingInference ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Sparkles className="mr-2 h-4 w-4" />}
                确认字段并激活
              </Button>
            </div>
          </div>
        ) : null}
      </div>

      <div className="mb-6 rounded-[24px] border border-[rgba(56,189,248,0.12)] bg-slate-900/60 p-4 md:hidden">
        {dataSourceReady ? (
          <>
            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-slate-500">
              <CalendarRange className="h-3.5 w-3.5" />
              分析窗口
            </div>
            <div className="mt-3 grid grid-cols-1 gap-3">
              <Select value={visibleStartPeriod} onValueChange={setSelectedStartPeriod}>
                <SelectTrigger className="h-11 rounded-2xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100">
                  <SelectValue placeholder="开始时间" />
                </SelectTrigger>
                <SelectContent>
                  {periodOptions.map((period) => (
                    <SelectItem key={period} value={period}>
                      {period}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={visibleEndPeriod} onValueChange={setSelectedEndPeriod}>
                <SelectTrigger className="h-11 rounded-2xl border-[rgba(56,189,248,0.12)] bg-slate-950/60 text-slate-100">
                  <SelectValue placeholder="结束时间" />
                </SelectTrigger>
                <SelectContent>
                  {periodOptions.map((period) => (
                    <SelectItem key={period} value={period}>
                      {period}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </>
        ) : (
          <div className="text-sm leading-7 text-slate-300">上传真实数据后，这里会自动展示当前活动数据源的时间范围。</div>
        )}
      </div>

      <div className="flex-1">
        <div className="relative min-h-[760px] rounded-[40px] border border-[rgba(56,189,248,0.12)] bg-[rgba(8,12,24,0.84)] shadow-panel backdrop-blur-xl" data-testid="analysis-chat-shell">
          <div className="border-b border-[rgba(56,189,248,0.08)] px-6 py-5">
            <div className="flex items-center gap-3">
              <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-gradient-to-br from-neon-cyan to-sky-500 text-slate-950">
                <MessageSquareText className="h-5 w-5" />
              </div>
              <div>
                <div className="text-sm font-semibold text-slate-100">分析助手</div>
                <div className="text-xs text-slate-500" data-testid="active-data-source-label">
                  {dataSourceReady ? `当前数据源：${metadata.data_source.filename}` : "请先完成真实数据导入"}
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-4 px-5 py-6 md:px-6">
            <ThinkingPanel steps={analysisSteps} />
            {landingMessages.map((message) => (
              <ChatBubble key={message.id} role={message.role} content={message.content} />
            ))}

            {needsSubjectOnly ? (
              <div className="flex justify-start">
                <div className="w-full max-w-[85%] rounded-[24px] border border-[rgba(56,189,248,0.1)] bg-slate-900/70 p-5">
                  <div className="text-sm font-medium text-slate-100">先确认这次分析的科目</div>
                  <div className="mt-2 text-sm text-slate-400">
                    我会沿用当前活动数据源的时间范围，并按默认分析框架继续生成报告。
                  </div>
                  <div className="mt-4 flex flex-wrap gap-2">
                    {clarification?.clarification.subject_options.map((subject) => (
                      <Button
                        key={subject}
                        type="button"
                        variant={selectedSubject === subject ? "default" : "outline"}
                        className="rounded-full"
                        onClick={() => void handleSubjectConfirm(subject)}
                        disabled={isLoading}
                      >
                        {subject}
                      </Button>
                    ))}
                  </div>
                </div>
              </div>
            ) : null}

            {error ? (
              <div className="flex justify-start">
                <div className="max-w-[85%] rounded-[24px] border border-red-500/20 bg-red-500/10 px-5 py-4 text-sm text-red-300">
                  {error}
                </div>
              </div>
            ) : null}

            {visibleStartPeriod > visibleEndPeriod ? (
              <div className="flex justify-start">
                <div className="max-w-[85%] rounded-[24px] border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-200">
                  开始时间不能晚于结束时间，请先调整分析窗口。
                </div>
              </div>
            ) : null}
          </div>

          {libraryPanelOpen ? (
            <div className="absolute bottom-[116px] left-4 right-4 z-20 md:left-auto md:right-6 md:w-[480px]">
              <div className="rounded-[30px] border border-[rgba(56,189,248,0.16)] bg-[rgba(8,12,24,0.96)] p-4 shadow-[0_24px_80px_rgba(2,6,23,0.45)] backdrop-blur-xl">
                <div className="mb-4 flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-slate-100">分析辅助库</div>
                    <div className="text-xs text-slate-500">按需展开，避免首页信息过满。</div>
                  </div>
                  <Button variant="ghost" size="sm" className="rounded-xl text-slate-400" onClick={() => setLibraryPanelOpen(false)}>
                    收起
                  </Button>
                </div>
                <Tabs value={libraryPanelTab} onValueChange={(value) => setLibraryPanelTab(value as "knowledge" | "template")}>
                  <TabsList className="mb-4 grid w-full grid-cols-2 rounded-[20px]">
                    <TabsTrigger value="knowledge" className="rounded-2xl">
                      知识库
                    </TabsTrigger>
                    <TabsTrigger value="template" className="rounded-2xl">
                      模版库
                    </TabsTrigger>
                  </TabsList>
                </Tabs>
                {libraryPanelTab === "knowledge" ? (
                  <KnowledgeBasePanel items={knowledgeBaseItems} />
                ) : (
                  <TemplatePanel
                    options={reportTemplateOptions}
                    selectedTemplateId={selectedTemplateId}
                    onSelect={setSelectedTemplateId}
                  />
                )}
              </div>
            </div>
          ) : null}

          <div className="border-t border-[rgba(56,189,248,0.08)] px-5 py-5 md:px-6">
            <div className="flex flex-col gap-3">
              <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                <div className="flex flex-wrap gap-2">
                      {promptSuggestions.map((prompt) => (
                        <button
                          key={prompt}
                          type="button"
                          onClick={() => {
                            if (!dataSourceReady) return
                            setDraft(prompt)
                            void handleSubmit(prompt)
                          }}
                          disabled={!dataSourceReady}
                          className={`rounded-full border px-4 py-2 text-xs transition-colors ${
                            dataSourceReady
                              ? "border-[rgba(56,189,248,0.12)] bg-slate-900/60 text-slate-400 hover:border-neon-cyan/40 hover:text-neon-cyan"
                              : "cursor-not-allowed border-[rgba(56,189,248,0.08)] bg-slate-900/30 text-slate-600"
                          }`}
                        >
                          {prompt}
                        </button>
                  ))}
                </div>
                <div className="flex items-center justify-end gap-2">
                  <button
                    type="button"
                    onClick={() => openLibraryPanel("knowledge")}
                    className={`inline-flex min-w-[108px] items-center justify-center gap-2 rounded-full border px-5 py-2 text-xs whitespace-nowrap transition-colors ${
                      libraryPanelOpen && libraryPanelTab === "knowledge"
                        ? "border-neon-cyan/40 bg-neon-cyan/10 text-neon-cyan"
                        : "border-[rgba(56,189,248,0.12)] bg-slate-900/60 text-slate-400 hover:border-neon-cyan/30 hover:text-slate-100"
                    }`}
                  >
                    <Database className="h-3.5 w-3.5" />
                    知识库
                  </button>
                  <button
                    type="button"
                    onClick={() => openLibraryPanel("template")}
                    className={`inline-flex min-w-[108px] items-center justify-center gap-2 rounded-full border px-5 py-2 text-xs whitespace-nowrap transition-colors ${
                      libraryPanelOpen && libraryPanelTab === "template"
                        ? "border-neon-cyan/40 bg-neon-cyan/10 text-neon-cyan"
                        : "border-[rgba(56,189,248,0.12)] bg-slate-900/60 text-slate-400 hover:border-neon-cyan/30 hover:text-slate-100"
                    }`}
                  >
                    <BookOpenText className="h-3.5 w-3.5" />
                    模版库
                  </button>
                </div>
              </div>
              <div className="flex gap-3">
                <Input
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  className="h-14 rounded-[22px] border-[rgba(56,189,248,0.12)] bg-slate-900/60 text-slate-100"
                  placeholder={dataSourceReady ? "例如：请分析最近 12 个月底薪结构变化" : "请先上传并激活真实 CSV 数据"}
                  disabled={!dataSourceReady}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault()
                      void handleSubmit()
                    }
                  }}
                />
                <Button className="h-14 rounded-[22px] px-7" onClick={() => void handleSubmit()} disabled={isLoading || !draft.trim() || !dataSourceReady}>
                  {isLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Sparkles className="mr-2 h-4 w-4" />}
                  分析
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
