import { create } from "zustand"
import type {
  AnalysisStep,
  DataSourceInfo,
  MetadataResponse,
  ReportResponse,
  ClarificationResponse,
  ApiResponse,
  MonitorItem,
  HistoryEntry,
  FollowUpMessage,
  LandingMessage,
  SavedReportSummary,
  SavedReportSnapshot,
  KnowledgeBaseItem,
  ReportTemplateOption,
  ReportRevisionRequest,
} from "@/types"

const defaultMetadata: MetadataResponse = {
  subjects: [],
  dimensions: [],
  primary_dimension: "BU",
  row_count: 0,
  period_start: "",
  period_end: "",
  data_source: {
    ready: false,
    filename: "",
    path: "",
    row_count: 0,
    period_start: "",
    period_end: "",
    imported_at: "",
    signature: "",
    encoding: "",
    validation_status: "missing",
    message: "请先导入兼容当前宽表结构的真实 CSV 数据。",
  },
}

export const promptSuggestions = [
  "请分析最近 12 个月底薪结构变化及主要驱动因素。",
  "哪些 BU 和人群对当前薪酬总额贡献最高？",
  "帮我生成一份按 BU 拆解的正式分析报告。",
  "最近一个月环比变化最大的 BU 是谁？",
  "异常员工主要集中在哪些群体？",
]

export const DEFAULT_SECONDARY_DIMENSIONS = ["部门", "级别", "去年绩效排名", "年龄分箱"]
export const DEFAULT_METRICS = ["总额", "平均金额", "领取人数", "发放覆盖率", "占比", "环比", "同比"]
export const DEFAULT_HISTORY_TAB = "saved"

export const defaultKnowledgeBaseItems: KnowledgeBaseItem[] = [
  {
    id: "kb-1",
    title: "历史报告库接入位",
    description: "后续可接入真实历史报告快照，辅助统一输出口径。",
    status: "待接入",
    updated_at: "2026-03-01",
  },
  {
    id: "kb-2",
    title: "制度资料接入位",
    description: "可接入薪酬制度、政策说明和业务口径文档，供分析引用。",
    status: "待接入",
    updated_at: "2026-02-20",
  },
  {
    id: "kb-3",
    title: "专项分析模版位",
    description: "可绑定真实专项分析模版，支持不同汇报场景复用。",
    status: "待接入",
    updated_at: "2026-03-08",
  },
]

export const reportTemplateOptions: ReportTemplateOption[] = [
  {
    id: "standard",
    name: "标准分析报告",
    description: "适合完整拆解结构、给出结论与治理动作。",
    accent: "from-sky-400/30 to-cyan-400/10",
  },
  {
    id: "briefing",
    name: "管理层简报",
    description: "更强调一句话判断、关键风险与决策动作。",
    accent: "from-emerald-400/30 to-teal-400/10",
  },
  {
    id: "diagnosis",
    name: "问题诊断版",
    description: "突出异常群体、风险来源与原因拆解。",
    accent: "from-amber-400/30 to-orange-400/10",
  },
  {
    id: "governance",
    name: "治理建议版",
    description: "适合把重点放在政策动作、落地节奏和组织影响。",
    accent: "from-fuchsia-400/25 to-pink-400/10",
  },
]

function createLandingMessage(role: LandingMessage["role"], content: string): LandingMessage {
  return {
    id: `${role}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    role,
    content,
  }
}

function buildReportState(report: ReportResponse | null) {
  if (!report) {
    return {
      report: null,
      previousRequest: null,
      previousSummary: null,
    }
  }

  return {
    report,
    previousRequest: report.request,
    previousSummary: report.report.executive_summary,
  }
}

function toRevisionBody(
  request: ReportResponse["request"],
  report: ReportResponse["report"],
  revision_instruction: string,
  follow_up_messages: FollowUpMessage[],
): ReportRevisionRequest {
  return {
    request,
    report,
    revision_instruction,
    follow_up_messages,
  }
}

function hasUserInteraction(messages: LandingMessage[]) {
  return messages.some((message) => message.role === "user")
}

function buildInitialAssistantContent(dataSource: DataSourceInfo) {
  if (!dataSource.ready) {
    return "请先导入真实薪酬宽表 CSV。导入成功后，我会基于当前活动数据源开始分析。"
  }
  return `当前活动数据源已就绪：${dataSource.filename || "未命名数据源"}。现在可以直接输入分析问题。`
}

function createInitialLandingMessages(meta: MetadataResponse) {
  return [createLandingMessage("assistant", buildInitialAssistantContent(meta.data_source))]
}

type AppView = "chat" | "report"
type HistoryTab = "saved" | "queries"

type SubmitOverrides = {
  subject?: string
  secondary_dimensions?: string[]
  metrics?: string[]
  context?: { previous_request: ReportResponse["request"]; previous_summary: string }
}

type AppState = {
  metadata: MetadataResponse
  setMetadata: (meta: MetadataResponse) => void
  resetForActivatedDataSource: (meta: MetadataResponse) => void

  question: string
  setQuestion: (q: string) => void

  report: ReportResponse | null
  setReport: (r: ReportResponse | null) => void

  clarification: ClarificationResponse | null
  setClarification: (c: ClarificationResponse | null) => void

  isLoading: boolean
  setIsLoading: (v: boolean) => void
  error: string
  setError: (e: string) => void

  appView: AppView
  setAppView: (view: AppView) => void

  resultTab: string
  setResultTab: (t: string) => void
  activeDimensionTab: string
  setActiveDimensionTab: (t: string) => void

  selectedSubject: string
  setSelectedSubject: (s: string) => void
  selectedDimensions: string[]
  setSelectedDimensions: (d: string[]) => void
  selectedMetrics: string[]
  setSelectedMetrics: (m: string[]) => void
  selectedStartPeriod: string
  setSelectedStartPeriod: (value: string) => void
  selectedEndPeriod: string
  setSelectedEndPeriod: (value: string) => void

  isStreaming: boolean
  setIsStreaming: (v: boolean) => void
  streamedDimensions: ReportResponse["report"]["dimension_reports"]
  appendStreamedDimension: (d: ReportResponse["report"]["dimension_reports"][0]) => void
  resetStreamedDimensions: () => void
  streamingMessage: string
  setStreamingMessage: (msg: string) => void
  analysisSteps: AnalysisStep[]
  upsertAnalysisStep: (step: Omit<AnalysisStep, "status">) => void
  clearAnalysisSteps: () => void
  completeAnalysisSteps: () => void

  previousRequest: ReportResponse["request"] | null
  previousSummary: string | null

  followUpMessages: FollowUpMessage[]
  appendFollowUp: (msg: FollowUpMessage) => void
  clearFollowUps: () => void
  isFollowUpLoading: boolean
  setIsFollowUpLoading: (v: boolean) => void
  followUpDrawerOpen: boolean
  setFollowUpDrawerOpen: (v: boolean) => void
  followUpDrawerWidth: number
  setFollowUpDrawerWidth: (v: number) => void
  reportRevisionLoading: boolean
  setReportRevisionLoading: (v: boolean) => void

  monitorItems: MonitorItem[]
  setMonitorItems: (items: MonitorItem[]) => void

  historyEntries: HistoryEntry[]
  setHistoryEntries: (entries: HistoryEntry[]) => void
  historyOpen: boolean
  setHistoryOpen: (v: boolean) => void
  selectedHistoryTab: HistoryTab
  setSelectedHistoryTab: (tab: HistoryTab) => void
  savedReports: SavedReportSummary[]
  setSavedReports: (entries: SavedReportSummary[]) => void
  savedReportsLoading: boolean
  setSavedReportsLoading: (v: boolean) => void
  saveReportLoading: boolean
  setSaveReportLoading: (v: boolean) => void
  currentSavedReportId: number | null
  currentReportSourceType: "manual" | "revised"
  currentReportBaseSavedReportId: number | null
  currentReportRevisionInstruction: string | null
  fetchSavedReports: () => Promise<void>
  saveCurrentReport: () => Promise<SavedReportSnapshot | null>
  openSavedReport: (id: number) => Promise<void>

  landingMessages: LandingMessage[]
  appendLandingMessage: (msg: LandingMessage) => void
  clearLandingMessages: () => void
  beginChatQuestion: (question: string) => void
  acknowledgeSubjectSelection: (subject: string) => void
  knowledgeBaseItems: KnowledgeBaseItem[]
  selectedTemplateId: string
  setSelectedTemplateId: (templateId: string) => void
  reportTemplateOptions: ReportTemplateOption[]

  submitReport: (overrides?: SubmitOverrides) => Promise<void>
  submitReportStream: (overrides?: { subject?: string; secondary_dimensions?: string[]; metrics?: string[] }) => Promise<void>
  reviseCurrentReport: (instruction: string) => Promise<ReportResponse | null>
  fetchMetadata: () => Promise<void>
}

export const useAppStore = create<AppState>((set, get) => ({
  metadata: defaultMetadata,
  setMetadata: (meta) => set({ metadata: meta }),
  resetForActivatedDataSource: (meta) =>
    set({
      metadata: meta,
      question: "",
      report: null,
      clarification: null,
      isLoading: false,
      error: "",
      appView: "chat",
      resultTab: "summary",
      activeDimensionTab: "dimension",
      selectedSubject: "",
      selectedDimensions: DEFAULT_SECONDARY_DIMENSIONS,
      selectedMetrics: DEFAULT_METRICS,
      selectedStartPeriod: meta.period_start,
      selectedEndPeriod: meta.period_end,
      isStreaming: false,
      streamedDimensions: [],
      streamingMessage: "",
      analysisSteps: [],
      previousRequest: null,
      previousSummary: null,
      followUpMessages: [],
      isFollowUpLoading: false,
      followUpDrawerOpen: false,
      reportRevisionLoading: false,
      currentSavedReportId: null,
      currentReportSourceType: "manual",
      currentReportBaseSavedReportId: null,
      currentReportRevisionInstruction: null,
      landingMessages: createInitialLandingMessages(meta),
    }),

  question: "",
  setQuestion: (q) => set({ question: q }),

  report: null,
  setReport: (r) => set((state) => ({ ...buildReportState(r), previousRequest: r?.request ?? state.previousRequest, previousSummary: r?.report.executive_summary ?? state.previousSummary })),

  clarification: null,
  setClarification: (c) => set({ clarification: c }),

  isLoading: false,
  setIsLoading: (v) => set({ isLoading: v }),
  error: "",
  setError: (e) => set({ error: e }),

  appView: "chat",
  setAppView: (appView) => set({ appView }),

  resultTab: "summary",
  setResultTab: (t) => set({ resultTab: t }),
  activeDimensionTab: "dimension",
  setActiveDimensionTab: (t) => set({ activeDimensionTab: t }),

  selectedSubject: "",
  setSelectedSubject: (s) => set({ selectedSubject: s }),
  selectedDimensions: DEFAULT_SECONDARY_DIMENSIONS,
  setSelectedDimensions: (d) => set({ selectedDimensions: d }),
  selectedMetrics: DEFAULT_METRICS,
  setSelectedMetrics: (m) => set({ selectedMetrics: m }),
  selectedStartPeriod: defaultMetadata.period_start,
  setSelectedStartPeriod: (value) => set({ selectedStartPeriod: value }),
  selectedEndPeriod: defaultMetadata.period_end,
  setSelectedEndPeriod: (value) => set({ selectedEndPeriod: value }),

  isStreaming: false,
  setIsStreaming: (v) => set({ isStreaming: v }),
  streamedDimensions: [],
  appendStreamedDimension: (d) => set((state) => ({ streamedDimensions: [...state.streamedDimensions, d] })),
  resetStreamedDimensions: () => set({ streamedDimensions: [] }),
  streamingMessage: "",
  setStreamingMessage: (msg) => set({ streamingMessage: msg }),
  analysisSteps: [],
  upsertAnalysisStep: (step) =>
    set((state) => {
      const nextSteps = state.analysisSteps
        .filter((item) => item.step_index !== step.step_index)
        .concat({
          ...step,
          status: "active" as const,
        })
        .sort((a, b) => a.step_index - b.step_index)
        .map((item) => {
          if (item.step_index < step.step_index) {
            return { ...item, status: "completed" as const }
          }
          if (item.step_index === step.step_index) {
            return { ...item, ...step, status: "active" as const }
          }
          return item
        })

      return {
        analysisSteps: nextSteps,
        streamingMessage: step.message,
      }
    }),
  clearAnalysisSteps: () => set({ analysisSteps: [] }),
  completeAnalysisSteps: () =>
    set((state) => ({
      analysisSteps: state.analysisSteps.map((item) => ({ ...item, status: "completed" as const })),
    })),

  previousRequest: null,
  previousSummary: null,

  followUpMessages: [],
  appendFollowUp: (msg) => set((state) => ({ followUpMessages: [...state.followUpMessages, msg] })),
  clearFollowUps: () => set({ followUpMessages: [] }),
  isFollowUpLoading: false,
  setIsFollowUpLoading: (v) => set({ isFollowUpLoading: v }),
  followUpDrawerOpen: false,
  setFollowUpDrawerOpen: (v) => set({ followUpDrawerOpen: v }),
  followUpDrawerWidth: 520,
  setFollowUpDrawerWidth: (v) => set({ followUpDrawerWidth: Math.max(380, Math.min(860, v)) }),
  reportRevisionLoading: false,
  setReportRevisionLoading: (v) => set({ reportRevisionLoading: v }),

  monitorItems: [],
  setMonitorItems: (items) => set({ monitorItems: items }),

  historyEntries: [],
  setHistoryEntries: (entries) => set({ historyEntries: entries }),
  historyOpen: false,
  setHistoryOpen: (v) => set({ historyOpen: v }),
  selectedHistoryTab: DEFAULT_HISTORY_TAB,
  setSelectedHistoryTab: (tab) => set({ selectedHistoryTab: tab }),
  savedReports: [],
  setSavedReports: (entries) => set({ savedReports: entries }),
  savedReportsLoading: false,
  setSavedReportsLoading: (v) => set({ savedReportsLoading: v }),
  saveReportLoading: false,
  setSaveReportLoading: (v) => set({ saveReportLoading: v }),
  currentSavedReportId: null,
  currentReportSourceType: "manual",
  currentReportBaseSavedReportId: null,
  currentReportRevisionInstruction: null,

  landingMessages: createInitialLandingMessages(defaultMetadata),
  appendLandingMessage: (msg) => set((state) => ({ landingMessages: [...state.landingMessages, msg] })),
  clearLandingMessages: () =>
    set({
      landingMessages: createInitialLandingMessages(get().metadata),
    }),
  beginChatQuestion: (question) => {
    set({
      question,
      error: "",
      clarification: null,
      selectedSubject: "",
      selectedDimensions: DEFAULT_SECONDARY_DIMENSIONS,
      selectedMetrics: DEFAULT_METRICS,
    })
    get().appendLandingMessage(createLandingMessage("user", question))
  },
  acknowledgeSubjectSelection: (subject) => {
    set({
      selectedSubject: subject,
      selectedDimensions: DEFAULT_SECONDARY_DIMENSIONS,
      selectedMetrics: DEFAULT_METRICS,
    })
    get().appendLandingMessage(createLandingMessage("assistant", `已确认科目为“${subject}”，我将基于当前活动数据源开始流式分析。`))
  },
  knowledgeBaseItems: defaultKnowledgeBaseItems,
  selectedTemplateId: "standard",
  setSelectedTemplateId: (selectedTemplateId) => set({ selectedTemplateId }),
  reportTemplateOptions,

  fetchSavedReports: async () => {
    set({ savedReportsLoading: true })
    try {
      const response = await fetch("/api/saved-reports")
      if (!response.ok) {
        throw new Error("已保存报告加载失败")
      }
      const data: SavedReportSummary[] = await response.json()
      set({ savedReports: data })
    } catch (fetchError) {
      set({ error: fetchError instanceof Error ? fetchError.message : "已保存报告加载失败" })
    } finally {
      set({ savedReportsLoading: false })
    }
  },

  saveCurrentReport: async () => {
    const state = get()
    if (!state.report) return null
    set({ saveReportLoading: true, error: "" })
    try {
      const response = await fetch("/api/saved-reports", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          request: state.report.request,
          report: state.report.report,
          source_type: state.currentReportSourceType,
          base_saved_report_id: state.currentReportBaseSavedReportId,
          revision_instruction: state.currentReportRevisionInstruction,
        }),
      })
      if (!response.ok) {
        const payload = await response.json().catch(() => null)
        throw new Error(payload?.detail || "保存报告失败")
      }
      const savedSnapshot: SavedReportSnapshot = await response.json()
      set((current) => ({
        savedReports: [savedSnapshot, ...current.savedReports.filter((item) => item.id !== savedSnapshot.id)],
        currentSavedReportId: savedSnapshot.id,
      }))
      return savedSnapshot
    } catch (saveError) {
      set({ error: saveError instanceof Error ? saveError.message : "保存报告失败" })
      return null
    } finally {
      set({ saveReportLoading: false })
    }
  },

  openSavedReport: async (id) => {
    set({ savedReportsLoading: true, error: "" })
    try {
      const response = await fetch(`/api/saved-reports/${id}`)
      if (!response.ok) {
        const payload = await response.json().catch(() => null)
        throw new Error(payload?.detail || "保存报告加载失败")
      }
      const snapshot: SavedReportSnapshot = await response.json()
      const hydrated: ReportResponse = {
        request: snapshot.request,
        report: snapshot.report,
      }
      set({
        ...buildReportState(hydrated),
        appView: "report",
        resultTab: "summary",
        activeDimensionTab: snapshot.report.dimension_reports[0]?.dimension || "dimension",
        currentSavedReportId: snapshot.id,
        currentReportSourceType: snapshot.source_type,
        currentReportBaseSavedReportId: snapshot.base_saved_report_id,
        currentReportRevisionInstruction: snapshot.revision_instruction,
      })
    } catch (openError) {
      set({ error: openError instanceof Error ? openError.message : "保存报告加载失败" })
    } finally {
      set({ savedReportsLoading: false })
    }
  },

  fetchMetadata: async () => {
    try {
      const response = await fetch("/api/metadata")
      const meta: MetadataResponse = await response.json()
      set((state) => ({
        metadata: meta,
        landingMessages: hasUserInteraction(state.landingMessages) ? state.landingMessages : createInitialLandingMessages(meta),
        selectedStartPeriod:
          !meta.data_source.ready
            ? ""
            : !state.selectedStartPeriod ||
                (state.selectedStartPeriod === state.metadata.period_start && state.selectedEndPeriod === state.metadata.period_end)
              ? meta.period_start
              : state.selectedStartPeriod,
        selectedEndPeriod:
          !meta.data_source.ready
            ? ""
            : !state.selectedEndPeriod ||
                (state.selectedStartPeriod === state.metadata.period_start && state.selectedEndPeriod === state.metadata.period_end)
              ? meta.period_end
              : state.selectedEndPeriod,
      }))
    } catch (fetchError) {
      set({ error: fetchError instanceof Error ? fetchError.message : "元数据加载失败" })
    }
  },

  submitReport: async (overrides) => {
    const state = get()
    if (!state.metadata.data_source.ready) {
      set({ error: state.metadata.data_source.message || "请先导入真实数据后再开始分析。" })
      return
    }
    set({ isLoading: true, error: "" })
    try {
      const body: Record<string, unknown> = {
        question: state.question,
        subject: overrides?.subject,
        secondary_dimensions: overrides?.secondary_dimensions,
        metrics: overrides?.metrics,
        start_period: state.selectedStartPeriod,
        end_period: state.selectedEndPeriod,
      }
      if (overrides?.context) {
        body.context = overrides.context
      }
      const response = await fetch("/api/report", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      })
      if (!response.ok) {
        const payload = await response.json().catch(() => null)
        throw new Error(payload?.detail || "报告生成失败")
      }
      const payload: ApiResponse = await response.json()
      if (payload.mode === "clarification") {
        set({
          clarification: payload,
          report: null,
          selectedSubject: payload.request_draft.subject || "",
          selectedDimensions: payload.request_draft.secondary_dimensions?.length ? payload.request_draft.secondary_dimensions : DEFAULT_SECONDARY_DIMENSIONS,
          selectedMetrics: payload.request_draft.metrics?.length ? payload.request_draft.metrics : DEFAULT_METRICS,
          selectedStartPeriod: payload.request_draft.start_period || state.selectedStartPeriod,
          selectedEndPeriod: payload.request_draft.end_period || state.selectedEndPeriod,
          appView: "chat",
        })
        return
      }
      const nextReport = payload as ReportResponse
      set({
        clarification: null,
        report: nextReport,
        resultTab: "summary",
        activeDimensionTab: nextReport.report.dimension_reports[0]?.dimension || "dimension",
        previousRequest: nextReport.request,
        previousSummary: nextReport.report.executive_summary,
        appView: "report",
        currentSavedReportId: null,
        currentReportSourceType: "manual",
        currentReportBaseSavedReportId: null,
        currentReportRevisionInstruction: null,
      })
    } catch (submitError) {
      set({ error: submitError instanceof Error ? submitError.message : "报告生成失败" })
    } finally {
      set({ isLoading: false })
    }
  },

  submitReportStream: async (overrides) => {
    const state = get()
    if (!state.metadata.data_source.ready) {
      set({ error: state.metadata.data_source.message || "请先导入真实数据后再开始分析。" })
      return
    }
    set({
      isLoading: true,
      isStreaming: true,
      error: "",
      report: null,
      clarification: null,
      streamingMessage: "",
      analysisSteps: [],
      appView: "chat",
      currentSavedReportId: null,
      currentReportSourceType: "manual",
      currentReportBaseSavedReportId: null,
      currentReportRevisionInstruction: null,
    })
    state.resetStreamedDimensions()
    state.clearAnalysisSteps()

    try {
      const body: Record<string, unknown> = {
        question: state.question,
        subject: overrides?.subject,
        secondary_dimensions: overrides?.secondary_dimensions,
        metrics: overrides?.metrics,
        start_period: state.selectedStartPeriod,
        end_period: state.selectedEndPeriod,
      }
      const response = await fetch("/api/report/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      })
      if (!response.ok) {
        const payload = await response.json().catch(() => null)
        throw new Error(payload?.detail || "流式请求失败")
      }
      if (!response.body) {
        throw new Error("流式请求失败")
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        const lines = buffer.split("\n")
        buffer = lines.pop() || ""

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue
          const jsonStr = line.slice(6).trim()
          if (!jsonStr) continue

          try {
            const event = JSON.parse(jsonStr) as {
              type: string
              data?: unknown
              message?: string
              stage?: string
              label?: string
              step_index?: number
              step_total?: number
            }

            if (event.type === "clarification") {
              const clarData = event.data as ClarificationResponse
              const needsSubjectOnly =
                clarData.clarification.needs_subject &&
                !clarData.clarification.needs_dimensions &&
                !clarData.clarification.needs_metrics

              set({
                clarification: clarData,
                report: null,
                selectedSubject: clarData.request_draft.subject || "",
                selectedDimensions: clarData.request_draft.secondary_dimensions?.length ? clarData.request_draft.secondary_dimensions : DEFAULT_SECONDARY_DIMENSIONS,
                selectedMetrics: clarData.request_draft.metrics?.length ? clarData.request_draft.metrics : DEFAULT_METRICS,
                selectedStartPeriod: clarData.request_draft.start_period || state.selectedStartPeriod,
                selectedEndPeriod: clarData.request_draft.end_period || state.selectedEndPeriod,
              })

              get().appendLandingMessage(
                createLandingMessage(
                  "assistant",
                  needsSubjectOnly ? "我先帮你把分析口径补齐。请先确认这次要分析的科目。" : clarData.message,
                ),
              )
            } else if (event.type === "progress") {
              if (event.message) {
                set({ streamingMessage: event.message })
              }
              if (typeof event.step_index === "number" && typeof event.step_total === "number" && event.stage && event.label && event.message) {
                get().upsertAnalysisStep({
                  stage: event.stage,
                  label: event.label,
                  step_index: event.step_index,
                  step_total: event.step_total,
                  message: event.message,
                })
              }
            } else if (event.type === "hero") {
              set((prev) => ({
                report: {
                  ...prev.report,
                  request:
                    prev.report?.request || {
                      subject: overrides?.subject || "",
                      primary_dimension: "BU",
                      secondary_dimensions: overrides?.secondary_dimensions || DEFAULT_SECONDARY_DIMENSIONS,
                      start_period: state.selectedStartPeriod,
                      end_period: state.selectedEndPeriod,
                      metrics: overrides?.metrics || DEFAULT_METRICS,
                      question: state.question,
                    },
                  report: {
                    ...(prev.report?.report || ({} as ReportResponse["report"])),
                    hero_metrics: event.data as ReportResponse["report"]["hero_metrics"],
                  },
                } as ReportResponse,
                resultTab: "summary",
              }))
            } else if (event.type === "overview") {
              const overview = event.data as {
                bu_overview: ReportResponse["report"]["bu_overview"]
                overview_charts: ReportResponse["report"]["overview_charts"]
              }
              set((prev) => ({
                report: prev.report
                  ? {
                      ...prev.report,
                      report: { ...prev.report.report, bu_overview: overview.bu_overview, overview_charts: overview.overview_charts },
                    }
                  : prev.report,
              }))
            } else if (event.type === "dimension") {
              const dim = event.data as ReportResponse["report"]["dimension_reports"][0]
              get().appendStreamedDimension(dim)
              set((prev) => ({
                report: prev.report
                  ? {
                      ...prev.report,
                      report: { ...prev.report.report, dimension_reports: [...(prev.report.report.dimension_reports || []), dim] },
                    }
                  : prev.report,
                activeDimensionTab: prev.report?.report.dimension_reports?.length === 0 ? dim.dimension : prev.activeDimensionTab,
              }))
            } else if (event.type === "consolidated") {
              const full = event.data as ReportResponse
              set({
                report: full,
                resultTab: "summary",
                activeDimensionTab: full.report.dimension_reports[0]?.dimension || "dimension",
                previousRequest: full.request,
                previousSummary: full.report.executive_summary,
                appView: "report",
                currentSavedReportId: null,
                currentReportSourceType: "manual",
                currentReportBaseSavedReportId: null,
                currentReportRevisionInstruction: null,
              })
              get().completeAnalysisSteps()
              get().appendLandingMessage(createLandingMessage("assistant", "分析已完成，正在切换到报告页。"))
            }
          } catch {
            // skip malformed JSON lines
          }
        }
      }
    } catch (streamError) {
      set({ error: streamError instanceof Error ? streamError.message : "流式报告生成失败" })
    } finally {
      set({ isLoading: false, isStreaming: false, streamingMessage: "" })
    }
  },

  reviseCurrentReport: async (instruction) => {
    const state = get()
    const trimmedInstruction = instruction.trim()
    if (!state.report || !trimmedInstruction) return null
    set({ reportRevisionLoading: true, error: "" })
    try {
      const response = await fetch("/api/report/revise", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          toRevisionBody(state.report.request, state.report.report, trimmedInstruction, state.followUpMessages),
        ),
      })
      if (!response.ok) {
        const payload = await response.json().catch(() => null)
        throw new Error(payload?.detail || "生成新报告失败")
      }
      const nextReport: ReportResponse = await response.json()
      set({
        report: nextReport,
        previousRequest: nextReport.request,
        previousSummary: nextReport.report.executive_summary,
        appView: "report",
        resultTab: "summary",
        currentSavedReportId: null,
        currentReportSourceType: "revised",
        currentReportBaseSavedReportId: state.currentSavedReportId ?? state.currentReportBaseSavedReportId,
        currentReportRevisionInstruction: trimmedInstruction,
      })
      return nextReport
    } catch (revisionError) {
      set({ error: revisionError instanceof Error ? revisionError.message : "生成新报告失败" })
      return null
    } finally {
      set({ reportRevisionLoading: false })
    }
  },
}))
