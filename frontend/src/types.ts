export type DataSourceInfo = {
  ready: boolean
  filename: string
  path: string
  row_count: number
  period_start: string
  period_end: string
  imported_at: string
  signature: string
  encoding: string
  validation_status: string
  schema_id?: string
  schema_name?: string
  schema_mode?: string
  source_manifest?: Record<string, unknown> | null
  message: string
}

export type DataSourceCapabilities = {
  supports_trend_analysis: boolean
  supports_employee_level_detail: boolean
  supports_yoy?: boolean
  supports_mom?: boolean
  external_research_enabled?: boolean
  external_research_status?: string
}

export type MetadataResponse = {
  subjects: string[]
  subject_catalog?: Array<{
    subject: string
    category: string
  }>
  dimension_catalog?: Array<{
    dimension: string
    category: string
  }>
  dimensions: string[]
  primary_dimension: string
  row_count: number
  period_start: string
  period_end: string
  schema_mode?: string
  capabilities?: DataSourceCapabilities
  source_manifest?: Record<string, unknown> | null
  data_source: DataSourceInfo
}

export type InferredColumn = {
  name: string
  detected_type: string
  canonical_name: string
  confidence: number
  reason: string
  sample_values: string[]
  non_empty_ratio: number
  numeric_ratio: number
}

export type InferredSchemaDraft = {
  schema_id: string
  display_name: string
  schema_mode: string
  period_mode: string
  period: {
    year_column?: string
    month_column?: string
    period_column?: string
  }
  text_dimension_columns: string[]
  dimension_columns: string[]
  display_dimension_columns: string[]
  subject_columns: string[]
  default_subject: string
  default_secondary_dimensions: string[]
  source_column_map: Record<string, string>
  dimension_aliases: Record<string, string>
  subject_aliases: Record<string, string>
  synthetic_defaults: Record<string, string>
  capabilities: DataSourceCapabilities
  columns: InferredColumn[]
  ignored_columns: string[]
}

export type UploadActivationResponse = {
  ok: boolean
  mode: "activated"
  filename: string
  stored_filename: string
  row_count: number
  period_start: string
  period_end: string
  data_source: DataSourceInfo
}

export type UploadInferenceResponse = {
  ok: boolean
  mode: "inference_required"
  filename: string
  stored_filename: string
  path: string
  encoding: string
  draft: InferredSchemaDraft
}

export type ChartConfig = {
  chart_type: string
  chart_title: string
  chart_insight: string
  chart_payload: Record<string, unknown>
}

export type InlineTable = {
  table_title: string
  columns: string[]
  rows: Array<Record<string, string | number>>
}

export type ReportSection = {
  id: string
  title: string
  content: string
  data_tables?: InlineTable[]
  charts?: ChartConfig[]
}

export type FollowUpMessage = {
  question: string
  answer: string
  mode?: string
  columns?: string[]
  rows?: Array<Record<string, string | number>>
  chart?: ChartConfig | null
  data_table?: InlineTable | null
}

export type LandingMessage = {
  id: string
  role: "user" | "assistant" | "status"
  content: string
}

export type AnalysisStep = {
  stage: string
  label: string
  step_index: number
  step_total: number
  message: string
  status: "pending" | "active" | "completed"
}

export type AnomalyPerson = {
  BU: string
  dimension: string
  dimension_value: string
  员工ID: string
  职能: string
  绩效分位: string
  级别: string
  司龄分箱: string
  年龄分箱: string
  total_amount: number
  avg_paid_amount: number
  paid_months: number
  z_score?: number
  signal?: string
  group_total_amount?: number
}

export type DimensionReport = {
  dimension: string
  source_mode: string
  column_identity?: string
  analysis_model?: {
    model: string
    insight?: string
    [key: string]: unknown
  }
  headline: string
  narrative: string
  key_findings: string[]
  anomalies: string[]
  anomaly_people: AnomalyPerson[]
  possible_drivers: string[]
  management_implications: string[]
  chart_data: {
    primary_chart: ChartConfig
    secondary_chart: ChartConfig
    supporting_charts: ChartConfig[]
  }
}

export type HeroMetrics = {
  total_amount: number
  avg_amount: number
  employee_count: number
  issued_employee_count: number
  coverage_rate: number
  trend_snapshot?: {
    latest_period: string
    latest_total: number
    previous_period?: string
    previous_total?: number | null
    mom_delta?: number | null
    mom_rate?: number | null
    yoy_period?: string
    yoy_total?: number | null
    yoy_delta?: number | null
    yoy_rate?: number | null
  }
}

export type ExternalSource = {
  source_name: string
  title: string
  published_at: string
  summary: string
  url: string
  query_topic: string
}

export type ReportResponse = {
  mode?: "report"
  request: {
    subject: string
    primary_dimension: string
    secondary_dimensions: string[]
    start_period: string
    end_period: string
    metrics: string[]
    question: string
  }
  report: {
    executive_summary: string
    short_answer?: string
    cross_dimension_summary: string[]
    priority_actions: Array<string | { action?: string; priority?: string; rationale?: string }>
    global_risks: string[]
    report_title: string
    report_subtitle: string
    leadership_takeaways: string[]
    appendix_notes: string[]
    external_research_summary: string[]
    external_sources: ExternalSource[]
    research_mode: string
    full_report_sections: Array<string | ReportSection>
    hero_metrics: HeroMetrics
    bu_overview: Array<Record<string, unknown>>
    overview_charts: ChartConfig[]
    dimension_reports: DimensionReport[]
    consolidated_charts: ChartConfig[]
    methodology: {
      data_source: string
      data_source_signature?: string
      data_source_imported_at?: string
      analysis_mode: string
      note: string
    }
  }
}

export type ClarificationResponse = {
  mode: "clarification"
  message: string
  request_draft: {
    subject: string
    primary_dimension: string
    secondary_dimensions: string[]
    start_period: string
    end_period: string
    metrics: string[]
    question: string
  }
  clarification: {
    needs_subject: boolean
    needs_time_window?: boolean
    needs_dimensions: boolean
    needs_metrics: boolean
    current_step?: "subject" | "time_window" | "dimensions" | "metrics"
    subject_prompt: string
    time_window_prompt?: string
    dimension_prompt: string
    metric_prompt: string
    subject_prompt_reason?: string
    subject_options: string[]
    subject_candidate_options?: string[]
    subject_catalog?: Array<{
      subject: string
      category: string
    }>
    dimension_options: string[]
    time_window_options?: Array<{
      label: string
      start_period: string
      end_period: string
    }>
    metric_options: string[]
    matched_terms?: string[]
    dimension_presets: Array<{
      label: string
      dimensions: string[]
    }>
  }
}

export type ApiResponse = ReportResponse | ClarificationResponse

export type EmployeeTimelineRecord = {
  period: string
  amount: number
}

// Monitor types (Phase 3.1)
export type MonitorItem = {
  subject: string
  mom_rate: number
  anomaly_count: number
  severity: "red" | "yellow" | "green"
}

// History types (Phase 3.2)
export type HistoryEntry = {
  id: number
  question: string
  request_json: string
  data_source_name?: string
  data_source_signature?: string
  created_at: string
}

export type SavedReportSummary = {
  id: number
  title: string
  subject: string
  question: string
  source_type: "manual" | "revised"
  base_saved_report_id: number | null
  revision_instruction: string | null
  data_source_name?: string
  data_source_signature?: string
  created_at: string
}

export type SavedReportSnapshot = SavedReportSummary & {
  request: ReportResponse["request"]
  report: ReportResponse["report"]
}

export type ReportRevisionRequest = {
  request: ReportResponse["request"]
  report: ReportResponse["report"]
  revision_instruction: string
  follow_up_messages?: FollowUpMessage[]
}

export type KnowledgeBaseItem = {
  id: string
  title: string
  description: string
  status: "已接入" | "待接入" | "示例"
  updated_at: string
}

export type ReportTemplateOption = {
  id: string
  name: string
  description: string
  accent: string
}
