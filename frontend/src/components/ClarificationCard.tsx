import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { useState } from "react"
import { useAppStore } from "@/store"
import type { ClarificationResponse } from "@/types"

function shouldShowSubjectOptions(clarification: ClarificationResponse, selectedSubject: string) {
  return clarification.clarification.needs_subject && !selectedSubject
}

function shouldShowTimeWindowOptions(clarification: ClarificationResponse, selectedSubject: string, selectedStartPeriod: string, selectedEndPeriod: string) {
  if (!clarification.clarification.needs_time_window) return false
  if (clarification.clarification.needs_subject && !selectedSubject) return false
  return !(selectedStartPeriod && selectedEndPeriod)
}

function shouldShowDimensionOptions(
  clarification: ClarificationResponse,
  selectedSubject: string,
  selectedStartPeriod: string,
  selectedEndPeriod: string,
  selectedDimensions: string[],
) {
  if (!clarification.clarification.needs_dimensions) return false
  if (clarification.clarification.needs_subject && !selectedSubject) return false
  if (clarification.clarification.needs_time_window && !(selectedStartPeriod && selectedEndPeriod)) return false
  return !selectedDimensions.length
}

function shouldShowMetricOptions(
  clarification: ClarificationResponse,
  selectedSubject: string,
  selectedStartPeriod: string,
  selectedEndPeriod: string,
  selectedDimensions: string[],
  selectedMetrics: string[],
) {
  if (!clarification.clarification.needs_metrics) return false
  if (clarification.clarification.needs_subject && !selectedSubject) return false
  if (clarification.clarification.needs_time_window && !(selectedStartPeriod && selectedEndPeriod)) return false
  if (clarification.clarification.needs_dimensions && !selectedDimensions.length) return false
  return !selectedMetrics.length
}

function arraysEqual(left: string[], right: string[]) {
  return JSON.stringify(left) === JSON.stringify(right)
}

function groupSubjectCatalog(
  subjectCatalog: Array<{
    subject: string
    category: string
  }>,
) {
  const grouped = new Map<string, string[]>()
  subjectCatalog.forEach((item) => {
    const current = grouped.get(item.category) || []
    if (!current.includes(item.subject)) {
      current.push(item.subject)
    }
    grouped.set(item.category, current)
  })
  return Array.from(grouped.entries()).map(([category, subjects]) => ({ category, subjects }))
}

function getCurrentClarificationStep(
  clarification: ClarificationResponse,
  selectedSubject: string,
  selectedStartPeriod: string,
  selectedEndPeriod: string,
  selectedDimensions: string[],
  selectedMetrics: string[],
) {
  if (shouldShowSubjectOptions(clarification, selectedSubject)) {
    return { index: 1, title: "确认科目" }
  }
  if (shouldShowTimeWindowOptions(clarification, selectedSubject, selectedStartPeriod, selectedEndPeriod)) {
    return { index: 2, title: "确认时间窗口" }
  }
  if (shouldShowDimensionOptions(clarification, selectedSubject, selectedStartPeriod, selectedEndPeriod, selectedDimensions)) {
    return { index: 3, title: "选择维度" }
  }
  if (shouldShowMetricOptions(clarification, selectedSubject, selectedStartPeriod, selectedEndPeriod, selectedDimensions, selectedMetrics)) {
    return { index: 4, title: "选择指标" }
  }
  return { index: 4, title: "确认分析" }
}

export default function ClarificationCard() {
  const clarification = useAppStore((s) => s.clarification)
  const selectedSubject = useAppStore((s) => s.selectedSubject)
  const setSelectedSubject = useAppStore((s) => s.setSelectedSubject)
  const selectedDimensions = useAppStore((s) => s.selectedDimensions)
  const setSelectedDimensions = useAppStore((s) => s.setSelectedDimensions)
  const selectedMetrics = useAppStore((s) => s.selectedMetrics)
  const setSelectedMetrics = useAppStore((s) => s.setSelectedMetrics)
  const selectedStartPeriod = useAppStore((s) => s.selectedStartPeriod)
  const setSelectedStartPeriod = useAppStore((s) => s.setSelectedStartPeriod)
  const selectedEndPeriod = useAppStore((s) => s.selectedEndPeriod)
  const setSelectedEndPeriod = useAppStore((s) => s.setSelectedEndPeriod)
  const isLoading = useAppStore((s) => s.isLoading)
  const submitReport = useAppStore((s) => s.submitReport)
  const [showAllSubjects, setShowAllSubjects] = useState(false)

  if (!clarification) return null

  const recommendedSubjects = clarification.clarification.subject_candidate_options?.length
    ? clarification.clarification.subject_candidate_options
    : clarification.clarification.subject_options
  const groupedCatalog = groupSubjectCatalog(clarification.clarification.subject_catalog || [])
  const totalSubjectCount = clarification.clarification.subject_catalog?.length || 0

  const clarificationStep = getCurrentClarificationStep(
    clarification,
    selectedSubject,
    selectedStartPeriod,
    selectedEndPeriod,
    selectedDimensions,
    selectedMetrics,
  )

  function continueClarification() {
    void submitReport({
      subject: selectedSubject || undefined,
      secondary_dimensions: selectedDimensions.length ? selectedDimensions : undefined,
      metrics: selectedMetrics.length ? selectedMetrics : undefined,
    })
  }

  const canContinue =
    (!clarification.clarification.needs_subject || !!selectedSubject) &&
    (!clarification.clarification.needs_time_window || (!!selectedStartPeriod && !!selectedEndPeriod)) &&
    (!clarification.clarification.needs_dimensions || !!selectedDimensions.length) &&
    (!clarification.clarification.needs_metrics || !!selectedMetrics.length)

  return (
    <Card className="rounded-[28px] border-[rgba(56,189,248,0.1)] bg-slate-900/50 shadow-none">
      <CardContent className="space-y-5 p-6">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-[0.18em] text-neon-cyan">
              Step {clarificationStep.index}
            </div>
            <div className="text-base font-semibold text-slate-100">
              {clarificationStep.title}
            </div>
          </div>
          <Badge variant="secondary" className="rounded-full px-3 py-1">
            当前主维度 BU
          </Badge>
        </div>

        <div className="rounded-[22px] border border-[rgba(56,189,248,0.1)] bg-slate-800/60 px-5 py-4">
          <div className="text-sm leading-7 text-slate-300">{clarification.message}</div>
          {clarification.clarification.subject_prompt_reason ? (
            <div className="mt-2 text-xs leading-6 text-amber-300">{clarification.clarification.subject_prompt_reason}</div>
          ) : null}
          {clarification.clarification.matched_terms?.length ? (
            <div className="mt-2 text-xs leading-6 text-slate-400">已识别线索：{clarification.clarification.matched_terms.join("、")}</div>
          ) : null}
        </div>

        {shouldShowSubjectOptions(clarification, selectedSubject) ? (
          <div className="space-y-3">
            <div className="text-sm font-medium text-slate-200">{clarification.clarification.subject_prompt}</div>
            <div className="flex flex-wrap gap-2">
              {recommendedSubjects.map((subject) => (
                <Button
                  key={subject}
                  type="button"
                  variant={selectedSubject === subject ? "default" : "outline"}
                  className="rounded-full"
                  onClick={() => setSelectedSubject(subject)}
                >
                  {subject}
                </Button>
              ))}
            </div>
            {totalSubjectCount > recommendedSubjects.length ? (
              <div className="flex items-center justify-between gap-3 rounded-[18px] border border-slate-800 bg-slate-950/60 px-4 py-3">
                <div className="text-xs leading-6 text-slate-400">
                  当前数据源共有 {totalSubjectCount} 个可分析科目，默认先展示高相关候选。
                </div>
                <Button
                  type="button"
                  variant="outline"
                  className="rounded-full"
                  onClick={() => setShowAllSubjects((value) => !value)}
                >
                  {showAllSubjects ? "收起" : "查看更多科目"}
                </Button>
              </div>
            ) : null}
            {showAllSubjects && groupedCatalog.length ? (
              <div className="space-y-3 rounded-[20px] border border-slate-800 bg-slate-950/70 p-4">
                {groupedCatalog.map((group) => (
                  <div key={group.category} className="space-y-2">
                    <div className="text-xs font-medium uppercase tracking-[0.14em] text-slate-500">{group.category}</div>
                    <div className="flex flex-wrap gap-2">
                      {group.subjects.map((subject) => (
                        <Button
                          key={`${group.category}-${subject}`}
                          type="button"
                          variant={selectedSubject === subject ? "default" : "outline"}
                          className="rounded-full"
                          onClick={() => setSelectedSubject(subject)}
                        >
                          {subject}
                        </Button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        {shouldShowTimeWindowOptions(clarification, selectedSubject, selectedStartPeriod, selectedEndPeriod) ? (
          <div className="space-y-3">
            <div className="text-sm font-medium text-slate-200">{clarification.clarification.time_window_prompt}</div>
            <div className="flex flex-wrap gap-2">
              {clarification.clarification.time_window_options?.map((option) => (
                <Button
                  key={`${option.start_period}-${option.end_period}`}
                  type="button"
                  variant={selectedStartPeriod === option.start_period && selectedEndPeriod === option.end_period ? "default" : "outline"}
                  className="rounded-full"
                  onClick={() => {
                    setSelectedStartPeriod(option.start_period)
                    setSelectedEndPeriod(option.end_period)
                  }}
                >
                  {option.label}
                </Button>
              ))}
            </div>
          </div>
        ) : null}

        {shouldShowDimensionOptions(clarification, selectedSubject, selectedStartPeriod, selectedEndPeriod, selectedDimensions) ? (
          <div className="space-y-3">
            <div className="text-sm font-medium text-slate-200">{clarification.clarification.dimension_prompt}</div>
            <div className="flex flex-wrap gap-2">
              {clarification.clarification.dimension_presets.map((preset) => (
                <Button
                  key={preset.label}
                  type="button"
                  variant={arraysEqual(selectedDimensions, preset.dimensions) ? "default" : "outline"}
                  className="rounded-full"
                  onClick={() => setSelectedDimensions(preset.dimensions)}
                >
                  {preset.label}
                </Button>
              ))}
            </div>
          </div>
        ) : null}

        {shouldShowMetricOptions(clarification, selectedSubject, selectedStartPeriod, selectedEndPeriod, selectedDimensions, selectedMetrics) ? (
          <div className="space-y-3">
            <div className="space-y-1">
              <div className="text-sm font-medium text-slate-200">{clarification.clarification.metric_prompt}</div>
              <div className="text-xs text-slate-500">这些指标支持多选，至少选择 1 个。</div>
            </div>
            <div className="flex flex-wrap gap-2">
              {clarification.clarification.metric_options.map((metric) => {
                const selected = selectedMetrics.includes(metric)
                return (
                  <Button
                    key={metric}
                    type="button"
                    variant={selected ? "default" : "outline"}
                    className="rounded-full"
                    onClick={() => {
                      const nextMetrics = selected
                        ? selectedMetrics.filter((item) => item !== metric)
                        : [...selectedMetrics, metric]
                      setSelectedMetrics(nextMetrics)
                    }}
                  >
                    {metric}
                  </Button>
                )
              })}
            </div>
          </div>
        ) : null}

        <div className="rounded-[22px] border border-slate-800 bg-slate-950/70 px-4 py-3 text-xs text-slate-400">
          <div>当前草稿科目：{selectedSubject || clarification.request_draft.subject || "未确认"}</div>
          <div>当前时间窗口：{selectedStartPeriod || clarification.request_draft.start_period} 至 {selectedEndPeriod || clarification.request_draft.end_period}</div>
          <div>当前维度：{selectedDimensions.length ? selectedDimensions.join("、") : "未确认"}</div>
          <div>当前指标：{selectedMetrics.length ? selectedMetrics.join("、") : "未确认"}</div>
        </div>

        <div className="flex justify-end">
          <Button className="rounded-2xl px-6" onClick={continueClarification} disabled={!canContinue || isLoading}>
            继续分析
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
