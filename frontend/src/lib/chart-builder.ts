import type { ChartConfig } from "@/types"
import { formatCompact } from "@/lib/formatters"

export function isFilteredConsolidatedChart(chart: ChartConfig) {
  const title = chart.chart_title || ""
  return chart.chart_type !== "radar" && chart.chart_type !== "matrix" && !title.includes("解释力") && !title.includes("信号强度矩阵")
}

export function buildChartOption(chart: ChartConfig) {
  const payload = chart.chart_payload || {}
  const type = chart.chart_type
  const base = {
    animationDuration: 300,
    backgroundColor: "transparent",
    textStyle: { fontFamily: "ui-sans-serif, system-ui, sans-serif", color: "#94a3b8" },
    grid: { top: 24, left: 56, right: 24, bottom: 48, containLabel: true },
    tooltip: {
      trigger: "item",
      backgroundColor: "rgba(15, 23, 42, 0.95)",
      borderColor: "rgba(56, 189, 248, 0.2)",
      borderWidth: 1,
      textStyle: { color: "#f8fafc" },
    },
  }

  if (type === "bar" || type === "grouped-bar") {
    const labels = ((payload.labels || payload.categories) as string[]) || []
    const series = (payload.series as number[]) || []
    return {
      ...base,
      xAxis: {
        type: "value",
        axisLabel: {
          color: "#94a3b8",
          formatter: (value: number) => formatCompact(value),
        },
        splitLine: { lineStyle: { color: "rgba(56, 189, 248, 0.08)" } },
      },
      yAxis: {
        type: "category",
        data: labels,
        axisTick: { show: false },
        axisLabel: { color: "#cbd5e1", width: 124, overflow: "truncate" },
      },
      series: [
        {
          type: "bar",
          barWidth: 16,
          data: series,
          itemStyle: {
            borderRadius: [0, 10, 10, 0],
            color: {
              type: "linear",
              x: 0, y: 0, x2: 1, y2: 0,
              colorStops: [
                { offset: 0, color: "rgba(56, 189, 248, 0.6)" },
                { offset: 1, color: "#38bdf8" },
              ],
            },
          },
        },
      ],
    }
  }

  if (type === "line") {
    const periods = (payload.periods as string[]) || []
    const series = (payload.series as number[]) || []
    const isPercent = payload.value_type === "percent"
    return {
      ...base,
      xAxis: {
        type: "category",
        data: periods,
        axisLabel: { color: "#94a3b8", rotate: 30 },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          color: "#94a3b8",
          formatter: (value: number) =>
            isPercent ? `${Number(value || 0).toFixed(2)}%` : formatCompact(value),
        },
        splitLine: { lineStyle: { color: "rgba(56, 189, 248, 0.08)" } },
      },
      series: [
        {
          type: "line",
          smooth: true,
          symbolSize: 6,
          data: series,
          lineStyle: { width: 3, color: "#38bdf8" },
          areaStyle: {
            color: {
              type: "linear",
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: "rgba(56, 189, 248, 0.25)" },
                { offset: 1, color: "rgba(56, 189, 248, 0.02)" },
              ],
            },
          },
          itemStyle: { color: "#38bdf8" },
        },
      ],
    }
  }

  if (type === "scatter") {
    const points = (payload.points as Array<Record<string, number | string>>) || []
    return {
      ...base,
      xAxis: {
        type: "value",
        name: "覆盖率",
        axisLabel: { color: "#94a3b8", formatter: "{value}%" },
        splitLine: { lineStyle: { color: "rgba(56, 189, 248, 0.08)" } },
      },
      yAxis: {
        type: "value",
        name: "均值",
        axisLabel: { color: "#94a3b8", formatter: (value: number) => formatCompact(value) },
        splitLine: { lineStyle: { color: "rgba(56, 189, 248, 0.08)" } },
      },
      series: [
        {
          type: "scatter",
          data: points.map((point) => ({
            name: point.name,
            value: [point.coverage_rate, point.avg_amount, point.employee_count],
          })),
          symbolSize: (value: number[]) => Math.max(12, Math.min(36, Math.sqrt(value[2]) / 4)),
          itemStyle: { color: "rgba(129, 140, 248, 0.8)" },
        },
      ],
    }
  }

  if (type === "radar") {
    return {
      ...base,
      radar: {
        indicator: payload.indicators || [],
        radius: "58%",
        axisName: { color: "#94a3b8" },
        splitLine: { lineStyle: { color: "rgba(56, 189, 248, 0.08)" } },
        splitArea: { areaStyle: { color: ["transparent"] } },
      },
      series: [
        {
          type: "radar",
          data: [
            {
              value: payload.values || [],
              areaStyle: { color: "rgba(56, 189, 248, 0.12)" },
              lineStyle: { color: "#38bdf8", width: 2 },
              itemStyle: { color: "#38bdf8" },
            },
          ],
        },
      ],
    }
  }

  if (type === "pie") {
    const items = (payload.items as Array<Record<string, string | number>>) || []
    const labels = ((payload.labels || payload.categories) as string[]) || []
    const series = (payload.series as number[]) || []
    const pieData: Array<{ name: string; value: number; share?: number }> = items.length > 0
      ? items.map((item) => ({
          name: String(item.name || "未命名"),
          value: Number(item.value || 0),
          share: Number(item.share || 0),
        }))
      : labels.map((name, i) => ({ name, value: Number(series[i] || 0) }))
    const palette = ["#38bdf8", "#22c55e", "#f59e0b", "#a78bfa", "#f97316", "#14b8a6", "#fb7185", "#60a5fa", "#84cc16", "#facc15"]
    return {
      ...base,
      color: palette,
      legend: {
        orient: "vertical",
        right: 8,
        top: "middle",
        icon: "circle",
        textStyle: { color: "#cbd5e1", fontSize: 12 },
        formatter: (name: string) => {
          const item = pieData.find((entry) => entry.name === name)
          if (!item) return name
          const share = typeof item.share === "number" && item.share > 0 ? `${item.share.toFixed(1)}%` : ""
          return share ? `${name}  ${share}` : name
        },
      },
      tooltip: {
        ...base.tooltip,
        trigger: "item",
        formatter: (params: { name: string; value: number; percent: number; data?: { share?: number } }) => {
          const share = params.data?.share ?? params.percent
          return `${params.name}<br/>金额: ${formatCompact(params.value)}<br/>占比: ${Number(share || 0).toFixed(2)}%`
        },
      },
      series: [
        {
          type: "pie",
          radius: ["42%", "72%"],
          center: ["36%", "50%"],
          minAngle: 3,
          avoidLabelOverlap: true,
          itemStyle: { borderRadius: 10, borderColor: "#0f172a", borderWidth: 3 },
          labelLine: { length: 12, length2: 10, lineStyle: { color: "rgba(148, 163, 184, 0.45)" } },
          label: {
            color: "#e2e8f0",
            fontSize: 11,
            formatter: (params: { name: string; percent: number; data?: { share?: number } }) => {
              const share = params.data?.share ?? params.percent
              return `${params.name}
${Number(share || 0).toFixed(1)}%`
            },
          },
          emphasis: {
            scale: true,
            scaleSize: 6,
            itemStyle: { shadowBlur: 24, shadowColor: "rgba(15, 23, 42, 0.35)" },
          },
          data: pieData,
        },
      ],
      graphic: [
        {
          type: "text",
          left: "36%",
          top: "46%",
          style: {
            text: "BU分布",
            textAlign: "center",
            fill: "#cbd5e1",
            fontSize: 14,
            fontWeight: 600,
          },
        },
        {
          type: "text",
          left: "36%",
          top: "53%",
          style: {
            text: `${pieData.length}个BU`,
            textAlign: "center",
            fill: "#64748b",
            fontSize: 11,
          },
        },
      ],
    }
  }

  if (type === "matrix") {
    const rows = (payload.rows as Array<Record<string, number | string>>) || []
    return {
      ...base,
      grid: { top: 16, left: 96, right: 20, bottom: 86, containLabel: true },
      xAxis: {
        type: "category",
        data: ["信号强度", "异常数量", "发现数量"],
        axisLabel: { color: "#94a3b8", interval: 0, margin: 14 },
      },
      yAxis: {
        type: "category",
        data: rows.map((row) => String(row.dimension || "")),
        axisLabel: { color: "#cbd5e1", width: 84, overflow: "break" },
      },
      visualMap: {
        min: 0,
        max: Math.max(...rows.map((row) => Number(row.signal_strength || 0)), 1),
        orient: "horizontal",
        left: "center",
        bottom: 18,
        inRange: { color: ["#0f1629", "#1e3a5f", "#38bdf8"] },
        textStyle: { color: "#94a3b8" },
      },
      series: [
        {
          type: "heatmap",
          data: rows.flatMap((row, index) => [
            [0, index, Number(row.signal_strength || 0)],
            [1, index, Number(row.anomaly_count || 0)],
            [2, index, Number(row.finding_count || 0)],
          ]),
        },
      ],
    }
  }

  return {
    title: {
      text: "暂无可渲染图表",
      left: "center",
      top: "center",
      textStyle: { color: "#64748b", fontSize: 14, fontWeight: 500 },
    },
  }
}
