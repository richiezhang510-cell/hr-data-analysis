import { useEffect, useState } from "react"
import { AlertTriangle, CheckCircle, AlertCircle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import { formatMoneySmart } from "@/lib/formatters"

type MonitorEntry = {
  subject: string
  latest_amount: number
  mom_rate: number | null
  anomaly_count: number
  headcount: number
  severity: "red" | "yellow" | "green"
}

const severityConfig = {
  red: { icon: AlertCircle, label: "高风险", className: "text-red-400 bg-red-500/10 border-red-500/30" },
  yellow: { icon: AlertTriangle, label: "关注", className: "text-amber-400 bg-amber-500/10 border-amber-500/30" },
  green: { icon: CheckCircle, label: "正常", className: "text-emerald-400 bg-emerald-500/10 border-emerald-500/30" },
}

export default function MonitorDashboard() {
  const [items, setItems] = useState<MonitorEntry[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch("/api/monitor")
      .then((res) => res.json())
      .then((data: MonitorEntry[]) => setItems(data))
      .catch(() => setItems([]))
      .finally(() => setLoading(false))
  }, [])

  const redCount = items.filter((i) => i.severity === "red").length
  const yellowCount = items.filter((i) => i.severity === "yellow").length

  return (
    <Card className="rounded-[32px]">
      <CardHeader className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="space-y-1">
          <CardTitle className="text-2xl">异常监控</CardTitle>
          <p className="text-sm text-slate-500">全科目最新月度异常扫描</p>
        </div>
        <div className="flex gap-2">
          {redCount > 0 && (
            <Badge className="rounded-full bg-red-500/10 px-3 py-1 text-red-400">{redCount} 高风险</Badge>
          )}
          {yellowCount > 0 && (
            <Badge className="rounded-full bg-amber-500/10 px-3 py-1 text-amber-400">{yellowCount} 关注</Badge>
          )}
          {redCount === 0 && yellowCount === 0 && (
            <Badge className="rounded-full bg-emerald-500/10 px-3 py-1 text-emerald-400">全部正常</Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="py-12 text-center text-sm text-slate-500">扫描中...</div>
        ) : (
          <div className="space-y-3">
            {items.map((item) => {
              const config = severityConfig[item.severity]
              const Icon = config.icon
              return (
                <div
                  key={item.subject}
                  className={cn(
                    "flex items-center justify-between gap-4 rounded-[20px] border px-5 py-4 transition-colors",
                    config.className,
                  )}
                >
                  <div className="flex items-center gap-4">
                    <Icon className="h-5 w-5 shrink-0" />
                    <div>
                      <div className="text-sm font-semibold">{item.subject}</div>
                      <div className="mt-1 text-xs opacity-70">
                        领取 {item.headcount} 人 · 异常 {item.anomaly_count} 人
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-6 text-right">
                    <div>
                      <div className="text-sm font-semibold">{formatMoneySmart(item.latest_amount)}</div>
                      <div className="text-xs opacity-70">最新月总额</div>
                    </div>
                    <div className="min-w-[80px]">
                      <div className="text-sm font-semibold">
                        {item.mom_rate != null ? `${item.mom_rate >= 0 ? "+" : ""}${item.mom_rate}%` : "--"}
                      </div>
                      <div className="text-xs opacity-70">环比</div>
                    </div>
                    <Badge variant="outline" className={cn("rounded-full px-3 py-1 text-xs", config.className)}>
                      {config.label}
                    </Badge>
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
