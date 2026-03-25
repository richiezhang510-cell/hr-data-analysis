import { Download, Copy, Check } from "lucide-react"
import { useState } from "react"
import { Button } from "@/components/ui/button"
import { useAppStore } from "@/store"

export default function ExportButton() {
  const report = useAppStore((s) => s.report)
  const [copied, setCopied] = useState(false)

  if (!report) return null

  async function handleCopy() {
    const sections = report!.report.full_report_sections
    const text = sections
      .map((s) => (typeof s === "string" ? s : s.content || ""))
      .filter(Boolean)
      .join("\n\n")
    const fullText = `${report!.report.report_title}\n${report!.report.report_subtitle}\n\n${text}`
    await navigator.clipboard.writeText(fullText)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  async function handleExportPDF() {
    const { exportReportToPDF } = await import("@/lib/export")
    await exportReportToPDF()
  }

  return (
    <div className="flex gap-2">
      <Button variant="outline" className="rounded-2xl" onClick={() => void handleExportPDF()}>
        <Download className="mr-2 h-4 w-4" />
        导出 PDF
      </Button>
      <Button variant="outline" className="rounded-2xl" onClick={() => void handleCopy()}>
        {copied ? <Check className="mr-2 h-4 w-4" /> : <Copy className="mr-2 h-4 w-4" />}
        {copied ? "已复制" : "复制文本"}
      </Button>
    </div>
  )
}
