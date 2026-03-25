import { useAppStore } from "@/store"

export function StreamingProgress() {
  const streamingMessage = useAppStore((s) => s.streamingMessage)

  if (!streamingMessage) return null

  return (
    <div className="flex items-center gap-2 text-sm text-slate-400 mt-3 animate-pulse" data-testid="streaming-progress">
      <div className="w-4 h-4 border-2 border-neon-cyan border-t-transparent rounded-full animate-spin" />
      <span data-testid="streaming-progress-message">{streamingMessage}</span>
    </div>
  )
}
