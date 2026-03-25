export function formatCurrency(value: number | null | undefined) {
  return `¥ ${Number(value || 0).toLocaleString("zh-CN")}`
}

export function formatMoneySmart(value: number | null | undefined) {
  const amount = Number(value || 0)
  if (Math.abs(amount) >= 100000000) return `¥ ${(amount / 100000000).toFixed(2)} 亿`
  if (Math.abs(amount) >= 10000) return `¥ ${(amount / 10000).toFixed(2)} 万`
  return formatCurrency(amount)
}

export function formatPercent(value: number | null | undefined) {
  return `${Number(value || 0).toFixed(2)}%`
}

export function formatCompact(value: number | null | undefined) {
  const number = Number(value || 0)
  if (Math.abs(number) >= 100000000) return `${(number / 100000000).toFixed(2)} 亿`
  if (Math.abs(number) >= 10000) return `${(number / 10000).toFixed(1)} 万`
  return number.toLocaleString("zh-CN")
}

export function formatPublishedAt(value: string) {
  return value?.trim() || "日期未标注"
}
