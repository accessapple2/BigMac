/**
 * Timezone utilities for TradeMinds dashboard.
 * Backend middleware converts all timestamps to Arizona time (UTC-7) before sending.
 * These helpers just format the already-converted timestamps for display.
 */

const TZ = 'America/Phoenix'

/** Parse a timestamp string — backend sends Arizona time without TZ suffix */
function parseTS(dateStr) {
  if (!dateStr) return null
  const s = dateStr.trim()
  // Create date in local context (already Arizona time from backend)
  // We need to tell JS this is Arizona time, not local browser time
  // Append offset so Date parses correctly regardless of browser timezone
  if (!s.endsWith('Z') && !s.includes('+') && !s.includes('-', 11)) {
    return s.replace(' ', 'T') + '-07:00' // Arizona = UTC-7
  }
  return s
}

/** Relative time string ("5m ago", "2h ago", etc.) */
export function timeAgo(dateStr) {
  if (!dateStr) return ''
  const ts = parseTS(dateStr)
  const diff = (Date.now() - new Date(ts).getTime()) / 1000
  if (diff < 0) return 'just now'
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

/** Format as Arizona time: "8:13 AM" */
export function formatTimeAZ(dateStr) {
  if (!dateStr) return ''
  const ts = parseTS(dateStr)
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ''
  return d.toLocaleTimeString('en-US', {
    timeZone: TZ,
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  })
}

/** Format as Arizona date + time: "Mar 13, 8:13 AM" */
export function formatDateTimeAZ(dateStr) {
  if (!dateStr) return ''
  const ts = parseTS(dateStr)
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ''
  return d.toLocaleString('en-US', {
    timeZone: TZ,
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  })
}
