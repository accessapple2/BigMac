import React from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { formatDateTimeAZ } from '../utils/time'

const BRIEFING_TYPE_META = {
  pre_market:  { label: 'Pre-Market Briefing',  icon: '\u2600\uFE0F', color: '#f59e0b' },
  post_open:   { label: 'Opening Update',       icon: '\u{1F514}',    color: '#3b82f6' },
  pre_close:   { label: 'Closing Strategy',     icon: '\u{1F3AF}',    color: '#a855f7' },
  post_close:  { label: 'End of Day Wrap',      icon: '\u{1F319}',    color: '#22c55e' },
}

function parseSections(briefingText) {
  const sections = []
  const lines = briefingText.split('\n')
  let currentSection = null

  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed) continue

    const headerMatch = trimmed.match(/^(?:\*\*)?([A-Z][A-Z\s']+(?:ADVISORY|OUTLOOK|CHECK|REPORT|WATCHLIST|ALERT)S?)(?:\*\*)?[:\s]/)
    if (headerMatch) {
      currentSection = { title: headerMatch[1].trim(), content: trimmed.substring(headerMatch[0].length).trim() ? [trimmed.substring(headerMatch[0].length).trim()] : [] }
      sections.push(currentSection)
    } else if (currentSection) {
      currentSection.content.push(trimmed)
    } else {
      if (!sections.length) sections.push({ title: 'BRIEFING', content: [] })
      sections[0].content.push(trimmed)
    }
  }
  return sections
}

const sectionIcons = {
  'MARKET OUTLOOK': '\u{1F30D}',
  "STEVE'S PORTFOLIO ADVISORY": '\u{1F4BC}',
  'FLOW CHECK': '\u{1F4CA}',
  'AI ARENA REPORT': '\u2694',
  "TODAY'S WATCHLIST": '\u{1F440}',
  'RISK ALERT': '\u26A0',
  'BRIEFING': '\u{1F3AF}',
}

const sectionColors = {
  'MARKET OUTLOOK': '#3b82f6',
  "STEVE'S PORTFOLIO ADVISORY": '#22c55e',
  'FLOW CHECK': '#a855f7',
  'AI ARENA REPORT': '#ef4444',
  "TODAY'S WATCHLIST": '#eab308',
  'RISK ALERT': '#f97316',
  'BRIEFING': '#06b6d4',
}

function BriefingCard({ briefing, defaultOpen = false }) {
  const bt = BRIEFING_TYPE_META[briefing.briefing_type] || BRIEFING_TYPE_META.pre_market
  const sections = parseSections(briefing.briefing)

  return (
    <details open={defaultOpen} style={{ marginBottom: 8 }}>
      <summary style={{
        display: 'flex', alignItems: 'center', gap: 10,
        padding: '12px 16px', cursor: 'pointer',
        background: 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
        borderRadius: 10, border: '1px solid #2a2a4a',
        listStyle: 'none',
      }}>
        <span style={{ fontSize: 18 }}>{bt.icon}</span>
        <span style={{ color: bt.color, fontWeight: 700, fontSize: 13, letterSpacing: 0.5 }}>
          {bt.label.toUpperCase()}
        </span>
        <span style={{ color: '#64748b', fontSize: 11, marginLeft: 'auto' }}>
          {formatDateTimeAZ(briefing.created_at)}
        </span>
      </summary>
      <div style={{
        border: '1px solid #2a2a4a', borderTop: 'none',
        borderRadius: '0 0 10px 10px', overflow: 'hidden',
      }}>
        {sections.map((section, i) => {
          const icon = sectionIcons[section.title] || '\u{1F4CB}'
          const color = sectionColors[section.title] || '#94a3b8'
          return (
            <div key={i} style={{
              padding: '12px 18px',
              borderBottom: i < sections.length - 1 ? '1px solid #1e2336' : 'none',
              background: i % 2 === 0 ? '#0f1020' : '#12132a',
            }}>
              <div style={{
                display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6,
                color, fontWeight: 700, fontSize: 11, letterSpacing: 0.5,
              }}>
                <span>{icon}</span>
                {section.title}
              </div>
              <div style={{ color: '#cbd5e1', fontSize: 12, lineHeight: 1.7 }}>
                {section.content.map((line, j) => (
                  <div key={j} style={{
                    marginBottom: line.startsWith('-') || line.startsWith('\u2022') ? 3 : 5,
                    paddingLeft: line.startsWith('-') || line.startsWith('\u2022') ? 12 : 0,
                  }}>
                    {line}
                  </div>
                ))}
              </div>
            </div>
          )
        })}
      </div>
    </details>
  )
}

export default function CTOAdvisory({ compact = false }) {
  const { data, loading } = usePolling(api.getCTOBriefing, 60000)

  const today = data?.today || []
  const latest = data?.latest
  const history = data?.history || []

  if (loading && !latest) {
    return <div className="empty-state">Loading CTO Advisory...</div>
  }

  if (!latest) {
    return (
      <div style={{
        padding: 20, background: 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
        borderRadius: 12, border: '1px solid #2a2a4a',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
          <span style={{ fontSize: 20 }}>&#x1F3AF;</span>
          <span style={{ color: '#ef4444', fontWeight: 700, fontSize: 14 }}>CTO ADVISORY</span>
          <span style={{ color: '#64748b', fontSize: 11 }}>Grok 4.2</span>
        </div>
        <div style={{ color: '#94a3b8', fontSize: 13 }}>
          No briefings yet today. The CTO advisory runs 4x daily: pre-market (9:00 AM ET),
          post-open (9:45 AM ET), pre-close (3:45 PM ET), post-close (4:15 PM ET).
        </div>
      </div>
    )
  }

  if (compact) {
    const bt = BRIEFING_TYPE_META[latest.briefing_type] || BRIEFING_TYPE_META.pre_market
    const sections = parseSections(latest.briefing)
    const first = sections[0]
    return (
      <div style={{
        padding: '12px 16px', background: 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
        borderRadius: 10, border: '1px solid #2a2a4a',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
          <span style={{ fontSize: 16 }}>{bt.icon}</span>
          <span style={{ color: bt.color, fontWeight: 700, fontSize: 12, letterSpacing: 1 }}>
            CTO: {bt.label.toUpperCase()}
          </span>
          <span style={{ color: '#64748b', fontSize: 10, marginLeft: 'auto' }}>
            {formatDateTimeAZ(latest.created_at)}
          </span>
        </div>
        {first && (
          <div style={{ color: '#e2e8f0', fontSize: 12, lineHeight: 1.5 }}>
            {first.content.slice(0, 3).join(' ')}
          </div>
        )}
        {today.length > 1 && (
          <div style={{ color: '#64748b', fontSize: 10, marginTop: 6 }}>
            {today.length} briefings today
          </div>
        )}
      </div>
    )
  }

  return (
    <div>
      {/* Header */}
      <div style={{
        padding: '14px 20px', marginBottom: 12,
        background: 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
        borderRadius: 12, border: '1px solid #2a2a4a',
        display: 'flex', alignItems: 'center', gap: 10,
      }}>
        <span style={{ fontSize: 22 }}>&#x1F3AF;</span>
        <div>
          <div style={{ color: '#ef4444', fontWeight: 700, fontSize: 15, letterSpacing: 1 }}>
            CTO ADVISORY
          </div>
          <div style={{ color: '#64748b', fontSize: 11 }}>
            Grok 4.2 | {today.length}/4 briefings today
          </div>
        </div>
      </div>

      {/* Today's briefings — most recent first, newest open by default */}
      {today.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ color: '#94a3b8', fontSize: 11, fontWeight: 700, letterSpacing: 1, marginBottom: 8 }}>
            TODAY
          </div>
          {[...today].reverse().map((b, i) => (
            <BriefingCard key={b.created_at} briefing={b} defaultOpen={i === 0} />
          ))}
        </div>
      )}

      {/* Previous days */}
      {history.filter(h => !today.find(t => t.created_at === h.created_at)).length > 0 && (
        <div>
          <div style={{ color: '#64748b', fontSize: 11, fontWeight: 700, letterSpacing: 1, marginBottom: 8 }}>
            PREVIOUS BRIEFINGS
          </div>
          {history
            .filter(h => !today.find(t => t.created_at === h.created_at))
            .map((h, i) => (
              <BriefingCard key={h.created_at + i} briefing={h} defaultOpen={false} />
            ))
          }
        </div>
      )}
    </div>
  )
}
