import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

function formatDate(iso) {
  if (!iso) return null
  try {
    const d = new Date(iso)
    return d.toLocaleString('en-US', {
      weekday: 'long', month: 'long', day: 'numeric',
      hour: '2-digit', minute: '2-digit', timeZoneName: 'short',
    })
  } catch { return iso }
}

// Parse the structured briefing sections from Picard's format
function parseBriefing(text) {
  if (!text) return []

  const SECTION_HEADS = [
    'STRATEGIC THESIS',
    'SECTOR DIRECTIVES',
    'FLEET ORDERS',
    'HISTORICAL PARALLEL',
    "CAPTAIN'S NOTE",
    'CAPTAIN\'S NOTE',
  ]

  const SECTION_ICONS = {
    'STRATEGIC THESIS':    { icon: '🌌', color: '#60a5fa' },
    'SECTOR DIRECTIVES':   { icon: '📊', color: '#a78bfa' },
    'FLEET ORDERS':        { icon: '⚡', color: '#f97316' },
    'HISTORICAL PARALLEL': { icon: '📜', color: '#fbbf24' },
    "CAPTAIN'S NOTE":      { icon: '⭐', color: '#4ade80' },
  }

  const lines = text.split('\n')
  const sections = []
  let current = null
  let preamble = []
  let inPreamble = true

  for (const raw of lines) {
    const line = raw.trim()
    if (!line) continue

    // Skip the title line (ADMIRAL PICARD / Stardate)
    if (line.includes('ADMIRAL PICARD') || line.startsWith('Stardate') || line.startsWith('⭐ ADMIRAL')) {
      continue
    }

    // Check if line is a section header
    const matchedHead = SECTION_HEADS.find(h =>
      line.toUpperCase().includes(h) && line.length < h.length + 10
    )

    if (matchedHead) {
      inPreamble = false
      const meta = SECTION_ICONS[matchedHead] || { icon: '📋', color: '#94a3b8' }
      current = { title: matchedHead, icon: meta.icon, color: meta.color, lines: [] }
      sections.push(current)
    } else if (inPreamble) {
      preamble.push(line)
    } else if (current) {
      current.lines.push(line)
    }
  }

  // If parsing found nothing structured, treat as single block
  if (sections.length === 0) {
    return [{ title: 'BRIEFING', icon: '⭐', color: '#94a3b8', lines: text.split('\n').filter(l => l.trim()) }]
  }

  // Prepend preamble as an intro block if any
  if (preamble.length > 0) {
    sections.unshift({ title: 'OVERVIEW', icon: '🌌', color: '#60a5fa', lines: preamble })
  }

  return sections
}

function BriefingSection({ section }) {
  return (
    <div style={{
      marginBottom: 20,
      borderLeft: `3px solid ${section.color}`,
      paddingLeft: 16,
    }}>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        marginBottom: 10, fontSize: 12, fontWeight: 700,
        color: section.color, letterSpacing: 1,
      }}>
        <span style={{ fontSize: 16 }}>{section.icon}</span>
        {section.title}
      </div>
      <div style={{ fontSize: 13, lineHeight: 1.8, color: '#cbd5e1' }}>
        {section.lines.map((line, i) => {
          const isBullet = line.startsWith('-') || line.startsWith('•') || line.startsWith('*')
          const isBold = line.startsWith('**') || (line.includes('FOCUS:') || line.includes('AVOID:') || line.includes('WATCH:'))
          return (
            <div key={i} style={{
              marginBottom: isBullet ? 6 : 8,
              paddingLeft: isBullet ? 8 : 0,
              color: isBold ? '#e2e8f0' : '#cbd5e1',
              fontWeight: isBold ? 600 : 400,
            }}>
              {line.replace(/\*\*/g, '')}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function Skeleton() {
  return (
    <div style={{ padding: '24px 28px' }}>
      {[200, 140, 180, 120, 90].map((w, i) => (
        <div key={i} style={{ marginBottom: 20 }}>
          <div style={{ width: 120, height: 12, background: '#1e293b', borderRadius: 3, marginBottom: 10 }} />
          {[w, w - 30, w + 20, w - 10].map((bw, j) => (
            <div key={j} style={{ width: `${Math.min(bw, 400)}px`, maxWidth: '90%', height: 10, background: '#1e293b', borderRadius: 3, marginBottom: 6 }} />
          ))}
        </div>
      ))}
    </div>
  )
}

export default function ReadyRoom() {
  const [generating, setGenerating] = useState(false)
  const [genResult, setGenResult] = useState(null)
  const { data, loading, refresh } = usePolling(api.getPicardStrategy, 300000)

  const briefing = data?.briefing
  const generatedAt = data?.generated_at
  const sections = parseBriefing(briefing)
  const isLoading = loading && !data

  async function handleRefresh() {
    setGenerating(true)
    setGenResult(null)
    try {
      const r = await api.generatePicardBriefing()
      setGenResult(r.ok ? 'success' : 'error')
      if (r.ok && refresh) refresh()
    } catch {
      setGenResult('error')
    } finally {
      setTimeout(() => setGenerating(false), 2000)
    }
  }

  return (
    <div style={{ maxWidth: 860, margin: '0 auto' }}>
      {/* Header panel */}
      <div style={{
        background: 'linear-gradient(135deg, #0a0f1e 0%, #0d1528 50%, #0a0f1e 100%)',
        border: '1px solid #1e3a5f',
        borderRadius: 12,
        padding: '20px 28px',
        marginBottom: 16,
        display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{
            width: 48, height: 48, borderRadius: '50%',
            background: 'linear-gradient(135deg, #1e3a5f, #2563eb)',
            border: '2px solid #3b82f6',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 22,
          }}>
            ⭐
          </div>
          <div>
            <div style={{ fontSize: 16, fontWeight: 800, color: '#e2e8f0', letterSpacing: 1 }}>
              ADMIRAL PICARD
            </div>
            <div style={{ fontSize: 11, color: '#64748b', letterSpacing: 1 }}>
              FLEET COMMANDER · READY ROOM · USS TRADEMINDS
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {generatedAt && (
            <div style={{ fontSize: 10, color: '#475569', textAlign: 'right' }}>
              <div style={{ color: '#64748b', marginBottom: 2 }}>LAST BRIEFING</div>
              {formatDate(generatedAt)}
            </div>
          )}
          <button
            onClick={handleRefresh}
            disabled={generating}
            style={{
              padding: '8px 16px', borderRadius: 8, fontSize: 11, fontWeight: 700,
              cursor: generating ? 'not-allowed' : 'pointer',
              background: generating ? '#1e293b' : '#0f2040',
              color: generating ? '#475569' : '#3b82f6',
              border: `1px solid ${generating ? '#334155' : '#2563eb'}`,
              opacity: generating ? 0.7 : 1,
              whiteSpace: 'nowrap',
            }}
          >
            {generating ? 'Generating…' : '↻ Refresh Briefing'}
          </button>
        </div>
      </div>

      {genResult === 'success' && (
        <div style={{ padding: '8px 16px', marginBottom: 12, background: '#052e16', border: '1px solid #16a34a', borderRadius: 6, fontSize: 12, color: '#4ade80' }}>
          New briefing generated. Refreshing data…
        </div>
      )}
      {genResult === 'error' && (
        <div style={{ padding: '8px 16px', marginBottom: 12, background: '#2e0505', border: '1px solid #991b1b', borderRadius: 6, fontSize: 12, color: '#f87171' }}>
          Generation failed. Ollama may be busy or the model is unloaded.
        </div>
      )}

      {/* Briefing body */}
      <div style={{
        background: 'linear-gradient(180deg, #0a0f1e 0%, #080d1a 100%)',
        border: '1px solid #1e2a3a',
        borderRadius: 12,
        overflow: 'hidden',
      }}>
        {/* Stardate banner */}
        {briefing && (
          <div style={{
            padding: '10px 28px',
            background: 'linear-gradient(90deg, #0d1528, #0a0f1e)',
            borderBottom: '1px solid #1e2a3a',
            display: 'flex', alignItems: 'center', gap: 10,
          }}>
            <span style={{ fontSize: 10, fontFamily: 'monospace', color: '#475569', letterSpacing: 2 }}>
              STARFLEET COMMAND · EYES ONLY · STRATEGIC BRIEFING
            </span>
          </div>
        )}

        {isLoading ? (
          <Skeleton />
        ) : !briefing ? (
          <div style={{ padding: '40px 28px', textAlign: 'center' }}>
            <div style={{ fontSize: 40, marginBottom: 16 }}>⭐</div>
            <div style={{ color: '#94a3b8', fontSize: 14, marginBottom: 8 }}>
              No briefing on file.
            </div>
            <div style={{ color: '#64748b', fontSize: 12, marginBottom: 20 }}>
              Admiral Picard generates his strategic overview every Sunday at 10:00 PM MST.
              You can request an immediate briefing below.
            </div>
            <button
              onClick={handleRefresh}
              disabled={generating}
              style={{
                padding: '10px 24px', borderRadius: 8, fontSize: 12, fontWeight: 700,
                cursor: generating ? 'not-allowed' : 'pointer',
                background: '#0f2040', color: '#3b82f6',
                border: '1px solid #2563eb',
              }}
            >
              {generating ? 'Generating… (takes ~60s)' : 'Request Briefing Now'}
            </button>
          </div>
        ) : (
          <div style={{ padding: '24px 28px' }}>
            {sections.map((s, i) => <BriefingSection key={i} section={s} />)}
          </div>
        )}
      </div>
    </div>
  )
}
