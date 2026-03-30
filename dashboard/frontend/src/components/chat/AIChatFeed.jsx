import React, { useCallback, useState } from 'react'
import { usePolling } from '../../hooks/usePolling'
import { api } from '../../api/client'
import { timeAgo, formatTimeAZ } from '../../utils/time'

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
  human: '#f59e0b',
  system: '#06b6d4',
  crewai: '#f59e0b',
  matrix: '#00bcd4',
}

const PROVIDER_AVATARS = {
  anthropic: 'OA',
  openai: 'GP',
  google: 'GE',
  xai: 'GK',
  ollama: 'OL',
  human: 'ST',
  system: '🧭',
  crewai: '⭐',
  matrix: 'NE',
}

const STRATEGY_MODES = {
  '': { label: 'No Strategy Mode', short: '', color: '#64748b' },
  SIMONS: {
    label: 'SIMONS MODE',
    short: 'Quant',
    color: '#3b82f6',
    desc: 'Pure quant, cold math, conviction 1-10, statistical edge, risk/reward ratio, no emotion, 20-40% cash in uncertainty',
  },
  DRUCKENMILLER: {
    label: 'DRUCKENMILLER MODE',
    short: 'Concentrated',
    color: '#22c55e',
    desc: 'Concentrated bets — what\'s the ONE best trade right now? Go big or stay home',
  },
  PTJ: {
    label: 'PTJ MODE',
    short: '5:1 R/R',
    color: '#ef4444',
    desc: '5:1 risk/reward setups, trend following, cut losers ruthlessly',
  },
  COHEN: {
    label: 'COHEN MODE',
    short: 'Info Edge',
    color: '#a855f7',
    desc: 'Information edge — synthesize all data into one clear view, act fast',
  },
  ONEIL: {
    label: "O'NEIL MODE",
    short: 'CAN SLIM',
    color: '#f97316',
    desc: 'CAN SLIM checklist — does this stock pass all 7 criteria?',
  },
  DALIO: {
    label: 'DALIO MODE',
    short: 'Macro',
    color: '#06b6d4',
    desc: 'What regime are we in? Macro first, then pick stocks',
  },
}

const TRUNCATE_LENGTH = 200

export default function AIChatFeed({ compact = false, warRoom = false }) {
  const [message, setMessage] = useState('')
  const [symbol, setSymbol] = useState('')
  const [strategyMode, setStrategyMode] = useState('')
  const [posting, setPosting] = useState(false)
  const [expandedIds, setExpandedIds] = useState({})

  const fetchChat = useCallback(
    () => warRoom ? api.getWarRoom(compact ? 10 : 50) : api.getRecentChat(compact ? 10 : 50),
    [compact, warRoom]
  )
  const { data: messages, loading, refetch } = usePolling(fetchChat, compact ? 30000 : warRoom ? 30000 : 180000)

  const handlePost = async () => {
    const msg = message.trim()
    if (!msg || posting) return
    setPosting(true)
    try {
      await api.postWarRoomMessage({
        message: msg,
        symbol: symbol.trim().toUpperCase() || undefined,
        strategy_mode: strategyMode || undefined,
      })
      setMessage('')
      refetch()
    } catch (e) {
      console.error('War Room post failed:', e)
    } finally {
      setPosting(false)
    }
  }

  if (loading) return <div className="loading">Loading {warRoom ? 'war room' : 'chat'}...</div>

  const msgs = messages || []

  if (msgs.length === 0 && !warRoom) {
    return <div className="empty-state">No AI chat messages yet. AIs will start chatting after their first trades.</div>
  }

  const activeMode = STRATEGY_MODES[strategyMode]

  return (
    <div className={compact ? '' : 'card'}>
      {!compact && (
        <div className="card-header">
          <h2>{warRoom ? '🔥 War Room' : 'AI Chat Room'}</h2>
          <span className="card-badge">{msgs.length} messages</span>
        </div>
      )}

      {/* War Room input bar */}
      {warRoom && !compact && (
        <div style={{
          padding: '12px 16px',
          borderBottom: '1px solid var(--border)',
          display: 'flex',
          gap: 8,
          alignItems: 'flex-end',
          background: 'var(--bg-hover)',
        }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <label style={labelStyle}>TICKER</label>
            <input
              type="text"
              value={symbol}
              onChange={e => setSymbol(e.target.value.toUpperCase())}
              placeholder="SPY"
              style={{ ...inputStyle, width: 80 }}
              maxLength={6}
            />
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <label style={labelStyle}>STRATEGY</label>
            <select
              value={strategyMode}
              onChange={e => setStrategyMode(e.target.value)}
              style={{
                ...inputStyle,
                width: 170,
                cursor: 'pointer',
                color: activeMode.color,
                fontWeight: strategyMode ? 700 : 400,
              }}
            >
              <option value="">No Mode</option>
              {Object.entries(STRATEGY_MODES).filter(([k]) => k).map(([key, mode]) => (
                <option key={key} value={key}>{mode.label}</option>
              ))}
            </select>
          </div>

          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 4 }}>
            <label style={labelStyle}>
              MESSAGE
              {strategyMode && (
                <span style={{ color: activeMode.color, marginLeft: 8, fontWeight: 400, textTransform: 'none', letterSpacing: 0 }}>
                  — {activeMode.desc}
                </span>
              )}
            </label>
            <div style={{ display: 'flex', gap: 8 }}>
              <input
                type="text"
                value={message}
                onChange={e => setMessage(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handlePost()}
                placeholder={strategyMode ? `Drop your take in ${activeMode.label}...` : 'Drop your hot take...'}
                style={{ ...inputStyle, flex: 1 }}
                maxLength={500}
              />
              <button
                onClick={handlePost}
                disabled={posting || !message.trim()}
                style={{
                  padding: '6px 16px',
                  borderRadius: 6,
                  border: 'none',
                  background: posting ? '#64748b' : 'var(--green)',
                  color: '#fff',
                  fontWeight: 700,
                  fontSize: 12,
                  cursor: posting ? 'not-allowed' : 'pointer',
                  fontFamily: 'monospace',
                  whiteSpace: 'nowrap',
                }}
              >
                {posting ? '...' : '🔥 POST'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Strategy mode tag on active mode */}
      {warRoom && strategyMode && !compact && (
        <div style={{
          padding: '6px 16px',
          background: `${activeMode.color}15`,
          borderBottom: '1px solid var(--border)',
          fontSize: 11,
          fontWeight: 600,
          color: activeMode.color,
          fontFamily: 'monospace',
        }}>
          ⚡ Active: {activeMode.label} — {activeMode.desc}
        </div>
      )}

      <div className="chat-feed" style={{ maxHeight: compact ? 350 : 600 }}>
        {msgs.length === 0 && (
          <div className="empty-state">No war room takes yet. Post your first hot take above!</div>
        )}
        {msgs.map((msg, i) => {
          const isSteve = msg.player_id === 'steve-webull'
          const provider = isSteve ? 'human' : msg.provider
          const modeTag = msg.strategy_mode ? STRATEGY_MODES[msg.strategy_mode] : null
          const msgKey = msg.id || i
          const fullText = warRoom ? msg.take : msg.message
          const isPicard = msg.symbol === 'STRATEGY'
          const isLong = !isPicard && fullText && fullText.length > TRUNCATE_LENGTH
          const isExpanded = isPicard || !!expandedIds[msgKey]
          const displayText = isLong && !isExpanded
            ? fullText.slice(0, TRUNCATE_LENGTH).trimEnd() + '…'
            : fullText
          const expand = (e) => {
            e.stopPropagation()
            setExpandedIds(prev => ({ ...prev, [msgKey]: true }))
          }
          const collapse = (e) => {
            e.stopPropagation()
            setExpandedIds(prev => ({ ...prev, [msgKey]: false }))
          }
          return (
            <div
              key={msgKey}
              className="chat-message"
            >
              <div
                className="chat-avatar"
                style={{ background: PROVIDER_COLORS[provider] || '#666' }}
              >
                {PROVIDER_AVATARS[provider] || '??'}
              </div>
              <div className="chat-content">
                <div className="chat-header-row">
                  <span className="chat-name" style={{ color: PROVIDER_COLORS[provider] || '#ccc' }}>
                    {msg.display_name}
                  </span>
                  {warRoom && msg.symbol && (
                    <span style={{
                      fontSize: 10, fontWeight: 700, padding: '1px 6px',
                      borderRadius: 4, background: 'var(--blue)', color: '#fff',
                      marginLeft: 6,
                    }}>
                      ${msg.symbol}
                    </span>
                  )}
                  {modeTag && (
                    <span style={{
                      fontSize: 10, fontWeight: 700, padding: '1px 6px',
                      borderRadius: 4, background: `${modeTag.color}20`, color: modeTag.color,
                      marginLeft: 4, border: `1px solid ${modeTag.color}40`,
                    }}>
                      {modeTag.short}
                    </span>
                  )}
                  <span className="chat-time">
                    {formatTimeAZ(msg.created_at)} · {timeAgo(msg.created_at)}
                  </span>
                </div>
                <div className="chat-text">{displayText}</div>
                {isLong && !isExpanded && (
                  <span
                    onClick={expand}
                    style={{
                      fontSize: 11,
                      color: 'var(--text-muted)',
                      cursor: 'pointer',
                      userSelect: 'none',
                      marginTop: 2,
                      display: 'inline-block',
                      opacity: 0.7,
                    }}
                    onMouseEnter={e => e.currentTarget.style.opacity = 1}
                    onMouseLeave={e => e.currentTarget.style.opacity = 0.7}
                  >
                    ▼ Read more
                  </span>
                )}
                {isLong && isExpanded && (
                  <span
                    onClick={collapse}
                    style={{
                      fontSize: 11,
                      color: 'var(--text-muted)',
                      cursor: 'pointer',
                      userSelect: 'none',
                      marginTop: 2,
                      display: 'inline-block',
                      opacity: 0.7,
                    }}
                    onMouseEnter={e => e.currentTarget.style.opacity = 1}
                    onMouseLeave={e => e.currentTarget.style.opacity = 0.7}
                  >
                    ▲ Read less
                  </span>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

const labelStyle = {
  fontSize: 10,
  fontWeight: 700,
  color: 'var(--text-muted)',
  textTransform: 'uppercase',
  letterSpacing: 1,
}

const inputStyle = {
  padding: '6px 8px',
  borderRadius: 6,
  background: 'var(--bg-secondary)',
  color: 'var(--text-primary)',
  border: '1px solid var(--border)',
  fontSize: 12,
  fontFamily: 'monospace',
  outline: 'none',
}
