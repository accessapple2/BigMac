import React from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

const OUTLOOK_COLORS = { BULLISH: '#22c55e', BEARISH: '#ef4444', NEUTRAL: '#eab308' }
const OUTLOOK_ICONS = { BULLISH: '🟢', BEARISH: '🔴', NEUTRAL: '🟡' }
const ACTION_COLORS = {
  BUY: '#22c55e', ADD: '#22c55e', HOLD: '#94a3b8', TRIM: '#eab308',
  SELL: '#ef4444', CLOSE: '#ef4444', SKIP: '#475569',
}

const COMPARISON_DISPLAY = {
  agree: { icon: '✅', label: 'AGREE', color: '#22c55e' },
  partial: { icon: '⚠️', label: 'PARTIAL', color: '#eab308' },
  disagree: { icon: '⚠️', label: 'DISAGREE', color: '#f97316' },
  opposite: { icon: '❌', label: 'OPPOSITE', color: '#ef4444' },
  skip: { icon: '🟰', label: 'PARTIAL', color: '#64748b' },
  no_data: { icon: '—', label: 'NO DATA', color: '#475569' },
}

function ActionBadge({ action }) {
  if (!action) return <span style={{ color: '#475569' }}>—</span>
  return (
    <span style={{
      color: ACTION_COLORS[action] || '#94a3b8',
      fontWeight: 700,
      fontFamily: 'JetBrains Mono, monospace',
      fontSize: 12,
    }}>
      {action}
    </span>
  )
}

function CrewPollBar({ poll }) {
  if (!poll || poll.total_votes === 0) return null
  const { action_counts, total_votes, consensus_action, consensus_pct } = poll

  return (
    <div style={{ marginTop: 4 }}>
      <div style={{ display: 'flex', gap: 2, height: 6, borderRadius: 3, overflow: 'hidden' }}>
        {Object.entries(action_counts).sort((a, b) => b[1] - a[1]).map(([action, count]) => (
          <div key={action} style={{
            flex: count,
            background: ACTION_COLORS[action] || '#475569',
            opacity: action === consensus_action ? 1 : 0.4,
          }} />
        ))}
      </div>
      <div style={{ fontSize: 10, color: '#64748b', marginTop: 2 }}>
        {consensus_pct}% {consensus_action} ({total_votes} votes)
      </div>
    </div>
  )
}

export default function OfficerConsensus({ compact = false }) {
  const { data, loading, error } = usePolling(api.getConsensus, 60000)

  if (loading && !data) return <div style={{ color: '#64748b', padding: 16 }}>Loading consensus...</div>
  if (error && !data) return null
  if (!data || !data.tickers) return null

  const { market_outlook, tickers, overall_agreement, high_conviction_calls } = data

  // Border color based on agreement
  const borderColor = overall_agreement >= 75 ? '#22c55e'
    : overall_agreement >= 50 ? '#eab308' : '#ef4444'

  const tickerEntries = Object.entries(tickers)

  return (
    <div style={{
      background: '#0f1219',
      border: `2px solid ${borderColor}`,
      borderRadius: 12,
      padding: compact ? 12 : 16,
      marginBottom: 12,
    }}>
      {/* Header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: compact ? 8 : 12,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 18 }}>🖖🤖</span>
          <span style={{ color: '#e2e8f0', fontWeight: 700, fontSize: compact ? 13 : 15 }}>
            Officer Consensus
          </span>
        </div>
        <div style={{
          fontSize: 11, fontWeight: 700, fontFamily: 'JetBrains Mono, monospace',
          color: borderColor,
          padding: '2px 8px', borderRadius: 6,
          background: `${borderColor}15`,
        }}>
          {overall_agreement}% AGREE
        </div>
      </div>

      {/* Market Outlook */}
      {market_outlook && (market_outlook.spock !== 'NEUTRAL' || market_outlook.data !== 'NEUTRAL') && (
        <div style={{
          background: '#1a1f2e', borderRadius: 8, padding: compact ? 8 : 10,
          marginBottom: compact ? 8 : 12,
        }}>
          <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, letterSpacing: 1, marginBottom: 4 }}>
            MARKET OUTLOOK
          </div>
          <div style={{ display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap' }}>
            <span style={{ fontSize: 12 }}>
              🖖 <span style={{ color: OUTLOOK_COLORS[market_outlook.spock] || '#94a3b8', fontWeight: 600 }}>
                {OUTLOOK_ICONS[market_outlook.spock]} {market_outlook.spock}
              </span>
            </span>
            <span style={{ fontSize: 12 }}>
              🤖 <span style={{ color: OUTLOOK_COLORS[market_outlook.data] || '#94a3b8', fontWeight: 600 }}>
                {OUTLOOK_ICONS[market_outlook.data]} {market_outlook.data}
              </span>
            </span>
            <span style={{
              fontSize: 11, color: market_outlook.agree ? '#22c55e' : '#eab308',
              fontWeight: 600,
            }}>
              {market_outlook.agree ? '✅ AGREE' : '⚠️ DISAGREE'}
            </span>
          </div>
        </div>
      )}

      {/* Portfolio Calls */}
      <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, letterSpacing: 1, marginBottom: 6 }}>
        PORTFOLIO CALLS
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: compact ? 2 : 4 }}>
        {tickerEntries.map(([ticker, result]) => {
          const comp = COMPARISON_DISPLAY[result.comparison] || COMPARISON_DISPLAY.no_data
          return (
            <div key={ticker} style={{
              display: 'flex', alignItems: 'center', gap: 8,
              padding: '4px 8px', borderRadius: 6,
              background: result.comparison === 'agree' ? 'rgba(34,197,94,0.05)'
                : result.comparison === 'opposite' ? 'rgba(239,68,68,0.05)' : 'transparent',
              fontSize: 12, fontFamily: 'JetBrains Mono, monospace',
            }}>
              <span style={{ color: '#e2e8f0', fontWeight: 700, width: 48 }}>{ticker}</span>
              <span style={{ width: 70 }}>🖖 <ActionBadge action={result.spock?.action} /></span>
              <span style={{ width: 70 }}>🤖 <ActionBadge action={result.data?.action} /></span>
              <span style={{ color: comp.color, fontWeight: 600, fontSize: 11 }}>
                {comp.icon} {comp.label}
              </span>
              {!compact && result.crew_poll?.total_votes > 0 && (
                <span style={{ color: '#64748b', fontSize: 10, marginLeft: 'auto' }}>
                  {result.crew_poll.total_votes} votes
                </span>
              )}
            </div>
          )
        })}
      </div>

      {/* Summary */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginTop: compact ? 8 : 12, paddingTop: 8,
        borderTop: '1px solid #1e2336',
      }}>
        <span style={{ color: '#94a3b8', fontSize: 11 }}>
          CONSENSUS: {data.agree_count}/{data.total_compared} AGREE ({overall_agreement}%)
        </span>
        {high_conviction_calls && high_conviction_calls.length > 0 && (
          <span style={{ color: '#22c55e', fontSize: 11, fontWeight: 600 }}>
            High conviction: {high_conviction_calls.join(', ')}
          </span>
        )}
      </div>

      {/* Crew Poll detail (full mode only) */}
      {!compact && tickerEntries.some(([, r]) => r.crew_poll?.total_votes >= 3) && (
        <div style={{ marginTop: 12 }}>
          <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, letterSpacing: 1, marginBottom: 8 }}>
            CREW POLL
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 8 }}>
            {tickerEntries
              .filter(([, r]) => r.crew_poll?.total_votes >= 2)
              .map(([ticker, result]) => (
                <div key={ticker} style={{
                  background: '#1a1f2e', borderRadius: 8, padding: 10,
                }}>
                  <div style={{
                    fontWeight: 700, color: '#e2e8f0', fontSize: 13, marginBottom: 6,
                    fontFamily: 'JetBrains Mono, monospace',
                  }}>
                    {ticker}
                  </div>
                  {result.crew_poll.entries.map((entry, i) => (
                    <div key={i} style={{
                      display: 'flex', justifyContent: 'space-between', padding: '2px 0',
                      fontSize: 11,
                    }}>
                      <span style={{ color: '#94a3b8' }}>
                        {entry.emoji} {entry.name}
                      </span>
                      <span style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                        <ActionBadge action={entry.action} />
                        <span style={{ color: '#475569', fontSize: 10, width: 36, textAlign: 'right' }}>
                          ({entry.conviction.toFixed(2)})
                        </span>
                      </span>
                    </div>
                  ))}
                  <CrewPollBar poll={result.crew_poll} />
                  {result.crew_poll.outlier && (
                    <div style={{
                      marginTop: 4, fontSize: 10, color: '#eab308', fontStyle: 'italic',
                    }}>
                      {result.crew_poll.outlier.emoji} {result.crew_poll.outlier.name} is the lone{' '}
                      {result.crew_poll.outlier.action === 'SELL' || result.crew_poll.outlier.action === 'TRIM'
                        ? 'bear' : 'bull'}
                    </div>
                  )}
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  )
}
