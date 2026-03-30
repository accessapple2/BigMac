import React, { useState, useEffect, useRef, useCallback } from 'react'

const MODEL_COLORS = {
  'claude-sonnet': '#22c55e',
  'claude-haiku': '#16a34a',
  'gpt-4o': '#22c55e',
  'gpt-o3': '#16a34a',
  'gemini-2.5-pro': '#3b82f6',
  'gemini-2.5-flash': '#60a5fa',
  'grok-3': '#ef4444',
  'grok-4': '#f97316',
  'ollama-local': '#94a3b8',
  'ollama-llama': '#78716c',
  'ollama-deepseek': '#a3a3a3',
  'ollama-qwen3': '#737373',
  'ollama-gemma27b': '#6b7280',
  'dayblade-0dte': '#f59e0b',
  'steve-webull': '#fbbf24',
}

const PROVIDER_ABBREV = {
  anthropic: 'OA', openai: 'GP', google: 'GE', xai: 'GK',
  ollama: 'OL', dayblade: 'DB', webull: 'WB',
}

// Simple audio synthesis for notifications
function playSound(type) {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)()
    const osc = ctx.createOscillator()
    const gain = ctx.createGain()
    osc.connect(gain)
    gain.connect(ctx.destination)

    if (type === 'buy') {
      // Cash register cha-ching: two quick high notes
      osc.frequency.setValueAtTime(1200, ctx.currentTime)
      osc.frequency.setValueAtTime(1600, ctx.currentTime + 0.08)
      osc.frequency.setValueAtTime(2000, ctx.currentTime + 0.16)
      gain.gain.setValueAtTime(0.08, ctx.currentTime)
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3)
      osc.start(ctx.currentTime)
      osc.stop(ctx.currentTime + 0.3)
    } else {
      // Soft click for sell
      osc.frequency.setValueAtTime(400, ctx.currentTime)
      osc.frequency.exponentialRampToValueAtTime(200, ctx.currentTime + 0.15)
      gain.gain.setValueAtTime(0.06, ctx.currentTime)
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.2)
      osc.start(ctx.currentTime)
      osc.stop(ctx.currentTime + 0.2)
    }
  } catch (e) {
    // Audio not available
  }
}

function formatTradeText(trade) {
  const isBuy = trade.action?.startsWith('BUY')
  const isOption = trade.asset_type === 'option'
  const icon = isBuy ? '\u{1F7E2}' : '\u{1F534}'
  const name = trade.display_name || trade.player_id
  let detail = `${trade.symbol}`

  if (isOption && trade.option_type) {
    const type = trade.option_type.toUpperCase()
    const strike = trade.strike_price ? ` $${Number(trade.strike_price).toFixed(0)}` : ''
    const expiry = trade.expiry_date ? ` exp ${trade.expiry_date.slice(5)}` : ''
    detail = `${trade.action.replace('_', ' ')} ${trade.symbol}${strike}${expiry}`
  } else {
    detail = `${trade.action} ${trade.symbol}`
  }

  const qty = Number(trade.qty).toFixed(trade.qty >= 10 ? 0 : 2)
  const price = Number(trade.price).toFixed(2)
  const reasoning = (trade.reasoning || '').replace(/^\[.*?\]\s*/, '').slice(0, 80)

  return { icon, name, detail, qty, price, reasoning, isBuy }
}

export default function TradeNotifications({ onTradeClick }) {
  const [toasts, setToasts] = useState([])
  const [history, setHistory] = useState([])
  const [showBell, setShowBell] = useState(false)
  const [muted, setMuted] = useState(() => localStorage.getItem('tm_muted') === '1')
  const [unreadCount, setUnreadCount] = useState(0)
  const lastTradeIdRef = useRef(null)
  const initialLoadRef = useRef(true)

  const toggleMute = useCallback(() => {
    setMuted(prev => {
      const next = !prev
      localStorage.setItem('tm_muted', next ? '1' : '0')
      return next
    })
  }, [])

  // Poll for new trades
  useEffect(() => {
    let mounted = true

    async function check() {
      try {
        const res = await fetch('/api/trades/recent?limit=5')
        const trades = await res.json()
        if (!mounted || !trades || trades.length === 0) return

        const latestId = trades[0].executed_at + trades[0].player_id + trades[0].symbol

        // Skip initial load (don't toast old trades)
        if (initialLoadRef.current) {
          initialLoadRef.current = false
          lastTradeIdRef.current = latestId
          return
        }

        if (latestId === lastTradeIdRef.current) return
        lastTradeIdRef.current = latestId

        // Find new trades (ones we haven't seen)
        const newTrades = []
        for (const t of trades) {
          const tid = t.executed_at + t.player_id + t.symbol
          if (history.some(h => h.id === tid)) break
          newTrades.push({ ...t, id: tid, timestamp: Date.now() })
        }

        if (newTrades.length === 0) return

        // Add to history (max 20)
        setHistory(prev => [...newTrades, ...prev].slice(0, 20))
        setUnreadCount(prev => prev + newTrades.length)

        // Update page title
        document.title = `TradeMinds (${newTrades.length})`
        setTimeout(() => { document.title = 'TradeMinds' }, 10000)

        // Show toasts
        setToasts(prev => [...newTrades.map(t => ({ ...t, fadeOut: false })), ...prev])

        // Play sound for first trade
        if (!muted) {
          const isBuy = newTrades[0].action?.startsWith('BUY')
          playSound(isBuy ? 'buy' : 'sell')
        }

        // Auto-remove toasts after 8 seconds
        newTrades.forEach(t => {
          setTimeout(() => {
            setToasts(prev => prev.map(toast =>
              toast.id === t.id ? { ...toast, fadeOut: true } : toast
            ))
          }, 7000)
          setTimeout(() => {
            setToasts(prev => prev.filter(toast => toast.id !== t.id))
          }, 8000)
        })
      } catch (e) {
        // Network error, skip
      }
    }

    const interval = setInterval(check, 15000)
    check() // Initial check
    return () => { mounted = false; clearInterval(interval) }
  }, [muted, history])

  const color = (trade) => MODEL_COLORS[trade.player_id] || '#94a3b8'

  return (
    <>
      {/* Bell icon in header */}
      <div className="notif-bell-wrapper" style={{ position: 'relative', display: 'inline-flex', alignItems: 'center', gap: 8 }}>
        <button
          className="notif-mute-btn"
          onClick={toggleMute}
          title={muted ? 'Unmute sounds' : 'Mute sounds'}
          style={{
            background: 'none', border: 'none', cursor: 'pointer', fontSize: 16,
            color: muted ? '#ef4444' : '#94a3b8', padding: '4px',
          }}
        >
          {muted ? '\u{1F507}' : '\u{1F50A}'}
        </button>
        <button
          className="notif-bell-btn"
          onClick={() => { setShowBell(!showBell); setUnreadCount(0) }}
          style={{
            background: 'none', border: 'none', cursor: 'pointer', fontSize: 18,
            color: '#e2e8f0', position: 'relative', padding: '4px',
          }}
        >
          {'\u{1F514}'}
          {unreadCount > 0 && (
            <span style={{
              position: 'absolute', top: -4, right: -6,
              background: '#ef4444', color: '#fff', fontSize: 10, fontWeight: 700,
              borderRadius: '50%', width: 18, height: 18, display: 'flex',
              alignItems: 'center', justifyContent: 'center',
            }}>
              {unreadCount > 9 ? '9+' : unreadCount}
            </span>
          )}
        </button>

        {/* Bell dropdown */}
        {showBell && (
          <div style={{
            position: 'absolute', top: 32, right: 0, width: 360,
            background: '#1a1f2e', border: '1px solid #2d3348', borderRadius: 10,
            boxShadow: '0 8px 32px rgba(0,0,0,.5)', zIndex: 9999, maxHeight: 420,
            overflowY: 'auto',
          }}>
            <div style={{ padding: '10px 14px', borderBottom: '1px solid #2d3348', color: '#94a3b8', fontSize: 12, fontWeight: 600 }}>
              RECENT NOTIFICATIONS ({history.length})
            </div>
            {history.length === 0 ? (
              <div style={{ padding: 20, color: '#64748b', textAlign: 'center', fontSize: 13 }}>
                No trade notifications yet
              </div>
            ) : (
              history.map(trade => {
                const t = formatTradeText(trade)
                return (
                  <div key={trade.id} style={{
                    padding: '8px 14px', borderBottom: '1px solid #1e2336',
                    display: 'flex', gap: 8, alignItems: 'flex-start', cursor: 'pointer',
                  }} onClick={() => { setShowBell(false); onTradeClick?.() }}>
                    <span style={{
                      width: 28, height: 28, borderRadius: '50%', display: 'flex',
                      alignItems: 'center', justifyContent: 'center', fontSize: 10,
                      fontWeight: 700, flexShrink: 0, color: '#fff',
                      background: color(trade),
                    }}>
                      {PROVIDER_ABBREV[trade.provider] || '??'}
                    </span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 12, fontWeight: 600 }}>
                        <span style={{ color: color(trade) }}>{t.name}</span>
                        <span style={{ color: t.isBuy ? '#22c55e' : '#ef4444', marginLeft: 6 }}>{t.detail}</span>
                        <span style={{ color: '#94a3b8', marginLeft: 4 }}>x{t.qty} @ ${t.price}</span>
                      </div>
                      <div style={{ fontSize: 11, color: '#64748b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {t.reasoning}
                      </div>
                    </div>
                  </div>
                )
              })
            )}
          </div>
        )}
      </div>

      {/* Toast notifications — bottom right */}
      <div style={{
        position: 'fixed', bottom: 20, right: 20, zIndex: 10000,
        display: 'flex', flexDirection: 'column-reverse', gap: 8, pointerEvents: 'none',
      }}>
        {toasts.slice(0, 5).map(trade => {
          const t = formatTradeText(trade)
          return (
            <div key={trade.id} style={{
              pointerEvents: 'auto',
              background: '#1a1f2e', border: `2px solid ${color(trade)}`,
              borderRadius: 10, padding: '10px 14px', minWidth: 320, maxWidth: 420,
              boxShadow: `0 4px 24px rgba(0,0,0,.6), 0 0 8px ${color(trade)}33`,
              opacity: trade.fadeOut ? 0 : 1,
              transform: trade.fadeOut ? 'translateX(100px)' : 'translateX(0)',
              transition: 'opacity 0.5s ease, transform 0.5s ease',
              animation: trade.fadeOut ? 'none' : 'slideIn 0.3s ease',
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{
                  width: 26, height: 26, borderRadius: '50%', display: 'flex',
                  alignItems: 'center', justifyContent: 'center', fontSize: 9,
                  fontWeight: 700, color: '#fff', background: color(trade), flexShrink: 0,
                }}>
                  {PROVIDER_ABBREV[trade.provider] || '??'}
                </span>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 13, fontWeight: 600 }}>
                    <span>{t.icon} </span>
                    <span style={{ color: color(trade) }}>{t.name}</span>
                    <span style={{ color: '#94a3b8' }}> — </span>
                    <span style={{ color: t.isBuy ? '#22c55e' : '#ef4444', fontWeight: 700 }}>{t.detail}</span>
                  </div>
                  <div style={{ fontSize: 12, color: '#e2e8f0' }}>
                    x{t.qty} @ ${t.price}
                  </div>
                </div>
                <button
                  onClick={() => { onTradeClick?.(); setToasts(prev => prev.filter(x => x.id !== trade.id)) }}
                  style={{
                    background: '#2d3348', border: 'none', color: '#94a3b8', fontSize: 10,
                    padding: '3px 8px', borderRadius: 4, cursor: 'pointer', fontWeight: 600,
                  }}
                >
                  View
                </button>
              </div>
              {t.reasoning && (
                <div style={{
                  fontSize: 11, color: '#64748b', marginTop: 4,
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}>
                  {t.reasoning}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* Slide-in animation */}
      <style>{`
        @keyframes slideIn {
          from { opacity: 0; transform: translateX(100px); }
          to { opacity: 1; transform: translateX(0); }
        }
      `}</style>
    </>
  )
}
