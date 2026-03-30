import React, { useState, useEffect, useRef } from 'react'
import { api } from '../api/client'
import { formatTimeAZ, formatDateTimeAZ } from '../utils/time'

const COLORS = [
  '#00d4aa', '#ff6b6b', '#4ecdc4', '#ffe66d', '#a29bfe',
  '#fd79a8', '#00b894', '#e17055', '#6c5ce7', '#74b9ff',
  '#55efc4', '#fab1a0', '#81ecec', '#dfe6e9',
]

function EquityCurveChart({ results }) {
  const canvasRef = useRef(null)

  useEffect(() => {
    if (!canvasRef.current || !results) return
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    const dpr = window.devicePixelRatio || 1

    canvas.width = canvas.offsetWidth * dpr
    canvas.height = canvas.offsetHeight * dpr
    ctx.scale(dpr, dpr)

    const W = canvas.offsetWidth
    const H = canvas.offsetHeight
    const pad = { top: 20, right: 20, bottom: 30, left: 60 }

    ctx.clearRect(0, 0, W, H)
    ctx.fillStyle = '#1a1a2e'
    ctx.fillRect(0, 0, W, H)

    // Collect all equity curves
    const entries = Object.entries(results).filter(([, r]) => {
      const eq = r.equity_curve || r.equity_json
      return eq && eq.length > 1
    })
    if (!entries.length) {
      ctx.fillStyle = '#666'
      ctx.font = '14px monospace'
      ctx.fillText('No equity curve data', W / 2 - 80, H / 2)
      return
    }

    let allValues = []
    entries.forEach(([, r]) => {
      const eq = r.equity_curve || r.equity_json || []
      eq.forEach(p => allValues.push(p.value))
    })

    const minV = Math.min(...allValues) * 0.999
    const maxV = Math.max(...allValues) * 1.001
    const maxLen = Math.max(...entries.map(([, r]) => (r.equity_curve || r.equity_json || []).length))

    const xScale = (W - pad.left - pad.right) / Math.max(maxLen - 1, 1)
    const yScale = (H - pad.top - pad.bottom) / (maxV - minV || 1)

    // Grid
    ctx.strokeStyle = '#2a2a4a'
    ctx.lineWidth = 0.5
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + (H - pad.top - pad.bottom) * i / 4
      ctx.beginPath()
      ctx.moveTo(pad.left, y)
      ctx.lineTo(W - pad.right, y)
      ctx.stroke()

      const val = maxV - (maxV - minV) * i / 4
      ctx.fillStyle = '#666'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.fillText('$' + val.toFixed(0), pad.left - 5, y + 3)
    }

    // Baseline ($10,000)
    const baseY = pad.top + (maxV - 10000) * yScale
    if (baseY > pad.top && baseY < H - pad.bottom) {
      ctx.strokeStyle = '#444'
      ctx.setLineDash([4, 4])
      ctx.beginPath()
      ctx.moveTo(pad.left, baseY)
      ctx.lineTo(W - pad.right, baseY)
      ctx.stroke()
      ctx.setLineDash([])
    }

    // Draw each model's curve
    entries.forEach(([pid, r], idx) => {
      const eq = r.equity_curve || r.equity_json || []
      const color = COLORS[idx % COLORS.length]
      ctx.strokeStyle = color
      ctx.lineWidth = 2
      ctx.beginPath()
      eq.forEach((p, i) => {
        const x = pad.left + i * xScale
        const y = pad.top + (maxV - p.value) * yScale
        if (i === 0) ctx.moveTo(x, y)
        else ctx.lineTo(x, y)
      })
      ctx.stroke()
    })

    // Legend
    entries.forEach(([pid, r], idx) => {
      const color = COLORS[idx % COLORS.length]
      const name = r.display_name || pid
      const x = pad.left + 10
      const y = pad.top + 15 + idx * 16
      ctx.fillStyle = color
      ctx.fillRect(x, y - 6, 12, 3)
      ctx.fillStyle = '#ccc'
      ctx.font = '11px monospace'
      ctx.textAlign = 'left'
      ctx.fillText(name, x + 18, y)
    })
  }, [results])

  return <canvas ref={canvasRef} style={{ width: '100%', height: 300, borderRadius: 8 }} />
}

export default function BacktestLab() {
  const [models, setModels] = useState([])
  const [selectedModels, setSelectedModels] = useState([])
  const [date, setDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [multiDay, setMultiDay] = useState(false)
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(0)
  const [progressMsg, setProgressMsg] = useState('')
  const [results, setResults] = useState(null)
  const [activeRunId, setActiveRunId] = useState(null)
  const [pastRuns, setPastRuns] = useState([])
  const [rankings, setRankings] = useState([])
  const [tab, setTab] = useState('run') // run, history, rankings, timemachine
  const [tradeLogModel, setTradeLogModel] = useState(null)
  const pollRef = useRef(null)

  // Time Machine state
  const [tmPlayer, setTmPlayer] = useState('')
  const [tmUseRange, setTmUseRange] = useState(false)
  const [tmDays, setTmDays] = useState(30)
  const [tmStartDate, setTmStartDate] = useState('')
  const [tmEndDate, setTmEndDate] = useState('')
  const [tmRunning, setTmRunning] = useState(false)
  const [tmResult, setTmResult] = useState(null)
  const [tmError, setTmError] = useState('')

  // Default date = last weekday
  useEffect(() => {
    const d = new Date()
    d.setDate(d.getDate() - 1)
    while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() - 1)
    setDate(d.toISOString().split('T')[0])
    setEndDate(d.toISOString().split('T')[0])
  }, [])

  useEffect(() => {
    api.getBacktestModels().then(setModels).catch(() => {})
    api.getBacktestRuns().then(setPastRuns).catch(() => {})
    api.getBacktestRankings().then(setRankings).catch(() => {})
  }, [])

  const toggleModel = (id) => {
    setSelectedModels(prev =>
      prev.includes(id) ? prev.filter(m => m !== id) : [...prev, id]
    )
  }

  const selectAll = () => {
    setSelectedModels(selectedModels.length === models.length ? [] : models.map(m => m.id))
  }

  const runBacktest = async () => {
    if (!date || !selectedModels.length) return
    setRunning(true)
    setProgress(0)
    setProgressMsg('Starting...')
    setResults(null)

    try {
      const payload = { date, model_ids: selectedModels }
      if (multiDay && endDate && endDate !== date) {
        payload.end_date = endDate
      }
      const { run_id, error } = await api.runBacktest(payload)
      if (error) { setProgressMsg(error); setRunning(false); return }

      setActiveRunId(run_id)

      // Poll for status
      pollRef.current = setInterval(async () => {
        try {
          const st = await api.getBacktestStatus(run_id)
          setProgress(st.progress || 0)
          setProgressMsg(st.message || '')

          if (st.status === 'complete') {
            clearInterval(pollRef.current)
            setRunning(false)
            setResults(st.results)
            // Refresh runs list and rankings
            api.getBacktestRuns().then(setPastRuns).catch(() => {})
            api.getBacktestRankings().then(setRankings).catch(() => {})
          } else if (st.status === 'error') {
            clearInterval(pollRef.current)
            setRunning(false)
            setProgressMsg('Error: ' + st.message)
          }
        } catch (e) {
          // continue polling
        }
      }, 2000)
    } catch (e) {
      setProgressMsg('Failed: ' + e.message)
      setRunning(false)
    }
  }

  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [])

  const runTimeMachine = async () => {
    if (!tmPlayer) return
    setTmRunning(true)
    setTmError('')
    setTmResult(null)
    try {
      const opts = tmUseRange && tmStartDate
        ? { startDate: tmStartDate, endDate: tmEndDate || undefined }
        : { days: tmDays }
      const data = await api.timeMachine(tmPlayer, opts)
      if (data.error) { setTmError(data.error); setTmRunning(false); return }
      setTmResult(data)
    } catch (e) {
      setTmError('Failed: ' + e.message)
    }
    setTmRunning(false)
  }

  const loadRun = async (runId) => {
    try {
      const data = await api.getBacktestRunDetail(runId)
      // Group by player_id
      const grouped = {}
      data.forEach(r => {
        if (!grouped[r.player_id]) {
          grouped[r.player_id] = { ...r, equity_curve: r.equity_curve || [] }
        }
      })
      setResults(grouped)
      setTab('run')
    } catch (e) {
      console.error(e)
    }
  }

  // Parse results for display
  const resultEntries = results ? (
    results.days
      ? Object.entries(results.cumulative || {})
      : Object.entries(results)
  ) : []

  const singleDayResults = results && !results.days ? results : null

  return (
    <div className="backtest-lab">
      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        {['run', 'timemachine', 'history', 'rankings'].map(t => (
          <button
            key={t}
            className={`btn ${tab === t ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setTab(t)}
            style={{
              padding: '8px 20px', borderRadius: 6,
              background: tab === t ? '#00d4aa' : '#2a2a4a',
              color: tab === t ? '#000' : '#ccc', border: 'none', cursor: 'pointer',
              fontWeight: tab === t ? 700 : 400,
            }}
          >
            {t === 'run' ? 'Run Backtest' : t === 'timemachine' ? 'Time Machine' : t === 'history' ? 'Past Runs' : 'Model Rankings'}
          </button>
        ))}
      </div>

      {/* === RUN TAB === */}
      {tab === 'run' && (
        <>
          <div className="card" style={{ padding: 20, marginBottom: 16 }}>
            <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Configuration</h3>

            <div style={{ display: 'flex', gap: 16, alignItems: 'end', flexWrap: 'wrap', marginBottom: 16 }}>
              <div>
                <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>
                  {multiDay ? 'Start Date' : 'Date'}
                </label>
                <input type="date" value={date} onChange={e => setDate(e.target.value)}
                  style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }} />
              </div>

              {multiDay && (
                <div>
                  <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>End Date</label>
                  <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)}
                    style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }} />
                </div>
              )}

              <label style={{ display: 'flex', alignItems: 'center', gap: 6, color: '#ccc', cursor: 'pointer' }}>
                <input type="checkbox" checked={multiDay} onChange={e => setMultiDay(e.target.checked)} />
                Multi-day range
              </label>

              <button
                onClick={runBacktest}
                disabled={running || !selectedModels.length}
                style={{
                  padding: '10px 24px', borderRadius: 6, border: 'none', cursor: 'pointer',
                  background: running ? '#555' : '#00d4aa', color: '#000', fontWeight: 700, fontSize: 14,
                }}
              >
                {running ? 'Running...' : 'Run Backtest'}
              </button>
            </div>

            {/* Model selector */}
            <div style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 12 }}>
              <span style={{ color: '#999', fontSize: 13 }}>Models:</span>
              <button onClick={selectAll} style={{
                padding: '4px 10px', fontSize: 11, background: '#2a2a4a', color: '#ccc',
                border: '1px solid #444', borderRadius: 4, cursor: 'pointer',
              }}>
                {selectedModels.length === models.length ? 'Deselect All' : 'Select All'}
              </button>
              <span style={{ color: '#666', fontSize: 12 }}>{selectedModels.length} selected</span>
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
              {models.map(m => (
                <button
                  key={m.id}
                  onClick={() => toggleModel(m.id)}
                  style={{
                    padding: '6px 14px', borderRadius: 20, fontSize: 12, cursor: 'pointer',
                    border: selectedModels.includes(m.id) ? '2px solid #00d4aa' : '1px solid #444',
                    background: selectedModels.includes(m.id) ? 'rgba(0,212,170,0.15)' : '#1a1a2e',
                    color: selectedModels.includes(m.id) ? '#00d4aa' : '#999',
                  }}
                >
                  {m.name}
                </button>
              ))}
            </div>
          </div>

          {/* Progress bar */}
          {running && (
            <div className="card" style={{ padding: 16, marginBottom: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <span style={{ color: '#ccc' }}>{progressMsg}</span>
                <span style={{ color: '#00d4aa' }}>{progress}%</span>
              </div>
              <div style={{ background: '#1a1a2e', borderRadius: 4, height: 8, overflow: 'hidden' }}>
                <div style={{ width: `${progress}%`, height: '100%', background: '#00d4aa', transition: 'width 0.3s' }} />
              </div>
            </div>
          )}

          {/* Results */}
          {results && (
            <>
              {/* Results table */}
              <div className="card" style={{ padding: 16, marginBottom: 16, overflowX: 'auto' }}>
                <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Results</h3>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                  <thead>
                    <tr style={{ borderBottom: '1px solid #333' }}>
                      {['Model', 'Return %', 'Win Rate', 'Sharpe', 'Max DD', 'Trades', 'Best', 'Worst', ''].map(h => (
                        <th key={h} style={{ padding: '8px 10px', textAlign: 'left', color: '#999', fontWeight: 500 }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {resultEntries
                      .sort((a, b) => {
                        const retA = a[1].total_return_pct ?? a[1].cumulative_return ?? 0
                        const retB = b[1].total_return_pct ?? b[1].cumulative_return ?? 0
                        return retB - retA
                      })
                      .map(([pid, r], i) => {
                        const ret = r.total_return_pct ?? r.cumulative_return ?? r.avg_return ?? 0
                        const wr = r.win_rate ?? r.avg_win_rate ?? 0
                        const sharpe = r.sharpe_ratio ?? r.avg_sharpe ?? 0
                        const dd = r.max_drawdown ?? r.avg_max_drawdown ?? 0
                        const trades = r.num_trades ?? r.total_trades ?? 0
                        const best = r.best_trade_pct ?? r.best_ever_trade ?? 0
                        const worst = r.worst_trade_pct ?? r.worst_ever_trade ?? 0
                        return (
                          <tr key={pid} style={{ borderBottom: '1px solid #222' }}>
                            <td style={{ padding: '8px 10px', color: COLORS[i % COLORS.length], fontWeight: 600 }}>
                              {r.display_name || pid}
                            </td>
                            <td style={{ padding: '8px 10px', color: ret >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 700 }}>
                              {ret >= 0 ? '+' : ''}{ret.toFixed(2)}%
                            </td>
                            <td style={{ padding: '8px 10px', color: '#ccc' }}>{wr.toFixed(1)}%</td>
                            <td style={{ padding: '8px 10px', color: sharpe > 0 ? '#4ecdc4' : '#e17055' }}>{sharpe.toFixed(2)}</td>
                            <td style={{ padding: '8px 10px', color: '#ff6b6b' }}>{dd.toFixed(2)}%</td>
                            <td style={{ padding: '8px 10px', color: '#ccc' }}>{trades}</td>
                            <td style={{ padding: '8px 10px', color: '#00d4aa' }}>{best > 0 ? '+' : ''}{best.toFixed(2)}%</td>
                            <td style={{ padding: '8px 10px', color: '#ff6b6b' }}>{worst.toFixed(2)}%</td>
                            <td style={{ padding: '8px 10px' }}>
                              {singleDayResults && r.trades && (
                                <button onClick={() => setTradeLogModel(tradeLogModel === pid ? null : pid)}
                                  style={{ padding: '3px 10px', fontSize: 11, background: '#2a2a4a', color: '#ccc', border: '1px solid #444', borderRadius: 4, cursor: 'pointer' }}>
                                  {tradeLogModel === pid ? 'Hide' : 'Trades'}
                                </button>
                              )}
                            </td>
                          </tr>
                        )
                      })}
                  </tbody>
                </table>
              </div>

              {/* Equity curve */}
              {singleDayResults && (
                <div className="card" style={{ padding: 16, marginBottom: 16 }}>
                  <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Equity Curves</h3>
                  <EquityCurveChart results={singleDayResults} />
                </div>
              )}

              {/* Trade log */}
              {tradeLogModel && singleDayResults && singleDayResults[tradeLogModel] && (
                <div className="card" style={{ padding: 16, marginBottom: 16, overflowX: 'auto' }}>
                  <h3 style={{ margin: '0 0 12px', color: '#eee' }}>
                    Trade Log: {singleDayResults[tradeLogModel].display_name || tradeLogModel}
                  </h3>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid #333' }}>
                        {['Time', 'Symbol', 'Action', 'Price', 'Qty', 'Confidence', 'P&L', 'Reasoning'].map(h => (
                          <th key={h} style={{ padding: '6px 8px', textAlign: 'left', color: '#999' }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(singleDayResults[tradeLogModel].trades || []).map((t, i) => (
                        <tr key={i} style={{ borderBottom: '1px solid #1a1a2e' }}>
                          <td style={{ padding: '6px 8px', color: '#888', fontFamily: 'monospace', fontSize: 11 }}>
                            {t.timestamp ? formatTimeAZ(t.timestamp) : '-'}
                          </td>
                          <td style={{ padding: '6px 8px', color: '#eee', fontWeight: 600 }}>{t.symbol}</td>
                          <td style={{ padding: '6px 8px', color: t.action === 'BUY' ? '#00d4aa' : '#ff6b6b', fontWeight: 700 }}>
                            {t.action}
                          </td>
                          <td style={{ padding: '6px 8px', color: '#ccc' }}>${t.price?.toFixed(2)}</td>
                          <td style={{ padding: '6px 8px', color: '#ccc' }}>{t.qty?.toFixed(4)}</td>
                          <td style={{ padding: '6px 8px', color: '#a29bfe' }}>{(t.confidence || 0).toFixed(2)}</td>
                          <td style={{
                            padding: '6px 8px', fontWeight: 600,
                            color: t.action === 'SELL' ? (t.pnl >= 0 ? '#00d4aa' : '#ff6b6b') : '#555',
                          }}>
                            {t.action === 'SELL' ? (t.pnl >= 0 ? '+' : '') + '$' + t.pnl?.toFixed(2) : '-'}
                          </td>
                          <td style={{ padding: '6px 8px', color: '#888', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {t.reasoning || '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Multi-day cumulative */}
              {results?.days && (
                <div className="card" style={{ padding: 16, marginBottom: 16 }}>
                  <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Daily Breakdown</h3>
                  {results.days.map(day => (
                    <div key={day.date} style={{ marginBottom: 12 }}>
                      <h4 style={{ color: '#999', margin: '0 0 6px' }}>{day.date}</h4>
                      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
                        {Object.entries(day.results).map(([pid, r]) => (
                          <div key={pid} style={{ background: '#1a1a2e', padding: '8px 14px', borderRadius: 8, fontSize: 12 }}>
                            <span style={{ color: '#ccc', fontWeight: 600 }}>{r.display_name || pid}</span>{' '}
                            <span style={{ color: r.total_return_pct >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 700 }}>
                              {r.total_return_pct >= 0 ? '+' : ''}{r.total_return_pct?.toFixed(2)}%
                            </span>{' '}
                            <span style={{ color: '#666' }}>{r.num_trades}t</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </>
      )}

      {/* === TIME MACHINE TAB === */}
      {tab === 'timemachine' && (
        <>
          <div className="card" style={{ padding: 20, marginBottom: 16 }}>
            <h3 style={{ margin: '0 0 4px', color: '#eee' }}>Time Machine</h3>
            <p style={{ color: '#888', fontSize: 12, margin: '0 0 16px' }}>
              Replay a model's recorded signals against historical prices. Supports up to 10 years of data.
            </p>

            <div style={{ display: 'flex', gap: 16, alignItems: 'end', flexWrap: 'wrap', marginBottom: 16 }}>
              <div>
                <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>Model</label>
                <select value={tmPlayer} onChange={e => setTmPlayer(e.target.value)}
                  style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6, minWidth: 180 }}>
                  <option value="">Select a model...</option>
                  {models.map(m => <option key={m.id} value={m.id}>{m.name}</option>)}
                </select>
              </div>

              {!tmUseRange && (
                <div>
                  <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>Lookback</label>
                  <select value={tmDays} onChange={e => setTmDays(Number(e.target.value))}
                    style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }}>
                    <option value={7}>7 days</option>
                    <option value={30}>30 days</option>
                    <option value={90}>90 days</option>
                    <option value={180}>6 months</option>
                    <option value={365}>1 year</option>
                    <option value={730}>2 years</option>
                    <option value={1825}>5 years</option>
                    <option value={3650}>10 years</option>
                  </select>
                </div>
              )}

              {tmUseRange && (
                <>
                  <div>
                    <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>Start Date</label>
                    <input type="date" value={tmStartDate} onChange={e => setTmStartDate(e.target.value)}
                      style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }} />
                  </div>
                  <div>
                    <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>End Date</label>
                    <input type="date" value={tmEndDate} onChange={e => setTmEndDate(e.target.value)}
                      style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }} />
                  </div>
                </>
              )}

              <label style={{ display: 'flex', alignItems: 'center', gap: 6, color: '#ccc', cursor: 'pointer' }}>
                <input type="checkbox" checked={tmUseRange} onChange={e => setTmUseRange(e.target.checked)} />
                Custom date range
              </label>

              <button onClick={runTimeMachine} disabled={tmRunning || !tmPlayer}
                style={{
                  padding: '10px 24px', borderRadius: 6, border: 'none', cursor: 'pointer',
                  background: tmRunning ? '#555' : '#00d4aa', color: '#000', fontWeight: 700, fontSize: 14,
                }}>
                {tmRunning ? 'Running...' : 'Run'}
              </button>
            </div>

            {tmError && <p style={{ color: '#ff6b6b', margin: '8px 0 0' }}>{tmError}</p>}
          </div>

          {/* Time Machine Results */}
          {tmResult && (
            <>
              <div className="card" style={{ padding: 16, marginBottom: 16 }}>
                <h3 style={{ margin: '0 0 4px', color: '#eee' }}>{tmResult.name}</h3>
                <p style={{ color: '#888', fontSize: 12, margin: '0 0 12px' }}>
                  {tmResult.start_date} to {tmResult.end_date} ({tmResult.days} days) — {tmResult.signals_tested} signals tested
                </p>

                <div style={{ display: 'flex', gap: 20, flexWrap: 'wrap', marginBottom: 16 }}>
                  {[
                    { label: 'Total Return', value: `${tmResult.stats.total_return_pct >= 0 ? '+' : ''}${tmResult.stats.total_return_pct}%`, color: tmResult.stats.total_return_pct >= 0 ? '#00d4aa' : '#ff6b6b' },
                    { label: 'Final Value', value: `$${tmResult.stats.final_value.toLocaleString()}`, color: '#eee' },
                    { label: 'Win Rate', value: `${tmResult.stats.win_rate}%`, color: tmResult.stats.win_rate >= 50 ? '#00d4aa' : '#ff6b6b' },
                    { label: 'Trades', value: tmResult.stats.total_trades, color: '#ccc' },
                    { label: 'Total P&L', value: `$${tmResult.stats.total_pnl.toFixed(2)}`, color: tmResult.stats.total_pnl >= 0 ? '#00d4aa' : '#ff6b6b' },
                    { label: 'Best Trade', value: `$${tmResult.stats.best_trade.toFixed(2)}`, color: '#00d4aa' },
                    { label: 'Worst Trade', value: `$${tmResult.stats.worst_trade.toFixed(2)}`, color: '#ff6b6b' },
                  ].map(s => (
                    <div key={s.label} style={{ background: '#1a1a2e', padding: '10px 16px', borderRadius: 8, minWidth: 100 }}>
                      <div style={{ color: '#666', fontSize: 11, marginBottom: 2 }}>{s.label}</div>
                      <div style={{ color: s.color, fontSize: 18, fontWeight: 700 }}>{s.value}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Equity curve */}
              {tmResult.equity_curve?.length > 1 && (
                <div className="card" style={{ padding: 16, marginBottom: 16 }}>
                  <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Equity Curve</h3>
                  <EquityCurveChart results={{ [tmResult.player_id]: tmResult }} />
                </div>
              )}

              {/* Trade log */}
              {tmResult.trades?.length > 0 && (
                <div className="card" style={{ padding: 16, overflowX: 'auto' }}>
                  <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Trade Log ({tmResult.trades.length})</h3>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid #333' }}>
                        {['Symbol', 'Signal', 'Conf', 'Entry Date', 'Entry $', 'Exit Date', 'Exit $', 'P&L', 'P&L %'].map(h => (
                          <th key={h} style={{ padding: '6px 8px', textAlign: 'left', color: '#999' }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {tmResult.trades.map((t, i) => (
                        <tr key={i} style={{ borderBottom: '1px solid #1a1a2e' }}>
                          <td style={{ padding: '6px 8px', color: '#eee', fontWeight: 600 }}>{t.symbol}</td>
                          <td style={{ padding: '6px 8px', color: '#a29bfe' }}>{t.signal}</td>
                          <td style={{ padding: '6px 8px', color: '#ccc' }}>{t.confidence.toFixed(2)}</td>
                          <td style={{ padding: '6px 8px', color: '#888', fontFamily: 'monospace' }}>{t.entry_date}</td>
                          <td style={{ padding: '6px 8px', color: '#ccc' }}>${t.entry_price}</td>
                          <td style={{ padding: '6px 8px', color: '#888', fontFamily: 'monospace' }}>{t.exit_date}</td>
                          <td style={{ padding: '6px 8px', color: '#ccc' }}>${t.exit_price}</td>
                          <td style={{ padding: '6px 8px', color: t.pnl >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 600 }}>
                            {t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(2)}
                          </td>
                          <td style={{ padding: '6px 8px', color: t.pnl_pct >= 0 ? '#00d4aa' : '#ff6b6b' }}>
                            {t.pnl_pct >= 0 ? '+' : ''}{t.pnl_pct.toFixed(2)}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </>
      )}

      {/* === HISTORY TAB === */}
      {tab === 'history' && (
        <div className="card" style={{ padding: 16 }}>
          <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Past Backtest Runs</h3>
          {pastRuns.length === 0 ? (
            <p style={{ color: '#666' }}>No backtests run yet.</p>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Run #', 'Type', 'Date(s)', 'Models', 'Status', 'When', ''].map(h => (
                    <th key={h} style={{ padding: '8px 10px', textAlign: 'left', color: '#999' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pastRuns.map(run => (
                  <tr key={run.id} style={{ borderBottom: '1px solid #222' }}>
                    <td style={{ padding: '8px 10px', color: '#ccc' }}>#{run.id}</td>
                    <td style={{ padding: '8px 10px', color: '#a29bfe' }}>{run.run_type}</td>
                    <td style={{ padding: '8px 10px', color: '#ccc' }}>
                      {run.start_date}{run.end_date !== run.start_date ? ` to ${run.end_date}` : ''}
                    </td>
                    <td style={{ padding: '8px 10px', color: '#888', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {(() => { try { return JSON.parse(run.model_ids).length + ' models' } catch { return run.model_ids } })()}
                    </td>
                    <td style={{ padding: '8px 10px' }}>
                      <span style={{
                        padding: '2px 8px', borderRadius: 10, fontSize: 11,
                        background: run.status === 'complete' ? 'rgba(0,212,170,0.2)' : run.status === 'error' ? 'rgba(255,107,107,0.2)' : 'rgba(255,230,109,0.2)',
                        color: run.status === 'complete' ? '#00d4aa' : run.status === 'error' ? '#ff6b6b' : '#ffe66d',
                      }}>{run.status}</span>
                    </td>
                    <td style={{ padding: '8px 10px', color: '#888', fontSize: 11 }}>
                      {run.created_at ? formatDateTimeAZ(run.created_at) : ''}
                    </td>
                    <td style={{ padding: '8px 10px' }}>
                      {run.status === 'complete' && (
                        <button onClick={() => loadRun(run.id)} style={{
                          padding: '3px 10px', fontSize: 11, background: '#2a2a4a',
                          color: '#00d4aa', border: '1px solid #333', borderRadius: 4, cursor: 'pointer',
                        }}>View</button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {/* === RANKINGS TAB === */}
      {tab === 'rankings' && (
        <div className="card" style={{ padding: 16 }}>
          <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Model Rankings (All Backtests)</h3>
          {rankings.length === 0 ? (
            <p style={{ color: '#666' }}>Run some backtests first to see rankings.</p>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Rank', 'Model', 'Days Tested', 'Avg Return', 'Cumulative', 'Win Rate', 'Sharpe', 'Avg Max DD', 'Total Trades', 'Best Ever', 'Worst Ever'].map(h => (
                    <th key={h} style={{ padding: '8px 8px', textAlign: 'left', color: '#999', fontWeight: 500 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rankings.map((r, i) => (
                  <tr key={r.player_id} style={{ borderBottom: '1px solid #222' }}>
                    <td style={{ padding: '8px', color: i < 3 ? '#ffe66d' : '#666', fontWeight: 700, fontSize: 16 }}>
                      {i === 0 ? '1st' : i === 1 ? '2nd' : i === 2 ? '3rd' : `${i + 1}th`}
                    </td>
                    <td style={{ padding: '8px', color: COLORS[i % COLORS.length], fontWeight: 600 }}>{r.display_name || r.player_id}</td>
                    <td style={{ padding: '8px', color: '#ccc' }}>{r.days_tested}</td>
                    <td style={{ padding: '8px', color: r.avg_return >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 700 }}>
                      {r.avg_return >= 0 ? '+' : ''}{r.avg_return?.toFixed(2)}%
                    </td>
                    <td style={{ padding: '8px', color: r.cumulative_return >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 700 }}>
                      {r.cumulative_return >= 0 ? '+' : ''}{r.cumulative_return?.toFixed(2)}%
                    </td>
                    <td style={{ padding: '8px', color: '#ccc' }}>{r.avg_win_rate?.toFixed(1)}%</td>
                    <td style={{ padding: '8px', color: r.avg_sharpe > 0 ? '#4ecdc4' : '#e17055' }}>{r.avg_sharpe?.toFixed(2)}</td>
                    <td style={{ padding: '8px', color: '#ff6b6b' }}>{r.avg_max_drawdown?.toFixed(2)}%</td>
                    <td style={{ padding: '8px', color: '#ccc' }}>{r.total_trades}</td>
                    <td style={{ padding: '8px', color: '#00d4aa' }}>+{r.best_ever_trade?.toFixed(2)}%</td>
                    <td style={{ padding: '8px', color: '#ff6b6b' }}>{r.worst_ever_trade?.toFixed(2)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}
