import React, { useState, useEffect, useRef } from 'react'
import { api } from '../api/client'

const COLORS = [
  '#00d4aa', '#ff6b6b', '#4ecdc4', '#ffe66d', '#a29bfe',
  '#fd79a8', '#00b894', '#e17055', '#6c5ce7', '#74b9ff',
]

function EquityCurve({ equity }) {
  const canvasRef = useRef(null)

  useEffect(() => {
    if (!canvasRef.current || !equity || equity.length < 2) return
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    const dpr = window.devicePixelRatio || 1
    canvas.width = canvas.offsetWidth * dpr
    canvas.height = canvas.offsetHeight * dpr
    ctx.scale(dpr, dpr)
    const W = canvas.offsetWidth, H = canvas.offsetHeight
    const pad = { top: 20, right: 20, bottom: 30, left: 65 }

    ctx.clearRect(0, 0, W, H)
    ctx.fillStyle = '#1a1a2e'
    ctx.fillRect(0, 0, W, H)

    const values = equity.map(p => p.value)
    const minV = Math.min(...values) * 0.999
    const maxV = Math.max(...values) * 1.001
    const xScale = (W - pad.left - pad.right) / Math.max(equity.length - 1, 1)
    const yScale = (H - pad.top - pad.bottom) / (maxV - minV || 1)

    // Grid
    ctx.strokeStyle = '#2a2a4a'
    ctx.lineWidth = 0.5
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + (H - pad.top - pad.bottom) * i / 4
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke()
      const val = maxV - (maxV - minV) * i / 4
      ctx.fillStyle = '#666'; ctx.font = '10px monospace'; ctx.textAlign = 'right'
      ctx.fillText('$' + val.toFixed(0), pad.left - 5, y + 3)
    }

    // Baseline
    const baseY = pad.top + (maxV - 10000) * yScale
    if (baseY > pad.top && baseY < H - pad.bottom) {
      ctx.strokeStyle = '#444'; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(pad.left, baseY); ctx.lineTo(W - pad.right, baseY); ctx.stroke()
      ctx.setLineDash([])
    }

    // Curve
    ctx.strokeStyle = '#00d4aa'; ctx.lineWidth = 2; ctx.beginPath()
    equity.forEach((p, i) => {
      const x = pad.left + i * xScale
      const y = pad.top + (maxV - p.value) * yScale
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y)
    })
    ctx.stroke()

    // Fill
    const lastX = pad.left + (equity.length - 1) * xScale
    ctx.lineTo(lastX, H - pad.bottom)
    ctx.lineTo(pad.left, H - pad.bottom)
    ctx.closePath()
    ctx.fillStyle = 'rgba(0,212,170,0.08)'
    ctx.fill()
  }, [equity])

  return <canvas ref={canvasRef} style={{ width: '100%', height: 250, borderRadius: 8 }} />
}

export default function StrategyLab() {
  const [strategies, setStrategies] = useState({})
  const [strategy, setStrategy] = useState('')
  const [symbol, setSymbol] = useState('AAPL')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [customParams, setCustomParams] = useState({})
  const [mode, setMode] = useState('single') // single or optimize
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(0)
  const [progressMsg, setProgressMsg] = useState('')
  const [result, setResult] = useState(null)
  const [optimizeResults, setOptimizeResults] = useState(null)
  const [expandedRow, setExpandedRow] = useState(null)
  const [deploying, setDeploying] = useState(false)
  const [deployMsg, setDeployMsg] = useState('')
  const pollRef = useRef(null)
  const [tab, setTab] = useState('backtest') // backtest, auto, history
  const [latestReport, setLatestReport] = useState(null)
  const [reportHistory, setReportHistory] = useState([])
  const [autoRunning, setAutoRunning] = useState(false)
  const [autoRunId, setAutoRunId] = useState(null)
  const [autoProgress, setAutoProgress] = useState(0)
  const [autoMsg, setAutoMsg] = useState('')
  const [autoResult, setAutoResult] = useState(null)
  const autoPollRef = useRef(null)

  // Defaults
  useEffect(() => {
    api.getStrategies().then(s => {
      setStrategies(s)
      const first = Object.keys(s)[0]
      if (first) setStrategy(first)
    }).catch(() => {})
    api.getLatestOptimization().then(setLatestReport).catch(() => {})
    api.getOptimizationHistory().then(setReportHistory).catch(() => {})

    const d = new Date()
    setEndDate(d.toISOString().split('T')[0])
    const s = new Date(d)
    s.setFullYear(s.getFullYear() - 1)
    setStartDate(s.toISOString().split('T')[0])
  }, [])

  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
      if (autoPollRef.current) clearInterval(autoPollRef.current)
    }
  }, [])

  const runAutoOptimize = async () => {
    setAutoRunning(true); setAutoProgress(0); setAutoMsg('Starting...'); setAutoResult(null)
    try {
      const { run_id, error } = await api.runAutoOptimize()
      if (error) { setAutoMsg(error); setAutoRunning(false); return }
      setAutoRunId(run_id)
      autoPollRef.current = setInterval(async () => {
        try {
          const st = await api.getOptimizeStatus(run_id)
          setAutoProgress(st.progress || 0)
          setAutoMsg(st.message || '')
          if (st.status === 'complete') {
            clearInterval(autoPollRef.current)
            setAutoResult(st.results)
            setAutoRunning(false)
            api.getLatestOptimization().then(setLatestReport).catch(() => {})
            api.getOptimizationHistory().then(setReportHistory).catch(() => {})
          } else if (st.status === 'error') {
            clearInterval(autoPollRef.current)
            setAutoMsg('Error: ' + st.message)
            setAutoRunning(false)
          }
        } catch { /* keep polling */ }
      }, 3000)
    } catch (e) {
      setAutoMsg('Failed: ' + e.message); setAutoRunning(false)
    }
  }

  // When strategy changes, reset custom params
  useEffect(() => {
    if (strategy && strategies[strategy]) {
      const p = {}
      Object.entries(strategies[strategy].params).forEach(([k, v]) => { p[k] = v.default })
      setCustomParams(p)
    }
  }, [strategy, strategies])

  const runSingle = async () => {
    setRunning(true); setResult(null); setOptimizeResults(null)
    try {
      const data = await api.runStrategyBacktest({
        strategy, symbol: symbol.toUpperCase(), start_date: startDate, end_date: endDate,
        params: customParams,
      })
      setResult(data)
    } catch (e) {
      setResult({ error: e.message })
    }
    setRunning(false)
  }

  const runOptimize = async () => {
    setRunning(true); setResult(null); setOptimizeResults(null)
    setProgress(0); setProgressMsg('Starting optimization...')
    try {
      const { run_id, error } = await api.runOptimize({
        strategy, symbol: symbol.toUpperCase(), start_date: startDate, end_date: endDate,
      })
      if (error) { setProgressMsg(error); setRunning(false); return }

      pollRef.current = setInterval(async () => {
        try {
          const st = await api.getOptimizeStatus(run_id)
          setProgress(st.progress || 0)
          setProgressMsg(st.message || '')
          if (st.status === 'complete') {
            clearInterval(pollRef.current)
            setOptimizeResults(st.results)
            setRunning(false)
          } else if (st.status === 'error') {
            clearInterval(pollRef.current)
            setProgressMsg('Error: ' + st.message)
            setRunning(false)
          }
        } catch { /* keep polling */ }
      }, 1500)
    } catch (e) {
      setProgressMsg('Failed: ' + e.message)
      setRunning(false)
    }
  }

  const deployWinner = async (params, stats) => {
    setDeploying(true); setDeployMsg('')
    try {
      const res = await api.deployStrategy({ strategy, params, stats })
      setDeployMsg(res.message || res.error || 'Done')
    } catch (e) {
      setDeployMsg('Failed: ' + e.message)
    }
    setDeploying(false)
  }

  const strat = strategies[strategy]

  return (
    <div>
      {/* Description for new traders */}
      <div style={{
        padding: '12px 16px', marginBottom: 16, borderRadius: 8,
        background: '#0d1117', border: '1px solid #1e293b',
      }}>
        <p style={{ margin: 0, fontSize: 13, color: '#64748b', lineHeight: 1.6 }}>
          Describe a trading strategy in plain English and the AI crew will test it against up to 365 days of real historical data. No coding required.{' '}
          <span style={{ color: '#475569' }}>
            Try: <em>Buy when RSI drops below 30, sell when it goes above 70.</em>
          </span>{' '}
          The crew shows you win rate, total return, and best entry points.
        </p>
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        {[
          { id: 'backtest', label: 'Backtest & Optimize' },
          { id: 'auto', label: 'Auto-Optimize' },
          { id: 'history', label: 'Optimization History' },
        ].map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            style={{
              padding: '8px 20px', borderRadius: 6, border: 'none', cursor: 'pointer',
              background: tab === t.id ? '#a29bfe' : '#2a2a4a',
              color: tab === t.id ? '#000' : '#ccc', fontWeight: tab === t.id ? 700 : 400,
            }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* === AUTO-OPTIMIZE TAB === */}
      {tab === 'auto' && (
        <>
          <div className="card" style={{ padding: 20, marginBottom: 16 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <div>
                <h3 style={{ margin: '0 0 4px', color: '#eee' }}>Full Auto-Optimization</h3>
                <p style={{ color: '#888', fontSize: 12, margin: 0 }}>
                  Tests all 5 strategies across all 16 watchlist stocks with 20+ parameter variations.
                  Auto-deploys winning params if they beat current rules by &gt;10%.
                  Runs automatically every Sunday at midnight.
                </p>
              </div>
              <button onClick={runAutoOptimize} disabled={autoRunning}
                style={{
                  padding: '12px 28px', borderRadius: 6, border: 'none', cursor: 'pointer',
                  background: autoRunning ? '#555' : '#a29bfe', color: '#000', fontWeight: 700, fontSize: 14,
                  whiteSpace: 'nowrap',
                }}>
                {autoRunning ? 'Running...' : 'Run Now'}
              </button>
            </div>

            {autoRunning && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ color: '#ccc', fontSize: 13 }}>{autoMsg}</span>
                  <span style={{ color: '#a29bfe' }}>{autoProgress}%</span>
                </div>
                <div style={{ background: '#1a1a2e', borderRadius: 4, height: 8, overflow: 'hidden' }}>
                  <div style={{ width: `${autoProgress}%`, height: '100%', background: '#a29bfe', transition: 'width 0.3s' }} />
                </div>
              </div>
            )}

            {/* Latest report summary */}
            {latestReport && latestReport.best_strategy && (
              <div style={{ background: '#1a1a2e', padding: 16, borderRadius: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 12 }}>
                  <h4 style={{ margin: 0, color: '#ccc' }}>Latest Optimization Report</h4>
                  <span style={{ color: '#666', fontSize: 12 }}>{latestReport.timestamp}</span>
                </div>
                <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 12 }}>
                  {[
                    { l: 'Best Strategy', v: latestReport.best_strategy?.strategy_name || 'N/A', c: '#a29bfe' },
                    { l: 'Avg Profit Factor', v: latestReport.best_strategy?.avg_profit_factor?.toFixed(2) || '0', c: '#00d4aa' },
                    { l: 'Avg Win Rate', v: `${latestReport.best_strategy?.avg_win_rate?.toFixed(1) || 0}%`, c: '#00d4aa' },
                    { l: 'Stocks Tested', v: latestReport.stocks_tested || 0, c: '#ccc' },
                    { l: 'Strategies', v: latestReport.strategies_tested || 0, c: '#ccc' },
                    { l: 'Params Deployed', v: latestReport.deployed?.length || 0, c: latestReport.deployed?.length > 0 ? '#ffe66d' : '#666' },
                  ].map(s => (
                    <div key={s.l} style={{ minWidth: 110 }}>
                      <div style={{ color: '#666', fontSize: 11 }}>{s.l}</div>
                      <div style={{ color: s.c, fontSize: 18, fontWeight: 700 }}>{s.v}</div>
                    </div>
                  ))}
                </div>

                {/* Best params */}
                {latestReport.best_strategy?.best_params && (
                  <div style={{ marginBottom: 8 }}>
                    <span style={{ color: '#888', fontSize: 12 }}>Best Parameters: </span>
                    <span style={{ color: '#ccc', fontSize: 12, fontFamily: 'monospace' }}>
                      {Object.entries(latestReport.best_strategy.best_params).map(([k, v]) => `${k}=${v}`).join(', ')}
                    </span>
                  </div>
                )}

                {/* Deployed changes */}
                {latestReport.deployed?.length > 0 && (
                  <div style={{ marginTop: 8 }}>
                    <span style={{ color: '#ffe66d', fontSize: 12, fontWeight: 600 }}>Auto-deployed: </span>
                    {latestReport.deployed.map((d, i) => (
                      <span key={i} style={{ color: '#ccc', fontSize: 12, marginRight: 12 }}>
                        {d.param}: {d.old} → {d.new} (+{d.improvement_pct}%)
                      </span>
                    ))}
                  </div>
                )}

                {/* Per-strategy ranking */}
                {latestReport.results?.length > 0 && (
                  <div style={{ marginTop: 16 }}>
                    <h4 style={{ margin: '0 0 8px', color: '#999', fontSize: 13 }}>Strategy Rankings</h4>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                      <thead>
                        <tr style={{ borderBottom: '1px solid #333' }}>
                          {['#', 'Strategy', 'Avg PF', 'Avg Win Rate', 'Stocks', 'Best Params'].map(h => (
                            <th key={h} style={{ padding: '6px 8px', textAlign: 'left', color: '#666' }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {latestReport.results.map((r, i) => (
                          <tr key={r.strategy} style={{ borderBottom: '1px solid #1a1a2e' }}>
                            <td style={{ padding: '6px 8px', color: i === 0 ? '#ffe66d' : '#666', fontWeight: 700 }}>{i + 1}</td>
                            <td style={{ padding: '6px 8px', color: COLORS[i % COLORS.length], fontWeight: 600 }}>{r.strategy_name}</td>
                            <td style={{ padding: '6px 8px', color: r.avg_profit_factor >= 1.5 ? '#00d4aa' : r.avg_profit_factor >= 1 ? '#ffe66d' : '#ff6b6b', fontWeight: 700 }}>
                              {r.avg_profit_factor.toFixed(2)}
                            </td>
                            <td style={{ padding: '6px 8px', color: '#ccc' }}>{r.avg_win_rate.toFixed(1)}%</td>
                            <td style={{ padding: '6px 8px', color: '#888' }}>{r.stocks_tested}</td>
                            <td style={{ padding: '6px 8px', color: '#888', fontFamily: 'monospace', fontSize: 11 }}>
                              {Object.entries(r.best_params).map(([k, v]) => `${k}=${v}`).join(', ')}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {!latestReport?.best_strategy && !autoRunning && (
              <p style={{ color: '#666', margin: '16px 0 0' }}>No optimization reports yet. Click "Run Now" to start.</p>
            )}
          </div>
        </>
      )}

      {/* === HISTORY TAB === */}
      {tab === 'history' && (
        <div className="card" style={{ padding: 20 }}>
          <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Optimization History</h3>
          {reportHistory.length === 0 ? (
            <p style={{ color: '#666' }}>No optimization runs yet.</p>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Date', 'Best Strategy', 'Avg PF', 'Win Rate', 'Stocks', 'Deployed'].map(h => (
                    <th key={h} style={{ padding: '8px 10px', textAlign: 'left', color: '#999' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {reportHistory.map((r, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid #222' }}>
                    <td style={{ padding: '8px 10px', color: '#ccc', fontFamily: 'monospace', fontSize: 12 }}>{r.timestamp}</td>
                    <td style={{ padding: '8px 10px', color: COLORS[i % COLORS.length], fontWeight: 600 }}>{r.best_strategy}</td>
                    <td style={{ padding: '8px 10px', color: r.avg_profit_factor >= 1.5 ? '#00d4aa' : '#ccc', fontWeight: 700 }}>{r.avg_profit_factor?.toFixed(2)}</td>
                    <td style={{ padding: '8px 10px', color: '#ccc' }}>{r.avg_win_rate?.toFixed(1)}%</td>
                    <td style={{ padding: '8px 10px', color: '#888' }}>{r.stocks_tested}</td>
                    <td style={{ padding: '8px 10px' }}>
                      {r.deployed_count > 0 ? (
                        <span style={{ color: '#ffe66d', fontWeight: 600 }}>{r.deployed_count} param(s)</span>
                      ) : (
                        <span style={{ color: '#666' }}>None</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {/* === BACKTEST TAB === */}
      {tab === 'backtest' && (<>
      {/* Config */}
      <div className="card" style={{ padding: 20, marginBottom: 16 }}>
        <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Strategy Lab</h3>

        <div style={{ display: 'flex', gap: 16, alignItems: 'end', flexWrap: 'wrap', marginBottom: 16 }}>
          <div>
            <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>Strategy</label>
            <select value={strategy} onChange={e => setStrategy(e.target.value)}
              style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6, minWidth: 200 }}>
              {Object.entries(strategies).map(([k, v]) => (
                <option key={k} value={k}>{v.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>Symbol</label>
            <input value={symbol} onChange={e => setSymbol(e.target.value.toUpperCase())}
              style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6, width: 90 }} />
          </div>
          <div>
            <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>Start Date</label>
            <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)}
              style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }} />
          </div>
          <div>
            <label style={{ display: 'block', color: '#999', fontSize: 12, marginBottom: 4 }}>End Date</label>
            <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)}
              style={{ padding: '8px 12px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6 }} />
          </div>
        </div>

        {strat && <p style={{ color: '#888', fontSize: 12, margin: '0 0 12px' }}>{strat.description}</p>}

        {/* Parameter editor for single run */}
        {strat && mode === 'single' && (
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
            {Object.entries(strat.params).map(([k, v]) => (
              <div key={k}>
                <label style={{ display: 'block', color: '#888', fontSize: 11, marginBottom: 2 }}>{v.label}</label>
                <input type="number" value={customParams[k] ?? v.default}
                  onChange={e => setCustomParams(p => ({ ...p, [k]: Number(e.target.value) }))}
                  step={v.type === 'float' ? 0.5 : 1}
                  style={{ padding: '6px 10px', background: '#1a1a2e', color: '#eee', border: '1px solid #333', borderRadius: 6, width: 80 }} />
              </div>
            ))}
          </div>
        )}

        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={() => { setMode('single'); setTimeout(runSingle, 0) }}
            disabled={running || !strategy}
            style={{
              padding: '10px 20px', borderRadius: 6, border: 'none', cursor: 'pointer',
              background: running ? '#555' : '#00d4aa', color: '#000', fontWeight: 700,
            }}>
            {running && mode === 'single' ? 'Running...' : 'Run Backtest'}
          </button>
          <button onClick={() => { setMode('optimize'); setTimeout(runOptimize, 0) }}
            disabled={running || !strategy}
            style={{
              padding: '10px 20px', borderRadius: 6, border: 'none', cursor: 'pointer',
              background: running ? '#555' : '#a29bfe', color: '#000', fontWeight: 700,
            }}>
            {running && mode === 'optimize' ? 'Optimizing...' : 'Optimize Parameters'}
          </button>
        </div>
      </div>

      {/* Progress */}
      {running && mode === 'optimize' && (
        <div className="card" style={{ padding: 16, marginBottom: 16 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
            <span style={{ color: '#ccc' }}>{progressMsg}</span>
            <span style={{ color: '#a29bfe' }}>{progress}%</span>
          </div>
          <div style={{ background: '#1a1a2e', borderRadius: 4, height: 8, overflow: 'hidden' }}>
            <div style={{ width: `${progress}%`, height: '100%', background: '#a29bfe', transition: 'width 0.3s' }} />
          </div>
        </div>
      )}

      {/* Single result */}
      {result && !result.error && (
        <>
          <div className="card" style={{ padding: 16, marginBottom: 16 }}>
            <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Results — {result.symbol}</h3>
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 16 }}>
              {[
                { l: 'Return', v: `${result.stats.total_return_pct >= 0 ? '+' : ''}${result.stats.total_return_pct}%`, c: result.stats.total_return_pct >= 0 ? '#00d4aa' : '#ff6b6b' },
                { l: 'Win Rate', v: `${result.stats.win_rate}%`, c: result.stats.win_rate >= 50 ? '#00d4aa' : '#ff6b6b' },
                { l: 'Profit Factor', v: result.stats.profit_factor, c: result.stats.profit_factor >= 1 ? '#00d4aa' : '#ff6b6b' },
                { l: 'Max Drawdown', v: `${result.stats.max_drawdown_pct}%`, c: '#ff6b6b' },
                { l: 'Trades', v: result.stats.total_trades, c: '#ccc' },
                { l: 'Avg Hold', v: `${result.stats.avg_hold_days}d`, c: '#ccc' },
                { l: 'Total P&L', v: `$${result.stats.total_pnl.toFixed(2)}`, c: result.stats.total_pnl >= 0 ? '#00d4aa' : '#ff6b6b' },
              ].map(s => (
                <div key={s.l} style={{ background: '#1a1a2e', padding: '10px 16px', borderRadius: 8, minWidth: 100 }}>
                  <div style={{ color: '#666', fontSize: 11, marginBottom: 2 }}>{s.l}</div>
                  <div style={{ color: s.c, fontSize: 18, fontWeight: 700 }}>{s.v}</div>
                </div>
              ))}
            </div>
          </div>

          {result.equity_curve?.length > 1 && (
            <div className="card" style={{ padding: 16, marginBottom: 16 }}>
              <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Equity Curve</h3>
              <EquityCurve equity={result.equity_curve} />
            </div>
          )}

          {result.trades?.length > 0 && (
            <div className="card" style={{ padding: 16, marginBottom: 16, overflowX: 'auto' }}>
              <h3 style={{ margin: '0 0 12px', color: '#eee' }}>Trades ({result.trades.length})</h3>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid #333' }}>
                    {['Symbol', 'Entry', 'Entry $', 'Exit', 'Exit $', 'P&L', '%', 'Reason'].map(h => (
                      <th key={h} style={{ padding: '6px 8px', textAlign: 'left', color: '#999' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.trades.map((t, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #1a1a2e' }}>
                      <td style={{ padding: '6px 8px', color: '#eee', fontWeight: 600 }}>{t.symbol}</td>
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
                      <td style={{ padding: '6px 8px', color: '#888' }}>{t.exit_reason}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
      {result?.error && (
        <div className="card" style={{ padding: 16, marginBottom: 16, color: '#ff6b6b' }}>{result.error}</div>
      )}

      {/* Optimization results */}
      {optimizeResults && !optimizeResults.error && (
        <>
          <div className="card" style={{ padding: 16, marginBottom: 16 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <h3 style={{ margin: 0, color: '#eee' }}>
                Optimization Results — {optimizeResults.strategy_name} on {optimizeResults.symbol}
              </h3>
              <span style={{ color: '#888', fontSize: 12 }}>
                {optimizeResults.total_combinations} combinations tested
              </span>
            </div>

            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['#', 'Parameters', 'Profit Factor', 'Return %', 'Win Rate', 'Max DD', 'Trades', 'Avg Hold', 'P&L', ''].map(h => (
                    <th key={h} style={{ padding: '8px 8px', textAlign: 'left', color: '#999', fontWeight: 500 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {optimizeResults.results.slice(0, 25).map((r, i) => {
                  const s = r.stats
                  const paramStr = Object.entries(r.params)
                    .filter(([k]) => optimizeResults.results.length > 1)
                    .map(([k, v]) => `${k}=${v}`)
                    .join(', ')
                  return (
                    <React.Fragment key={i}>
                      <tr style={{
                        borderBottom: '1px solid #222',
                        background: i === 0 ? 'rgba(0,212,170,0.05)' : 'transparent',
                        cursor: 'pointer',
                      }}
                        onClick={() => setExpandedRow(expandedRow === i ? null : i)}>
                        <td style={{ padding: '8px', color: i === 0 ? '#ffe66d' : '#666', fontWeight: 700 }}>
                          {i + 1}
                        </td>
                        <td style={{ padding: '8px', color: '#ccc', fontSize: 11, fontFamily: 'monospace', maxWidth: 250 }}>
                          {paramStr}
                        </td>
                        <td style={{ padding: '8px', color: s.profit_factor >= 1.5 ? '#00d4aa' : s.profit_factor >= 1 ? '#ffe66d' : '#ff6b6b', fontWeight: 700 }}>
                          {s.profit_factor >= 999 ? 'INF' : s.profit_factor.toFixed(2)}
                        </td>
                        <td style={{ padding: '8px', color: s.total_return_pct >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 600 }}>
                          {s.total_return_pct >= 0 ? '+' : ''}{s.total_return_pct}%
                        </td>
                        <td style={{ padding: '8px', color: '#ccc' }}>{s.win_rate}%</td>
                        <td style={{ padding: '8px', color: '#ff6b6b' }}>{s.max_drawdown_pct}%</td>
                        <td style={{ padding: '8px', color: '#ccc' }}>{s.total_trades}</td>
                        <td style={{ padding: '8px', color: '#ccc' }}>{s.avg_hold_days}d</td>
                        <td style={{ padding: '8px', color: s.total_pnl >= 0 ? '#00d4aa' : '#ff6b6b', fontWeight: 600 }}>
                          ${s.total_pnl.toFixed(2)}
                        </td>
                        <td style={{ padding: '8px' }}>
                          {i === 0 && (
                            <button onClick={e => { e.stopPropagation(); deployWinner(r.params, r.stats) }}
                              disabled={deploying}
                              style={{
                                padding: '4px 12px', fontSize: 11, borderRadius: 4, border: 'none',
                                cursor: 'pointer', background: '#ffe66d', color: '#000', fontWeight: 700,
                              }}>
                              {deploying ? 'Deploying...' : 'Deploy to Models'}
                            </button>
                          )}
                        </td>
                      </tr>
                      {expandedRow === i && r.equity_curve?.length > 1 && (
                        <tr>
                          <td colSpan={10} style={{ padding: 16, background: '#0d0d1a' }}>
                            <EquityCurve equity={r.equity_curve} />
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  )
                })}
              </tbody>
            </table>

            {deployMsg && (
              <p style={{ marginTop: 12, color: deployMsg.includes('Failed') ? '#ff6b6b' : '#00d4aa', fontSize: 13 }}>
                {deployMsg}
              </p>
            )}
          </div>
        </>
      )}
      {optimizeResults?.error && (
        <div className="card" style={{ padding: 16, marginBottom: 16, color: '#ff6b6b' }}>{optimizeResults.error}</div>
      )}
      </>)}
    </div>
  )
}
