import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

export default function ChartAnalyzer() {
  const [symbol, setSymbol] = useState('SPY')
  const [model, setModel] = useState('codex')
  const [analyzing, setAnalyzing] = useState(false)
  const [analysis, setAnalysis] = useState(null)
  const [history, setHistory] = useState({})
  const [error, setError] = useState(null)

  const tickers = ['SPY','QQQ','NVDA','TSLA','AAPL','AMD','META','MSFT','GOOGL','AMZN','MU','ORCL','NOW','AVGO','PLTR','DELL']
  const models = [{id:'codex',label:'OpenAI Codex'},{id:'gemini',label:'Gemini'},{id:'grok',label:'Grok'},{id:'ollama',label:'Ollama'}]

  async function runAnalysis() {
    setAnalyzing(true)
    setError(null)
    try {
      const data = await api.analyzeChart(symbol, model)
      const a = data.analysis || data
      setAnalysis({ ...a, model, timestamp: new Date().toISOString() })
    } catch(e) {
      setError('Analysis failed: ' + e.message)
    } finally {
      setAnalyzing(false)
    }
    // Comparison fetch is separate — failure here never blocks or alerts
    try {
      const comp = await api.getChartComparison(symbol)
      setHistory(comp?.models || {})
    } catch {
      // non-fatal — comparison stays empty until next successful run
    }
  }

  const trendColor = analysis?.trend === 'bullish' ? '#22c55e' : analysis?.trend === 'bearish' ? '#ef4444' : '#94a3b8'

  return (
    <div>
      {/* Ticker + Model Selection */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <h2>AI Chart Analyzer</h2>
        </div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center', marginBottom: 12 }}>
          {tickers.map(t => (
            <button key={t} onClick={() => setSymbol(t)}
              style={{
                padding: '6px 12px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
                background: symbol === t ? '#00d4aa' : '#1a1a2e',
                color: symbol === t ? '#0a0a1a' : '#94a3b8',
                border: '1px solid #333',
              }}>{t}</button>
          ))}
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <select value={model} onChange={e => setModel(e.target.value)}
            style={{ padding: '8px 12px', borderRadius: 6, background: '#1a1a2e', color: '#e2e8f0', border: '1px solid #333', fontSize: 13 }}>
            {models.map(m => <option key={m.id} value={m.id}>{m.label}</option>)}
          </select>
          <button onClick={runAnalysis} disabled={analyzing}
            style={{
              padding: '8px 20px', borderRadius: 6, fontSize: 13, fontWeight: 600, cursor: 'pointer',
              background: analyzing ? '#333' : '#00d4aa', color: '#0a0a1a', border: 'none',
            }}>
            {analyzing ? 'Analyzing...' : 'Analyze ' + symbol}
          </button>
        </div>
      </div>

      {/* Inline error (replaces alert popup) */}
      {error && (
        <div style={{ background: '#1a0a0a', border: '1px solid #ef4444', borderRadius: 8, padding: '12px 16px', marginBottom: 16, color: '#ef4444', fontSize: 13, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>{error}</span>
          <span onClick={() => setError(null)} style={{ cursor: 'pointer', fontSize: 16, lineHeight: 1 }}>✕</span>
        </div>
      )}

      {/* Analysis Results */}
      {analysis && (
        <div className="card" style={{ marginBottom: 16 }}>
          <div className="card-header">
            <h2>Analysis: {symbol}</h2>
            <span style={{ fontSize: 12, color: '#64748b', fontFamily: 'monospace' }}>
              {model.toUpperCase()} — {new Date(analysis.timestamp).toLocaleString('en-US', { timeZone: 'America/Phoenix' })}
            </span>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12, marginBottom: 16 }}>
            <div style={{ background: '#0f172a', borderRadius: 8, padding: 14, borderLeft: `3px solid ${trendColor}` }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Trend</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: trendColor, fontFamily: 'monospace' }}>{(analysis.trend || 'N/A').toUpperCase()}</div>
            </div>
            <div style={{ background: '#0f172a', borderRadius: 8, padding: 14 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Confidence</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: (analysis.confidence || 0) >= 70 ? '#22c55e' : '#eab308', fontFamily: 'monospace' }}>{analysis.confidence || 0}%</div>
            </div>
            <div style={{ background: '#0f172a', borderRadius: 8, padding: 14 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Patterns</div>
              <div style={{ fontSize: 13, fontFamily: 'monospace', color: '#e2e8f0' }}>{(analysis.patterns || []).join(', ') || 'None'}</div>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
            <div style={{ background: '#0f172a', borderRadius: 8, padding: 14 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#22c55e', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Support</div>
              <div style={{ fontFamily: 'monospace', fontSize: 14, color: '#e2e8f0' }}>
                {(analysis.support || []).map(p => '$' + Number(p).toFixed(2)).join('  |  ') || 'N/A'}
              </div>
            </div>
            <div style={{ background: '#0f172a', borderRadius: 8, padding: 14 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#ef4444', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Resistance</div>
              <div style={{ fontFamily: 'monospace', fontSize: 14, color: '#e2e8f0' }}>
                {(analysis.resistance || []).map(p => '$' + Number(p).toFixed(2)).join('  |  ') || 'N/A'}
              </div>
            </div>
          </div>

          <div style={{ background: '#0f172a', borderRadius: 8, padding: 14, marginBottom: 12 }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Volume</div>
            <div style={{ fontSize: 13, color: '#e2e8f0' }}>{analysis.volume_assessment || 'N/A'}</div>
          </div>

          <div style={{ background: 'linear-gradient(135deg, #0f172a, #1e1b4b)', borderRadius: 8, padding: 16, border: '1px solid #00d4aa' }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#00d4aa', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Recommendation</div>
            <div style={{ fontSize: 15, fontWeight: 600, color: '#e2e8f0', lineHeight: 1.5 }}>{analysis.recommendation || 'N/A'}</div>
          </div>
        </div>
      )}

      {/* Model Comparison History */}
      {Object.keys(history).length > 0 && (
        <div className="card">
          <div className="card-header"><h2>Model Comparison: {symbol}</h2></div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 12 }}>
            {Object.entries(history).map(([mdl, entry]) => {
              const a = entry?.analysis || entry
              const tc = a?.trend === 'bullish' ? '#22c55e' : a?.trend === 'bearish' ? '#ef4444' : '#94a3b8'
              return (
                <div key={mdl} style={{ background: '#0f172a', borderRadius: 10, padding: 14, borderLeft: `3px solid ${tc}` }}>
                  <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 6 }}>{mdl.toUpperCase()}</div>
                  <div style={{ fontSize: 12, color: tc, fontWeight: 600, marginBottom: 4 }}>{(a?.trend || '').toUpperCase()} ({a?.confidence || '?'}%)</div>
                  <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>Patterns: {(a?.patterns || []).join(', ') || 'None'}</div>
                  <div style={{ fontSize: 11, lineHeight: 1.4, color: '#e2e8f0' }}>{a?.recommendation || ''}</div>
                  <div style={{ fontSize: 10, color: '#64748b', marginTop: 6, fontFamily: 'monospace' }}>{a?.timestamp ? new Date(a.timestamp).toLocaleString('en-US', { timeZone: 'America/Phoenix' }) : ''}</div>
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
