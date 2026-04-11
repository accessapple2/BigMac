import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
  dayblade: '#f59e0b',
  matrix: '#00bcd4',
}

export default function ModelControl() {
  const { data, refetch } = usePolling(api.getModelControl, 30000)
  const fetchCosts = useCallback(() => api.getCostDashboard(), [])
  const { data: costData } = usePolling(fetchCosts, 15000)
  const [scanning, setScanning] = useState(false)
  const [scanMsg, setScanMsg] = useState(null)
  const [collapsed, setCollapsed] = useState({})

  if (!data) return <div className="empty-state">Loading model control...</div>

  // Build cost-adjusted returns lookup
  const roiMap = {}
  if (costData?.roi_ranking) {
    for (const r of costData.roi_ranking) roiMap[r.player_id] = r
  }
  const gradeMap = {}
  if (costData?.efficiency_grades) {
    for (const g of costData.efficiency_grades) gradeMap[g.player_id] = g
  }

  const handleForceScan = async () => {
    setScanning(true)
    setScanMsg(null)
    try {
      const res = await api.forceScan()
      setScanMsg(res.ok ? 'Scan started — check console for progress' : res.message)
    } catch {
      setScanMsg('Failed to trigger scan')
    }
    setTimeout(() => setScanning(false), 5000)
  }

  const handlePauseAll = async () => {
    await api.togglePauseAll()
    refetch()
  }

  const handleFallbackToggle = async () => {
    await api.toggleFallbacks()
    refetch()
  }

  const handleToggle = async (playerId) => {
    await api.togglePausePlayer(playerId)
    refetch()
  }

  // Group by provider
  const grouped = {}
  for (const m of data.models) {
    if (!grouped[m.provider]) grouped[m.provider] = []
    grouped[m.provider].push(m)
  }

  return (
    <div className="model-control">
      {/* Global Controls */}
      <div className="card">
        <div className="card-header">
          <h2>Global Controls</h2>
          <div className="mc-summary">
            <span className="mc-stat">
              <span className="mc-stat-label">Session Cost</span>
              <span className="mc-stat-value">${data.grand_total_cost.toFixed(4)}</span>
            </span>
            <span className="mc-stat">
              <span className="mc-stat-label">Total Calls</span>
              <span className="mc-stat-value">
                {data.models.reduce((s, m) => s + m.api_calls_today, 0)}
              </span>
            </span>
          </div>
        </div>
        <div className="mc-pause-all">
          <button
            className={`mc-pause-btn ${data.pause_all ? 'paused' : 'active'}`}
            onClick={handlePauseAll}
          >
            {data.pause_all ? '\u25B6 Resume All Scanning' : '\u23F8 Pause All Scanning'}
          </button>
          {data.pause_all && (
            <div className="mc-pause-warning">All AI scanning is paused. Models will not analyze stocks.</div>
          )}
          <button
            className={`mc-pause-btn ${data.fallbacks_enabled ? 'active' : 'paused'}`}
            onClick={handleFallbackToggle}
            title="When ON, paused paid models automatically route to a free local Ollama model"
          >
            {data.fallbacks_enabled ? '\u26A1 Fallbacks ON' : '\u26A1 Fallbacks OFF'}
          </button>
          {data.fallbacks_enabled && (
            <div className="mc-pause-warning" style={{ color: '#eab308' }}>
              Paused models will use free local Ollama fallbacks instead of stopping.
            </div>
          )}
          <button
            className={`mc-scan-btn ${scanning ? 'scanning' : ''}`}
            onClick={handleForceScan}
            disabled={scanning}
          >
            {scanning ? 'Scanning...' : '\u{1F50D} Manual Scan Now'}
          </button>
          {scanMsg && <div className="mc-scan-msg">{scanMsg}</div>}
        </div>
      </div>

      {/* Models by Provider */}
      {Object.entries(grouped).map(([provider, models]) => {
        const isCollapsed = collapsed[provider]
        return (
        <div className="card" key={provider}>
          <div className="card-header" style={{ cursor: 'pointer' }}
            onClick={() => setCollapsed(prev => ({ ...prev, [provider]: !prev[provider] }))}>
            <h2 style={{ color: PROVIDER_COLORS[provider] || '#fff' }}>
              <span style={{ display: 'inline-block', width: 16, fontSize: 10, marginRight: 4 }}>
                {isCollapsed ? '\u25B6' : '\u25BC'}
              </span>
              {provider.charAt(0).toUpperCase() + provider.slice(1)}
            </h2>
            <span className="card-badge">
              {models.filter(m => !m.is_paused || m.is_fallback).length}/{models.length} active
              {models.some(m => m.is_fallback) && (
                <span style={{ color: '#eab308', marginLeft: 4 }}>
                  ({models.filter(m => m.is_fallback).length} fallback)
                </span>
              )}
            </span>
          </div>
          {!isCollapsed && <div className="mc-models">
            {models.map(m => (
              <div
                key={m.player_id}
                className={`mc-model-row ${m.is_paused ? 'paused' : ''} ${data.pause_all ? 'global-paused' : ''}`}
              >
                <div className="mc-model-info">
                  <div className="mc-model-name">
                    <span
                      className="mc-provider-dot"
                      style={{ background: PROVIDER_COLORS[m.provider] || '#666' }}
                    />
                    {m.display_name}
                    {m.is_fallback && (
                      <span style={{
                        marginLeft: 8, fontSize: 10, fontWeight: 700,
                        background: '#78350f', color: '#fbbf24',
                        padding: '1px 6px', borderRadius: 4, letterSpacing: 1,
                      }}>FALLBACK</span>
                    )}
                  </div>
                  <div className="mc-model-id">
                    {m.is_fallback && m.fallback_model
                      ? <span style={{ color: '#fbbf24' }}>{m.fallback_model} (free)</span>
                      : m.model_id
                    }
                  </div>
                </div>

                <div className="mc-model-costs">
                  <div className="mc-cost-item">
                    <span className="mc-cost-label">Per Scan</span>
                    <span className={`mc-cost-value ${m.cost_per_scan === 0 ? 'free' : ''}`}>
                      {m.cost_per_scan === 0 ? 'FREE' : `$${m.cost_per_scan.toFixed(3)}`}
                    </span>
                  </div>
                  <div className="mc-cost-item">
                    <span className="mc-cost-label">Today</span>
                    <span className="mc-cost-value">${m.total_cost_today.toFixed(4)}</span>
                  </div>
                  <div className="mc-cost-item">
                    <span className="mc-cost-label">Calls</span>
                    <span className="mc-cost-value">{m.api_calls_today}</span>
                  </div>
                  {roiMap[m.player_id] && (
                    <div className="mc-cost-item">
                      <span className="mc-cost-label">Net P&L</span>
                      <span className={`mc-cost-value ${roiMap[m.player_id].net_pnl >= 0 ? 'positive' : 'negative'}`}>
                        {roiMap[m.player_id].net_pnl >= 0 ? '+' : ''}${roiMap[m.player_id].net_pnl.toFixed(0)}
                      </span>
                    </div>
                  )}
                  {gradeMap[m.player_id] && (
                    <div className="mc-cost-item">
                      <span className="mc-cost-label">Grade</span>
                      <span className="mc-cost-value" style={{
                        color: {A:'#22c55e',B:'#84cc16',C:'#eab308',D:'#f97316',F:'#ef4444'}[gradeMap[m.player_id].grade] || '#fff',
                        fontWeight: 800, fontSize: 16,
                      }}>
                        {gradeMap[m.player_id].grade}
                      </span>
                    </div>
                  )}
                </div>

                <div className="mc-model-toggle">
                  {m.can_pause === false ? (
                    <button className="mc-toggle-btn paused" disabled title="Control Neo from port 8000">
                      <span className="mc-toggle-track">
                        <span className="mc-toggle-thumb" />
                      </span>
                      <span className="mc-toggle-label">Matrix / Read Only</span>
                    </button>
                  ) : (
                    <button
                      className={`mc-toggle-btn ${m.is_paused ? 'paused' : 'active'}`}
                      onClick={() => handleToggle(m.player_id)}
                      title={m.is_paused ? 'Resume' : 'Pause'}
                    >
                      <span className="mc-toggle-track">
                        <span className="mc-toggle-thumb" />
                      </span>
                      <span className="mc-toggle-label">
                        {m.is_fallback ? 'Fallback' : m.is_paused ? 'Paused' : 'Active'}
                      </span>
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>}
        </div>
        )
      })}
    </div>
  )
}
