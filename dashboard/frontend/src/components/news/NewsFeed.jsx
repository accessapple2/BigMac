import React, { useState, useCallback } from 'react'
import { usePolling } from '../../hooks/usePolling'
import { api } from '../../api/client'
import { timeAgo, formatTimeAZ } from '../../utils/time'

const SYMBOL_COLORS = {
  SPY: '#3b82f6',
  QQQ: '#a855f7',
  NVDA: '#22c55e',
  TSLA: '#ef4444',
  AAPL: '#94a3b8',
}

export default function NewsFeed() {
  const [filter, setFilter] = useState('all')
  const fetchNews = useCallback(() => api.getRecentNews(50), [])
  const { data: news, loading } = usePolling(fetchNews, 30000)

  if (loading) return <div className="loading">Loading news...</div>

  const symbols = ['all', 'SPY', 'QQQ', 'NVDA', 'TSLA', 'AAPL']
  const filtered = filter === 'all' ? (news || []) : (news || []).filter(n => n.symbol === filter)

  return (
    <div className="card">
      <div className="card-header">
        <h2>Market News</h2>
        <span className="card-badge">{filtered.length} articles</span>
      </div>

      {/* Symbol filter tabs */}
      <div className="news-filters">
        {symbols.map(s => (
          <button
            key={s}
            className={`filter-btn ${filter === s ? 'active' : ''}`}
            onClick={() => setFilter(s)}
            style={filter === s && s !== 'all' ? { borderColor: SYMBOL_COLORS[s] } : {}}
          >
            {s === 'all' ? 'All' : s}
          </button>
        ))}
      </div>

      {filtered.length === 0 ? (
        <div className="empty-state">No news yet. News will appear after the first fetch cycle (~5 min).</div>
      ) : (
        <div className="news-list">
          {filtered.map((item, i) => (
            <div key={i} className="news-item">
              <div className="news-symbol-tag" style={{ color: SYMBOL_COLORS[item.symbol] || '#94a3b8' }}>
                {item.symbol}
              </div>
              <div className="news-content">
                <a
                  href={item.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="news-headline"
                >
                  {item.headline}
                </a>
                {item.summary && (
                  <div className="news-summary">{item.summary.substring(0, 150)}</div>
                )}
                <div className="news-meta">
                  <span>{item.source}</span>
                  <span>{formatTimeAZ(item.fetched_at)} · {timeAgo(item.fetched_at)}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
