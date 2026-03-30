import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

function formatMktCap(val) {
  if (!val) return '-'
  if (val >= 1e12) return `$${(val / 1e12).toFixed(1)}T`
  if (val >= 1e9) return `$${(val / 1e9).toFixed(1)}B`
  if (val >= 1e6) return `$${(val / 1e6).toFixed(0)}M`
  return `$${val.toLocaleString()}`
}

function fmtPct(val) {
  if (val == null) return '-'
  const color = val > 0 ? 'var(--green)' : val < 0 ? 'var(--red)' : 'var(--text-secondary)'
  return <span style={{ color }}>{val > 0 ? '+' : ''}{val.toFixed(1)}%</span>
}

function fmtNum(val) {
  if (val == null) return '-'
  return val.toFixed(2)
}

function GradeBadge({ grade, score }) {
  const colors = {
    A: { bg: '#0f5132', text: '#75b798' },
    B: { bg: '#1a3a2a', text: '#6ec891' },
    C: { bg: '#3a3a1a', text: '#c8c86e' },
    D: { bg: '#3a2a1a', text: '#c8916e' },
    F: { bg: '#5f1d1d', text: '#f08080' },
  }
  const c = colors[grade] || colors.C
  return (
    <span style={{
      background: c.bg, color: c.text, padding: '2px 10px',
      borderRadius: '4px', fontSize: '12px', fontWeight: 700, letterSpacing: '0.5px'
    }}>
      {grade} ({score})
    </span>
  )
}

function ShortBadge({ pct }) {
  if (pct == null) return <span style={{ color: '#666' }}>-</span>
  const color = pct < 5 ? '#6ec891' : pct < 10 ? '#c8c86e' : pct < 20 ? '#c8916e' : '#f08080'
  return <span style={{ color, fontWeight: 600 }}>{pct.toFixed(1)}%</span>
}

function RecBadge({ rec }) {
  if (!rec) return <span style={{ color: '#666' }}>-</span>
  const colors = {
    buy: '#6ec891', strongBuy: '#75b798', strong_buy: '#75b798',
    hold: '#c8c86e', sell: '#f08080', strongSell: '#f08080', strong_sell: '#f08080',
    underperform: '#c8916e', overweight: '#6ec891',
  }
  const color = colors[rec] || '#999'
  return <span style={{ color, fontWeight: 600, textTransform: 'uppercase', fontSize: '11px' }}>{rec.replace(/([A-Z])/g, ' $1').trim()}</span>
}

export default function Fundamentals() {
  const { data: fundamentals } = usePolling(api.getFundamentals, 60000)
  const [expandedSymbol, setExpandedSymbol] = useState(null)
  const [sortKey, setSortKey] = useState('smart_score')
  const [sortDir, setSortDir] = useState(-1) // -1 = descending

  const sorted = [...(fundamentals || [])].sort((a, b) => {
    const av = a[sortKey] ?? -Infinity
    const bv = b[sortKey] ?? -Infinity
    return (av - bv) * sortDir
  })

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(d => d * -1)
    } else {
      setSortKey(key)
      setSortDir(-1)
    }
  }

  const SortTh = ({ k, children }) => (
    <th style={{ ...thStyle, cursor: 'pointer', userSelect: 'none' }} onClick={() => handleSort(k)}>
      {children} {sortKey === k ? (sortDir > 0 ? '\u25B2' : '\u25BC') : ''}
    </th>
  )

  return (
    <div>
      <div className="card" style={{ marginBottom: '16px' }}>
        <div className="card-header">
          <h2>Stock Fundamentals</h2>
          <span className="card-badge">{fundamentals?.length || 0} stocks</span>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #333', textAlign: 'left' }}>
                <SortTh k="symbol">Symbol</SortTh>
                <SortTh k="smart_score">Score</SortTh>
                <th style={thStyle}>Sector</th>
                <SortTh k="market_cap">Mkt Cap</SortTh>
                <SortTh k="pe_trailing">P/E</SortTh>
                <SortTh k="revenue_growth">Rev Growth</SortTh>
                <SortTh k="profit_margin">Net Margin</SortTh>
                <SortTh k="debt_to_equity">D/E</SortTh>
                <SortTh k="short_pct_float">Short %</SortTh>
                <SortTh k="analyst_upside">Analyst</SortTh>
                <SortTh k="days_to_earnings">Earnings</SortTh>
                <th style={thStyle}>Rec</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map(stock => (
                <tr key={stock.symbol}
                  style={{ borderBottom: '1px solid #222', cursor: 'pointer' }}
                  onClick={() => setExpandedSymbol(expandedSymbol === stock.symbol ? null : stock.symbol)}
                >
                  <td style={tdStyle}>
                    <span style={{ fontWeight: 700, color: 'var(--accent)' }}>{stock.symbol}</span>
                    <div style={{ fontSize: '11px', color: '#888' }}>{stock.company_name}</div>
                  </td>
                  <td style={tdStyle}>
                    {stock.grade ? <GradeBadge grade={stock.grade} score={stock.smart_score} /> : '-'}
                  </td>
                  <td style={tdStyle}><span style={{ fontSize: '12px' }}>{stock.sector}</span></td>
                  <td style={tdStyle}>{formatMktCap(stock.market_cap)}</td>
                  <td style={tdStyle}>{fmtNum(stock.pe_trailing)}</td>
                  <td style={tdStyle}>{fmtPct(stock.revenue_growth)}</td>
                  <td style={tdStyle}>{fmtPct(stock.profit_margin)}</td>
                  <td style={tdStyle}>{fmtNum(stock.debt_to_equity)}</td>
                  <td style={tdStyle}><ShortBadge pct={stock.short_pct_float} /></td>
                  <td style={tdStyle}>
                    {stock.analyst_upside != null ? (
                      <span style={{ color: stock.analyst_upside > 0 ? 'var(--green)' : 'var(--red)', fontWeight: 600 }}>
                        {stock.analyst_upside > 0 ? '+' : ''}{stock.analyst_upside.toFixed(1)}%
                      </span>
                    ) : '-'}
                    {stock.target_mean && <div style={{ fontSize: '11px', color: '#888' }}>${stock.target_mean.toFixed(0)}</div>}
                  </td>
                  <td style={tdStyle}>
                    {stock.days_to_earnings != null && stock.days_to_earnings >= 0 ? (
                      <span style={{ color: stock.days_to_earnings <= 3 ? '#f08080' : stock.days_to_earnings <= 7 ? '#c8c86e' : '#999', fontWeight: stock.days_to_earnings <= 7 ? 700 : 400 }}>
                        {stock.days_to_earnings}d
                      </span>
                    ) : '-'}
                    {stock.next_earnings && <div style={{ fontSize: '10px', color: '#666' }}>{stock.next_earnings}</div>}
                  </td>
                  <td style={tdStyle}><RecBadge rec={stock.recommendation} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {expandedSymbol && (
        <ExpandedDetail
          symbol={expandedSymbol}
          fundamental={sorted.find(f => f.symbol === expandedSymbol)}
          onClose={() => setExpandedSymbol(null)}
        />
      )}
    </div>
  )
}

function ExpandedDetail({ symbol, fundamental, onClose }) {
  const { data: filings } = usePolling(() => api.getFilings(symbol), 120000)
  const f = fundamental
  if (!f) return null

  const sc = f.score_components || {}

  return (
    <div className="card" style={{ marginBottom: '16px' }}>
      <div className="card-header">
        <h2>
          {symbol} — {f.company_name}
          {f.grade && <span style={{ marginLeft: 12 }}><GradeBadge grade={f.grade} score={f.smart_score} /></span>}
        </h2>
        <button onClick={onClose} style={{ background: 'none', border: 'none', color: '#999', cursor: 'pointer', fontSize: '18px' }}>x</button>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px', padding: '16px' }}>

        {/* Smart Score Breakdown */}
        {Object.keys(sc).length > 0 && (
          <div style={sectionStyle}>
            <h3 style={sectionTitle}>Smart Score Breakdown</h3>
            {Object.entries(sc).map(([key, comp]) => (
              <div key={key} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0', borderBottom: '1px solid #1a1a1a' }}>
                <span style={{ color: '#999', fontSize: '13px', textTransform: 'capitalize' }}>{key.replace('_', ' ')}</span>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{ width: 80, height: 6, background: '#333', borderRadius: 3, overflow: 'hidden' }}>
                    <div style={{ width: `${(comp.score / comp.max) * 100}%`, height: '100%', background: comp.score / comp.max > 0.7 ? '#6ec891' : comp.score / comp.max > 0.4 ? '#c8c86e' : '#f08080', borderRadius: 3 }} />
                  </div>
                  <span style={{ color: '#ddd', fontSize: '12px', fontWeight: 600, minWidth: 40, textAlign: 'right' }}>{comp.score}/{comp.max}</span>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Valuation */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Valuation</h3>
          <MetricRow label="P/E (TTM)" value={fmtNum(f.pe_trailing)} />
          <MetricRow label="P/E (Forward)" value={fmtNum(f.pe_forward)} />
          <MetricRow label="PEG Ratio" value={fmtNum(f.peg_ratio)} />
          <MetricRow label="Price/Book" value={fmtNum(f.price_to_book)} />
          <MetricRow label="Beta" value={fmtNum(f.beta)} />
          <MetricRow label="52W Range" value={f.week52_low && f.week52_high ? `$${f.week52_low.toFixed(0)} - $${f.week52_high.toFixed(0)}` : '-'} />
          {f.week52_pct != null && <MetricRow label="52W Position" value={`${f.week52_pct.toFixed(0)}%`} />}
        </div>

        {/* Growth */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Growth</h3>
          <MetricRow label="Revenue Growth" value={fmtPct(f.revenue_growth)} />
          <MetricRow label="Earnings Growth" value={fmtPct(f.earnings_growth)} />
          <MetricRow label="EPS (TTM)" value={f.eps_trailing != null ? `$${fmtNum(f.eps_trailing)}` : '-'} />
          <MetricRow label="EPS (Forward)" value={f.eps_forward != null ? `$${fmtNum(f.eps_forward)}` : '-'} />
        </div>

        {/* Margins & Returns */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Margins & Returns</h3>
          <MetricRow label="Gross Margin" value={fmtPct(f.gross_margin)} />
          <MetricRow label="Operating Margin" value={fmtPct(f.operating_margin)} />
          <MetricRow label="Net Margin" value={fmtPct(f.profit_margin)} />
          <MetricRow label="EBITDA Margin" value={fmtPct(f.ebitda_margin)} />
          <MetricRow label="ROE" value={fmtPct(f.roe)} />
          <MetricRow label="ROA" value={fmtPct(f.roa)} />
        </div>

        {/* Financial Health */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Financial Health</h3>
          <MetricRow label="Debt/Equity" value={fmtNum(f.debt_to_equity)} />
          <MetricRow label="Current Ratio" value={fmtNum(f.current_ratio)} />
          <MetricRow label="Quick Ratio" value={fmtNum(f.quick_ratio)} />
          <MetricRow label="Free Cash Flow" value={formatMktCap(f.free_cash_flow)} />
          <MetricRow label="Total Cash" value={formatMktCap(f.total_cash)} />
          <MetricRow label="Total Debt" value={formatMktCap(f.total_debt)} />
          <MetricRow label="Div Yield" value={f.dividend_yield != null ? `${f.dividend_yield.toFixed(2)}%` : '-'} />
        </div>

        {/* Short Interest */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Short Interest</h3>
          <MetricRow label="Short % of Float" value={f.short_pct_float != null ? <ShortBadge pct={f.short_pct_float} /> : '-'} />
          <MetricRow label="Days to Cover" value={fmtNum(f.short_ratio)} />
          <MetricRow label="Shares Short" value={f.shares_short ? (f.shares_short / 1e6).toFixed(1) + 'M' : '-'} />
          <MetricRow label="Float" value={f.float_shares ? (f.float_shares / 1e9).toFixed(2) + 'B' : '-'} />
        </div>

        {/* Analyst Consensus */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Analyst Consensus</h3>
          <MetricRow label="Target High" value={f.target_high ? `$${f.target_high.toFixed(2)}` : '-'} />
          <MetricRow label="Target Mean" value={f.target_mean ? `$${f.target_mean.toFixed(2)}` : '-'} />
          <MetricRow label="Target Low" value={f.target_low ? `$${f.target_low.toFixed(2)}` : '-'} />
          <MetricRow label="Upside" value={f.analyst_upside != null ? fmtPct(f.analyst_upside) : '-'} />
          <MetricRow label="Recommendation" value={<RecBadge rec={f.recommendation} />} />
          <MetricRow label="# Analysts" value={f.num_analysts || '-'} />
          {f.rec_summary && Object.keys(f.rec_summary).length > 0 && (
            <div style={{ display: 'flex', gap: 4, marginTop: 8, flexWrap: 'wrap' }}>
              {[['strong_buy', '#0f5132', '#75b798'], ['buy', '#1a3a2a', '#6ec891'], ['hold', '#3a3a1a', '#c8c86e'], ['sell', '#3a1a1a', '#c86e6e'], ['strong_sell', '#5f1d1d', '#f08080']].map(([k, bg, txt]) => (
                <span key={k} style={{ background: bg, color: txt, padding: '2px 6px', borderRadius: 3, fontSize: '10px', fontWeight: 600 }}>
                  {k.replace('_', ' ').toUpperCase()}: {f.rec_summary[k] || 0}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Ownership */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Ownership</h3>
          <MetricRow label="Institutional %" value={f.institutional_pct != null ? `${f.institutional_pct.toFixed(1)}%` : '-'} />
          <MetricRow label="Insider %" value={f.insider_pct != null ? `${f.insider_pct.toFixed(1)}%` : '-'} />
          <MetricRow label="# Institutions" value={f.institutions_count ? f.institutions_count.toLocaleString() : '-'} />
        </div>

        {/* Earnings */}
        <div style={sectionStyle}>
          <h3 style={sectionTitle}>Earnings</h3>
          <MetricRow label="Next Earnings" value={f.next_earnings || '-'} />
          <MetricRow label="Days Until" value={f.days_to_earnings != null ? (
            <span style={{ color: f.days_to_earnings <= 3 ? '#f08080' : f.days_to_earnings <= 7 ? '#c8c86e' : '#ddd', fontWeight: f.days_to_earnings <= 7 ? 700 : 400 }}>
              {f.days_to_earnings} days
            </span>
          ) : '-'} />
          <MetricRow label="EPS Estimate" value={f.eps_estimate != null ? `$${f.eps_estimate.toFixed(2)}` : '-'} />
        </div>

        {/* SEC Filings */}
        {filings && filings.length > 0 && (
          <div style={{ ...sectionStyle, gridColumn: 'span 2' }}>
            <h3 style={sectionTitle}>Recent SEC Filings</h3>
            {filings.slice(0, 6).map((filing, i) => (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid #1a1a1a' }}>
                <span style={{ fontWeight: 600, color: '#ddd', fontSize: '12px', minWidth: '60px' }}>{filing.type}</span>
                <span style={{ color: '#999', fontSize: '12px', flex: 1, marginLeft: '12px' }}>{filing.description}</span>
                <span style={{ color: '#666', fontSize: '11px' }}>{filing.date}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function MetricRow({ label, value }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid #1a1a1a' }}>
      <span style={{ color: '#999', fontSize: '13px' }}>{label}</span>
      <span style={{ color: '#ddd', fontSize: '13px', fontWeight: 500 }}>{value}</span>
    </div>
  )
}

const thStyle = { padding: '8px 12px', color: '#888', fontSize: '12px', fontWeight: 600, whiteSpace: 'nowrap' }
const tdStyle = { padding: '8px 12px', fontSize: '13px', color: '#ddd' }
const sectionStyle = { background: '#1a1a1a', borderRadius: '8px', padding: '12px' }
const sectionTitle = { color: '#aaa', fontSize: '13px', fontWeight: 600, marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '0.5px' }
