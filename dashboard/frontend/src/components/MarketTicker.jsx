import React, { useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

export default function MarketTicker() {
  const fetchPrices = useCallback(() => api.getMarketPrices(), [])
  const { data: prices } = usePolling(fetchPrices, 15000)

  if (!prices || Object.keys(prices).length === 0) return null

  return (
    <div className="market-ticker">
      {Object.entries(prices).map(([symbol, data]) => (
        <div key={symbol} className="ticker-item">
          <span className="ticker-symbol">{symbol}</span>
          <span className="ticker-price">${data.price?.toFixed(2)}</span>
          <span className={`ticker-change ${data.change_pct >= 0 ? 'positive' : 'negative'}`}>
            {data.change_pct >= 0 ? '+' : ''}{data.change_pct?.toFixed(2)}%
          </span>
        </div>
      ))}
    </div>
  )
}
