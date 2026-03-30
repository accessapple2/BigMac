import { useState, useEffect, useCallback, useRef } from 'react'

// Global auto-refresh state (shared across all hooks)
let _autoRefresh = true
const _listeners = new Set()

export function getAutoRefresh() { return _autoRefresh }
export function setAutoRefresh(val) {
  _autoRefresh = val
  _listeners.forEach(fn => fn(val))
}
export function onAutoRefreshChange(fn) {
  _listeners.add(fn)
  return () => _listeners.delete(fn)
}

export function usePolling(fetchFn, interval = 5000, { enabled = true } = {}) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [autoOn, setAutoOn] = useState(_autoRefresh)

  // Listen for global toggle changes
  useEffect(() => {
    return onAutoRefreshChange(setAutoOn)
  }, [])

  const refresh = useCallback(async () => {
    try {
      const result = await fetchFn()
      setData(result)
      setError(null)
      setLastUpdated(new Date())
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [fetchFn])

  // Renamed to 'refetch' for manual trigger
  const refetch = refresh

  useEffect(() => {
    refresh() // Always fetch once on mount
    if (!enabled || !autoOn) return
    const id = setInterval(refresh, interval)
    return () => clearInterval(id)
  }, [refresh, interval, enabled, autoOn])

  return { data, error, loading, refresh, refetch, lastUpdated }
}
