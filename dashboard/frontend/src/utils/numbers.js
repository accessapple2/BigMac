/**
 * Portfolio Display & State Helpers
 *
 * Purpose:
 * Centralize how portfolios are identified, labeled, and displayed across the UI.
 *
 * Key Concepts:
 * - Agents (e.g., Ray, Anderson) generate signals
 * - Portfolios hold capital and determine execution behavior
 *
 * Special Handling:
 * - Metals (Physical Holdings):
 *     - Internal names: "Computer", "Enterprise Computer"
 *     - Display name: "Metals — Tracking Only (Physical)"
 *     - execution_mode = "tracking"
 *     - type = "physical"
 *     - Never executes trades
 *
 * Helpers:
 * - getPortfolioDisplayName(portfolio)
 *     Normalizes portfolio names for UI display
 *
 * - isTrackingOnlyPortfolio(portfolio)
 *     Returns true for portfolios that should not execute trades
 *
 * Guarantees:
 * - Display naming is consistent across all UI surfaces
 * - Tracking-only portfolios are clearly identified
 * - Agent names are never conflated with portfolio names
 */
export function safeNumber(value, fallback = 0) {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

export function formatMoney(value) {
  const amount = safeNumber(value, 0)
  return `$${amount.toFixed(2)}`
}

export function formatPercent(value, digits = 2, signed = false) {
  const pct = safeNumber(value, 0)
  return `${signed && pct >= 0 ? '+' : ''}${pct.toFixed(digits)}%`
}

export function clampPercent(value, min = 0, max = 100) {
  return Math.min(max, Math.max(min, safeNumber(value, min)))
}

/**
 * Leaderboard Math Helpers
 *
 * Purpose:
 * Ensure consistent and correct calculation of:
 * - current value
 * - total P&L
 * - day P&L
 * - return %
 *
 * Prevents:
 * - total P&L showing as day P&L
 * - incorrect spikes (e.g. +6000 in one day)
 * - inconsistent calculations across UI
 */
export function getCurrentValue(p) {
  return safeNumber(
    p?.current_equity ??
    p?.total_value ??
    p?.account_value ??
    p?.cash_plus_market_value ??
    p?.starting_capital,
    0,
  )
}

export function getTotalPnL(p) {
  const current = getCurrentValue(p)
  const start = safeNumber(p?.starting_capital, current)
  return current - start
}

export function getDayPnL(p) {
  // Preferred: use snapshot field if available
  if (typeof p?.day_pnl === 'number') {
    return p.day_pnl
  }

  // Fallback: use last known equity snapshot
  const current = getCurrentValue(p)
  const prev = safeNumber(p?.previous_equity, current)

  return current - prev
}

export function getReturnPct(p) {
  const current = getCurrentValue(p)
  const start = safeNumber(p?.starting_capital, current)
  if (start === 0) return 0
  return ((current - start) / start) * 100
}

export function getDisplayCapital(portfolio) {
  return getCurrentValue(portfolio)
}

export function isTrackingOnlyPortfolio(portfolio) {
  return portfolio?.execution_mode === 'tracking'
}

export function getPortfolioDisplayName(portfolio) {
  const rawName = portfolio?.name || ''
  if (rawName === 'Computer' || rawName === 'Enterprise Computer') {
    return 'Metals — Tracking Only (Physical)'
  }

  return rawName || 'Unknown Portfolio'
}
