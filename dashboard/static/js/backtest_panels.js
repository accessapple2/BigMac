/**
 * backtest_panels.js — Strategy Breakdown, Trade Log, Regime Analysis panels
 * Loaded by backtest_arena.html
 */

// ── Strategy Breakdown ────────────────────────────────────────────────────────
async function loadStrategyBreakdown(days) {
    days = days || 30;
    var c = document.getElementById('bt-strategy-content');
    if (!c) return;
    c.innerHTML = '<div class="bt-loading">Loading strategies…</div>';

    try {
        var r = await fetch('/api/backtest/strategies?days=' + days);
        var d = await r.json();
        var rows = d.strategies || [];

        if (!rows.length) {
            c.innerHTML = '<div class="bt-empty">No strategy data for last ' + days + ' days.</div>';
            return;
        }

        var html = '<table class="bt-table"><thead><tr>'
            + '<th>Grade</th><th>Strategy</th><th>Agent</th>'
            + '<th>Trades</th><th>Win%</th><th>PF</th><th>P&amp;L</th></tr></thead><tbody>';

        rows.slice(0, 15).forEach(function(s) {
            var gc = s.grade === 'A' ? 'bt-grade-a'
                   : s.grade === 'B' ? 'bt-grade-b'
                   : s.grade === 'F' ? 'bt-grade-f' : '';
            var pc = s.total_pnl >= 0 ? 'bt-pos' : 'bt-neg';
            html += '<tr>'
                + '<td class="' + gc + '">' + s.grade + '</td>'
                + '<td>' + s.strategy + '</td>'
                + '<td>' + s.agent + '</td>'
                + '<td>' + s.total_trades + '</td>'
                + '<td>' + s.win_rate + '%</td>'
                + '<td>' + s.profit_factor.toFixed(2) + '</td>'
                + '<td class="' + pc + '">' + (s.total_pnl >= 0 ? '+' : '') + s.total_pnl.toFixed(2) + '</td>'
                + '</tr>';
        });
        html += '</tbody></table>';
        c.innerHTML = html;
    } catch (e) {
        c.innerHTML = '<div class="bt-error">Error: ' + e.message + '</div>';
    }
}

// ── Trade Log ─────────────────────────────────────────────────────────────────
async function loadTradeLog(days, limit) {
    days  = days  || 7;
    limit = limit || 20;
    var c = document.getElementById('bt-tradelog-content');
    if (!c) return;
    c.innerHTML = '<div class="bt-loading">Loading trades…</div>';

    try {
        var r = await fetch('/api/backtest/trades?days=' + days + '&limit=' + limit);
        var d = await r.json();
        var s = d.summary || {};

        var pc = (s.total_pnl || 0) >= 0 ? 'bt-pos' : 'bt-neg';
        var html = '<div class="bt-summary">'
            + '<span>Total: <strong>' + (s.total_trades || 0) + '</strong></span>'
            + '<span>W: <strong class="bt-pos">' + (s.wins || 0) + '</strong></span>'
            + '<span>L: <strong class="bt-neg">' + (s.losses || 0) + '</strong></span>'
            + '<span>WR: <strong>' + (s.win_rate || 0) + '%</strong></span>'
            + '<span class="' + pc + '">P&amp;L: <strong>'
            + ((s.total_pnl || 0) >= 0 ? '+' : '') + (s.total_pnl || 0).toFixed(2)
            + '</strong></span></div>';

        html += '<div class="bt-trade-list">';
        (d.trades || []).forEach(function(t) {
            var icon = t.outcome === 'WIN' ? '●' : (t.outcome === 'LOSS' ? '●' : '○');
            var ic   = t.outcome === 'WIN' ? 'bt-pos' : (t.outcome === 'LOSS' ? 'bt-neg' : '');
            var rpct = (t.return_pct || 0);
            var rc   = rpct >= 0 ? 'bt-pos' : 'bt-neg';
            html += '<div class="bt-trade-row">'
                + '<span class="' + ic + '">' + icon + '</span>'
                + '<span class="bt-sym">' + t.symbol + '</span>'
                + '<span class="bt-act">' + t.action + '</span>'
                + '<span class="bt-px">' + (t.entry_price || '—') + ' → ' + (t.exit_price || '—') + '</span>'
                + '<span class="' + rc + '">' + (rpct >= 0 ? '+' : '') + rpct + '%</span>'
                + '<span class="bt-strat">' + (t.strategy || '') + '</span>'
                + '<span class="bt-agent">' + (t.agent || '') + '</span>'
                + '</div>';
        });
        html += '</div>';
        c.innerHTML = html;
    } catch (e) {
        c.innerHTML = '<div class="bt-error">Error: ' + e.message + '</div>';
    }
}

// ── Regime Analysis ───────────────────────────────────────────────────────────
async function loadRegimeAnalysis(days) {
    days = days || 90;
    var c = document.getElementById('bt-regime-content');
    if (!c) return;
    c.innerHTML = '<div class="bt-loading">Loading regime data…</div>';

    var emo = {BULL:'🟢', CAUTIOUS:'🟡', BEAR:'🔴', CRISIS:'⚫', NEUTRAL:'⚪', UNKNOWN:'⬜'};

    try {
        var r = await fetch('/api/backtest/regime?days=' + days);
        var d = await r.json();

        var html = '<div class="bt-regime-grid">';
        Object.entries(d.summary || {}).forEach(function(e) {
            var regime = e[0], st = e[1];
            if (!st.total_trades) return;
            var pc = st.total_pnl >= 0 ? 'bt-pos' : 'bt-neg';
            html += '<div class="bt-regime-card">'
                + '<div class="bt-regime-hdr">' + (emo[regime] || '⬜') + ' ' + regime + '</div>'
                + '<div class="bt-regime-stat">Trades: ' + st.total_trades + '</div>'
                + '<div class="bt-regime-stat">Win%: ' + st.win_rate + '%</div>'
                + '<div class="bt-regime-stat ' + pc + '">P&L: ' + (st.total_pnl >= 0 ? '+' : '') + st.total_pnl.toFixed(2) + '</div>'
                + '</div>';
        });
        html += '</div>';

        var best = d.best_agents || {};
        if (Object.keys(best).length) {
            html += '<div class="bt-section-sub"><strong>Top by Regime:</strong><ul>';
            Object.entries(best).forEach(function(e) {
                html += '<li>' + e[0] + ': ' + e[1].best_agent + ' (+$' + e[1].pnl.toFixed(0) + ')</li>';
            });
            html += '</ul></div>';
        }

        var recs = d.recommendations || [];
        if (recs.length) {
            html += '<div class="bt-section-sub"><strong>Insights:</strong><ul>';
            recs.forEach(function(rec) {
                html += '<li>' + (rec.type === 'WARNING' ? '⚠ ' : '→ ') + rec.message + '</li>';
            });
            html += '</ul></div>';
        }

        c.innerHTML = html;
    } catch (e) {
        c.innerHTML = '<div class="bt-error">Error: ' + e.message + '</div>';
    }
}

// ── Auto-load ─────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', function() {
    if (document.getElementById('bt-strategy-content')) loadStrategyBreakdown(30);
    if (document.getElementById('bt-tradelog-content')) loadTradeLog(7, 20);
    if (document.getElementById('bt-regime-content'))   loadRegimeAnalysis(90);
});
