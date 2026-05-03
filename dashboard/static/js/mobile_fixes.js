/**
 * mobile_fixes.js — OllieTrades mobile UX
 * Escape key closes Archer, touch feedback on floating button,
 * and card collapse pattern for mobile compactness.
 */
(function () {
    'use strict';

    // Escape key → close Archer panel
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            var panel = document.getElementById('computerChatPanel');
            if (panel && panel.classList.contains('open')) {
                if (typeof toggleComputerChat === 'function') {
                    toggleComputerChat();
                }
            }
        }
    });

    // Touch feedback: brief scale-down on tap
    document.addEventListener('DOMContentLoaded', function () {
        var btn = document.getElementById('computerChatBtn');
        if (!btn) return;
        btn.addEventListener('touchstart', function () {
            btn.style.transform = 'scale(0.93)';
        }, { passive: true });
        btn.addEventListener('touchend', function () {
            btn.style.transform = '';
        }, { passive: true });
    });

    /* ── Mobile Card Collapser ──────────────────────────────────────
       Only activates on screens ≤ 768px.
       Adds a small ▶/▾ button to each targeted card header.
       The button uses stopPropagation so it never triggers any
       existing onclick handlers (toggleBridgeVote, toggleFleetReport,
       toggleTroiBridge, etc.) — those all still work normally when
       the user taps the header itself after the card is expanded.
    ───────────────────────────────────────────────────────────────── */
    document.addEventListener('DOMContentLoaded', function () {
        if (window.innerWidth > 768) return;

        // Cards to make collapsible on mobile.
        // open: true  → starts expanded  (primary/key cards)
        // open: false → starts collapsed (secondary/verbose cards)
        var TARGETS = [
            { id: 'glance-card',            open: true  }, // Bridge Status — primary view
            { id: 'game-plan-card',         open: false },
            { id: 'fleet-report-card',      open: false },
            { id: 'troi-advisory-card',     open: false },
            { id: 'bridge-vote-card',       open: false },
            { id: 'volume-radar-card',      open: false },
            { id: 'gex-overlay-card',       open: false },
            { id: 'gex-card',              open: false },
            { id: 'battle-station-card',    open: false },
            { id: 'bs0dte-card',            open: false },
            { id: 'recovery-card',          open: false },
            { id: 'kirk-advisory-card',     open: false },
            { id: 'tax-alpha-card',         open: false },
            { id: 'risk-vitals-card',       open: false },
            { id: 'generated-indexes-card', open: false },
            { id: 'bridge-heatmap',         open: false },
            { id: 'bridge-fear-greed',      open: false },
        ];

        TARGETS.forEach(function (t) {
            var card = document.getElementById(t.id);
            if (!card) return;

            // Find the header: prefer .card-header, fall back to first element child
            var hdr = card.querySelector('.card-header') || card.firstElementChild;
            if (!hdr) return;

            // Mark the card
            card.classList.add('mob-collapse-target');
            if (!t.open) card.classList.add('mob-collapsed');

            // Build the tiny collapse button
            var colBtn = document.createElement('button');
            colBtn.className = 'mob-col-btn';
            colBtn.setAttribute('aria-label', 'Toggle section');
            colBtn.setAttribute('type', 'button');
            colBtn.textContent = t.open ? '▾' : '▶';

            // stopPropagation so we don't fire the card's own onclick handler
            colBtn.addEventListener('click', function (e) {
                e.stopPropagation();
                var nowCollapsed = card.classList.toggle('mob-collapsed');
                colBtn.textContent = nowCollapsed ? '▶' : '▾';
            });

            // Prepend before existing header content
            hdr.insertBefore(colBtn, hdr.firstChild);
        });

        // Tactical display <details> — close by default on mobile
        // (it has `open` attribute so it starts expanded on desktop)
        var tacDetails = document.getElementById('tactical-card-details');
        if (tacDetails) tacDetails.removeAttribute('open');

        // Earnings banner — hide entirely on mobile if content is empty.
        // The banner is already display:none by default; it's shown by JS
        // when earnings data loads. If earningsContent is blank, keep hidden.
        var earningsBanner  = document.getElementById('earningsBanner');
        var earningsContent = document.getElementById('earningsContent');
        if (earningsBanner && earningsContent) {
            // Watch for when JS populates it, then check again
            var _ebObs = new MutationObserver(function () {
                if (!earningsContent.textContent.trim()) {
                    earningsBanner.style.setProperty('display', 'none', 'important');
                }
            });
            _ebObs.observe(earningsContent, { childList: true, characterData: true, subtree: true });
        }
    });

}());
