// Market Closed Handler - Replace Loading with Market Closed on weekends/after hours
(function() {
  function isMarketOpen() {
    const now = new Date();
    const day = now.getDay();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const time = hour * 100 + minute;
    if (day === 0 || day === 6) return false;
    if (time < 630 || time > 1300) return false;
    return true;
  }
  
  function updateLoadingElements() {
    if (isMarketOpen()) return;

    document.querySelectorAll('*').forEach(el => {
      if (el.children.length === 0) {
        // HM-FEAR-GREED-FIX 2026-05-22: scope skip — F&G and Market Breadth
        // are always-on indicators (CNN F&G publishes continuously, breadth
        // has historical fallback) and were collateral damage of the
        // startsWith('loading') rewrite. Two opt-outs:
        //   1. Inside #section-fear-greed (hardcoded for legacy compat)
        //   2. Inside any element marked data-no-market-gate (future contract)
        if (el.closest('#section-fear-greed')
            || el.closest('[data-no-market-gate]')) {
          return;
        }
        const text = el.textContent.trim().toLowerCase();
        if (text === 'loading…' || text === 'loading...' || text === 'loading' || text.startsWith('loading')) {
          el.innerHTML = '<span style="color: #f59e0b;">📅 Market Closed</span>';
        }
      }
    });
  }
  
  // Run multiple times to catch late-loading elements
  setTimeout(updateLoadingElements, 1000);
  setTimeout(updateLoadingElements, 2000);
  setTimeout(updateLoadingElements, 3000);
  setTimeout(updateLoadingElements, 5000);
  setTimeout(updateLoadingElements, 8000);
  setInterval(updateLoadingElements, 10000);
  
  console.log('[Market Closed] Active - Market open:', isMarketOpen());
})();
