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
