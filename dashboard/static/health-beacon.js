// Server Health Beacon - Pings API every 10 seconds
(function() {
  const PING_INTERVAL = 10000;
  const API_URL = '/api/status';  // Changed to working endpoint
  
  const beacon = document.createElement('div');
  beacon.id = 'serverHealthBeacon';
  beacon.innerHTML = '<span class="health-dot"></span><span class="health-text">Checking...</span>';
  beacon.style.cssText = 'display:flex;align-items:center;gap:6px;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;cursor:default;transition:all 0.3s ease;';
  
  const navRight = document.querySelector('.nav-right');
  if (navRight) navRight.insertBefore(beacon, navRight.firstChild);
  
  const style = document.createElement('style');
  style.textContent = `
    #serverHealthBeacon .health-dot { width:8px;height:8px;border-radius:50%;transition:all 0.3s ease; }
    #serverHealthBeacon.online { background:rgba(34,197,94,0.15);border:1px solid rgba(34,197,94,0.3); }
    #serverHealthBeacon.online .health-dot { background:#22c55e;box-shadow:0 0 8px #22c55e;animation:pulse-green 2s infinite; }
    #serverHealthBeacon.online .health-text { color:#22c55e; }
    #serverHealthBeacon.offline { background:rgba(239,68,68,0.15);border:1px solid rgba(239,68,68,0.3); }
    #serverHealthBeacon.offline .health-dot { background:#ef4444;box-shadow:0 0 12px #ef4444;animation:pulse-red 0.5s infinite; }
    #serverHealthBeacon.offline .health-text { color:#ef4444; }
    @keyframes pulse-green { 0%,100%{opacity:1;transform:scale(1);} 50%{opacity:0.7;transform:scale(1.1);} }
    @keyframes pulse-red { 0%,100%{opacity:1;} 50%{opacity:0.5;} }
  `;
  document.head.appendChild(style);
  
  async function checkHealth() {
    const dot = beacon.querySelector('.health-dot');
    const text = beacon.querySelector('.health-text');
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const response = await fetch(API_URL, { signal: controller.signal, cache: 'no-store' });
      clearTimeout(timeout);
      if (response.ok) {
        beacon.className = 'online';
        text.textContent = 'All Systems Online';
      } else { throw new Error('Bad response'); }
    } catch (err) {
      beacon.className = 'offline';
      text.textContent = '⚠️ SERVER DOWN';
    }
  }
  
  checkHealth();
  setInterval(checkHealth, PING_INTERVAL);
  console.log('[Health Beacon] Monitoring /api/status every 10s');
})();
