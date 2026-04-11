"""
engine/ntfy.py — ntfy.sh push notifications for TradeMinds

Topic: https://ntfy.sh/ollietrades
No external dependencies — uses stdlib urllib.request only.
Fire-and-forget: all sends are in a daemon thread, never block the caller.
"""
import threading
import urllib.request
import urllib.error
import json

NTFY_URL = "https://ntfy.sh/ollietrades"

# Priority constants (ntfy.sh values)
P_MAX     = 5   # urgent — sound + persistent
P_HIGH    = 4
P_DEFAULT = 3
P_LOW     = 2
P_MIN     = 1

# Emoji tags for message types
_TAGS = {
    "buy":        "white_check_mark",
    "sell":       "moneybag",
    "tp1":        "green_circle",
    "tp2":        "large_green_circle",
    "tp3":        "trophy",
    "stop":       "red_circle",
    "trail_stop": "orange_circle",
    "time_stop":  "alarm_clock",
    "stop_loss":  "red_circle",
    "regime":     "compass",
    "restart":    "warning",
    "tpol_buy":   "crystal_ball",
    "tpol_sell":  "crystal_ball",
}


def _send(title: str, body: str, priority: int = P_DEFAULT, tags: str = "") -> None:
    """POST a single ntfy message. Called in a daemon thread."""
    try:
        data = json.dumps({"topic": "ollietrades",
                           "title": title,
                           "message": body,
                           "priority": priority,
                           "tags": [tags] if tags else []}).encode()
        req = urllib.request.Request(
            "https://ntfy.sh",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=6)
    except Exception:
        pass   # ntfy failures must never crash trading logic


def _fire(title: str, body: str, priority: int = P_DEFAULT, tags: str = "") -> None:
    """Spawn a daemon thread and return immediately."""
    t = threading.Thread(target=_send, args=(title, body, priority, tags), daemon=True)
    t.start()


# ── Public helpers ─────────────────────────────────────────────────────────

def notify_ollie_buy(symbol: str, price: float, qty: float,
                     grade: str, prob: float,
                     stop: float, tp1: float, tp2: float, tp3: float,
                     regime: str, source: str) -> None:
    title = f"Ollie BUY {symbol}"
    body  = (f"${price:.2f} × {qty:.2f} sh | {source.upper()} grade={grade} prob={prob:.0%}\n"
             f"Stop ${stop:.2f}  TP1 ${tp1:.2f}  TP2 ${tp2:.2f}  TP3 ${tp3:.2f}\n"
             f"Regime: {regime}")
    _fire(title, body, priority=P_HIGH, tags=_TAGS["buy"])


def notify_ollie_tp(action: str, symbol: str, price: float,
                    qty: float = 0.0, pnl: float = 0.0) -> None:
    """action: tp1 | tp2 | tp3 | trail_stop | stop_loss"""
    labels = {
        "tp1":        "TP1 — 50% exit",
        "tp2":        "TP2 — 25% exit",
        "tp3":        "TP3 — full close",
        "trail_stop": "Trail stop hit",
        "stop_loss":  "Stop loss hit",
        "time_stop":  "Time stop — 11AM ET",
    }
    label = labels.get(action, action.upper())
    pnl_str = f"  PnL ${pnl:+.2f}" if pnl != 0 else ""
    qty_str = f"  qty {qty:.2f}" if qty > 0 else ""
    priority = P_HIGH if action in ("stop_loss", "trail_stop", "time_stop") else P_DEFAULT
    title = f"Ollie {label} {symbol}"
    body  = f"${price:.2f}{qty_str}{pnl_str}"
    tag   = _TAGS.get(action, "moneybag")
    _fire(title, body, priority=priority, tags=tag)


def notify_tpol_buy(symbol: str, price: float, qty: float,
                    option_type: str, strike: float, dte: int,
                    premium: float, reasoning: str = "") -> None:
    title = f"T'Pol BUY {option_type.upper()} {symbol}"
    body  = (f"Strike ${strike:.0f}  {dte}DTE  premium ${premium:.2f}  qty {qty:.2f}\n"
             f"Stock @ ${price:.2f}\n"
             f"{reasoning[:120]}" if reasoning else f"Stock @ ${price:.2f}")
    _fire(title, body, priority=P_HIGH, tags=_TAGS["tpol_buy"])


def notify_tpol_sell(symbol: str, option_type: str, price: float,
                     pnl: float, reasoning: str = "") -> None:
    emoji = "green_circle" if pnl >= 0 else "red_circle"
    title = f"T'Pol SELL {option_type.upper()} {symbol}  PnL ${pnl:+.2f}"
    body  = (f"Exit @ ${price:.2f}\n"
             f"{reasoning[:120]}" if reasoning else f"Exit @ ${price:.2f}")
    _fire(title, body, priority=P_DEFAULT, tags=emoji)


def notify_regime_change(old_regime: str, new_regime: str,
                         spy_close: float = 0.0, spy_ma8: float = 0.0,
                         spy_ma21: float = 0.0) -> None:
    priority = P_MAX if "CRISIS" in new_regime or "BEAR" in new_regime else P_HIGH
    title = f"Regime: {old_regime} → {new_regime}"
    spy_str = f"SPY ${spy_close:.2f}  8MA ${spy_ma8:.2f}  21MA ${spy_ma21:.2f}" if spy_close else ""
    body  = spy_str or new_regime
    _fire(title, body, priority=priority, tags=_TAGS["regime"])


def notify_crusher_restart(reason: str = "Port 8080 down") -> None:
    title = "TradeMinds RESTARTING"
    body  = reason
    _fire(title, body, priority=P_MAX, tags=_TAGS["restart"])
