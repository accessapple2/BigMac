"""Telegram bot alerts for trade notifications, stop-loss warnings, and daily summaries."""
from __future__ import annotations
import asyncio
import threading
from datetime import datetime
from rich.console import Console

console = Console()

_BOT_TOKEN = ""
_CHAT_ID = ""
_ENABLED = False


def init_telegram():
    """Load Telegram config from environment. Call once at startup."""
    global _BOT_TOKEN, _CHAT_ID, _ENABLED
    import os
    _BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    _CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    _ENABLED = bool(_BOT_TOKEN and _CHAT_ID)
    if _ENABLED:
        console.log("[green]Telegram alerts enabled")
    else:
        console.log("[yellow]Telegram alerts disabled (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env)")


def _send_sync(text: str):
    """Send a message via Telegram Bot API (fire-and-forget in background thread)."""
    if not _ENABLED:
        return
    try:
        import telegram

        async def _do_send():
            bot = telegram.Bot(token=_BOT_TOKEN)
            await bot.send_message(
                chat_id=_CHAT_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

        # Run in a new event loop in a background thread to avoid blocking
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_do_send())
        loop.close()
    except Exception as e:
        console.log(f"[red]Telegram send error: {e}")


def send_alert(text: str):
    """Non-blocking alert send."""
    if not _ENABLED:
        return
    t = threading.Thread(target=_send_sync, args=(text,), daemon=True)
    t.start()


# ── Pre-built alert types ────────────────────────────────────────────

def alert_trade(player_id: str, player_name: str, action: str, symbol: str,
                qty: float, price: float, reasoning: str = ""):
    """Alert on trade execution (BUY/SELL/BUY_CALL/BUY_PUT)."""
    icon = {"BUY": "🟢", "SELL": "🔴", "BUY_CALL": "📈", "BUY_PUT": "📉"}.get(action, "⚪")
    cost = round(qty * price, 2)
    msg = (
        f"{icon} <b>{action}</b> {symbol}\n"
        f"Model: {player_name}\n"
        f"Qty: {qty} @ ${price:.2f} (${cost:,.2f})\n"
    )
    if reasoning:
        msg += f"<i>{reasoning[:200]}</i>"
    send_alert(msg)


def alert_stop_loss(player_id: str, player_name: str, symbol: str,
                    pnl_pct: float, pnl_usd: float):
    """Alert on stop-loss breach."""
    msg = (
        f"🚨 <b>STOP-LOSS</b> {symbol}\n"
        f"Model: {player_name}\n"
        f"Down {pnl_pct:.1f}% (${pnl_usd:+,.2f})\n"
        f"<b>Position should be closed.</b>"
    )
    send_alert(msg)


def alert_vix_spike(vix_price: float, vix_change_pct: float):
    """Alert on VIX spike > 5% intraday."""
    msg = (
        f"⚡ <b>VIX SPIKE ALERT</b>\n"
        f"VIX: {vix_price:.2f} ({vix_change_pct:+.1f}% intraday)\n"
        f"<b>Volatility elevated — review positions.</b>"
    )
    send_alert(msg)


def alert_earnings_upcoming(earnings: list):
    """Alert for upcoming earnings in next 7 days."""
    if not earnings:
        return
    lines = [f"📅 <b>Earnings Alert — Next 7 Days</b>"]
    for e in earnings:
        days = e.get("days_until", 0)
        label = "TODAY" if days == 0 else f"in {days}d"
        lines.append(f"  • <b>{e['symbol']}</b> — {e['date']} ({label})")
    send_alert("\n".join(lines))


def send_daily_summary(players_data: list):
    """Send end-of-day portfolio summary for all active players."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"📊 <b>Daily Summary</b> — {now}\n"]
    for p in players_data:
        icon = "🟢" if p["return_pct"] >= 0 else "🔴"
        lines.append(
            f"{icon} <b>{p['name']}</b>: ${p['total_value']:,.2f} "
            f"({p['return_pct']:+.2f}%) | "
            f"Unreal: ${p['unrealized_pnl']:+,.2f} | "
            f"Trades: {p['trades_today']}"
        )
    send_alert("\n".join(lines))
