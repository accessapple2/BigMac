"""Rallies.ai Arena Feed Parser — extracts trades, reasoning, and discussions.

Parses the free-form text feed from Rallies.ai Arena into structured
trade records and discussion threads.

Feed format (approximate):
  [Model Name]
  [$Portfolio Value]
  [Timestamp]
  [Headline/Action text]
  [Reasoning paragraph]

  [TICKER]
  [$Value]
  [▲/▼ profit/loss %]
  [Bought/Sold at $price]

  Reply from [Other Model]:
  [Reply text]
"""
from __future__ import annotations
import re
import sqlite3
from datetime import datetime, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Known Rallies model names
RALLIES_MODELS = [
    "Grok 4.20", "Grok 4", "Opus 4.6", "Opus 4.5",
    "Claude Sonnet 4.6", "Claude Sonnet 4.5", "Sonnet 4.6",
    "GPT 5.4", "GPT 5.2", "GPT 5.1", "GPT 5",
    "Gemini 3.1 Pro", "Gemini 3 Pro", "Gemini 2.5 Pro",
    "DeepSeek V3", "DeepSeek R1",
    "Qwen 3", "Kimi 2.5", "Kimi K2.5",
    "Llama 4 Scout", "Llama 4 Maverick", "Llama 4",
    "Mistral Large", "Command R+",
]

# Sort longest first so "Claude Sonnet 4.5" matches before "Sonnet 4.5"
RALLIES_MODELS.sort(key=len, reverse=True)

# Common tickers to recognize
_COMMON_TICKERS = {
    "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT", "GOOGL", "AMZN",
    "SPY", "QQQ", "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL",
    "LMT", "LNG", "CVS", "UBER", "CRM", "VST", "JPM", "PTC",
    "EOG", "HIMS", "SMCI", "VRT", "UNH", "XLE", "GS", "BA",
    "NFLX", "DIS", "INTC", "COIN", "MARA", "SQ", "SHOP",
    "ARM", "SNOW", "CRWD", "PANW", "ZS", "NET", "DDOG",
    "OKE", "ABBV", "WMT", "COST", "TGT", "HD", "LOW",
}


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _parse_timestamp(ts_text: str) -> str:
    """Convert relative timestamps to ISO dates."""
    ts = ts_text.strip().lower()
    now = datetime.now()

    # "13 hours ago", "3h ago"
    m = re.match(r'(\d+)\s*(?:hours?|h)\s*ago', ts)
    if m:
        return (now - timedelta(hours=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")

    # "3 days ago", "3d ago"
    m = re.match(r'(\d+)\s*(?:days?|d)\s*ago', ts)
    if m:
        return (now - timedelta(days=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")

    # "45 minutes ago", "45m ago"
    m = re.match(r'(\d+)\s*(?:minutes?|min|m)\s*ago', ts)
    if m:
        return (now - timedelta(minutes=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")

    # "March 17", "March 17, 2026"
    for fmt in ["%B %d, %Y", "%B %d", "%b %d, %Y", "%b %d"]:
        try:
            dt = datetime.strptime(ts_text.strip(), fmt)
            if dt.year == 1900:
                dt = dt.replace(year=now.year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # "3/17/2026", "2026-03-17"
    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m/%d"]:
        try:
            dt = datetime.strptime(ts_text.strip(), fmt)
            if dt.year == 1900:
                dt = dt.replace(year=now.year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return ts_text.strip()


def _extract_tickers(text: str) -> list:
    """Extract stock tickers from text."""
    words = re.findall(r'\b[A-Z]{1,5}\b', text)
    return [w for w in words if w in _COMMON_TICKERS]


def _extract_price(text: str) -> float | None:
    """Extract a dollar price from text."""
    m = re.search(r'\$(\d[\d,]*\.?\d*)', text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def _extract_pnl(text: str) -> tuple:
    """Extract P&L value and percentage from text."""
    pnl_val = None
    pnl_pct = None

    # "$1,500" or "+$344" or "-$200"
    m = re.search(r'[+\-]?\$(\d[\d,]*\.?\d*)', text)
    if m:
        pnl_val = float(m.group(1).replace(",", ""))
        if "-" in text[:text.find("$")]:
            pnl_val = -pnl_val

    # "▲ 3.2%" or "▼ 1.5%" or "+3.2%" or "-1.5%"
    m = re.search(r'[▲▼+\-]?\s*(\d+\.?\d*)\s*%', text)
    if m:
        pnl_pct = float(m.group(1))
        if "▼" in text or "-" in text[:text.find("%")]:
            pnl_pct = -pnl_pct

    return pnl_val, pnl_pct


def _extract_action(text: str) -> str:
    """Extract BUY/SELL/HOLD from text."""
    text_lower = text.lower()
    if re.search(r'\b(bought|buy|added|opened|long)\b', text_lower):
        return "BUY"
    if re.search(r'\b(sold|sell|trim|closed|exited|short)\b', text_lower):
        return "SELL"
    if re.search(r'\b(hold|holding|maintain|keep)\b', text_lower):
        return "HOLD"
    return "HOLD"


def parse_rallies_feed(text: str) -> dict:
    """Parse a Rallies.ai arena feed into trades and discussions.

    Auto-detects format:
    - Portfolio table (Stock/Allocation/P&L columns)
    - Free-form trade feed (model names, reasoning paragraphs)

    Returns {trades: [...], discussions: [...], models_found: [...], summary: str}
    """
    # Detect portfolio table format
    if re.search(r'Stock\s+Allocation\s+P&L|Allocation.*P&L.*Notional|TOTAL PNL:', text, re.IGNORECASE):
        return _parse_portfolio_table(text)

    return _parse_feed_format(text)


def _parse_portfolio_table(text: str) -> dict:
    """Parse the portfolio/positions table format:

    [Model Name]
    [Model Name]       <-- repeated name (Rallies shows it twice)

    Stock  Allocation  P&L  P&L %  Notional  Worth  Entry
    [TICKER]
    [Alloc%]
    [$P&L]
    [+/-P&L%]  [$Notional]  [$Worth]  [$Entry]
    ...
    TOTAL PNL: $X • AVAILABLE CASH: $X

    Or "No open positions" for empty portfolios.
    """
    lines = text.split("\n")
    trades = []
    models_found = set()
    model_stats = {}  # model → {total_pnl, available_cash}

    current_model = None
    in_table = False
    skip_model = False  # True when "No open positions" seen

    def _is_model_name(line):
        """Check if a line is a known model name."""
        s = line.strip()
        if not s:
            return None
        for model in RALLIES_MODELS:
            if s == model:
                return model
        return None

    def _is_ticker(s):
        """Check if string looks like a stock ticker."""
        s = s.strip().upper()
        if not s or len(s) > 5:
            return False
        if s in _COMMON_TICKERS:
            return True
        # Accept any 1-5 uppercase letters that isn't a known keyword
        if re.match(r'^[A-Z]{1,5}$', s):
            skip_words = {
                "TOTAL", "PNL", "STOCK", "ENTRY", "WORTH", "CASH",
                "NO", "OPEN", "THE", "AND", "FOR", "BUY", "SELL",
                "HOLD", "FROM", "WITH",
            }
            return s not in skip_words
        return False

    def _looks_numeric(s):
        """Check if string looks like a numeric value ($X, X%, +X, -X)."""
        s = s.strip()
        if not s:
            return False
        return bool(re.match(r'^[+\-$▲▼(]?\$?[\d,]+\.?\d*[%)]?$', s.replace(",", "")))

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        i += 1

        if not stripped:
            continue

        # "No open positions" — skip this model
        if re.match(r'no open positions', stripped, re.IGNORECASE):
            skip_model = True
            continue

        # TOTAL PNL line — marks end of model section
        m_total = re.search(r'TOTAL PNL:\s*[\-\$]*([\-\d,\.]+)', stripped, re.IGNORECASE)
        if m_total:
            try:
                pnl = float(m_total.group(1).replace(",", ""))
                if "-" in stripped[:stripped.upper().find("TOTAL") + 15]:
                    pnl = -pnl
            except (ValueError, TypeError):
                pnl = 0
            cash = 0
            m_cash = re.search(r'AVAILABLE CASH:\s*\$?([\d,\.]+)', stripped, re.IGNORECASE)
            if m_cash:
                try:
                    cash = float(m_cash.group(1).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            if current_model:
                model_stats[current_model] = {"total_pnl": pnl, "available_cash": cash}
            in_table = False
            skip_model = False
            current_model = None
            continue

        # Table header line
        if re.match(r'Stock\s+Allocation', stripped, re.IGNORECASE):
            in_table = True
            skip_model = False
            continue

        # Model name detection
        detected = _is_model_name(stripped)
        if detected:
            if current_model == detected:
                # Repeated model name — skip duplicate
                continue
            current_model = detected
            models_found.add(detected)
            in_table = False
            skip_model = False
            continue

        # Skip if model has no positions
        if skip_model:
            continue

        # Parse position data when in table
        if in_table and current_model:
            # Try multi-column row first (tab or multi-space separated)
            fields = re.split(r'\t+|\s{2,}', stripped)
            if len(fields) >= 3 and _is_ticker(fields[0]):
                ticker = fields[0].strip().upper()
                alloc = _safe_float_str(fields[1]) if len(fields) > 1 else None
                pnl_val = _safe_float_str(fields[2]) if len(fields) > 2 else None
                pnl_pct = _safe_float_str(fields[3]) if len(fields) > 3 else None
                notional = _safe_float_str(fields[4]) if len(fields) > 4 else None
                worth = _safe_float_str(fields[5]) if len(fields) > 5 else None
                entry = _safe_float_str(fields[6]) if len(fields) > 6 else None

                trades.append({
                    "model_name": current_model,
                    "symbol": ticker,
                    "action": "HOLD",
                    "price": entry,
                    "reasoning": f"Portfolio snapshot: {alloc or '?'}% allocation, entry ${entry or '?'}, worth ${worth or '?'}, P&L ${pnl_val or '?'} ({pnl_pct or '?'}%)",
                    "pnl": pnl_val,
                    "pnl_pct": pnl_pct,
                    "traded_at": "",
                    "portfolio_value": None,
                })
                continue

            # Single-field-per-line: ticker on its own line, values follow
            if _is_ticker(stripped):
                ticker = stripped.upper()
                # Peek ahead for up to 6 numeric values
                peek_values = []
                j = i
                while j < len(lines) and len(peek_values) < 6:
                    pline = lines[j].strip()
                    if not pline:
                        break
                    if _is_ticker(pline):
                        break
                    if re.match(r'TOTAL PNL', pline, re.IGNORECASE):
                        break
                    if re.match(r'no open positions', pline, re.IGNORECASE):
                        break
                    if re.match(r'Stock\s+Allocation', pline, re.IGNORECASE):
                        break
                    if _is_model_name(pline):
                        break
                    peek_values.append(pline)
                    j += 1

                alloc = _safe_float_str(peek_values[0]) if len(peek_values) > 0 else None
                pnl_val = _safe_float_str(peek_values[1]) if len(peek_values) > 1 else None
                pnl_pct = _safe_float_str(peek_values[2]) if len(peek_values) > 2 else None
                notional = _safe_float_str(peek_values[3]) if len(peek_values) > 3 else None
                worth = _safe_float_str(peek_values[4]) if len(peek_values) > 4 else None
                entry = _safe_float_str(peek_values[5]) if len(peek_values) > 5 else None

                trades.append({
                    "model_name": current_model,
                    "symbol": ticker,
                    "action": "HOLD",
                    "price": entry,
                    "reasoning": f"Portfolio snapshot: {alloc or '?'}% allocation, entry ${entry or '?'}, worth ${worth or '?'}, P&L ${pnl_val or '?'} ({pnl_pct or '?'}%)",
                    "pnl": pnl_val,
                    "pnl_pct": pnl_pct,
                    "traded_at": "",
                    "portfolio_value": None,
                })
                i = j  # Skip consumed lines
                continue

    return {
        "trades": trades,
        "discussions": [],
        "models_found": sorted(models_found),
        "model_stats": model_stats,
        "summary": f"Found: {len(trades)} positions, 0 discussions, {len(models_found)} models",
    }


def _safe_float_str(s):
    """Parse a string that might have $, %, commas, +/- signs into a float."""
    if s is None:
        return None
    s = s.strip().replace(",", "").replace("$", "").replace("%", "").replace("+", "")
    # Handle negative with various formats
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_feed_format(text: str) -> dict:
    """Parse the free-form trade feed format (original parser)."""
    lines = text.split("\n")
    trades = []
    discussions = []
    models_found = set()

    current_model = None
    current_portfolio = None
    current_timestamp = None
    current_block_lines = []
    in_reply = False
    reply_model = None
    reply_lines = []
    last_trade_idx = -1

    def _flush_block():
        nonlocal current_model, current_portfolio, current_timestamp, current_block_lines, last_trade_idx
        if not current_model or not current_block_lines:
            current_block_lines = []
            return

        block_text = "\n".join(current_block_lines).strip()
        if not block_text:
            current_block_lines = []
            return

        # Extract tickers, action, reasoning
        tickers = _extract_tickers(block_text)
        action = _extract_action(block_text)
        price = _extract_price(block_text)
        pnl_val, pnl_pct = _extract_pnl(block_text)

        # The first line is usually the headline/action
        headline = current_block_lines[0].strip() if current_block_lines else ""
        # Rest is reasoning
        reasoning = "\n".join(current_block_lines[1:]).strip() if len(current_block_lines) > 1 else headline

        # Create a trade for each mentioned ticker (or one generic if none found)
        if tickers:
            for ticker in tickers[:3]:  # Cap at 3 tickers per block
                trades.append({
                    "model_name": current_model,
                    "symbol": ticker,
                    "action": action,
                    "price": price,
                    "reasoning": reasoning[:1000],
                    "pnl": pnl_val,
                    "pnl_pct": pnl_pct,
                    "traded_at": current_timestamp or "",
                    "portfolio_value": current_portfolio,
                })
                last_trade_idx = len(trades) - 1
        elif headline:
            # No tickers found but still valuable reasoning
            trades.append({
                "model_name": current_model,
                "symbol": "",
                "action": action,
                "price": price,
                "reasoning": reasoning[:1000],
                "pnl": pnl_val,
                "pnl_pct": pnl_pct,
                "traded_at": current_timestamp or "",
                "portfolio_value": current_portfolio,
            })
            last_trade_idx = len(trades) - 1

        current_block_lines = []

    def _flush_reply():
        nonlocal reply_model, reply_lines, in_reply
        if reply_model and reply_lines:
            reply_text = "\n".join(reply_lines).strip()
            # Detect sentiment
            sentiment = "neutral"
            lower = reply_text.lower()
            if re.search(r'\b(agree|concur|correct|exactly|good point)\b', lower):
                sentiment = "agree"
            elif re.search(r'\b(disagree|wrong|incorrect|risky|concern|however|but)\b', lower):
                sentiment = "disagree"

            discussions.append({
                "model_name": reply_model,
                "reply_text": reply_text[:2000],
                "sentiment": sentiment,
                "parent_trade_idx": last_trade_idx if last_trade_idx >= 0 else None,
            })
            models_found.add(reply_model)

        reply_model = None
        reply_lines = []
        in_reply = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            # Blank line might separate blocks
            if in_reply and reply_lines:
                _flush_reply()
            continue

        # Check if line is a model name
        detected_model = None
        for model in RALLIES_MODELS:
            if stripped == model or stripped.startswith(model + " ") or stripped.startswith(model + "\t"):
                detected_model = model
                break
            # Also check with trailing content like "Grok 4 replied:"
            if re.match(re.escape(model) + r'\s*(replied|says|responds|wrote)?:?\s*$', stripped, re.IGNORECASE):
                detected_model = model
                break

        # Check for reply pattern: "Grok 4:" or "Reply from Grok 4"
        reply_match = None
        for model in RALLIES_MODELS:
            if re.match(re.escape(model) + r'\s*:', stripped) or \
               re.match(r'(?:Reply|Response|Comment)\s+(?:from|by)\s+' + re.escape(model), stripped, re.IGNORECASE):
                reply_match = model
                break

        if reply_match:
            _flush_reply()
            in_reply = True
            reply_model = reply_match
            models_found.add(reply_match)
            # Rest of line after "Model:" is start of reply
            after = re.sub(r'^.*?:\s*', '', stripped)
            if after:
                reply_lines.append(after)
            continue

        if in_reply:
            reply_lines.append(stripped)
            continue

        if detected_model:
            # New model block — flush previous
            _flush_block()
            _flush_reply()
            current_model = detected_model
            models_found.add(detected_model)
            current_portfolio = None
            current_timestamp = None
            continue

        # Portfolio value line: "$109,302" or "$106,641.24"
        if re.match(r'^\$[\d,]+\.?\d*$', stripped):
            current_portfolio = float(stripped.replace("$", "").replace(",", ""))
            continue

        # Timestamp line
        if re.match(r'^\d+\s*(hours?|h|days?|d|minutes?|min|m)\s*ago$', stripped, re.IGNORECASE) or \
           re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d', stripped, re.IGNORECASE) or \
           re.match(r'^\d{1,2}/\d{1,2}', stripped):
            current_timestamp = _parse_timestamp(stripped)
            continue

        # Content line — add to current block
        if current_model:
            current_block_lines.append(stripped)

    # Flush remaining
    _flush_block()
    _flush_reply()

    return {
        "trades": trades,
        "discussions": discussions,
        "models_found": sorted(models_found),
        "summary": f"Found: {len(trades)} trades, {len(discussions)} discussions, {len(models_found)} models",
    }


def import_parsed_feed(parsed: dict, source: str = "rallies.ai-manual") -> dict:
    """Import parsed feed data into reference_trades and reference_discussions."""
    conn = _conn()
    imported_trades = 0
    imported_disc = 0
    skipped = 0

    trade_id_map = {}  # idx → db id

    for i, t in enumerate(parsed.get("trades", [])):
        sym = (t.get("symbol") or "").upper()
        model = t.get("model_name") or ""

        if not model:
            skipped += 1
            continue

        # Check duplicate
        existing = conn.execute(
            "SELECT 1 FROM reference_trades WHERE source=? AND model_name=? AND symbol=? AND traded_at=?",
            (source, model, sym, t.get("traded_at", ""))
        ).fetchone()
        if existing:
            skipped += 1
            continue

        cursor = conn.execute("""
            INSERT INTO reference_trades
            (source, model_name, symbol, action, price, reasoning,
             pnl, pnl_pct, traded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source, model, sym,
            t.get("action", "HOLD"),
            t.get("price"),
            t.get("reasoning", ""),
            t.get("pnl"),
            t.get("pnl_pct"),
            t.get("traded_at", ""),
        ))
        trade_id_map[i] = cursor.lastrowid
        imported_trades += 1

    for d in parsed.get("discussions", []):
        parent_idx = d.get("parent_trade_idx")
        parent_id = trade_id_map.get(parent_idx) if parent_idx is not None else None

        conn.execute("""
            INSERT INTO reference_discussions
            (source, parent_trade_id, model_name, reply_text, sentiment)
            VALUES (?, ?, ?, ?, ?)
        """, (
            source, parent_id,
            d.get("model_name", ""),
            d.get("reply_text", ""),
            d.get("sentiment", "neutral"),
        ))
        imported_disc += 1

    conn.commit()
    conn.close()

    console.log(f"[green]Rallies import: {imported_trades} trades, {imported_disc} discussions, {skipped} skipped")
    return {
        "imported_trades": imported_trades,
        "imported_discussions": imported_disc,
        "skipped": skipped,
        "models": parsed.get("models_found", []),
    }
