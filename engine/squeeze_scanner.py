"""Short Squeeze Scanner — finds high short-interest, low-float tickers.

Squeeze Score (1–10) based on:
  - Short Interest % of Float  (>20% required, higher = more points)
  - Float size                 (<20M required, smaller = more points)
  - Volume Ratio today vs avg  (>2x required, higher = more points)
  - Price vs 10-day high       (breaking out = more points)
  - RSI                        (<70 required — not already overbought)

Auto-posts War Room alert from Chekov (mlx-qwen3) when score > 8.

HM-AO-β (2026-05-08) — Ghost Watcher persistence:
  - run_scan() now invokes _persist_results() which writes each result
    with score >= 5 (composite >= 50) into the squeeze_watch table.
  - Writes are scoped to surface candidates to the Admiral via dashboard
    + ntfy. NO signals-table writes. NO trade-execution paths.
  - Tier mapping (composite_score = scanner score * 10):
      WATCH    50-74  (scores 5-7)
      ALERT    75-89  (score 8)
      PRIORITY 90-100 (scores 9-10)
  - 24h dedupe: same-symbol re-scan only inserts if it upgrades the tier
    (or no row exists in the last 24h).
"""
from __future__ import annotations
import os
import sqlite3
import time
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from rich.console import Console

console = Console()

_scan_lock = threading.Lock()
_last_result: dict | None = None
_last_scan_ts: float = 0.0
_CACHE_TTL: int = 300  # 5 min cache

# HM-AO-β: canonical trader DB (mirrors engine/fast_scanner.py path fix)
_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")
_MIN_PERSIST_SCORE = 5      # scanner score (1-10); rows below this are ignored
_DEDUPE_HOURS = 24          # re-insert only if same-symbol row in last 24h is lower-tier


def _tier_for_composite(composite: float) -> str:
    if composite >= 90:
        return "PRIORITY"
    if composite >= 75:
        return "ALERT"
    return "WATCH"


def _tier_rank(tier: str) -> int:
    return {"WATCH": 1, "ALERT": 2, "PRIORITY": 3}.get(tier, 0)


def _fetch_finviz_candidates() -> list[dict]:
    """Use Finviz Ownership screener to pull stocks with high short interest + small float."""
    try:
        from finvizfinance.screener.ownership import Ownership
        screener = Ownership()
        # Filter: Float Short > 20% (high squeeze potential)
        screener.set_filter(filters_dict={"Float Short": "Over 20%"})
        df = screener.screener_view()
        if df is None or df.empty:
            return []
        return df.to_dict("records")
    except Exception as e:
        console.log(f"[yellow]Squeeze: Finviz fetch error: {e}")
        return []


# === HM-DASH.2 === Polygon /stocks/v1/short-interest is now primary SI source.
# Finviz still does the screener pass (Polygon lacks a "screen high-SI tickers"
# single-call endpoint), but Polygon's raw_short_interest + native days_to_cover
# override Finviz's "Short Float"/"Short Ratio" values when present. Finviz
# numbers are kept on Polygon error/empty → "fallback only" per Captain spec.

_polygon_si_cache: dict[str, tuple[float, dict | None]] = {}
_POLYGON_SI_CACHE_TTL = 86400  # 24h — Polygon SI reports settle bi-monthly


def _fetch_polygon_si(ticker: str) -> dict | None:
    """Latest short-interest report from Polygon. None on error/empty.

    Returns: {raw_short_interest, days_to_cover, settlement_date, avg_daily_volume}
    Cached 24h per ticker — Polygon SI updates twice a month, not real-time.
    """
    try:
        import os
        import requests
        from dotenv import load_dotenv
        load_dotenv("/Users/bigmac/autonomous-trader/.env")
        key = os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_STOCKS_API_KEY")
        if not key:
            return None
        now = time.time()
        sym = ticker.upper()
        cached = _polygon_si_cache.get(sym)
        if cached and now - cached[0] < _POLYGON_SI_CACHE_TTL:
            return cached[1]
        url = f"https://api.polygon.io/stocks/v1/short-interest?ticker={sym}&limit=1&apiKey={key}"
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            _polygon_si_cache[sym] = (now, None)
            return None
        data = r.json()
        results = data.get("results") or []
        if not results:
            _polygon_si_cache[sym] = (now, None)
            return None
        latest = results[0]
        out = {
            "raw_short_interest": float(latest.get("short_interest") or 0),
            "days_to_cover":      float(latest.get("days_to_cover") or 0),
            "settlement_date":    latest.get("settlement_date") or "",
            "avg_daily_volume":   float(latest.get("avg_daily_volume") or 0),
        }
        _polygon_si_cache[sym] = (now, out)
        return out
    except Exception as e:
        console.log(f"[yellow]Squeeze: Polygon SI fetch error for {ticker}: {e}")
        # Cache the None too so we don't retry hot
        try:
            _polygon_si_cache[ticker.upper()] = (time.time(), None)
        except Exception:
            pass
        return None
# === /HM-DASH.2 ===


def _get_yfinance_data(ticker: str) -> dict | None:
    """Fetch RSI, volume ratio, 10d high — migrated 2026-04-27 yfinance -> Alpaca."""
    try:
        from engine.market_data import get_alpaca_bars
        hist = get_alpaca_bars(ticker, timeframe="1Day", days=30)
        if hist is None or len(hist) < 11:
            return None
        close = hist["Close"].squeeze().dropna()
        volume = hist["Volume"].squeeze().dropna()
        if len(close) < 11 or len(volume) < 11:
            return None

        current_price = float(close.iloc[-1])
        today_vol = float(volume.iloc[-1])
        avg_vol = float(volume.iloc[-21:-1].mean()) if len(volume) >= 21 else float(volume.iloc[:-1].mean())
        vol_ratio = round(today_vol / avg_vol, 2) if avg_vol > 0 else 1.0
        high_10d = float(close.iloc[-11:-1].max())
        above_10d_high = current_price > high_10d

        # RSI-14
        delta = close.diff().dropna()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(14).mean().iloc[-1]
        avg_loss = loss.rolling(14).mean().iloc[-1]
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = round(100 - (100 / (1 + rs)), 1)

        return {
            "price": round(current_price, 2),
            "vol_ratio": vol_ratio,
            "above_10d_high": above_10d_high,
            "high_10d": round(high_10d, 2),
            "rsi": rsi,
        }
    except Exception as e:
        console.log(f"[dim]Squeeze: yfinance error for {ticker}: {e}")
        return None


def _parse_float_val(val) -> float:
    """Parse finviz values:
    - '21.68%' → 21.68 (percentage, strip %)
    - 232440000.0 → 232.44 (raw share count, convert to millions)
    - '5.23M' → 5.23 (with suffix, keep as millions)
    """
    if val is None:
        return 0.0
    s = str(val).strip().replace(",", "")
    # Handle percentage strings like '21.68%'
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return 0.0
    # Handle suffixes
    multiplier = 1.0
    if s.endswith("B"):
        multiplier = 1_000.0
        s = s[:-1]
    elif s.endswith("M"):
        multiplier = 1.0
        s = s[:-1]
    elif s.endswith("K"):
        multiplier = 0.001
        s = s[:-1]
    try:
        raw = float(s) * multiplier
        # If raw value is very large (raw share count > 1M), convert to millions
        if raw > 1_000_000:
            return raw / 1_000_000
        return raw
    except ValueError:
        return 0.0


def _score_candidate(row: dict, yf_data: dict) -> int:
    """Compute squeeze score 1–10."""
    score = 0

    # Short interest component (0–3 pts) — 'Short Float' col has value like '21.68%'
    short_pct = _parse_float_val(row.get("Short Float", 0))
    if short_pct >= 40:
        score += 3
    elif short_pct >= 30:
        score += 2
    elif short_pct >= 20:
        score += 1

    # Float component (0–2 pts) — 'Float' col has raw share count (e.g. 232440000.0)
    float_m = _parse_float_val(row.get("Float", 0))
    if float_m <= 5:
        score += 2
    elif float_m <= 20:
        score += 1

    # Volume ratio (0–2 pts)
    vol_ratio = yf_data.get("vol_ratio", 1.0)
    if vol_ratio >= 5:
        score += 2
    elif vol_ratio >= 2:
        score += 1

    # Price above 10-day high (0–2 pts)
    if yf_data.get("above_10d_high"):
        score += 2

    # RSI not overbought (1 pt bonus if RSI < 60 — room to run)
    rsi = yf_data.get("rsi", 50)
    if rsi < 60:
        score += 1

    return max(1, min(10, score))


def run_scan(force: bool = False) -> dict:
    """Run the squeeze scan, return results. Cached 5 min unless force=True."""
    global _last_result, _last_scan_ts

    if not force and _last_result is not None:
        if time.time() - _last_scan_ts < _CACHE_TTL:
            return _last_result

    with _scan_lock:
        # Double-check inside lock
        if not force and _last_result is not None:
            if time.time() - _last_scan_ts < _CACHE_TTL:
                return _last_result

        console.log("[cyan]Squeeze Scanner: fetching Finviz candidates...")
        candidates = _fetch_finviz_candidates()
        console.log(f"[cyan]Squeeze Scanner: {len(candidates)} candidates from Finviz")

        results = []
        for row in candidates[:40]:  # cap at 40 to avoid rate limits
            ticker = str(row.get("Ticker", "")).strip()
            if not ticker:
                continue

            short_pct = _parse_float_val(row.get("Short Float", 0))
            float_m = _parse_float_val(row.get("Float", 0))

            # Must meet base criteria
            if short_pct < 20:
                continue

            yf_data = _get_yfinance_data(ticker)
            if yf_data is None:
                continue

            # === HM-DASH.2 === enrich with Polygon canonical SI + DTC
            polygon_si = _fetch_polygon_si(ticker)
            dtc = _parse_float_val(row.get("Short Ratio"))
            si_source = "finviz"
            settlement_date = ""
            if polygon_si:
                # SI% = polygon raw_short_interest / (finviz float_m * 1M) * 100
                raw_si = polygon_si.get("raw_short_interest") or 0
                if float_m and raw_si:
                    short_pct = round(raw_si / (float_m * 1_000_000) * 100, 1)
                    # Mutate row so _score_candidate uses Polygon-derived SI%
                    row["Short Float"] = short_pct
                if polygon_si.get("days_to_cover"):
                    dtc = float(polygon_si["days_to_cover"])
                settlement_date = polygon_si.get("settlement_date") or ""
                si_source = "polygon"
            # === /HM-DASH.2 ===

            vol_ratio = yf_data.get("vol_ratio", 1.0)
            rsi = yf_data.get("rsi", 50.0)

            if vol_ratio < 2.0 or rsi >= 70:
                continue

            score = _score_candidate(row, yf_data)

            change_pct = 0.0
            try:
                raw_change = row.get("Change", 0)
                cv = float(str(raw_change).replace("%", ""))
                # Finviz Ownership returns Change as decimal fraction (e.g. -0.0118 = -1.18%)
                change_pct = cv * 100 if abs(cv) < 1.0 else cv
            except Exception:
                pass

            results.append({
                "ticker": ticker,
                "short_interest_pct": round(short_pct, 1),
                "float_m": round(float_m, 2),
                "days_to_cover": dtc,
                "vol_ratio": vol_ratio,
                "price": yf_data["price"],
                "day_change_pct": round(change_pct, 2),
                "rsi": rsi,
                "above_10d_high": yf_data["above_10d_high"],
                "score": score,
                # === HM-DASH.2 === observability fields (not persisted to DB)
                "si_source": si_source,
                "si_settlement_date": settlement_date,
                # === /HM-DASH.2 ===
            })

        # Sort by score desc
        results.sort(key=lambda x: x["score"], reverse=True)

        # Auto-post War Room for score > 8
        _post_war_room_alerts(results)

        # HM-AO-β Ghost Watcher persistence — never raises.
        persist_summary: dict = {}
        try:
            persist_summary = _persist_results(results)
        except Exception as e:
            console.log(f"[yellow]squeeze_watch persist top-level error: {type(e).__name__}: {e!r}")

        _last_result = {
            "results": results,
            "scanned_at": datetime.now().isoformat(),
            "candidate_count": len(candidates),
            "watch_persist": persist_summary,
        }
        _last_scan_ts = time.time()

        console.log(f"[green]Squeeze Scanner: {len(results)} squeeze candidates found")
        return _last_result


def _post_war_room_alerts(results: list[dict]) -> None:
    """Post War Room hot takes from Chekov for any score > 8 ticker."""
    try:
        from engine.war_room import save_hot_take
        for r in results:
            if r["score"] > 8:
                ticker = r["ticker"]
                short_pct = r["short_interest_pct"]
                float_m = r["float_m"]
                vol_ratio = r["vol_ratio"]
                take = (
                    f"SQUEEZE ALERT: {ticker} — short interest {short_pct}%, "
                    f"float {float_m}M, volume {vol_ratio}x. Shields up."
                )
                saved = save_hot_take("mlx-qwen3", ticker, take)
                if saved:
                    console.log(f"[bold magenta]Chekov → War Room: {ticker} squeeze alert posted")
    except Exception as e:
        console.log(f"[yellow]Squeeze War Room post error: {e}")


# ─── HM-AO-β Ghost Watcher persistence ────────────────────────────────────


def _conn(db_path: str = _DB_PATH) -> sqlite3.Connection:
    c = sqlite3.connect(db_path, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _is_quiet_hours_et() -> bool:
    """22:00-06:00 ET = 02:00-10:00 UTC (during DST). Defer ntfy in that window."""
    hour_utc = datetime.now(timezone.utc).hour
    return 2 <= hour_utc < 10


def _persist_results(results: list[dict], db_path: str = _DB_PATH) -> dict:
    """Write each result with score >= _MIN_PERSIST_SCORE into squeeze_watch.

    Returns a summary dict {inserted, deferred, skipped_dedup, ntfy_fired}.
    Wraps every DB call in try/except — never raises into caller. Failures
    are logged to trader.log via console.

    Dedupe rule: if the same symbol already has a non-dismissed row in the
    last _DEDUPE_HOURS, only insert if the new tier is strictly higher.
    """
    summary = {"inserted": 0, "deferred": 0, "skipped_dedup": 0, "ntfy_fired": 0}
    if not results:
        return summary

    quiet = _is_quiet_hours_et()
    cutoff_ts = (datetime.now(timezone.utc) - timedelta(hours=_DEDUPE_HOURS)).isoformat()
    scan_ts = datetime.now(timezone.utc).isoformat()
    inserted_ids: list[int] = []

    try:
        conn = _conn(db_path)
        for r in results:
            score = int(r.get("score", 0) or 0)
            if score < _MIN_PERSIST_SCORE:
                continue
            ticker = r.get("ticker") or ""
            if not ticker:
                continue

            composite = float(score) * 10.0
            tier = _tier_for_composite(composite)

            # Dedupe — most recent row in last _DEDUPE_HOURS for same symbol
            row = conn.execute(
                """SELECT id, threshold_tier FROM squeeze_watch
                   WHERE symbol = ? AND scan_ts >= ? AND dismissed = 0
                   ORDER BY scan_ts DESC LIMIT 1""",
                (ticker, cutoff_ts),
            ).fetchone()
            if row is not None and _tier_rank(row["threshold_tier"]) >= _tier_rank(tier):
                summary["skipped_dedup"] += 1
                continue

            breakout = 1.0 if r.get("above_10d_high") else 0.0
            notes = (
                f"raw_score={score}; days_to_cover={r.get('days_to_cover')}; "
                f"day_change_pct={r.get('day_change_pct')}; "
                f"high_10d_break={'yes' if r.get('above_10d_high') else 'no'}; "
                # === HM-DASH.2 === SI provenance for traceability
                f"si_source={r.get('si_source', 'finviz')}; "
                f"si_settle={r.get('si_settlement_date', '')}"
                # === /HM-DASH.2 ===
            )
            ntfy_deferred = 1 if (tier == "PRIORITY" and quiet) else 0

            try:
                cur = conn.execute(
                    """INSERT INTO squeeze_watch
                       (symbol, scan_ts, short_pct, float_m, vol_ratio, rsi,
                        breakout_score, composite_score, threshold_tier,
                        price_at_scan, notes, ntfy_sent, ntfy_deferred)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?)""",
                    (
                        ticker, scan_ts,
                        float(r.get("short_interest_pct", 0) or 0),
                        float(r.get("float_m", 0) or 0),
                        float(r.get("vol_ratio", 0) or 0),
                        float(r.get("rsi", 0) or 0),
                        breakout, composite, tier,
                        float(r.get("price", 0) or 0),
                        notes, ntfy_deferred,
                    ),
                )
                inserted_ids.append(int(cur.lastrowid))
                summary["inserted"] += 1
                if ntfy_deferred:
                    summary["deferred"] += 1
            except Exception as e:
                console.log(f"[yellow]squeeze_watch insert error for {ticker}: {type(e).__name__}: {e!r}")

        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[yellow]squeeze_watch persist error: {type(e).__name__}: {e!r}")
        return summary

    # ntfy surfacer for PRIORITY rows that aren't deferred
    if not quiet:
        try:
            summary["ntfy_fired"] = _ntfy_priority_candidates(db_path)
        except Exception as e:
            console.log(f"[yellow]squeeze_watch ntfy error: {type(e).__name__}: {e!r}")

    if summary["inserted"] or summary["skipped_dedup"]:
        console.log(
            f"[cyan]squeeze_watch: inserted={summary['inserted']} "
            f"skipped_dedup={summary['skipped_dedup']} "
            f"deferred={summary['deferred']} ntfy_fired={summary['ntfy_fired']}"
        )
    return summary


def _ntfy_priority_candidates(db_path: str = _DB_PATH, max_individual: int = 5) -> int:
    """ntfy ollietrades-admin for PRIORITY rows that haven't been notified.

    Throttle: at most `max_individual` per scan run. If more than that,
    a single rollup ntfy fires instead. Never raises.
    """
    import urllib.request

    fired = 0
    try:
        conn = _conn(db_path)
        rows = conn.execute(
            """SELECT id, symbol, composite_score, short_pct, float_m, vol_ratio,
                      rsi, price_at_scan
               FROM squeeze_watch
               WHERE threshold_tier = 'PRIORITY'
                 AND ntfy_sent = 0
                 AND ntfy_deferred = 0
                 AND dismissed = 0
               ORDER BY scan_ts DESC""",
        ).fetchall()
    except Exception as e:
        console.log(f"[yellow]squeeze_watch ntfy query error: {type(e).__name__}: {e!r}")
        return 0

    if not rows:
        try:
            conn.close()
        except Exception:
            pass
        return 0

    pending = list(rows)
    topic = os.environ.get("NTFY_ADMIN_TOPIC", "ollietrades-admin")

    def _post(title: str, body: str) -> bool:
        safe_title = title.encode("ascii", "replace").decode("ascii")
        try:
            req = urllib.request.Request(
                f"https://ntfy.sh/{topic}",
                data=body.encode("utf-8"),
                method="POST",
            )
            req.add_header("Title", safe_title)
            req.add_header("Priority", "default")
            urllib.request.urlopen(req, timeout=8).read()
            return True
        except Exception as ex:
            console.log(f"[yellow]squeeze_watch ntfy POST error: {type(ex).__name__}: {ex!r}")
            return False

    if len(pending) > max_individual:
        # Rollup ntfy — single notification for all PRIORITY-tier hits
        symbols = ", ".join(r["symbol"] for r in pending[:10])
        body = (
            f"{len(pending)} PRIORITY squeeze candidates — "
            f"top: {symbols}. See dashboard /api/squeeze/recent for details."
        )
        if _post("Squeeze rollup PRIORITY", body):
            fired = len(pending)
            try:
                conn.executemany(
                    "UPDATE squeeze_watch SET ntfy_sent=1 WHERE id=?",
                    [(int(r["id"]),) for r in pending],
                )
                conn.commit()
            except Exception as e:
                console.log(f"[yellow]squeeze_watch ntfy_sent update error: {type(e).__name__}: {e!r}")
    else:
        for r in pending:
            body = (
                f"composite={r['composite_score']:.0f}  short={r['short_pct']:.1f}%  "
                f"float={r['float_m']:.1f}M  vol={r['vol_ratio']:.1f}x  "
                f"rsi={r['rsi']:.1f}  px=${r['price_at_scan']:.2f}"
            )
            if _post(f"Squeeze PRIORITY {r['symbol']}", body):
                try:
                    conn.execute("UPDATE squeeze_watch SET ntfy_sent=1 WHERE id=?", (int(r["id"]),))
                    conn.commit()
                    fired += 1
                except Exception as e:
                    console.log(f"[yellow]squeeze_watch ntfy_sent update error: {type(e).__name__}: {e!r}")

    try:
        conn.close()
    except Exception:
        pass
    return fired
