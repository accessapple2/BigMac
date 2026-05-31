"""engine/ollie_gb_gate.py — HM-OLLIE-LEARN Phase 2: GB learned-gate (SHADOW-MODE).

WHY (Phase 1 motivates this): the rule-optimizer (engine/ollie_optimizer.py) proved
OUT-OF-SAMPLE that re-thresholding the existing OllieScore gives no edge — the score
clusters 1.78–3.39 and OOS rows are ~all ≥2.0, so a threshold move is inert. The lever
is not a better threshold but a BETTER SCORE. This module learns one: a regularized
gradient-boosted classifier over the SAME gate-component features the linear OllieScore
uses (grade/alpha/agent-WR/regime/trade-alpha), so it can find a nonlinear combination
the fixed weights miss.

HONEST DATA LIMITS (both now measured, design matched to them):
  - Effective labeled slice ≈ 540 rows (backtest_v5_ollie_decisions with a shadow_pnl_pct
    outcome). The 70k decision_audit rows are NOT a usable label source — only 3 join to a
    closed trade — so they cannot feed supervised training. We train on the ~540 core rows.
  - One broad 5-month regime sequence → limited regime diversity. Hence: SIMPLE model
    (shallow trees, few estimators, strong regularization), CONTINUAL nightly re-fit (not a
    frozen one-shot), and honest cross-validation (never in-sample).
  - ~540 rows cannot support a complex model. If train-AUC ≫ CV-AUC we FLAG overfit.

SHADOW-MODE MANDATORY (RED+): this NEVER gates a live trade. It runs alongside the
existing 2.0 gate and logs what it WOULD decide vs what Ollie actually did, into
`ollie_gb_shadow`. A human compares on live results over time before it touches anything.

Honest expectation: 540 rows is thin and Phase 1 showed the inputs barely discriminate —
this may show only modest or no CV edge over the static gate. That is a valid outcome
(it would confirm the static gate is already near-optimal). Built to learn the truth.

Runs under .venv-backtest (sklearn present). CPU; idle 5080 optional.
"""
from __future__ import annotations

import os
import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BACKTEST_DB = "data/backtest.db"

# DATA REALITY (measured 2026-05-31): the gate's component columns
# (grade_pts/alpha_pts/agent_wr_pts/regime_pts) are 100% NULL in the corpus — they were
# never logged. The ONLY populated numeric features are `ollie_score` and `trade_alpha`
# (+ regime categorical). So the GB can only learn a nonlinear function of those, not a
# re-weighting of the (unavailable) components. This is a hard data limit, reported as-is.
FEATURES = ["ollie_score", "trade_alpha"]
REGIME_ONEHOT = ["bull", "bear", "cautious"]   # regimes present in the corpus

# Decision threshold for converting GB win-probability → approve/reject in shadow.
# 0.5 is the neutral default; tuning is a later (still-shadow) step.
GB_DECISION_PROB = 0.5
STATIC_GATE = 2.0   # the live OllieScore gate we shadow against


def _conn(db: str = BACKTEST_DB) -> sqlite3.Connection:
    c = sqlite3.connect(db, timeout=30.0)
    c.row_factory = sqlite3.Row
    return c


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (name,)).fetchone() is not None


def _regime_class(regime: str | None) -> str:
    m = {"BULL": "bull", "BULL_CROSS": "bull", "CAUTIOUS_BULL": "bull",
         "BEAR": "bear", "BEAR_CROSS": "bear", "CAUTIOUS_BEAR": "bear", "CRISIS": "bear",
         "CAUTIOUS": "cautious", "NEUTRAL": "neutral", "VOLATILE": "volatile"}
    return m.get((regime or "").strip().upper(), "unknown")


def _load_xy(table: str):
    """Return (X rows as list[list[float]], y win-labels, ollie_scores, regimes) from a
    decisions table. Rows need all FEATURES + ollie_score + shadow_pnl_pct populated."""
    conn = _conn()
    try:
        if not _table_exists(conn, table):
            return [], [], [], []
        cols = ", ".join(FEATURES) + ", ollie_score, shadow_pnl_pct, regime"
        rows = conn.execute(f"SELECT {cols} FROM {table}").fetchall()
    finally:
        conn.close()
    X, y, scores, regimes = [], [], [], []
    for r in rows:
        feats = [r[f] for f in FEATURES]
        if any(v is None for v in feats) or r["ollie_score"] is None or r["shadow_pnl_pct"] is None:
            continue
        try:
            feats = [float(v) for v in feats]
        except (TypeError, ValueError):
            continue
        rc = _regime_class(r["regime"])
        onehot = [1.0 if rc == g else 0.0 for g in REGIME_ONEHOT]
        X.append(feats + onehot)
        y.append(1 if float(r["shadow_pnl_pct"]) > 0 else 0)
        scores.append(float(r["ollie_score"]))
        regimes.append(rc)
    return X, y, scores, regimes


def _build_model():
    """Shallow, strongly-regularized GB — complexity matched to ~540 rows."""
    from sklearn.ensemble import GradientBoostingClassifier
    return GradientBoostingClassifier(
        n_estimators=60, max_depth=2, learning_rate=0.05,
        subsample=0.8, min_samples_leaf=30, random_state=42,
    )


def _ensure_shadow_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS ollie_gb_shadow (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               run_date TEXT,
               source TEXT,            -- which decision set the shadow row came from
               regime TEXT,
               ollie_score REAL,
               gb_win_prob REAL,
               gb_verdict TEXT,        -- APPROVE/REJECT the GB would have given
               static_verdict TEXT,    -- APPROVE/REJECT the live 2.0 gate gave
               agree INTEGER,          -- 1 if gb_verdict == static_verdict
               actual_win INTEGER,     -- realized outcome (shadow only — for scoring)
               note TEXT
           )"""
    )
    conn.commit()


def run_gb_shadow_eval() -> dict:
    """Train on the v5 corpus, CV-evaluate honestly, then SHADOW-LOG would-be vs actual
    decisions on the held-out OOS sets. Never gates live trades. status='error'+NTFY on
    any data shortfall — never a silent no-op."""
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _fail(cause: str) -> dict:
        logger.error("[ollie_gb_gate] %s", cause)
        try:
            from engine.notifier import notify
            notify(f"🔴 OLLIE-GB-GATE FAIL: {cause}", priority="high")
        except Exception:
            pass
        return {"status": "error", "cause": cause}

    try:
        import numpy as np
        from sklearn.model_selection import StratifiedKFold, cross_val_predict
        from sklearn.metrics import roc_auc_score, accuracy_score
    except Exception as e:
        return _fail(f"sklearn import failed: {e}")

    Xtr, ytr, score_tr, reg_tr = _load_xy("backtest_v5_ollie_decisions")
    Xoos, yoos, score_oos, reg_oos = ([], [], [], [])
    for t in ("backtest_decisions_oos", "backtest_decisions_oos_c"):
        a, b, c, d = _load_xy(t)
        Xoos += a; yoos += b; score_oos += c; reg_oos += d

    if len(Xtr) < 100:
        return _fail(f"train slice too small ({len(Xtr)} rows)")
    if len(set(ytr)) < 2:
        return _fail("train labels single-class — cannot learn")

    X = np.array(Xtr); y = np.array(ytr)
    base_rate = float(y.mean())              # win base rate (majority-class baseline)
    majority_acc = max(base_rate, 1 - base_rate)

    # ── Honest cross-validation (never in-sample) ────────────────────────────
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    model = _build_model()
    cv_prob = cross_val_predict(model, X, y, cv=skf, method="predict_proba")[:, 1]
    cv_auc = float(roc_auc_score(y, cv_prob))
    cv_acc = float(accuracy_score(y, (cv_prob >= GB_DECISION_PROB).astype(int)))

    # Static OllieScore's own discriminative power (does higher score → win?).
    score_auc = float(roc_auc_score(y, np.array(score_tr))) if len(set(y)) == 2 else float("nan")

    # Overfit check: full-fit train AUC vs CV AUC gap.
    model.fit(X, y)
    train_auc = float(roc_auc_score(y, model.predict_proba(X)[:, 1]))
    overfit_gap = round(train_auc - cv_auc, 3)

    # Per-regime CV accuracy (regime-robustness).
    regime_acc = {}
    reg_arr = np.array(reg_tr)
    cv_pred = (cv_prob >= GB_DECISION_PROB).astype(int)
    for rc in REGIME_ONEHOT:
        mask = reg_arr == rc
        if mask.sum() >= 20:
            regime_acc[rc] = round(float(accuracy_score(y[mask], cv_pred[mask])), 3)

    # ── SHADOW-LOG on held-out OOS (proves the would-be-vs-actual logging works) ──
    shadow_logged = 0
    agree = 0
    if Xoos:
        Xo = np.array(Xoos)
        probs = model.predict_proba(Xo)[:, 1]
        conn = _conn()
        try:
            _ensure_shadow_table(conn)
            conn.execute("DELETE FROM ollie_gb_shadow WHERE run_date=? AND source='oos_eval'",
                         (run_date,))
            for i in range(len(Xoos)):
                gb_v = "APPROVE" if probs[i] >= GB_DECISION_PROB else "REJECT"
                st_v = "APPROVE" if score_oos[i] >= STATIC_GATE else "REJECT"
                ag = 1 if gb_v == st_v else 0
                agree += ag
                conn.execute(
                    """INSERT INTO ollie_gb_shadow
                       (run_date, source, regime, ollie_score, gb_win_prob, gb_verdict,
                        static_verdict, agree, actual_win, note)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (run_date, "oos_eval", reg_oos[i], score_oos[i], round(float(probs[i]), 4),
                     gb_v, st_v, ag, yoos[i], "SHADOW — not live-gating"),
                )
                shadow_logged += 1
            conn.commit()
        finally:
            conn.close()
        # OOS scoring of the GB vs static gate (held-out honesty)
        oos_auc = float(roc_auc_score(yoos, probs)) if len(set(yoos)) == 2 else float("nan")
    else:
        oos_auc = float("nan")

    # Verdict: does the learned gate beat the static one CV/OOS? (honest, may be no.)
    # HONEST verdict keys off HELD-OUT OOS, not CV. CV (0.674) was inflated by regime
    # base-rate (in-sample bull won ~78%) which does NOT generalize — the OOS AUC is the
    # real test. Require a meaningful OOS edge above ~random to claim it beats static.
    beats_static = (oos_auc == oos_auc) and oos_auc > 0.55
    overfit_flag = overfit_gap > 0.15

    return {
        "status": "ok",
        "run_date": run_date,
        "mode": "SHADOW — never gates live trades",
        "train_n": len(Xtr), "oos_n": len(Xoos),
        "win_base_rate": round(base_rate, 3), "majority_acc": round(majority_acc, 3),
        "cv_auc": round(cv_auc, 3), "cv_acc": round(cv_acc, 3),
        "ollie_score_auc": round(score_auc, 3),
        "train_auc": round(train_auc, 3), "overfit_gap": overfit_gap,
        "overfit_flag": overfit_flag,
        "oos_auc": round(oos_auc, 3) if oos_auc == oos_auc else None,
        "per_regime_cv_acc": regime_acc,
        "shadow_logged": shadow_logged,
        "shadow_agree_with_static": agree,
        "beats_static": bool(beats_static),
        "note": ("GB learned-gate trained on the ~540-row core slice. SHADOW-MODE: "
                 "ollie_commander untouched, no live gating. Re-fit nightly. "
                 + ("⚠️ OVERFIT (train≫CV) — too thin for this complexity. "
                    if overfit_flag else "")
                 + (f"Learned gate shows OOS edge over static (oos_auc={round(oos_auc,3)})." if beats_static
                    else f"No robust OOS edge (oos_auc={round(oos_auc,3) if oos_auc==oos_auc else None}, "
                         "~random) — consistent with Phase 1 (static gate near-optimal; logged "
                         "features don't discriminate winners at trade level; revisit as regime "
                         "diversity grows).")),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    print(json.dumps(run_gb_shadow_eval(), indent=2))
