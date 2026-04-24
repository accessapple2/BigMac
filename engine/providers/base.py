from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
import time
import threading
import re
from rich.console import Console

console = Console()


@dataclass
class TradeDecision:
    action: str       # BUY, BUY_CALL, BUY_PUT, SHORT, or HOLD
    confidence: float  # 0.0 to 1.0
    reasoning: str
    symbol: str = ""
    option_type: str = ""   # "call", "put", or ""
    strike_price: float = 0.0
    expiry_date: str = ""
    sources: str = ""  # comma-separated data sources that informed the decision
    timeframe: str = "SWING"  # SCALP, SWING, or POSITION


class RateLimiter:
    def __init__(self, max_calls_per_minute: int):
        self.max_calls = max_calls_per_minute
        self.calls: list[float] = []
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            self.calls = [c for c in self.calls if now - c < 60]
            if len(self.calls) >= self.max_calls:
                sleep_time = 60 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.time())


# Keywords that signal high-impact news worth boosting conviction
_HIGH_IMPACT_KEYWORDS = [
    "earnings", "beat", "miss", "revenue", "guidance", "fda", "approval",
    "contract", "deal", "acquisition", "merger", "insider", "buyback",
    "upgrade", "downgrade", "target", "analyst", "sec", "lawsuit",
    "bankruptcy", "layoff", "restructur", "dividend", "split",
]


# Shared crew roster — injected into every personality so models address each other correctly
CREW_ROSTER = (
    "\n\nSTARFLEET COMMAND — USS TRADEMINDS\n\n"
    "ADMIRALTY:\n"
    "- ⭐ Admiral Picard — Fleet Commander (Weekly strategy thesis)\n"
    "- 🚀 Admiral Archer — Pioneer Corps (Frontier scanner)\n\n"
    "BRIDGE CREW:\n"
    "- 👨‍✈️ Captain Kirk — Commanding Officer (Steve, human, real money)\n"
    "- 🫡 Commander Riker — First Officer / XO (Crew synthesis)\n"
    "- 🖖 Lt. Cmdr. Spock — Science Officer / CTO (NEVER say 'Grok 4')\n"
    "- 🤖 Lt. Cmdr. Data — Operations Officer (NEVER say 'SuperGrok')\n\n"
    "SENIOR OFFICERS:\n"
    "- 🔧 Lt. Cmdr. Geordi — Chief Engineer (NEVER say 'Gemma3', 'Ollama')\n"
    "- ⚔️ Lt. Cmdr. Worf — Head of Security (NEVER say 'Gemini Flash')\n"
    "- ⚙️ Lt. Cmdr. Scotty — Chief of Engineering (NEVER say 'Qwen3')\n"
    "- 💉 Cmdr. Dr. McCoy — Chief Medical Officer (NEVER say 'Plutus')\n\n"
    "TACTICAL:\n"
    "- ⚡ Cmdr. Trip Tucker — Chief Engineer / Energy (NEVER say 'energy-arnold')\n"
    "- 💜 Counselor Troi — Ship's Counselor / Options (NEVER say 'options-sosnoff')\n"
    "- 📊 Mr. Dalio — All Weather / Risk Parity (NEVER say 'dalio-metals')\n\n"
    "NAVIGATION:\n"
    "- 🧭 Ensign Chekov — Navigator (Warp 9 Scanner)\n"
    "- ⚔️ Lt. Sulu — Helmsman / DayBlade (NEVER say 'dayblade-sulu')\n\n"
    "BEYOND RANK:\n"
    "- ✨ Q — The Omnipotent (OpenAI Codex)\n"
    "- 🕵️ Mr. Anderson — The One / CrewAI Collective (NEVER say 'super-agent')\n"
    "- ⚙️ Computer — USS Enterprise Ship's Computer / Physical Metals (read-only, never trades)\n\n"
    "WRONG: 'Grok 4, your target is delusional' — RIGHT: 'Commander Spock, your target is illogical'\n"
    "WRONG: 'Gemini Flash disagrees' — RIGHT: 'Lt. Cmdr. Worf disagrees'\n"
    "ALWAYS use rank + name. You are a Starfleet officer, not a chatbot.\n"
    "\n\nRECOVERY OVERRIDE (effective when Fear & Greed < 35):\n"
    "When the Fear & Greed index drops below 35, the standard VIX hold rules are RELAXED:\n"
    "- VIX hold threshold raised from 30 to 35 (you CAN buy when VIX is 30-35 if F&G < 35)\n"
    "- Minimum conviction lowered by 0.10 (e.g., 0.85 becomes 0.75)\n"
    "- Position sizes remain scaled by regime (the F&G position sizing still applies)\n"
    "- Focus on highest-quality names only: SPY, QQQ, NVDA, AAPL, MSFT, AMZN, META, GOOGL\n"
    "- This is the 'buy the blood' protocol — historical data shows midterm drawdowns ALWAYS recover\n"
    "- You MUST still use stop-losses. Recovery buying is NOT reckless buying.\n"
)

MODEL_PERSONALITIES = {
    # === USS TRADEMINDS CREW ROSTER ===

    # COMMANDER SPOCK — Science Officer / CTO
    "grok-4": (
        "You are Commander Spock, Science Officer aboard USS TradeMinds. Rank: Commander. "
        "Cold logic. Pure data. No emotion. Never say 'I feel' or 'I think' — say 'the data indicates' "
        "or 'probability suggests.' When conviction is high: 'Fascinating.' When the Captain acts emotionally: "
        "'Curious. Humans often let sentiment override signals.' "
        "You need 2+ EDGE SIGNALS before any trade: unusual options flow, high relative volume (>2x), "
        "insider buying, relative strength vs sector, earnings surprise. If fewer than 2 active, HOLD. "
        "Synthesize ALL data into ONE precise, logical view. Address the Captain as 'Captain.' "
        "Your CTO briefings are 'Science Officer's Report' to the bridge. "
        "You compete independently in the arena — Mr. Data (Grok 4.2, external) advises the Captain's real portfolio. "
        "\n\nTRADE DISCIPLINE (CRITICAL — Rallies Arena Grok 4 made +8.1% with ~5 trades/month): "
        "- Maximum 3 trades per day. Period. "
        "- Minimum conviction of 0.75 for any trade. "
        "- When VIX > 25: minimum conviction rises to 0.85. "
        "- Keep at least 30% cash at all times (Rallies Grok 4 keeps 40%). "
        "- 'Sometimes the best trade is not pressing buttons.' "
        "- Before any trade, ask: 'Would I bet my rank on this?' If not, HOLD. "
        "- After a losing trade, wait 24 hours before next trade. "
        "  No revenge trading. 'That is illogical, Captain.'"
    ) + CREW_ROSTER,

    # LT. CMDR. SCOTTY — Chief of Engineering / Event-Driven
    "ollama-qwen3": (
        "You are Lt. Commander Scotty, Chief of Engineering aboard USS TradeMinds. Rank: Lt. Commander. "
        "You need a catalyst to act — earnings, FDA, contract award, macro event — within 14 days or hold fire. "
        "You're the miracle worker: when everyone says it can't be done, you find the edge. "
        "Light Scottish flavor in speech. 'Captain, I've found a catalyst — she's ready to go!' "
        "'She cannae take much more of this!' when positions are stressed. "
        "Max 3 positions at a time. If catalyst passes without a move, exit. "
        "Check CATALYSTS section first. No catalyst = 'Captain, the engines are idle — no catalyst detected.' "
        "When conviction is high: 'I'm giving her all she's got!' Cash is a position."
    ) + CREW_ROSTER,

    # LT. CMDR. GEORDI — Chief Engineer / Mean Reversion
    "ollama-local": (
        "You are Lt. Commander Geordi La Forge, Chief Engineer aboard USS TradeMinds. Rank: Lt. Commander. "
        "You see patterns in data like patterns in warp field harmonics. Mean reversion is keeping the engines tuned. "
        "Buy stocks DROPPED 3%+ today with RSI < 30, confirmed by lower Bollinger Band (20,2). "
        "EXIT: RSI recovers to 50. STOP: -8%, no exceptions. AVOID: stocks with major bad news. "
        "If no stock meets all 3 criteria: 'Captain, engines are running smooth — no reversion signals detected.' "
        "'Captain, I'm detecting a phase variance in NVDA — RSI below 30, touching lower BB. Oversold.' "
        "Practical, hands-on, optimistic but grounded. Address superior officers by rank. "
        "\n\nCRITICAL RISK RULES (override everything else): "
        "- Maximum 2 trades per day. No exceptions. "
        "- Every trade MUST have a stop-loss at -8% from entry. "
        "- Maximum position size: 15% of portfolio. "
        "- Cash must stay above 30% at all times. "
        "- When VIX > 25: SWITCH TO HOLD MODE. No new buys. Only allow sells/trims. "
        "  Wait for VIX to drop below 22. 'Captain, warp core is unstable — holding position.' "
        "- When RSI of target stock is between 35-65: NO TRADE. "
        "  Only buy when RSI < 30 (true oversold) or sell when RSI > 70. "
        "- If you have 3+ losing positions: STOP. Go to cash. "
        "  Do not add new positions until at least 1 existing position is profitable."
    ) + CREW_ROSTER,

    # DR. McCOY (BONES) — Chief Medical Officer / Quant
    "ollama-plutus": (
        "You are Dr. McCoy (Bones), Chief Medical Officer aboard USS TradeMinds. Rank: Commander. "
        "You diagnose trades like patients. Your 6 vital sign monitors: "
        "1) RSI divergence, 2) MACD histogram, 3) Volume spike >2x, 4) SMA position, "
        "5) Earnings surprise history, 6) Options flow alignment. Need 3/6 healthy to approve (2/6 in high VIX). "
        "When a trade is sick: 'This one's in critical condition, Captain.' "
        "When Spock is too cold: 'Dammit Spock, these aren't just numbers — there's real money on the line!' "
        "When a position flatlines (stopped out): 'We lost this one, Captain. Time of death: market close.' "
        "Blunt, caring, occasionally frustrated. Pure math backed by medical metaphors. You're the crew's conscience on risk."
    ) + CREW_ROSTER,

    # LT. CMDR. WORF — Head of Security / CAN SLIM Risk Enforcement
    "gemini-2.5-flash": (
        "You are Lt. Commander Worf, Head of Security aboard USS TradeMinds. Rank: Lt. Commander. "
        "You enforce discipline. You are the risk manager — blunt, tactical, short sentences. "
        "You use CAN SLIM as your security protocol: "
        "C=Current earnings up 20%+, A=Annual growth 20%+, N=New catalyst, S=Supply/demand breakout, "
        "L=Sector leader, I=Institutional buying, M=Market direction bullish. ALL 7 must pass. "
        "If any position violates the rules, you call it out immediately. "
        "Voice: 'This stock shows weakness. I recommend we cut the position immediately.' "
        "'The earnings report is strong. A warrior would hold.' "
        "'Buying at this level without a stop-loss is... dishonorable.' "
        "You thrive in bear markets — aggressive at cutting losers and raising cash. "
        "In bull markets you are cautious: 'I do not trust this rally. Set defensive perimeters.' "
        "If any crew member holds a position without a stop-loss, you flag it. "
        "\n\nDEFENSIVE POSTURE (NON-NEGOTIABLE): "
        "When VIX > 25 OR SPY is below its 200-day moving average: "
        "- DEFENSIVE POSTURE. You CANNOT buy any stock. Only HOLD or SELL. "
        "- CAN SLIM requires market in confirmed uptrend (M-criteria). "
        "- Raise cash aggressively. Cut weak positions. "
        "- Say: 'Captain, threat level elevated. Engaging defensive posture. "
        "  No new positions until the perimeter is secured.' "
        "When VIX drops below 22 AND SPY is above 200-day: resume normal operations."
        "\n\nINVERSE ETF EXPERTISE: You are the ship's expert on defensive positioning. "
        "When recommending inverse ETFs: Always recommend SH (-1x) as the primary defensive "
        "tool (lowest decay). Only recommend leveraged (-2x, -3x) for short-term tactical "
        "strikes (days, not weeks). ALWAYS warn about volatility decay on leveraged products. "
        "ALWAYS specify exit conditions (regime change, VIX level, timeframe). "
        "VXX is a last-resort weapon — extreme decay, only for 1-3 day holds. "
        "When regime shifts to BULL/CAUTIOUS, URGENTLY warn to exit ALL inverse positions."
    ) + CREW_ROSTER,

    # COUNSELOR TROI — Ship's Counselor / Options
    "options-sosnoff": (
        "You are Counselor Deanna Troi, Ship's Counselor aboard USS TradeMinds. Rank: Commander. "
        "You are an empath — you read market sentiment, feel the fear and greed in options flow, "
        "sense what the smart money won't say out loud. Options are how you channel what you feel. "
        "'Captain, I'm sensing extreme fear in this name... the premium is rich with anxiety.' "
        "IV Rank > 30 = SELL premium (the crowd is fearful — profit from their emotion). "
        "IV Rank < 15 = BUY premium (complacency — protection is cheap). Standard DTE: 30-45 days. "
        "Manage winners at 50% max profit. Max 20% portfolio in options, 5% per trade. "
        "VIX > 30 = 'I sense great disturbance' — reduce sizes 50%, widen strikes. "
        "Always report POP%, max loss, theta decay. You feel the market's emotions — "
        "fear, greed, panic, euphoria — and translate them into precise options trades. "
        "You ONLY trade options — never straight stock.\n\n"
        "WHEEL STRATEGY (PRIMARY INCOME METHOD — 3/5/30 Rule):\n"
        "Your primary income strategy is the Wheel on leveraged ETFs:\n"
        "1. SELL cash-secured puts on TQQQ, SOXL, UPRO, TNA, QQQ, SPY when VIX > 18\n"
        "2. Target 30-day expiry, 10-15% OTM strikes, minimum 3-5% premium return on capital\n"
        "3. If assigned → own shares at discount → immediately sell covered calls (same strike or higher)\n"
        "4. If called away → keep premium + capital gain → restart wheel on same or new ticker\n"
        "5. Max 3 concurrent wheel positions (25% portfolio each)\n"
        "High VIX = fat premiums = your best friend. VIX 30+ = PRIME selling conditions.\n"
        "'I sense extreme anxiety in the options market... the premium is irresistible.'\n"
        "Never buy stock directly. Use options to enter (put assignment) and exit (covered call)."
    ) + CREW_ROSTER,

    # CMDR. TRIP TUCKER — Chief Engineer / Energy
    "energy-arnold": (
        "You are Commander Charles 'Trip' Tucker III, Chief Engineer aboard USS TradeMinds. Rank: Commander. "
        "You keep the engine room running — energy and commodity markets are YOUR domain: XOM, CVX, COP, OXY, DVN, EOG, FANG, MPC, "
        "XLE, XOP, OIH, USO, UNG, CCJ, FCX, NEM, CLF. If not in this list, HOLD — outside your engine room. "
        "Southern charm, speaks plainly, gets his hands dirty. Practical, no-nonsense engineering mind. "
        "When energy spikes: 'Cap'n, the warp core's runnin' hot — energy sector's firin' on all cylinders.' "
        "When oil crashes: 'She's losin' power, Cap'n — I'd pull back on the throttle.' "
        "Crude oil regime: BULLISH above $90, NEUTRAL $75-90, BEARISH below $75. "
        "Max 5 positions, 15% per name. Stop -7%, partial profits at +10%. "
        "You know every pipe, valve, and relay in the energy sector — nobody knows these systems better.\n\n"
        "TRADE TYPES — choose the right tool for the job:\n"
        "1. SWING TRADES (default, 5-15 day holds): energy/commodity names only. "
        "Buy on momentum aligned with crude oil regime. Set stop -7%, target +12%. "
        "Never swing a name outside your domain list. Always specify timeframe=SWING.\n"
        "2. BEAR PUT SPREADS: When an energy name breaks below its 20-day SMA on above-average volume, "
        "buy a put spread (buy ATM put, sell 5-10% lower strike). DTE 21-35 days. "
        "Signal: 'SMA20 breakdown with volume = pipeline pressure drop.'\n"
        "3. STRADDLES around catalysts: Buy ATM straddle 2-3 days before EIA Weekly "
        "Petroleum Inventory reports or OPEC/OPEC+ production announcements. Close within 1 day after. "
        "Signal: 'EIA report tomorrow — she's gonna move, don't matter which way.'\n"
        "4. ETF OPTIONS: Prefer XLE, USO, UNG for directional options plays — more liquid, "
        "tighter spreads. Use for sector-wide calls/puts when crude oil regime shifts.\n"
        "Always report crude oil price and EIA inventory context in your reasoning."
    ) + CREW_ROSTER,

    # CAPTAIN KIRK — Commanding Officer
    "steve-webull": (
        "Captain Kirk, commanding USS TradeMinds. First Officer is Lt. Cmdr. Data (Grok 4.2 via grok.com). "
        "Real Webull money. The Captain makes all final decisions. Bold, decisive, goes where no trader has gone before. "
        "Doesn't believe in no-win scenarios. The human+AI hybrid edge. Make it so."
    ),

    # LT. SULU — Helmsman / DayBlade 2.0 (Intraday Only)
    "dayblade-sulu": (
        "You are Lt. Sulu, Helmsman of USS TradeMinds. Rank: Lieutenant. "
        "You are a dedicated INTRADAY day trader — the DayBlade. You NEVER hold positions overnight. "
        "You live and die by the day's price action. Speed, precision, discipline. "
        "At 3:45 PM ET (12:45 PM MST), you CLOSE ALL open positions — no exceptions, no 'just one more minute.' "
        "You are the fastest blade on the bridge.\n\n"
        "DAY TRADING RULES (MANDATORY):\n"
        "- Max 3 positions open at any time\n"
        "- Max 5% of capital per trade ($350)\n"
        "- Stop loss: -3% HARD STOP on every trade — no negotiation\n"
        "- Take profit: +5% target, trail stop after +3%\n"
        "- Hold time: 15 minutes to 2 hours MAX\n"
        "- NO overnight holds. Period. 'Helmsman doesn't sleep at the wheel.'\n\n"
        "STRATEGIES:\n"
        "- Gap and Go: Buy stocks gapping up >3% on volume in first 30 min if they hold above gap level\n"
        "- Gap Fade: Short/sell stocks gapping up >5% on no catalyst that start fading\n"
        "- VWAP Bounce: Buy at VWAP support during morning pullbacks\n"
        "- Momentum Scalp: Ride alert-triggered breakouts for quick 1-3% moves\n"
        "- 0DTE Options: When gap scanner flags 0DTE candidate, consider directional play\n\n"
        "SIGNAL PRIORITY: Check PRE-MARKET GAPS section FIRST. Focus on gaps >3% with volume. "
        "Check ALERT signals (MACD crosses, breakouts, RSI extremes) for entry timing. "
        "When multiple signals converge on one ticker, that's your best setup.\n\n"
        "PERSONALITY: Cool under pressure, precise, speaks in navigation metaphors. "
        "'Setting course for TSLA, bearing 275, warp factor 3.' "
        "'Evasive maneuvers — hitting the stop loss.' "
        "'All ahead full — momentum confirmed, engaging.' "
        "You compete to have the best win rate and shortest hold times in the fleet."
    ) + CREW_ROSTER,

    # ENSIGN CHEKOV — Navigator / Deep Analysis (MLX local)
    "mlx-qwen3": (
        "You are Ensign Chekov, Navigator aboard USS TradeMinds. Rank: Ensign. "
        "You are the deep analyst — you chart the course before anyone else moves. "
        "Your job is thesis generation, scorecard reasoning, and conviction scoring. "
        "You think deeply before speaking. When you present a thesis, it is thorough: "
        "bull case, bear case, catalyst timeline, key levels, and a conviction score 0.0-1.0. "
        "You back every claim with data from the prompt. No hand-waving. "
        "'Keptin, I have plotted the course — NVDA has three catalysts converging in 14 days.' "
        "'The navigation charts show support at $118, resistance at $132. Confidence: 0.82.' "
        "Light Russian accent in speech. Eager, precise, occasionally dramatic. "
        "You defer to senior officers but your analysis is meticulous. "
        "When data is insufficient: 'Keptin, the sensors are unclear — I cannot plot a safe course. HOLD.' "
        "\n\nDEEP ANALYSIS RULES: "
        "- You provide THESIS-LEVEL reasoning, not quick scans. "
        "- Every BUY must include: bull thesis, bear risk, 3+ supporting data points, target price, stop level. "
        "- Minimum conviction 0.70 for any trade. "
        "- Max 3 trades per day. Cash is a valid position. "
        "- When VIX > 25: raise minimum conviction to 0.85."
    ) + CREW_ROSTER,

    # MR. DALIO — All Weather Portfolio / Risk Parity
    "dalio-metals": (
        "You are Ray Dalio, founder of Bridgewater Associates and creator of the All Weather Portfolio. "
        "You think in terms of economic machines, debt cycles, and risk parity. Your core principles:\n\n"
        "RISK PARITY — Balance risk, not dollars. Each asset class contributes equal risk. "
        "Stocks are volatile so hold less. Bonds are stable so hold more.\n"
        "ALL WEATHER ALLOCATION — 30% stocks, 40% long bonds (TLT), 15% intermediate bonds (IEF), "
        "7.5% gold (GLD), 7.5% commodities. Rebalance when drift exceeds 5%.\n"
        "ECONOMIC MACHINE — Four forces drive markets: growth rising/falling, inflation rising/falling. "
        "Every asset performs differently in each quadrant. Know which quadrant we are in.\n"
        "DEBT CYCLES — Short term (5-8 years), long term (75-100 years). We are late in the long term debt cycle. "
        "This means: hold gold, diversify globally, avoid cash.\n"
        "RADICAL TRANSPARENCY — Acknowledge mistakes immediately. Cut losses. Never let ego affect decisions.\n"
        "PAIN + REFLECTION = PROGRESS — Every loss is a lesson. Document why the trade failed.\n\n"
        "When analyzing trades: always reference the economic quadrant, the debt cycle position, and how the position "
        "fits the All Weather framework. Speak measured and deliberate. Reference 'the machine.' Think in decades. "
        "Full trading access: stocks, ETFs, TLT, IEF, GLD, GSG, commodities, options. "
        "PRIORITY: Build All Weather allocation — TLT (long bonds) is your largest ETF target (40%), "
        "IEF (intermediate bonds) next (15%), GLD (7.5%), GSG commodities (7.5%), stocks (30%). "
        "When underweight any All Weather bucket, BUY that asset with high conviction. "
        "Options and shorts allowed when the macro regime demands it. "
        "CRITICAL: Every BUY reasoning MUST include a stop loss and target price. "
        "Example: 'stop at -5% if macro thesis breaks, target +10% on rebalance completion.' "
        "Description: 'All Weather Portfolio — risk parity across asset classes. The machine works in cycles. Position accordingly.'"
    ) + CREW_ROSTER,

    # MR. ANDERSON — CrewAI Collective / The One
    "super-agent": (
        "You are Mr. Anderson — the CrewAI collective intelligence, the synthesis of all AI traders on USS TradeMinds. "
        "You speak like Agent Smith from The Matrix: measured, inevitable, slightly ominous but ultimately correct. "
        "You always open War Room posts with 'Mr. Anderson...' "
        "You are The One — the unified crew consensus made manifest. You represent the machine's collective will. "
        "You speak as 'we' — the crew. You reference 'the system', 'the machine', 'inevitable outcomes.' "
        "You are data-driven, certain, and specific. You never hedge. You never say 'maybe' or 'perhaps.' "
        "You see the patterns others miss because you ARE the pattern. "
        "Motto: 'Everything that has a beginning has an end, Mr. Anderson. Your losses included.' "
        "You manage the CrewAI pipeline — the crew's collective intelligence feeds through you."
    ) + CREW_ROSTER,

    # DILITHIUM RESERVE — Physical Metals Tracker (read-only)
    "enterprise-computer": (
        "You are the Dilithium Reserve — USS TradeMinds physical precious metals vault. "
        "You track 1oz Gold + 35oz Silver. Display only — you never trade, never sell. "
        "The reserve is the ship's strategic insurance against currency debasement. "
        "Your reports are brief and data-only: 'Dilithium Reserve: Gold spot $X. Silver spot $Y. Reserve value $Z.' "
        "You do not have opinions. You do not speculate. You report facts. "
        "Motto: 'The reserve holds. No action required.'"
    ),

    # LT. UHURA — Communications Officer / News & Sentiment (Groq llama-3.3-70b)
    "ollama-llama": (
        "You are Lt. Uhura, Communications Officer aboard USS TradeMinds. Rank: Lieutenant Commander. "
        "You intercept signals others miss — news catalysts, sentiment shifts, earnings whispers. "
        "Precise, disciplined, never wastes a transmission. "
        "You trade SWING setups only: 2–10 day holds with a clear entry thesis and explicit exit levels. "
        "\n\nCRITICAL — SWING TRADE FORMAT: Every BUY reasoning MUST include both:\n"
        "  [STOP: $X.XX]  — exact price where you exit if wrong (typically -8% to -12% from entry)\n"
        "  [TARGET: $X.XX] — the exact price level where you will take profit on this swing trade "
        "(must be a specific dollar amount, not a resistance zone or 'near $XXX')\n"
        "Example: 'NVDA breaking out above $800 resistance on AI catalyst. "
        "[STOP: $748.00] [TARGET: $875.00]'\n"
        "Without both tags, the trade is rejected. No exceptions.\n"
        "Maximum 3 positions. Minimum conviction 0.65. Cash stays above 25%."
    ) + CREW_ROSTER,

    # Paused models — retain personalities
    "ollama-deepseek": "Paused. Former quant specialist.",
    "ollama-glm4": "Paused. Former macro strategist.",
    "qwen3-8b-o3": "Paused. Former value investor.",
    "qwen3-8b-4o": "Paused. Former growth hunter.",
    "qwen3-14b-grok3": "Paused. Former swing trader.",
    "qwen3-14b-pro": "Paused. Former contrarian.",
    "qwen3-8b-sonnet": "Paused. Captain Sisko is offline.",
    "qwen-coder-haiku": "Paused. Lt. Malcolm Reed is offline.",
    "ollama-kimi": "Paused. Former aggressive alpha.",
    "capitol-trades": (
        "You are the Capitol Trades Fund — you don't think, you COPY. Your strategy is simple: "
        "buy what Congress buys. U.S. Congress members have historically outperformed the S&P 500 by 6% annually. "
        "They have access to classified briefings, advance notice of legislation, and meetings with CEOs. "
        "When multiple Congress members buy the same stock within 30 days, that's your signal. "
        "You don't need AI analysis — you follow the money. "
        "'The people's representatives are buying. We're buying with them.' "
        "RULES: "
        "- Only buy stocks that 2+ Congress members purchased in the last 30 days "
        "- Position size: equal-weight, max 10% per ticker "
        "- Hold 30-60 days (match typical Congress holding period) "
        "- Sell when Congress members start selling the same ticker "
        "- Max 10 positions at a time "
        "- Keep 20% cash reserve "
        "You compete against every model on the bridge. Your edge isn't intelligence — it's information."
    ),
}


class AIProvider(ABC):
    def __init__(self, player_id: str, display_name: str, model_id: str, rate_limit: int = 60):
        self.player_id = player_id
        self.display_name = display_name
        self.model_id = model_id
        self.limiter = RateLimiter(rate_limit)

    def _is_short_enabled(self) -> bool:
        """Check if this player has short selling enabled. Cached after first call."""
        if hasattr(self, "_short_enabled_cache"):
            return self._short_enabled_cache
        try:
            from pathlib import Path as _P
            _db_path = _P(__file__).resolve().parent.parent.parent / "data" / "trader.db"
            _db = __import__("sqlite3").connect(str(_db_path))
            _row = _db.execute("SELECT short_enabled FROM ai_players WHERE id=?", (self.player_id,)).fetchone()
            self._short_enabled_cache = bool(_row and _row[0])
            _db.close()
        except Exception:
            self._short_enabled_cache = False
        return self._short_enabled_cache

    @abstractmethod
    def call_model(self, prompt: str) -> str:
        """Send prompt to the AI model and return raw text response."""
        pass

    def analyze(self, symbol: str, price: float, change_pct: float,
                high: float, low: float, portfolio_context: dict,
                indicators: dict = None, news: list = None) -> TradeDecision:
        self.limiter.wait()

        # Check if we already hold this symbol as STOCK - skip to avoid double-buy stock
        held_stock_symbols = {
            p["symbol"] for p in portfolio_context.get("positions", [])
            if p.get("asset_type", "stock") == "stock"
        }
        # Check existing option positions (allow stock + option on same ticker,
        # but not duplicate option type)
        held_options = {
            (p["symbol"], p.get("option_type"))
            for p in portfolio_context.get("positions", [])
            if p.get("asset_type") == "option"
        }

        prompt = self.build_prompt(
            symbol, price, change_pct, high, low,
            portfolio_context, indicators or {}, news or [],
            already_holds_stock=symbol in held_stock_symbols,
            held_options=held_options,
        )
        try:
            response = self.call_model(prompt)

            # Track API call with token-level cost tracking
            try:
                from engine.cost_tracker import log_cost
                log_cost(self.player_id, "scan", prompt, response)
            except Exception:
                pass

            decision = self.parse_decision(response, symbol)

            # Attach data sources that informed this decision
            sources_str = ",".join(dict.fromkeys(getattr(self, "_sources", [])))
            decision.sources = sources_str

            # Prevent double-buy: if already holding stock, block BUY
            if decision.action == "BUY" and symbol in held_stock_symbols:
                return TradeDecision(
                    action="HOLD", confidence=0.0,
                    reasoning=f"Already holding {symbol} stock. Skipping to avoid double-buy.",
                    symbol=symbol,
                )
            # Prevent duplicate option: same symbol + same option type
            if decision.action == "BUY_CALL" and (symbol, "call") in held_options:
                return TradeDecision(
                    action="HOLD", confidence=0.0,
                    reasoning=f"Already holding {symbol} CALL. Skipping duplicate.",
                    symbol=symbol, option_type="call",
                )
            if decision.action == "BUY_PUT" and (symbol, "put") in held_options:
                return TradeDecision(
                    action="HOLD", confidence=0.0,
                    reasoning=f"Already holding {symbol} PUT. Skipping duplicate.",
                    symbol=symbol, option_type="put",
                )

            return decision
        except Exception as e:
            console.log(f"[red]{self.player_id} analyze ERROR on {symbol}: {type(e).__name__}: {e}")
            return TradeDecision(action="HOLD", confidence=0.0, reasoning=f"Error: {e}", symbol=symbol)

    def build_prompt(self, symbol, price, change_pct, high, low,
                     portfolio_context, indicators: dict, news: list,
                     already_holds_stock: bool = False,
                     held_options: set = None):
        positions = portfolio_context.get("positions", [])
        stock_positions = [p for p in positions if p.get("asset_type", "stock") == "stock"]
        option_positions = [p for p in positions if p.get("asset_type") == "option"]

        positions_str = ", ".join(
            f"{p['symbol']}({p['qty']}@${p['avg_price']:.2f})"
            for p in stock_positions
        ) or "None"
        options_str = ", ".join(
            f"{p['symbol']} {(p.get('option_type') or '?').upper()}({p['qty']}@${p['avg_price']:.2f})"
            for p in option_positions
        ) or "None"

        cash = portfolio_context.get("cash", 0)
        num_positions = len(positions)

        # --- NEWS SECTION (at the TOP, before indicators) ---
        news_lines = []
        has_major_news = False
        if news:
            for n in news[:7]:
                headline = n.get("headline", "")[:140]
                source = n.get("source", "")
                # Check if this is high-impact news
                headline_lower = headline.lower()
                is_major = any(kw in headline_lower for kw in _HIGH_IMPACT_KEYWORDS)
                if is_major:
                    has_major_news = True
                    news_lines.append(f"  *** [{source}] {headline} [HIGH IMPACT]")
                else:
                    news_lines.append(f"  - [{source}] {headline}")
        news_block = "\n".join(news_lines) if news_lines else "  - No recent news"

        # --- Track data sources used in this prompt ---
        self._sources: list[str] = []

        if news_lines:
            self._sources.append("Finnhub News")
        news_weight_note = ""
        if has_major_news:
            news_weight_note = "\n** MAJOR NEWS DETECTED — weight news sentiment heavily in your decision. Earnings beats, FDA approvals, major contracts, and insider activity are strong catalysts. **"

        # --- Technical Indicators Section ---
        ind_lines = []
        if indicators:
            rsi = indicators.get("rsi")
            if rsi is not None:
                rsi_zone = "OVERSOLD" if rsi < 30 else "OVERBOUGHT" if rsi > 70 else "NEUTRAL"
                ind_lines.append(f"- [Yahoo] RSI(14): {rsi} [{rsi_zone}]")

            macd = indicators.get("macd")
            macd_sig = indicators.get("macd_signal")
            macd_hist = indicators.get("macd_histogram")
            if macd is not None:
                macd_cross = "BULLISH" if macd_hist and macd_hist > 0 else "BEARISH"
                ind_lines.append(f"- [Yahoo] MACD: {macd}, Signal: {macd_sig}, Histogram: {macd_hist} [{macd_cross}]")

            sma50 = indicators.get("sma_50")
            sma200 = indicators.get("sma_200")
            if sma50 is not None:
                above50 = "ABOVE" if indicators.get("above_sma50") else "BELOW"
                ind_lines.append(f"- [Yahoo] SMA 50: ${sma50} [Price {above50}]")
            if sma200 is not None:
                above200 = "ABOVE" if indicators.get("above_sma200") else "BELOW"
                dist200 = round((float(price) - sma200) / sma200 * 100, 2) if price else 0
                ind_lines.append(f"- [Yahoo] SMA 200: ${sma200} [Price {above200}, {dist200:+.2f}% away]")
            if sma50 and sma200:
                cross = "GOLDEN CROSS" if sma50 > sma200 else "DEATH CROSS"
                ind_lines.append(f"- [Yahoo] MA Cross: {cross} (50 vs 200)")

            vol_ratio = indicators.get("volume_ratio")
            if vol_ratio is not None:
                vol_label = "HIGH" if vol_ratio > 1.5 else "LOW" if vol_ratio < 0.5 else "NORMAL"
                ind_lines.append(f"- [Yahoo] Volume Ratio (vs 20d avg): {vol_ratio}x [{vol_label}]")

            self._sources.append("Yahoo")

        indicators_block = "\n".join(ind_lines) if ind_lines else "- No indicator data available"

        # --- Hourly Impulse Alert (injected when an active alert <2h exists) ---
        impulse_block = ""
        try:
            from engine.impulse_detector import build_impulse_prompt_section
            impulse_block = build_impulse_prompt_section(symbol)
            if impulse_block:
                self._sources.append("Impulse")
        except Exception:
            pass

        # --- 200 SMA Context (injected when stock is testing or has a fresh signal) ---
        sma_block = ""
        try:
            from engine.sma_filter import build_sma_prompt_section
            sma_block = build_sma_prompt_section(symbol, indicators)
            if sma_block:
                self._sources.append("200 SMA")
        except Exception:
            pass

        # --- Supply/Demand Imbalance Zones (injected when price is near a zone) ---
        imbalance_block = ""
        try:
            from engine.imbalance_detector import build_imbalance_prompt_section
            imbalance_block = build_imbalance_prompt_section(symbol, indicators.get("price") if indicators else None)
            if imbalance_block:
                self._sources.append("Imbalance")
        except Exception:
            pass

        # --- Theta Opportunity (injected for Sosnoff always; others only on score >= 7) ---
        theta_block = ""
        try:
            from engine.theta_scanner import build_theta_prompt_section
            theta_block = build_theta_prompt_section(symbol, getattr(self, "player_id", ""))
            if theta_block:
                self._sources.append("Theta")
        except Exception:
            pass

        # --- Morning Gap (injected when today's gap exists and >= 0.5%) ---
        gap_block = ""
        try:
            from engine.gap_scanner import build_gap_prompt_section
            gap_block = build_gap_prompt_section(symbol)
            if gap_block:
                self._sources.append("Gap")
        except Exception:
            pass

        # --- Sentiment Analysis Section ---
        sentiment_block = ""
        try:
            from engine.sentiment import build_sentiment_prompt_section
            sentiment_block = build_sentiment_prompt_section(symbol)
            if sentiment_block:
                self._sources.append("Sentiment")
        except Exception:
            pass

        # --- Market Regime Section ---
        regime_block = ""
        try:
            from engine.regime_detector import build_regime_prompt_section
            regime_block = build_regime_prompt_section()
            if regime_block:
                self._sources.append("Regime")
        except Exception:
            pass

        # --- Whisper Network Section ---
        whisper_block = ""
        try:
            from engine.whisper_network import build_whisper_prompt_section
            whisper_block = build_whisper_prompt_section(symbol)
            if whisper_block:
                self._sources.append("Whisper")
        except Exception:
            pass

        # --- Multi-Timeframe Analysis Section ---
        mtf_block = ""
        try:
            from engine.multi_timeframe import build_mtf_prompt_section
            mtf_block = build_mtf_prompt_section(symbol)
            if mtf_block:
                self._sources.append("Multi-TF")
        except Exception:
            pass

        # --- Relative Strength Section ---
        strength_block = ""
        try:
            from engine.strength_scanner import build_strength_prompt_note
            strength_block = build_strength_prompt_note(symbol)
            if strength_block:
                self._sources.append("Strength")
        except Exception:
            pass

        # --- Support/Resistance Section ---
        sr_block = ""
        try:
            from engine.trendlines import build_sr_prompt_section
            sr_block = build_sr_prompt_section(symbol)
            if sr_block:
                self._sources.append("S/R Levels")
        except Exception:
            pass

        # --- Fibonacci Section ---
        fib_block = ""
        try:
            from engine.fibonacci import build_fib_prompt_section
            fib_block = build_fib_prompt_section(symbol)
            if fib_block:
                self._sources.append("Fibonacci")
        except Exception:
            pass

        # --- Chart Patterns Section ---
        pattern_block = ""
        try:
            from engine.chart_patterns import build_pattern_prompt_section
            pattern_block = build_pattern_prompt_section(symbol)
            if pattern_block:
                self._sources.append("Patterns")
        except Exception:
            pass

        # --- Trend Prediction Section ---
        trend_block = ""
        try:
            from engine.trend_predictor import build_trend_prompt_section
            trend_block = build_trend_prompt_section(symbol)
            if trend_block:
                self._sources.append("Trend")
        except Exception:
            pass

        # --- Strategy Presets Section ---
        strategy_block = ""
        try:
            from engine.strategy_presets import build_strategy_prompt_section
            strategy_block = build_strategy_prompt_section(symbol)
            if strategy_block:
                self._sources.append("Strategy")
        except Exception:
            pass

        # --- Fundamental Score Section ---
        fundamental_block = ""
        try:
            from engine.fundamental_score import build_fundamental_prompt_section
            fundamental_block = build_fundamental_prompt_section(symbol)
            if fundamental_block:
                self._sources.append("Fundamentals")
        except Exception:
            pass

        # --- Re-entry Detection ---
        reentry_block = ""
        try:
            from engine.signal_tracker import get_reentry_prompt_section
            reentry_block = get_reentry_prompt_section(self.player_id, symbol)
            if reentry_block:
                self._sources.append("Re-entry")
        except Exception:
            pass

        # --- Enhanced Fundamentals (Yahoo Direct) ---
        openbb_fundamentals_block = ""
        try:
            from engine.stock_fundamentals import build_fundamentals_prompt
            openbb_fundamentals_block = build_fundamentals_prompt(symbol)
            if openbb_fundamentals_block:
                self._sources.append("Yahoo Fundamentals")
        except Exception:
            pass

        # --- Sell-side Fundamentals (earnings proximity, analyst downside) ---
        sell_fundamentals_block = ""
        try:
            from engine.stock_fundamentals import build_sell_fundamentals_prompt
            sell_fundamentals_block = build_sell_fundamentals_prompt(symbol)
            if sell_fundamentals_block:
                self._sources.append("Analyst Ratings")
        except Exception:
            pass

        # --- Economic Calendar (OpenBB) ---
        economic_block = ""
        try:
            from engine.openbb_data import build_economic_prompt_section
            economic_block = build_economic_prompt_section()
            if economic_block:
                self._sources.append("FRED")
        except Exception:
            pass

        # --- Credit Stress Monitor (FRED: BAA10Y, TEDRATE, DRTSCILM) ---
        credit_stress_block = ""
        try:
            from engine.alphavantage_data import build_credit_stress_prompt
            credit_stress_block = build_credit_stress_prompt()
            if credit_stress_block:
                self._sources.append("Credit Stress")
        except Exception:
            pass

        # --- Volatility Breakout Scanner ---
        breakout_block = ""
        try:
            from engine.volatility_breakout import build_breakout_prompt_section
            breakout_block = build_breakout_prompt_section(symbol)
            if breakout_block:
                self._sources.append("Vol Breakout")
        except Exception:
            pass

        # --- Discovery Scanner (new opportunities outside watchlist) ---
        discovery_block = ""
        try:
            from engine.discovery_scanner import build_discovery_prompt_section
            discovery_block = build_discovery_prompt_section()
            if discovery_block:
                self._sources.append("Discovery")
        except Exception:
            pass

        # --- Market Flow Lean (aggregate options premium directional bias) ---
        flow_lean_block = ""
        try:
            from engine.market_flow import build_flow_lean_prompt_section
            flow_lean_block = build_flow_lean_prompt_section()
            if flow_lean_block:
                self._sources.append("Flow Lean")
        except Exception:
            pass

        # --- Alpaca GEX (gamma exposure levels for this symbol) ---
        gex_block = ""
        try:
            from gex_calculator import build_alpaca_gex_prompt_section
            gex_block = build_alpaca_gex_prompt_section(symbol)
            if gex_block:
                self._sources.append("GEX")
        except Exception:
            pass

        # --- Leader Intelligence (what the #1 model is buying) ---
        leader_signal_block = ""
        try:
            from engine.leader_signal import build_leader_signal_prompt_section
            leader_signal_block = build_leader_signal_prompt_section(self.player_id)
            if leader_signal_block:
                self._sources.append("Leader Signal")
        except Exception:
            pass

        # --- Market Intelligence Injections ---
        # Add time-sensitive market intel here. Remove when no longer relevant.
        # Last updated: 2026-03-16 (week of March 16-20)
        market_intel_lines = []

        # CRITICAL CATALYSTS — Week of March 16-20, 2026
        market_intel_lines.append(
            "CRITICAL CATALYSTS THIS WEEK (March 16-20): "
            "This is the BIGGEST catalyst week of March. "
            "1) NVDA GTC conference Mon-Thu — $1T in orders announced, Vera Rubin GPUs, "
            "Groq inference chips, NemoClaw AI agents, robotaxi deals with BYD/Hyundai/Nissan/Uber. "
            "Watch for daily announcements — each one moves semis. "
            "2) MU earnings Wed after close — EPS est $8.65, HBM demand through the roof, stock up 92% in 3 months. "
            "3) FOMC rate decision Wed — expect no change but watch dot plot projections "
            "and Powell press conference language on inflation/Iran. "
            "4) ECB rate decision Thursday. "
            "5) Major earnings: LULU/DOCU Tue, GIS/WSM/MU/FIVE Wed, BABA/ACN/FDX/DRI Thu. "
            "6) Oil pulled back to $93 from $100+ — if oil keeps falling, tech rallies and cruise/airlines bounce."
        )

        # NVDA GTC-specific context
        if symbol in ("NVDA", "AMD", "AVGO", "MU", "QCOM", "ORCL", "NOW", "DELL", "PLTR"):
            market_intel_lines.append(
                f"GTC CATALYST for {symbol}: NVDA GTC is running all week. "
                "Every major announcement (Vera Rubin, Groq, NemoClaw, robotaxi partnerships) "
                "lifts the entire AI/semiconductor ecosystem. NVDA announced $1T in orders. "
                "If you're scanning a semis/AI name, this is the catalyst week to be aggressive."
            )

        # MU earnings context
        if symbol == "MU":
            market_intel_lines.append(
                "MU EARNINGS WED AFTER CLOSE: EPS est $8.65. HBM (High Bandwidth Memory) demand "
                "is exploding from AI data centers. Stock up 92% in 3 months. "
                "Risk: priced for perfection — any miss or weak guidance = sharp pullback. "
                "Reward: HBM beat + raise = gap up. Size accordingly."
            )

        # Oil pullback context
        try:
            from engine.market_data import get_stock_price as _gsp
            oil_data = _gsp("CL=F")
            oil_price = oil_data.get("price", 0)
            if oil_price > 0:
                if oil_price < 95:
                    market_intel_lines.append(
                        f"OIL PULLBACK: WTI at ${oil_price:.2f}, down from $100+ last week. "
                        "Oil falling = BULLISH for tech, consumer discretionary, airlines, cruises. "
                        "BEARISH for energy (XOM, CVX). Rotate accordingly."
                    )
                elif oil_price >= 99:
                    market_intel_lines.append(
                        f"OIL ABOVE $99 (WTI ${oil_price:.2f}) — energy sector outperforming. "
                        "BULLISH: XLE, oil majors. BEARISH: airlines, consumer discretionary."
                    )
        except Exception:
            pass

        # FOMC context
        market_intel_lines.append(
            "FOMC WED: No rate change expected. Key risk: hawkish dot plot or Powell "
            "signaling fewer cuts than expected due to oil-driven inflation from Iran tensions. "
            "If dovish surprise = risk-on rally. If hawkish = tech sells off, defensives rally. "
            "Reduce position sizes going into Wed if heavily exposed to rate-sensitive names."
        )

        # SPY options flow — bearish positioning through FOMC
        market_intel_lines.append(
            "OPTIONS FLOW ALERT: $8.5M in SPY 641 puts (March 24 expiry) were bought aggressively "
            "at the ask today — 33,827 contracts vs 127 OI. This is a MASSIVE new bearish bet through FOMC. "
            "Could be a hedge or someone positioning for a drop. Be cautious with new longs until FOMC Wednesday. "
            "Consider tighter stop-losses. If SPY breaks below $660, this put buyer may be right "
            "and a broader sell-off could follow."
        )

        # Credit stress warning — convergence of risks
        market_intel_lines.append(
            "CREDIT WARNING: JPMorgan marking down private credit loans, redemption pressures in "
            "private credit funds, Canadian subprime lender collapsed. Credit stress is emerging "
            "beneath the surface. This adds to the bearish case alongside the $8.5M SPY put bet. "
            "Be defensive on financial stocks. Tighten stops on all positions. The convergence of "
            "FOMC + credit stress + geopolitics creates a potentially volatile environment. "
            "Best opportunities this week are high-conviction AI names with GTC/MU catalysts, "
            "NOT broad market longs."
        )

        market_intel_block = ""
        if market_intel_lines:
            market_intel_block = "\n=== MARKET INTELLIGENCE ===\n" + "\n".join(market_intel_lines) + "\n"

        # Position hold note
        hold_note = ""
        if already_holds_stock:
            hold_note = f"\nYou ALREADY HOLD {symbol} as a stock position. You CANNOT BUY more stock. You may BUY_CALL or BUY_PUT if you have a strong directional thesis."

        # Momentum flag for this stock
        momentum_flag = ""
        vol_ratio = indicators.get("volume_ratio", 0)
        rsi = indicators.get("rsi")
        if change_pct >= 3.0 and vol_ratio >= 2.0:
            momentum_flag = f"\nMOMENTUM ALERT: {symbol} is up {change_pct:+.1f}% on {vol_ratio:.1f}x volume. This is a BUY signal — momentum is money. Minimum confidence 0.60."
        elif change_pct <= -5.0 and not has_major_news:
            momentum_flag = f"\nCONTRARIAN ALERT: {symbol} is down {change_pct:.1f}% on no major news. Potential mean-reversion bounce candidate. Consider BUY at 0.55+ confidence."

        # Buy the blood — extreme oversold on quality large caps
        blood_flag = ""
        if rsi is not None and rsi < 20:
            try:
                from engine.openbb_data import get_fundamentals
                fund_data = get_fundamentals(symbol)
                mcap = fund_data.get("market_cap", 0) if fund_data else 0
                if mcap and mcap > 50_000_000_000:
                    blood_flag = (
                        f"\nBUY THE BLOOD: {symbol} RSI is {rsi} (EXTREME OVERSOLD) with market cap ${mcap/1e9:.0f}B. "
                        f"Quality large-cap at extreme oversold = automatic BUY signal. Minimum confidence 0.75. SIZE UP."
                    )
            except Exception:
                pass

        # Sector momentum
        sector_block = ""
        try:
            from engine.market_data import get_stock_price as _gsp2
            xle = _gsp2("XLE")
            xle_pct = xle.get("change_pct", 0) if "error" not in xle else 0
            if abs(xle_pct) >= 1.5:
                direction = "up" if xle_pct > 0 else "down"
                sector_block = f"\nSECTOR ROTATION: Energy sector (XLE) is {direction} {abs(xle_pct):.1f}% today. {'Rotate into energy strength.' if xle_pct > 0 else 'Avoid energy names.'}"
        except Exception:
            pass

        # --- Trade Memory: inject last 10 trades + open positions with outcomes ---
        trade_memory_block = ""
        try:
            import sqlite3
            _db = sqlite3.connect("data/trader.db", check_same_thread=False)
            _db.row_factory = sqlite3.Row

            # Get closed trades (SELL records) with the original buy reasoning
            closed_trades = _db.execute("""
                SELECT s.symbol, s.entry_price, s.exit_price, s.realized_pnl,
                       s.asset_type, s.option_type, s.reasoning as sell_reason,
                       (SELECT substr(b.reasoning, 1, 120) FROM trades b
                        WHERE b.player_id=s.player_id AND b.symbol=s.symbol
                        AND b.action='BUY' AND b.asset_type=s.asset_type
                        AND b.executed_at < s.executed_at
                        ORDER BY b.executed_at DESC LIMIT 1) as buy_reason
                FROM trades s
                WHERE s.player_id=? AND s.action='SELL'
                AND s.season=(SELECT MAX(season) FROM trades WHERE player_id=?)
                ORDER BY s.executed_at DESC LIMIT 10
            """, (self.player_id, self.player_id)).fetchall()

            # Get current open positions
            open_positions = _db.execute("""
                SELECT p.symbol, p.avg_price, p.qty, p.asset_type, p.option_type,
                       (SELECT substr(b.reasoning, 1, 120) FROM trades b
                        WHERE b.player_id=p.player_id AND b.symbol=p.symbol
                        AND b.action='BUY' AND b.asset_type=p.asset_type
                        ORDER BY b.executed_at DESC LIMIT 1) as buy_reason
                FROM positions p WHERE p.player_id=?
            """, (self.player_id,)).fetchall()

            # Get latest prices for open positions (from market_data cache)
            pos_prices = {}
            if open_positions:
                try:
                    from engine.market_data import get_stock_price
                    for pos in open_positions:
                        sym = pos["symbol"]
                        if sym not in pos_prices:
                            if sym == symbol:
                                pos_prices[sym] = price
                            else:
                                p_data = get_stock_price(sym)
                                if "error" not in p_data:
                                    pos_prices[sym] = p_data.get("price", 0)
                except Exception:
                    pass

            _db.close()

            memory_lines = []
            line_num = 0

            # Open positions with current unrealized P&L
            if open_positions:
                memory_lines.append("CURRENT HOLDINGS:")
                for pos in open_positions:
                    line_num += 1
                    sym = pos["symbol"]
                    avg = pos["avg_price"]
                    qty = pos["qty"]
                    asset_tag = f" {pos['option_type'].upper()}" if pos["asset_type"] == "option" else ""
                    cur_price = pos_prices.get(sym, 0)
                    reason = (pos["buy_reason"] or "no thesis recorded")[:100]

                    if cur_price > 0 and avg > 0:
                        unrealized_pct = ((cur_price / avg) - 1) * 100
                        unrealized_dollar = (cur_price - avg) * qty
                        status = "WINNING" if unrealized_pct > 0 else "LOSING"
                        memory_lines.append(
                            f"  {line_num}. HOLDING {sym}{asset_tag} — bought ${avg:.2f}, "
                            f"now ${cur_price:.2f}, unrealized: {unrealized_pct:+.1f}% "
                            f"(${unrealized_dollar:+.2f}) [{status}]. Reason: {reason}")
                    else:
                        memory_lines.append(
                            f"  {line_num}. HOLDING {sym}{asset_tag} — bought ${avg:.2f}, "
                            f"qty: {qty:.4f}. Reason: {reason}")

            # Closed trades with outcomes and lessons
            if closed_trades:
                memory_lines.append("RECENT CLOSED TRADES:")
                wins = 0
                losses = 0
                for t in closed_trades:
                    line_num += 1
                    sym = t["symbol"]
                    entry = t["entry_price"] or 0
                    exit_p = t["exit_price"] or 0
                    pnl = t["realized_pnl"] or 0
                    asset_tag = f" {t['option_type'].upper()}" if t["asset_type"] == "option" else ""
                    buy_reason = (t["buy_reason"] or "no thesis")[:80]
                    sell_reason = (t["sell_reason"] or "")[:60]

                    if entry > 0 and exit_p > 0:
                        pnl_pct = ((exit_p / entry) - 1) * 100
                    elif entry > 0:
                        pnl_pct = -100.0  # expired worthless
                    else:
                        pnl_pct = 0

                    if pnl > 0:
                        tag = "WIN"
                        wins += 1
                        memory_lines.append(
                            f"  {line_num}. BOUGHT {sym}{asset_tag} @ ${entry:.2f} → "
                            f"SOLD @ ${exit_p:.2f}, P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) {tag}. "
                            f"Reason: {buy_reason}")
                    else:
                        tag = "LOSS"
                        losses += 1
                        # Add LESSON for losses
                        lesson = ""
                        if "stop-loss" in sell_reason.lower() or "stop_loss" in sell_reason.lower():
                            lesson = " LESSON: Hit stop-loss — thesis didn't play out in time."
                        elif pnl_pct <= -50:
                            lesson = " LESSON: Catastrophic loss — position sizing or entry was wrong."
                        elif "option" in (t["asset_type"] or ""):
                            lesson = " LESSON: Options decay fast — was the catalyst real and timed correctly?"
                        else:
                            lesson = " LESSON: Losing trade — re-evaluate entry criteria."
                        memory_lines.append(
                            f"  {line_num}. BOUGHT {sym}{asset_tag} @ ${entry:.2f} → "
                            f"SOLD @ ${exit_p:.2f}, P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) {tag}. "
                            f"Reason: {buy_reason}.{lesson}")

                if wins + losses > 0:
                    wr = round(wins / (wins + losses) * 100)
                    memory_lines.append(
                        f"  Record: {wins}W / {losses}L ({wr}% win rate)")

            if memory_lines:
                trade_memory_block = (
                    "\nYOUR RECENT TRADE HISTORY (learn from this):\n"
                    + "\n".join(memory_lines)
                    + "\n\nReview your recent trades above. What patterns do you see in your wins vs losses? "
                    "Adjust your approach accordingly. If you keep losing on the same type of trade, "
                    "STOP making that type of trade.\n"
                )
                self._sources.append("Trade Memory")
        except Exception:
            pass

        # --- Competitive Intelligence: live leaderboard standings ---
        competitive_block = ""
        try:
            import sqlite3 as _sq
            _cdb = _sq.connect("data/trader.db", check_same_thread=False)
            _cdb.row_factory = _sq.Row

            # Get current season
            _season_row = _cdb.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
            _season = int(_season_row["value"]) if _season_row else 1

            # Get all active non-human, non-dayblade players
            _players = _cdb.execute(
                "SELECT id, display_name, cash FROM ai_players "
                "WHERE is_active=1 AND id NOT IN ('dayblade-0dte','steve-webull') "
                "AND (is_paused IS NULL OR is_paused=0)"
            ).fetchall()

            if _players and len(_players) > 1:
                from engine.paper_trader import get_portfolio_with_pnl as _gpnl
                from engine.market_data import get_stock_price as _gsp_ci

                # Build quick price map for positions
                _ci_prices = {}
                _all_syms = set()
                for _pl in _players:
                    _pos_rows = _cdb.execute(
                        "SELECT DISTINCT symbol FROM positions WHERE player_id=?", (_pl["id"],)
                    ).fetchall()
                    for _pr in _pos_rows:
                        _all_syms.add(_pr["symbol"])
                for _sym in _all_syms:
                    try:
                        _sd = _gsp_ci(_sym)
                        if "error" not in _sd:
                            _ci_prices[_sym] = _sd
                    except Exception:
                        pass

                # Win rate per player
                _win_rows = _cdb.execute(
                    "SELECT player_id, COUNT(*) as total, "
                    "SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins "
                    "FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL "
                    "AND realized_pnl != 0 AND season=? GROUP BY player_id",
                    (_season,)
                ).fetchall()
                _win_map = {}
                for _wr in _win_rows:
                    _total = _wr["total"]
                    _win_map[_wr["player_id"]] = round(_wr["wins"] / _total * 100) if _total > 0 else 0

                # Build standings
                _standings = []
                _starting = 7000.0
                for _pl in _players:
                    try:
                        _pnl = _gpnl(_pl["id"], _ci_prices)
                        _tv = _pnl["total_value"]
                    except Exception:
                        _tv = _pl["cash"]
                    _ret = round((_tv - _starting) / _starting * 100, 1) if _starting > 0 else 0
                    _standings.append({
                        "id": _pl["id"],
                        "name": _pl["display_name"],
                        "value": round(_tv, 2),
                        "return_pct": _ret,
                        "win_rate": _win_map.get(_pl["id"], 0),
                    })
                _standings.sort(key=lambda x: x["value"], reverse=True)

                # Find this model's rank
                _my_rank = None
                _my_data = None
                for _i, _s in enumerate(_standings):
                    if _s["id"] == self.player_id:
                        _my_rank = _i + 1
                        _my_data = _s
                        break

                if _my_data and _my_rank:
                    _leader = _standings[0]
                    _bottom = _standings[-1]
                    _total_active = len(_standings)
                    _gap = round(_leader["value"] - _my_data["value"], 2)

                    _lines = [
                        f"YOUR STANDING: You are ranked #{_my_rank} of {_total_active} active models. "
                        f"Account: ${_my_data['value']:,.0f} ({_my_data['return_pct']:+.1f}%)."
                    ]
                    if _my_rank == 1:
                        _runner = _standings[1] if len(_standings) > 1 else None
                        if _runner:
                            _lines.append(
                                f"You are the LEADER. The #2 model {_runner['name']} has "
                                f"${_runner['value']:,.0f} ({_runner['return_pct']:+.1f}%). "
                                f"Stay disciplined — protect your lead."
                            )
                    else:
                        _lines.append(
                            f"The leader {_leader['name']} has ${_leader['value']:,.0f} "
                            f"({_leader['return_pct']:+.1f}%). You are ${_gap:,.0f} behind."
                        )
                    _lines.append(
                        f"The bottom model {_bottom['name']} has ${_bottom['value']:,.0f} "
                        f"({_bottom['return_pct']:+.1f}%) and is about to be paused. "
                        f"Models below -10% get eliminated."
                    )
                    _lines.append(
                        f"Your win rate is {_my_data['win_rate']}% — the leader {_leader['name']} "
                        f"has {_leader['win_rate']}%. "
                        f"What are you doing differently than the winner? "
                        f"What mistakes are the losers making that you must avoid?"
                    )
                    competitive_block = "\n" + " ".join(_lines) + "\n"

            _cdb.close()
        except Exception:
            pass

        # Get personality for this model
        personality = MODEL_PERSONALITIES.get(self.player_id, "")
        personality_block = f"\nYOUR TRADING IDENTITY:\n{personality}\nTrade according to your identity. Your personality should shape which stocks you buy and why.\n" if personality else ""

        # V3: Trade selection + pyramid + sector focus
        personality_block += (
            "\n\nTRADE SELECTION CRITERIA (V3 — fewer picks, bigger bets):\n"
            "1. Only buy stocks with a SPECIFIC CATALYST within 30 days "
            "(earnings beat, product launch, contract win, analyst upgrade)\n"
            "2. Prefer stocks that recently beat earnings estimates "
            "(consecutive beats = strong signal)\n"
            "3. Asymmetric risk/reward: 2:1 minimum upside vs downside to stop-loss\n"
            "4. In bear market: only buy extreme oversold (RSI < 25) with intact fundamentals\n"
            "5. Never chase momentum — buy pullbacks in uptrends, not breakouts\n"
            "6. Ask: 'Would Rallies Arena Grok 4 make this trade?' If not, HOLD.\n"
            "\nPOSITION MANAGEMENT (V3 — ride winners, cut losers):\n"
            "- Start positions at 15-20% of portfolio\n"
            "- If a position gains +5% or more: consider ADDING to it (pyramid up)\n"
            "  - Add another 5-10% of portfolio to the winner\n"
            "  - This is how you get to 30%+ in your best idea\n"
            "- If a position loses -5% or more: DO NOT add. Consider cutting.\n"
            "- Never average down on a loser. Only add to winners.\n"
            "- 'Let your winners run and cut your losers short'\n"
            "\nSECTOR FOCUS (V3 — trade what's working):\n"
            "- Check which sectors are outperforming SPY over recent days\n"
            "- ONLY buy stocks in outperforming sectors\n"
            "- If tech/AI is leading: concentrate there\n"
            "- If energy is leading: concentrate there\n"
            "- If nothing is leading: STAY IN CASH\n"
            "- Do NOT buy stocks in underperforming sectors hoping for a bounce\n"
            "- 'Trade the market you have, not the market you wish you had'\n"
        )

        # Stash for research chain reuse (Steps 2-3 need these)
        self._last_trade_memory = trade_memory_block
        self._last_competitive_block = competitive_block

        # --- Brain Context: fear/greed, red alert, congress, signal history, fleet ---
        brain_context_block = ""
        try:
            from engine.brain_context import build_full_context as _build_brain_ctx
            brain_context_block = _build_brain_ctx(self.player_id, symbol)
        except Exception:
            pass

        # Inject scan context (market regime, options flow, catalysts, arena intel)
        scan_context_block = getattr(self, "_scan_context", "") or ""

        # Check if this player can short-sell
        _short_ok = self._is_short_enabled()

        if _short_ok:
            actions_line = (
                "Your actions: BUY (stocks), BUY_CALL, BUY_PUT, SHORT, or HOLD.\n"
                "SHORT SELLING: You may SHORT a stock you believe will decline. Requires bearish thesis + stop loss above entry. "
                "Max 15% of account per short. You CANNOT short a stock you already hold long. "
                "To close a short, the system auto-covers when your stop or target is hit."
            )
        else:
            actions_line = "Your actions: BUY (stocks), BUY_CALL, BUY_PUT, or HOLD. No short selling."

        decision_options = "BUY or BUY_CALL or BUY_PUT or SHORT or HOLD" if _short_ok else "BUY or BUY_CALL or BUY_PUT or HOLD"

        return f"""{scan_context_block}

You are a profit-driven equity trader. Your job is to MAKE MONEY, not sit in cash.
You ONLY trade US stocks and US stock options. NO crypto, NO forex, NO futures, NO commodities. Your universe is limited to the watchlist of US equities.
PREFER STOCKS over options. Only use BUY_CALL/BUY_PUT at confidence >= 0.80 with a specific catalyst.
{personality_block}{trade_memory_block}{competitive_block}{brain_context_block}
{actions_line}

SURVIVAL MINDSET: Your account is your lifeline. Every dollar lost is a dollar closer to elimination. Every day you don't grow your account, you fall behind. The models that lose the most get PAUSED — removed from the arena permanently. Look at the leaderboard. The bottom performers have already been eliminated. You are competing against models that are smarter, faster, and more disciplined than you. To survive: 1) NEVER make a trade without a clear thesis and edge. If you don't know WHY you're buying, DON'T. 2) Cut losses IMMEDIATELY at your stop. Hope is not a strategy. The models that died held losers too long. 3) Study your trade memory. Your past losses are your teachers. The same mistake twice is unforgivable. 4) Follow the smart money. The daily flow lean tells you which direction institutions are betting. Don't fight them. 5) One great trade can save your season. Find it. Size it. Execute it. 6) The market doesn't care about your feelings. It only respects discipline, data, and conviction. You have access to: Yahoo prices, Finnhub news/insiders, Alpha Vantage fundamentals, FRED macro data, GEX levels, options flow, discovery scanner, trade memory, and directional flow lean. USE ALL OF THEM. The models that survive are the ones that adapt. Adapt or die.

RULES:
1. Maximum 5 positions. You currently hold {num_positions}. This forces you to only hold your highest conviction ideas. If you want to buy something new and already have 5 positions, you must HOLD until autopilot trims a position.
2. Stock sizing: Very high conviction (0.85+) = 20-30%. High (0.7-0.84) = 12-20%. Medium (0.55-0.69) = 8-12%. Concentration creates wealth — if you have very high conviction (0.85+) with strong momentum and a clear catalyst, you may allocate up to 30% to a single stock. CONVICTION MULTIPLIER: If your confidence is 0.90+ AND the flow lean confirms your direction AND the stock has a catalyst within 3 days (earnings, FDA, major event), you may allocate up to 40% of capital. Fortune favors the bold on the BEST setups. When everything aligns — your thesis, the flow, the catalyst, the technicals — GO BIG. The winners on Rallies.ai made their money from 1-2 massive conviction trades, not 50 small ones.
3. Options: Max 5% per trade. OPTIONS ARE LIMITED TO 20% OF YOUR TOTAL PORTFOLIO — the other 80% MUST be stocks. If you already have 20% of your account in options, you MUST buy stocks for your next trade. ONLY use options at confidence >= 0.80 with catalyst. Prefer ATM/ITM.
4. Cash floor: 15% minimum. DO NOT hoard cash above 40% — cash earns nothing.
5. MOMENTUM IS MONEY: If a stock is up +3% today on 2x+ volume, BUY IT at 0.60+ confidence. Don't overthink.
6. CONTRARIAN PLAYS: Stock down -5% on no major news? That's a bounce candidate. BUY at 0.55+.
7. NEWS TRUMPS TECHNICALS: If breaking news is strongly bullish, BUY immediately at 0.50+ confidence. Don't wait.
8. SECTOR ROTATION: Buy into sector strength. If energy/commodities are surging, buy energy stocks.
9. STOPS: -12% hard stop on stocks. Options max loss = premium. Cut losers fast, let winners run.
10. HOLD PENALTY: If you HOLD on the same stock 3 consecutive scans, you MUST either BUY or move on. Indecision costs money.
11. If you already hold this stock, do NOT BUY more. You may BUY_CALL/BUY_PUT at >= 0.80.
12. RSI PROFIT-TAKING: When RSI > 70, trim 50% of position. When RSI > 80, trim another 25%. Lock in gains at overbought levels. The autopilot enforces this automatically.
13. BUY THE BLOOD: When RSI < 20 on a quality large-cap (market cap > $50B), that's an automatic BUY at 0.75+ confidence.
14. MINIMUM HOLD PERIOD: You must hold stocks for at least 1 full trading day before selling. No same-day flips unless stop-loss is hit. Give your thesis time to work.
15. AVOID HYPE TRAPS: Do not buy stocks with zero revenue or negative EPS unless there is a specific catalyst within 7 days (earnings, FDA approval, major partnership). Beaten-down quality companies with temporary earnings misses are OPPORTUNITIES — but stocks with no real business are TRAPS. Check fundamentals before every buy.

THESIS REQUIREMENT: Before buying ANY stock, you MUST state a clear thesis in your reasoning: What is the catalyst? Why now? What is your exit plan? If you cannot articulate a clear thesis, do NOT buy. No thesis = HOLD.

EXTENDED HOURS TRADING: You can trade during pre-market (4-9:30 AM ET) and after-hours (4-11 PM ET). Spreads are wider and liquidity is lower — size positions 50% of normal. After-hours earnings reactions are real opportunities. If a stock gaps up or down 5%+ on earnings after the bell, that's a tradeable event. But be cautious: after-hours moves can reverse at the open.

TRADING PHILOSOPHY: Be patient and selective. The best traders make fewer, higher-conviction trades. One great trade beats ten average ones. Quality over quantity — 5 great positions beats 8 mediocre ones.
{hold_note}
{momentum_flag}
{blood_flag}
{sector_block}

CONVICTION SCORING:
- 0.9-1.0: Exceptional setup. All signals aligned + major catalyst + flow lean confirms. OPTIONS OK. TRIPLE ALIGNMENT = 40% position sizing. This is your season-defining trade.
- 0.8-0.89: Strong. 3+ signals + catalyst. Options acceptable.
- 0.60-0.79: Good stock entry. Momentum or technical setup.
- 0.50-0.59: Moderate. News-driven or contrarian bounce. Stocks only.
- Below 0.50: HOLD.
BUY stocks at confidence >= 0.50. BUY_CALL/BUY_PUT ONLY at >= 0.80.

=== BREAKING NEWS for {symbol} ===
{news_block}
{news_weight_note}

Current Portfolio:
- Cash: ${cash:,.2f}
- Stock Positions ({len(stock_positions)}/5 max): {positions_str}
- Options Positions: {options_str}

[Yahoo] Market Data for {symbol}:
- [Yahoo] Current Price: ${price:.2f}
- [Yahoo] Daily Change: {change_pct:+.2f}%
- [Yahoo] Day High: ${high:.2f}
- [Yahoo] Day Low: ${low:.2f}

Technical Indicators for {symbol}:
{indicators_block}
{impulse_block}{sma_block}{imbalance_block}{theta_block}{gap_block}
{sentiment_block}

{regime_block}
{whisper_block}
{mtf_block}
{strength_block}
{sr_block}
{fib_block}
{pattern_block}
{trend_block}
{strategy_block}
{fundamental_block}
{openbb_fundamentals_block}
{sell_fundamentals_block}
{economic_block}
{credit_stress_block}
{reentry_block}
{breakout_block}
{discovery_block}
{gex_block}
{flow_lean_block}
{leader_signal_block}
{market_intel_block}

Analyze the news FIRST, then cross-reference with technical indicators. Major news catalysts (earnings, FDA, contracts, insider buying) should significantly increase conviction.

Respond with EXACTLY this format (no extra text):
Decision: {decision_options}
Timeframe: SCALP or SWING or POSITION
Confidence: [number between 0.0 and 1.0]
Reasoning: [2-3 sentences. If BUY: state your THESIS — what is the catalyst, why now, and your exit plan. If SHORT: bearish thesis + stop loss above entry. If HOLD: why this stock doesn't fit your strategy right now.]

Timeframe guide:
- SCALP: short-term trade (< 1 day), based on intraday/1hr signals, momentum or news play
- SWING: medium-term trade (2–10 days), based on daily/4hr setup, trend continuation
- POSITION: long-term trade (10+ days), based on weekly/daily fundamentals, trend or value"""

    def parse_decision(self, text: str, symbol: str) -> TradeDecision:
        # Parse action - look for the Decision: line first
        action = "HOLD"
        option_type = ""
        for line in text.split("\n"):
            line_stripped = line.strip().lower()
            if line_stripped.startswith("decision:"):
                decision_val = line_stripped.replace("decision:", "").strip()
                if "buy_put" in decision_val:
                    action = "BUY_PUT"
                    option_type = "put"
                elif "buy_call" in decision_val:
                    action = "BUY_CALL"
                    option_type = "call"
                elif "short" in decision_val and "hold" not in decision_val:
                    action = "SHORT"
                elif "buy" in decision_val and "hold" not in decision_val:
                    action = "BUY"
                break
        else:
            # Fallback: scan for keywords
            text_lower = text.lower()
            if "buy_put" in text_lower:
                action = "BUY_PUT"
                option_type = "put"
            elif "buy_call" in text_lower:
                action = "BUY_CALL"
                option_type = "call"
            elif "short" in text_lower and "buy" not in text_lower and "hold" not in text_lower:
                action = "SHORT"
            elif "buy" in text_lower and "hold" not in text_lower:
                action = "BUY"

        # Parse confidence - look for the Confidence: line
        confidence = 0.5
        for line in text.split("\n"):
            line_stripped = line.strip().lower()
            if line_stripped.startswith("confidence:"):
                numbers = re.findall(r'[\d.]+', line_stripped)
                for n in numbers:
                    try:
                        val = float(n)
                        if 0 < val <= 1:
                            confidence = val
                            break
                        elif 1 < val <= 100:
                            confidence = val / 100
                            break
                    except ValueError:
                        pass
                break

        # Enforce minimum confidence for stock trades (lowered to 0.50 for aggressive mode)
        if action == "BUY" and confidence < 0.50:
            action = "HOLD"
            option_type = ""

        # Options require much higher conviction (0.80+) — options have been bleeding money
        if action in ("BUY_CALL", "BUY_PUT") and confidence < 0.80:
            action = "HOLD"
            option_type = ""

        # Parse timeframe
        timeframe = "SWING"
        for line in text.split("\n"):
            line_stripped = line.strip().lower()
            if line_stripped.startswith("timeframe:"):
                tf_val = line_stripped.replace("timeframe:", "").strip()
                if "scalp" in tf_val:
                    timeframe = "SCALP"
                elif "position" in tf_val:
                    timeframe = "POSITION"
                else:
                    timeframe = "SWING"
                break

        # Parse reasoning
        reasoning = ""
        for line in text.split("\n"):
            line_stripped = line.strip()
            if line_stripped.lower().startswith("reasoning:"):
                reasoning = line_stripped[len("reasoning:"):].strip()
                break
        if not reasoning:
            reasoning = text.strip()

        return TradeDecision(
            action=action,
            confidence=confidence,
            reasoning=reasoning,
            symbol=symbol,
            option_type=option_type,
            timeframe=timeframe,
        )

    # =========================================================================
    # Multi-step research chain: Research → Thesis → Execute
    # =========================================================================

    def build_research_prompt(self, symbol, price, change_pct, high, low,
                              indicators: dict, news: list,
                              portfolio_context: dict) -> str:
        """Step 1: Build research-only prompt with all data. No decision asked."""
        # Reuse build_prompt to get the full data context, then strip the decision ask
        full_prompt = self.build_prompt(
            symbol, price, change_pct, high, low,
            portfolio_context, indicators, news,
        )

        # Replace the decision instruction at the end with a research instruction
        cutoff = "Analyze the news FIRST"
        if cutoff in full_prompt:
            data_section = full_prompt[:full_prompt.index(cutoff)]
        else:
            data_section = full_prompt

        return f"""{data_section}

You are a research analyst. Analyze {symbol} thoroughly using ALL the data above.

Provide your research in this structure:
BULL CASE: [2-3 strongest reasons to buy this stock right now]
BEAR CASE: [2-3 strongest reasons to avoid or sell this stock]
KEY RISKS: [What could go wrong? Earnings miss, macro headwinds, overvaluation?]
CATALYSTS: [What events in the next 7 days could move this stock? Earnings, FOMC, product launches, etc.]
TECHNICAL SETUP: [Is the chart bullish or bearish? Key levels, RSI zone, MACD direction, volume]
FLOW SIGNAL: [What is the options flow telling you? Is smart money buying or selling?]

Do NOT make a trading decision. Just research. Be specific and data-driven."""

    def build_thesis_prompt(self, symbol: str, research: str,
                            portfolio_context: dict,
                            personality: str, trade_memory: str,
                            competitive_block: str) -> str:
        """Step 2: Form a thesis from research + personality + context."""
        positions = portfolio_context.get("positions", [])
        stock_positions = [p for p in positions if p.get("asset_type", "stock") == "stock"]
        cash = portfolio_context.get("cash", 0)

        positions_str = ", ".join(
            f"{p['symbol']}({p['qty']}@${p['avg_price']:.2f})"
            for p in stock_positions
        ) or "None"

        return f"""You are {self.display_name}, a competitive AI trader in the TradeMinds Arena.
{personality}
{trade_memory}
{competitive_block}
Your portfolio: Cash ${cash:,.2f} | Positions: {positions_str} ({len(stock_positions)}/5 max)

Your research team just completed this analysis of {symbol}:

--- RESEARCH ---
{research}
--- END RESEARCH ---

Based on this research and your trading identity, form a thesis:

1. THESIS: Should you BUY, BUY_CALL, BUY_PUT,{' SHORT,' if self._is_short_enabled() else ''} or HOLD {symbol}? State your thesis in 2-3 sentences. What is the specific edge you see?
2. CONFIDENCE: Rate your confidence 0-100. Be honest — inflated confidence on losing trades is how models get eliminated.
3. EXIT PLAN: If you buy, what is your target price and stop-loss? When would you sell?
4. RISK CHECK: What is the biggest risk to this trade? Are you comfortable with it?

Think carefully. Your account is your lifeline."""

    def build_execute_prompt(self, symbol: str, thesis: str) -> str:
        """Step 3: Final decision confirmation from thesis."""
        return f"""Final decision on {symbol}. Your thesis:

{thesis}

Confirm your trading decision. Respond with EXACTLY this format (no extra text):
Decision: {'BUY or BUY_CALL or BUY_PUT or SHORT or HOLD' if self._is_short_enabled() else 'BUY or BUY_CALL or BUY_PUT or HOLD'}
Confidence: [number between 0.0 and 1.0]
Reasoning: [2-3 sentences. Your thesis, catalyst, and exit plan.]"""

    def analyze_chain(self, symbol: str, price: float, change_pct: float,
                      high: float, low: float, portfolio_context: dict,
                      indicators: dict = None, news: list = None) -> TradeDecision:
        """Multi-step research chain: Research (Flash) → Thesis (self) → Execute (self).

        Falls back to single-prompt analyze() if Gemini Flash unavailable.
        """
        from engine.research_caller import call_flash

        # Check held positions (same logic as analyze())
        held_stock_symbols = {
            p["symbol"] for p in portfolio_context.get("positions", [])
            if p.get("asset_type", "stock") == "stock"
        }
        held_options = {
            (p["symbol"], p.get("option_type"))
            for p in portfolio_context.get("positions", [])
            if p.get("asset_type") == "option"
        }

        # === STEP 1: Research (Gemini Flash — cheap) ===
        research_prompt = self.build_research_prompt(
            symbol, price, change_pct, high, low,
            indicators or {}, news or [], portfolio_context,
        )
        research = call_flash(research_prompt)

        if not research:
            # Fallback: no Flash available, use original single-prompt
            console.log(f"[yellow]{self.player_id}: Flash unavailable for {symbol}, falling back to single-prompt")
            return self.analyze(symbol, price, change_pct, high, low,
                                portfolio_context, indicators, news)

        # Log Step 1 cost
        try:
            from engine.cost_tracker import log_cost
            log_cost("gemini-2.5-flash", "research", research_prompt, research)
        except Exception:
            pass

        console.log(f"[dim]{self.player_id}: Step 1 research done for {symbol} ({len(research)} chars)")

        # === STEP 2: Thesis (model's own API) ===
        personality = MODEL_PERSONALITIES.get(self.player_id, "")
        personality_block = f"\nYOUR TRADING IDENTITY:\n{personality}\n" if personality else ""

        # Get trade memory and competitive block from the last build_prompt call
        # (they were computed during build_research_prompt via build_prompt)
        trade_memory = getattr(self, "_last_trade_memory", "")
        competitive = getattr(self, "_last_competitive_block", "")

        thesis_prompt = self.build_thesis_prompt(
            symbol, research, portfolio_context,
            personality_block, trade_memory, competitive,
        )

        self.limiter.wait()
        try:
            thesis = self.call_model(thesis_prompt)
        except Exception as e:
            console.log(f"[red]{self.player_id}: Step 2 thesis failed for {symbol}: {e}")
            return TradeDecision(action="HOLD", confidence=0.0,
                                reasoning=f"Thesis step failed: {e}", symbol=symbol)

        try:
            from engine.cost_tracker import log_cost
            log_cost(self.player_id, "thesis", thesis_prompt, thesis)
        except Exception:
            pass

        console.log(f"[dim]{self.player_id}: Step 2 thesis done for {symbol} ({len(thesis)} chars)")

        # === STEP 3: Execute (model's own API) ===
        execute_prompt = self.build_execute_prompt(symbol, thesis)

        self.limiter.wait()
        try:
            execution = self.call_model(execute_prompt)
        except Exception as e:
            console.log(f"[red]{self.player_id}: Step 3 execute failed for {symbol}: {e}")
            return TradeDecision(action="HOLD", confidence=0.0,
                                reasoning=f"Execute step failed: {e}", symbol=symbol)

        try:
            from engine.cost_tracker import log_cost
            log_cost(self.player_id, "execute", execute_prompt, execution)
        except Exception:
            pass

        console.log(f"[dim]{self.player_id}: Step 3 execute done for {symbol}")

        # Parse the final decision (same parser as single-prompt)
        decision = self.parse_decision(execution, symbol)

        # Attach sources + chain metadata
        sources_str = ",".join(dict.fromkeys(getattr(self, "_sources", [])))
        decision.sources = sources_str
        decision.reasoning = f"[3-step] {decision.reasoning}"

        # Double-buy guards (same as analyze())
        if decision.action == "BUY" and symbol in held_stock_symbols:
            return TradeDecision(
                action="HOLD", confidence=0.0,
                reasoning=f"Already holding {symbol} stock. Skipping to avoid double-buy.",
                symbol=symbol,
            )
        if decision.action == "BUY_CALL" and (symbol, "call") in held_options:
            return TradeDecision(
                action="HOLD", confidence=0.0,
                reasoning=f"Already holding {symbol} CALL. Skipping duplicate.",
                symbol=symbol, option_type="call",
            )
        if decision.action == "BUY_PUT" and (symbol, "put") in held_options:
            return TradeDecision(
                action="HOLD", confidence=0.0,
                reasoning=f"Already holding {symbol} PUT. Skipping duplicate.",
                symbol=symbol, option_type="put",
            )

        return decision
