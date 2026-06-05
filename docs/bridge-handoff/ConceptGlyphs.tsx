/**
 * ConceptGlyphs.tsx
 * OllieTrades Bridge — Tier 2 concept glyphs.
 *
 * 16 line-art SVG icons, 24×24 source viewBox, render at 16 px in
 * walk-back narration and on Concept Cards. All strokes use
 * `currentColor` — inherit the surrounding text color so glyphs sit
 * inline with paragraph text without fighting the palette.
 *
 * Pairing convention: every concept_id referenced in a trade signal
 * payload (see section 6 of the design handoff) maps 1:1 to a glyph
 * here, keyed in the `conceptGlyphs` registry at the bottom of the
 * file.
 *
 * Accessibility:
 *   • Each <svg> renders an inline <title> with the concept's display
 *     name. Override via the `title` prop, or pass `aria-hidden` when
 *     the parent (e.g. a Concept Card heading) already provides the
 *     label.
 *   • role="img" is set on the svg.
 *
 * Cardinal rule (state has three channels — color + shape + label):
 *   These glyphs are SHAPE only. Never use a glyph alone to convey
 *   trade state (use a <StateIndicator /> for that). Glyphs identify
 *   which concept is being discussed; state shapes (▲ ▼ ◆ ● ✦)
 *   carry stance.
 */

import React from 'react';

export interface IconProps extends Omit<React.SVGProps<SVGSVGElement>, 'size'> {
  /** Pixel size. The same value is applied to both width and height. */
  size?: number | string;
  /** Accessible name. Defaults to the concept's display name. */
  title?: string;
}

const SVG_DEFAULTS = {
  viewBox: '0 0 24 24',
  fill: 'none' as const,
  stroke: 'currentColor',
  strokeWidth: 1.6,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  role: 'img' as const,
  xmlns: 'http://www.w3.org/2000/svg',
};

/* ============================================================
 *  OPTION STRUCTURES (5)
 * ============================================================ */

export const BearCallSpreadGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Bear Call Spread',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Ceiling — the short-call strike */}
    <path d="M3 5 H21" />
    {/* Descending staircase */}
    <path d="M4 9 H9 V13 H14 V17 H20" />
  </svg>
);

export const BullPutSpreadGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Bull Put Spread',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Ascending staircase */}
    <path d="M4 17 H9 V13 H14 V9 H20" />
    {/* Floor — the short-put strike */}
    <path d="M3 21 H21" />
  </svg>
);

export const IronCondorGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Iron Condor',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Profit band in the middle */}
    <path d="M9 10 H15 V14 H9 Z" />
    {/* Left arrow pointing right (into band) */}
    <path d="M3 12 H8" />
    <path d="M5 10 L8 12 L5 14" />
    {/* Right arrow pointing left (into band) */}
    <path d="M21 12 H16" />
    <path d="M19 10 L16 12 L19 14" />
  </svg>
);

export const CoveredCallGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Covered Call',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Strike ceiling */}
    <path d="M3 7 H21" />
    {/* Stock position circle */}
    <circle cx="12" cy="16" r="4" />
    {/* Cap connector */}
    <path d="M12 7 V12" />
  </svg>
);

export const CashSecuredPutGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Cash-Secured Put',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Down arrow above */}
    <path d="M12 4 V14" />
    <path d="M8 11 L12 15 L16 11" />
    {/* Cash floor */}
    <path d="M3 19 H21" />
  </svg>
);

/* ============================================================
 *  INDICATORS (5)
 * ============================================================ */

export const RsiGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'RSI',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Gauge arc — semicircle */}
    <path d="M4 18 A8 8 0 0 1 20 18" />
    {/* Needle pointing into overbought zone */}
    <path d="M12 18 L17 9" />
    {/* Pivot */}
    <circle cx="12" cy="18" r="1.4" fill="currentColor" />
    {/* Edge ticks */}
    <path d="M4 18 L3 19" />
    <path d="M20 18 L21 19" />
  </svg>
);

export const RelativeVolumeGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Relative Volume',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Average-volume bar (short) */}
    <path d="M5 21 V14 H9 V21" />
    {/* Current-volume bar (tall) */}
    <path d="M14 21 V5 H18 V21" />
    {/* Baseline */}
    <path d="M3 21 H21" />
  </svg>
);

export const AtrGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'ATR',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Range stem */}
    <path d="M12 4 V20" />
    {/* High cap */}
    <path d="M8 4 H16" />
    {/* Low cap */}
    <path d="M8 20 H16" />
    {/* Last tick */}
    <path d="M9 13 H15" />
  </svg>
);

export const EmaRibbonGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'EMA Ribbon',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Three flowing parallel waves */}
    <path d="M3 8 Q8 5 12 8 T21 8" />
    <path d="M3 12 Q8 9 12 12 T21 12" />
    <path d="M3 16 Q8 13 12 16 T21 16" />
  </svg>
);

export const IvRankGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'IV Rank',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Thermometer stem */}
    <path d="M10 6 Q10 4 12 4 Q14 4 14 6 V15 H10 Z" />
    {/* Bulb */}
    <circle cx="12" cy="18" r="3" />
    {/* Tick marks on stem */}
    <path d="M14 7 H16" />
    <path d="M14 10 H15" />
    <path d="M14 13 H16" />
  </svg>
);

/* ============================================================
 *  REGIME & STRUCTURE (1) — gamma flip is its own family
 * ============================================================ */

export const GammaFlipGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Gamma Flip',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Pivot bar */}
    <path d="M3 12 H21" />
    {/* Up arrow (positive-gamma side) */}
    <path d="M7 12 V5" />
    <path d="M4 8 L7 5 L10 8" />
    {/* Down arrow (negative-gamma side) */}
    <path d="M17 12 V19" />
    <path d="M14 16 L17 19 L20 16" />
  </svg>
);

/* ============================================================
 *  EXIT RULES (3)
 * ============================================================ */

export const TimeStopGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Time Stop',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Clock face */}
    <circle cx="12" cy="12" r="8" />
    {/* 12 o'clock mark */}
    <path d="M12 4 V6" />
    {/* Hour hand (up) */}
    <path d="M12 12 V8" />
    {/* Minute hand (right) */}
    <path d="M12 12 L16 12" />
    {/* Center pivot */}
    <circle cx="12" cy="12" r="1" fill="currentColor" />
  </svg>
);

export const StopLossGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Stop Loss',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Trend line */}
    <path d="M3 13 H21" />
    {/* X — the stop break */}
    <path d="M10 10 L14 16" />
    <path d="M14 10 L10 16" />
  </svg>
);

export const TakeProfitGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Take Profit',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Upward trend line */}
    <path d="M3 19 L13 9" />
    {/* Target outer ring */}
    <circle cx="17" cy="7" r="4" />
    {/* Bullseye dot */}
    <circle cx="17" cy="7" r="1.2" fill="currentColor" />
  </svg>
);

/* ============================================================
 *  SIGNAL / MOMENTUM (2)
 * ============================================================ */

export const ConvergenceGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'Convergence',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Solid lines — primary signals */}
    <line x1="3" y1="3" x2="12" y2="12" strokeWidth={1.8} />
    <line x1="21" y1="3" x2="12" y2="12" strokeWidth={1.8} />
    {/* Dashed lines — secondary signals */}
    <line x1="3" y1="21" x2="12" y2="12" strokeWidth={1.4} strokeDasharray="2.5 2" />
    <line x1="21" y1="21" x2="12" y2="12" strokeWidth={1.4} strokeDasharray="2.5 2" />
    {/* Meeting point */}
    <circle cx="12" cy="12" r="2.5" fill="currentColor" />
  </svg>
);

export const MacdGlyph: React.FC<IconProps> = ({
  size = 16,
  title = 'MACD',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Origin dot — shared start of both EMAs */}
    <circle cx="3" cy="16" r="1.5" fill="currentColor" />
    {/* Fast EMA — aggressive curve */}
    <path d="M3 16 Q8 14 13 10 Q17 7 21 5" strokeWidth={1.8} />
    {/* Slow EMA — shallower, dashed */}
    <path d="M3 16 Q8 15 13 13 Q17 11 21 10" strokeWidth={1.4} strokeDasharray="2.5 2" />
    {/* Divergence gap indicator at right edge */}
    <line x1="21" y1="5" x2="21" y2="10" strokeWidth={1} opacity={0.5} />
  </svg>
);

/* ============================================================
 *  Registry + types
 * ============================================================ */

export const conceptGlyphs = {
  bear_call_spread: BearCallSpreadGlyph,
  bull_put_spread: BullPutSpreadGlyph,
  iron_condor: IronCondorGlyph,
  covered_call: CoveredCallGlyph,
  cash_secured_put: CashSecuredPutGlyph,
  rsi: RsiGlyph,
  relative_volume: RelativeVolumeGlyph,
  convergence: ConvergenceGlyph,
  macd: MacdGlyph,
  atr: AtrGlyph,
  ema_ribbon: EmaRibbonGlyph,
  iv_rank: IvRankGlyph,
  gamma_flip: GammaFlipGlyph,
  time_stop: TimeStopGlyph,
  stop_loss: StopLossGlyph,
  take_profit: TakeProfitGlyph,
} as const;

export type ConceptId = keyof typeof conceptGlyphs;

/**
 * Render any concept glyph by id. Useful when the concept is resolved
 * from a signal payload (e.g. `trigger.concept_id` or one of the
 * entries in `signal.concepts_referenced`).
 *
 *   <ConceptGlyphById id={trigger.concept_id} size={16} />
 */
export const ConceptGlyphById: React.FC<
  IconProps & { id: ConceptId }
> = ({ id, ...props }) => {
  const Component = conceptGlyphs[id];
  if (!Component) return null;
  return <Component {...props} />;
};
