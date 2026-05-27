/**
 * CharacterIcons.tsx
 * OllieTrades Bridge — Tier 1 character portraits (emoji-style).
 *
 * 12 filled SVG portraits, 64×64 source viewBox, render at 24 / 32 / 48 px.
 * Each portrait is a flat illustrated avatar — round head, simple features,
 * solid uniform — designed to evoke the OllieTrades crew personas while
 * remaining original characters in their own right. The text label that
 * accompanies the chip ("Worf · Bear Spreads", etc.) carries the persona
 * naming convention; the avatar carries the personality.
 *
 * Style notes:
 *   • Filled multi-color (skin / hair / uniform), not line-art
 *   • Uniform colors map to the existing --char-* CSS variables from
 *     section 7 of the design handoff, so the established palette flows
 *     through unchanged.
 *   • Outline strokes still use `currentColor` so dark / light themes and
 *     the high-contrast display mode continue to work.
 *   • Below ~20px, recommend the consumer (CharacterChip) degrade to a
 *     colored circle + initial — facial detail will blur.
 *
 * Theme rotation:
 *   When a non-Trek theme pack is active (Wars / MASH / Dallas / none),
 *   the parent looks up `strategy_id → persona` and may render a sibling
 *   icon set. These components stay locked to the default Trek-named
 *   roster.
 *
 * Accessibility:
 *   • Every <svg> renders an inline <title> with the character's display
 *     name. Override via the `title` prop, or pass `aria-hidden` when
 *     the parent already provides a label.
 *   • role="img" is set on the svg.
 *   • Color is never the sole disambiguator — the chip's text label is
 *     mandatory per the section-7 cardinal rule.
 */

import React from 'react';

export interface IconProps extends Omit<React.SVGProps<SVGSVGElement>, 'size'> {
  /** Pixel size. The same value is applied to both width and height. */
  size?: number | string;
  /** Accessible name. Defaults to the character's display name. */
  title?: string;
}

/* Shared svg defaults. */
const SVG_DEFAULTS = {
  viewBox: '0 0 64 64',
  role: 'img' as const,
  xmlns: 'http://www.w3.org/2000/svg',
};

/* ============================================================
 *  Color palette — kept here so adjustments propagate across
 *  the cast in one place. Skin / hair tones are inline (not
 *  theme variables); uniform colors use --char-* CSS vars from
 *  the design system.
 * ============================================================ */
const SKIN = {
  light: '#F5D0A8',
  midLight: '#E8B98A',
  cool: '#E4C9A8',
  golden: '#E8DCB8',
  tan: '#C99670',
  brown: '#8A5A3B',
  darkBrown: '#5C3B22',
  richBrown: '#6B3F23',
} as const;

const HAIR = {
  brown: '#5C3A1E',
  darkBrown: '#2F1E10',
  black: '#1A1410',
  gray: '#9C9C9C',
  auburn: '#7A3A1A',
} as const;

const FEATURE = {
  eye: '#1F1611',
  eyeGold: '#C9A028',
  beard: '#2F1E10',
} as const;

/* ============================================================
 *  Components
 * ============================================================ */

export const KirkIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Kirk',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — command gold */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-kirk, #E5B85C)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.light} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.light} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — side-parted brown sweep */}
    <path
      d="M20 23 Q20 12 32 11 Q44 12 44 23 Q42 18 32 18 Q26 19 22 21 Z"
      fill={HAIR.brown}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Subtle confident smile */}
    <path d="M29 32 Q32 34 35 32" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
    {/* Command chevron — small detail on uniform */}
    <path d="M40 50 L43 52 L40 54" stroke="currentColor" strokeWidth="1" fill="none" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

export const WorfIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Worf',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Long hair behind, falling past shoulders */}
    <path
      d="M14 60 Q12 38 17 22 Q22 12 32 11 Q42 12 47 22 Q52 38 50 60 Z"
      fill={HAIR.black}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Uniform — warrior burgundy */}
    <path
      d="M16 60 L18 46 Q20 38 26 38 H38 Q44 38 46 46 L48 60 Z"
      fill="var(--char-worf, #9B5188)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.darkBrown} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.darkBrown} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair on forehead — heavy, falling */}
    <path
      d="M19 22 Q20 13 32 12 Q44 13 45 22 Q40 17 32 18 Q24 17 19 22 Z"
      fill={HAIR.black}
    />
    {/* Heavy scowling brows */}
    <path d="M24 23 Q27 21 30 23" stroke="#100A06" strokeWidth="1.5" fill="none" strokeLinecap="round" />
    <path d="M34 23 Q37 21 40 23" stroke="#100A06" strokeWidth="1.5" fill="none" strokeLinecap="round" />
    {/* Eyes — intense */}
    <circle cx="27" cy="26" r="1.4" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.4" fill={FEATURE.eye} />
    {/* Goatee */}
    <path d="M28 34 Q32 38 36 34 Q34 40 32 40 Q30 40 28 34 Z" fill={FEATURE.beard} />
    {/* Warrior baldric (sash) */}
    <path d="M22 44 L42 58" stroke="#3A2818" strokeWidth="3.5" strokeLinecap="round" />
  </svg>
);

export const McCoyIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'McCoy',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — medical teal */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-mccoy, #3FA796)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.light} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.light} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — graying, swept back */}
    <path
      d="M21 22 Q21 14 32 13 Q43 14 43 22 Q40 17 32 18 Q24 17 21 22 Z"
      fill={HAIR.gray}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Brows — slight worry */}
    <path d="M25 22 Q27 21 29 22" stroke={HAIR.gray} strokeWidth="1.25" fill="none" strokeLinecap="round" />
    <path d="M35 22 Q37 21 39 22" stroke={HAIR.gray} strokeWidth="1.25" fill="none" strokeLinecap="round" />
    {/* Eyes */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Thoughtful frown */}
    <path d="M29 33 Q32 31 35 33" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
    {/* Caduceus on uniform chest — universal medical symbol */}
    <path d="M32 47 V56" stroke="#FFFFFF" strokeWidth="1.2" strokeLinecap="round" />
    <path d="M29 48 Q31 46 32 48" stroke="#FFFFFF" strokeWidth="1.2" fill="none" strokeLinecap="round" />
    <path d="M35 48 Q33 46 32 48" stroke="#FFFFFF" strokeWidth="1.2" fill="none" strokeLinecap="round" />
    <path d="M32 50 Q34 52 32 54" stroke="#FFFFFF" strokeWidth="1.2" fill="none" strokeLinecap="round" />
  </svg>
);

export const SpockIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Spock',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — science blue */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-spock, #5778AB)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.cool} stroke="currentColor" strokeWidth="1" />
    {/* Head — slightly longer / cooler tone */}
    <ellipse cx="32" cy="24" rx="12.5" ry="14" fill={SKIN.cool} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — severe straight cut, black */}
    <path
      d="M19.5 22 Q20 12 32 11 Q44 12 44.5 22 Q42 17 32 18 Q24 17 19.5 22 Z"
      fill={HAIR.black}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyebrows — one raised (viewer's left) */}
    <path d="M24 22 Q27 20 30 22" stroke={HAIR.black} strokeWidth="1.5" fill="none" strokeLinecap="round" />
    <path d="M34 23 Q37 22 40 23" stroke={HAIR.black} strokeWidth="1.5" fill="none" strokeLinecap="round" />
    {/* Eyes */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Flat, neutral mouth */}
    <path d="M30 33 H34" stroke={FEATURE.eye} strokeWidth="1" strokeLinecap="round" />
  </svg>
);

export const TroiIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Troi',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Long wavy hair behind */}
    <path
      d="M12 60 Q8 38 16 22 Q22 12 32 11 Q42 12 48 22 Q56 38 52 60 Z"
      fill={HAIR.brown}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Uniform — lavender */}
    <path
      d="M16 60 L18 46 Q20 38 26 38 H38 Q44 38 46 46 L48 60 Z"
      fill="var(--char-troi, #C685D1)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.midLight} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.midLight} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair on forehead — parted in middle */}
    <path
      d="M20 22 Q20 13 32 12 Q44 13 44 22 Q40 18 32 19 Q24 18 20 22 Z"
      fill={HAIR.brown}
    />
    {/* Subtle wave / hair texture */}
    <path d="M19 28 Q17 30 19 34" stroke={HAIR.brown} strokeWidth="1" fill="none" strokeLinecap="round" />
    <path d="M45 28 Q47 30 45 34" stroke={HAIR.brown} strokeWidth="1" fill="none" strokeLinecap="round" />
    {/* Eyes — warm, kind */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Soft warm smile */}
    <path d="M28 32 Q32 35 36 32" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
    {/* Tiny earrings */}
    <circle cx="20" cy="28" r="0.8" fill="#E5B85C" />
    <circle cx="44" cy="28" r="0.8" fill="#E5B85C" />
  </svg>
);

export const ChekovIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Chekov',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — convergence olive */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-chekov, #82B05B)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.light} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.light} stroke="currentColor" strokeWidth="1.25" />
    {/* Curly bangs — youthful */}
    <path
      d="M19 22 Q19 12 32 11 Q45 12 45 22 Q43 18 39 18 Q36 15 32 18 Q28 15 25 18 Q21 18 19 22 Z"
      fill={HAIR.brown}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes — alert */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Bright small smile */}
    <path d="M28 32 Q32 34 36 32" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
  </svg>
);

export const DaxIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Dax',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — value coral */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-dax, #E0897C)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.midLight} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.midLight} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — short, pulled-back */}
    <path
      d="M20 22 Q20 13 32 12 Q44 13 44 22 Q40 17 32 18 Q24 17 20 22 Z"
      fill={HAIR.darkBrown}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Half-smile — wise / amused */}
    <path d="M28 32 Q31 33 35 32" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
  </svg>
);

export const ScottyIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Scotty',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — engineering brown */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-scotty, #B08A5F)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.light} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.light} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — auburn, side-parted, slightly receding */}
    <path
      d="M22 22 Q23 14 32 13 Q42 14 43 22 Q40 18 32 19 Q24 18 22 22 Z"
      fill={HAIR.auburn}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes — focused */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Small grin */}
    <path d="M28 32 Q32 34 36 32" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
    {/* Tiny wrench on uniform chest */}
    <path d="M40 48 L44 52" stroke="#FFFFFF" strokeWidth="1.2" strokeLinecap="round" />
    <circle cx="40" cy="48" r="1.4" fill="none" stroke="#FFFFFF" strokeWidth="1.2" />
  </svg>
);

export const RikerIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Riker',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — briefing slate */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-riker, #6B8BAC)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.light} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.light} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — medium, swept back */}
    <path
      d="M21 22 Q21 13 32 12 Q43 13 43 22 Q40 17 32 18 Q24 17 21 22 Z"
      fill={HAIR.darkBrown}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes */}
    <circle cx="27" cy="25" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="25" r="1.3" fill={FEATURE.eye} />
    {/* BEARD — Riker's defining feature */}
    <path
      d="M22 30 Q22 36 26 38 Q30 40 32 40 Q34 40 38 38 Q42 36 42 30 Q40 32 32 32 Q24 32 22 30 Z"
      fill={FEATURE.beard}
    />
    {/* Small mouth visible above beard */}
    <path d="M30 31 H34" stroke={FEATURE.beard} strokeWidth="1" strokeLinecap="round" />
  </svg>
);

export const DataIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Data',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — android silver */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-data, #C2C7CE)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.golden} stroke="currentColor" strokeWidth="1" />
    {/* Head — pale golden tint */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.golden} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — slicked back, severe */}
    <path
      d="M19 22 Q19 14 32 13 Q45 14 45 22 Q42 17 32 18 Q24 17 19 22 Z"
      fill={HAIR.black}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes — gold (synthetic / non-human cue) */}
    <circle cx="27" cy="26" r="1.5" fill={FEATURE.eyeGold} />
    <circle cx="37" cy="26" r="1.5" fill={FEATURE.eyeGold} />
    <circle cx="27" cy="26" r="0.7" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="0.7" fill={FEATURE.eye} />
    {/* Perfectly neutral mouth */}
    <path d="M30 33 H34" stroke={FEATURE.eye} strokeWidth="1" strokeLinecap="round" />
  </svg>
);

export const UhuraIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Uhura',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Uniform — comms magenta */}
    <path
      d="M14 60 L16 46 Q18 38 26 38 H38 Q46 38 48 46 L50 60 Z"
      fill="var(--char-uhura, #DC72A8)"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Neck */}
    <path d="M28 36 H36 V42 H28 Z" fill={SKIN.richBrown} stroke="currentColor" strokeWidth="1" />
    {/* Head */}
    <ellipse cx="32" cy="24" rx="13" ry="14" fill={SKIN.richBrown} stroke="currentColor" strokeWidth="1.25" />
    {/* Hair — pulled back */}
    <path
      d="M19 22 Q19 12 32 11 Q45 12 45 22 Q40 17 32 18 Q24 17 19 22 Z"
      fill={HAIR.black}
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Eyes */}
    <circle cx="27" cy="26" r="1.3" fill={FEATURE.eye} />
    <circle cx="37" cy="26" r="1.3" fill={FEATURE.eye} />
    {/* Warm smile */}
    <path d="M28 32 Q32 34 36 32" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
    {/* Headset earpiece — Uhura's signature accessory */}
    <ellipse cx="46" cy="26" rx="2.5" ry="3.5" fill="#3A3A3A" stroke="currentColor" strokeWidth="0.75" />
    {/* Mic boom curving forward */}
    <path d="M46 29 Q42 32 38 33" stroke="#3A3A3A" strokeWidth="1.5" fill="none" strokeLinecap="round" />
    <circle cx="37" cy="33" r="1" fill="#3A3A3A" />
  </svg>
);

export const OllieIcon: React.FC<IconProps> = ({
  size = 32,
  title = 'Ollie',
  ...props
}) => (
  <svg {...SVG_DEFAULTS} width={size} height={size} {...props}>
    <title>{title}</title>
    {/* Chest fur — lighter cream */}
    <path
      d="M14 60 L18 44 Q22 42 26 42 H38 Q42 42 46 44 L50 60 Z"
      fill="#F5DCB0"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
    {/* Collar */}
    <path d="M18 44 Q32 48 46 44" stroke="var(--char-ollie, #F0AA5C)" strokeWidth="3" fill="none" strokeLinecap="round" />
    {/* Collar tag */}
    <circle cx="32" cy="48" r="2" fill="#E5B85C" stroke="currentColor" strokeWidth="0.75" />
    {/* Floppy ears (behind head) */}
    <path
      d="M18 18 Q12 18 11 26 Q12 38 22 38 L22 22 Q20 18 18 18 Z"
      fill="#8B4513"
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    <path
      d="M46 18 Q52 18 53 26 Q52 38 42 38 L42 22 Q44 18 46 18 Z"
      fill="#8B4513"
      stroke="currentColor"
      strokeWidth="0.75"
      strokeLinejoin="round"
    />
    {/* Head — beagle round face */}
    <ellipse cx="32" cy="28" rx="13" ry="13" fill="#E8B98A" stroke="currentColor" strokeWidth="1.25" />
    {/* White muzzle / mask */}
    <ellipse cx="32" cy="34" rx="8" ry="5.5" fill="#F8E5C2" />
    {/* Dark patches around eyes — beagle markings */}
    <ellipse cx="26" cy="25" rx="3" ry="2.5" fill="#8B4513" opacity="0.4" />
    <ellipse cx="38" cy="25" rx="3" ry="2.5" fill="#8B4513" opacity="0.4" />
    {/* Eyes — big and friendly */}
    <circle cx="26" cy="26" r="1.6" fill={FEATURE.eye} />
    <circle cx="38" cy="26" r="1.6" fill={FEATURE.eye} />
    {/* Eye highlights */}
    <circle cx="26.5" cy="25.5" r="0.5" fill="#FFFFFF" />
    <circle cx="38.5" cy="25.5" r="0.5" fill="#FFFFFF" />
    {/* Black nose */}
    <ellipse cx="32" cy="32" rx="1.8" ry="1.4" fill={FEATURE.eye} />
    {/* Mouth */}
    <path d="M32 33 V35" stroke={FEATURE.eye} strokeWidth="1" strokeLinecap="round" />
    <path d="M32 35 Q30 37 28 36" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
    <path d="M32 35 Q34 37 36 36" stroke={FEATURE.eye} strokeWidth="1" fill="none" strokeLinecap="round" />
  </svg>
);

/* ============================================================
 *  Registry + types
 * ============================================================ */

export const characterIcons = {
  kirk: KirkIcon,
  worf: WorfIcon,
  mccoy: McCoyIcon,
  spock: SpockIcon,
  troi: TroiIcon,
  chekov: ChekovIcon,
  dax: DaxIcon,
  scotty: ScottyIcon,
  riker: RikerIcon,
  data: DataIcon,
  uhura: UhuraIcon,
  ollie: OllieIcon,
} as const;

export type CharacterId = keyof typeof characterIcons;

/**
 * Render any character by id. Useful when the character is resolved
 * from a signal payload (e.g. `signal.character.id`).
 *
 *   <CharacterIconById id={signal.character.id} size={32} />
 */
export const CharacterIconById: React.FC<
  IconProps & { id: CharacterId }
> = ({ id, ...props }) => {
  const Component = characterIcons[id];
  return <Component {...props} />;
};
