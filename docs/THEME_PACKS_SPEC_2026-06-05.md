# Theme Packs — Persona Mapping Spec (Star Wars / MASH / Dallas)

**Status:** Design-only deliverable (no code). Overnight queue Task 6, 2026-06-05.
**Goal:** Map the fleet's **strategy ROLES** (not the Trek names) onto three
alternate persona themes, so the dashboard could swap the cast without touching
any strategy logic. Icons stay **role-keyed**; only the persona registry swaps.

---

## Design principle — role is the primary key, persona is a skin

Today every agent is identified two ways:
- **Strategy role** (the load-bearing identity): "high-VIX CSP seller",
  "RSI mean-reversion", "bear/short specialist", etc. This drives gates,
  routing, scoring — and is theme-independent.
- **Persona** (the skin): the Star Trek name (McCoy, Spock, Worf…) + glyph.

A theme pack is a **persona registry** keyed by role. The role→persona map is
the only thing that swaps. `ai_players.id`, strategy code, `CREW_MANIFEST`
mandates, and the role-keyed glyph registry (`conceptGlyphs`) are untouched.

```
role_key  ──►  persona_registry[active_theme][role_key]  ──►  {name, glyph_key, blurb}
```

`active_theme ∈ {trek (default), star_wars, mash, dallas}`. Switching themes =
re-pointing `active_theme`; a single render pass relabels every card. Because
glyphs are already role-keyed (`conceptGlyphs.rsi`, `.macd`, …), icons need **no**
change across themes — only display names + flavor blurbs move.

---

## Mapping table

| Role (load-bearing) | Trek (current) | Star Wars | MASH | Dallas |
|---|---|---|---|---|
| High-VIX CSP income seller | McCoy | Han Solo (thrives in chaos) | Hawkeye Pierce (best under fire) | J.R. Ewing (profits off volatility) |
| Low-VIX CSP income seller | Dax | Lando Calrissian (smooth, calm conditions) | BJ Hunnicutt (steady hand) | Bobby Ewing (steady, disciplined) |
| Rule-based GEX/premium detector | Neo | R2-D2 (deterministic droid, reads the machine) | Radar O'Reilly (anticipates everything) | Harv Smithfield (the numbers man) |
| Smart-money / Congress copy | Capitol | Cassian Andor (intel operative) | Sgt. Rizzo (works every angle) | Cliff Barnes (always angling) |
| RSI mean-reversion | Spock | Yoda (patience, fade the extremes) | Father Mulcahy (patient, principled) | Miss Ellie (patient matriarch) |
| Bear specialist / shorts | Worf | Darth Vader (aggressive downside) | Col. Flagg (covert aggression) | Carter McKay (corporate raider) |
| Momentum / EMA pullback | Chekov (Navigator) | Poe Dameron (momentum pilot) | Trapper John (fast mover) | Ray Krebbs (ranch hand on the move) |
| Institutional 13F / Form-4 | Uhura | Mon Mothma (institutional intel) | Col. Potter (institutional authority) | Jock Ewing (the founder) |
| Iron condor / spreads | Sulu | Wedge Antilles (precision wings) | Charles Winchester (refined precision) | Sue Ellen (calculated plays) |
| Contrarian / fade extremes | Trip (energy-arnold) | Obi-Wan ("from a certain point of view") | Sidney Freedman (contrarian read) | Cliff Barnes (anti-Ewing contrarian) |
| XO synthesis / orchestration | Riker | Admiral Ackbar (reads the trap) | Col. Sherman Potter (command synthesis) | Bobby Ewing (operations chief) |
| Quality gate / approval | Ollie | Mace Windu (council approval) | Margaret Houlihan (enforces standards) | Jock Ewing (final say) |
| Long-horizon compounders (5/10/20y) | Sarek / Janeway / Surak | Qui-Gon / Yoda / Luke (masters across eras) | Sidney / Potter / Mulcahy (the elders) | Jock / Miss Ellie / Punk Anderson |
| Metals macro | Dalio | Watto (commodities trader) | Klinger (barters everything) | Punk Anderson (oil & commodities) |
| Advisory / flagship | Kirk | Luke Skywalker (hero-advisor) | Hawkeye (lead voice) | J.R. Ewing (the boss) |
| Post-earnings short fade (HM-SHORT-ENGINE) | Sell-The-News | Saw Gerrera (opportunistic strike) | Col. Flagg (ambush) | Carter McKay (raids weakness) |

> Roles, not names, are canonical. If a role is retired/added, only this table
> changes — no strategy code moves.

---

## Switch mechanism (implementation sketch — for a future build, not now)

1. **Persona registry** — a static map `PERSONAS[theme][role_key] = {name, glyph_key, blurb}`.
   Trek is the default/fallback; a missing role in a theme falls back to Trek so
   no card ever renders blank.
2. **Active theme** — one setting (`ui.active_theme`, default `trek`), persisted
   in user/settings; no per-agent state.
3. **Render** — card components already receive a `role_key`; they look up
   `PERSONAS[active_theme][role_key]` for the display name + blurb and reuse the
   existing role-keyed `conceptGlyphs[glyph_key]`. **Icons do not change** across
   themes (they're concept glyphs, not portraits) — only names/blurbs swap.
4. **No backend touch** — `ai_players.id`, routing tables, gates, scoring, and
   the W0/shadow substrate are all role/id-keyed and theme-agnostic. A theme
   swap is purely a presentation-layer relabel.

**Open product decisions (for Admiral):** (a) per-user vs global theme; (b)
whether to also theme the section headers (Bridge → "War Room"/"Swamp"/"Southfork");
(c) optional themed portraits (a bigger lift than the role-keyed glyph reuse above).
