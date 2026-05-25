"""HM-INLINE-STYLE-SWEEP — scoped inline-style hex → var migrator.

Used by Batches 2 + 3 (and any future power-paste batch that follows the
same pattern). Reads dashboard/static/index.html, scopes to a line range,
finds every inline ``style="..."`` attribute within that range, and
substitutes a known-safe set of hex literals with their canonical
``var(...)`` form.

Safety rules (deliberately conservative):
  1. Only acts INSIDE ``style="..."`` attribute values. CSS class rule
     blocks (between `<style>` tags) and SVG `fill=""` / `stroke=""`
     attributes are untouched.
  2. Only the 5 hex codes whose canonical var EXISTS in the production
     :root block are mapped. Others are deliberately left in place to
     be banked with a comment in a future batch.
  3. Operates within a CALLER-SPECIFIED line range, so cross-batch
     bleed cannot happen.

Usage:
    python scripts/hm_inline_style_sweep_batch_migrate.py <start> <end>
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

INDEX_HTML = Path("dashboard/static/index.html")

# Five hex codes whose canonical var is DEFINED in the production :root
# block (verified by grep on 2026-05-25). Any hex not in this map is
# left untouched.
HEX_MAP = {
    "#ef4444": "var(--down)",
    "#22c55e": "var(--up)",
    "#94a3b8": "var(--text-secondary)",
    "#64748b": "var(--text-muted)",
    "#e2e8f0": "var(--text-primary)",
}

# Match a complete style attribute value: style="...". The negated
# character class ensures we don't cross attribute boundaries.
STYLE_ATTR_RE = re.compile(r'style="([^"]*)"')


def migrate(start: int, end: int) -> dict:
    """Migrate hex literals to canonical vars inside lines [start, end].

    Returns a per-hex substitution count + grand total.
    """
    content = INDEX_HTML.read_text()
    lines = content.split("\n")
    if start < 1 or end > len(lines):
        raise ValueError(
            f"Line range [{start}, {end}] out of file bounds [1, {len(lines)}]"
        )

    # Build a counter dict and a substitution helper that mutates it.
    counts = {hex_code: 0 for hex_code in HEX_MAP}

    def _replace_style(match: re.Match) -> str:
        style_value = match.group(1)
        for hex_code, var_form in HEX_MAP.items():
            # Use a per-hex substitution loop with a word-ish boundary so
            # e.g. "#ef4444" doesn't bleed into a longer hex like
            # "#ef4444a0". Hex chars are [0-9a-fA-F]; we require the next
            # char to NOT be one of them.
            pattern = re.escape(hex_code) + r"(?![0-9a-fA-F])"
            new_value, n = re.subn(pattern, var_form, style_value)
            if n:
                counts[hex_code] += n
                style_value = new_value
        return f'style="{style_value}"'

    # Reconstruct the file, mutating only the target range.
    new_lines = []
    for i, line in enumerate(lines, start=1):
        if start <= i <= end:
            new_lines.append(STYLE_ATTR_RE.sub(_replace_style, line))
        else:
            new_lines.append(line)

    INDEX_HTML.write_text("\n".join(new_lines))
    total = sum(counts.values())
    return {"per_hex": counts, "total": total, "range": (start, end)}


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    result = migrate(start, end)
    print(f"Range L{result['range'][0]} → L{result['range'][1]}")
    print(f"Total migrated: {result['total']}")
    print("Per-hex breakdown:")
    for hex_code, n in result["per_hex"].items():
        var_form = HEX_MAP[hex_code]
        print(f"  {hex_code:<10} → {var_form:<28} : {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
