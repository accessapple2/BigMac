#!/usr/bin/env python3
"""HM-PRIME Part C — move (not delete) sections out of CLAUDE.md into docs.

Per the sacred-data rule: content is RELOCATED, never destroyed. For every
section moved, the script asserts the extracted text is present byte-for-byte
in the destination file before rewriting CLAUDE.md. Run per batch:

    python3 tools/lean_claudemd.py batch1

Sections are located by header text and bounded by the next header of equal or
higher level, with fenced code blocks (```...```) ignored so comment-headers
like `# Avoid:` inside a fence are never mistaken for sections.
"""
import os
import sys

ROOT = os.path.expanduser("~/autonomous-trader")
SRC = os.path.join(ROOT, "CLAUDE.md")


def hdr_level(line):
    if not line.startswith("#"):
        return 0
    return len(line) - len(line.lstrip("#"))


def find_section(lines, prefix):
    """Return (start, end) line indices for the section whose header startswith prefix."""
    in_fence = False
    start = None
    level = None
    for i, l in enumerate(lines):
        if l.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if start is None:
            if l.startswith(prefix) and hdr_level(l) > 0:
                start, level = i, hdr_level(l)
        else:
            lv = hdr_level(l)
            if 0 < lv <= level:
                return start, i
    if start is not None:
        return start, len(lines)
    return None


# (header_prefix, destination_relpath, pointer_lines, mode)  mode: 'move' | 'archive'
BATCHES = {
    "batch1": [
        ("## Fleet Roster", "docs/FLEET-ROSTER.md",
         ["## Fleet Roster",
          "Full roster (active/bench/sniper/elder/metals/retired) moved to "
          "[`docs/FLEET-ROSTER.md`](docs/FLEET-ROSTER.md).",
          "**Live counts are authoritative via the SessionStart primer "
          "(`data/trader.db` `ai_players`), not a static list here.**"],
         "move"),
        ("## Doctrine Lessons", "docs/DOCTRINE.md",
         ["## Doctrine Lessons",
          "Distilled sprint lessons moved to [`docs/DOCTRINE.md`](docs/DOCTRINE.md) "
          "(titles indexed there). Load on demand; not needed every session."],
         "move"),
    ],
    "batch2": [
        ("## Architecture: Two-Book Bridge Policy", "docs/architecture/two-book-bridge.md",
         ["## Architecture: Two-Book Bridge Policy",
          "Full policy moved to [`docs/architecture/two-book-bridge.md`]"
          "(docs/architecture/two-book-bridge.md).",
          "Summary: Option β two books; internal book + broker book; forwarding "
          "gates decide what routes to the broker. See doc for routing rules + naming discipline."],
         "move"),
        ("## SUPER_MAX Wave Program", "docs/SUPER_MAX.md",
         ["## SUPER_MAX Wave Program",
          "Full W0–W4 program moved to [`docs/SUPER_MAX.md`](docs/SUPER_MAX.md).",
          "Load-bearing: graduation gate + hard shadow boundary are documented there."],
         "move"),
        ("## Ghost Tracking Architecture", "docs/architecture/ghost-tracking.md",
         ["## Ghost Tracking Architecture",
          "Two-system ghost-tracking detail moved to "
          "[`docs/architecture/ghost-tracking.md`](docs/architecture/ghost-tracking.md). "
          "Do not consolidate the two systems (see doc)."],
         "move"),
        ("## strategy_signals", "docs/data-planes.md",
         ["## strategy_signals (convergence scanner data plane)",
          "Schema + data-plane detail moved to [`docs/data-planes.md`](docs/data-planes.md)."],
         "move"),
    ],
    "batch3": [
        ("## Network Bindings", "docs/runbooks/network-bindings.md",
         ["## Network Bindings",
          "Port/host binding reference moved to "
          "[`docs/runbooks/network-bindings.md`](docs/runbooks/network-bindings.md)."],
         "move"),
        ("## RAM Discipline", "docs/runbooks/ram-discipline.md",
         ["## RAM Discipline",
          "Post-MSI-migration RAM discipline moved to "
          "[`docs/runbooks/ram-discipline.md`](docs/runbooks/ram-discipline.md). "
          "Rule: respect model co-residency limits; don't overcommit VRAM."],
         "move"),
    ],
    "batch5": [
        ("## Error Handling Posture", "docs/DOCTRINE.md",
         ["## Error Handling Posture",
          "Full posture + Avoid/Prefer code examples moved to "
          "[`docs/DOCTRINE.md`](docs/DOCTRINE.md).",
          "**Rule (load-bearing):** no silent catch; handle async errors "
          "explicitly; bounded I/O timeouts; degrade, don't crash."],
         "move"),
        ("### Scheduler-owned jobs", "docs/runbooks/reboot-lifecycle.md",
         ["### Scheduler-owned jobs",
          "launchd plist archival + scheduler-ownership detail moved to "
          "[`docs/runbooks/reboot-lifecycle.md`](docs/runbooks/reboot-lifecycle.md)."],
         "move"),
        ("## Logging Sink Split", "docs/runbooks/logging.md",
         ["## Logging Sink Split (trader.log vs trader_error.log)",
          "Detail moved to [`docs/runbooks/logging.md`](docs/runbooks/logging.md)."],
         "move"),
    ],
    "batch4": [
        ("## Drift Catalog 2026-05-17", "docs/CLAUDE-archive-2026-05.md",
         ["## Drift Catalog 2026-05-17",
          "Historical drift snapshot archived to "
          "[`docs/CLAUDE-archive-2026-05.md`](docs/CLAUDE-archive-2026-05.md)."],
         "archive"),
        ("### Future considered epic", "docs/CLAUDE-archive-2026-05.md",
         ["### Future considered epic: submit-time manual-halt gate (NOT built)",
          "Speculative/unbuilt design archived to "
          "[`docs/CLAUDE-archive-2026-05.md`](docs/CLAUDE-archive-2026-05.md)."],
         "archive"),
    ],
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in BATCHES:
        print("usage: lean_claudemd.py <batch1|batch2|batch3|batch4>")
        sys.exit(2)
    batch = sys.argv[1]
    with open(SRC) as fh:
        text = fh.read()
    lines = text.splitlines(keepends=True)

    # locate all sections first (against current file), then apply bottom-up
    plan = []
    for prefix, dest, pointer, mode in BATCHES[batch]:
        loc = find_section(lines, prefix)
        if loc is None:
            print(f"  SKIP (not found, already moved?): {prefix}")
            continue
        plan.append((loc[0], loc[1], prefix, dest, pointer, mode))
    plan.sort(key=lambda x: x[0], reverse=True)

    moved = []
    for start, end, prefix, dest, pointer, mode in plan:
        block = "".join(lines[start:end])
        absdest = os.path.join(ROOT, dest)
        os.makedirs(os.path.dirname(absdest), exist_ok=True)
        new = not os.path.exists(absdest)
        with open(absdest, "a") as dh:
            if new:
                dh.write(f"# {os.path.basename(dest)}\n\n"
                         f"> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).\n\n")
            else:
                dh.write("\n\n---\n\n> Relocated from CLAUDE.md (HM-PRIME Part C).\n\n")
            dh.write(block.rstrip("\n") + "\n")
        # CONSERVATION ASSERT: extracted block must now exist in dest
        with open(absdest) as dh:
            destdata = dh.read()
        assert block.rstrip("\n") in destdata, f"CONSERVATION FAILED for {prefix} -> {dest}"
        # replace in source with pointer
        ptr = "\n".join(pointer) + "\n\n"
        lines[start:end] = [ptr]
        moved.append((prefix, dest, end - start, mode))

    with open(SRC, "w") as fh:
        fh.write("".join(lines))

    print(f"[{batch}] moved {len(moved)} section(s):")
    for prefix, dest, n, mode in sorted(moved):
        print(f"  {mode:7} {n:>4} lines  {prefix}  ->  {dest}")
    with open(SRC) as fh:
        print(f"  CLAUDE.md now {sum(1 for _ in fh)} lines")


if __name__ == "__main__":
    main()
