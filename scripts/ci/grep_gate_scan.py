#!/usr/bin/env python3
"""HM-GREP-GATE-PROSE-FALSE-POSITIVE-2026-09-11: forbidden-pattern scanner
for .github/workflows/grep-gate.yml, extracted out of raw `grep -r` because
plain grep can't tell "this line performs the forbidden thing" from "this
line is a comment/docstring narrating why we don't do it" -- exactly the
gap that let docs/XO_BACKLOG.md:11146 (quoting tests/test_rule1_delete_
guard.py's own deliberate `DROP TABLE signals` while explaining why that
test exists) trip the DROP TABLE check, the second time this exact class of
false positive has hit this gate (first: HM-GREP-GATE-RELAY-REGRESSION-
2026-09-10, a relay doc quoting the same test).

Two independent layers, both must pass a line for it to be reported:
1. Directory/file/path excludes (docs/**, data/reports/relay/**, exact
   legitimate-reference files) -- same mechanism the old inline grep had,
   just centralized so all four checks share one implementation instead of
   four copy-pasted exclusion chains that drift.
2. Prose stripping for *.py files only: comments and DOCSTRING-SHAPED bare
   string-literal statements (module/class/def docstrings, and any ad hoc
   triple-quoted block used as an inline comment) are blanked out before
   matching, via `ast` + `tokenize` -- not by removing every string
   literal, so a real `conn.execute("DROP TABLE signals")` in actual code
   still matches; only bare string EXPRESSION STATEMENTS (not assigned,
   not passed as an argument, not returned into anything) are prose.
   *.sh files get simple whole-line `#`-comment stripping (no docstring
   concept in shell).

Reports every survivor as `path:lineno:content` (from the ORIGINAL file,
not the blanked copy, so the output stays human-readable) and exits 1 if
any survive, 0 if the tree is clean -- the direction a normal CI script
expects, deliberately the opposite of grep's own 0-means-found convention
that the old inline version had to work around with `if grep ...; then`.
"""
from __future__ import annotations

import argparse
import ast
import io
import os
import re
import sys
import tokenize


def _docstring_line_ranges(source: str) -> set[int]:
    """Line numbers (1-indexed) covered by a bare string-literal expression
    statement anywhere in the module -- real docstrings (module/class/def)
    plus any ad hoc triple-quoted comment block used as a standalone
    statement. NOT string literals used as arguments, assigned, or
    returned -- those are real code and must stay visible to the scan.
    """
    lines: set[int] = set()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return lines  # unparsable file -- fall back to comment-stripping only
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str):
            start = node.lineno
            end = getattr(node, "end_lineno", start)
            lines.update(range(start, end + 1))
    return lines


def _comment_line_ranges(source: str) -> set[int]:
    """Line numbers of tokenize COMMENT tokens (the `# ...` itself; a line
    with real code before the comment is NOT added here -- only whole-
    comment-token lines get blanked whole, matching the "comment line"
    case explicitly named in the request. A trailing `code()  # comment`
    line keeps its code half scannable.
    """
    lines: set[int] = set()
    try:
        for tok in tokenize.generate_tokens(io.StringIO(source).readline):
            if tok.type == tokenize.COMMENT:
                # Blank the whole line only if the comment starts at (or
                # near) column 0 after whitespace -- i.e. the line IS the
                # comment, not code-then-comment.
                line_text = source.splitlines()[tok.start[0] - 1]
                if line_text[: tok.start[1]].strip() == "":
                    lines.add(tok.start[0])
    except tokenize.TokenError:
        pass
    return lines


def _blank_prose(source: str) -> str:
    """Return `source` with docstring-statement lines and whole-comment
    lines replaced by blank lines (same line count, so line numbers in the
    grep output still match the original file).
    """
    blank = _docstring_line_ranges(source) | _comment_line_ranges(source)
    if not blank:
        return source
    out = []
    for i, line in enumerate(source.splitlines(), start=1):
        out.append("" if i in blank else line)
    return "\n".join(out)


def _strip_shell_comments(source: str) -> str:
    out = []
    for line in source.splitlines():
        out.append("" if line.lstrip().startswith("#") else line)
    return "\n".join(out)


def _iter_files(root: str, include_ext: list[str], include_name_contains: list[str],
                 exclude_dirs: set[str]) -> list[str]:
    matches = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs and d != ".git"]
        for fn in filenames:
            ext_match = os.path.splitext(fn)[1] in include_ext
            name_match = any(sub in fn for sub in include_name_contains)
            if ext_match or name_match:
                matches.append(os.path.relpath(os.path.join(dirpath, fn), root))
    return matches


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--pattern", required=True, help="Regex to search for (re.IGNORECASE-able via --ignore-case)")
    p.add_argument("--ignore-case", action="store_true")
    p.add_argument("--include-ext", nargs="*", default=[], help="File extensions to scan, e.g. .py .yaml")
    p.add_argument("--include-name-contains", nargs="*", default=[], help="Scan any file whose basename contains this substring, e.g. .env")
    p.add_argument("--exclude-dir", nargs="*", default=[], help="Directory names to skip anywhere in the tree (matches a path component, e.g. '_archive')")
    p.add_argument("--exclude-path-prefix", nargs="*", default=[], help="Skip any file whose repo-relative path starts with this, e.g. 'docs/' or 'data/reports/relay/' -- precise, unlike --exclude-dir")
    p.add_argument("--exclude-path-exact", nargs="*", default=[], help="Exact repo-relative file paths to skip entirely")
    p.add_argument("--exclude-path-contains", nargs="*", default=[], help="Skip any file whose relative path contains this substring")
    p.add_argument("--exclude-line-contains", nargs="*", default=[], help="Skip a matched line if it contains this substring")
    p.add_argument("--root", default=".")
    args = p.parse_args()

    flags = re.IGNORECASE if args.ignore_case else 0
    pattern = re.compile(args.pattern, flags)
    # Always skip vendored/generated trees regardless of caller-supplied
    # excludes -- these are .gitignore'd (won't exist in a fresh CI
    # checkout at all) but a local dev run of this script should not choke
    # walking + AST-parsing thousands of third-party files either.
    exclude_dirs = set(args.exclude_dir) | {
        "venv", ".venv", "scrapling-venv", "node_modules", "__pycache__",
    }

    hits: list[str] = []
    for relpath in _iter_files(args.root, set(args.include_ext), args.include_name_contains, exclude_dirs):
        norm = relpath.replace(os.sep, "/")
        if norm in args.exclude_path_exact:
            continue
        if any(sub in norm for sub in args.exclude_path_contains):
            continue
        if any(norm.startswith(pre) for pre in args.exclude_path_prefix):
            continue

        full = os.path.join(args.root, relpath)
        try:
            with open(full, "r", encoding="utf-8", errors="replace") as f:
                original = f.read()
        except OSError:
            continue

        if norm.endswith(".py"):
            scan_text = _blank_prose(original)
        elif norm.endswith(".sh"):
            scan_text = _strip_shell_comments(original)
        else:
            scan_text = original

        original_lines = original.splitlines()
        for i, line in enumerate(scan_text.splitlines(), start=1):
            if pattern.search(line):
                real_line = original_lines[i - 1] if i - 1 < len(original_lines) else line
                if any(sub in real_line for sub in args.exclude_line_contains):
                    continue
                hits.append(f"{norm}:{i}:{real_line}")

    if hits:
        for h in hits:
            print(h)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
