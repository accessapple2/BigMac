#!/usr/bin/env python3
"""
OllieTrades Model Watcher
=========================
Weekly check for:
1. Updates to installed Ollama models (bigmac + Ollie Box)
2. Finance-focused Ollama watchlist movement
3. New Hugging Face + GitHub finance/trading model releases

Reports via ntfy + markdown + append-only jsonl. Never auto-pulls.

Usage:
  python3 scripts/model_watcher.py [--dry-run] [--config config/model_watchlist.yml]
"""

import argparse
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import urllib.request
import urllib.error

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml not installed. Run: pip install pyyaml --break-system-packages", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parent.parent
USER_AGENT = "OllieTrades-ModelWatcher/1.0"
HTTP_TIMEOUT = 15


def http_get_json(url, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError) as e:
        return {"_error": f"{type(e).__name__}: {e}"}


def http_get_text(url, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return r.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        return None


def get_ollama_local_models(host):
    """Returns dict {model_name: digest} from a running Ollama instance."""
    url = f"http://{host}/api/tags"
    data = http_get_json(url)
    if "_error" in data:
        return {"_error": data["_error"]}
    return {m["name"]: m.get("digest", "")[:12] for m in data.get("models", [])}


def get_ollama_registry_digest(model_name):
    """
    Fetch the current upstream digest for `<model>:<tag>` from the Ollama
    registry via a HEAD request.

    HM-AY-β fix (2026-05-08): the original probe read `Docker-Content-Digest`
    (the OCI-standard header), but Ollama's registry actually serves the
    digest on a non-standard header `ollama-content-digest` with no
    `sha256:` prefix. Reading the wrong header silently returned None for
    every installed model, leaving Layer 1 always "(unknown)". Verified
    against qwen3:8b, qwen3:14b, deepseek-r1:14b, ministral-3:3b, gemma3:4b,
    phi3:mini — all six return correct 12-char digests matching their
    locally-installed digests on a fully-current host.
    """
    base = model_name.split(":")[0]
    tag = model_name.split(":")[1] if ":" in model_name else "latest"
    url = f"https://registry.ollama.ai/v2/library/{base}/manifests/{tag}"
    headers = {
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        "User-Agent": USER_AGENT,
    }
    try:
        req = urllib.request.Request(url, headers=headers, method="HEAD")
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            digest = r.headers.get("ollama-content-digest") or r.headers.get(
                "Docker-Content-Digest", ""
            )
            if not digest:
                return None
            digest = digest.strip().replace("sha256:", "")
            return digest[:12] if digest else None
    except Exception:
        return None


def search_huggingface(query, limit=20, recency_days=7):
    url = f"https://huggingface.co/api/models?search={query}&sort=lastModified&direction=-1&limit={limit}"
    data = http_get_json(url)
    if isinstance(data, dict) and "_error" in data:
        return [], data["_error"]
    if not isinstance(data, list):
        return [], "unexpected response shape"
    cutoff = datetime.now(timezone.utc) - timedelta(days=recency_days)
    fresh = []
    for m in data:
        last = m.get("lastModified") or m.get("createdAt")
        if not last:
            continue
        try:
            ts = datetime.fromisoformat(last.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts >= cutoff:
            fresh.append({
                "id": m.get("id"),
                "downloads": m.get("downloads", 0),
                "lastModified": last,
                "tags": m.get("tags", [])[:8],
            })
    return fresh, None


def github_latest_release(repo):
    url = f"https://api.github.com/repos/{repo}/releases/latest"
    data = http_get_json(url)
    if isinstance(data, dict) and "_error" in data:
        return {"repo": repo, "error": data["_error"]}
    return {
        "repo": repo,
        "tag": data.get("tag_name", "(none)"),
        "name": data.get("name"),
        "published_at": data.get("published_at"),
        "html_url": data.get("html_url"),
    }


def load_log(log_path):
    if not log_path.exists():
        return []
    out = []
    for line in log_path.read_text().splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def last_seen_github_tag(history, repo):
    for entry in reversed(history):
        for r in entry.get("github", []):
            if r.get("repo") == repo and r.get("tag"):
                return r["tag"]
    return None


_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def ntfy_send(topic, title, message, priority="default"):
    """HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: this box has no working IPv6
    route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07) — forces IPv4 via a
    local getaddrinfo monkeypatch."""
    if not topic:
        return
    url = f"https://ntfy.sh/{topic}"
    data = message.encode("utf-8")
    # HTTP headers are latin-1; ASCII-fold the title to avoid em-dash crashes.
    safe_title = title.encode("ascii", "replace").decode("ascii")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Title", safe_title)
    req.add_header("Priority", priority)
    req.add_header("User-Agent", USER_AGENT)
    try:
        with _ntfy_ipv4_lock:
            socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
    except Exception as e:
        print(f"ntfy failed: {e}", file=sys.stderr)


def render_markdown(report):
    lines = [f"# Model Watch Report — {report['date']}\n"]
    lines.append(f"_Generated: {report['generated_at']}_\n")
    lines.append("## Summary\n")
    lines.append(f"- Installed models with upstream changes: **{report['summary']['installed_changed']}**")
    lines.append(f"- Hugging Face fresh releases (last {report['recency_days']}d): **{report['summary']['hf_fresh']}**")
    lines.append(f"- GitHub releases since last check: **{report['summary']['gh_new']}**\n")

    lines.append("## Installed Models\n")
    for host, models in report["installed"].items():
        lines.append(f"### {host}")
        if "_error" in models:
            lines.append(f"- unreachable: `{models['_error']}`\n")
            continue
        for name, info in models.items():
            mark = "update available" if info.get("update_available") else "current"
            lines.append(f"- `{name}` — local `{info['local']}` / upstream `{info.get('upstream') or '(unknown)'}` — {mark}")
        lines.append("")

    lines.append("## Ollama Finance Watchlist\n")
    for name, digest in report["watchlist_digests"].items():
        lines.append(f"- `{name}` — registry digest `{digest or '(not found)'}`")
    lines.append("")

    lines.append("## Hugging Face — Fresh Releases\n")
    if not report["huggingface"]:
        lines.append("_No fresh finance/trading releases this week._\n")
    else:
        for q, results in report["huggingface"].items():
            if results:
                lines.append(f"### Query: `{q}`")
                for r in results[:10]:
                    lines.append(f"- [{r['id']}](https://huggingface.co/{r['id']}) — {r['lastModified']} — downloads {r['downloads']:,} — tags: {', '.join(r['tags']) or '—'}")
                lines.append("")

    lines.append("## GitHub Releases\n")
    for r in report["github"]:
        if "error" in r:
            lines.append(f"- `{r['repo']}`: {r['error']}")
        else:
            new_marker = " (new)" if r.get("is_new") else ""
            lines.append(f"- `{r['repo']}` -> [{r['tag']}]({r['html_url']}){new_marker} — {r['published_at']}")
    lines.append("")

    lines.append("---\n_Reports only. No models pulled. Admiral decides on changes._\n")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(ROOT / "config" / "model_watchlist.yml"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    log_path = ROOT / cfg["log_path"]
    report_dir = ROOT / cfg["report_dir"]
    report_dir.mkdir(parents=True, exist_ok=True)

    history = load_log(log_path)

    # 1. Installed models — both hosts
    installed = {}
    for host_label, host_addr in [("bigmac", "localhost:11434"), ("ollie_box", "192.168.1.166:11434")]:
        local = get_ollama_local_models(host_addr)
        if "_error" in local:
            installed[host_label] = local
            continue
        per_host = {}
        for name in cfg["installed_local"].get(host_label, []):
            local_d = local.get(name, "(not installed)")
            upstream = get_ollama_registry_digest(name)
            per_host[name] = {
                "local": local_d if local_d else "(none)",
                "upstream": upstream,
                "update_available": bool(upstream and local_d and local_d != upstream and local_d != "(not installed)"),
            }
        installed[host_label] = per_host

    # 2. Ollama finance watchlist
    watchlist_digests = {}
    for name in cfg["ollama_finance_watchlist"]:
        watchlist_digests[name] = get_ollama_registry_digest(name)

    # 3. Hugging Face
    hf_results = {}
    for s in cfg["huggingface_searches"]:
        fresh, err = search_huggingface(s["query"], s.get("limit", 20), cfg.get("recency_days", 7))
        if fresh:
            hf_results[s["query"]] = fresh

    # 4. GitHub
    gh = []
    for repo in cfg["github_repos"]:
        rel = github_latest_release(repo)
        last = last_seen_github_tag(history, repo)
        if "error" not in rel:
            rel["is_new"] = (last is not None and last != rel.get("tag"))
        gh.append(rel)

    # Aggregate
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary = {
        "installed_changed": sum(1 for h in installed.values() if isinstance(h, dict) for m in h.values() if isinstance(m, dict) and m.get("update_available")),
        "hf_fresh": sum(len(v) for v in hf_results.values()),
        "gh_new": sum(1 for r in gh if r.get("is_new")),
    }
    report = {
        "date": today,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "recency_days": cfg.get("recency_days", 7),
        "summary": summary,
        "installed": installed,
        "watchlist_digests": watchlist_digests,
        "huggingface": hf_results,
        "github": gh,
    }

    # Markdown
    md_path = report_dir / f"MODEL_WATCH_{today}.md"
    md_path.write_text(render_markdown(report))

    # JSONL append
    if not args.dry_run:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a") as f:
            f.write(json.dumps(report) + "\n")

    # ntfy
    title_prefix = "OK"
    if summary["installed_changed"] > 0:
        title_prefix = "UPDATE"
    if summary["gh_new"] > 0 or summary["hf_fresh"] > 0:
        title_prefix = "NEW"
    title = f"[{title_prefix}] Model Watch - {today}"
    body = (
        f"{summary['installed_changed']} installed updates · "
        f"{summary['hf_fresh']} HF fresh · "
        f"{summary['gh_new']} GH new releases\n\n"
        f"Report: docs/model_watch/MODEL_WATCH_{today}.md"
    )
    if not args.dry_run:
        ntfy_send(cfg.get("ntfy_topic", "ollietrades-admin"), title, body)

    # stdout summary
    print(title)
    print(body)
    print(f"\nMarkdown: {md_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
