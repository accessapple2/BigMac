# ram-discipline.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## RAM Discipline (post-MSI-migration 2026-05-20)
- **bigmac (Mac Mini M4, 16GB RAM)** — runs FastAPI trader, dashboard,
  schedulers, signal center. Ollama is NO LONGER co-located here.
- **Ollie Max (`olliemax.home.local`, 192.168.1.168, RTX 5080 16GB VRAM)** —
  sole Ollama host. Budget: TWO 7–8B-class models fully co-resident (~10–12GB;
  live `/api/ps` 2026-05-28 showed qwen3:8b 5.98GB + ministral-3:3b 4.62GB =
  10.6GB resident together). A 14B fits solo but TWO 14B cannot co-reside in
  16GB → 14B-vs-14B rotation still swaps. **(Corrected 2026-05-28 HM-AUDIT-T0:**
  prior "RTX 5060 8GB / one 7B fits" was WRONG — it drove HM-WR-VRAM-THRASHING's
  premise + the navigator "too big for 8GB" swap, both now suspect; keep_alive/
  batching fixes still help, only the scheduling *rationale* changes. 16GB
  confirmed via live /api/ps; exact model per XO audit.) **SSH-gap RESOLVED
  2026-05-31:** passwordless `ssh bigmac@192.168.1.168` works (provisioned this
  day; verified `echo OK`). Ollama store on .168 = `/usr/share/ollama/.ollama/
  models` (service user; manifests/blobs world-readable, no sudo for reads;
  `ollama rm`/`list` work as bigmac via the API). The prior "SSH-to-Ollie-Max
  key gap" note is STALE.
- **Preferred local workhorse:** `qwen3:8b` (7 active agents share it per
  HM-CD `_HM_CD_KEEP_ALIVE` lookup).
- `qwen3:30b` rejected — too slow for this GPU (latency, not a VRAM-fit issue).
- Avoid loading full datasets into memory; stream or chunk.
- McCoy (CSP) runs on **`plutus-v1`** — the finance-trained model resolved via
  `ai_players.ollama-plutus` (`config.py:175`, "McCoy's finance brain"). Doc
  previously asserted McCoy=`0xroyce/plutus`; corrected to plutus-v1 (doc-vs-
  reality drift, HM-MODEL-RETIRE pre-check C 2026-05-31). The `0xroyce/plutus:
  latest` tag is unwired (its only ref, `dayblade-0dte`, is halt_mode='full',
  inert) and is **slated for retirement under HM-MODEL-RETIRE** (host `ollama
  rm` pending manual execution). Re-pullable from HF if ever needed.
