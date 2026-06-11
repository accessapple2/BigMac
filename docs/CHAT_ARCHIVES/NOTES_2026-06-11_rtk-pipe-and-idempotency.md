# Notes 2026-06-11 — RTK piped-output digest + the duplicate-order trap

## The `curl … | python3 -m json.tool` "JSON error" is RTK, not a bug
The RTK token-killer hook intercepts **piped** stdout and replaces a JSON body
with a **type-schema digest** (`{ action: string, ceiling: float, … }` — keys +
types, values stripped; its 60–90% token-savings transform). `json.tool` then
fails (`Expecting property name … line 2 column 3`) because the digest isn't JSON.
Confirmed by `od -c` on the piped bytes vs a clean `-o file` capture.

**Get raw JSON when piping:**
- `curl … -o /tmp/x.json` then parse (RTK doesn't touch file output), or
- `rtk proxy curl …` (documented raw passthrough), or
- read the digest as a schema view (it's informative, not an error).

## The duplicate-order incident it caused (and the rule)
RTK transforms the **output**, not the **request**. The first
`curl -X POST …/spread/submit … | python3 -m json.tool` **did submit the order**;
only the displayed result was the schema digest, which looked "broken" — so it was
re-fired twice → **3 duplicate live (paper) CEG spreads (ids 93/94/95)** instead of 1.
94/95 were cancelled; 93 left as the intended hedge.

**Rule (now also in global memory `feedback_rtk_piped_mutation_rerun`):** a piped
non-idempotent command (POST / order / write) has ALREADY executed regardless of
how its output renders. NEVER re-run it to "get a clean response" — verify state
(broker/DB) first. Prefer `-o file` for any mutation whose response you need to read.

## Backstop shipped
W2.1 added a submit idempotency guard (L1 deterministic `client_order_id` →
Alpaca dedup; L2 local guard at the chokepoint returns the existing order for an
identical OPEN spread). So this specific footgun on the spread path is now caught
even if the re-run happens — but the behavioral rule above is the general fix.
