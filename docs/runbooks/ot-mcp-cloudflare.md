# ot-mcp — Cloudflare settings to apply by hand (STAGED, not applied)

The read-only MCP observer listens on `127.0.0.1:8765` (`scripts/ot_mcp_observer.py`,
LaunchDaemon `infra/launchd/com.trademinds.ot-mcp.plist`). Nothing on the Cloudflare
side has been changed from bigmac. There's no scoped API token, by decision (2026-09-14).
Apply in this order so the hostname is never reachable without Access in front of it.

## 0. Before you start

- Install and verify the LaunchDaemon first (install steps in the plist header). Then check
  `lsof -nP -iTCP:8765 -sTCP:LISTEN` shows Python on `127.0.0.1:8765` only.
- The observer refuses any Host header except `127.0.0.1:8765`, `localhost:8765` and
  `ot-mcp.ollietrades.com` (DNS-rebinding protection, verified: spoofed Host returns `421`).
  Do **not** set an HTTP Host header override on the tunnel route.

## 1. Access application first

**Zero Trust → Access controls → Applications → Add an application → Self-hosted**

| Setting | Value |
|---|---|
| Application name | `ot-mcp` |
| Public hostname / domain | `ot-mcp.ollietrades.com` (path empty) |
| Policy | same rule as `bridge-allow` — see note below |
| Session duration | your call (the bridge app uses 730h; Managed OAuth has its own lifetimes, below) |

**About reusing `bridge-allow`:** `docs/runbooks/network-bindings.md` records that
`bridge-allow` was created as an **inline** policy attached to the "bridge" app (2026-06-12).
Cloudflare's Managed OAuth page doesn't say whether inline policies can be shared. Pick one:
- If `bridge-allow` shows under reusable policies, attach it to `ot-mcp`.
- If it's inline-only, create a policy `ot-mcp-allow` with the same rule: Action Allow,
  Include Emails, the same three addresses as `bridge-allow` (listed in CLAUDE.md).
  That list includes a non-Captain address, so decide whether the observer should have
  the same audience as the bridge.

Never leave the app with zero attached policies. An app with no attached policy fails
closed, which is the 2026-06-12 lesson.

### Managed OAuth (so an MCP client can authenticate without a browser session)

**Same application → ⋯ → Edit → Advanced settings**

| Setting | Value |
|---|---|
| **Managed OAuth** | On |
| **Access token lifetime** | 5–15 min (Cloudflare's recommendation for MCP clients) |
| **Grant session duration** | 1–2 weeks (refresh re-evaluates the policy each time) |
| **Allowed redirect URIs** | the XO client's OAuth callback (https only) — fill in from that client's docs |
| **Allow localhost clients** / **Allow loopback clients** | On only if a local MCP client (e.g. Claude Code on a laptop) will connect |

Prerequisite: the connecting client must support OAuth resource indicators (RFC 8707).

## 2. Tunnel route second

**Zero Trust → Networks → Tunnels → (the bigmac tunnel, remote-managed) → Public hostnames → Add**

| Setting | Value |
|---|---|
| Subdomain | `ot-mcp` |
| Domain | `ollietrades.com` |
| Path | *(empty)* |
| Service type | `HTTP` |
| URL | `localhost:8765` |

This creates the proxied DNS record. The tunnel's remote config version will bump
(it was v11 per CLAUDE.md).

## 3. Verify

1. **Unauthenticated is refused:** `curl -s -o /dev/null -w '%{http_code}\n' https://ot-mcp.ollietrades.com/mcp`
   must NOT be `200`. Expect an Access redirect or `401`/`403`.
2. **Authenticated MCP client:** connect to `https://ot-mcp.ollietrades.com/mcp`, complete
   the Access login, list tools. Expect exactly 10, all read-only.
3. **Audit:** `tail -n 3 logs/ot_observer.log` shows those calls with tool, args, row count
   and duration.
4. **Kill switch:** `touch data/ot_observer.disabled` makes any tool call return status
   `disabled`. Then `rm data/ot_observer.disabled`.

## Rollback (any one is enough, fastest first)

- `touch data/ot_observer.disabled`: every tool answers `disabled`, and the process stays up.
- Delete the `ot-mcp` public hostname on the tunnel.
- `sudo launchctl bootout system/com.trademinds.ot-mcp`.

## Not built (option)

Access forwards the user's JWT in `Cf-Access-Jwt-Assertion`. The observer could validate
it (team domain + application AUD) as a second layer behind Access. Not implemented.

Sources: [Managed OAuth](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/managed-oauth/),
[Managed OAuth changelog 2026-03-20](https://developers.cloudflare.com/changelog/post/2026-03-20-managed-oauth/),
[Secure MCP servers](https://developers.cloudflare.com/cloudflare-one/access-controls/ai-controls/secure-mcp-servers/).
