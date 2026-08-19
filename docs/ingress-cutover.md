# Direct Funnel ingress cutover

This runbook moves the Linear webhook to a Tailscale Funnel that terminates
public HTTPS on the same Mac that runs `linear-agent-bridge`. Funnel proxies
directly to `http://127.0.0.1:<PORT>`. There is no production TCP forwarder or
second serving host in this topology.

```text
Linear webhook delivery
  -> https://<serving-host>.<tailnet>.ts.net/webhook
  -> Tailscale Funnel on <serving-host>
  -> http://127.0.0.1:<PORT>/webhook
  -> com.linear-agent-bridge
```

`deploy/tcp_forward.py` is diagnostic-only. It remains available to isolate a
private last-hop problem, is loopback-bound, and must not become the canonical
ingress or remain in the production path after diagnosis. A separate ingress
host cannot connect directly to the bridge host's private address because the
bridge listens only on `127.0.0.1`.

For that diagnostic, establish a local SSH tunnel on the ingress host first:

```bash
ssh -N -T \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=2 \
  -L 127.0.0.1:9900:127.0.0.1:3979 bridge-host
```

In a second shell on the ingress host, start the forwarder against the local
tunnel endpoint and point the existing HTTPS ingress at `127.0.0.1:8899`:

```bash
python3 deploy/tcp_forward.py 8899 127.0.0.1 9900
```

```text
HTTPS ingress on diagnostic host
  -> 127.0.0.1:8899 tcp_forward.py
  -> 127.0.0.1:9900 SSH local tunnel
  -> bridge host 127.0.0.1:3979
```

The SSH destination resolves its final `127.0.0.1:3979` hop on the bridge
host. Do not pass a LAN, Tailscale, or other bridge address to
`tcp_forward.py`; it rejects non-loopback upstreams. Stop both diagnostic
processes after restoring direct Funnel.

## Cutover record

Fill these fields before changing Linear:

| Field | Required value |
| --- | --- |
| Public-edge owner | Named operator accountable for Funnel and DNS/TLS checks |
| Serving host | Exact hostname running both the service and Funnel |
| Canonical webhook URL | `https://<serving-host>.<tailnet>.ts.net/webhook`, including a port when Funnel is not on 443 |
| Previous webhook URL | Exact URL currently saved in the Linear app |
| Rollback owner | Operator authorized to restore the previous URL |
| Cutover time | Scheduled time with time zone |

### A port in the URL is easy to lose

Funnel does not have to be on 443, and when it is not, the port is part of the
URL. Two things follow.

The provider's webhook field may drop or reject a non-default port. Check by
saving, reloading the settings page, and reading the value back, rather than
trusting the save. A URL that silently lost its port points at whatever else
answers on 443, or at nothing.

Delivery failures are logged only after retries are exhausted, so an empty
failures panel does not mean delivery is working. It can equally mean attempts
are still in flight. Confirm with a durable receipt on the serving host, not
with the absence of a logged failure.

Both cost time during the 2026-08-19 cutover.

The URL is canonical only when `tailscale funnel status --json` contains
exactly one public handler whose proxy is `http://127.0.0.1:<PORT>`. Do not
select the first URL from human-readable Funnel output. `deploy/install.sh`
preflights the JSON status before mutation. An existing unique route to the
exact target is an idempotent success. Any unrelated, unresolved, or ambiguous
public route is left untouched and causes a failure. Only a proven empty public
Funnel state permits enablement. Cleanup is armed before that enable command;
if post-enable status retrieval or parsing fails, the installer disables only
the route it attempted to create. If cleanup also fails, it prints the exact
`tailscale funnel --https=443 off` remediation and still exits nonzero.

## Local service checks

macOS:

- launchd label: `com.linear-agent-bridge`
- launchd plist: `~/Library/LaunchAgents/com.linear-agent-bridge.plist`
- combined stdout/stderr log: `~/Library/Logs/linear-agent-bridge.log`
- runs as the invoking user

Linux:

- systemd unit: `/etc/systemd/system/linear-agent-bridge.service`
- status: `systemctl status linear-agent-bridge`
- log: `journalctl -u linear-agent-bridge`
- runs as the dedicated `linear-agent-bridge` account
- listener: IPv4 loopback only, `127.0.0.1:<PORT>`
- local liveness: `curl -q -fsS http://127.0.0.1:<PORT>/healthz`

Use the filtered check below to confirm the job without rendering its inherited
environment, then use `lsof` to confirm the listener:

```bash
launchctl print gui/$(id -u)/com.linear-agent-bridge \
  | awk '/^[[:space:]]*(path|state|pid) = / { print }'
lsof -nP -iTCP:<PORT> -sTCP:LISTEN
```

Do not run, capture, or paste unfiltered `launchctl print` output, process
environment blocks, or shell tracing. They can expose credentials inherited
from `.env`. If any webhook, OAuth, or client credential is exposed, stop the
cutover, rotate the affected credential at its authority, update `.env`,
restart the launchd job, and repeat local health plus signed public verification
before declaring readiness.

A listener on
`0.0.0.0`, `::`, a Tailscale address, or a LAN address is a stop condition.
The local health check must return exactly `ok`. `deploy/install.sh` exits
nonzero if it cannot prove that result. `SKIP_FUNNEL=1` stops after this local
verification and bypasses Tailscale binary discovery, status reads, and route
enablement.

Durable ingress state is stored at `BRIDGE_STATE_STORE_PATH`, which defaults to
`data/bridge-state.json`. Completed and failed webhook receipts are retained for
seven days and bounded to 10,000 terminal receipts. Active claims are never
evicted. Preserve this file through a cutover and rollback.

Reconciliation runs at startup and every `RECONCILE_INTERVAL_MS` (default
60,000 ms). It discovers sessions within `RECONCILE_LOOKBACK_MS` (default 24
hours), up to `RECONCILE_MAX_SESSIONS` (default 250), while always including
locally known sessions. `RECONCILE_LOOKBACK_MS` also bounds how far back
activities are read. A session's first reconciliation dispatches nothing and
only adopts a watermark, so historical prompts are never replayed as new turns.
`AGENT_SESSION_ACK_GRACE_MS` (default 120,000 ms) controls when an old unclaimed
prompt produces the bounded `stalled_agent_session` warning. That warning is
rate-limited to once per session every 15 minutes.

## Reverify anything checked before 2026-08-19

A pass from a host inside the tailnet used to mean nothing. The verifier
resolved the hostname with the system resolver, so on any tailnet-joined
machine every probe travelled the overlay path: the node answered with a valid
certificate and correct status codes while public ingress was down. The natural
place to run it is the machine you just installed on, which is exactly the
machine that could not test the public path.

If you relied on a verification recorded before this change, run it again. It
did not prove what it appeared to prove.

## Signed public verification

Run the verifier only with the canonical, credential-free HTTPS URL ending in
`/webhook`. It derives the sibling `/healthz` while preserving any legitimate
path prefix.

```bash
WEBHOOK_URL="$(sed -n 's/^WEBHOOK_URL=//p' .env)" \
LINEAR_WEBHOOK_SECRET="$(sed -n 's/^LINEAR_WEBHOOK_SECRET=//p' .env)" \
  ./deploy/verify-ingress.sh
```

Do not use `set -a` or export the whole `.env` file for verification. The
extraction above passes only the public URL and webhook secret to the verifier.
The verifier then runs URL/body generation and all three curl probes with clean
child environments. The HMAC-generation Node child reads the secret and body
from owner-only temporary files; neither value enters its arguments or
environment. Unrelated Linear, OAuth, runtime, and shell credentials are
excluded.

The verifier requires `WEBHOOK_URL` and `LINEAR_WEBHOOK_SECRET`. It performs a
bounded public `GET /healthz`, sends a harmless unsigned non-AgentSession
`POST /webhook` that must return HTTP 401, then sends the same request with a
current-time HMAC-SHA256 signature and requires HTTP 200. This control proves
the route enforces the bridge's authentication boundary before the signed
probe can pass. Neither request can start an agent turn. Curl ignores user curl
configuration, follows no redirects, emits no response body, and uses
five-second connect and 15-second total timeouts. Output is limited to the
sanitized URL, status, and elapsed time. The secret, signature, request body,
and authorization data are not printed or passed in curl arguments.

## Tracing a delivery

Correlate records without copying prompt text:

- `webhookId` identifies the Linear delivery and durable receipt.
- execution identity is `created:<agentSession.id>` for a created session and
  `agentActivity.id` for a prompted session.
- `agentSession.id` ties the receipt, reconciliation scan, queue lifecycle, and
  Linear activity stream together.
- caller-generated activity UUIDs identify retried outbound Linear activities.
- the diagnostic forwarder generates a 16-character connection ID for its
  bounded start, upstream-connected, close, and upstream-failure logs.

In Linear, open Settings, API, Applications, select the agent application, then
inspect its webhook delivery history. Compare the delivery time, HTTP status,
and webhook ID with the launchd log and durable receipt. Delivery history is
edge evidence; the receipt is execution evidence. A 200 delivery alone does
not prove a turn ran.

## Failure matrix

| Signal | Likely boundary | Check | Safe response |
| --- | --- | --- | --- |
| DNS failure | Public hostname | Resolve the exact canonical host from a non-tailnet resolver | Keep the old URL; wait for Funnel DNS or fix the serving host |
| TLS failure | Funnel certificate/public edge | Run the signed verifier and inspect Funnel status JSON | Do not bypass TLS; keep or restore the old URL |
| Forwarder or tunnel failure | Diagnostic private hop only | Correlate the forwarder connection ID, confirm the local SSH tunnel endpoint, and inspect its bounded upstream failure class | Stop both diagnostic processes and restore direct Funnel before cutover |
| HTTP 401 signature | Secret or exact body mismatch | Confirm the Linear app secret and verifier environment without printing either | Correct the secret; never disable HMAC validation |
| Duplicate receipt | Linear delivery retry | Match `webhookId` in `BRIDGE_STATE_STORE_PATH` | Expected deduplication; do not delete the receipt |
| Duplicate activity | Outbound retry or semantic duplicate | Compare activity UUID and execution identity | Preserve state and investigate before replaying |
| Recovery does not dispatch | Post-dispatch ambiguity or stop fence | Inspect receipt disposition, dispatch marker, and reconciliation watermark | Resolve manually; do not force a blind replay |
| Inactivity stop | Runtime produced no progress | Find the session terminal reason and `RUN_INACTIVITY_TIMEOUT_MS` | Diagnose runtime health; ingress replay is not the fix |
| `stalled_agent_session` | Prompt lacks a durable claim after grace | Compare Linear history, watermarks, and receipt store | Fix ingress/reconciliation, then let semantic claims prevent duplication |

## Rollback

Verify the previous URL still answers before relying on it. During the
2026-08-19 cutover the previous host's Funnel still reported itself as on while
its public path failed the TLS handshake, so rolling back to it would have moved
delivery from one broken endpoint to another. A rollback target is only a
rollback target once it has passed the signed public verification above.

Rollback uses the exact previous webhook URL recorded above. Do not invent a
new hostname during an incident.

1. Confirm the previous ingress is serving its health and signed webhook probe.
2. Restore the recorded previous webhook URL in the same Linear application.
3. Confirm a new delivery in Linear history and its durable receipt locally.
4. On the new serving host, disable the root Funnel with
   `tailscale funnel --https=443 off` only after Linear is back on the old URL.
5. Keep `data/bridge-state.json`, session mappings, and OAuth tokens intact.
6. Record the rollback time, owner, delivery ID, and reason.

If the previous ingress cannot pass the signed verifier, stop and repair it.
Do not delete receipts or point Linear at an unverified third route.

## Live cutover checklist

Repository preparation does not perform these live actions. An authorized
operator completes them in order:

- [ ] Record public-edge owner, serving host, canonical URL, previous URL,
  rollback owner, and cutover time.
- [ ] Confirm `com.linear-agent-bridge`, its log path, loopback listener, local
  health, durable state path, and reconciliation settings on the serving host.
- [ ] Build and install the accepted commit with `./deploy/install.sh`.
- [ ] Confirm the installer preflight reported either an empty public Funnel
  state or the single existing exact target, with no unrelated public route.
- [ ] Confirm final Funnel status JSON has exactly one public route to the exact
  loopback target reported by the installer.
- [ ] Run `deploy/verify-ingress.sh` against the reported canonical URL.
- [ ] Save the verified canonical URL in the existing Linear application.
- [ ] Observe Linear delivery history for HTTP 200 and correlate its webhook ID
  to the launchd log and durable receipt.
- [ ] Send one authorized agent-session prompt and confirm one receipt, one
  semantic claim, one activity sequence, and no stalled warning.
- [ ] Confirm reconciliation remains healthy for at least one interval.
- [ ] Remove any diagnostic forwarder from the path.
- [ ] Keep the recorded previous URL and rollback procedure available through
  the observation window.
