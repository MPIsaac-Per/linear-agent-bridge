# linear-agent-bridge

Run your Claude Code setup as a Linear agent. Delegate an issue to it or
message it in an agent session, and a Claude Agent SDK session runs on
your machine, in a working directory you choose, with everything that
directory carries: CLAUDE.md instructions, MCP servers, skills. Replies
land in the issue's agent-session thread.

This is a compact reference implementation (no framework, tested). It is not
a coding agent; for assign-an-issue-get-a-PR flows,
see [Cyrus](https://github.com/ceedaragents/cyrus). This bridge is for
talking to an agent that knows your context: a knowledge base, an ops
repo, a project directory.

Runs on Claude Code subscription auth. No Anthropic API key.

## Architecture

```
Linear (mention / delegate / follow-up prompt)
  -> webhook + reconciliation: AgentSessionEvent / AgentSession activities
  -> src/server.ts        verify HMAC, persist ingress, ack < 5s
  -> src/state/store.ts   durable receipt + semantic execution claim
  -> src/queue.ts         serial execution, concurrency 1
  -> src/runtime/claude.ts  Agent SDK query(), cwd = KB_PATH, resume
  -> src/linear/client.ts   agentActivityCreate (thought/action/response)
```

Session mapping (Linear session id -> SDK session id), bounded webhook state,
and Linear's rotating OAuth token pair persist in JSON files. Follow-up prompts
resume the same conversation, webhook retries do not dispatch the same turn
twice, accepted work survives a process exit before dispatch, missed prompt and
stop webhooks recover through reconciliation, and access
refreshes without another browser authorization.

For each valid agent event, the bridge durably persists a receipt keyed by
Linear's `webhookId` and a semantic execution claim before returning 200.
Created turns use `created:<agentSession.id>` as the execution identity;
prompted turns use `agentActivity.id`. A claim left active by a process crash is
reclaimed by a replacement process only when dispatch never started. The bridge
persists a dispatch marker as the first processing step; a retry after that
marker is explicitly recorded as ambiguous and is not run automatically.
Before acknowledging a new turn, the bridge stores its recovery payload in an
AES-256-GCM envelope. Startup processes marker-free accepted turns in durable
order before reporting healthy. If persisting the dispatch marker fails before
it is written, the bridge releases the pre-dispatch claim and schedules the
same recovery path without requiring another Linear delivery. The envelope is
deleted atomically with the dispatch marker; terminal and superseded receipts
do not retain it.
Marker-free accepted events are hard-capped at 128; new ingress fails closed
with 503 when that recovery capacity is full. Separately, terminal state is
retained for seven days and capped at 10,000 receipts. Active claims are never
evicted, so the receipt store can exceed 10,000 only when active claims alone
exceed that limit.

In-progress thoughts and tool calls use Linear's ephemeral activity UI. Tool
results close the matching action, `stop` cancels the active and queued turns
for that session, and `RUN_INACTIVITY_TIMEOUT_MS` stops a run only when its
runtime has been silent for the configured interval (5 minutes by default).
Completed assistant text is posted as a durable response as soon as Claude
marks the turn `end_turn`; an identical trailing SDK result is suppressed,
while a differing result is forwarded.

## Prerequisites

- Node 22+, a machine that stays on, and [Claude Code](https://claude.com/claude-code)
  installed and logged in as the user who runs the service.
- On macOS, Xcode Command Line Tools (`xcode-select --install`). The build uses
  the supported libproc API to compile a small local process-identity helper.
- A Linear workspace where you can create OAuth applications.
- A public HTTPS route to the loopback-bound service. The supported macOS
  topology uses Tailscale Funnel directly on the bridge host.

## Setup

### 1. Create the Linear OAuth app

Linear Settings -> API -> Applications -> new application:

- Name it (this is the agent's visible name), fill developer fields.
- Redirect URI: `http://localhost:3979/oauth/callback`
- Toggle **Webhooks** on. Note the pre-generated signing secret.
- Webhook URL: your public HTTPS host + `/webhook` (can be corrected later).
- Under **App events**, check **Agent session events**. There is no scopes
  picker on the app; scopes are requested at install time.

### 2. Configure and run

```bash
umask 077
npm install
cp .env.example .env
chmod 600 .env
node -e 'console.log(require("node:crypto").randomBytes(32).toString("base64url"))'
# fill LINEAR_CLIENT_ID, LINEAR_CLIENT_SECRET, LINEAR_WEBHOOK_SECRET
# set LINEAR_ACCESS_TOKEN=pending (placeholder until step 3)
# paste the generated value into INGRESS_RECOVERY_KEY
# set KB_PATH to the directory whose context the agent should carry
npm run dev
```

`INGRESS_RECOVERY_KEY` is required and must be canonical, unpadded base64url
for exactly 32 random bytes. Keep `.env` out of version control. The macOS
installer repairs its mode to `0600` and refuses a symlink or a file owned by
another user.

The `dev`, `test`, `build`, and `start` scripts build the macOS helper when its
source, target architecture, or compile flags change. The native step is
skipped on other platforms. A required macOS rebuild fails closed with an
installation command when Command Line Tools are unavailable.

### 3. Install the app as an agent (actor=app)

Start the service, then open the `OAuth authorization URL` printed in its
console or launchd log. The URL contains a random, one-time `state` value
and is valid for 10 minutes. Restart the service to issue another URL if it
expires.

The callback stores Linear's access and refresh tokens in
`data/oauth-tokens.json`. The app now appears as an assignable agent in your
workspace. Access tokens expire after 24 hours; the bridge consumes the
stored refresh token, saves Linear's replacement pair, and retries the failed
request automatically.

`LINEAR_ACCESS_TOKEN` is a bootstrap value. After authorization, the token
store takes precedence across process and machine restarts. Keep the token
store on persistent local storage. Its file mode is `0600`, and `data/` is
excluded from Git.

`BRIDGE_STATE_STORE_PATH` defaults to `data/bridge-state.json`. Keep it on
persistent local storage as well. It contains bounded identifiers, status
timestamps, intended HTTP/result/disposition metadata, static error classes,
caller-generated activity UUIDs, reconciliation watermarks, stop fences, and
recovery ciphertext for accepted turns whose dispatch marker is absent.
Plaintext recovery routing metadata includes the action,
session/webhook/execution IDs, recovery sequence, event timestamp, envelope
`keyId`, and stop-fence provenance. Prompt, issue, and comment text, the raw
signal, and the stop/body semantics remain inside the encrypted envelope until
the dispatch marker is committed. Ciphertext length still reveals an
approximate prompt length, so protect the state file and its backups as
sensitive data.

AES-256-GCM authenticates each encrypted payload against its routing identity,
sequence, and `keyId`. It does not authenticate the state file as a whole.
Owner-only file permissions and host integrity remain part of the trust
boundary. Writes sync an owner-only temporary file before a same-directory
atomic rename, then sync the containing directory.

A process-owned lock bounds contention to one second so the webhook can still
return 503 before Linear's five-second deadline. An old lock is retained while
its recorded local process is alive. Lock ownership includes a boot-scoped
process birth identity, so a recycled PID cannot preserve or steal an old
lock. Linux combines the kernel boot ID with `/proc/<pid>/stat` start ticks.
macOS combines the boot-session UUID with the microsecond process start time
returned by a locally compiled libproc helper at
`dist/native/process_identity`. The helper receives only a numeric PID, emits
only `seconds:microseconds`, and runs within the same one-second lock budget.
On the same boot, the recorded numeric user ID is checked before process birth
so an inaccessible PID recycled across users can be reclaimed safely. An
exited process's lock can be reclaimed without allowing the prior owner to
unlink a replacement lock.

#### Rotate the ingress recovery key

Generate a replacement with the same command used during setup. Edit `.env`
so the new key is `INGRESS_RECOVERY_KEY` and the former primary is the first
entry in `INGRESS_RECOVERY_PREVIOUS_KEYS`. Retain any older reader keys after
it, separated by commas with no spaces; at most four previous keys are allowed.
Then run:

```bash
chmod 600 .env
./deploy/install.sh
```

The restart loads the new writer and all retained readers before accepting
webhooks. Envelopes normally disappear when the dispatch marker is committed.
The following conservative check prints `0` when no envelope needs any
previous key. Substitute your configured state path if it differs:

```bash
node -e 'const s=require("./data/bridge-state.json"); console.log(Object.values(s.receipts??{}).filter(r=>r.recoveryEnvelope).length)'
```

Remove retired keys from `INGRESS_RECOVERY_PREVIOUS_KEYS` and run the installer
again only after that check prints `0`. Losing a key while one of its
marker-free envelopes remains makes recovery unavailable and keeps the service
unhealthy.

Reconciliation runs once at startup and every `RECONCILE_INTERVAL_MS` (default
`60000`). Each scan includes every locally known Linear session plus at most
`RECONCILE_MAX_SESSIONS` (default `250`) app-owned sessions updated within
`RECONCILE_LOOKBACK_MS` (default `86400000`, 24 hours). That same window bounds
how far back session activities are read, and reads resume from a durable
per-session watermark.

The first time reconciliation sees a session it dispatches nothing. It adopts
the newest activity it observes as the watermark and picks up genuinely new
prompts from the next scan onward. A session already in Linear predates the
bridge watching it, so its history is not missed work; without this, a first
run would replay every prompt in the window as a fresh turn. `AGENT_SESSION_ACK_GRACE_MS` (default `120000`) controls when an old,
unclaimed prompt produces a bounded `stalled_agent_session` diagnostic;
repeats are limited to once per session every 15 minutes.

#### Upgrading an existing installation

Installations created before refresh-token support have only an access token
in `.env`. Pull the new version, rebuild and restart the service, then open the
authorization URL printed at startup once. The callback will create the token store. No
further browser authorization is needed during normal token rotation.

If Linear returns `401` and the bridge reports that no refresh token is
stored, repeat the authorization step. Removing the app from Linear, revoking
its grant, or deleting the token store also requires authorization again.

Releases that predate recovery envelopes may have a `received` or `claimed`
receipt with no `dispatchStartedAt`, `recoverySequence`, or
`recoveryEnvelope`. Its original prompt was deliberately not stored. During
startup the bridge stays unhealthy and accepts only a fresh, correctly signed
Linear redelivery whose webhook, execution, session, and action identity match
that true legacy receipt. The matching delivery attaches an encrypted envelope
durably, then startup recovery runs the turn and opens normal webhook traffic.
Invalid signatures, unrelated events, and nonmatching deliveries cannot repair
the receipt.

Before upgrading, let accepted work drain and inspect the state file. This
read-only command labels true legacy state as `awaiting-redelivery` and an
asymmetric sequence/envelope pair as `invalid`:

```bash
node -e 'const s=require("./data/bridge-state.json"); for(const r of Object.values(s.receipts??{})){if(!["received","claimed"].includes(r.status)||r.dispatchStartedAt)continue; const q=r.recoverySequence!==undefined,e=r.recoveryEnvelope!==undefined; if(!q||!e)console.log(!q&&!e?"awaiting-redelivery":"invalid",r.webhookId,r.linearSessionId,r.executionId)}'
```

The macOS installer polls health for a bounded window. If Linear does not
redeliver during that window, the installer restores the previous build. Back
up the state file, reconcile the affected Linear session manually, and remove
or archive that stale receipt and its matching claim before retrying. An
asymmetric pair, corrupted envelope, or missing reader key always fails closed
and never enters redelivery repair. Do not discard the whole state file without
reviewing the deduplication and activity IDs it contains.

### 4. Expose the webhook

`./deploy/install.sh` runs on macOS and Linux. It detects the platform and
branches only where the service manager differs; the transaction around it is
identical on both. It builds the service, installs it under launchd or systemd,
verifies
`http://127.0.0.1:<PORT>/healthz`, opens a Tailscale Funnel directly to that
loopback listener, verifies the exact route in Funnel status JSON, and prints
the canonical webhook URL to paste into the app config. It validates the
complete application configuration before stopping an existing service. Health
is polled for up to ten seconds after restart. A failed health check exits
nonzero and restores the prior build and service definition; the message reports
whether the restored service answered its health probe. Before mutation it
accepts only an empty public Funnel state or the single existing exact target;
unrelated or ambiguous public routes are left untouched. It fails closed if
local health, Funnel setup, or route discovery fails. `SKIP_FUNNEL=1` is an
explicit local-only installation mode. It completes the build, service setup,
and loopback health check without discovering, inspecting, or changing
Tailscale state. Secrets are loaded from `.env`, not passed in command
arguments.

The two platforms differ in three places, deliberately.

**Where the service definition lives.** macOS renders a launchd user agent at
`~/Library/LaunchAgents/com.linear-agent-bridge.plist`. Linux renders a systemd
unit at `/etc/systemd/system/linear-agent-bridge.service`; override the
directory with `SYSTEMD_UNIT_DIR`.

**Which account runs it.** macOS runs the service as you, in your login session,
so the Agent SDK finds your Claude Code credentials with no extra setup. Linux
runs it as a dedicated system account, `linear-agent-bridge` by default and
configurable with `SERVICE_USER`, created by the installer if absent. That
account needs its own authenticated Claude Code login, and it is the boundary
`AGENT_OUTPUT_PATH` relies on. macOS is the exception here on purpose: a
dedicated account there means a system LaunchDaemon, sudo on every install, and
a second login with no Keychain access, which buys little on a workstation.
Because Linux manages a system account and a unit, the installer requires root
there and says so rather than failing part-way.

**Where the logs go.** macOS writes a combined log to
`~/Library/Logs/linear-agent-bridge.log`. Linux writes to the journal; read it
with `journalctl -u linear-agent-bridge`. Rollback messages name whichever
applies.

On Linux the installer also sets `.env` to 0640 owned by you with the service
group, since the service account has to read it. That happens only after the
configuration validates, so a failed install neither creates an account nor
widens a secret.

Set `WEBHOOK_URL` to the printed credential-free HTTPS URL, keep
`LINEAR_WEBHOOK_SECRET` in the environment, and run
`./deploy/verify-ingress.sh` before changing Linear. The verifier probes public
`GET /healthz`, requires an unsigned harmless `POST /webhook` to return 401,
then requires the same correctly signed non-AgentSession request to return 200.
It does not print the secret, signature, or body.

Every probe is forced onto an address returned by a public resolver rather than
the system one, and the report names the address each probe used. This matters
on any machine joined to the same overlay network as the service, which is
usually the machine you just installed on. There, the system resolver returns
the node's private address, the node terminates TLS locally with a valid
certificate, and all three probes pass while public ingress is down. Set
`VERIFY_INGRESS_RESOLVERS` to override the resolvers, comma-separated; the
default is `1.1.1.1,8.8.8.8`, two of them so one provider outage does not read
as a broken ingress. When the system and public answers differ, the split is
reported first, because that difference is the whole problem. When no
configured resolver answers, the verifier exits nonzero and says the public
path was not tested rather than quietly falling back and reporting success.

A second host on a different network is a stronger check still, since it
exercises the path a real client takes end to end. That is worth doing by hand
where you have one; it is deliberately not built into the script.

The HTTP server binds only to `127.0.0.1`. `deploy/tcp_forward.py` is retained
only as a bounded, loopback-bound diagnostic for a private last hop. On a
separate ingress host, first establish an SSH local tunnel to the bridge host's
`127.0.0.1:<PORT>` listener, then point the forwarder at that local tunnel
endpoint. The forwarder rejects a bridge private address as its upstream. It
is not a supported production ingress. See
[the ingress cutover runbook](docs/ingress-cutover.md) for ownership fields,
tracing, failure diagnosis, the live checklist, and rollback.

### 5. Talk to it

Delegate any issue to the agent, or message it in the session thread.
First thought lands within seconds; answers take as long as a real agent
session takes.

## Field notes (things the docs won't tell you)

- The `prompted` webhook carries the user's text at
  `agentActivity.content.body`, not `agentActivity.body`. Reading the
  wrong field yields an agent that answers "Standing by." to everything,
  because it is resumed with an empty prompt.
- Prompted activity ordering uses `agentActivity.createdAt` and
  `agentActivity.id`. The bridge rejects prompted payloads without a valid
  activity timestamp rather than substituting webhook delivery time.
- AgentSessionEvent payloads put their fields at the top level of the
  webhook body (no `data` wrapper, unlike data-change webhooks).
- `webhookId` identifies a Linear delivery. The created semantic execution id
  is `created:<agentSession.id>`; prompted execution is identified by
  `agentActivity.id`. Linear activity creation receives a caller-generated UUID
  persisted before the request, so an OAuth retry reuses the same id.
- A restart recovers an accepted claim whose dispatch marker is absent by
  decrypting its bounded recovery envelope. Once the marker exists, a
  different process records `AmbiguousDispatch` and does not execute the turn
  again. Same-process duplicate deliveries remain ordinary duplicates.
- On startup and every reconciliation interval, the bridge paginates recent
  sessions for the authenticated app user and all locally known sessions, then
  paginates typed Agent Activities to each durable watermark. Activities are
  processed by `createdAt`, with `id` as the deterministic tie-breaker. A
  fetched stop is claimed together with its durable fence before any older
  unseen prompt can dispatch; prompts after the fence may resume normally.
  Synthetic reconciliation delivery IDs are deterministic while
  `agentActivity.id` remains the semantic execution identity, so a late real
  webhook converges on the same claim.
- The HMAC-SHA256 signature (`linear-signature` header) covers the raw
  body; the replay-protection timestamp is `webhookTimestamp` inside the
  JSON, milliseconds, reject beyond 60s skew.
- Persist the receipt and semantic claim, then ack the webhook within the 5s
  limit. Do external work after the ack and emit a thought immediately on
  `created` (10s liveness limit), or Linear marks the session unresponsive.
- Keep runtime execution serial. Concurrent headless Claude sessions on
  one host have produced cross-session content contamination.
- Claude Agent SDK tool results arrive as `user` messages containing
  `tool_result` blocks. Pair them to the preceding `tool_use` ID so Linear
  receives a completed action instead of a permanent spinner.
- Forward Linear's `stop` signal through an `AbortController`. A plain `stop`
  prompt is accepted as a fallback. `RUN_INACTIVITY_TIMEOUT_MS` (default:
  `300000`) starts when a queued run begins executing and resets on raw runtime
  progress, session start, or activity output. Queue wait, webhook handling,
  and Linear delivery do not extend it. The runtime `done` marker ends the turn
  immediately without resetting the watchdog, and later iterator events are
  ignored. There is no total wall-clock cap while the runtime remains active.
  Cancellation closes the SDK query process handle exactly once. Before
  inactivity releases the global serial queue, the server invokes the
  runtime's synchronous force-close control; an uncooperative iterator still
  cannot retain the queue.
  Turn-scoped Linear activity requests receive the same abort signal, late
  events are ignored, and inactivity is reported once on a best-effort basis.
- `RUN_TIMEOUT_MS` remains a deprecated fallback for one release. The bridge
  logs one bounded warning whenever the legacy variable is present;
  `RUN_INACTIVITY_TIMEOUT_MS` takes precedence when both are present.
- Turn lifecycle logs contain bounded operational fields: session id and queue
  size at start, then session id, terminal reason, and remaining queue size at
  completion. Prompt and issue contents are not included.
- Invalid JSON and invalid agent-event diagnostics are static classes. Ingress
  failures log bounded error classes rather than raw errors, and Linear HTTP or
  GraphQL response bodies are not copied into thrown errors or logs.
- Reconciliation failures are isolated per scan and session, never block the
  HTTP listener or webhook path, and do not include prompt text or raw Linear
  response bodies. Shutdown cancels an in-flight reconciliation read and
  clears the interval timer.
- `permissionMode: "bypassPermissions"` does nothing without
  `allowDangerouslySkipPermissions: true`; the SDK requires the pair.
- Old Agent SDK versions (0.1.x) fail to resume sessions whose transcript
  contains empty text blocks (API 400: "text content blocks must be
  non-empty"). Use 0.3.x or later.
- Linear OAuth access tokens expire after 24 hours. Every refresh returns a
  replacement access token and refresh token; persist the pair atomically and
  retry one failed 401 request with the replacement access token.

## Security notes

The loopback-only webhook endpoint verifies signatures, rejects stale timestamps, and does
not return 200 for a valid agent event unless its receipt and claim are durable.
Marker-free accepted turns retain encrypted prompt material until the dispatch
marker is durable. Bounded action, identity, sequence, timestamp, `keyId`, and
stop-fence provenance remain plaintext for routing. Prompt, issue, and comment
text, raw signal, and stop/body semantics remain encrypted, while ciphertext
size leaks an approximate length. The envelope authenticates its payload and
routing association, not the whole state file. Keep `.env`, the state file, and
their backups owner-only; losing every reader key for an active envelope blocks
startup rather than dropping accepted work.
The OAuth callback consumes a random, expiring state value before exchanging an
authorization code; only the local service log receives the matching setup
URL. `/healthz` and `/oauth/callback` are the only other routes. Understand
what you are wiring up: anyone who can mention the agent in your Linear
workspace steers an unattended agent session running with permissions
bypassed in `KB_PATH`. Use it in workspaces you trust, and scope `KB_PATH`
deliberately.

### Confining what the agent can write

A webhook-driven agent cannot answer a permission prompt, so unattended runs
pass `permissionMode: "bypassPermissions"`. That is not changing. It means no
prompt stands between the agent and the working directory, and for many
operators `KB_PATH` is a knowledge base synced to other machines, where a
mistaken write is recoverable by diff and a mistaken delete often is not.

`AGENT_OUTPUT_PATH` makes a read-only working directory a supported posture. It
is optional; unset, nothing changes. Set it and the runtime names the directory
in its prompt so the agent knows where artifacts go instead of discovering the
boundary by hitting `EACCES`.

**The filesystem is the enforcement, not the setting.** Anything enforced inside
the agent is advisory: a tool call, a subprocess, or a bug walks through it.
Surfacing the path is for usability and nothing else. Three tiers:

| Path | Access for the service account |
| --- | --- |
| Working directory content | read-only |
| Agent tooling state inside the working directory | writable |
| `AGENT_OUTPUT_PATH` | writable |

The middle tier exists because agent tooling writes state inside the working
directory, not only content. Verify the exact set against your installed SDK
version rather than trusting a list that will age.

To set it up: run the service as a dedicated account, not your login account
and **not root**, since running as root defeats the entire model. Grant that
account read on the working tree and write on the output path and the tooling
state path. On Linux the installer creates that account for you; see the
deployment section above.

The trade is real and worth stating plainly. The agent can no longer edit an
existing file, fix a typo, or maintain an index in place. Every change becomes
a new artifact in the output path for a human to review and merge. If you want
an agent that maintains its working tree, leave `AGENT_OUTPUT_PATH` unset.

On the sync interaction: confining writes to one directory reduces the conflict
surface to that directory, and it only conflicts if a human edits the same
directory at the same time. A denied write is the agent's problem, not the
service's. The bridge does not crash, does not retry, and reports a bounded
error class.
## Shutdown

The service handles `SIGINT` and `SIGTERM` and calls `server.close()` exactly
once. That path is what sets `closing`, aborts the shutdown and reconciliation
controllers, clears the reconciliation timer, waits for the queue boundary, and
lets in-flight dispatch markers settle. It is routine rather than exceptional:
the installer stops and restarts the service on every run.

`SHUTDOWN_TIMEOUT_MS` bounds the close, defaulting to 10000. Ten seconds sits
inside launchd's 20-second `SIGKILL` window; systemd's default
`TimeoutStopSec` is far longer, so the same value is safe there. When the
deadline elapses the process logs a bounded diagnostic and exits 1 rather than
hanging. A second signal during shutdown does not start a second close and does
not shorten the deadline.

## Billing note

The Claude Agent SDK currently draws on Claude Code subscription
credentials and standard plan limits. Anthropic announced, then paused, a
change that would move Agent SDK usage to a separate metered credit pool.
Check current terms before depending on the economics.

## License

MIT
