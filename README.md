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
  -> webhook: AgentSessionEvent (created | prompted)
  -> src/server.ts        verify HMAC, persist ingress, ack < 5s
  -> src/state/store.ts   durable receipt + semantic execution claim
  -> src/queue.ts         serial execution, concurrency 1
  -> src/runtime/claude.ts  Agent SDK query(), cwd = KB_PATH, resume
  -> src/linear/client.ts   agentActivityCreate (thought/action/response)
```

Session mapping (Linear session id -> SDK session id), bounded webhook state,
and Linear's rotating OAuth token pair persist in JSON files. Follow-up prompts
resume the same conversation, webhook retries do not dispatch the same turn
twice, pre-intent work survives a process exit, and access refreshes without
another browser authorization.

For each valid agent event, the bridge durably persists a receipt keyed by
Linear's `webhookId` and a semantic execution claim before returning 200.
Created turns use `created:<agentSession.id>` as the execution identity;
prompted turns use `agentActivity.id`. Before acknowledging a new turn, the
bridge stores its recovery payload in an AES-256-GCM envelope. The envelope
remains through the Linear liveness activity, serial-queue wait, and durable
runtime-start intent written immediately before runtime invocation. Failures
before that intent release the claim for ordered replay without another Linear
delivery. A stop or shutdown that wins before invocation rolls the intent back.

A replacement process replays accepted work with no runtime-start intent. An
intent-bearing receipt is kept as conservative `AmbiguousDispatch` state and is
not invoked again because the replacement cannot prove whether runtime work
started. This irreversible runtime-start window is the remaining ambiguous
execution boundary. Recovery ciphertext is removed only when the receipt
becomes terminal or is superseded.

Retained recovery events, including intent-bearing receipts, are hard-capped at
128. Normal ingress leaves one slot reserved for stop. At capacity, a
same-session stop can supersede older pre-intent work to enter; other new
ingress fails closed with 503. Separately, terminal state is retained for seven
days and capped at 10,000 receipts. Active claims are never evicted, so the
receipt store can exceed 10,000 only when active claims alone exceed that limit.

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
- A public HTTPS route to the service (tailscale funnel, cloudflared, or
  any reverse proxy).

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

OAuth tokens are adopted only after the rotating pair is written and synced;
a failed persistence attempt keeps the pair pending and retries it. Graceful
SIGINT or SIGTERM shutdown waits within a bounded window for an in-flight
refresh and flushes a pending replacement pair before process exit. OAuth,
runtime-session mappings, and bridge state use owner-only temporary files,
file sync, same-directory atomic rename, and directory sync. A missing store
starts empty where allowed. Malformed or unreadable durable state fails closed
instead of being treated as empty.

`BRIDGE_STATE_STORE_PATH` defaults to `data/bridge-state.json`. Keep it on
persistent local storage as well. It contains bounded identifiers, status
timestamps, intended HTTP/result/disposition metadata, static error classes,
caller-generated activity UUIDs, a bounded pre-intent activity outbox, and
recovery ciphertext for accepted turns that have not reached terminal state.
The plaintext outbox records the activity UUID, session ID, bounded static
renderer version, content digest, attempt count, delivery status, and
timestamps, but not the activity content. Plaintext recovery routing metadata
includes the action,
session/webhook/execution IDs, recovery sequence, event timestamp, envelope
`keyId`, runtime-start intent timestamp, and stop-fence provenance. Prompt,
issue, and comment text, the raw signal, and the stop/body semantics remain
inside the encrypted envelope until terminalization or supersession.
Ciphertext length still reveals an approximate prompt length, so protect the
state file and its backups as sensitive data.

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
webhooks. Envelopes disappear when their receipts become terminal or are
superseded; intent-bearing receipts retain their envelopes.
The following conservative check prints `0` when no envelope needs any
previous key. Substitute your configured state path if it differs:

```bash
node -e 'const s=require("./data/bridge-state.json"); console.log(Object.values(s.receipts??{}).filter(r=>r.recoveryEnvelope).length)'
```

Remove retired keys from `INGRESS_RECOVERY_PREVIOUS_KEYS` and run the installer
again only after that check prints `0`. Losing a key while one of its
retained envelopes remains makes recovery unavailable and keeps the service
unhealthy.

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

On macOS, `./deploy/install.sh` builds the service, installs a launchd
user agent (so the SDK sees your Claude Code credentials), opens a
tailscale funnel, and prints the webhook URL to paste into the app config.
It validates the complete application configuration before stopping an
existing service. Health is polled for up to ten seconds after restart. A
failed health check exits nonzero and restores the prior build and launchd
files; the message reports whether the restored service answered its health
probe. Secrets are loaded from `.env`, not passed in command arguments.

Any other HTTPS ingress works; the service only needs POST /webhook
reachable. If your TLS terminator runs on a different host,
`deploy/tcp_forward.py` is a dependency-free TCP forwarder to bridge the
last hop over a private network.

### 5. Talk to it

Delegate any issue to the agent, or message it in the session thread.
First thought lands within seconds; answers take as long as a real agent
session takes.

## Field notes (things the docs won't tell you)

- The `prompted` webhook carries the user's text at
  `agentActivity.content.body`, not `agentActivity.body`. Reading the
  wrong field yields an agent that answers "Standing by." to everything,
  because it is resumed with an empty prompt.
- AgentSessionEvent payloads put their fields at the top level of the
  webhook body (no `data` wrapper, unlike data-change webhooks).
- `webhookId` identifies a Linear delivery. The created semantic execution id
  is `created:<agentSession.id>`; prompted execution is identified by
  `agentActivity.id`. Linear activity creation receives a caller-generated UUID
  persisted before the request, so an OAuth retry reuses the same id.
- Before replaying an uncertain pre-intent activity, the bridge queries its
  exact caller UUID and verifies the owning session up to four times over a
  bounded convergence window. If it remains absent, creation is retried with
  the same UUID. Linear's [agent interaction guide](https://linear.app/developers/agent-interaction)
  and [published SDL](https://github.com/linear/linear/blob/master/packages/sdk/src/schema.graphql)
  expose caller IDs and activity lookup, but specify neither query-visibility
  delay nor create idempotency for a repeated UUID. A delayed query can
  therefore lead to a duplicate or create error. The bridge accepts that
  availability tradeoff rather than leave accepted liveness or stop output
  permanently pending.
- A restart recovers accepted pre-intent work by decrypting its bounded
  envelope. The runtime-start intent is written only after liveness and queue
  work, immediately before runtime invocation. A different process treats an
  intent-bearing receipt as `AmbiguousDispatch`; same-process stop or shutdown
  before invocation confirms the intent and rolls it back when durability can
  be established. If final confirmation remains uncertain, the receipt stays
  conservatively ambiguous. Same-process duplicate deliveries remain ordinary
  duplicates.
- Pre-runtime liveness and stop outboxes persist a static renderer version.
  Keep prior renderers when changing this copy so pending work can reconcile
  its original caller UUID and content binding across an upgrade. Legacy
  digest-only entries and caller UUIDs are migrated conservatively and queried
  by exact activity ID before replay.
- Session mapping writes abort before entering atomic rename. Shutdown waits a
  bounded second for a rename already in flight to finish and sync. A timeout
  rejects close as incomplete, and production signal handling exits nonzero
  instead of claiming a fully drained shutdown.
- Session mapping mutations are serialized. An inactive turn gives an in-flight
  mapping write a bounded handoff window; queued follow-ups then use a bounded
  read barrier, so they resume the durable runtime ID or remain recoverable
  without starting a fresh runtime session.
- The HMAC-SHA256 signature (`linear-signature` header) covers the raw
  body; the replay-protection timestamp is `webhookTimestamp` inside the
  JSON, milliseconds, reject beyond 60s skew.
- Persist the receipt, semantic claim, and recovery envelope, then ack the
  webhook within the 5s limit. Persist each activity UUID and outbox entry
  before its outbound request. Emit a thought immediately on `created` (10s
  liveness limit), or Linear marks the session unresponsive.
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
- `permissionMode: "bypassPermissions"` does nothing without
  `allowDangerouslySkipPermissions: true`; the SDK requires the pair.
- Old Agent SDK versions (0.1.x) fail to resume sessions whose transcript
  contains empty text blocks (API 400: "text content blocks must be
  non-empty"). Use 0.3.x or later.
- Linear OAuth access tokens expire after 24 hours. Every refresh returns a
  replacement access token and refresh token; persist the pair atomically and
  retry one failed 401 request with the replacement access token.

## Security notes

The webhook endpoint verifies signatures, rejects stale timestamps, and does
not return 200 for a valid agent event unless its receipt and claim are durable.
Accepted turns retain encrypted prompt material through pre-intent delivery,
queueing, and runtime-start intent until terminalization or supersession.
Bounded action, identity, sequence, timestamp, `keyId`, runtime-start intent,
and stop-fence provenance remain plaintext for routing. Prompt, issue, and
comment text, raw signal, and stop/body semantics remain encrypted, while
ciphertext size leaks an approximate length. The envelope authenticates its
payload and routing association, not the whole state file. Keep `.env`, the
state file, and their backups owner-only; losing every reader key for a retained
envelope blocks startup rather than dropping accepted work.
The OAuth callback consumes a random, expiring state value before exchanging an
authorization code; only the local service log receives the matching setup
URL. `/healthz` and `/oauth/callback` are the only other routes. Understand
what you are wiring up: anyone who can mention the agent in your Linear
workspace steers an unattended agent session running with permissions
bypassed in `KB_PATH`. Use it in workspaces you trust, and scope `KB_PATH`
deliberately.

## Billing note

The Claude Agent SDK currently draws on Claude Code subscription
credentials and standard plan limits. Anthropic announced, then paused, a
change that would move Agent SDK usage to a separate metered credit pool.
Check current terms before depending on the economics.

## License

MIT
