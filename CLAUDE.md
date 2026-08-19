# linear-agent-bridge

Bridge between Linear's Agent Interaction API and a local agent runtime
(Claude Agent SDK by default). Mention or assign the agent in Linear; it
answers in the issue's agent-session thread with the full context of the
working directory it runs in (CLAUDE.md stack, MCP servers).

## Architecture

Linear webhook (AgentSessionEvent) plus startup/interval activity
reconciliation -> src/server.ts -> JsonBridgeStateStore -> SerialQueue ->
AgentRuntime (src/runtime/claude.ts, cwd=KB_PATH) -> activities back via
src/linear/client.ts (agentActivityCreate). Durable receipts, semantic claims,
per-session watermarks, and stop fences prevent duplicate or post-stop dispatch
across delivery and process retries. A claim can transfer after restart only
until its durable dispatch marker is set; later cross-process retries remain
ambiguous and undispatched.
Session mapping persists in JsonSessionStore so `prompted` events
resume the same runtime session.
LinearOAuthTokenManager persists Linear's rotating OAuth token pair and
refreshes it after an authenticated request returns 401.

## Hard constraints

- **Serial execution only.** Parallel headless Claude invocations sharing
  one host have produced cross-session content contamination. SerialQueue
  stays at concurrency 1.
- **Never pass `model` or tool overrides** when invoking the Claude
  runtime. The operator's Claude Code config is the source of truth.
  Unattended runs use `permissionMode: "bypassPermissions"` paired with
  `allowDangerouslySkipPermissions: true` (the SDK requires the pair).
- **Auth is subscription-based.** No ANTHROPIC_API_KEY anywhere; the Agent
  SDK resolves Claude Code's stored credentials.
- The runtime runs with cwd=KB_PATH; that directory's own CLAUDE.md rules
  apply inside runtime sessions automatically.
- Linear timing rules: ack webhooks < 5s; emit a first activity < 10s on
  `created`. Persist the bounded receipt and semantic claim before ack; do all
  external work after. Mark dispatch durably before the first external or
  runtime side effect.
- Tool results must close their matching Linear action. Stop signals abort
  active and queued turns for that session. `RUN_INACTIVITY_TIMEOUT_MS` bounds
  runtime silence, not total wall-clock duration; raw runtime progress resets
  the watchdog without rendering in Linear. A runtime `done` event ends the
  turn immediately and never resets the watchdog.
- Reconciliation's first sighting of a session dispatches nothing, because
  everything already in Linear predates the bridge knowing about it. The one
  exception is a session Linear created after the durable `watchingSince`
  marker, inside `RECONCILE_LOOKBACK_MS`, older than `AGENT_SESSION_ACK_GRACE_MS`,
  and carrying no `created:<sessionId>` claim: that is a lost `created`
  webhook, and its opening prompt is dispatched through the normal path. The
  grace period is what stops recovery racing a webhook still in flight, and the
  claim check is required because the two paths key their claims differently.
  `watchingSince` is written once and never rewritten.
- Every state mutation is owed one lock acquire attempt, including the owner
  probe that reclaims an abandoned lock, even when its budget is already spent.
  Directory sync and the owner write can consume a small budget on a loaded
  host; refusing to try there fails a mutation that would have succeeded and
  leaves a stale lock in place. That attempt has its own bounded floor rather
  than the caller's timeout, and winning the rename always runs the operation:
  discarding a held lock to honour an expired deadline helps no other caller.
  Later retries check the deadline as before.
- Linear OAuth access tokens expire after 24 hours. Persist both replacement
  tokens atomically after every authorization and refresh. Never log either
  token.
- OAuth callbacks consume a random, expiring, one-time `state` issued only in
  the local service log. Never accept a bare authorization code.
- Linear payload facts (verified against live payloads 2026-08-12): the
  prompted user text is `agentActivity.content.body`; AgentSessionEvent
  fields sit at the payload top level; prompted ordering uses the required
  `agentActivity.createdAt`; the HMAC covers the raw body and `webhookTimestamp`
  rides inside the JSON.

## Gates (every commit)

typecheck 0 errors, tests 100% pass, TDD: test-file changes land before or
with implementation. No `--no-verify`.

## Verification

    npm run typecheck && npm test
