# linear-agent-bridge

Bridge between Linear's Agent Interaction API and a local agent runtime.
`RUNTIME` selects the Claude Agent SDK or Codex SDK (`claude` by default).
Mention or assign the agent in Linear; it answers in the issue's agent-session
thread with the selected runtime's full working-directory context.

## Architecture

Linear webhook (AgentSessionEvent) plus startup/interval activity
reconciliation -> src/server.ts -> JsonBridgeStateStore -> SessionLanes ->
AgentRuntime (src/runtime/{claude,codex}.ts, cwd=KB_PATH) -> activities back via
src/linear/client.ts (agentActivityCreate). Durable receipts, semantic claims,
per-session watermarks, and stop fences prevent duplicate or post-stop dispatch
across delivery and process retries. A claim can transfer after restart only
until its durable dispatch marker is set; later cross-process retries remain
ambiguous and undispatched.
Session mapping persists in JsonSessionStore so `prompted` events
resume the same runtime session.
When `AUTONOMOUS_GOAL_LABEL_ID` is configured, JsonBridgeStateStore also owns a
provider-neutral autonomous-goal lifecycle. The current issue label is the
native authorization guard; provider sessions supply conversation continuity,
not scheduling authority.
LinearOAuthTokenManager persists Linear's rotating OAuth token pair and
refreshes it after an authenticated request returns 401.

## Hard constraints

- **Runtime concurrency is per Linear agent-session.** Turns in one Linear
  agent-session run FIFO and resume that session's provider-native session.
  Distinct Linear sessions run concurrently because both adapters isolate
  conversations by provider-native session id. The host-wide concurrency-1
  rule is retired:
  MPI-682 imported it from a 2026-04-26 `claude -p` file-write incident, which
  never applied to these SDKs' session-isolated conversations.
- **Never pass model, reasoning-effort, or tool overrides.** The selected
  service account's Claude Code or Codex configuration is the source of truth.
  Claude unattended runs pair `permissionMode: "bypassPermissions"` with
  `allowDangerouslySkipPermissions: true`; Codex uses approval policy `never`
  and sandbox mode `danger-full-access`.
- **Auth is subscription-based.** Neither adapter uses an API key. Each SDK
  resolves the selected service account's stored Claude Code or Codex login.
- The runtime runs with cwd=KB_PATH, where it loads its normal instruction,
  skill, and MCP configuration stack.
- Every runtime prompt states that the agent is already inside a Linear Agent
  Session and that the bridge posts its final response automatically. Linear
  tools remain available for deliberate issue mutations, not for duplicating
  conversation updates or the final response as comments.
- **Do not infer delegation from `AgentSessionEvent.created`.** Linear uses the
  same action for mention and delegation and provides no causal discriminator.
  Autonomous work is opt-in through the configured visible issue label, which
  must be checked before each provider turn and again before completion. Unset
  configuration preserves one turn per message.
- Autonomous scheduling is bridge-owned and provider-neutral. Persist each
  state transition before its next side effect, keep every continuation in the
  existing per-session FIFO lane, and never run more than
  `AUTONOMOUS_GOAL_MAX_STEPS` between human messages. Yield before another
  continuation when guidance is already durably claimed, even if its callback
  has not entered the in-memory queue. A blocked goal emits one elicitation and
  schedules nothing until a new prompt arrives.
- A runtime's completion claim is necessary but insufficient. Require its
  nonempty verification summary, recheck the label and issue state, durably
  enter `completing`, then set the issue's completed workflow state. Reconcile
  the stable completion activity ID before emission so crash recovery cannot
  duplicate the final response. Cross a second durable dispatch boundary after
  the label read and before `issueUpdate`; a stop that won before that boundary
  must prevent the mutation.
- A restart may resume a goal only between bounded provider turns. A goal left
  `running` by another process has unknown side effects and must become blocked
  durably before reconciliation can dispatch downtime guidance. Emit that
  restart elicitation before processing the guidance; never replay the
  interrupted turn. A provider mismatch is also blocked; provider-native
  session IDs never cross adapters. Reconcile a recoverable goal's own Agent
  Session before dispatching accepted ingress or enqueuing recovery so stops
  and guidance sent during downtime run first. A failed goal-session preflight
  keeps startup unready; it must not fall through to accepted-ingress dispatch.
- Persist the opening objective and every exact pending elicitation or
  completion response in recovery-key AES-GCM envelopes. Reconcile stable
  activity IDs after restart and decrypt notice content only when emission is
  still required; never store this user-visible text in plaintext or replace
  it with generic crash-recovery wording.
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
- **Per-delivery identity is the `Linear-Delivery` header, never `webhookId`.**
  Linear defines `webhookId` as "ID uniquely identifying this webhook", the
  configuration, so it repeats on every delivery that webhook sends. The header
  is "a UUID (v4) that uniquely identifies this payload". Keying a durable
  receipt on `webhookId` lets the first delivery take the slot and rejects every
  later one as a conflicting replay, which is exactly what happened once ingress
  started working on 2026-08-19. Verified against live deliveries the same day.
- A queued session and a lost one look identical from Linear for the first
  minute. The discriminator is the liveness activity: a session the bridge
  accepted posts one within seconds, and a session whose webhook never arrived
  posts nothing ever. Check for it before concluding a message was lost.
- `LINEAR_ACCESS_TOKEN` is a bootstrap value only. Once the OAuth callback has
  run, the rotating pair in `OAUTH_TOKEN_STORE_PATH` supersedes it and the env
  value is never used again. It stays required so the service can boot before
  first authorization, so a stale one is expected rather than a defect; keep it
  an obvious placeholder so nobody debugs against it.
- The comment that opens an AgentSession never becomes an activity on it. Its
  text arrives only in the `created` webhook payload, and a session whose
  `created` webhook was lost reports zero activities indefinitely. Reconciliation
  can therefore detect a lost opening but can never replay it; there is nothing
  to read. Do not design recovery that assumes otherwise.

## Gates (every commit)

typecheck 0 errors, tests 100% pass, TDD: test-file changes land before or
with implementation. No `--no-verify`.

## Verification

    npm run typecheck && npm test
