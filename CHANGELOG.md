# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Changes awaiting a tagged release remain under Unreleased.

## [Unreleased]

### Added

- Linux deployment. `deploy/install.sh` detects the platform and branches only
  at the service-manager boundary, rendering a systemd unit from the new
  `deploy/systemd.service.template` alongside the existing launchd plist. The
  transaction is unchanged on both: configuration preflight before any service
  mutation, previous build and service definition preserved and restored on
  failure, bounded health polling, and the same Funnel preflight. On Linux the
  service runs as a dedicated `linear-agent-bridge` account created by the
  installer, logs to the journal, and requires root; macOS keeps the per-user
  launchd agent. `deploy/install.test.sh` runs every case against both fakes,
  rollback included.

### Fixed

- Guarantee one lock acquire attempt per state mutation. The retry loop checked
  its deadline before attempting anything, and the directory sync and owner
  write ahead of it failed the same way, so a mutation whose budget was spent
  during setup gave up without trying the lock and without ever probing the
  owner of a stale one. `mutate` had the same refuse-before-attempt gate: it
  checked the clock when its turn came up, so a loaded host could time out
  without calling `withFileLock` at all. On a loaded host that failed
  mutations that would have succeeded and left abandoned locks unreclaimed,
  which is what made the bridge state lock tests fail intermittently on CI.
  The guaranteed attempt has its own bounded floor, and winning the rename now
  runs the operation instead of discarding a lock nobody else could use.
  Work the admission timer already rejected while queued still does not run.
- Force `deploy/verify-ingress.sh` onto the public path. It resolved the webhook
  hostname with the system resolver, so on a host joined to the same overlay
  network as the service every probe travelled the private path: the node
  terminated TLS locally with a valid certificate and returned correct status
  codes while public ingress was down, and the script exited 0. It now resolves
  through configured public resolvers, connects to those addresses explicitly,
  probes every address in both families, names the address each probe used, and
  reports a split between system and public resolution as the headline
  diagnostic. With no reachable resolver it exits nonzero and states that the
  public path was not tested rather than falling back to the system resolver.
  Configure with `VERIFY_INGRESS_RESOLVERS`, default `1.1.1.1,8.8.8.8`.

- Stop reconciliation replaying a session's history on its first sighting. A
  session already in Linear predates the bridge watching it, so nothing in the
  window is missed work. The first pass now adopts the newest observed activity
  as the watermark and dispatches nothing. Previously the first reconciliation
  of every session dispatched every prompt in the window as a fresh turn.
- Bound the activity read with `RECONCILE_LOOKBACK_MS`. The window was a
  hardcoded seven days, so the setting only gated which sessions were scanned
  and appeared to control a window it did not.

### Added

- Add a deterministic signed public-ingress verifier, strict Funnel status
  route parsing, deployment seam tests, and an operator cutover/rollback
  runbook for the direct loopback-to-Funnel topology.
- Add bounded lifecycle tracing, connection and idle timeouts, and half-close
  cancellation to the diagnostic-only TCP forwarder.

- Reconcile Linear AgentSession activities at startup and every minute so
  missed prompt and stop webhooks recover across process restarts. Scans cover
  every locally known session plus up to 250 recent sessions owned by the
  authenticated app user, paginate activities to durable watermarks with a
  seven-day bound, and share webhook semantic claims and outbound idempotency.
- Persist stop fences before cancellation or acknowledgement so a later stop
  suppresses older unseen prompts while newer prompts can resume. Emit bounded
  `stalled_agent_session` diagnostics after the configurable acknowledgement
  grace period, rate-limited once per session every 15 minutes.
- Add `RECONCILE_INTERVAL_MS`, `RECONCILE_LOOKBACK_MS`,
  `RECONCILE_MAX_SESSIONS`, and `AGENT_SESSION_ACK_GRACE_MS` configuration.

- Persist bounded webhook receipts, semantic execution claims, and
  caller-generated Linear activity UUIDs before acknowledging valid agent
  events. Delivery retries deduplicate by `webhookId`; created and prompted
  turns claim `created:<agentSession.id>` and `agentActivity.id` respectively.
  Terminal entries expire after seven days and are capped at 10,000 while
  active claims are preserved.
- Mark dispatch durably before any external or runtime side effect. A replacement
  process safely reclaims a claim when dispatch never began; retries after the
  marker persist an `AmbiguousDispatch` outcome and remain undispatched.
- Encrypt the bounded recovery payload for each marker-free accepted turn with
  a required AES-256-GCM ingress key. Startup replays recoverable work before
  becoming healthy, and dispatch-marker persistence removes the envelope.
  Bounded routing metadata remains plaintext: action, session/webhook/execution
  IDs, recovery sequence, event timestamp, envelope `keyId`, and stop-fence
  provenance. Prompt, issue, and comment text, raw signal, and stop/body
  semantics remain encrypted. The envelope authenticates its payload and
  routing association, not the state file as a whole.
- Hard-cap marker-free accepted recovery events at 128. New ingress fails
  closed with 503 at capacity; the separate receipt-retention cap remains
  10,000 entries while active claims remain non-evictable.
- Add reader-first recovery-key rotation through up to four retained previous
  keys. The primary key writes new envelopes while the retained keyring reads
  outstanding envelopes from earlier deployments.

### Changed

- Bind the bridge HTTP listener explicitly to `127.0.0.1`. The macOS installer
  now fails on local health or ingress setup errors, targets Funnel directly at
  the loopback listener, and reports only the exact matching public webhook
  route.
- Extend CI with deployment syntax checks, diagnostic forwarder tests, and an
  explicit MPI-1448 lost-prompt/later-stop regression gate before the complete
  npm suite.

- Rename the project from `linear-claude-bridge` to `linear-agent-bridge` to
  reflect its runtime-agnostic architecture. The macOS installer removes the
  legacy launchd job during upgrade so only one bridge process remains active.
- Make the macOS installer validate configuration and recovery keys before any
  launchd mutation, repair `.env` to mode `0600`, poll health with a bounded
  deadline, and restore the prior build and launchd files when restart fails.
- Publish non-empty Claude assistant text immediately when an `end_turn`
  message arrives, suppress an identical trailing success result, and retain a
  differing result as a second durable response.
- Replace the wall-clock turn deadline with a runtime inactivity watchdog set
  by `RUN_INACTIVITY_TIMEOUT_MS` (five minutes by default). Raw Claude SDK
  messages keep active runs alive without rendering in Linear; queue wait,
  webhook work, and outbound delivery do not reset the watchdog. The runtime
  `done` marker ends the turn immediately without accepting later iterator
  events. Inactivity force-closes the runtime, aborts in-flight delivery,
  ignores late events, releases the serial queue, and reports the stop once.
  The deprecated `RUN_TIMEOUT_MS` remains a fallback for one release and emits
  one bounded warning whenever present, with the new variable taking precedence.
- Log bounded turn lifecycle records with session id, terminal reason, and
  queue size without including prompt or issue content.
- Return a non-200 response when ingress state cannot be persisted, retain
  durable received/claimed/completed/failed/superseded lifecycle metadata, and
  leave ambiguous claims from another process undispatched for manual review.
- Keep startup unhealthy when an older marker-free receipt lacks both recovery
  fields, while allowing only an exact signed matching Linear redelivery to
  attach its encrypted envelope and resume recovery. Asymmetric recovery state,
  corrupted envelopes, missing reader keys, invalid signatures, and unrelated
  events remain fail closed. Recovery ciphertext reveals approximate prompt
  length but never stores prompt, issue, comment, signal, or body semantics in
  plaintext.
- Remove prompted activity serialization from empty-body diagnostics so logs
  contain only bounded identifiers and status metadata.
- Persist bounded HTTP status, result, disposition, and error-class outcome
  metadata for valid receipts. Invalid JSON and invalid agent events emit only
  static diagnostics, ingress failures log only static classes, and Linear
  response bodies are no longer echoed into errors.

### Fixed

- Prevent a missed continuation webhook from starting after a later stop when
  the bridge restarts and recovers both activities through reconciliation
  (MPI-1448).

- Preserve and close the Claude Agent SDK `Query.close()` handle exactly once
  when stop, inactivity, or shutdown aborts an active turn, including through
  the server's explicit force-close control.

## [0.1.0] - 2026-08-16

### Added

- Persist Linear's rotating OAuth access and refresh token pair, refresh it
  automatically after an authenticated request returns `401`, and retry the
  request without another browser authorization. The OAuth callback now
  validates a random, one-time state value and writes the token store with
  owner-only permissions. ([#2](https://github.com/MPIsaac-Per/linear-claude-bridge/pull/2))
- Use ephemeral Linear activities for in-progress thoughts and tool calls,
  then close tool actions when the Claude Agent SDK returns their matching
  results. ([#3](https://github.com/MPIsaac-Per/linear-claude-bridge/pull/3))
- Cancel active, queued, and still-preparing turns when Linear sends a `stop`
  signal. A plain `stop` prompt remains available as a compatibility fallback.
  ([#3](https://github.com/MPIsaac-Per/linear-claude-bridge/pull/3))
- Bound each agent turn with `RUN_TIMEOUT_MS`, which defaults to five minutes.
  ([#3](https://github.com/MPIsaac-Per/linear-claude-bridge/pull/3))

### Fixed

- Resume follow-up prompts from `agentActivity.content.body`, matching Linear's
  webhook payload shape. ([#3](https://github.com/MPIsaac-Per/linear-claude-bridge/pull/3))
- Read stop metadata from `agentActivity.content.signal` and register
  cancellation before asynchronous follow-up setup, preventing work from being
  queued after a stop response. ([#3](https://github.com/MPIsaac-Per/linear-claude-bridge/pull/3))

[Unreleased]: https://github.com/MPIsaac-Per/linear-agent-bridge/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/MPIsaac-Per/linear-agent-bridge/releases/tag/v0.1.0
