# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Changes awaiting a tagged release remain under Unreleased.

## [Unreleased]

### Added

- Persist bounded webhook receipts, semantic execution claims, and
  caller-generated Linear activity UUIDs before acknowledging valid agent
  events. Delivery retries deduplicate by `webhookId`; created and prompted
  turns claim `created:<agentSession.id>` and `agentActivity.id` respectively.
  Terminal entries expire after seven days and are capped at 10,000 while
  active claims are preserved.
- Retain recovery through the liveness activity and serial queue, then persist
  runtime-start intent immediately before invocation. Pre-intent failures
  release for ordered replay. A stop or shutdown that wins before invocation
  rolls intent back; a replacement process treats retained intent as
  `AmbiguousDispatch` because runtime execution can no longer be disproved.
- Encrypt the bounded recovery payload for each accepted turn with
  a required AES-256-GCM ingress key. Startup replays recoverable work before
  becoming healthy, and terminalization or supersession removes the envelope.
  Bounded routing metadata remains plaintext: action, session/webhook/execution
  IDs, recovery sequence, event timestamp, envelope `keyId`, and stop-fence
  provenance. Prompt, issue, and comment text, raw signal, and stop/body
  semantics remain encrypted. The envelope authenticates its payload and
  routing association, not the state file as a whole.
- Hard-cap retained recovery events at 128, including intent-bearing receipts.
  Normal ingress reserves one slot for stop; a same-session stop can supersede
  older pre-intent work at capacity, while other ingress fails closed with 503.
  The separate receipt-retention cap remains 10,000 entries while active claims
  remain non-evictable.
- Persist a content-bound pre-intent activity outbox before delivery. An
  uncertain replay queries the exact caller UUID and session over a bounded
  convergence window, then retries create with the same UUID if still absent.
  Linear documents caller IDs and lookup but gives no query-visibility or
  repeated-create idempotency guarantee, leaving a disclosed duplicate or
  create-error risk in exchange for availability. Outbox state retains bounded
  IDs, a static renderer version, content digest, attempt count, delivery
  status, and timestamps without storing activity content. Digest-only outbox
  records and legacy caller UUIDs migrate through retained renderers and exact
  ID reconciliation.
- Serialize durable runtime-session mappings and gate queued follow-ups behind
  a bounded write/read handoff. A stuck prior rename releases the global queue
  without letting the follow-up start a fresh runtime session.
- Add reader-first recovery-key rotation through up to four retained previous
  keys. The primary key writes new envelopes while the retained keyring reads
  outstanding envelopes from earlier deployments.

### Changed

- Rename the project from `linear-claude-bridge` to `linear-agent-bridge` to
  reflect its runtime-agnostic architecture. The macOS installer removes the
  legacy launchd job during upgrade so only one bridge process remains active.
- Make the macOS installer validate configuration and recovery keys before any
  launchd mutation, repair `.env` to mode `0600`, poll health with a bounded
  deadline, and restore the prior build and launchd files when restart fails.
- Harden OAuth token, runtime-session, and bridge-state writes with owner-only
  synced temporary files, atomic replacement, directory sync, and fail-closed
  reads. Rotated OAuth tokens are not adopted before persistence succeeds.
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
