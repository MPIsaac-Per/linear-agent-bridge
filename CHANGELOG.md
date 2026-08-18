# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Changes awaiting a tagged release remain under Unreleased.

## [Unreleased]

### Changed

- Rename the project from `linear-claude-bridge` to `linear-agent-bridge` to
  reflect its runtime-agnostic architecture. The macOS installer removes the
  legacy launchd job during upgrade so only one bridge process remains active.
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
