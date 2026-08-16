# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The project has not published a tagged release, so merged changes remain under
Unreleased.

## [Unreleased]

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
