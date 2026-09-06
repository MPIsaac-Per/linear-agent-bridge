# Linear agent bridge

## Working agreement

Finish the authorized task through verification and integration. Preserve existing edits and unresolved requests across interruptions. Make routine implementation decisions locally; ask only when a missing decision materially changes the outcome. Existing authorization persists across turns.

Use tools available in the current session. Skills provide task guidance; they do not impose unrelated workflows or authorize external actions. Keep runtime model selection in the harness. Commits, pushes, publishing, messages, credential changes, and destructive operations need authorization covering the action. Do not add agent or model attribution.

Run checks appropriate to the change and required repository gates. Broaden or repeat checks for new changes, failures, or unresolved concerns. Report the result, verification, and actual limitations concisely.

## Repository context and verification

`src/server.ts` verifies and durably records webhook ingress; `src/state/` owns persistence, `src/queue.ts` owns per-session ordering, and `src/runtime/` owns runtime adapters. Read the architecture and hard constraints in `CLAUDE.md` for changes to dispatch, recovery, timing, or authentication. They describe the product runtime; they do not select this maintenance session's model or permission mode.

Preserve delivery identity, durable claims before acknowledgement, dispatch fences, per-session FIFO order, and atomic OAuth token persistence. Use synthetic payloads and temporary stores. Do not start the bridge, use the configured knowledge base, post Linear activities, or change OAuth credentials for routine checks.

Before commits, run `npm run typecheck` and `npm test`. Keep tests deterministic; never substitute live sessions for fixtures. Installation changes also need `npm run test:install`. Do not bypass hooks.
