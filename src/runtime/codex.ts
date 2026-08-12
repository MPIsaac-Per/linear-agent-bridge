import {
  NotImplementedError,
  type AgentRuntime,
  type RuntimeEvent,
  type SessionRequest,
} from "../types.js";

/**
 * Codex SDK runtime (@openai/codex-sdk) — the priced exit hatch if
 * Anthropic un-pauses the Agent SDK subscription-credit change.
 * Thread start/resume maps onto SessionRequest.resumeSessionId via
 * `startThread()` / `resumeThread(threadId)`. Not wired up yet; selecting
 * RUNTIME=codex fails loudly rather than silently degrading.
 */
export class CodexRuntime implements AgentRuntime {
  readonly name = "codex";

  // eslint-disable-next-line require-yield
  async *runSession(_request: SessionRequest): AsyncIterable<RuntimeEvent> {
    throw new NotImplementedError("codex-runtime ticket (backlog, unscheduled)");
  }
}
