// Core domain types for the Linear <-> agent-runtime bridge.

/** Subset of Linear's AgentSessionEvent webhook payload we consume. */
export interface LinearAgentSessionEvent {
  action: "created" | "prompted";
  agentSession: {
    id: string;
    issue?: { id: string; identifier: string; title: string } | undefined;
    comment?: { id: string; body: string } | undefined;
  };
  /** Formatted context string on `created` (issue details, comments, guidance). */
  promptContext?: string | undefined;
  /**
   * Follow-up user prompt on `prompted`. Verified against live payloads
   * 2026-08-12: the text lives in the typed content union at
   * `agentActivity.content.body`; a bare `body` is kept as fallback.
   */
  agentActivity?:
    | {
        body?: string | undefined;
        content?: { type?: string; body?: string; signal?: string } | undefined;
        signal?: string | undefined;
      }
    | undefined;
  previousComments?: unknown;
  guidance?: string | undefined;
}

/** Activity types Linear renders in the agent session thread. */
export type AgentActivityContent =
  | { type: "thought"; body: string }
  | { type: "response"; body: string }
  | { type: "error"; body: string }
  | { type: "elicitation"; body: string }
  | { type: "action"; action: string; parameter: string; result?: string };

/** One unit of work handed to a runtime. */
export interface SessionRequest {
  /** Linear agent session id — the stable key across follow-up prompts. */
  linearSessionId: string;
  /** Prompt text (promptContext on created, agentActivity.body on prompted). */
  prompt: string;
  /** Runtime session id from a prior turn, when resuming. */
  resumeSessionId?: string | undefined;
  /** Cancels the active runtime turn for a Linear stop signal or deadline. */
  abortController?: AbortController | undefined;
}

/** Events a runtime yields while working a session. */
export type RuntimeEvent =
  | { kind: "session-started"; runtimeSessionId: string }
  | { kind: "activity"; activity: AgentActivityContent }
  | { kind: "done" };

/**
 * The runtime seam. ClaudeRuntime is the default; CodexRuntime is the
 * priced exit if Anthropic un-pauses the Agent SDK credit pool.
 */
export interface AgentRuntime {
  readonly name: string;
  runSession(request: SessionRequest): AsyncIterable<RuntimeEvent>;
}

export class NotImplementedError extends Error {
  constructor(ticket: string) {
    super(`Not implemented — tracked as ${ticket}`);
    this.name = "NotImplementedError";
  }
}
