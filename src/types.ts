// Core domain types for the Linear <-> agent-runtime bridge.

interface LinearAgentSessionEventBase {
  /** Linear's unique delivery id, used for durable receipt deduplication. */
  webhookId: string;
  agentSession: {
    id: string;
    issue?: { id: string; identifier: string; title: string } | undefined;
    comment?: { id: string; body: string } | undefined;
  };
  /** Formatted context string on `created` (issue details, comments, guidance). */
  promptContext?: string | undefined;
  previousComments?: unknown;
  guidance?: string | undefined;
}

/** Subset of Linear's AgentSessionEvent webhook payload we consume. */
export type LinearAgentSessionEvent =
  | (LinearAgentSessionEventBase & {
      action: "created";
      agentActivity?: undefined;
    })
  | (LinearAgentSessionEventBase & {
      action: "prompted";
      /**
       * Linear's activity id is the prompted turn's semantic execution id.
       * User text lives in the typed content union; a bare body remains a
       * compatibility fallback for older payloads.
       */
      agentActivity: {
        id: string;
        body?: string | undefined;
        content?: { type?: string; body?: string; signal?: string } | undefined;
        signal?: string | undefined;
      };
    });

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
  /** Cancels the active runtime turn for a Linear stop signal or inactivity. */
  abortController?: AbortController | undefined;
}

/** Events a runtime yields while working a session. Progress never renders. */
export type RuntimeEvent =
  | { kind: "progress" }
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
  /**
   * Optional synchronous hard-stop control. Returning means the runtime has
   * invoked its underlying resource closer; implementations must be
   * idempotent. The server uses this before releasing a timed-out queue slot.
   */
  forceCloseSession?(request: SessionRequest): void;
}

export class NotImplementedError extends Error {
  constructor(ticket: string) {
    super(`Not implemented — tracked as ${ticket}`);
    this.name = "NotImplementedError";
  }
}
