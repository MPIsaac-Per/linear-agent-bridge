import type {
  AgentRuntime,
  AgentActivityContent,
  RuntimeEvent,
  SessionRequest,
} from "../types.js";
import type { Options, SDKMessage } from "@anthropic-ai/claude-agent-sdk";

/** Max length of a compact, one-line tool-input summary before truncation. */
const MAX_ACTION_PARAMETER_LENGTH = 200;
/** Keep completed tool cards useful without dumping large or sensitive outputs into Linear. */
const MAX_ACTION_RESULT_LENGTH = 500;

interface PendingToolUse {
  action: string;
  parameter: string;
}

/**
 * Injectable shape of the SDK's `query()` function. The real one is
 * lazy-imported (see `defaultQuery` below) so importing this module never
 * loads the Claude Code CLI; tests inject their own stub and never touch it.
 */
export type QueryFn = (params: {
  prompt: string;
  options?: Options;
}) => AsyncIterable<SDKMessage>;

/**
 * Default `QueryFn`: an async generator function. Calling it returns the
 * generator object synchronously without running any body code, so the
 * dynamic `import()` — and therefore the CLI — only loads on first
 * iteration (i.e. when a real session actually runs).
 */
async function* defaultQuery(params: {
  prompt: string;
  options?: Options;
}): AsyncGenerator<SDKMessage, void> {
  const { query } = await import("@anthropic-ai/claude-agent-sdk");
  yield* query(params);
}

/** Compact one-line JSON summary of a tool call's input, truncated if long. */
function summarizeToolInput(input: unknown): string {
  const json = JSON.stringify(input) ?? "";
  if (json.length <= MAX_ACTION_PARAMETER_LENGTH) {
    return json;
  }
  return `${json.slice(0, MAX_ACTION_PARAMETER_LENGTH - 3)}...`;
}

/** Compact a tool_result content block into a bounded result for Linear. */
function summarizeToolResult(content: unknown, isError: boolean): string {
  if (!isError) {
    return "Completed.";
  }

  let text: string;
  if (typeof content === "string") {
    text = content;
  } else if (Array.isArray(content)) {
    const parts = content.flatMap((block: unknown) => {
      if (block !== null && typeof block === "object") {
        const record = block as Record<string, unknown>;
        if (record.type === "text" && typeof record.text === "string") {
          return [record.text];
        }
        if (record.type === "image") {
          return ["Image returned."];
        }
      }
      return [];
    });
    text = parts.join("\n");
  } else {
    text = JSON.stringify(content) ?? "";
  }

  const compact = text.replace(/\s+/g, " ").trim();
  const prefixed = compact !== "" ? `Failed: ${compact}` : "Tool failed.";
  if (prefixed.length <= MAX_ACTION_RESULT_LENGTH) {
    return prefixed;
  }
  return `${prefixed.slice(0, MAX_ACTION_RESULT_LENGTH - 3)}...`;
}

/**
 * Claude Agent SDK runtime.
 *
 * Implementation contract (verified against SDK docs and the installed
 * @anthropic-ai/claude-agent-sdk type declarations, 2026-08-12):
 * - `query({ prompt, options })` from @anthropic-ai/claude-agent-sdk.
 * - `options.cwd = config.kbPath` — running in your knowledge base (or any
 *   project) auto-loads its CLAUDE.md stack and your user/project-scope
 *   MCP servers via `settingSources: ["user", "project"]`.
 * - `options.resume = request.resumeSessionId` continues a prior session;
 *   capture the new session id from the init system message and yield it
 *   as `session-started`.
 * - NEVER pass `model` or override allowed tools — operator config is the
 *   source of truth (standing rule). Unattended runs use
 *   `permissionMode: "bypassPermissions"` (paired with
 *   `allowDangerouslySkipPermissions: true`, which the SDK's own Options
 *   type requires to actually take effect).
 * - Auth is Claude Code subscription credentials; no ANTHROPIC_API_KEY.
 *
 * Activity mapping: interim assistant text -> thought; tool use -> action;
 * final result -> response; errors (stream throw, or a result message with
 * a non-success subtype) -> error.
 */
export class ClaudeRuntime implements AgentRuntime {
  readonly name = "claude";

  constructor(
    private readonly kbPath: string,
    private readonly queryFn: QueryFn = defaultQuery,
  ) {}

  async *runSession(request: SessionRequest): AsyncIterable<RuntimeEvent> {
    const options: Options = {
      cwd: this.kbPath,
      settingSources: ["user", "project"],
      permissionMode: "bypassPermissions",
      allowDangerouslySkipPermissions: true,
      stderr: (data: string) => {
        console.error(`[claude-cli] ${data.trimEnd()}`);
      },
      ...(request.abortController !== undefined
        ? { abortController: request.abortController }
        : {}),
      ...(request.resumeSessionId !== undefined
        ? { resume: request.resumeSessionId }
        : {}),
    };

    // The final assistant text and the result message usually carry the same
    // body; forwarding both renders duplicated text in Linear. Hold each
    // thought back one step and drop it if the result repeats it.
    let pendingThought: RuntimeEvent | undefined;
    const pendingToolUses = new Map<string, PendingToolUse>();
    try {
      for await (const message of this.queryFn({ prompt: request.prompt, options })) {
        if (request.abortController?.signal.aborted === true) {
          break;
        }
        for (const ev of this.mapMessage(message, pendingToolUses)) {
          const isDuplicateResponse =
            ev.kind === "activity" &&
            ev.activity.type === "response" &&
            pendingThought?.kind === "activity" &&
            pendingThought.activity.type === "thought" &&
            pendingThought.activity.body === ev.activity.body;
          if (isDuplicateResponse) {
            pendingThought = undefined;
          } else if (pendingThought !== undefined) {
            yield pendingThought;
            pendingThought = undefined;
          }
          if (ev.kind === "activity" && ev.activity.type === "thought") {
            pendingThought = ev;
          } else {
            yield ev;
          }
        }
      }
      if (request.abortController?.signal.aborted === true) {
        yield { kind: "done" };
        return;
      }
      if (pendingThought !== undefined) {
        yield pendingThought;
      }
      yield { kind: "done" };
    } catch (err) {
      if (request.abortController?.signal.aborted === true) {
        yield { kind: "done" };
        return;
      }
      if (pendingThought !== undefined) {
        yield pendingThought;
      }
      const body = err instanceof Error ? err.message : String(err);
      yield { kind: "activity", activity: { type: "error", body } };
      throw err;
    }
  }

  private *mapMessage(
    message: SDKMessage,
    pendingToolUses: Map<string, PendingToolUse>,
  ): Generator<RuntimeEvent> {
    if (message.type === "system") {
      if (message.subtype === "init") {
        yield { kind: "session-started", runtimeSessionId: message.session_id };
      }
      return;
    }

    if (message.type === "assistant") {
      for (const block of message.message.content) {
        if (block.type === "text") {
          if (block.text.trim() !== "") {
            yield { kind: "activity", activity: { type: "thought", body: block.text } };
          }
        } else if (block.type === "tool_use") {
          const activity: AgentActivityContent = {
            type: "action",
            action: block.name,
            parameter: summarizeToolInput(block.input),
          };
          pendingToolUses.set(block.id, {
            action: activity.action,
            parameter: activity.parameter,
          });
          yield { kind: "activity", activity };
        }
      }
      return;
    }

    if (message.type === "user" && Array.isArray(message.message.content)) {
      for (const block of message.message.content) {
        if (block.type !== "tool_result") {
          continue;
        }
        const toolUse = pendingToolUses.get(block.tool_use_id);
        if (toolUse === undefined) {
          continue;
        }
        pendingToolUses.delete(block.tool_use_id);
        yield {
          kind: "activity",
          activity: {
            type: "action",
            action: toolUse.action,
            parameter: toolUse.parameter,
            result: summarizeToolResult(block.content, block.is_error === true),
          },
        };
      }
      return;
    }

    if (message.type === "result") {
      if (message.subtype === "success") {
        yield { kind: "activity", activity: { type: "response", body: message.result } };
      } else {
        yield {
          kind: "activity",
          activity: { type: "error", body: message.errors.join("; ") },
        };
      }
    }
  }
}
