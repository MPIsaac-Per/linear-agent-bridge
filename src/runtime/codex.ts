import type {
  Codex,
  ThreadEvent,
  ThreadItem,
  ThreadOptions,
} from "@openai/codex-sdk";
import type {
  AgentRuntime,
  RuntimeEvent,
  SessionRequest,
} from "../types.js";
import { withLinearAgentSessionContext } from "./prompt.js";

/** Keep Linear action cards compact and avoid preserving raw tool output. */
const MAX_ACTION_PARAMETER_LENGTH = 200;

/**
 * The small portion of the Codex SDK used by this adapter. Exported so tests
 * can exercise the event protocol without spawning the Codex CLI.
 */
export interface CodexThread {
  runStreamed(
    input: string,
    options?: { signal?: AbortSignal },
  ): Promise<{ events: AsyncIterable<ThreadEvent> }>;
}

export interface CodexClient {
  startThread(options?: ThreadOptions): CodexThread;
  resumeThread(id: string, options?: ThreadOptions): CodexThread;
}

export type CodexClientFactory = () => Promise<CodexClient>;

/** Lazy loading keeps ordinary bridge/test startup independent of the CLI. */
const defaultCodexClient: CodexClientFactory = async () => {
  const { Codex: SdkCodex } = await import("@openai/codex-sdk");
  return new SdkCodex() as Codex;
};

function compact(value: string): string {
  const oneLine = value.replace(/\s+/g, " ").trim();
  return oneLine.length <= MAX_ACTION_PARAMETER_LENGTH
    ? oneLine
    : `${oneLine.slice(0, MAX_ACTION_PARAMETER_LENGTH - 3)}...`;
}

function summarizeArguments(arguments_: unknown): string {
  try {
    return compact(JSON.stringify(arguments_) ?? "");
  } catch {
    return "[unserializable arguments]";
  }
}

function fileChangeParameter(item: Extract<ThreadItem, { type: "file_change" }>): string {
  return compact(item.changes.map((change) => `${change.kind}: ${change.path}`).join(", "));
}

function commandResult(item: Extract<ThreadItem, { type: "command_execution" }>): string {
  if (item.status === "failed") {
    return item.exit_code === undefined
      ? "Failed."
      : `Failed (exit ${item.exit_code}).`;
  }
  return item.exit_code === undefined ? "Completed." : `Completed (exit ${item.exit_code}).`;
}

function isSafeFatalError(error: unknown): error is Error {
  return (
    error instanceof Error &&
    [
      "Codex turn completed without a final response.",
      "Codex turn failed.",
      "Codex runtime error.",
      "Codex stream ended before turn completion.",
      "Codex did not provide a session id.",
      "Codex did not resume the saved session.",
    ].includes(error.message)
  );
}

/**
 * Codex SDK runtime.
 *
 * Codex inherits the service account's normal ChatGPT/Codex login and
 * ~/.codex/config.toml. The adapter intentionally omits `model` and
 * `modelReasoningEffort`; the operator's default model and effort remain
 * authoritative. It does, however, make the bridge's unattended-execution
 * contract explicit: run in KB_PATH, do not prompt for approval, and use the
 * same unrestricted workspace posture as Claude's bypassPermissions mode.
 */
export class CodexRuntime implements AgentRuntime {
  readonly name = "codex";
  private readonly activeControllers = new WeakMap<SessionRequest, AbortController>();

  constructor(
    private readonly kbPath = process.cwd(),
    private readonly createClient: CodexClientFactory = defaultCodexClient,
    private readonly agentOutputPath?: string,
  ) {}

  private composePrompt(prompt: string): string {
    const contextualPrompt = withLinearAgentSessionContext(prompt);
    if (this.agentOutputPath === undefined) {
      return contextualPrompt;
    }
    return `${contextualPrompt}\n\nWrite any files you produce to ${this.agentOutputPath}. The working directory may be read-only to this service, so a denied write there is expected rather than something to work around.`;
  }

  forceCloseSession(request: SessionRequest): void {
    const controller = this.activeControllers.get(request);
    if (controller !== undefined && !controller.signal.aborted) {
      controller.abort(new Error("Codex session stopped"));
    }
  }

  async *runSession(request: SessionRequest): AsyncIterable<RuntimeEvent> {
    if (request.abortController?.signal.aborted === true) {
      yield { kind: "done" };
      return;
    }

    const closeController = new AbortController();
    const signal =
      request.abortController === undefined
        ? closeController.signal
        : AbortSignal.any([request.abortController.signal, closeController.signal]);
    this.activeControllers.set(request, closeController);

    // Do not set `model` or `modelReasoningEffort` here. Both must come from
    // the service account's Codex configuration, just like Claude's defaults.
    const threadOptions: ThreadOptions = {
      workingDirectory: this.kbPath,
      skipGitRepoCheck: true,
      sandboxMode: "danger-full-access",
      approvalPolicy: "never",
    };

    let turnCompleted = false;
    let finalResponse: string | undefined;
    let runtimeSessionId: string | undefined;
    try {
      const client = await this.createClient();
      if (signal.aborted) {
        yield { kind: "done" };
        return;
      }
      const thread =
        request.resumeSessionId === undefined
          ? client.startThread(threadOptions)
          : client.resumeThread(request.resumeSessionId, threadOptions);
      const { events } = await thread.runStreamed(this.composePrompt(request.prompt), {
        signal,
      });

      for await (const event of events) {
        if (signal.aborted) {
          yield { kind: "done" };
          return;
        }
        yield { kind: "progress" };

        switch (event.type) {
          case "thread.started":
            if (event.thread_id.trim() === "") {
              throw new Error("Codex did not provide a session id.");
            }
            if (
              request.resumeSessionId !== undefined &&
              event.thread_id !== request.resumeSessionId
            ) {
              throw new Error("Codex did not resume the saved session.");
            }
            if (runtimeSessionId === undefined) {
              runtimeSessionId = event.thread_id;
              yield { kind: "session-started", runtimeSessionId };
            } else if (runtimeSessionId !== event.thread_id) {
              throw new Error("Codex runtime error.");
            }
            break;
          case "item.started":
            yield* this.mapStartedItem(event.item);
            break;
          case "item.completed":
            if (event.item.type === "agent_message") {
              finalResponse = event.item.text;
            } else {
              yield* this.mapCompletedItem(event.item);
            }
            break;
          case "turn.completed":
            if (runtimeSessionId === undefined) {
              throw new Error("Codex did not provide a session id.");
            }
            if (finalResponse === undefined || finalResponse.trim() === "") {
              throw new Error("Codex turn completed without a final response.");
            }
            yield {
              kind: "activity",
              activity: { type: "response", body: finalResponse },
            };
            turnCompleted = true;
            yield { kind: "done" };
            return;
          case "turn.failed":
            throw new Error("Codex turn failed.");
          case "error":
            throw new Error("Codex runtime error.");
          case "turn.started":
          case "item.updated":
            break;
        }
      }

      if (signal.aborted) {
        yield { kind: "done" };
        return;
      }
      if (!turnCompleted) {
        throw new Error("Codex stream ended before turn completion.");
      }
    } catch (error) {
      if (signal.aborted) {
        yield { kind: "done" };
        return;
      }
      // Codex CLI errors can include command lines or provider details. Keep
      // the Linear-visible failure safe; the server owns its final activity.
      if (isSafeFatalError(error)) {
        throw error;
      }
      throw new Error("Codex runtime failed.");
    } finally {
      if (this.activeControllers.get(request) === closeController) {
        this.activeControllers.delete(request);
      }
    }
  }

  private *mapStartedItem(item: ThreadItem): Generator<RuntimeEvent> {
    if (item.type === "command_execution") {
      yield {
        kind: "activity",
        activity: { type: "action", action: "Command", parameter: compact(item.command) },
      };
    } else if (item.type === "mcp_tool_call") {
      yield {
        kind: "activity",
        activity: {
          type: "action",
          action: `MCP: ${item.server}/${item.tool}`,
          parameter: summarizeArguments(item.arguments),
        },
      };
    } else if (item.type === "web_search") {
      yield {
        kind: "activity",
        activity: { type: "action", action: "Web search", parameter: compact(item.query) },
      };
    }
  }

  private *mapCompletedItem(item: ThreadItem): Generator<RuntimeEvent> {
    switch (item.type) {
      case "reasoning":
        if (item.text.trim() !== "") {
          yield { kind: "activity", activity: { type: "thought", body: item.text } };
        }
        return;
      case "command_execution":
        yield {
          kind: "activity",
          activity: {
            type: "action",
            action: "Command",
            parameter: compact(item.command),
            result: commandResult(item),
          },
        };
        return;
      case "file_change":
        yield {
          kind: "activity",
          activity: {
            type: "action",
            action: "File change",
            parameter: fileChangeParameter(item),
            result: item.status === "completed" ? "Completed." : "Failed.",
          },
        };
        return;
      case "mcp_tool_call":
        yield {
          kind: "activity",
          activity: {
            type: "action",
            action: `MCP: ${item.server}/${item.tool}`,
            parameter: summarizeArguments(item.arguments),
            result:
              item.status === "completed"
                ? "Completed."
                : "Failed.",
          },
        };
        return;
      case "web_search":
        yield {
          kind: "activity",
          activity: {
            type: "action",
            action: "Web search",
            parameter: compact(item.query),
            result: "Completed.",
          },
        };
        return;
      case "error":
        yield {
          kind: "activity",
          activity: { type: "error", body: "Codex reported an item error." },
        };
        return;
      case "todo_list":
      case "agent_message":
        return;
    }
  }
}
