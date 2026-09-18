import { describe, expect, it, vi } from "vitest";
import {
  CodexRuntime,
  type CodexClient,
  type CodexClientFactory,
  type CodexThread,
} from "../src/runtime/codex.js";
import type { RuntimeEvent, SessionRequest } from "../src/types.js";
import type { ThreadEvent, ThreadOptions } from "@openai/codex-sdk";
import { LINEAR_AGENT_SESSION_CONTEXT } from "../src/runtime/prompt.js";

const KB_PATH = "/tmp/example-kb";

function event<T extends ThreadEvent>(value: T): T {
  return value;
}

function eventStream(events: ThreadEvent[]): AsyncIterable<ThreadEvent> {
  return (async function* () {
    yield* events;
  })();
}

function stubClient(events: ThreadEvent[]): {
  client: CodexClient;
  startThread: ReturnType<typeof vi.fn>;
  resumeThread: ReturnType<typeof vi.fn>;
  runStreamed: ReturnType<typeof vi.fn>;
} {
  const runStreamed = vi.fn(async () => ({ events: eventStream(events) }));
  const thread: CodexThread = { runStreamed };
  const startThread = vi.fn(() => thread);
  const resumeThread = vi.fn(() => thread);
  return {
    client: { startThread, resumeThread },
    startThread,
    resumeThread,
    runStreamed,
  };
}

async function collect(
  runtime: CodexRuntime,
  request: SessionRequest,
): Promise<RuntimeEvent[]> {
  const output: RuntimeEvent[] = [];
  for await (const value of runtime.runSession(request)) {
    if (value.kind !== "progress") {
      output.push(value);
    }
  }
  return output;
}

describe("CodexRuntime", () => {
  it("starts a Codex thread with service defaults intact and maps streamed work", async () => {
    const sdkSessionId = "codex-thread-1";
    const stub = stubClient([
      event({ type: "thread.started", thread_id: sdkSessionId }),
      event({
        type: "item.completed",
        item: { id: "reasoning-1", type: "reasoning", text: "I will inspect the project." },
      }),
      event({
        type: "item.started",
        item: {
          id: "command-1",
          type: "command_execution",
          command: "npm test",
          aggregated_output: "",
          status: "in_progress",
        },
      }),
      event({
        type: "item.completed",
        item: {
          id: "command-1",
          type: "command_execution",
          command: "npm test",
          aggregated_output: "passed",
          exit_code: 0,
          status: "completed",
        },
      }),
      event({
        type: "item.completed",
        item: {
          id: "file-1",
          type: "file_change",
          changes: [{ path: "src/runtime/codex.ts", kind: "update" }],
          status: "completed",
        },
      }),
      event({
        type: "item.started",
        item: {
          id: "mcp-1",
          type: "mcp_tool_call",
          server: "linear",
          tool: "get_issue",
          arguments: { id: "MPI-1" },
          status: "in_progress",
        },
      }),
      event({
        type: "item.completed",
        item: {
          id: "mcp-1",
          type: "mcp_tool_call",
          server: "linear",
          tool: "get_issue",
          arguments: { id: "MPI-1" },
          result: { content: [], structured_content: {} },
          status: "completed",
        },
      }),
      event({
        type: "item.completed",
        item: { id: "message-1", type: "agent_message", text: "First draft" },
      }),
      event({
        type: "item.completed",
        item: { id: "message-2", type: "agent_message", text: "Completed successfully." },
      }),
      event({
        type: "turn.completed",
        usage: {
          input_tokens: 1,
          cached_input_tokens: 0,
          cache_write_input_tokens: 0,
          output_tokens: 1,
          reasoning_output_tokens: 1,
        },
      }),
    ]);
    const runtime = new CodexRuntime(KB_PATH, async () => stub.client);

    const events = await collect(runtime, {
      linearSessionId: "linear-1",
      prompt: "Implement it",
    });

    expect(stub.startThread).toHaveBeenCalledTimes(1);
    const options = stub.startThread.mock.calls[0]?.[0] as ThreadOptions;
    expect(options).toMatchObject({
      workingDirectory: KB_PATH,
      skipGitRepoCheck: true,
      sandboxMode: "danger-full-access",
      approvalPolicy: "never",
    });
    expect(options).not.toHaveProperty("model");
    expect(options).not.toHaveProperty("modelReasoningEffort");
    expect(stub.runStreamed).toHaveBeenCalledWith(
      `${LINEAR_AGENT_SESSION_CONTEXT}\n\nImplement it`,
      { signal: expect.any(AbortSignal) },
    );
    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sdkSessionId },
      { kind: "activity", activity: { type: "thought", body: "I will inspect the project." } },
      {
        kind: "activity",
        activity: { type: "action", action: "Command", parameter: "npm test" },
      },
      {
        kind: "activity",
        activity: {
          type: "action",
          action: "Command",
          parameter: "npm test",
          result: "Completed (exit 0).",
        },
      },
      {
        kind: "activity",
        activity: {
          type: "action",
          action: "File change",
          parameter: "update: src/runtime/codex.ts",
          result: "Completed.",
        },
      },
      {
        kind: "activity",
        activity: {
          type: "action",
          action: "MCP: linear/get_issue",
          parameter: '{"id":"MPI-1"}',
        },
      },
      {
        kind: "activity",
        activity: {
          type: "action",
          action: "MCP: linear/get_issue",
          parameter: '{"id":"MPI-1"}',
          result: "Completed.",
        },
      },
      {
        kind: "activity",
        activity: { type: "response", body: "Completed successfully." },
      },
      { kind: "done" },
    ]);
  });

  it("resumes the saved Codex thread with the same execution contract", async () => {
    const stub = stubClient([
      event({ type: "thread.started", thread_id: "prior-codex-thread" }),
      event({
        type: "item.completed",
        item: { id: "message", type: "agent_message", text: "Follow-up complete." },
      }),
      event({
        type: "turn.completed",
        usage: {
          input_tokens: 1,
          cached_input_tokens: 0,
          cache_write_input_tokens: 0,
          output_tokens: 1,
          reasoning_output_tokens: 0,
        },
      }),
    ]);
    const runtime = new CodexRuntime(KB_PATH, async () => stub.client);

    await collect(runtime, {
      linearSessionId: "linear-resume",
      prompt: "Continue",
      resumeSessionId: "prior-codex-thread",
    });

    expect(stub.startThread).not.toHaveBeenCalled();
    expect(stub.resumeThread).toHaveBeenCalledWith("prior-codex-thread", {
      workingDirectory: KB_PATH,
      skipGitRepoCheck: true,
      sandboxMode: "danger-full-access",
      approvalPolicy: "never",
    });
  });

  it("fails when Codex silently starts a different thread during resume", async () => {
    const stub = stubClient([
      event({ type: "thread.started", thread_id: "different-codex-thread" }),
    ]);
    const runtime = new CodexRuntime(KB_PATH, async () => stub.client);

    await expect(
      collect(runtime, {
        linearSessionId: "linear-resume-mismatch",
        prompt: "Continue",
        resumeSessionId: "saved-codex-thread",
      }),
    ).rejects.toThrow("Codex did not resume the saved session.");
  });

  it("surfaces the configured writable output path without changing Codex defaults", async () => {
    const stub = stubClient([
      event({ type: "thread.started", thread_id: "codex-output-path" }),
      event({
        type: "item.completed",
        item: { id: "message", type: "agent_message", text: "Done." },
      }),
      event({
        type: "turn.completed",
        usage: {
          input_tokens: 1,
          cached_input_tokens: 0,
          cache_write_input_tokens: 0,
          output_tokens: 1,
          reasoning_output_tokens: 0,
        },
      }),
    ]);
    const runtime = new CodexRuntime(
      KB_PATH,
      async () => stub.client,
      "/srv/agent-out",
    );

    await collect(runtime, {
      linearSessionId: "linear-output-path",
      prompt: "Produce the artifact",
    });

    expect(stub.runStreamed).toHaveBeenCalledWith(
      `${LINEAR_AGENT_SESSION_CONTEXT}\n\nProduce the artifact\n\nWrite any files you produce to /srv/agent-out. The working directory may be read-only to this service, so a denied write there is expected rather than something to work around.`,
      { signal: expect.any(AbortSignal) },
    );
  });

  it("throws safe failures for provider failure, stream error, and premature completion", async () => {
    const cases: Array<{ events: ThreadEvent[]; message: string }> = [
      {
        events: [
          event({ type: "turn.failed", error: { message: "secret provider detail" } }),
        ],
        message: "Codex turn failed.",
      },
      {
        events: [event({ type: "error", message: "secret provider detail" })],
        message: "Codex runtime error.",
      },
      { events: [event({ type: "turn.started" })], message: "Codex stream ended before turn completion." },
      {
        events: [
          event({
            type: "item.completed",
            item: { id: "message", type: "agent_message", text: "Done" },
          }),
          event({
            type: "turn.completed",
            usage: {
              input_tokens: 1,
              cached_input_tokens: 0,
              cache_write_input_tokens: 0,
              output_tokens: 1,
              reasoning_output_tokens: 0,
            },
          }),
        ],
        message: "Codex did not provide a session id.",
      },
      {
        events: [event({ type: "thread.started", thread_id: "" })],
        message: "Codex did not provide a session id.",
      },
      {
        events: [
          event({ type: "thread.started", thread_id: "codex-blank-final" }),
          event({
            type: "item.completed",
            item: { id: "message-1", type: "agent_message", text: "Interim" },
          }),
          event({
            type: "item.completed",
            item: { id: "message-2", type: "agent_message", text: "   " },
          }),
          event({
            type: "turn.completed",
            usage: {
              input_tokens: 1,
              cached_input_tokens: 0,
              cache_write_input_tokens: 0,
              output_tokens: 1,
              reasoning_output_tokens: 0,
            },
          }),
        ],
        message: "Codex turn completed without a final response.",
      },
    ];

    for (const testCase of cases) {
      const stub = stubClient(testCase.events);
      const runtime = new CodexRuntime(KB_PATH, async () => stub.client);
      await expect(
        collect(runtime, { linearSessionId: "linear-failure", prompt: "Run it" }),
      ).rejects.toThrow(testCase.message);
    }
  });

  it("keeps nonfatal provider details out of Linear activities", async () => {
    const stub = stubClient([
      event({ type: "thread.started", thread_id: "codex-safe-items" }),
      event({
        type: "item.completed",
        item: {
          id: "mcp-failure",
          type: "mcp_tool_call",
          server: "example",
          tool: "lookup",
          arguments: { id: "safe-parameter" },
          error: { message: "bearer secret-token" },
          status: "failed",
        },
      }),
      event({
        type: "item.completed",
        item: { id: "item-error", type: "error", message: "private provider detail" },
      }),
      event({
        type: "item.completed",
        item: { id: "message", type: "agent_message", text: "Finished safely." },
      }),
      event({
        type: "turn.completed",
        usage: {
          input_tokens: 1,
          cached_input_tokens: 0,
          cache_write_input_tokens: 0,
          output_tokens: 1,
          reasoning_output_tokens: 0,
        },
      }),
    ]);
    const runtime = new CodexRuntime(KB_PATH, async () => stub.client);

    const events = await collect(runtime, {
      linearSessionId: "linear-safe-items",
      prompt: "Run it",
    });

    expect(events).toContainEqual({
      kind: "activity",
      activity: {
        type: "action",
        action: "MCP: example/lookup",
        parameter: '{"id":"safe-parameter"}',
        result: "Failed.",
      },
    });
    expect(events).toContainEqual({
      kind: "activity",
      activity: { type: "error", body: "Codex reported an item error." },
    });
    expect(JSON.stringify(events)).not.toContain("secret-token");
    expect(JSON.stringify(events)).not.toContain("private provider detail");
  });

  it("sanitizes SDK exceptions before the server reports the final error", async () => {
    const runStreamed = vi.fn(async () => {
      throw new Error("Codex Exec exited with code 1: bearer secret-token");
    });
    const client: CodexClient = {
      startThread: () => ({ runStreamed }),
      resumeThread: () => ({ runStreamed }),
    };
    const runtime = new CodexRuntime(KB_PATH, async () => client);

    await expect(
      collect(runtime, { linearSessionId: "linear-exception", prompt: "Run it" }),
    ).rejects.toThrow("Codex runtime failed.");
  });

  it("stops cleanly for abort and force-close, only once, and cleans up the active handle", async () => {
    let entered!: () => void;
    const enteredWait = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const releaseWait = new Promise<void>((resolve) => {
      release = resolve;
    });
    const runStreamed = vi.fn(async (_prompt: string, options?: { signal?: AbortSignal }) => ({
      events: (async function* () {
        yield event({ type: "thread.started", thread_id: "codex-stopped" });
        entered();
        await releaseWait;
        expect(options?.signal?.aborted).toBe(true);
        yield event({ type: "turn.started" });
      })(),
    }));
    const client: CodexClient = {
      startThread: () => ({ runStreamed }),
      resumeThread: () => ({ runStreamed }),
    };
    const runtime = new CodexRuntime(KB_PATH, async () => client);
    const request: SessionRequest = { linearSessionId: "linear-stop", prompt: "Run it" };
    const running = collect(runtime, request);

    await enteredWait;
    runtime.forceCloseSession(request);
    runtime.forceCloseSession(request);
    release();
    await expect(running).resolves.toEqual([
      { kind: "session-started", runtimeSessionId: "codex-stopped" },
      { kind: "done" },
    ]);

    // A later force-close is a no-op because the finished session was removed.
    expect(() => runtime.forceCloseSession(request)).not.toThrow();
  });

  it("forwards an active server abort to the streamed Codex turn", async () => {
    let entered!: () => void;
    const enteredWait = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const releaseWait = new Promise<void>((resolve) => {
      release = resolve;
    });
    const runStreamed = vi.fn(async (_prompt: string, options?: { signal?: AbortSignal }) => ({
      events: (async function* () {
        yield event({ type: "thread.started", thread_id: "codex-aborted" });
        entered();
        await releaseWait;
        expect(options?.signal?.aborted).toBe(true);
        yield event({ type: "turn.started" });
      })(),
    }));
    const client: CodexClient = {
      startThread: () => ({ runStreamed }),
      resumeThread: () => ({ runStreamed }),
    };
    const controller = new AbortController();
    const runtime = new CodexRuntime(KB_PATH, async () => client);
    const running = collect(runtime, {
      linearSessionId: "linear-abort",
      prompt: "Run it",
      abortController: controller,
    });

    await enteredWait;
    controller.abort(new Error("stopped by Linear"));
    release();
    await expect(running).resolves.toEqual([
      { kind: "session-started", runtimeSessionId: "codex-aborted" },
      { kind: "done" },
    ]);
  });

  it("does not spawn Codex when the server has already stopped the request", async () => {
    const controller = new AbortController();
    controller.abort();
    const createClient: CodexClientFactory = vi.fn(async () => {
      throw new Error("must not create Codex");
    });
    const runtime = new CodexRuntime(KB_PATH, createClient);

    await expect(
      collect(runtime, {
        linearSessionId: "linear-prestopped",
        prompt: "Run it",
        abortController: controller,
      }),
    ).resolves.toEqual([{ kind: "done" }]);
    expect(createClient).not.toHaveBeenCalled();
  });
});
