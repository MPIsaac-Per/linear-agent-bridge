import { describe, expect, it, vi } from "vitest";
import { ClaudeRuntime, type QueryFn } from "../src/runtime/claude.js";
import type { RuntimeEvent, SessionRequest } from "../src/types.js";
import type { Options, SDKMessage } from "@anthropic-ai/claude-agent-sdk";

const KB_PATH = "/tmp/example-kb";

function systemInit(sessionId: string): SDKMessage {
  return {
    type: "system",
    subtype: "init",
    apiKeySource: "user",
    claude_code_version: "1.0.0",
    cwd: KB_PATH,
    tools: [],
    mcp_servers: [],
    model: "claude-opus-4",
    permissionMode: "bypassPermissions",
    slash_commands: [],
    output_style: "default",
    skills: [],
    plugins: [],
    uuid: "00000000-0000-0000-0000-000000000001",
    session_id: sessionId,
  } as unknown as SDKMessage;
}

function assistantMessage(
  sessionId: string,
  content: Array<Record<string, unknown>>,
  stopReason: string | null = null,
): SDKMessage {
  return {
    type: "assistant",
    message: {
      id: "msg_1",
      type: "message",
      role: "assistant",
      model: "claude-opus-4",
      content,
      stop_reason: stopReason,
      stop_sequence: null,
      usage: {},
    },
    parent_tool_use_id: null,
    uuid: "00000000-0000-0000-0000-000000000002",
    session_id: sessionId,
  } as unknown as SDKMessage;
}

function assistantText(sessionId: string, text: string): SDKMessage {
  return assistantMessage(sessionId, [{ type: "text", text }]);
}

function assistantToolUse(
  sessionId: string,
  name: string,
  input: Record<string, unknown>,
  toolUseId = "tool_1",
): SDKMessage {
  return assistantMessage(sessionId, [{ type: "tool_use", id: toolUseId, name, input }]);
}

function userToolResult(
  sessionId: string,
  toolUseId: string,
  content: string,
  isError = false,
): SDKMessage {
  return {
    type: "user",
    message: {
      role: "user",
      content: [{ type: "tool_result", tool_use_id: toolUseId, content, is_error: isError }],
    },
    parent_tool_use_id: null,
    uuid: "00000000-0000-0000-0000-000000000003",
    session_id: sessionId,
  } as unknown as SDKMessage;
}

function resultSuccess(sessionId: string, result: string): SDKMessage {
  return {
    type: "result",
    subtype: "success",
    duration_ms: 100,
    duration_api_ms: 90,
    is_error: false,
    num_turns: 1,
    result,
    total_cost_usd: 0.01,
    usage: {},
    modelUsage: {},
    permission_denials: [],
    uuid: "00000000-0000-0000-0000-000000000004",
    session_id: sessionId,
  } as unknown as SDKMessage;
}

function resultError(sessionId: string, errors: string[]): SDKMessage {
  return {
    type: "result",
    subtype: "error_max_turns",
    duration_ms: 100,
    duration_api_ms: 90,
    is_error: true,
    num_turns: 10,
    total_cost_usd: 0.02,
    usage: {},
    modelUsage: {},
    permission_denials: [],
    errors,
    uuid: "00000000-0000-0000-0000-000000000006",
    session_id: sessionId,
  } as unknown as SDKMessage;
}

async function collect(request: SessionRequest, runtime: ClaudeRuntime): Promise<RuntimeEvent[]> {
  const events: RuntimeEvent[] = [];
  for await (const event of runtime.runSession(request)) {
    events.push(event);
  }
  return events;
}

describe("ClaudeRuntime", () => {
  it("exposes runtime name 'claude'", () => {
    async function* stub(): AsyncGenerator<SDKMessage> {
      // never invoked in this test
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    expect(runtime.name).toBe("claude");
  });

  it("yields session-started, thought, action, response, done in order (happy path)", async () => {
    const sessionId = "sdk-session-1";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantText(sessionId, "Looking into it...");
      yield assistantToolUse(sessionId, "Bash", { command: "ls -la" });
      yield resultSuccess(sessionId, "Here is the answer");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const request: SessionRequest = { linearSessionId: "linear-1", prompt: "hello" };

    const events = await collect(request, runtime);

    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "thought", body: "Looking into it..." } },
      {
        kind: "activity",
        activity: { type: "action", action: "Bash", parameter: '{"command":"ls -la"}' },
      },
      { kind: "activity", activity: { type: "response", body: "Here is the answer" } },
      { kind: "done" },
    ]);
  });

  it("suppresses a thought whose body the final response repeats (no double render)", async () => {
    const sessionId = "sdk-session-dedupe";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantText(sessionId, "Interim finding");
      yield assistantText(sessionId, "The final answer");
      yield resultSuccess(sessionId, "The final answer");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const request: SessionRequest = { linearSessionId: "linear-dedupe", prompt: "hello" };

    const events = await collect(request, runtime);

    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "thought", body: "Interim finding" } },
      { kind: "activity", activity: { type: "response", body: "The final answer" } },
      { kind: "done" },
    ]);
  });

  it("emits end-turn assistant text as a durable response when no result follows", async () => {
    const sessionId = "sdk-session-end-turn";
    let streamOpen!: () => void;
    const streamOpenWait = new Promise<void>((resolve) => {
      streamOpen = resolve;
    });
    let release!: () => void;
    const releaseWait = new Promise<void>((resolve) => {
      release = resolve;
    });
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantMessage(
        sessionId,
        [{ type: "text", text: "The durable answer" }],
        "end_turn",
      );
      streamOpen();
      await releaseWait;
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const events: RuntimeEvent[] = [];
    const collection = (async () => {
      for await (const event of runtime.runSession({
        linearSessionId: "linear-end-turn",
        prompt: "hello",
      })) {
        events.push(event);
      }
    })();

    await streamOpenWait;
    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "response", body: "The durable answer" } },
    ]);

    release();
    await collection;
    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "response", body: "The durable answer" } },
      { kind: "done" },
    ]);
  });

  it("suppresses a success result that repeats an emitted end-turn response", async () => {
    const sessionId = "sdk-session-end-turn-dedupe";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield assistantMessage(
        sessionId,
        [{ type: "text", text: "The durable answer" }],
        "end_turn",
      );
      yield resultSuccess(sessionId, "The durable answer");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);

    const events = await collect(
      { linearSessionId: "linear-end-turn-dedupe", prompt: "hello" },
      runtime,
    );

    expect(events).toEqual([
      { kind: "activity", activity: { type: "response", body: "The durable answer" } },
      { kind: "done" },
    ]);
  });

  it("forwards a success result that differs from an emitted end-turn response", async () => {
    const sessionId = "sdk-session-end-turn-different";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield assistantMessage(
        sessionId,
        [{ type: "text", text: "The immediate answer" }],
        "end_turn",
      );
      yield resultSuccess(sessionId, "The corrected answer");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);

    const events = await collect(
      { linearSessionId: "linear-end-turn-different", prompt: "hello" },
      runtime,
    );

    expect(events).toEqual([
      { kind: "activity", activity: { type: "response", body: "The immediate answer" } },
      { kind: "activity", activity: { type: "response", body: "The corrected answer" } },
      { kind: "done" },
    ]);
  });

  it("combines multiple end-turn text blocks into one durable response", async () => {
    const sessionId = "sdk-session-end-turn-blocks";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield assistantMessage(
        sessionId,
        [
          { type: "text", text: "First paragraph." },
          { type: "text", text: "Second paragraph." },
        ],
        "end_turn",
      );
      yield resultSuccess(sessionId, "First paragraph.\nSecond paragraph.");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);

    const events = await collect(
      { linearSessionId: "linear-end-turn-blocks", prompt: "hello" },
      runtime,
    );

    expect(events).toEqual([
      {
        kind: "activity",
        activity: {
          type: "response",
          body: "First paragraph.\nSecond paragraph.",
        },
      },
      { kind: "done" },
    ]);
  });

  it("emits a thought and an action from a single assistant message with mixed content blocks", async () => {
    const sessionId = "sdk-session-mixed";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantMessage(sessionId, [
        { type: "text", text: "Checking the file first" },
        { type: "tool_use", id: "tool_2", name: "Read", input: { file_path: "/tmp/x.md" } },
      ]);
      yield resultSuccess(sessionId, "done");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const events = await collect({ linearSessionId: "linear-mixed", prompt: "hi" }, runtime);

    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "thought", body: "Checking the file first" } },
      {
        kind: "activity",
        activity: { type: "action", action: "Read", parameter: '{"file_path":"/tmp/x.md"}' },
      },
      { kind: "activity", activity: { type: "response", body: "done" } },
      { kind: "done" },
    ]);
  });

  it("pairs completed tool results with their Linear actions", async () => {
    const sessionId = "sdk-session-tool-result";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantToolUse(sessionId, "Bash", { command: "printf hello" }, "tool-1");
      yield userToolResult(sessionId, "tool-1", "hello");
      yield resultSuccess(sessionId, "done");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);

    const events = await collect(
      { linearSessionId: "linear-tool-result", prompt: "run it" },
      runtime,
    );

    expect(events).toContainEqual({
      kind: "activity",
      activity: {
        type: "action",
        action: "Bash",
        parameter: '{"command":"printf hello"}',
        result: "Completed.",
      },
    });
  });

  it("marks failed tool results as failed completed actions", async () => {
    const sessionId = "sdk-session-tool-error";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantToolUse(sessionId, "WebFetch", { url: "https://example.com" }, "tool-err");
      yield userToolResult(sessionId, "tool-err", "request timed out", true);
      yield resultSuccess(sessionId, "Could not fetch that page.");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);

    const events = await collect(
      { linearSessionId: "linear-tool-error", prompt: "fetch it" },
      runtime,
    );

    expect(events).toContainEqual({
      kind: "activity",
      activity: {
        type: "action",
        action: "WebFetch",
        parameter: '{"url":"https://example.com"}',
        result: "Failed: request timed out",
      },
    });
  });

  it("maps a result message with an error subtype to an error activity (no throw)", async () => {
    const sessionId = "sdk-session-err-result";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield resultError(sessionId, ["max turns exceeded"]);
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const events = await collect({ linearSessionId: "linear-err", prompt: "hi" }, runtime);

    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "error", body: "max turns exceeded" } },
      { kind: "done" },
    ]);
  });

  it("truncates a very long tool input summary to stay compact and one-line", async () => {
    const sessionId = "sdk-session-long";
    const bigInput = { data: "x".repeat(500) };
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantToolUse(sessionId, "Write", bigInput);
      yield resultSuccess(sessionId, "done");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const events = await collect({ linearSessionId: "linear-long", prompt: "hi" }, runtime);

    const action = events.find(
      (e): e is Extract<RuntimeEvent, { kind: "activity" }> =>
        e.kind === "activity" && e.activity.type === "action",
    );
    expect(action).toBeDefined();
    const parameter = action!.activity.type === "action" ? action!.activity.parameter : "";
    expect(parameter.includes("\n")).toBe(false);
    expect(parameter.length).toBeLessThanOrEqual(203);
    expect(parameter.endsWith("...")).toBe(true);
  });

  it("passes resume only when request.resumeSessionId is present", async () => {
    let capturedWithResume: Options | undefined;
    async function* stubWith(params: {
      prompt: string;
      options?: Options;
    }): AsyncGenerator<SDKMessage> {
      capturedWithResume = params.options;
      yield systemInit("s-with-resume");
      yield resultSuccess("s-with-resume", "done");
    }
    const runtimeWith = new ClaudeRuntime(KB_PATH, stubWith as QueryFn);
    await collect(
      { linearSessionId: "l1", prompt: "hi", resumeSessionId: "prior-runtime-session" },
      runtimeWith,
    );
    expect(capturedWithResume?.resume).toBe("prior-runtime-session");

    let capturedWithoutResume: Options | undefined;
    async function* stubWithout(params: {
      prompt: string;
      options?: Options;
    }): AsyncGenerator<SDKMessage> {
      capturedWithoutResume = params.options;
      yield systemInit("s-without-resume");
      yield resultSuccess("s-without-resume", "done");
    }
    const runtimeWithout = new ClaudeRuntime(KB_PATH, stubWithout as QueryFn);
    await collect({ linearSessionId: "l2", prompt: "hi" }, runtimeWithout);
    expect(capturedWithoutResume).not.toHaveProperty("resume");
  });

  it("passes the request abort controller to the SDK query", async () => {
    let captured: Options | undefined;
    async function* stub(params: {
      prompt: string;
      options?: Options;
    }): AsyncGenerator<SDKMessage> {
      captured = params.options;
      yield systemInit("s-abort");
      yield resultSuccess("s-abort", "done");
    }
    const controller = new AbortController();
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);

    await collect(
      {
        linearSessionId: "linear-abort",
        prompt: "start",
        abortController: controller,
      },
      runtime,
    );

    expect(captured?.abortController).toBe(controller);
  });

  it("closes the active SDK query exactly once when the request is aborted", async () => {
    let entered!: () => void;
    const enteredWait = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const releaseWait = new Promise<void>((resolve) => {
      release = resolve;
    });
    async function* messages(): AsyncGenerator<SDKMessage> {
      yield systemInit("s-close-on-abort");
      entered();
      await releaseWait;
    }
    const query = messages();
    const close = vi.fn();
    Object.assign(query, { close });
    const runtime = new ClaudeRuntime(KB_PATH, (() => query) as QueryFn);
    const controller = new AbortController();

    const collection = collect(
      {
        linearSessionId: "linear-close-on-abort",
        prompt: "start",
        abortController: controller,
      },
      runtime,
    );
    await enteredWait;
    controller.abort(new Error("cancelled"));
    release();
    await collection;

    expect(close).toHaveBeenCalledTimes(1);
  });

  it("force-closes the active SDK query synchronously and idempotently", async () => {
    let entered!: () => void;
    const enteredWait = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const releaseWait = new Promise<void>((resolve) => {
      release = resolve;
    });
    async function* messages(): AsyncGenerator<SDKMessage> {
      yield systemInit("s-force-close");
      entered();
      await releaseWait;
    }
    const query = messages();
    const close = vi.fn();
    Object.assign(query, { close });
    const runtime = new ClaudeRuntime(KB_PATH, (() => query) as QueryFn);
    const request: SessionRequest = {
      linearSessionId: "linear-force-close",
      prompt: "start",
      abortController: new AbortController(),
    };
    const collection = collect(request, runtime);
    await enteredWait;

    runtime.forceCloseSession(request);
    runtime.forceCloseSession(request);

    expect(close).toHaveBeenCalledTimes(1);
    release();
    await collection;
  });

  it("preserves the default SDK query close handle across lazy loading", async () => {
    let entered!: () => void;
    const enteredWait = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const releaseWait = new Promise<void>((resolve) => {
      release = resolve;
    });
    async function* messages(): AsyncGenerator<SDKMessage> {
      yield systemInit("s-default-close");
      entered();
      await releaseWait;
    }
    const sdkQuery = messages();
    const close = vi.fn();
    Object.assign(sdkQuery, { close });
    vi.doMock("@anthropic-ai/claude-agent-sdk", () => ({
      query: () => sdkQuery,
    }));
    const controller = new AbortController();

    try {
      const runtime = new ClaudeRuntime(KB_PATH);
      const collection = collect(
        {
          linearSessionId: "linear-default-close",
          prompt: "start",
          abortController: controller,
        },
        runtime,
      );
      await enteredWait;
      controller.abort(new Error("cancelled"));
      release();
      await collection;

      expect(close).toHaveBeenCalledTimes(1);
    } finally {
      vi.doUnmock("@anthropic-ai/claude-agent-sdk");
      release();
    }
  });

  it("sets cwd, settingSources, and bypassPermissions; never sets model or tool allowlists", async () => {
    let captured: Options | undefined;
    async function* stub(params: {
      prompt: string;
      options?: Options;
    }): AsyncGenerator<SDKMessage> {
      captured = params.options;
      yield systemInit("s-options");
      yield resultSuccess("s-options", "done");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    await collect({ linearSessionId: "l3", prompt: "hi" }, runtime);

    expect(captured?.cwd).toBe(KB_PATH);
    expect(captured?.settingSources).toEqual(["user", "project"]);
    expect(captured?.permissionMode).toBe("bypassPermissions");
    expect(captured).not.toHaveProperty("model");
    expect(captured).not.toHaveProperty("allowedTools");
    expect(captured).not.toHaveProperty("disallowedTools");
    expect(captured).not.toHaveProperty("tools");
  });

  it("passes the request prompt straight through to the query function", async () => {
    let capturedPrompt: string | undefined;
    async function* stub(params: {
      prompt: string;
      options?: Options;
    }): AsyncGenerator<SDKMessage> {
      capturedPrompt = params.prompt;
      yield systemInit("s-prompt");
      yield resultSuccess("s-prompt", "done");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    await collect({ linearSessionId: "l4", prompt: "what is the weather" }, runtime);

    expect(capturedPrompt).toBe("what is the weather");
  });

  it("yields an error activity then rethrows when the stream throws mid-session", async () => {
    const sessionId = "sdk-session-stream-error";
    async function* stub(): AsyncGenerator<SDKMessage> {
      yield systemInit(sessionId);
      yield assistantText(sessionId, "working on it");
      throw new Error("stream exploded");
    }
    const runtime = new ClaudeRuntime(KB_PATH, stub as QueryFn);
    const events: RuntimeEvent[] = [];

    await expect(async () => {
      for await (const event of runtime.runSession({ linearSessionId: "l5", prompt: "hi" })) {
        events.push(event);
      }
    }).rejects.toThrow("stream exploded");

    expect(events).toEqual([
      { kind: "session-started", runtimeSessionId: sessionId },
      { kind: "activity", activity: { type: "thought", body: "working on it" } },
      { kind: "activity", activity: { type: "error", body: "stream exploded" } },
    ]);
  });
});
