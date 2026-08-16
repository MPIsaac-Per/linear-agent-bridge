import { createHmac } from "node:crypto";
import { promises as fsPromises } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startServer, type ServerDeps } from "../src/server.js";
import type { Config } from "../src/config.js";
import { LinearAgentClient, type FetchFn } from "../src/linear/client.js";
import { LinearOAuthTokenManager } from "../src/linear/oauth.js";
import { JsonSessionStore } from "../src/sessions/store.js";
import { SerialQueue } from "../src/queue.js";
import type {
  AgentActivityContent,
  AgentRuntime,
  RuntimeEvent,
  SessionRequest,
} from "../src/types.js";

const WEBHOOK_SECRET = "whsec_test_secret";

function buildConfig(overrides: Partial<Config> = {}): Config {
  return {
    linearClientId: "client-id-test",
    linearClientSecret: "client-secret-test",
    linearWebhookSecret: WEBHOOK_SECRET,
    linearAccessToken: "access-token-test",
    port: 0,
    runtime: "claude",
    kbPath: "/tmp/kb-unused",
    sessionStorePath: "unused-see-store-field",
    oauthTokenStorePath: "unused-see-oauth-field",
    runTimeoutMs: 300000,
    ...overrides,
  };
}

function sign(body: string, secret: string): string {
  return createHmac("sha256", secret).update(body).digest("hex");
}

function createDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function jsonResponse(
  body: unknown,
  init: { ok?: boolean; status?: number; statusText?: string } = {},
): Response {
  const ok = init.ok ?? true;
  const status = init.status ?? (ok ? 200 : 500);
  return {
    ok,
    status,
    statusText: init.statusText ?? (ok ? "OK" : "Internal Server Error"),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response;
}

interface LinearCall {
  agentSessionId: string;
  content: AgentActivityContent;
  ephemeral?: boolean;
}

/** Fakes the Linear GraphQL endpoint: records every agentActivityCreate call. */
function fakeLinearFetch(calls: LinearCall[]): FetchFn {
  return (async (_url: RequestInfo | URL, init?: RequestInit) => {
    const parsed = JSON.parse(init?.body as string) as {
      variables: {
        input: {
          agentSessionId: string;
          content: AgentActivityContent;
          ephemeral?: boolean;
        };
      };
    };
    calls.push({
      agentSessionId: parsed.variables.input.agentSessionId,
      content: parsed.variables.input.content,
      ...(parsed.variables.input.ephemeral !== undefined
        ? { ephemeral: parsed.variables.input.ephemeral }
        : {}),
    });
    return jsonResponse({ data: { agentActivityCreate: { success: true } } });
  }) as FetchFn;
}

/** Fake AgentRuntime: records every request it's asked to run and yields a scripted event stream. */
class FakeRuntime implements AgentRuntime {
  readonly name = "fake";
  lastRequest: SessionRequest | undefined;
  requests: SessionRequest[] = [];

  constructor(private readonly produce: (request: SessionRequest) => AsyncIterable<RuntimeEvent>) {}

  async *runSession(request: SessionRequest): AsyncIterable<RuntimeEvent> {
    this.lastRequest = request;
    this.requests.push(request);
    yield* this.produce(request);
  }
}

/** Polls a (possibly async) predicate until true or a timeout elapses. */
async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

interface Harness {
  port: number;
  close: () => Promise<void>;
  calls: LinearCall[];
  store: JsonSessionStore;
  tokenFetch: ReturnType<typeof vi.fn>;
  oauthTokenStorePath: string;
  authorizationUrl: Promise<string>;
}

async function startTestServer(
  runtime: AgentRuntime,
  options: { tokenFetchImpl?: FetchFn; configOverrides?: Partial<Config> } = {},
): Promise<Harness> {
  const calls: LinearCall[] = [];
  const linear = new LinearAgentClient("test-linear-token", fakeLinearFetch(calls));

  const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "server-test-"));
  const store = new JsonSessionStore(path.join(tmpDir, "sessions.json"));
  const oauthTokenStorePath = path.join(tmpDir, "oauth-tokens.json");

  const tokenFetch = vi.fn(
    options.tokenFetchImpl ?? (async () => jsonResponse({ access_token: "unused" })),
  );

  let resolveListening!: (port: number) => void;
  const listening = new Promise<number>((resolve) => {
    resolveListening = resolve;
  });
  let resolveAuthorizationUrl!: (url: string) => void;
  const authorizationUrl = new Promise<string>((resolve) => {
    resolveAuthorizationUrl = resolve;
  });

  const queue = new SerialQueue();
  const oauth = new LinearOAuthTokenManager({
    clientId: "client-id-test",
    clientSecret: "client-secret-test",
    initialAccessToken: "test-linear-token",
    storePath: oauthTokenStorePath,
    fetchFn: tokenFetch as unknown as FetchFn,
  });
  const deps: ServerDeps = {
    config: buildConfig({ port: 0, ...options.configOverrides }),
    runtime,
    linear,
    oauth,
    store,
    queue,
    tokenFetch: tokenFetch as unknown as FetchFn,
    onListening: resolveListening,
    onOAuthAuthorizationUrl: resolveAuthorizationUrl,
  };

  const server = startServer(deps);
  const port = await listening;

  return {
    port,
    close: async () => {
      // Drain in-flight session runs (FIFO queue) before removing the temp
      // dir, or the store's atomic-write temp file races the rm (ENOTEMPTY).
      await queue.enqueue(async () => {});
      await server.close();
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    },
    calls,
    store,
    tokenFetch,
    oauthTokenStorePath,
    authorizationUrl,
  };
}

let activeHarness: Harness | undefined;

afterEach(async () => {
  await activeHarness?.close();
  activeHarness = undefined;
});

describe("startServer", () => {
  it("signed created event: acks 200, emits the liveness thought, forwards runtime activities in order, persists the session mapping", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "session-started", runtimeSessionId: "runtime-session-abc" };
      yield { kind: "activity", activity: { type: "thought", body: "Looking at the issue" } };
      yield { kind: "activity", activity: { type: "response", body: "Fixed it" } };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    const payload = {
      type: "AgentSessionEvent",
      action: "created",
      agentSession: {
        id: "agent-session-1",
        issue: { id: "issue-1", identifier: "MPI-1", title: "Fix the bug" },
      },
      promptContext: "<issue>Fix the bug</issue>",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const signature = sign(body, WEBHOOK_SECRET);

    const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "linear-signature": signature },
      body,
    });

    expect(response.status).toBe(200);

    await waitFor(() => harness.calls.length >= 3);

    expect(harness.calls[0]).toEqual({
      agentSessionId: "agent-session-1",
      content: { type: "thought", body: "Reading the issue and gathering context…" },
      ephemeral: true,
    });
    expect(harness.calls[1]).toEqual({
      agentSessionId: "agent-session-1",
      content: { type: "thought", body: "Looking at the issue" },
      ephemeral: true,
    });
    expect(harness.calls[2]).toEqual({
      agentSessionId: "agent-session-1",
      content: { type: "response", body: "Fixed it" },
    });

    expect(runtime.lastRequest).toEqual({
      linearSessionId: "agent-session-1",
      prompt: "<issue>Fix the bug</issue>",
      abortController: expect.any(AbortController),
    });

    const record = await harness.store.get("agent-session-1");
    expect(record).toEqual({
      linearSessionId: "agent-session-1",
      runtimeSessionId: "runtime-session-abc",
      runtime: "fake",
      issueIdentifier: "MPI-1",
      updatedAt: expect.any(String),
    });
  });

  it("signed prompted event with a pre-seeded store record: fake runtime receives resumeSessionId", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "session-started", runtimeSessionId: "runtime-session-resumed" };
      yield { kind: "activity", activity: { type: "response", body: "continuing" } };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    await harness.store.put({
      linearSessionId: "agent-session-2",
      runtimeSessionId: "prior-runtime-session",
      runtime: "fake",
      issueIdentifier: "MPI-2",
      updatedAt: "2026-08-01T00:00:00.000Z",
    });

    const payload = {
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "agent-session-2" },
      // Live payload shape (2026-08-12): text rides the content union.
      agentActivity: { content: { type: "prompt", body: "please continue" } },
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const signature = sign(body, WEBHOOK_SECRET);

    const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "linear-signature": signature },
      body,
    });

    expect(response.status).toBe(200);

    await waitFor(() => runtime.lastRequest !== undefined);

    expect(runtime.lastRequest).toEqual({
      linearSessionId: "agent-session-2",
      prompt: "please continue",
      resumeSessionId: "prior-runtime-session",
      abortController: expect.any(AbortController),
    });
  });

  it("a stop signal aborts the active turn and closes the Linear session", async () => {
    const release = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      yield { kind: "session-started", runtimeSessionId: "runtime-stop" };
      await Promise.race([
        release.promise,
        new Promise<void>((resolve) => {
          request.abortController?.signal.addEventListener("abort", () => resolve(), {
            once: true,
          });
        }),
      ]);
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    try {
      const createdPayload = {
        type: "AgentSessionEvent",
        action: "created",
        agentSession: {
          id: "agent-session-stop",
          issue: { id: "issue-stop", identifier: "MPI-STOP", title: "Long request" },
        },
        promptContext: "do a long task",
        webhookTimestamp: Date.now(),
      };
      const createdBody = JSON.stringify(createdPayload);
      await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(createdBody, WEBHOOK_SECRET),
        },
        body: createdBody,
      });
      await waitFor(() => runtime.requests.length === 1);

      const stopPayload = {
        type: "AgentSessionEvent",
        action: "prompted",
        agentSession: { id: "agent-session-stop" },
        agentActivity: {
          content: { type: "prompt", body: "stop" },
          signal: "stop",
        },
        webhookTimestamp: Date.now(),
      };
      const stopBody = JSON.stringify(stopPayload);
      const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(stopBody, WEBHOOK_SECRET),
        },
        body: stopBody,
      });
      expect(response.status).toBe(200);

      await waitFor(() =>
        harness.calls.some(
          (call) => call.content.type === "response" && call.content.body === "Stopped.",
        ),
      );
      expect(runtime.requests).toHaveLength(1);
      expect(runtime.requests[0]?.abortController?.signal.aborted).toBe(true);
    } finally {
      release.resolve();
    }
  });

  it("aborts a turn at the configured deadline and reports the timeout", async () => {
    const release = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      yield { kind: "session-started", runtimeSessionId: "runtime-timeout" };
      await Promise.race([
        release.promise,
        new Promise<void>((resolve) => {
          request.abortController?.signal.addEventListener("abort", () => resolve(), {
            once: true,
          });
        }),
      ]);
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      configOverrides: { runTimeoutMs: 25 },
    });
    const harness = activeHarness;

    try {
      const payload = {
        type: "AgentSessionEvent",
        action: "created",
        agentSession: {
          id: "agent-session-timeout",
          issue: { id: "issue-timeout", identifier: "MPI-TIME", title: "Bounded request" },
        },
        promptContext: "do a bounded task",
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(body, WEBHOOK_SECRET),
        },
        body,
      });
      expect(response.status).toBe(200);

      await waitFor(() =>
        harness.calls.some(
          (call) =>
            call.content.type === "error" &&
            call.content.body === "This request timed out after 25 ms.",
        ),
      );
      expect(runtime.requests[0]?.abortController?.signal.aborted).toBe(true);
    } finally {
      release.resolve();
    }
  });

  it("rejects an invalid signature with 401 and enqueues nothing", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    const payload = {
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-bad-sig", issue: { id: "i", identifier: "MPI-3", title: "t" } },
      promptContext: "ctx",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const badSignature = sign(body, "wrong-secret");

    const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "linear-signature": badSignature },
      body,
    });

    expect(response.status).toBe(401);

    // Give any (incorrect) async processing a moment to run, then confirm nothing happened.
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(harness.calls).toEqual([]);
    expect(runtime.lastRequest).toBeUndefined();
  });

  it("emits an error activity when the runtime throws, and the server stays up for /healthz", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      throw new Error("kaboom");
      // eslint-disable-next-line no-unreachable
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    const payload = {
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-err", issue: { id: "i", identifier: "MPI-4", title: "t" } },
      promptContext: "ctx",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const signature = sign(body, WEBHOOK_SECRET);

    const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "linear-signature": signature },
      body,
    });

    expect(response.status).toBe(200);

    await waitFor(() => harness.calls.length >= 2);

    expect(harness.calls[0]).toEqual({
      agentSessionId: "agent-session-err",
      content: { type: "thought", body: "Reading the issue and gathering context…" },
      ephemeral: true,
    });
    expect(harness.calls[1]).toEqual({
      agentSessionId: "agent-session-err",
      content: { type: "error", body: "kaboom" },
    });

    const health = await fetch(`http://127.0.0.1:${harness.port}/healthz`);
    expect(health.status).toBe(200);
    expect(await health.text()).toBe("ok");
  });

  it("oauth callback: exchanges the code and persists the rotating token pair", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });

    const tokenResponse = {
      access_token: "tok-secret-123",
      token_type: "Bearer",
      expires_in: 86399,
      scope: "read write",
      refresh_token: "rt-1",
    };

    activeHarness = await startTestServer(runtime, {
      tokenFetchImpl: (async () => jsonResponse(tokenResponse)) as FetchFn,
    });
    const harness = activeHarness;

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    try {
      const authorizationUrl = new URL(await harness.authorizationUrl);
      const state = authorizationUrl.searchParams.get("state");
      expect(authorizationUrl.origin).toBe("https://linear.app");
      expect(authorizationUrl.pathname).toBe("/oauth/authorize");
      expect(state).toMatch(/^[A-Za-z0-9_-]{40,}$/);

      const response = await fetch(
        `http://127.0.0.1:${harness.port}/oauth/callback?code=auth-code-xyz&state=${state}`,
      );
      expect(response.status).toBe(200);

      expect(harness.tokenFetch).toHaveBeenCalledTimes(1);
      const [url, init] = harness.tokenFetch.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("https://api.linear.app/oauth/token");
      expect(init.method).toBe("POST");
      expect(init.headers).toMatchObject({
        "Content-Type": "application/x-www-form-urlencoded",
      });

      const params = new URLSearchParams(init.body as string);
      expect(params.get("code")).toBe("auth-code-xyz");
      expect(params.get("client_id")).toBe("client-id-test");
      expect(params.get("client_secret")).toBe("client-secret-test");
      expect(params.get("grant_type")).toBe("authorization_code");
      expect(params.get("redirect_uri")).toBe("http://localhost:3979/oauth/callback");

      expect(logSpy).toHaveBeenCalledWith(
        "[linear-atlas-agent] OAuth token pair installed",
      );
      const logged = logSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("tok-secret-123");
      expect(logged).not.toContain("rt-1");
      expect(
        JSON.parse(await fsPromises.readFile(harness.oauthTokenStorePath, "utf8")),
      ).toMatchObject({
        accessToken: "tok-secret-123",
        refreshToken: "rt-1",
      });
    } finally {
      logSpy.mockRestore();
    }
  });

  it("oauth callback: rejects a missing or replayed state before token exchange", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    activeHarness = await startTestServer(runtime, {
      tokenFetchImpl: (async () =>
        jsonResponse({
          access_token: "access",
          refresh_token: "refresh",
          expires_in: 86399,
        })) as FetchFn,
    });
    const harness = activeHarness;

    const missingState = await fetch(
      `http://127.0.0.1:${harness.port}/oauth/callback?code=auth-code-xyz`,
    );
    expect(missingState.status).toBe(400);
    expect(harness.tokenFetch).not.toHaveBeenCalled();

    const state = new URL(await harness.authorizationUrl).searchParams.get("state");
    expect(state).not.toBeNull();
    const callbackUrl =
      `http://127.0.0.1:${harness.port}/oauth/callback?code=auth-code-xyz&state=${state}`;

    expect((await fetch(callbackUrl)).status).toBe(200);
    expect(harness.tokenFetch).toHaveBeenCalledTimes(1);
    expect((await fetch(callbackUrl)).status).toBe(400);
    expect(harness.tokenFetch).toHaveBeenCalledTimes(1);
  });

  it("ignores a non-agent-session webhook category but still acks 200", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    const payload = {
      type: "Comment",
      action: "create",
      data: { id: "c1", body: "hi" },
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const signature = sign(body, WEBHOOK_SECRET);

    const response = await fetch(`http://127.0.0.1:${harness.port}/webhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "linear-signature": signature },
      body,
    });

    expect(response.status).toBe(200);

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(harness.calls).toEqual([]);
    expect(runtime.lastRequest).toBeUndefined();
  });
});
