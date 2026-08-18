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
import {
  JsonBridgeStateStore,
  type JsonBridgeStateStoreOptions,
} from "../src/state/store.js";
import { SerialQueue } from "../src/queue.js";
import { ClaudeRuntime, type QueryFn } from "../src/runtime/claude.js";
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
    bridgeStateStorePath: "unused-see-bridge-state-field",
    oauthTokenStorePath: "unused-see-oauth-field",
    runInactivityTimeoutMs: 300000,
    ...overrides,
  };
}

function sign(body: string, secret: string): string {
  return createHmac("sha256", secret).update(body).digest("hex");
}

function serverUrl(port: number, pathName: string): string {
  return `http://[::1]:${port}${pathName}`;
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
function fakeLinearFetch(calls: LinearCall[], activityIds: string[]): FetchFn {
  return (async (_url: RequestInfo | URL, init?: RequestInit) => {
    const parsed = JSON.parse(init?.body as string) as {
      variables: {
        input: {
          id?: string;
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
    if (parsed.variables.input.id !== undefined) {
      activityIds.push(parsed.variables.input.id);
    }
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
  bridgeState: JsonBridgeStateStore;
  bridgeStatePath: string;
  activityIds: string[];
  tokenFetch: ReturnType<typeof vi.fn>;
  oauthTokenStorePath: string;
  authorizationUrl: Promise<string>;
}

async function startTestServer(
  runtime: AgentRuntime,
  options: {
    tokenFetchImpl?: FetchFn;
    linearFetchImpl?: (calls: LinearCall[]) => FetchFn;
    configOverrides?: Partial<Config>;
    makeBridgeStatePathDirectory?: boolean;
    bridgeStateOwnerId?: string;
    bridgeStateOptions?: JsonBridgeStateStoreOptions;
    prepareBridgeState?: (storePath: string) => Promise<void>;
  } = {},
): Promise<Harness> {
  const calls: LinearCall[] = [];
  const activityIds: string[] = [];
  const linear = new LinearAgentClient(
    "test-linear-token",
    options.linearFetchImpl?.(calls) ?? fakeLinearFetch(calls, activityIds),
  );

  const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "server-test-"));
  const store = new JsonSessionStore(path.join(tmpDir, "sessions.json"));
  const bridgeStatePath = path.join(tmpDir, "bridge-state.json");
  if (options.makeBridgeStatePathDirectory === true) {
    await fsPromises.mkdir(bridgeStatePath);
  }
  await options.prepareBridgeState?.(bridgeStatePath);
  const bridgeState = new JsonBridgeStateStore(bridgeStatePath, {
    ...options.bridgeStateOptions,
    ...(options.bridgeStateOwnerId !== undefined
      ? { ownerId: options.bridgeStateOwnerId }
      : {}),
  });
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
    bridgeState,
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
    bridgeState,
    bridgeStatePath,
    activityIds,
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
      webhookId: "webhook-created-1",
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

    const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt("webhook-created-1"))?.status ===
      "completed",
    );
    await expect(
      harness.bridgeState.getClaim("created:agent-session-1"),
    ).resolves.toMatchObject({
      webhookId: "webhook-created-1",
      status: "completed",
      activityIds: expect.any(Object),
    });
    expect(harness.activityIds).toHaveLength(3);
    expect(new Set(harness.activityIds).size).toBe(3);
  });

  it("does not acknowledge a valid event when durable receipt persistence fails", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime, {
        makeBridgeStatePathDirectory: true,
      });
      const harness = activeHarness;
      const payload = {
        webhookId: "webhook-persistence-failure",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-persistence-failure" },
        promptContext: "raw-persistence-prompt-body",
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);

      const response = await fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(body, WEBHOOK_SECRET),
        },
        body,
      });

      expect(response.status).toBe(503);
      expect(runtime.requests).toHaveLength(0);
      expect(harness.calls).toHaveLength(0);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).toContain("error=FilesystemError");
      expect(logged).not.toContain("raw-persistence-prompt-body");
      expect(logged).not.toContain("EISDIR");
      expect(logged).not.toContain("bridge-state.json");
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("retries a visible claim after its directory sync fails and executes it once", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let openSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        bridgeStateOwnerId: "runtime-a",
      });
      const harness = activeHarness;
      const stateDirectory = path.dirname(harness.bridgeStatePath);
      const originalOpen = fsPromises.open.bind(fsPromises);
      let stateDirectorySyncs = 0;
      openSpy = vi
        .spyOn(fsPromises, "open")
        .mockImplementation(async (...args) => {
          const handle = await originalOpen(...args);
          if (String(args[0]) === stateDirectory) {
            const originalSync = handle.sync.bind(handle);
            vi.spyOn(handle, "sync").mockImplementation(async () => {
              stateDirectorySyncs += 1;
              if (stateDirectorySyncs === 2) {
                throw new Error("synthetic final claim directory sync failure");
              }
              await originalSync();
            });
          }
          return handle;
        });
      const payload = {
        webhookId: "webhook-final-claim-sync-retry",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-final-claim-sync-retry" },
        promptContext: "private retry exactly once prompt",
        webhookTimestamp: Date.now(),
      };
      const send = async (): Promise<Response> => {
        const body = JSON.stringify(payload);
        return fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(body, WEBHOOK_SECRET),
          },
          body,
        });
      };

      expect((await send()).status).toBe(503);
      expect(runtime.requests).toHaveLength(0);
      await expect(
        harness.bridgeState.getReceipt(payload.webhookId),
      ).resolves.toMatchObject({
        status: "claimed",
        ownerId: "runtime-a",
      });

      const [retry, concurrentRetry] = await Promise.all([send(), send()]);
      expect(retry.status).toBe(200);
      expect(concurrentRetry.status).toBe(200);
      await waitFor(() => runtime.requests.length === 1);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(runtime.requests).toHaveLength(1);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("private retry exactly once prompt");
      expect(logged).not.toContain("synthetic final claim directory sync failure");
    } finally {
      openSpy?.mockRestore();
      errorSpy.mockRestore();
    }
  });

  it("retries after marker and release fail before either state write", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let openSpy: ReturnType<typeof vi.spyOn> | undefined;
    let readSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        bridgeStateOwnerId: "runtime-a",
      });
      const harness = activeHarness;
      const originalOpen = fsPromises.open.bind(fsPromises);
      let stateTempOpens = 0;
      let markerOpenFailed = false;
      openSpy = vi
        .spyOn(fsPromises, "open")
        .mockImplementation(async (...args) => {
          const openedPath = String(args[0]);
          if (
            openedPath.includes(".bridge-state.json.") &&
            openedPath.endsWith(".tmp")
          ) {
            stateTempOpens += 1;
            if (stateTempOpens === 3) {
              markerOpenFailed = true;
              throw new Error("synthetic marker open failure");
            }
          }
          return await originalOpen(...args);
        });
      const originalReadFile = fsPromises.readFile.bind(fsPromises);
      let releaseReadFailed = false;
      readSpy = vi
        .spyOn(fsPromises, "readFile")
        .mockImplementation(async (...args) => {
          if (
            markerOpenFailed &&
            !releaseReadFailed &&
            String(args[0]) === harness.bridgeStatePath
          ) {
            releaseReadFailed = true;
            throw new Error("synthetic release read failure");
          }
          return await originalReadFile(...args);
        });
      const payload = {
        webhookId: "webhook-marker-release-retry",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-marker-release-retry" },
        promptContext: "private marker release retry prompt",
        webhookTimestamp: Date.now(),
      };
      const send = async (): Promise<Response> => {
        const body = JSON.stringify(payload);
        return fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(body, WEBHOOK_SECRET),
          },
          body,
        });
      };

      expect((await send()).status).toBe(200);
      await waitFor(() => releaseReadFailed);
      expect(runtime.requests).toHaveLength(0);
      const visibleReceipt = await harness.bridgeState.getReceipt(payload.webhookId);
      expect(visibleReceipt?.dispatchStartedAt).toBeUndefined();

      const [retry, concurrentRetry] = await Promise.all([send(), send()]);
      expect(retry.status).toBe(200);
      expect(concurrentRetry.status).toBe(200);
      await waitFor(() => runtime.requests.length === 1);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(runtime.requests).toHaveLength(1);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("private marker release retry prompt");
      expect(logged).not.toContain("synthetic marker open failure");
      expect(logged).not.toContain("synthetic release read failure");
    } finally {
      readSpy?.mockRestore();
      openSpy?.mockRestore();
      errorSpy.mockRestore();
    }
  });

  it("returns 503 with headroom before Linear's five-second deadline when the state lock is busy", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime, {
        prepareBridgeState: async (storePath) => {
          const lockPath = `${storePath}.lock`;
          const token = "live-lock-owner";
          await fsPromises.mkdir(lockPath, { mode: 0o700 });
          await fsPromises.writeFile(
            path.join(lockPath, `${token}.json`),
            `${JSON.stringify({ token, pid: process.pid, hostname: os.hostname() })}\n`,
            { mode: 0o600 },
          );
        },
      });
      const harness = activeHarness;
      const payload = {
        webhookId: "webhook-lock-contention",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-lock-contention" },
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      const startedAt = Date.now();
      const response = await fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(body, WEBHOOK_SECRET),
        },
        body,
      });

      expect(response.status).toBe(503);
      expect(Date.now() - startedAt).toBeLessThan(2_500);
      expect(runtime.requests).toHaveLength(0);
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining("error=BridgeStateLockTimeoutError"),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("times out while queued behind a same-store mutation and never runs the abandoned claim later", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime, {
        bridgeStateOptions: { lockTimeoutMs: 100 },
      });
      const harness = activeHarness;
      const releaseMutationTail = createDeferred<void>();
      (
        harness.bridgeState as unknown as {
          mutationTail: Promise<void>;
        }
      ).mutationTail = releaseMutationTail.promise;

      const payload = {
        webhookId: "webhook-queued-timeout",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-queued-timeout" },
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      const startedAt = Date.now();
      const responsePromise = fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(body, WEBHOOK_SECRET),
        },
        body,
      });
      let queuedMutationDrain: Promise<void> | undefined;
      try {
        const response = await responsePromise;
        expect(response.status).toBe(503);
        expect(Date.now() - startedAt).toBeLessThan(1_000);
        queuedMutationDrain = (
          harness.bridgeState as unknown as {
            mutationTail: Promise<void>;
          }
        ).mutationTail;
      } finally {
        releaseMutationTail.resolve(undefined);
        await queuedMutationDrain;
      }
      await expect(
        harness.bridgeState.getReceipt(payload.webhookId),
      ).resolves.toBeUndefined();
      expect(runtime.requests).toHaveLength(0);
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining("error=BridgeStateLockTimeoutError"),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("releases a pre-dispatch claim when the dispatch marker fails so a retry can run", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime);
      const harness = activeHarness;
      const originalMarkDispatchStarted =
        harness.bridgeState.markDispatchStarted.bind(harness.bridgeState);
      const markSpy = vi
        .spyOn(harness.bridgeState, "markDispatchStarted")
        .mockImplementation(originalMarkDispatchStarted);
      markSpy.mockRejectedValueOnce(new Error("synthetic marker write failure"));
      const payload = {
        webhookId: "webhook-dispatch-marker-retry",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-dispatch-marker-retry" },
        promptContext: "private retry prompt",
        webhookTimestamp: Date.now(),
      };
      const send = async (): Promise<Response> => {
        const body = JSON.stringify(payload);
        return fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(body, WEBHOOK_SECRET),
          },
          body,
        });
      };

      expect((await send()).status).toBe(200);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "received",
      );
      expect(runtime.requests).toHaveLength(0);

      expect((await send()).status).toBe(200);
      await waitFor(() => runtime.requests.length === 1);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(markSpy).toHaveBeenCalledTimes(2);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("private retry prompt");
      expect(logged).not.toContain("synthetic marker write failure");
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("deduplicates webhook retries and supersedes a second receipt for the same created execution", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "activity", activity: { type: "response", body: "once" } };
      yield { kind: "done" };
    });
    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;
    const basePayload = {
      webhookId: "webhook-dedupe-original",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-dedupe" },
      promptContext: "run exactly once",
      webhookTimestamp: Date.now(),
    };
    const send = async (payload: typeof basePayload): Promise<Response> => {
      const body = JSON.stringify(payload);
      return fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(body, WEBHOOK_SECRET),
        },
        body,
      });
    };

    expect((await send(basePayload)).status).toBe(200);
    await waitFor(() => runtime.requests.length === 1);
    expect((await send(basePayload)).status).toBe(200);
    expect(
      (
        await send({
          ...basePayload,
          webhookId: "webhook-dedupe-crossed-runtime",
          webhookTimestamp: Date.now(),
        })
      ).status,
    ).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(runtime.requests).toHaveLength(1);
    await expect(
      harness.bridgeState.getReceipt("webhook-dedupe-crossed-runtime"),
    ).resolves.toMatchObject({
      status: "superseded",
      supersededByWebhookId: "webhook-dedupe-original",
    });
  });

  it("reclaims and executes a prior-process claim that never started dispatch", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "activity", activity: { type: "response", body: "recovered" } };
      yield { kind: "done" };
    });
    activeHarness = await startTestServer(runtime, {
      bridgeStateOwnerId: "runtime-after-restart",
      prepareBridgeState: async (storePath) => {
        const prior = new JsonBridgeStateStore(storePath, {
          ownerId: "runtime-before-crash",
        });
        await prior.claimEvent({
          webhookId: "webhook-reclaim-before-dispatch",
          executionId: "created:agent-session-reclaim-before-dispatch",
          linearSessionId: "agent-session-reclaim-before-dispatch",
          action: "created",
        });
      },
    });
    const harness = activeHarness;
    const payload = {
      webhookId: "webhook-reclaim-before-dispatch",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-reclaim-before-dispatch" },
      promptContext: "safe to retry",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);

    expect(
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(body, WEBHOOK_SECRET),
          },
          body,
        })
      ).status,
    ).toBe(200);
    await waitFor(() => runtime.requests.length === 1);
    await waitFor(async () =>
      (
        await harness.bridgeState.getReceipt(
          "webhook-reclaim-before-dispatch",
        )
      )?.status === "completed",
    );
    await expect(
      harness.bridgeState.getClaim(
        "created:agent-session-reclaim-before-dispatch",
      ),
    ).resolves.toMatchObject({
      ownerId: "runtime-after-restart",
      dispatchStartedAt: expect.any(String),
    });
  });

  it("surfaces a post-dispatch retry as ambiguous without running it again", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    activeHarness = await startTestServer(runtime, {
      bridgeStateOwnerId: "runtime-after-restart",
      prepareBridgeState: async (storePath) => {
        const prior = new JsonBridgeStateStore(storePath, {
          ownerId: "runtime-before-crash",
        });
        await prior.claimEvent({
          webhookId: "webhook-ambiguous-after-dispatch",
          executionId: "created:agent-session-ambiguous-after-dispatch",
          linearSessionId: "agent-session-ambiguous-after-dispatch",
          action: "created",
        });
        await prior.markDispatchStarted("webhook-ambiguous-after-dispatch");
      },
    });
    const harness = activeHarness;
    const payload = {
      webhookId: "webhook-ambiguous-after-dispatch",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-ambiguous-after-dispatch" },
      promptContext: "must not run again",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);

    expect(
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(body, WEBHOOK_SECRET),
          },
          body,
        })
      ).status,
    ).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(runtime.requests).toHaveLength(0);
    expect(harness.calls).toHaveLength(0);
    await expect(
      harness.bridgeState.getReceipt("webhook-ambiguous-after-dispatch"),
    ).resolves.toMatchObject({
      status: "claimed",
      outcome: {
        httpStatus: 200,
        result: "not_dispatched",
        disposition: "ambiguous",
        errorClass: "AmbiguousDispatch",
      },
    });
  });

  it("rejects agent events without the required durable identities", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;
    const payloads = [
      {
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-missing-webhook" },
        webhookTimestamp: Date.now(),
      },
      {
        webhookId: "webhook-missing-activity",
        type: "AgentSessionEvent",
        action: "prompted",
        agentSession: { id: "agent-session-missing-activity" },
        agentActivity: { content: { type: "prompt", body: "hello" } },
        webhookTimestamp: Date.now(),
      },
    ];

    for (const payload of payloads) {
      const body = JSON.stringify(payload);
      const response = await fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(body, WEBHOOK_SECRET),
        },
        body,
      });
      expect(response.status).toBe(400);
    }
    expect(runtime.requests).toHaveLength(0);
  });

  it("logs bounded static diagnostics for invalid JSON and invalid agent events", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      activeHarness = await startTestServer(runtime);
      const harness = activeHarness;
      const invalidJson = '{"secret":"raw-invalid-json-body"';
      const invalidJsonResponse = await fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(invalidJson, WEBHOOK_SECRET),
        },
        body: invalidJson,
      });
      expect(invalidJsonResponse.status).toBe(400);

      const invalidEvent = JSON.stringify({
        type: "AgentSessionEvent",
        action: "prompted",
        agentSession: { id: "session-invalid-event" },
        agentActivity: {
          content: { type: "prompt", body: "raw-invalid-agent-body" },
        },
        webhookTimestamp: Date.now(),
      });
      const invalidEventResponse = await fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(invalidEvent, WEBHOOK_SECRET),
        },
        body: invalidEvent,
      });
      expect(invalidEventResponse.status).toBe(400);

      expect(errorSpy.mock.calls.map((call) => call.join(" "))).toEqual([
        "[linear-agent-bridge] webhook rejected: error=InvalidJson",
        "[linear-agent-bridge] webhook rejected: error=InvalidAgentSessionEvent",
      ]);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("raw-invalid-json-body");
      expect(logged).not.toContain("raw-invalid-agent-body");
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("logs turn lifecycle with bounded session, reason, and queue fields", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    try {
      activeHarness = await startTestServer(runtime);
      const harness = activeHarness;
      const payload = {
        webhookId: "webhook-created-log",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-log", issue: { title: "secret issue title" } },
        promptContext: "secret prompt contents",
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      expect(
        (
          await fetch(serverUrl(harness.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);

      await waitFor(() =>
        logSpy.mock.calls.some((call) => String(call[0]).includes("turn terminal")),
      );
      expect(
        logSpy.mock.calls
          .map((call) => call.join(" "))
          .filter((line) => line.includes("[linear-agent-bridge] turn ")),
      ).toEqual([
        "[linear-agent-bridge] turn start: session=agent-session-log queue=1",
        "[linear-agent-bridge] turn terminal: session=agent-session-log reason=completed queue=0",
      ]);
      const logged = logSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("secret issue title");
      expect(logged).not.toContain("secret prompt contents");
    } finally {
      logSpy.mockRestore();
    }
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
      webhookId: "webhook-prompted-2",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "agent-session-2" },
      // Live payload shape (2026-08-12): text rides the content union.
      agentActivity: {
        id: "activity-prompted-2",
        content: { type: "prompt", body: "please continue" },
      },
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const signature = sign(body, WEBHOOK_SECRET);

    const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
    await waitFor(async () =>
      (await harness.bridgeState.getClaim("activity-prompted-2"))?.status ===
      "completed",
    );
  });

  it("never serializes a prompted activity or body into logs", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    try {
      activeHarness = await startTestServer(runtime);
      const harness = activeHarness;
      const payload = {
        webhookId: "webhook-empty-prompt",
        type: "AgentSessionEvent",
        action: "prompted",
        agentSession: { id: "agent-session-empty-prompt" },
        agentActivity: {
          id: "activity-empty-prompt",
          content: { type: "prompt", body: "" },
          privateMarker: "secret-activity-marker",
        },
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);

      expect(
        (
          await fetch(serverUrl(harness.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);
      await waitFor(() => runtime.requests.length === 1);

      const logged = logSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("secret-activity-marker");
      expect(logged).not.toContain('"body"');
    } finally {
      logSpy.mockRestore();
    }
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
        webhookId: "webhook-stop-created",
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
      await fetch(serverUrl(harness.port, "/webhook"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "linear-signature": sign(createdBody, WEBHOOK_SECRET),
        },
        body: createdBody,
      });
      await waitFor(() => runtime.requests.length === 1);

      const stopPayload = {
        webhookId: "webhook-stop-prompted",
        type: "AgentSessionEvent",
        action: "prompted",
        agentSession: { id: "agent-session-stop" },
        agentActivity: {
          id: "activity-stop-prompted",
          content: { type: "prompt", body: "cancel this run", signal: "stop" },
        },
        webhookTimestamp: Date.now(),
      };
      const stopBody = JSON.stringify(stopPayload);
      const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt("webhook-stop-prompted"))?.status ===
        "completed",
      );
    } finally {
      release.resolve();
    }
  });

  it("a stop received during follow-up setup prevents the turn from being enqueued", async () => {
    const thoughtStarted = createDeferred<void>();
    const releaseThought = createDeferred<void>();
    const thoughtFinished = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      linearFetchImpl: (calls) =>
        (async (_url: RequestInfo | URL, init?: RequestInit) => {
          const parsed = JSON.parse(init?.body as string) as {
            variables: { input: LinearCall };
          };
          const input = parsed.variables.input;
          calls.push({
            agentSessionId: input.agentSessionId,
            content: input.content,
            ...(input.ephemeral !== undefined ? { ephemeral: input.ephemeral } : {}),
          });
          if (input.content.type === "thought" && input.content.body === "Working on it…") {
            thoughtStarted.resolve();
            await releaseThought.promise;
            thoughtFinished.resolve();
          }
          return jsonResponse({ data: { agentActivityCreate: { success: true } } });
        }) as FetchFn,
    });
    const harness = activeHarness;
    await harness.store.put({
      linearSessionId: "agent-session-setup-race",
      runtimeSessionId: "runtime-prior",
      runtime: "fake",
      updatedAt: new Date().toISOString(),
    });

    const promptedPayload = {
      webhookId: "webhook-setup-prompted",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "agent-session-setup-race" },
      agentActivity: {
        id: "activity-setup-prompted",
        content: { type: "prompt", body: "continue the work" },
      },
      webhookTimestamp: Date.now(),
    };
    const promptedBody = JSON.stringify(promptedPayload);
    await fetch(serverUrl(harness.port, "/webhook"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "linear-signature": sign(promptedBody, WEBHOOK_SECRET),
      },
      body: promptedBody,
    });
    await thoughtStarted.promise;

    const stopPayload = {
      webhookId: "webhook-setup-stop",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "agent-session-setup-race" },
      agentActivity: {
        id: "activity-setup-stop",
        content: { type: "prompt", body: "cancel this run", signal: "stop" },
      },
      webhookTimestamp: Date.now(),
    };
    const stopBody = JSON.stringify(stopPayload);
    await fetch(serverUrl(harness.port, "/webhook"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "linear-signature": sign(stopBody, WEBHOOK_SECRET),
      },
      body: stopBody,
    });
    await waitFor(() =>
      harness.calls.some(
        (call) => call.content.type === "response" && call.content.body === "Stopped.",
      ),
    );

    releaseThought.resolve();
    await thoughtFinished.promise;
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt(promptedPayload.webhookId))?.status ===
      "completed",
    );
    expect(runtime.requests).toHaveLength(0);
  });

  it("aborts a turn after the configured inactivity period and reports it", async () => {
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
      configOverrides: { runInactivityTimeoutMs: 25 },
    });
    const harness = activeHarness;

    try {
      const payload = {
        webhookId: "webhook-timeout",
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
      const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
            call.content.body === "This request was inactive for 25 ms and was stopped.",
        ),
      );
      expect(runtime.requests[0]?.abortController?.signal.aborted).toBe(true);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt("webhook-timeout"))?.status ===
        "failed",
      );
      await expect(
        harness.bridgeState.getReceipt("webhook-timeout"),
      ).resolves.toMatchObject({
        outcome: {
          httpStatus: 200,
          result: "processing_failed",
          disposition: "claimed",
          errorClass: "RuntimeTimeout",
        },
      });
    } finally {
      release.resolve();
    }
  });

  it("allows a long turn with runtime activity and starts a queued turn's watchdog only when it executes", async () => {
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      if (request.linearSessionId === "agent-session-active-long") {
        await new Promise((resolve) => setTimeout(resolve, 40));
        yield { kind: "progress" };
        await new Promise((resolve) => setTimeout(resolve, 40));
        yield { kind: "session-started", runtimeSessionId: "runtime-active-long" };
        await new Promise((resolve) => setTimeout(resolve, 40));
      }
      yield {
        kind: "activity",
        activity: { type: "response", body: `completed ${request.linearSessionId}` },
      };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 75 },
    });
    const harness = activeHarness;

    for (const sessionId of ["agent-session-active-long", "agent-session-waiting"]) {
      const payload = {
        webhookId: `webhook-${sessionId}`,
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: sessionId, issue: { title: "Active long request" } },
        promptContext: sessionId,
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      expect(
        (
          await fetch(serverUrl(harness.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);
    }

    await waitFor(
      () =>
        harness.calls.some(
          (call) =>
            call.content.type === "response" &&
            call.content.body === "completed agent-session-waiting",
        ),
      500,
    );
    expect(
      harness.calls.some(
        (call) =>
          call.content.type === "error" &&
          call.content.body.includes("was inactive"),
      ),
    ).toBe(false);
    expect(
      harness.calls.filter(
        (call) => call.agentSessionId === "agent-session-active-long",
      ),
    ).toEqual([
      {
        agentSessionId: "agent-session-active-long",
        content: { type: "thought", body: "Reading the issue and gathering context…" },
        ephemeral: true,
      },
      {
        agentSessionId: "agent-session-active-long",
        content: {
          type: "response",
          body: "completed agent-session-active-long",
        },
      },
    ]);
  });

  it("ends a turn immediately on done without resetting the watchdog or accepting later events", async () => {
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      if (request.linearSessionId === "agent-session-done") {
        yield { kind: "progress" };
        yield { kind: "done" };
        await new Promise((resolve) => setTimeout(resolve, 100));
        yield {
          kind: "activity",
          activity: { type: "response", body: "late after done" },
        };
        return;
      }
      yield {
        kind: "activity",
        activity: { type: "response", body: "queued after done" },
      };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 50 },
    });
    const harness = activeHarness;

    for (const sessionId of ["agent-session-done", "agent-session-after-done"]) {
      const payload = {
        webhookId: `webhook-${sessionId}`,
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: sessionId, issue: { title: "Done is terminal" } },
        promptContext: "finish",
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      expect(
        (
          await fetch(serverUrl(harness.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);
    }

    await waitFor(() =>
      harness.calls.some(
        (call) =>
          call.content.type === "response" && call.content.body === "queued after done",
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 75));
    expect(
      harness.calls.some(
        (call) =>
          call.content.type === "response" && call.content.body === "late after done",
      ),
    ).toBe(false);
    expect(
      harness.calls.some(
        (call) => call.content.type === "error" && call.content.body.includes("was inactive"),
      ),
    ).toBe(false);
  });

  it("aborts an in-flight turn activity delivery after inactivity", async () => {
    const activityStarted = createDeferred<void>();
    let activitySignal: AbortSignal | null | undefined;
    let activityCompleted = false;
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield {
        kind: "activity",
        activity: { type: "response", body: "activity delivery that hangs" },
      };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 30 },
      linearFetchImpl: (calls) =>
        (async (_url: RequestInfo | URL, init?: RequestInit) => {
          const parsed = JSON.parse(init?.body as string) as {
            variables: { input: LinearCall };
          };
          const input = parsed.variables.input;
          calls.push({
            agentSessionId: input.agentSessionId,
            content: input.content,
            ...(input.ephemeral !== undefined ? { ephemeral: input.ephemeral } : {}),
          });
          if (
            input.content.type === "response" &&
            input.content.body === "activity delivery that hangs"
          ) {
            activitySignal = init?.signal;
            activityStarted.resolve();
            await new Promise<void>((_resolve, reject) => {
              const rejectAbort = (): void => {
                const error = new Error("activity delivery aborted");
                error.name = "AbortError";
                reject(error);
              };
              if (activitySignal?.aborted === true) {
                rejectAbort();
              } else {
                activitySignal?.addEventListener("abort", rejectAbort, { once: true });
              }
            });
            activityCompleted = true;
          }
          return jsonResponse({ data: { agentActivityCreate: { success: true } } });
        }) as FetchFn,
    });
    const harness = activeHarness;
    const payload = {
      webhookId: "webhook-activity-timeout",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-activity-timeout", issue: { title: "Hang" } },
      promptContext: "deliver a response",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);

    expect(
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(body, WEBHOOK_SECRET),
          },
          body,
        })
      ).status,
    ).toBe(200);
    await activityStarted.promise;
    await waitFor(() =>
      harness.calls.some(
        (call) =>
          call.agentSessionId === "agent-session-activity-timeout" &&
          call.content.type === "error" &&
          call.content.body === "This request was inactive for 30 ms and was stopped.",
      ),
    );

    expect(activitySignal?.aborted).toBe(true);
    expect(activityCompleted).toBe(false);
  });

  it("inactivity releases the serial queue and ignores late runtime events", async () => {
    const releaseFirst = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      yield {
        kind: "session-started",
        runtimeSessionId: `runtime-${request.linearSessionId}`,
      };
      if (request.linearSessionId === "agent-session-hard-timeout") {
        await releaseFirst.promise;
        yield {
          kind: "activity",
          activity: { type: "response", body: "late response must be ignored" },
        };
        return;
      }
      yield {
        kind: "activity",
        activity: { type: "response", body: "queued turn completed" },
      };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 30 },
      linearFetchImpl: (calls) =>
        (async (_url: RequestInfo | URL, init?: RequestInit) => {
          const parsed = JSON.parse(init?.body as string) as {
            variables: { input: LinearCall };
          };
          const input = parsed.variables.input;
          calls.push({
            agentSessionId: input.agentSessionId,
            content: input.content,
            ...(input.ephemeral !== undefined ? { ephemeral: input.ephemeral } : {}),
          });
          if (
            input.content.type === "error" &&
            input.content.body === "This request was inactive for 30 ms and was stopped."
          ) {
            return await new Promise<Response>(() => {});
          }
          return jsonResponse({ data: { agentActivityCreate: { success: true } } });
        }) as FetchFn,
    });
    const harness = activeHarness;

    try {
      for (const sessionId of ["agent-session-hard-timeout", "agent-session-queued"]) {
        const payload = {
          webhookId: `webhook-${sessionId}`,
          type: "AgentSessionEvent",
          action: "created",
          agentSession: { id: sessionId, issue: { title: "Bounded request" } },
          promptContext: sessionId,
          webhookTimestamp: Date.now(),
        };
        const body = JSON.stringify(payload);
        expect(
          (
            await fetch(serverUrl(harness.port, "/webhook"), {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "linear-signature": sign(body, WEBHOOK_SECRET),
              },
              body,
            })
          ).status,
        ).toBe(200);
      }

      await waitFor(
        () =>
          runtime.requests.length === 2 &&
          harness.calls.some(
            (call) =>
              call.agentSessionId === "agent-session-queued" &&
              call.content.type === "response" &&
              call.content.body === "queued turn completed",
          ),
        250,
      );

      const timeoutCalls = harness.calls.filter(
        (call) =>
          call.agentSessionId === "agent-session-hard-timeout" &&
          call.content.type === "error" &&
          call.content.body === "This request was inactive for 30 ms and was stopped.",
      );
      expect(timeoutCalls).toHaveLength(1);

      releaseFirst.resolve();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(
        harness.calls.some(
          (call) =>
            call.content.type === "response" &&
            call.content.body === "late response must be ignored",
        ),
      ).toBe(false);
      expect(
        harness.calls.filter(
          (call) =>
            call.agentSessionId === "agent-session-hard-timeout" &&
            call.content.type === "error" &&
            call.content.body === "This request was inactive for 30 ms and was stopped.",
        ),
      ).toHaveLength(1);
    } finally {
      releaseFirst.resolve();
    }
  });

  it("force-closes an inactive runtime before the next queued turn starts", async () => {
    const releaseFirst = createDeferred<void>();
    const order: string[] = [];
    const runtime: AgentRuntime = {
      name: "force-close-aware",
      forceCloseSession(request): void {
        order.push(`closed:${request.linearSessionId}`);
      },
      async *runSession(request): AsyncGenerator<RuntimeEvent> {
        order.push(`started:${request.linearSessionId}`);
        if (request.linearSessionId === "agent-session-close-first") {
          await releaseFirst.promise;
          return;
        }
        yield { kind: "done" };
      },
    };
    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 30 },
    });
    const harness = activeHarness;

    try {
      for (const sessionId of ["agent-session-close-first", "agent-session-close-next"]) {
        const payload = {
          webhookId: `webhook-${sessionId}`,
          type: "AgentSessionEvent",
          action: "created",
          agentSession: { id: sessionId, issue: { title: "Force close ordering" } },
          promptContext: sessionId,
          webhookTimestamp: Date.now(),
        };
        const body = JSON.stringify(payload);
        expect(
          (
            await fetch(serverUrl(harness.port, "/webhook"), {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "linear-signature": sign(body, WEBHOOK_SECRET),
              },
              body,
            })
          ).status,
        ).toBe(200);
      }

      await waitFor(() => order.includes("started:agent-session-close-next"), 250);
      expect(order).toEqual([
        "started:agent-session-close-first",
        "closed:agent-session-close-first",
        "started:agent-session-close-next",
      ]);
    } finally {
      releaseFirst.resolve();
    }
  });

  it("closes the first Claude query before starting the next queued Claude turn", async () => {
    const releaseFirst = createDeferred<void>();
    const order: string[] = [];
    const queryFn: QueryFn = ({ prompt }) => {
      order.push(`started:${prompt}`);
      const stream = (async function* () {
        if (prompt === "claude-close-first") {
          await releaseFirst.promise;
        }
      })();
      return Object.assign(stream, {
        close(): void {
          order.push(`closed:${prompt}`);
        },
      });
    };
    const runtime = new ClaudeRuntime("/tmp/kb-unused", queryFn);
    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 30 },
    });
    const harness = activeHarness;

    try {
      for (const sessionId of ["claude-close-first", "claude-close-next"]) {
        const payload = {
          webhookId: `webhook-${sessionId}`,
          type: "AgentSessionEvent",
          action: "created",
          agentSession: { id: sessionId, issue: { title: "Claude close ordering" } },
          promptContext: sessionId,
          webhookTimestamp: Date.now(),
        };
        const body = JSON.stringify(payload);
        expect(
          (
            await fetch(serverUrl(harness.port, "/webhook"), {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "linear-signature": sign(body, WEBHOOK_SECRET),
              },
              body,
            })
          ).status,
        ).toBe(200);
      }

      await waitFor(() => order.includes("started:claude-close-next"), 250);
      expect(order).toEqual([
        "started:claude-close-first",
        "closed:claude-close-first",
        "started:claude-close-next",
      ]);
    } finally {
      releaseFirst.resolve();
    }
  });

  it("rejects an invalid signature with 401 and enqueues nothing", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;

    const payload = {
      webhookId: "webhook-bad-signature",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-bad-sig", issue: { id: "i", identifier: "MPI-3", title: "t" } },
      promptContext: "ctx",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const badSignature = sign(body, "wrong-secret");

    const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
      webhookId: "webhook-runtime-error",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "agent-session-err", issue: { id: "i", identifier: "MPI-4", title: "t" } },
      promptContext: "ctx",
      webhookTimestamp: Date.now(),
    };
    const body = JSON.stringify(payload);
    const signature = sign(body, WEBHOOK_SECRET);

    const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt("webhook-runtime-error"))?.status ===
      "failed",
    );
    await expect(
      harness.bridgeState.getReceipt("webhook-runtime-error"),
    ).resolves.toMatchObject({
      outcome: {
        httpStatus: 200,
        result: "processing_failed",
        disposition: "claimed",
        errorClass: "RuntimeExecutionError",
      },
    });

    const health = await fetch(serverUrl(harness.port, "/healthz"));
    expect(health.status).toBe(200);
    expect(await health.text()).toBe("ok");
  });

  it("logs only a bounded error class when processing fails", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      throw new Error("raw-runtime-error-body");
      // eslint-disable-next-line no-unreachable
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      activeHarness = await startTestServer(runtime);
      const harness = activeHarness;
      const payload = {
        webhookId: "webhook-bounded-processing-error",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-bounded-processing-error" },
        promptContext: "raw-secret-prompt-body",
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      expect(
        (
          await fetch(serverUrl(harness.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);
      await waitFor(async () =>
        (
          await harness.bridgeState.getReceipt(
            "webhook-bounded-processing-error",
          )
        )?.status === "failed",
      );

      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).toContain("error=RuntimeExecutionError");
      expect(logged).not.toContain("raw-runtime-error-body");
      expect(logged).not.toContain("raw-secret-prompt-body");
    } finally {
      errorSpy.mockRestore();
    }
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
        serverUrl(harness.port, `/oauth/callback?code=auth-code-xyz&state=${state}`),
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
        "[linear-agent-bridge] OAuth token pair installed",
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
      serverUrl(harness.port, "/oauth/callback?code=auth-code-xyz"),
    );
    expect(missingState.status).toBe(400);
    expect(harness.tokenFetch).not.toHaveBeenCalled();

    const state = new URL(await harness.authorizationUrl).searchParams.get("state");
    expect(state).not.toBeNull();
    const callbackUrl = serverUrl(
      harness.port,
      `/oauth/callback?code=auth-code-xyz&state=${state}`,
    );

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

    const response = await fetch(serverUrl(harness.port, "/webhook"), {
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
