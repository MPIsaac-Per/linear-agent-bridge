import { createHmac } from "node:crypto";
import { EventEmitter } from "node:events";
import { promises as fsPromises } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startServer, type ServerDeps } from "../src/server.js";
import {
  awaitReadyOrShutdown,
  installGracefulShutdown,
} from "../src/shutdown.js";
import type { Config } from "../src/config.js";
import { LinearAgentClient, type FetchFn } from "../src/linear/client.js";
import { LinearOAuthTokenManager } from "../src/linear/oauth.js";
import { JsonSessionStore } from "../src/sessions/store.js";
import {
  BridgeStateIntegrityError,
  DispatchMarkerDurabilityError,
  JsonBridgeStateStore,
  type JsonBridgeStateStoreOptions,
} from "../src/state/store.js";
import {
  createIngressRecoveryKeyring,
  IngressRecoveryEnvelopeError,
} from "../src/state/recovery-envelope.js";
import { SerialQueue } from "../src/queue.js";
import { ClaudeRuntime, type QueryFn } from "../src/runtime/claude.js";
import type {
  AgentActivityContent,
  AgentRuntime,
  RuntimeEvent,
  SessionRequest,
} from "../src/types.js";

const WEBHOOK_SECRET = "whsec_test_secret";
const INGRESS_RECOVERY_KEY = "A".repeat(43);

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
    ingressRecoveryKey: INGRESS_RECOVERY_KEY,
    ingressRecoveryPreviousKeys: [],
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

function observableFailureResponse(
  status: number,
  statusText: string,
  secretBody: string,
  onCancel: () => void,
): Response {
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(secretBody));
      },
      cancel() {
        onCancel();
      },
    }),
    { status, statusText },
  );
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
  ready: Promise<void>;
  close: () => Promise<void>;
  tmpDir: string;
  calls: LinearCall[];
  store: JsonSessionStore;
  bridgeState: JsonBridgeStateStore;
  bridgeStatePath: string;
  queue: SerialQueue;
  activityIds: string[];
  tokenFetch: ReturnType<typeof vi.fn>;
  oauth: LinearOAuthTokenManager;
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
    tmpDir?: string;
    removeTmpDirOnClose?: boolean;
    schedulePostResponseWork?: (work: () => void) => void;
    recoveryKey?: string;
    recoveryPreviousKeys?: string[];
    awaitReady?: boolean;
    afterStart?: (server: ReturnType<typeof startServer>) => Promise<void>;
    linearUsesOAuth?: boolean;
    prepareOAuthTokenStore?: (storePath: string) => Promise<void>;
    shutdownRequestTimeoutMs?: number;
  } = {},
): Promise<Harness> {
  const calls: LinearCall[] = [];
  const activityIds: string[] = [];

  const tmpDir =
    options.tmpDir ??
    (await fsPromises.mkdtemp(path.join(os.tmpdir(), "server-test-")));
  const store = new JsonSessionStore(path.join(tmpDir, "sessions.json"));
  const bridgeStatePath = path.join(tmpDir, "bridge-state.json");
  if (options.makeBridgeStatePathDirectory === true) {
    await fsPromises.mkdir(bridgeStatePath);
  }
  await options.prepareBridgeState?.(bridgeStatePath);
  const bridgeState = new JsonBridgeStateStore(bridgeStatePath, {
    ...options.bridgeStateOptions,
    recoveryKeyring: createIngressRecoveryKeyring(
      options.recoveryKey ?? INGRESS_RECOVERY_KEY,
      options.recoveryPreviousKeys ?? [],
    ),
    ...(options.bridgeStateOwnerId !== undefined
      ? { ownerId: options.bridgeStateOwnerId }
      : {}),
  });
  const oauthTokenStorePath = path.join(tmpDir, "oauth-tokens.json");
  await options.prepareOAuthTokenStore?.(oauthTokenStorePath);

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
  const linear = new LinearAgentClient(
    options.linearUsesOAuth === true ? oauth : "test-linear-token",
    options.linearFetchImpl?.(calls) ?? fakeLinearFetch(calls, activityIds),
  );
  const deps = {
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
    ...(options.shutdownRequestTimeoutMs !== undefined
      ? { shutdownRequestTimeoutMs: options.shutdownRequestTimeoutMs }
      : {}),
    ...(options.schedulePostResponseWork !== undefined
      ? { schedulePostResponseWork: options.schedulePostResponseWork }
      : {}),
  } as ServerDeps;

  const server = startServer(deps);
  try {
    await options.afterStart?.(server);
    if (options.awaitReady !== false) {
      await server.ready;
    }
  } catch (error) {
    await server.close().catch(() => undefined);
    if (options.removeTmpDirOnClose !== false) {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
    throw error;
  }
  const port = await listening;

  return {
    port,
    ready: server.ready,
    close: async () => {
      await server.close();
      // Drain session finalizers before removing the temp dir, or the store's
      // atomic-write temp file can race the rm (ENOTEMPTY).
      await queue.enqueue(async () => {});
      if (options.removeTmpDirOnClose !== false) {
        await fsPromises.rm(tmpDir, { recursive: true, force: true });
      }
    },
    tmpDir,
    calls,
    store,
    bridgeState,
    bridgeStatePath,
    queue,
    activityIds,
    tokenFetch,
    oauth,
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
  it("closes cleanly while listen is still pending and never starts later work", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let closeElapsedMs = Number.POSITIVE_INFINITY;

    await expect(
      startTestServer(runtime, {
        afterStart: async (server) => {
          const startedAt = Date.now();
          await server.close();
          closeElapsedMs = Date.now() - startedAt;
        },
      }),
    ).rejects.toThrow("Server shutting down");
    expect(closeElapsedMs).toBeLessThan(1_000);
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(runtime.requests).toHaveLength(0);
  });

  it("rejects ready promptly when the configured port is already occupied", async () => {
    const occupied = createHttpServer();
    await new Promise<void>((resolve, reject) => {
      occupied.once("error", reject);
      occupied.listen(0, resolve);
    });
    const address = occupied.address();
    if (address === null || typeof address === "string") {
      throw new Error("occupied test listener did not expose a port");
    }
    try {
      await expect(
        startTestServer(
          new FakeRuntime(async function* () {
            yield { kind: "done" } as RuntimeEvent;
          }),
          { configOverrides: { port: address.port } },
        ),
      ).rejects.toThrow("Bridge HTTP listener could not start");
    } finally {
      await new Promise<void>((resolve, reject) => {
        occupied.close((error) => (error === undefined ? resolve() : reject(error)));
      });
    }
  });

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

  it("keeps ingress unavailable when durable receipt persistence cannot be initialized", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime, {
        makeBridgeStatePathDirectory: true,
        awaitReady: false,
      });
      const harness = activeHarness;
      let readySettled = false;
      void harness.ready.then(
        () => {
          readySettled = true;
        },
        () => {
          readySettled = true;
        },
      );
      await waitFor(() =>
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("transient ingress recovery failure"),
        ),
      );
      expect(readySettled).toBe(false);
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

  it("fails closed when persisted bridge state fails integrity validation", async () => {
    const secret = "private-corrupt-state-value";
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        prepareBridgeState: async (storePath) => {
          await fsPromises.writeFile(
            storePath,
            JSON.stringify({ version: 1, receipts: secret }),
            { mode: 0o600 },
          );
        },
      });
      const harness = activeHarness;
      await expect(harness.ready).rejects.toBeInstanceOf(
        BridgeStateIntegrityError,
      );
      expect(
        (await fetch(serverUrl(harness.port, "/healthz"))).status,
      ).toBe(503);
      const payload = {
        webhookId: "webhook-corrupt-state",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-corrupt-state" },
        promptContext: "private-corrupt-state-webhook-body",
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
      ).toBe(503);
      expect(runtime.requests).toHaveLength(0);
      expect(await fsPromises.readFile(harness.bridgeStatePath, "utf8")).toContain(
        secret,
      );
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).toContain("error=BridgeStateIntegrityError");
      expect(logged).not.toContain(secret);
      expect(logged).not.toContain(payload.promptContext);
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("recovers an accepted created event after restart before dispatch without another webhook", async () => {
    const sharedTmpDir = await fsPromises.mkdtemp(
      path.join(os.tmpdir(), "server-accepted-created-restart-"),
    );
    const pendingPostResponseWork: Array<() => void> = [];
    const firstRuntime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    const prompt = "private created prompt recovered exactly once";

    try {
      activeHarness = await startTestServer(firstRuntime, {
        tmpDir: sharedTmpDir,
        removeTmpDirOnClose: false,
        schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
      });
      const first = activeHarness;
      const payload = {
        webhookId: "webhook-created-crash-before-dispatch",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: {
          id: "session-created-crash-before-dispatch",
          issue: {
            id: "issue-created-crash-before-dispatch",
            identifier: "MPI-1448",
            title: "Recover accepted work",
          },
        },
        promptContext: prompt,
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);

      expect(
        (
          await fetch(serverUrl(first.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);
      expect(pendingPostResponseWork).toHaveLength(1);
      expect(firstRuntime.requests).toHaveLength(0);
      expect(await fsPromises.readFile(first.bridgeStatePath, "utf8")).not.toContain(
        prompt,
      );

      await first.close();
      activeHarness = undefined;

      const recoveredRuntime = new FakeRuntime(async function* () {
        yield { kind: "done" } as RuntimeEvent;
      });
      activeHarness = await startTestServer(recoveredRuntime, {
        tmpDir: sharedTmpDir,
        removeTmpDirOnClose: false,
        bridgeStateOwnerId: "runtime-after-created-crash",
      });
      const recovered = activeHarness;
      await waitFor(() => recoveredRuntime.requests.length === 1);

      expect(recoveredRuntime.requests).toEqual([
        expect.objectContaining({
          linearSessionId: "session-created-crash-before-dispatch",
          prompt,
        }),
      ]);
      await waitFor(async () =>
        (await recovered.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(await fsPromises.readFile(recovered.bridgeStatePath, "utf8")).not.toContain(
        prompt,
      );
    } finally {
      await activeHarness?.close();
      activeHarness = undefined;
      await fsPromises.rm(sharedTmpDir, { recursive: true, force: true });
    }
  });

  it("retries a transient startup recovery scan failure without another webhook", async () => {
    const prompt = "private prompt survives transient startup recovery failure";
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let recoveryAssertSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        bridgeStateOwnerId: "runtime-after-transient-recovery-failure",
        prepareBridgeState: async (storePath) => {
          const prior = new JsonBridgeStateStore(storePath, {
            ownerId: "runtime-before-transient-recovery-failure",
            recoveryKeyring: createIngressRecoveryKeyring(
              INGRESS_RECOVERY_KEY,
            ),
          });
          await prior.claimEvent(
            {
              webhookId: "webhook-transient-recovery-failure",
              executionId: "created:session-transient-recovery-failure",
              linearSessionId: "session-transient-recovery-failure",
              action: "created",
            },
            {
              action: "created",
              prompt,
              occurredAt: "2026-08-18T12:00:00.000Z",
            },
          );
          const original =
            JsonBridgeStateStore.prototype.assertRecoverableEventsAvailable;
          let failedOnce = false;
          recoveryAssertSpy = vi
            .spyOn(
              JsonBridgeStateStore.prototype,
              "assertRecoverableEventsAvailable",
            )
            .mockImplementation(async function () {
              if (!failedOnce) {
                failedOnce = true;
                throw new Error("synthetic transient recovery scan failure");
              }
              return original.call(this);
            });
        },
      });
      const harness = activeHarness;

      await harness.ready;
      await waitFor(() => runtime.requests.length === 1);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(
          "webhook-transient-recovery-failure",
        ))?.status === "completed",
      );
      expect(runtime.requests).toEqual([
        expect.objectContaining({
          linearSessionId: "session-transient-recovery-failure",
          prompt,
        }),
      ]);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain(prompt);
    } finally {
      recoveryAssertSpy?.mockRestore();
      errorSpy.mockRestore();
    }
  });

  it("recovers an accepted prompted event with its exact body and persisted resume session", async () => {
    const sharedTmpDir = await fsPromises.mkdtemp(
      path.join(os.tmpdir(), "server-accepted-prompted-restart-"),
    );
    const pendingPostResponseWork: Array<() => void> = [];
    const prompt = "private prompted body recovered exactly once";
    const payload = {
      webhookId: "webhook-prompted-crash-before-dispatch",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-prompted-crash-before-dispatch" },
      agentActivity: {
        id: "activity-prompted-crash-before-dispatch",
        createdAt: new Date().toISOString(),
        content: { type: "prompt", body: prompt },
      },
      webhookTimestamp: Date.now(),
    };

    try {
      const firstRuntime = new FakeRuntime(async function* () {
        yield { kind: "done" } as RuntimeEvent;
      });
      activeHarness = await startTestServer(firstRuntime, {
        tmpDir: sharedTmpDir,
        removeTmpDirOnClose: false,
        schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
      });
      const first = activeHarness;
      await first.store.put({
        linearSessionId: payload.agentSession.id,
        runtimeSessionId: "runtime-session-before-prompted-crash",
        runtime: "fake",
        issueIdentifier: "MPI-1448",
        updatedAt: new Date().toISOString(),
      });
      const body = JSON.stringify(payload);
      expect(
        (
          await fetch(serverUrl(first.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(body, WEBHOOK_SECRET),
            },
            body,
          })
        ).status,
      ).toBe(200);
      expect(firstRuntime.requests).toHaveLength(0);
      expect(await fsPromises.readFile(first.bridgeStatePath, "utf8")).not.toContain(
        prompt,
      );

      await first.close();
      activeHarness = undefined;
      const recoveredRuntime = new FakeRuntime(async function* () {
        yield { kind: "done" } as RuntimeEvent;
      });
      activeHarness = await startTestServer(recoveredRuntime, {
        tmpDir: sharedTmpDir,
        removeTmpDirOnClose: false,
        bridgeStateOwnerId: "runtime-after-prompted-crash",
      });
      const recovered = activeHarness;
      await waitFor(() => recoveredRuntime.requests.length === 1);
      expect(recoveredRuntime.requests[0]).toEqual(
        expect.objectContaining({
          linearSessionId: payload.agentSession.id,
          prompt,
          resumeSessionId: "runtime-session-before-prompted-crash",
        }),
      );
      await waitFor(async () =>
        (await recovered.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );

      const lateSame = JSON.stringify({
        ...payload,
        webhookTimestamp: Date.now(),
      });
      expect(
        (
          await fetch(serverUrl(recovered.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(lateSame, WEBHOOK_SECRET),
            },
            body: lateSame,
          })
        ).status,
      ).toBe(200);
      const crossed = JSON.stringify({
        ...payload,
        webhookId: "webhook-prompted-late-crossed-delivery",
        webhookTimestamp: Date.now(),
      });
      expect(
        (
          await fetch(serverUrl(recovered.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": sign(crossed, WEBHOOK_SECRET),
            },
            body: crossed,
          })
        ).status,
      ).toBe(200);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(recoveredRuntime.requests).toHaveLength(1);
      await expect(
        recovered.bridgeState.getReceipt(
          "webhook-prompted-late-crossed-delivery",
        ),
      ).resolves.toMatchObject({ status: "superseded" });
    } finally {
      await activeHarness?.close();
      activeHarness = undefined;
      await fsPromises.rm(sharedTmpDir, { recursive: true, force: true });
    }
  });

  it("recovers created then prompted in order and resumes the newly persisted runtime session", async () => {
    const sharedTmpDir = await fsPromises.mkdtemp(
      path.join(os.tmpdir(), "server-accepted-created-prompted-restart-"),
    );
    const pendingPostResponseWork: Array<() => void> = [];
    const now = Date.now();
    const createdPrompt = "private recovered created body";
    const promptedPrompt = "private recovered follow-up body";
    const created = {
      webhookId: "webhook-recovered-created-before-prompt",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-recovered-created-prompted" },
      promptContext: createdPrompt,
      webhookTimestamp: now,
    };
    const prompted = {
      webhookId: "webhook-recovered-prompt-after-created",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-recovered-created-prompted" },
      agentActivity: {
        id: "activity-recovered-prompt-after-created",
        createdAt: new Date(now + 1).toISOString(),
        content: { type: "prompt", body: promptedPrompt },
      },
      webhookTimestamp: now + 1,
    };

    try {
      activeHarness = await startTestServer(
        new FakeRuntime(async function* () {
          yield { kind: "done" } as RuntimeEvent;
        }),
        {
          tmpDir: sharedTmpDir,
          removeTmpDirOnClose: false,
          schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
        },
      );
      const first = activeHarness;
      for (const payload of [created, prompted]) {
        const body = JSON.stringify(payload);
        expect(
          (
            await fetch(serverUrl(first.port, "/webhook"), {
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
      await first.close();
      activeHarness = undefined;

      const recoveredRuntime = new FakeRuntime(async function* (request) {
        if (request.prompt === createdPrompt) {
          yield {
            kind: "session-started",
            runtimeSessionId: "runtime-created-during-recovery",
          } as RuntimeEvent;
        }
        yield { kind: "done" } as RuntimeEvent;
      });
      activeHarness = await startTestServer(recoveredRuntime, {
        tmpDir: sharedTmpDir,
        removeTmpDirOnClose: false,
        bridgeStateOwnerId: "runtime-after-created-prompted-crash",
      });
      await waitFor(() => recoveredRuntime.requests.length === 2);
      expect(recoveredRuntime.requests.map((request) => request.prompt)).toEqual([
        createdPrompt,
        promptedPrompt,
      ]);
      expect(recoveredRuntime.requests[0]?.resumeSessionId).toBeUndefined();
      expect(recoveredRuntime.requests[1]?.resumeSessionId).toBe(
        "runtime-created-during-recovery",
      );
    } finally {
      await activeHarness?.close();
      activeHarness = undefined;
      await fsPromises.rm(sharedTmpDir, { recursive: true, force: true });
    }
  });

  it("establishes a recovered stop fence before dispatching older accepted work", async () => {
    const sharedTmpDir = await fsPromises.mkdtemp(
      path.join(os.tmpdir(), "server-recovered-stop-fence-"),
    );
    const pendingPostResponseWork: Array<() => void> = [];
    const now = Date.now();
    const created = {
      webhookId: "webhook-created-before-recovered-stop",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-recovered-stop-fence" },
      promptContext: "older work must never execute after recovered stop",
      webhookTimestamp: now,
    };
    const stop = {
      webhookId: "webhook-recovered-stop",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-recovered-stop-fence" },
      agentActivity: {
        id: "activity-recovered-stop",
        createdAt: new Date(now + 1).toISOString(),
        content: { type: "prompt", body: "stop", signal: "stop" },
      },
      webhookTimestamp: now + 1,
    };

    try {
      activeHarness = await startTestServer(
        new FakeRuntime(async function* () {
          yield { kind: "done" } as RuntimeEvent;
        }),
        {
          tmpDir: sharedTmpDir,
          removeTmpDirOnClose: false,
          schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
        },
      );
      const first = activeHarness;
      for (const payload of [created, stop]) {
        const body = JSON.stringify(payload);
        expect(
          (
            await fetch(serverUrl(first.port, "/webhook"), {
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
      await first.close();
      activeHarness = undefined;

      const recoveredRuntime = new FakeRuntime(async function* () {
        yield { kind: "done" } as RuntimeEvent;
      });
      activeHarness = await startTestServer(recoveredRuntime, {
        tmpDir: sharedTmpDir,
        removeTmpDirOnClose: false,
        bridgeStateOwnerId: "runtime-after-recovered-stop",
      });
      const recovered = activeHarness;
      expect(recoveredRuntime.requests).toHaveLength(0);
      await expect(
        recovered.bridgeState.getReceipt(created.webhookId),
      ).resolves.toMatchObject({
        status: "superseded",
        supersededByWebhookId: stop.webhookId,
      });
      await expect(
        recovered.bridgeState.getReceipt(stop.webhookId),
      ).resolves.toMatchObject({ status: "completed" });
    } finally {
      await activeHarness?.close();
      activeHarness = undefined;
      await fsPromises.rm(sharedTmpDir, { recursive: true, force: true });
    }
  });

  it("aborts a stalled startup recovery activity and closes promptly", async () => {
    const activityStarted = createDeferred<void>();
    let activityAborted = false;
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, {
      awaitReady: false,
      bridgeStateOwnerId: "runtime-after-stalled-activity",
      prepareBridgeState: async (storePath) => {
        const prior = new JsonBridgeStateStore(storePath, {
          ownerId: "runtime-before-stalled-activity",
          recoveryKeyring: createIngressRecoveryKeyring(INGRESS_RECOVERY_KEY),
        });
        await prior.claimEvent(
          {
            webhookId: "webhook-stalled-recovery-activity",
            executionId: "created:session-stalled-recovery-activity",
            linearSessionId: "session-stalled-recovery-activity",
            action: "created",
          },
          {
            action: "created",
            prompt: "stalled activity recovery prompt",
            occurredAt: "2026-08-18T12:00:00.000Z",
          },
        );
      },
      linearFetchImpl: () =>
        (async (_url, init) => {
          activityStarted.resolve();
          return await new Promise<Response>((_resolve, reject) => {
            const signal = init?.signal;
            signal?.addEventListener(
              "abort",
              () => {
                activityAborted = true;
                reject(signal.reason);
              },
              { once: true },
            );
          });
        }) as FetchFn,
    });
    const harness = activeHarness;
    await activityStarted.promise;

    const startedAt = Date.now();
    await harness.close();
    activeHarness = undefined;
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(activityAborted).toBe(true);
    expect(runtime.requests).toHaveLength(0);
  });

  it("waits for an in-flight OAuth refresh to persist before close completes", async () => {
    const refreshStarted = createDeferred<void>();
    const finishRefresh = createDeferred<Response>();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let activityAttempts = 0;
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        removeTmpDirOnClose: false,
        bridgeStateOwnerId: "runtime-after-stalled-oauth",
        linearUsesOAuth: true,
        prepareOAuthTokenStore: async (storePath) => {
          await fsPromises.writeFile(
            storePath,
            JSON.stringify({
              accessToken: "expired-access-token",
              refreshToken: "refresh-token",
              expiresAt: "2026-08-18T12:00:00.000Z",
            }),
            { mode: 0o600 },
          );
        },
        tokenFetchImpl: (async () => {
          refreshStarted.resolve();
          return await finishRefresh.promise;
        }) as FetchFn,
        linearFetchImpl: () =>
          (async () => {
            activityAttempts += 1;
            return jsonResponse({}, { ok: false, status: 401 });
          }) as FetchFn,
        prepareBridgeState: async (storePath) => {
          const prior = new JsonBridgeStateStore(storePath, {
            ownerId: "runtime-before-stalled-oauth",
            recoveryKeyring: createIngressRecoveryKeyring(
              INGRESS_RECOVERY_KEY,
            ),
          });
          await prior.claimEvent(
            {
              webhookId: "webhook-stalled-recovery-oauth",
              executionId: "created:session-stalled-recovery-oauth",
              linearSessionId: "session-stalled-recovery-oauth",
              action: "created",
            },
            {
              action: "created",
              prompt: "stalled oauth recovery prompt",
              occurredAt: "2026-08-18T12:00:00.000Z",
            },
          );
        },
      });
      harness = activeHarness;
      await refreshStarted.promise;

      let closeSettled = false;
      const closing = harness.close().then(() => {
        closeSettled = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(closeSettled).toBe(false);
      expect(runtime.requests).toHaveLength(0);
      expect(activityAttempts).toBe(1);

      finishRefresh.resolve(
        jsonResponse({
          access_token: "fresh-access-token",
          refresh_token: "fresh-refresh-token",
          expires_in: 86_400,
        }),
      );
      await closing;
      activeHarness = undefined;
      expect(closeSettled).toBe(true);
      expect(await fsPromises.readFile(harness.oauthTokenStorePath, "utf8")).toContain(
        "fresh-refresh-token",
      );
      expect(activityAttempts).toBe(1);
      expect(runtime.requests).toHaveLength(0);
      const receiptAfterClose = await harness.bridgeState.getReceipt(
        "webhook-stalled-recovery-oauth",
      );
      expect(receiptAfterClose).toMatchObject({ status: "claimed" });
      expect(receiptAfterClose).not.toHaveProperty("dispatchStartedAt");
      expect(receiptAfterClose).not.toHaveProperty("completedAt");
      expect(receiptAfterClose).not.toHaveProperty("failedAt");
      expect(await fsPromises.readFile(harness.bridgeStatePath, "utf8")).toContain(
        "recoveryEnvelope",
      );
      expect(
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("recovery processing failed"),
        ),
      ).toBe(false);
    } finally {
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("flushes a pending rotated OAuth pair before graceful close completes", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
        tokenFetchImpl: (async () =>
          jsonResponse({
            access_token: "rotated-close-access",
            refresh_token: "rotated-close-refresh",
            expires_in: 86_400,
          })) as FetchFn,
      });
      harness = activeHarness;
      await harness.oauth.install({
        access_token: "initial-close-access",
        refresh_token: "initial-close-refresh",
        expires_in: 86_400,
      });
      (
        harness.oauth as unknown as {
          persistenceRetryDelayMs: number;
        }
      ).persistenceRetryDelayMs = 60_000;
      const persistenceFailure = new Error("synthetic close token persistence failure");
      const originalRename = fsPromises.rename.bind(fsPromises);
      vi.spyOn(fsPromises, "rename")
        .mockRejectedValueOnce(persistenceFailure)
        .mockImplementation(originalRename);
      await expect(
        harness.oauth.refreshAfterUnauthorized("initial-close-access"),
      ).rejects.toBe(persistenceFailure);

      await harness.close();
      activeHarness = undefined;
      const restarted = new LinearOAuthTokenManager({
        clientId: "client-id-test",
        clientSecret: "client-secret-test",
        initialAccessToken: "unused",
        storePath: harness.oauthTokenStorePath,
      });
      await expect(restarted.getAccessToken()).resolves.toBe(
        "rotated-close-access",
      );
    } finally {
      vi.restoreAllMocks();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("exits after bounded signal shutdown when an OAuth refresh never settles", async () => {
    const refreshStarted = createDeferred<void>();
    const neverRefreshes = new Promise<Response>(() => undefined);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
        tokenFetchImpl: (async () => {
          refreshStarted.resolve();
          return await neverRefreshes;
        }) as FetchFn,
      });
      harness = activeHarness;
      await harness.oauth.install({
        access_token: "initial-never-settles-access",
        refresh_token: "initial-never-settles-refresh",
        expires_in: 86_400,
      });
      void harness.oauth
        .refreshAfterUnauthorized("initial-never-settles-access")
        .catch(() => undefined);
      await refreshStarted.promise;
      const originalFlush = harness.oauth.flushPendingPersistence.bind(
        harness.oauth,
      );
      vi.spyOn(harness.oauth, "flushPendingPersistence").mockImplementation(
        async () => originalFlush(25),
      );
      const signals = new EventEmitter();
      const exitObserved = createDeferred<number>();
      const closeSpy = vi.fn(harness.close);
      installGracefulShutdown(
        { close: closeSpy },
        {
          signalSource: signals,
          exit: (code) => exitObserved.resolve(code),
        },
      );

      signals.emit("SIGTERM");
      signals.emit("SIGTERM");
      await expect(
        Promise.race([
          exitObserved.promise,
          new Promise<never>((_resolve, reject) =>
            setTimeout(() => reject(new Error("signal exit timed out")), 500),
          ),
        ]),
      ).resolves.toBe(1);
      activeHarness = undefined;
      expect(closeSpy).toHaveBeenCalledTimes(1);
      expect(
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("OAuth token flush failed"),
        ),
      ).toBe(true);
    } finally {
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("waits for the queue boundary on close without starting or terminalizing queued ingress", async () => {
    const releaseQueue = createDeferred<void>();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
      });
      harness = activeHarness;
      const occupied = harness.queue.enqueue(async () => {
        await releaseQueue.promise;
      });
      const payload = {
        webhookId: "webhook-queued-during-close",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-queued-during-close" },
        promptContext: "queued work must not start after close",
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
        (await harness!.bridgeState.getClaim(
          "created:session-queued-during-close",
        )) !== undefined,
      );

      let closeSettled = false;
      const closing = harness.close().then(() => {
        closeSettled = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(closeSettled).toBe(false);
      releaseQueue.resolve();
      await occupied;
      await closing;
      activeHarness = undefined;

      expect(runtime.requests).toHaveLength(0);
      const receipt = await harness.bridgeState.getReceipt(payload.webhookId);
      expect(receipt).toMatchObject({
        status: "claimed",
      });
      expect(receipt).not.toHaveProperty("dispatchStartedAt");
      expect(receipt).toHaveProperty("recoveryEnvelope");
      expect(receipt).not.toHaveProperty("completedAt");
      expect(receipt).not.toHaveProperty("failedAt");
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(runtime.requests).toHaveLength(0);
      expect(
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("queued turn finalization failed"),
        ),
      ).toBe(false);
    } finally {
      releaseQueue.resolve();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("closes an uncooperative active runtime promptly without terminal state or diagnostics", async () => {
    const runtimeStarted = createDeferred<void>();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      runtimeStarted.resolve();
      await new Promise<void>(() => undefined);
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
      });
      harness = activeHarness;
      const payload = {
        webhookId: "webhook-uncooperative-runtime-close",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-uncooperative-runtime-close" },
        promptContext: "uncooperative runtime close",
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
      await runtimeStarted.promise;

      const startedAt = Date.now();
      await harness.close();
      activeHarness = undefined;
      expect(Date.now() - startedAt).toBeLessThan(1_000);
      expect(runtime.requests).toHaveLength(1);
      const receipt = await harness.bridgeState.getReceipt(payload.webhookId);
      expect(receipt).toMatchObject({
        status: "claimed",
        dispatchStartedAt: expect.any(String),
      });
      expect(receipt).not.toHaveProperty("completedAt");
      expect(receipt).not.toHaveProperty("failedAt");
      expect(
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("processing failed"),
        ),
      ).toBe(false);
    } finally {
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("rolls back an in-flight runtime intent when close wins before runtime invocation", async () => {
    const pendingPostResponseWork: Array<() => void> = [];
    const markerEntered = createDeferred<void>();
    const releaseMarker = createDeferred<void>();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
        schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
      });
      harness = activeHarness;
      const originalMark = harness.bridgeState.markDispatchStarted.bind(
        harness.bridgeState,
      );
      vi.spyOn(harness.bridgeState, "markDispatchStarted").mockImplementation(
        async (webhookId) => {
          markerEntered.resolve();
          await releaseMarker.promise;
          return await originalMark(webhookId);
        },
      );
      const payload = {
        webhookId: "webhook-marker-in-flight-during-close",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-marker-in-flight-during-close" },
        promptContext: "marker must settle before close returns",
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
      pendingPostResponseWork[0]!();
      await markerEntered.promise;

      let closeSettled = false;
      const closing = harness.close().then(() => {
        closeSettled = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(closeSettled).toBe(false);
      releaseMarker.resolve();
      await closing;
      activeHarness = undefined;

      expect(runtime.requests).toHaveLength(0);
      const receipt = await harness.bridgeState.getReceipt(payload.webhookId);
      expect(receipt).toMatchObject({
        status: "claimed",
        recoveryEnvelope: expect.any(Object),
      });
      expect(receipt).not.toHaveProperty("dispatchStartedAt");
      expect(receipt).not.toHaveProperty("completedAt");
      expect(receipt).not.toHaveProperty("failedAt");
      await new Promise((resolve) => setTimeout(resolve, 20));
      await expect(
        harness.bridgeState.getReceipt(payload.webhookId),
      ).resolves.toEqual(receipt);
      expect(errorSpy).not.toHaveBeenCalledWith(
        expect.stringContaining("processing failed"),
      );
    } finally {
      releaseMarker.resolve();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("rolls back a persisted runtime intent when SIGTERM starts graceful shutdown", async () => {
    const markerEntered = createDeferred<void>();
    const releaseMarker = createDeferred<void>();
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
      });
      harness = activeHarness;
      const originalMark = harness.bridgeState.markDispatchStarted.bind(
        harness.bridgeState,
      );
      vi.spyOn(harness.bridgeState, "markDispatchStarted").mockImplementation(
        async (webhookId) => {
          const result = await originalMark(webhookId);
          markerEntered.resolve();
          await releaseMarker.promise;
          return result;
        },
      );
      const signals = new EventEmitter();
      const exitCodes: number[] = [];
      const closeSpy = vi.fn(harness.close);
      const lifecycle = installGracefulShutdown(
        { close: closeSpy },
        { signalSource: signals, exit: (code) => exitCodes.push(code) },
      );
      const payload = {
        webhookId: "webhook-sigterm-runtime-intent",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-sigterm-runtime-intent" },
        promptContext: "SIGTERM must roll back before runtime",
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
      await markerEntered.promise;

      signals.emit("SIGTERM");
      signals.emit("SIGTERM");
      const closing = lifecycle.shutdown();
      expect(lifecycle.shutdown()).toBe(closing);
      releaseMarker.resolve();
      await closing;
      activeHarness = undefined;
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(exitCodes).toEqual([0]);
      expect(closeSpy).toHaveBeenCalledTimes(1);
      expect(runtime.requests).toHaveLength(0);
      const receipt = await harness.bridgeState.getReceipt(payload.webhookId);
      expect(receipt).toMatchObject({
        status: "claimed",
        recoveryEnvelope: expect.any(Object),
      });
      expect(receipt).not.toHaveProperty("dispatchStartedAt");
      lifecycle.dispose();
    } finally {
      releaseMarker.resolve();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("confirms and rolls back an uncertain runtime intent after close begins", async () => {
    const markerEntered = createDeferred<void>();
    const releaseMarker = createDeferred<void>();
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, { removeTmpDirOnClose: false });
    const harness = activeHarness;
    const originalMark = harness.bridgeState.markDispatchStarted.bind(
      harness.bridgeState,
    );
    let markerAttempts = 0;
    vi.spyOn(harness.bridgeState, "markDispatchStarted").mockImplementation(
      async (webhookId) => {
        markerAttempts += 1;
        const result = await originalMark(webhookId);
        if (markerAttempts === 1) {
          markerEntered.resolve();
          await releaseMarker.promise;
          throw new DispatchMarkerDurabilityError();
        }
        return result;
      },
    );
    const payload = {
      webhookId: "webhook-marker-retry-close",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-marker-retry-close" },
      promptContext: "marker retry must stop during close",
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
    await markerEntered.promise;

    const closing = harness.close();
    releaseMarker.resolve();
    await expect(
      Promise.race([
        closing,
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error("server close timed out")), 750),
        ),
      ]),
    ).resolves.toBeUndefined();
    activeHarness = undefined;
    expect(markerAttempts).toBe(2);
    expect(runtime.requests).toHaveLength(0);
    const receipt = await harness.bridgeState.getReceipt(payload.webhookId);
    expect(receipt).toMatchObject({
      status: "claimed",
      recoveryEnvelope: expect.any(Object),
    });
    expect(receipt).not.toHaveProperty("dispatchStartedAt");
    const restarted = new JsonBridgeStateStore(harness.bridgeStatePath, {
      ownerId: "runtime-after-marker-close",
      recoveryKeyring: createIngressRecoveryKeyring(INGRESS_RECOVERY_KEY),
    });
    await expect(restarted.listRecoverableEvents()).resolves.toEqual([
      expect.objectContaining({
        identity: expect.objectContaining({ webhookId: payload.webhookId }),
        available: true,
      }),
    ]);
    await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
  });

  it("confirms and rolls back when close aborts marker retry backoff", async () => {
    const firstMarkerFailed = createDeferred<void>();
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, { removeTmpDirOnClose: false });
    const harness = activeHarness;
    const originalMark = harness.bridgeState.markDispatchStarted.bind(
      harness.bridgeState,
    );
    let markerAttempts = 0;
    vi.spyOn(harness.bridgeState, "markDispatchStarted").mockImplementation(
      async (webhookId) => {
        markerAttempts += 1;
        const result = await originalMark(webhookId);
        if (markerAttempts === 1) {
          firstMarkerFailed.resolve();
          throw new DispatchMarkerDurabilityError();
        }
        return result;
      },
    );
    const payload = {
      webhookId: "webhook-marker-backoff-close",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-marker-backoff-close" },
      promptContext: "marker backoff must confirm during close",
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
    await firstMarkerFailed.promise;
    await new Promise((resolve) => setTimeout(resolve, 1));

    await expect(
      Promise.race([
        harness.close(),
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error("server close timed out")), 750),
        ),
      ]),
    ).resolves.toBeUndefined();
    activeHarness = undefined;
    expect(markerAttempts).toBe(2);
    expect(runtime.requests).toHaveLength(0);
    const receipt = await harness.bridgeState.getReceipt(payload.webhookId);
    expect(receipt).toMatchObject({
      status: "claimed",
      recoveryEnvelope: expect.any(Object),
    });
    expect(receipt).not.toHaveProperty("dispatchStartedAt");
    await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
  });

  it("bounds close when final runtime-intent confirmation remains uncertain", async () => {
    const markerEntered = createDeferred<void>();
    const releaseMarker = createDeferred<void>();
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, { removeTmpDirOnClose: false });
    const harness = activeHarness;
    const originalMark = harness.bridgeState.markDispatchStarted.bind(
      harness.bridgeState,
    );
    let markerAttempts = 0;
    vi.spyOn(harness.bridgeState, "markDispatchStarted").mockImplementation(
      async (webhookId) => {
        markerAttempts += 1;
        if (markerAttempts > 2) {
          throw new Error("marker retry guard tripped");
        }
        await originalMark(webhookId);
        if (markerAttempts === 1) {
          markerEntered.resolve();
          await releaseMarker.promise;
        }
        throw new DispatchMarkerDurabilityError();
      },
    );
    const payload = {
      webhookId: "webhook-marker-final-confirmation-close",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-marker-final-confirmation-close" },
      promptContext: "final marker confirmation remains uncertain",
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
    await markerEntered.promise;

    const closing = harness.close();
    releaseMarker.resolve();
    await expect(
      Promise.race([
        closing,
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error("server close timed out")), 750),
        ),
      ]),
    ).resolves.toBeUndefined();
    activeHarness = undefined;
    expect(markerAttempts).toBe(2);
    expect(runtime.requests).toHaveLength(0);
    await expect(harness.bridgeState.getReceipt(payload.webhookId)).resolves.toMatchObject({
      status: "claimed",
      dispatchStartedAt: expect.any(String),
      recoveryEnvelope: expect.any(Object),
    });
    const restarted = new JsonBridgeStateStore(harness.bridgeStatePath, {
      ownerId: "runtime-after-uncertain-marker-close",
      recoveryKeyring: createIngressRecoveryKeyring(INGRESS_RECOVERY_KEY),
    });
    await expect(restarted.listRecoverableEvents()).resolves.toEqual([]);
    await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
  });

  it("rolls back a delayed runtime intent when a stop wins before runtime invocation", async () => {
    const markerEntered = createDeferred<void>();
    const releaseMarker = createDeferred<void>();
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;
    const originalMark = harness.bridgeState.markDispatchStarted.bind(
      harness.bridgeState,
    );
    vi.spyOn(harness.bridgeState, "markDispatchStarted").mockImplementation(
      async (webhookId) => {
        const result = await originalMark(webhookId);
        if (webhookId === "webhook-stop-during-runtime-intent") {
          markerEntered.resolve();
          await releaseMarker.promise;
        }
        return result;
      },
    );
    const created = {
      webhookId: "webhook-stop-during-runtime-intent",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-stop-during-runtime-intent" },
      promptContext: "private delayed runtime intent prompt",
      webhookTimestamp: Date.now(),
    };
    const createdBody = JSON.stringify(created);
    expect(
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(createdBody, WEBHOOK_SECRET),
          },
          body: createdBody,
        })
      ).status,
    ).toBe(200);
    await markerEntered.promise;

    const stop = {
      webhookId: "webhook-stop-wins-runtime-intent",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-stop-during-runtime-intent" },
      agentActivity: {
        id: "activity-stop-wins-runtime-intent",
        createdAt: new Date().toISOString(),
        content: { type: "prompt", body: "stop", signal: "stop" },
      },
      webhookTimestamp: Date.now(),
    };
    const stopBody = JSON.stringify(stop);
    expect(
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(stopBody, WEBHOOK_SECRET),
          },
          body: stopBody,
        })
      ).status,
    ).toBe(200);
    await waitFor(() =>
      harness.calls.some(
        (call) =>
          call.content.type === "response" && call.content.body === "Stopped.",
      ),
    );
    releaseMarker.resolve();
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt(created.webhookId))?.status ===
      "superseded",
    );
    expect(runtime.requests).toHaveLength(0);
  });

  it("cancels recovery backoff on close without a false fatal diagnostic", async () => {
    const markerFailed = createDeferred<void>();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    let openSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        removeTmpDirOnClose: false,
        bridgeStateOwnerId: "runtime-after-backoff-close",
        prepareBridgeState: async (storePath) => {
          const prior = new JsonBridgeStateStore(storePath, {
            ownerId: "runtime-before-backoff-close",
            recoveryKeyring: createIngressRecoveryKeyring(
              INGRESS_RECOVERY_KEY,
            ),
          });
          await prior.claimEvent(
            {
              webhookId: "webhook-recovery-backoff-close",
              executionId: "created:session-recovery-backoff-close",
              linearSessionId: "session-recovery-backoff-close",
              action: "created",
            },
            {
              action: "created",
              prompt: "retryable close during backoff",
              occurredAt: "2026-08-18T12:00:00.000Z",
            },
          );
          const originalOpen = fsPromises.open.bind(fsPromises);
          let stateTempOpens = 0;
          openSpy = vi
            .spyOn(fsPromises, "open")
            .mockImplementation(async (...args) => {
              const openedPath = String(args[0]);
              if (
                openedPath.includes(".bridge-state.json.") &&
                openedPath.endsWith(".tmp")
              ) {
                stateTempOpens += 1;
                if (stateTempOpens === 2) {
                  markerFailed.resolve();
                  throw new Error("synthetic marker failure before backoff");
                }
              }
              return await originalOpen(...args);
            });
        },
      });
      harness = activeHarness;
      await markerFailed.promise;
      await waitFor(async () =>
        (await harness!.bridgeState.getReceipt(
          "webhook-recovery-backoff-close",
        ))?.status === "received",
      );

      const startedAt = Date.now();
      await harness.close();
      activeHarness = undefined;
      expect(Date.now() - startedAt).toBeLessThan(1_000);
      expect(runtime.requests).toHaveLength(0);
      const receipt = await harness.bridgeState.getReceipt(
        "webhook-recovery-backoff-close",
      );
      expect(receipt).toMatchObject({
        status: "received",
        recoveryEnvelope: expect.any(Object),
      });
      expect(
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("ingress recovery failed"),
        ),
      ).toBe(false);
    } finally {
      openSpy?.mockRestore();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("handles startup-attempt rejection when close aborts OAuth backoff", async () => {
    const oauthFailureObserved = createDeferred<void>();
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", onUnhandled);
    const hasRefreshTokenSpy = vi
      .spyOn(LinearOAuthTokenManager.prototype, "hasRefreshToken")
      .mockImplementationOnce(async () => {
        oauthFailureObserved.resolve();
        throw new Error("synthetic startup OAuth failure");
      })
      .mockResolvedValue(false);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        removeTmpDirOnClose: false,
      });
      harness = activeHarness;
      await oauthFailureObserved.promise;
      await new Promise((resolve) => setTimeout(resolve, 1));
      await harness.close();
      activeHarness = undefined;
      await new Promise<void>((resolve) => setImmediate(resolve));
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(unhandled).toEqual([]);
    } finally {
      process.removeListener("unhandledRejection", onUnhandled);
      hasRefreshTokenSpy.mockRestore();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("awaits graceful shutdown when SIGTERM arrives before readiness", async () => {
    const oauthFailureObserved = createDeferred<void>();
    const hasRefreshTokenSpy = vi
      .spyOn(LinearOAuthTokenManager.prototype, "hasRefreshToken")
      .mockImplementationOnce(async () => {
        oauthFailureObserved.resolve();
        throw new Error("synthetic pre-ready OAuth failure");
      })
      .mockResolvedValue(false);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        removeTmpDirOnClose: false,
      });
      harness = activeHarness;
      await oauthFailureObserved.promise;
      await new Promise((resolve) => setTimeout(resolve, 1));
      const signals = new EventEmitter();
      const exitCodes: number[] = [];
      const closeSpy = vi.fn(harness.close);
      const lifecycle = installGracefulShutdown(
        { close: closeSpy },
        {
          signalSource: signals,
          exit: (code) => exitCodes.push(code),
        },
      );
      const readiness = awaitReadyOrShutdown(harness.ready, lifecycle);

      signals.emit("SIGTERM");
      signals.emit("SIGINT");
      await expect(readiness).resolves.toBe(false);
      activeHarness = undefined;
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(closeSpy).toHaveBeenCalledTimes(1);
      expect(exitCodes).toEqual([0]);
    } finally {
      hasRefreshTokenSpy.mockRestore();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("durably fences an older accepted callback when a later stop runs first", async () => {
    const pendingPostResponseWork: Array<() => void> = [];
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, {
      schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
    });
    const harness = activeHarness;
    const now = Date.now();
    const created = {
      webhookId: "webhook-created-before-stop",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-created-before-stop" },
      promptContext: "work that the later stop must fence",
      webhookTimestamp: now - 1_000,
    };
    const stop = {
      webhookId: "webhook-stop-after-created",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-created-before-stop" },
      agentActivity: {
        id: "activity-stop-after-created",
        createdAt: new Date(now).toISOString(),
        content: { type: "prompt", body: "stop", signal: "stop" },
      },
      webhookTimestamp: now,
    };
    const send = async (payload: typeof created | typeof stop): Promise<void> => {
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
    };

    await send(created);
    await send(stop);
    expect(pendingPostResponseWork).toHaveLength(2);

    pendingPostResponseWork[1]!();
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt(stop.webhookId))?.status ===
      "completed",
    );
    pendingPostResponseWork[0]!();
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt(created.webhookId))?.status ===
      "superseded",
    );

    expect(runtime.requests).toHaveLength(0);
    await expect(
      harness.bridgeState.getReceipt(created.webhookId),
    ).resolves.toMatchObject({
      status: "superseded",
      supersededByWebhookId: stop.webhookId,
    });
  });

  it("executes the original stop once when its semantic activity is redelivered under another webhook id", async () => {
    const pendingPostResponseWork: Array<() => void> = [];
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, {
      schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
    });
    const harness = activeHarness;
    const base = {
      webhookId: "webhook-stop-semantic-original",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-stop-semantic-redelivery" },
      agentActivity: {
        id: "activity-stop-semantic-redelivery",
        createdAt: new Date().toISOString(),
        content: { type: "prompt", body: "stop", signal: "stop" },
      },
      webhookTimestamp: Date.now(),
    };
    for (const payload of [
      base,
      {
        ...base,
        webhookId: "webhook-stop-semantic-redelivery",
        webhookTimestamp: Date.now() + 1,
      },
    ]) {
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
    expect(pendingPostResponseWork).toHaveLength(1);
    pendingPostResponseWork[0]!();
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt(base.webhookId))?.status ===
      "completed",
    );
    await expect(
      harness.bridgeState.getReceipt("webhook-stop-semantic-redelivery"),
    ).resolves.toMatchObject({ status: "superseded" });
    expect(
      harness.calls.filter(
        (call) =>
          call.content.type === "response" && call.content.body === "Stopped.",
      ),
    ).toHaveLength(1);
  });

  it("does not let an overlapping same-millisecond stop abort a later accepted prompt", async () => {
    const pendingPostResponseWork: Array<() => void> = [];
    const runtimeStarted = createDeferred<void>();
    const finishRuntime = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (request) {
      expect(request.abortController.signal.aborted).toBe(false);
      runtimeStarted.resolve();
      await finishRuntime.promise;
      expect(request.abortController.signal.aborted).toBe(false);
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime, {
      schedulePostResponseWork: (work) => pendingPostResponseWork.push(work),
    });
    const harness = activeHarness;
    const now = Date.now();
    const stop = {
      webhookId: "webhook-overlapping-older-stop",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-overlapping-newer-prompt" },
      agentActivity: {
        id: "activity-overlapping-older-stop",
        createdAt: new Date(now).toISOString(),
        content: { type: "prompt", body: "stop", signal: "stop" },
      },
      webhookTimestamp: now,
    };
    const newerPrompt = {
      webhookId: "webhook-overlapping-newer-prompt",
      type: "AgentSessionEvent",
      action: "prompted",
      agentSession: { id: "session-overlapping-newer-prompt" },
      agentActivity: {
        id: "activity-overlapping-newer-prompt",
        createdAt: new Date(now).toISOString(),
        content: { type: "prompt", body: "continue with newer work" },
      },
      webhookTimestamp: now + 1,
    };
    const send = async (payload: typeof stop | typeof newerPrompt): Promise<void> => {
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
    };

    await send(stop);
    await send(newerPrompt);
    expect(pendingPostResponseWork).toHaveLength(2);

    // The stop enters its async marker write first. Before that await resumes,
    // the newer turn registers its controller and enters its own marker write.
    pendingPostResponseWork[0]!();
    pendingPostResponseWork[1]!();

    await Promise.race([
      runtimeStarted.promise,
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error("newer prompt did not start within 1 second")),
          1_000,
        ),
      ),
    ]);
    expect(runtime.requests).toHaveLength(1);
    expect(runtime.requests[0]?.prompt).toBe("continue with newer work");
    expect(runtime.requests[0]?.abortController.signal.aborted).toBe(false);

    finishRuntime.resolve();
    await waitFor(async () =>
      (await harness.bridgeState.getReceipt(newerPrompt.webhookId))?.status ===
      "completed",
    );
  });

  it("retries a visible claim after its directory sync fails and executes it once", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let openSpy: ReturnType<typeof vi.spyOn> | undefined;
    let renameSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        bridgeStateOwnerId: "runtime-a",
      });
      const harness = activeHarness;
      const stateDirectory = path.dirname(harness.bridgeStatePath);
      const originalOpen = fsPromises.open.bind(fsPromises);
      const originalRename = fsPromises.rename.bind(fsPromises);
      let stateRenames = 0;
      let failedFinalClaimSync = false;
      renameSpy = vi
        .spyOn(fsPromises, "rename")
        .mockImplementation(async (from, to) => {
          await originalRename(from, to);
          if (String(to) === harness.bridgeStatePath) {
            stateRenames += 1;
          }
        });
      openSpy = vi
        .spyOn(fsPromises, "open")
        .mockImplementation(async (...args) => {
          const handle = await originalOpen(...args);
          if (String(args[0]) === stateDirectory) {
            const originalSync = handle.sync.bind(handle);
            vi.spyOn(handle, "sync").mockImplementation(async () => {
              if (stateRenames === 2 && !failedFinalClaimSync) {
                failedFinalClaimSync = true;
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
      renameSpy?.mockRestore();
      openSpy?.mockRestore();
      errorSpy.mockRestore();
    }
  });

  it("retries an idempotent liveness failure before runtime start without losing accepted work", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    const livenessActivityIds: string[] = [];
    let livenessAttempts = 0;
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime, {
        linearFetchImpl: (calls) =>
          (async (_url: RequestInfo | URL, init?: RequestInit) => {
            const parsed = JSON.parse(init?.body as string) as {
              variables: {
                input?: {
                  id?: string;
                  agentSessionId: string;
                  content: AgentActivityContent;
                  ephemeral?: boolean;
                };
                id?: string;
              };
            };
            if (parsed.variables.id !== undefined) {
              return jsonResponse({
                data: { agentActivities: { nodes: [] } },
              });
            }
            const input = parsed.variables.input;
            if (input === undefined) {
              throw new Error("missing activity create input");
            }
            calls.push({
              agentSessionId: input.agentSessionId,
              content: input.content,
              ...(input.ephemeral !== undefined
                ? { ephemeral: input.ephemeral }
                : {}),
            });
            if (
              input.content.type === "thought" &&
              input.content.body === "Reading the issue and gathering context…"
            ) {
              livenessAttempts += 1;
              if (input.id !== undefined) {
                livenessActivityIds.push(input.id);
              }
              if (livenessAttempts === 1) {
                return jsonResponse(
                  { error: "synthetic transient liveness failure" },
                  { ok: false, status: 503 },
                );
              }
            }
            return jsonResponse({
              data: { agentActivityCreate: { success: true } },
            });
          }) as FetchFn,
      });
      const harness = activeHarness;
      const prompt = "private liveness retry prompt";
      const payload = {
        webhookId: "webhook-liveness-retry-before-runtime",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-liveness-retry-before-runtime" },
        promptContext: prompt,
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
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(livenessAttempts).toBe(2);
      expect(livenessActivityIds).toHaveLength(2);
      expect(new Set(livenessActivityIds).size).toBe(1);
      expect(runtime.requests).toHaveLength(1);
      expect(await fsPromises.readFile(harness.bridgeStatePath, "utf8")).not.toContain(
        prompt,
      );
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain(prompt);
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("reconciles a delivered liveness activity before replaying its pending outbox", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let createdActivityId: string | undefined;
    let createAttempts = 0;
    let queryAttempts = 0;
    const queriedActivityIds: string[] = [];
    activeHarness = await startTestServer(runtime, {
      linearFetchImpl: (calls) =>
        (async (_url: RequestInfo | URL, init?: RequestInit) => {
          const parsed = JSON.parse(init?.body as string) as {
            variables: {
              id?: string;
              input?: LinearCall & { id?: string };
            };
          };
          if (parsed.variables.id !== undefined) {
            queryAttempts += 1;
            queriedActivityIds.push(parsed.variables.id);
            return jsonResponse({
              data: {
                agentActivities: {
                  nodes:
                    queryAttempts >= 4 &&
                    createdActivityId === parsed.variables.id
                      ? [
                          {
                            id: createdActivityId,
                            agentSession: {
                              id: "session-liveness-outbox-reconcile",
                            },
                          },
                        ]
                      : [],
                },
              },
            });
          }
          const input = parsed.variables.input!;
          createAttempts += 1;
          createdActivityId = input.id;
          calls.push({
            agentSessionId: input.agentSessionId,
            content: input.content,
            ...(input.ephemeral !== undefined
              ? { ephemeral: input.ephemeral }
              : {}),
          });
          return jsonResponse({
            data: { agentActivityCreate: { success: true } },
          });
        }) as FetchFn,
    });
    const harness = activeHarness;
    const originalDelivered = harness.bridgeState.markActivityDelivered.bind(
      harness.bridgeState,
    );
    vi.spyOn(harness.bridgeState, "markActivityDelivered")
      .mockRejectedValueOnce(new Error("synthetic delivered-state failure"))
      .mockImplementation(originalDelivered);
    const payload = {
      webhookId: "webhook-liveness-outbox-reconcile",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-liveness-outbox-reconcile" },
      promptContext: "private activity reconciliation prompt",
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
      (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
      "completed",
    );
    expect(createAttempts).toBe(1);
    expect(queryAttempts).toBe(4);
    expect(new Set(queriedActivityIds)).toEqual(
      new Set([createdActivityId]),
    );
  });

  it("confirms a locally visible runtime intent before starting the runtime once", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;
    const originalMark = harness.bridgeState.markDispatchStarted.bind(
      harness.bridgeState,
    );
    let first = true;
    const markSpy = vi
      .spyOn(harness.bridgeState, "markDispatchStarted")
      .mockImplementation(async (webhookId) => {
        const result = await originalMark(webhookId);
        if (first) {
          first = false;
          throw new DispatchMarkerDurabilityError();
        }
        return result;
      });
    const releaseSpy = vi.spyOn(
      harness.bridgeState,
      "releasePreDispatchClaim",
    );
    const payload = {
      webhookId: "webhook-runtime-intent-confirmation",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-runtime-intent-confirmation" },
      promptContext: "private runtime intent prompt",
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
      (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
      "completed",
    );
    expect(markSpy).toHaveBeenCalledTimes(2);
    expect(releaseSpy).not.toHaveBeenCalled();
    expect(runtime.requests).toHaveLength(1);
  });

  it("confirms a durable runtime intent after its lock release fails", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;
    const payload = {
      webhookId: "webhook-runtime-intent-release-failure",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-runtime-intent-release-failure" },
      promptContext: "private marker release failure prompt",
      webhookTimestamp: Date.now(),
    };
    const originalRename = fsPromises.rename.bind(fsPromises);
    const originalRmdir = fsPromises.rmdir.bind(fsPromises);
    let markerVisible = false;
    let releaseFailed = false;
    const renameSpy = vi
      .spyOn(fsPromises, "rename")
      .mockImplementation(async (from, to) => {
        let isMarkerWrite = false;
        if (String(to) === harness.bridgeStatePath) {
          const candidate = JSON.parse(
            await fsPromises.readFile(String(from), "utf8"),
          ) as {
            receipts?: Record<string, { dispatchStartedAt?: string }>;
          };
          isMarkerWrite =
            candidate.receipts?.[payload.webhookId]?.dispatchStartedAt !== undefined;
        }
        await originalRename(from, to);
        if (isMarkerWrite) {
          markerVisible = true;
        }
      });
    const rmdirSpy = vi
      .spyOn(fsPromises, "rmdir")
      .mockImplementation(async (...args) => {
        if (
          !releaseFailed &&
          markerVisible &&
          String(args[0]) === `${harness.bridgeStatePath}.lock`
        ) {
          releaseFailed = true;
          throw Object.assign(new Error("synthetic marker lock release failure"), {
            code: "EIO",
          });
        }
        return originalRmdir(...args);
      });
    const releaseSpy = vi.spyOn(
      harness.bridgeState,
      "releasePreDispatchClaim",
    );

    try {
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
      await waitFor(() => releaseFailed);
      await waitFor(() => runtime.requests.length === 1, 1_500);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(releaseSpy).not.toHaveBeenCalled();
      expect(runtime.requests).toHaveLength(1);
    } finally {
      rmdirSpy.mockRestore();
      renameSpy.mockRestore();
    }
  });

  it("retries terminal-state confirmation without invoking the runtime again", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    activeHarness = await startTestServer(runtime);
    const harness = activeHarness;
    const originalComplete = harness.bridgeState.completeEvent.bind(
      harness.bridgeState,
    );
    let first = true;
    const completeSpy = vi
      .spyOn(harness.bridgeState, "completeEvent")
      .mockImplementation(async (webhookId) => {
        await originalComplete(webhookId);
        if (first) {
          first = false;
          throw new Error("synthetic terminal durability uncertainty");
        }
      });
    const payload = {
      webhookId: "webhook-terminal-confirmation",
      type: "AgentSessionEvent",
      action: "created",
      agentSession: { id: "session-terminal-confirmation" },
      promptContext: "private terminal confirmation prompt",
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
      (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
      "completed",
    );
    await waitFor(() => completeSpy.mock.calls.length === 2);
    expect(runtime.requests).toHaveLength(1);
  });

  it("recovers without another webhook after marker and release fail before either state write", async () => {
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
      await waitFor(() => releaseReadFailed);
      expect(runtime.requests).toHaveLength(0);
      const visibleReceipt = await harness.bridgeState.getReceipt(payload.webhookId);
      expect(visibleReceipt?.dispatchStartedAt).toBeUndefined();

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

  it("recovers without another webhook after a marker failure and successful release", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      activeHarness = await startTestServer(runtime);
      const harness = activeHarness;
      const originalMarkDispatchStarted =
        harness.bridgeState.markDispatchStarted.bind(harness.bridgeState);
      const originalReleasePreDispatchClaim =
        harness.bridgeState.releasePreDispatchClaim.bind(harness.bridgeState);
      const markSpy = vi
        .spyOn(harness.bridgeState, "markDispatchStarted")
        .mockImplementation(originalMarkDispatchStarted);
      const releaseSpy = vi
        .spyOn(harness.bridgeState, "releasePreDispatchClaim")
        .mockImplementation(originalReleasePreDispatchClaim);
      markSpy.mockRejectedValueOnce(new Error("synthetic marker write failure"));
      const payload = {
        webhookId: "webhook-dispatch-marker-retry",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "agent-session-dispatch-marker-retry" },
        promptContext: "private retry prompt",
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
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(markSpy).toHaveBeenCalledTimes(2);
      expect(releaseSpy).toHaveBeenCalledTimes(1);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("private retry prompt");
      expect(logged).not.toContain("synthetic marker write failure");
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("retries the earliest recovered marker failure before dispatching later accepted work", async () => {
    const firstPrompt = "first accepted recovery prompt";
    const secondPrompt = "second accepted recovery prompt";
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let openSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        bridgeStateOwnerId: "runtime-after-ordered-recovery",
        prepareBridgeState: async (storePath) => {
          const prior = new JsonBridgeStateStore(storePath, {
            ownerId: "runtime-before-ordered-recovery",
            recoveryKeyring: createIngressRecoveryKeyring(
              INGRESS_RECOVERY_KEY,
            ),
          });
          for (const [index, prompt] of [firstPrompt, secondPrompt].entries()) {
            await prior.claimEvent(
              {
                webhookId: `webhook-ordered-recovery-${index}`,
                executionId: `created:session-ordered-recovery-${index}`,
                linearSessionId: `session-ordered-recovery-${index}`,
                action: "created",
              },
              {
                action: "created",
                prompt,
                occurredAt: new Date(
                  Date.parse("2026-08-18T12:00:00.000Z") + index,
                ).toISOString(),
              },
            );
          }
          const originalOpen = fsPromises.open.bind(fsPromises);
          let stateTempOpens = 0;
          openSpy = vi
            .spyOn(fsPromises, "open")
            .mockImplementation(async (...args) => {
              const openedPath = String(args[0]);
              if (
                openedPath.includes(".bridge-state.json.") &&
                openedPath.endsWith(".tmp")
              ) {
                stateTempOpens += 1;
                if (stateTempOpens === 2) {
                  throw new Error("synthetic first recovered marker failure");
                }
              }
              return await originalOpen(...args);
            });
        },
      });
      const harness = activeHarness;
      await waitFor(() => runtime.requests.length === 2);
      expect(runtime.requests.map((request) => request.prompt)).toEqual([
        firstPrompt,
        secondPrompt,
      ]);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt("webhook-ordered-recovery-1"))
          ?.status === "completed",
      );
    } finally {
      openSpy?.mockRestore();
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

  it("keeps startup unready until an exact signed redelivery repairs a true legacy receipt", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "activity", activity: { type: "response", body: "recovered" } };
      yield { kind: "done" };
    });
    activeHarness = await startTestServer(runtime, {
      bridgeStateOwnerId: "runtime-after-restart",
      awaitReady: false,
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
      (await fetch(serverUrl(harness.port, "/healthz"))).status,
    ).toBe(503);
    let readySettled = false;
    void harness.ready.then(() => {
      readySettled = true;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(readySettled).toBe(false);

    await waitFor(async () =>
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": "invalid-signature",
          },
          body,
        })
      ).status === 401,
    );
    const unrelated = {
      ...payload,
      webhookId: "webhook-unrelated-during-legacy-repair",
      agentSession: { id: "agent-session-unrelated-during-legacy-repair" },
    };
    const unrelatedBody = JSON.stringify(unrelated);
    expect(
      (
        await fetch(serverUrl(harness.port, "/webhook"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "linear-signature": sign(unrelatedBody, WEBHOOK_SECRET),
          },
          body: unrelatedBody,
        })
      ).status,
    ).toBe(503);
    await expect(
      harness.bridgeState.getReceipt(unrelated.webhookId),
    ).resolves.toBeUndefined();

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
    await harness.ready;
    expect((await fetch(serverUrl(harness.port, "/healthz"))).status).toBe(200);
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

  it("rescans a visibly repaired legacy receipt after its directory sync acknowledgement fails", async () => {
    const runtime = new FakeRuntime(async function* () {
      yield { kind: "done" } as RuntimeEvent;
    });
    let openSpy: ReturnType<typeof vi.spyOn> | undefined;
    let renameSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        bridgeStateOwnerId: "runtime-after-visible-legacy-repair",
        prepareBridgeState: async (storePath) => {
          const prior = new JsonBridgeStateStore(storePath, {
            ownerId: "runtime-before-visible-legacy-repair",
          });
          await prior.claimEvent({
            webhookId: "webhook-visible-legacy-repair",
            executionId: "created:session-visible-legacy-repair",
            linearSessionId: "session-visible-legacy-repair",
            action: "created",
          });
          const stateDirectory = path.dirname(storePath);
          const originalOpen = fsPromises.open.bind(fsPromises);
          const originalRename = fsPromises.rename.bind(fsPromises);
          let stateRenames = 0;
          let failedVisibleRepairSync = false;
          renameSpy = vi
            .spyOn(fsPromises, "rename")
            .mockImplementation(async (from, to) => {
              await originalRename(from, to);
              if (String(to) === storePath) {
                stateRenames += 1;
              }
            });
          openSpy = vi
            .spyOn(fsPromises, "open")
            .mockImplementation(async (...args) => {
              const handle = await originalOpen(...args);
              if (String(args[0]) === stateDirectory) {
                const originalSync = handle.sync.bind(handle);
                vi.spyOn(handle, "sync").mockImplementation(async () => {
                  if (stateRenames === 1 && !failedVisibleRepairSync) {
                    failedVisibleRepairSync = true;
                    throw new Error("synthetic visible legacy repair sync failure");
                  }
                  await originalSync();
                });
              }
              return handle;
            });
        },
      });
      const harness = activeHarness;
      const prompt = "private visibly repaired prompt";
      const payload = {
        webhookId: "webhook-visible-legacy-repair",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-visible-legacy-repair" },
        promptContext: prompt,
        webhookTimestamp: Date.now(),
      };
      const body = JSON.stringify(payload);
      await waitFor(async () =>
        (
          await fetch(serverUrl(harness.port, "/webhook"), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "linear-signature": "invalid-signature",
            },
            body,
          })
        ).status === 401,
      );
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
      ).toBe(503);

      await harness.ready;
      await waitFor(() => runtime.requests.length === 1);
      expect(runtime.requests[0]?.prompt).toBe(prompt);
      await waitFor(async () =>
        (await harness.bridgeState.getReceipt(payload.webhookId))?.status ===
        "completed",
      );
      expect(runtime.requests).toHaveLength(1);
    } finally {
      renameSpy?.mockRestore();
      openSpy?.mockRestore();
    }
  });

  it.each(["sequence-without-envelope", "unknown-key"] as const)(
    "keeps %s recovery state fail-closed and never opens legacy repair",
    async (failureMode) => {
      const runtime = new FakeRuntime(async function* () {
        yield { kind: "done" } as RuntimeEvent;
      });
      const prompt = "private unrecoverable prompt";
      activeHarness = await startTestServer(runtime, {
        awaitReady: false,
        bridgeStateOwnerId: "runtime-after-unrecoverable-state",
        ...(failureMode === "unknown-key"
          ? { recoveryKey: Buffer.alloc(32, 1).toString("base64url") }
          : {}),
        prepareBridgeState: async (storePath) => {
          const prior = new JsonBridgeStateStore(storePath, {
            ownerId: "runtime-before-unrecoverable-state",
            recoveryKeyring: createIngressRecoveryKeyring(
              INGRESS_RECOVERY_KEY,
            ),
          });
          await prior.claimEvent(
            {
              webhookId: "webhook-unrecoverable-state",
              executionId: "created:session-unrecoverable-state",
              linearSessionId: "session-unrecoverable-state",
              action: "created",
            },
            {
              action: "created",
              prompt,
              occurredAt: "2026-08-18T12:00:00.000Z",
            },
          );
          if (failureMode === "sequence-without-envelope") {
            const raw = JSON.parse(
              await fsPromises.readFile(storePath, "utf8"),
            ) as { receipts: Record<string, Record<string, unknown>> };
            delete raw.receipts["webhook-unrecoverable-state"]!
              .recoveryEnvelope;
            await fsPromises.writeFile(storePath, JSON.stringify(raw), {
              mode: 0o600,
            });
          }
        },
      });
      const harness = activeHarness;

      await expect(harness.ready).rejects.toBeInstanceOf(
        IngressRecoveryEnvelopeError,
      );
      expect((await fetch(serverUrl(harness.port, "/healthz"))).status).toBe(
        503,
      );
      const payload = {
        webhookId: "webhook-unrecoverable-state",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-unrecoverable-state" },
        promptContext: prompt,
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
      ).toBe(503);
      expect(runtime.requests).toHaveLength(0);
      expect(
        await fsPromises.readFile(harness.bridgeStatePath, "utf8"),
      ).not.toContain(prompt);
    },
  );

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
      "superseded",
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

  it("drains an in-flight inactivity activity before close and prevents late state mutation", async () => {
    const releaseRuntime = createDeferred<void>();
    const inactivityIdStarted = createDeferred<void>();
    const releaseInactivityId = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      await Promise.race([
        releaseRuntime.promise,
        new Promise<void>((resolve) => {
          request.abortController?.signal.addEventListener("abort", () => resolve(), {
            once: true,
          });
        }),
      ]);
      yield { kind: "done" };
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let harness: Harness | undefined;
    let activityIdSpy: ReturnType<typeof vi.spyOn> | undefined;

    try {
      activeHarness = await startTestServer(runtime, {
        configOverrides: { runInactivityTimeoutMs: 25 },
        removeTmpDirOnClose: false,
      });
      harness = activeHarness;
      const originalGetOrCreate =
        harness.bridgeState.getOrCreateActivityId.bind(harness.bridgeState);
      activityIdSpy = vi
        .spyOn(harness.bridgeState, "getOrCreateActivityId")
        .mockImplementation(async (executionId, activityKey, signal) => {
          if (activityKey === "inactivity-error") {
            inactivityIdStarted.resolve();
            await releaseInactivityId.promise;
          }
          return originalGetOrCreate(executionId, activityKey, signal);
        });
      const payload = {
        webhookId: "webhook-close-during-inactivity-activity",
        type: "AgentSessionEvent",
        action: "created",
        agentSession: { id: "session-close-during-inactivity-activity" },
        promptContext: "private inactivity close prompt",
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
      await inactivityIdStarted.promise;

      let closeSettled = false;
      const close = harness.close().then(() => {
        closeSettled = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(closeSettled).toBe(false);

      releaseInactivityId.resolve();
      await close;
      activeHarness = undefined;
      const claimAfterClose = await harness.bridgeState.getClaim(
        "created:session-close-during-inactivity-activity",
      );
      expect(claimAfterClose?.activityIds).not.toHaveProperty(
        "inactivity-error",
      );
      const stateAfterClose = await fsPromises.readFile(
        harness.bridgeStatePath,
        "utf8",
      );
      await new Promise((resolve) => setTimeout(resolve, 25));
      await expect(
        fsPromises.readFile(harness.bridgeStatePath, "utf8"),
      ).resolves.toBe(stateAfterClose);
      const logged = errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");
      expect(logged).not.toContain("private inactivity close prompt");
      expect(logged).not.toContain("failed to emit inactivity activity");
    } finally {
      releaseRuntime.resolve();
      releaseInactivityId.resolve();
      await activeHarness?.close().catch(() => undefined);
      activeHarness = undefined;
      activityIdSpy?.mockRestore();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("allows a long turn with runtime activity and starts a queued turn's watchdog only when it executes", async () => {
    const runtime = new FakeRuntime(async function* (
      request: SessionRequest,
    ): AsyncGenerator<RuntimeEvent> {
      if (request.linearSessionId === "agent-session-active-long") {
        await new Promise((resolve) => setTimeout(resolve, 100));
        yield { kind: "progress" };
        await new Promise((resolve) => setTimeout(resolve, 100));
        yield { kind: "session-started", runtimeSessionId: "runtime-active-long" };
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      yield {
        kind: "activity",
        activity: { type: "response", body: `completed ${request.linearSessionId}` },
      };
      yield { kind: "done" };
    });

    activeHarness = await startTestServer(runtime, {
      configOverrides: { runInactivityTimeoutMs: 250 },
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
      1_500,
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

  it("drains an accepted OAuth callback before the final shutdown token flush", async () => {
    const callbackStarted = createDeferred<void>();
    const finishTokenExchange = createDeferred<Response>();
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
        tokenFetchImpl: (async () => {
          callbackStarted.resolve();
          return await finishTokenExchange.promise;
        }) as FetchFn,
      });
      harness = activeHarness;
      const state = new URL(await harness.authorizationUrl).searchParams.get(
        "state",
      );
      const callback = fetch(
        serverUrl(
          harness.port,
          `/oauth/callback?code=auth-code-during-close&state=${state}`,
        ),
      );
      await callbackStarted.promise;

      let closeSettled = false;
      const closing = harness.close().then(() => {
        closeSettled = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(closeSettled).toBe(false);

      finishTokenExchange.resolve(
        jsonResponse({
          access_token: "callback-close-access",
          refresh_token: "callback-close-refresh",
          expires_in: 86_400,
        }),
      );
      expect((await callback).status).toBe(200);
      await closing;
      activeHarness = undefined;

      const restarted = new LinearOAuthTokenManager({
        clientId: "client-id-test",
        clientSecret: "client-secret-test",
        initialAccessToken: "unused",
        storePath: harness.oauthTokenStorePath,
      });
      await expect(restarted.getAccessToken()).resolves.toBe(
        "callback-close-access",
      );
    } finally {
      finishTokenExchange.resolve(
        jsonResponse({
          access_token: "callback-close-access",
          refresh_token: "callback-close-refresh",
          expires_in: 86_400,
        }),
      );
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("bounds signal shutdown when an accepted OAuth callback never settles", async () => {
    const callbackStarted = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
        shutdownRequestTimeoutMs: 25,
        tokenFetchImpl: (async (_input, init) => {
          callbackStarted.resolve();
          return await new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener(
              "abort",
              () => reject(new DOMException("Aborted", "AbortError")),
              { once: true },
            );
          });
        }) as FetchFn,
      });
      harness = activeHarness;
      const state = new URL(await harness.authorizationUrl).searchParams.get(
        "state",
      );
      const callback = fetch(
        serverUrl(
          harness.port,
          `/oauth/callback?code=auth-code-never-settles&state=${state}`,
        ),
      ).catch(() => undefined);
      await callbackStarted.promise;
      const signals = new EventEmitter();
      const exitObserved = createDeferred<number>();
      const closeSpy = vi.fn(harness.close);
      installGracefulShutdown(
        { close: closeSpy },
        {
          signalSource: signals,
          exit: (code) => exitObserved.resolve(code),
        },
      );

      signals.emit("SIGTERM");
      signals.emit("SIGTERM");
      await expect(
        Promise.race([
          exitObserved.promise,
          new Promise<never>((_resolve, reject) =>
            setTimeout(() => reject(new Error("signal exit timed out")), 500),
          ),
        ]),
      ).resolves.toBe(0);
      activeHarness = undefined;
      expect(closeSpy).toHaveBeenCalledTimes(1);

      await callback;
      await new Promise<void>((resolve) => setImmediate(resolve));
      await expect(
        fsPromises.readFile(harness.oauthTokenStorePath, "utf8"),
      ).rejects.toMatchObject({ code: "ENOENT" });
      expect(
        logSpy.mock.calls.some((call) =>
          call.join(" ").includes("OAuth token pair installed"),
        ),
      ).toBe(false);
      expect(
        errorSpy.mock.calls.some((call) =>
          call.join(" ").includes("OAuth token exchange failed"),
        ),
      ).toBe(false);
    } finally {
      logSpy.mockRestore();
      errorSpy.mockRestore();
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("persists a successful OAuth callback response that arrives at shutdown abort", async () => {
    const callbackStarted = createDeferred<void>();
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    let harness: Harness | undefined;
    try {
      activeHarness = await startTestServer(runtime, {
        removeTmpDirOnClose: false,
        shutdownRequestTimeoutMs: 25,
        tokenFetchImpl: (async (_input, init) => {
          callbackStarted.resolve();
          return await new Promise<Response>((resolve) => {
            init?.signal?.addEventListener(
              "abort",
              () =>
                resolve(
                  jsonResponse({
                    access_token: "abort-race-access",
                    refresh_token: "abort-race-refresh",
                    expires_in: 86_400,
                  }),
                ),
              { once: true },
            );
          });
        }) as FetchFn,
      });
      harness = activeHarness;
      const state = new URL(await harness.authorizationUrl).searchParams.get(
        "state",
      );
      const callback = fetch(
        serverUrl(
          harness.port,
          `/oauth/callback?code=auth-code-abort-race&state=${state}`,
        ),
      ).catch(() => undefined);
      await callbackStarted.promise;

      await harness.close();
      activeHarness = undefined;
      await callback;
      const restarted = new LinearOAuthTokenManager({
        clientId: "client-id-test",
        clientSecret: "client-secret-test",
        initialAccessToken: "unused",
        storePath: harness.oauthTokenStorePath,
      });
      await expect(restarted.getAccessToken()).resolves.toBe(
        "abort-race-access",
      );
    } finally {
      if (harness !== undefined) {
        await fsPromises.rm(harness.tmpDir, { recursive: true, force: true });
      }
    }
  });

  it("oauth callback: cancels a failed token response without exposing its body", async () => {
    const runtime = new FakeRuntime(async function* (): AsyncGenerator<RuntimeEvent> {
      yield { kind: "done" };
    });
    let bodyCanceled = false;
    activeHarness = await startTestServer(runtime, {
      tokenFetchImpl: (async () =>
        observableFailureResponse(
          503,
          "Service Unavailable",
          "raw-token-exchange-secret",
          () => {
            bodyCanceled = true;
          },
        )) as FetchFn,
    });
    const harness = activeHarness;
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      const state = new URL(await harness.authorizationUrl).searchParams.get(
        "state",
      );
      const response = await fetch(
        serverUrl(
          harness.port,
          `/oauth/callback?code=auth-code-failure&state=${state}`,
        ),
      );

      expect(response.status).toBe(500);
      expect(await response.text()).toBe("OAuth token exchange failed");
      expect(bodyCanceled).toBe(true);
      const logged = errorSpy.mock.calls.flat().join("\n");
      expect(logged).toContain("error=UnknownError");
      expect(logged).not.toContain("raw-token-exchange-secret");
    } finally {
      errorSpy.mockRestore();
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
