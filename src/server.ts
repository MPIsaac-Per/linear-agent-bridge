import { randomBytes } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type {
  AgentActivityContent,
  AgentRuntime,
  LinearAgentSessionEvent,
  RuntimeEvent,
  SessionRequest,
} from "./types.js";
import type { Config } from "./config.js";
import {
  hasFreshWebhookTimestamp,
  verifyWebhookSignature,
} from "./linear/webhook-verify.js";
import {
  discardResponseBody,
  LinearActivityError,
  type LinearAgentClient,
  type FetchFn,
} from "./linear/client.js";
import type {
  LinearOAuthTokenManager,
  LinearOAuthTokenResponse,
} from "./linear/oauth.js";
import type { JsonSessionStore } from "./sessions/store.js";
import type {
  BridgeStateStore,
  IngressEventIdentity,
  RecoverableIngressEvent,
  ReceiptErrorClass,
} from "./state/store.js";
import {
  BridgeStateLockTimeoutError,
  ClaimOwnershipError,
  LegacyIngressRecoveryMismatchError,
  LegacyIngressRecoveryUnavailableError,
} from "./state/store.js";
import {
  isStopPrompt,
  IngressRecoveryEnvelopeError,
  type IngressRecoveryPayload,
} from "./state/recovery-envelope.js";
import type { SerialQueue } from "./queue.js";

/** Linear's OAuth2 token-exchange endpoint (linear.app/developers/oauth-2-0-authentication). */
const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";

/**
 * Must match the redirect URI registered on the Linear OAuth2 Application
 * and passed to /oauth/authorize when installing the agent.
 */
const OAUTH_REDIRECT_URI = "http://localhost:3979/oauth/callback";
const LINEAR_AUTHORIZE_URL = "https://linear.app/oauth/authorize";
const OAUTH_STATE_TTL_MS = 10 * 60 * 1000;

/** Emitted immediately on `created` to satisfy Linear's 10s liveness rule. */
const CREATED_THOUGHT_BODY = "Reading the issue and gathering context…";
/** Acknowledge follow-up turns before they enter the host-wide serial queue. */
const PROMPTED_THOUGHT_BODY = "Working on it…";
const STOPPED_RESPONSE_BODY = "Stopped.";
type TurnTerminalReason = "completed" | "inactive" | "stopped" | "failed";

interface InternalServerDeps extends ServerDeps {
  activeRuns: Map<string, Set<ActiveRun>>;
  shutdownController: AbortController;
  processingInFlight: Set<Promise<void>>;
  closing: boolean;
  recoveryInFlight?: Promise<void> | undefined;
  recoveryRequested: boolean;
  recoveryBlocked: boolean;
  recoveryAwaitingRedelivery: boolean;
  dispatchReady: boolean;
  requestStartupRecovery: () => void;
}

interface RecoveryOrder {
  action: "created" | "prompted";
  occurredAt: string;
  sequence: number;
}

interface ActiveRun {
  controller: AbortController;
  recoveryOrder: RecoveryOrder;
}

export interface ServerDeps {
  config: Config;
  runtime: AgentRuntime;
  linear: LinearAgentClient;
  oauth: LinearOAuthTokenManager;
  store: JsonSessionStore;
  bridgeState: BridgeStateStore;
  queue: SerialQueue;
  /**
   * Fetch used for the OAuth token exchange in GET /oauth/callback.
   * Defaults to the global fetch; tests inject a fake so the real Linear
   * OAuth endpoint is never called.
   */
  tokenFetch?: FetchFn;
  /**
   * Test hook: called once with the actual bound port right after the
   * server starts listening. Lets tests set config.port = 0 (ephemeral)
   * and discover the real port to send requests to.
   */
  onListening?: (port: number) => void;
  /** Test hook for the state-bearing URL printed during initial setup. */
  onOAuthAuthorizationUrl?: (url: string) => void;
  /** Test seam for work that begins only after the HTTP acknowledgement. */
  schedulePostResponseWork?: (work: () => void) => void;
}

class OAuthStateStore {
  private readonly states = new Map<string, number>();

  issue(now = Date.now()): string {
    this.removeExpired(now);
    const state = randomBytes(32).toString("base64url");
    this.states.set(state, now + OAUTH_STATE_TTL_MS);
    return state;
  }

  consume(state: string, now = Date.now()): boolean {
    const expiresAt = this.states.get(state);
    this.states.delete(state);
    return expiresAt !== undefined && expiresAt >= now;
  }

  private removeExpired(now: number): void {
    for (const [state, expiresAt] of this.states) {
      if (expiresAt < now) {
        this.states.delete(state);
      }
    }
  }
}

class PreDispatchClaimReleasedError extends Error {
  constructor() {
    super("Dispatch marker was not persisted; the ingress claim was released");
    this.name = "PreDispatchClaimReleasedError";
  }
}

class ServerListenError extends Error {
  constructor() {
    super("Bridge HTTP listener could not start");
    this.name = "ServerListenError";
  }
}

/**
 * HTTP server + session orchestration.
 *
 * POST /webhook
 *   1. Read raw body, verify signature (webhook-verify).
 *   2. Persist a delivery receipt and semantic execution claim.
 *   3. Ack 200 (Linear requires a response within 5s).
 *   4. For newly claimed agent session events:
 *      - created: emit an immediate `thought` activity (10s liveness rule),
 *        then enqueue the session run.
 *      - prompted: look up the stored runtime session id and enqueue a
 *        resumed run with the follow-up prompt.
 *   5. Session run: iterate runtime.runSession(), forward each activity to
 *      Linear, persist the runtime session id on session-started, emit an
 *      `error` activity on failure so the session never hangs silently.
 *
 * GET /oauth/callback — verify one-time OAuth state, exchange the code for a
 * rotating actor=app token pair, and persist it for automatic refresh.
 * GET /healthz — liveness for launchd.
 */
export function startServer(deps: ServerDeps): {
  ready: Promise<void>;
  close(): Promise<void>;
} {
  const oauthStates = new OAuthStateStore();
  const internalDeps: InternalServerDeps = {
    ...deps,
    activeRuns: new Map<string, Set<ActiveRun>>(),
    shutdownController: new AbortController(),
    processingInFlight: new Set<Promise<void>>(),
    closing: false,
    recoveryRequested: false,
    recoveryBlocked: false,
    recoveryAwaitingRedelivery: false,
    dispatchReady: false,
    requestStartupRecovery: () => undefined,
  };
  const server = createServer((req, res) => {
    handleRequest(req, res, internalDeps, oauthStates).catch((error: unknown) => {
      console.error(
        `[linear-agent-bridge] request handler failed: error=${boundedErrorClass(error)}`,
      );
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "text/plain" });
      }
      res.end();
    });
  });

  let resolveReady!: () => void;
  let rejectReady!: (error: unknown) => void;
  let readySettled = false;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = () => {
      if (!readySettled) {
        readySettled = true;
        resolve();
      }
    };
    rejectReady = (error) => {
      if (!readySettled) {
        readySettled = true;
        reject(error);
      }
    };
  });
  // A caller may close immediately without ever awaiting ready. Keep the
  // returned promise rejectable while preventing an unhandled rejection.
  void ready.catch(() => undefined);

  let resolveListenOutcome!: () => void;
  const listenOutcome = new Promise<void>((resolve) => {
    resolveListenOutcome = resolve;
  });
  server.once("error", () => {
    resolveListenOutcome();
    rejectReady(new ServerListenError());
  });

  let startupAttempt: Promise<void> | undefined;
  const requestStartupRecovery = (): void => {
    if (
      internalDeps.closing ||
      internalDeps.dispatchReady ||
      internalDeps.recoveryBlocked ||
      startupAttempt !== undefined
    ) {
      return;
    }
    const attempt = (async () => {
      try {
        await scheduleAcceptedIngressRecovery(internalDeps);
        if (internalDeps.closing) {
          return;
        }
        await emitOAuthAuthorizationUrlIfNeeded(deps, oauthStates);
        if (internalDeps.closing) {
          return;
        }
        internalDeps.recoveryAwaitingRedelivery = false;
        internalDeps.dispatchReady = true;
        resolveReady();
      } catch (error) {
        if (error instanceof LegacyIngressRecoveryUnavailableError) {
          internalDeps.recoveryAwaitingRedelivery = true;
          return;
        }
        if (!internalDeps.closing) {
          internalDeps.recoveryAwaitingRedelivery = false;
          internalDeps.recoveryBlocked = true;
          rejectReady(error);
        }
      }
    })();
    startupAttempt = attempt;
    void attempt.then(() => {
      if (startupAttempt === attempt) {
        startupAttempt = undefined;
      }
    });
  };
  internalDeps.requestStartupRecovery = requestStartupRecovery;

  server.listen(deps.config.port, () => {
    resolveListenOutcome();
    const address = server.address();
    if (
      deps.onListening !== undefined &&
      address !== null &&
      typeof address === "object"
    ) {
      deps.onListening(address.port);
    }
    requestStartupRecovery();
  });

  return {
    ready,
    async close(): Promise<void> {
      internalDeps.closing = true;
      rejectReady(new Error("Server shutting down"));
      internalDeps.shutdownController.abort(new Error("Server shutting down"));
      for (const runs of internalDeps.activeRuns.values()) {
        for (const run of runs) {
          run.controller.abort(new Error("Server shutting down"));
        }
      }
      await Promise.allSettled([...internalDeps.processingInFlight]);
      await internalDeps.recoveryInFlight?.catch(() => undefined);
      await startupAttempt?.catch(() => undefined);
      await internalDeps.queue.enqueue(async () => {});
      await listenOutcome;
      if (!server.listening) {
        return;
      }
      await new Promise<void>((resolve, reject) => {
        server.close((err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
        // Force-close idle keep-alive sockets so close() doesn't hang
        // waiting for a client (e.g. undici's connection pool) to let go.
        server.closeAllConnections();
      });
    },
  };
}

async function handleRequest(
  req: IncomingMessage,
  res: ServerResponse,
  deps: InternalServerDeps,
  oauthStates: OAuthStateStore,
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");

  if (
    deps.recoveryBlocked &&
    (url.pathname === "/healthz" || url.pathname === "/webhook")
  ) {
    res.writeHead(503, { "Content-Type": "text/plain" });
    res.end("ingress recovery unavailable");
    return;
  }

  if (
    !deps.dispatchReady &&
    (url.pathname === "/healthz" ||
      (url.pathname === "/webhook" && !deps.recoveryAwaitingRedelivery))
  ) {
    res.writeHead(503, { "Content-Type": "text/plain" });
    res.end("ingress recovery unavailable");
    return;
  }

  if (req.method === "GET" && url.pathname === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }

  if (req.method === "GET" && url.pathname === "/oauth/callback") {
    await handleOAuthCallback(url, res, deps, oauthStates);
    return;
  }

  if (req.method === "POST" && url.pathname === "/webhook") {
    await handleWebhook(req, res, deps);
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("not found");
}

async function emitOAuthAuthorizationUrlIfNeeded(
  deps: ServerDeps,
  oauthStates: OAuthStateStore,
): Promise<void> {
  if (await deps.oauth.hasRefreshToken()) {
    return;
  }

  const url = new URL(LINEAR_AUTHORIZE_URL);
  url.search = new URLSearchParams({
    client_id: deps.config.linearClientId,
    redirect_uri: OAUTH_REDIRECT_URI,
    response_type: "code",
    scope: "read,write,app:assignable,app:mentionable",
    actor: "app",
    state: oauthStates.issue(),
  }).toString();
  const authorizationUrl = url.toString();
  console.log(
    `[linear-agent-bridge] OAuth authorization URL (valid for 10 minutes): ${authorizationUrl}`,
  );
  deps.onOAuthAuthorizationUrl?.(authorizationUrl);
}

async function handleWebhook(
  req: IncomingMessage,
  res: ServerResponse,
  deps: InternalServerDeps,
): Promise<void> {
  const rawBody = await readRawBody(req);

  const signatureHeaderRaw = req.headers["linear-signature"];
  const signatureHeader = Array.isArray(signatureHeaderRaw)
    ? signatureHeaderRaw[0]
    : signatureHeaderRaw;

  if (
    !verifyWebhookSignature(
      rawBody,
      signatureHeader,
      deps.config.linearWebhookSecret,
    )
  ) {
    console.error("[linear-agent-bridge] webhook rejected: error=InvalidSignature");
    res.writeHead(401, { "Content-Type": "text/plain" });
    res.end("invalid signature");
    return;
  }

  let payload: unknown;
  try {
    payload = JSON.parse(rawBody.toString("utf8"));
  } catch {
    console.error("[linear-agent-bridge] webhook rejected: error=InvalidJson");
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("invalid JSON");
    return;
  }

  if (!hasFreshWebhookTimestamp(payload)) {
    console.error("[linear-agent-bridge] webhook rejected: error=InvalidTimestamp");
    res.writeHead(401, { "Content-Type": "text/plain" });
    res.end("invalid timestamp");
    return;
  }

  const event = parseAgentSessionEvent(payload);
  if (event === undefined) {
    const record = asRecord(payload);
    if (record?.type === "AgentSessionEvent") {
      console.error(
        "[linear-agent-bridge] webhook rejected: error=InvalidAgentSessionEvent",
      );
      res.writeHead(400, { "Content-Type": "text/plain" });
      res.end("invalid agent session event");
      return;
    }

    console.log(
      `[linear-agent-bridge] ignored webhook: type=${boundedLogValue(record?.type)} action=${boundedLogValue(record?.action)}`,
    );
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }

  const identity = eventIdentity(event);
  const recoverablePayload = recoveryPayload(event);
  const repairingLegacyReceipt =
    !deps.dispatchReady &&
    !deps.recoveryBlocked &&
    deps.recoveryAwaitingRedelivery;
  let claimResult;
  let recoveryOrder: RecoveryOrder | undefined;
  try {
    claimResult = await deps.bridgeState.claimEvent(
      identity,
      recoverablePayload,
      repairingLegacyReceipt ? { repairLegacyOnly: true } : undefined,
    );
    if (claimResult.disposition === "claimed") {
      recoveryOrder = recoveryOrderFromReceipt(
        claimResult.receipt.recoverySequence,
        recoverablePayload,
      );
    }
    if (
      repairingLegacyReceipt &&
      (claimResult.disposition !== "claimed" ||
        !(await deps.bridgeState.releasePreDispatchClaim(identity.webhookId)))
    ) {
      throw new LegacyIngressRecoveryMismatchError();
    }
  } catch (error) {
    console.error(
      `[linear-agent-bridge] ingress persistence failed: webhook=${identity.webhookId} execution=${identity.executionId} error=${boundedErrorClass(error)}`,
    );
    res.writeHead(503, { "Content-Type": "text/plain" });
    res.end("ingress persistence unavailable");
    if (repairingLegacyReceipt) {
      deps.requestStartupRecovery();
    }
    return;
  }

  // Linear requires a response within 5s. The durable receipt and semantic
  // claim above are the only work allowed to precede this acknowledgement.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");

  if (repairingLegacyReceipt) {
    deps.requestStartupRecovery();
    return;
  }

  if (claimResult.disposition !== "claimed") {
    console.log(
      `[linear-agent-bridge] ingress ${claimResult.disposition}: webhook=${identity.webhookId} execution=${identity.executionId}`,
    );
    return;
  }

  const schedule = deps.schedulePostResponseWork ?? setImmediate;
  schedule(() => {
    if (!deps.closing) {
      trackClaimedEventProcessing(
        deps,
        processClaimedEvent(
          event,
          identity,
          recoveryOrder!,
          deps,
          "webhook",
        ),
      );
    }
  });
}

function trackClaimedEventProcessing(
  deps: InternalServerDeps,
  processing: Promise<"settled" | "retryable">,
): void {
  const tracked = processing.then(() => undefined);
  deps.processingInFlight.add(tracked);
  void tracked.then(
    () => deps.processingInFlight.delete(tracked),
    () => deps.processingInFlight.delete(tracked),
  );
}

function scheduleAcceptedIngressRecovery(
  deps: InternalServerDeps,
): Promise<void> {
  if (deps.closing) {
    return Promise.resolve();
  }
  deps.recoveryRequested = true;
  if (deps.recoveryInFlight !== undefined) {
    return deps.recoveryInFlight;
  }
  const recovery = runAcceptedIngressRecovery(deps)
    .catch((error: unknown) => {
      if (deps.closing) {
        return;
      }
      if (!(error instanceof LegacyIngressRecoveryUnavailableError)) {
        deps.recoveryBlocked = true;
      }
      console.error(
        `[linear-agent-bridge] ingress recovery failed: error=${boundedErrorClass(error)}`,
      );
      throw error;
    })
    .finally(() => {
      if (deps.recoveryInFlight === recovery) {
        deps.recoveryInFlight = undefined;
      }
    });
  deps.recoveryInFlight = recovery;
  return recovery;
}

async function runAcceptedIngressRecovery(
  deps: InternalServerDeps,
): Promise<void> {
  let retryDelayMs = 100;
  while (!deps.closing && deps.recoveryRequested) {
    deps.recoveryRequested = false;
    await recoverAcceptedIngressPass(deps);
    if (deps.recoveryRequested && !deps.closing) {
      await delay(retryDelayMs, deps.shutdownController.signal);
      retryDelayMs = Math.min(retryDelayMs * 2, 5_000);
    }
  }
}

async function recoverAcceptedIngressPass(
  deps: InternalServerDeps,
): Promise<void> {
  await deps.bridgeState.assertRecoverableEventsAvailable();
  let afterSequence = 0;
  while (!deps.closing) {
    const recoverable = await deps.bridgeState.listRecoverableEvents(
      afterSequence,
    );
    if (recoverable.length === 0) {
      return;
    }
    for (const candidate of recoverable) {
      if (deps.closing) {
        return;
      }
      if (!candidate.available) {
        throw new IngressRecoveryEnvelopeError();
      }
      afterSequence = candidate.sequence;
      const claim = await deps.bridgeState.claimEvent(candidate.identity);
      if (claim.disposition !== "claimed") {
        continue;
      }
      const outcome = await processClaimedEvent(
        recoveredEvent(candidate),
        candidate.identity,
        {
          action: candidate.payload.action,
          occurredAt: candidate.payload.occurredAt,
          sequence: candidate.sequence,
        },
        deps,
        "recovery",
      );
      if (outcome === "retryable") {
        return;
      }
    }
  }
}

async function processClaimedEvent(
  event: LinearAgentSessionEvent,
  identity: IngressEventIdentity,
  recoveryOrder: RecoveryOrder,
  deps: InternalServerDeps,
  scope: "webhook" | "recovery",
): Promise<"settled" | "retryable"> {
  try {
    await processClaimedWebhook(event, identity, recoveryOrder, deps);
    return "settled";
  } catch (error) {
    if (deps.closing && deps.shutdownController.signal.aborted) {
      return "settled";
    }
    console.error(
      `[linear-agent-bridge] ${scope} processing failed: webhook=${identity.webhookId} execution=${identity.executionId} error=${boundedErrorClass(error)}`,
    );
    if (error instanceof PreDispatchClaimReleasedError) {
      void scheduleAcceptedIngressRecovery(deps).catch(() => undefined);
      return "retryable";
    }
    if (!(error instanceof ClaimOwnershipError)) {
      await markIngressFailed(deps, identity, "WebhookProcessingError");
    }
    return "settled";
  }
}

function recoveredEvent(
  candidate: Extract<RecoverableIngressEvent, { available: true }>,
): LinearAgentSessionEvent {
  const { identity, payload } = candidate;
  if (payload.action === "created") {
    return {
      webhookId: identity.webhookId,
      webhookTimestamp: Date.parse(payload.occurredAt),
      action: "created",
      agentSession: {
        id: identity.linearSessionId,
        ...(payload.issueIdentifier !== undefined
          ? {
              issue: {
                id: payload.issueIdentifier,
                identifier: payload.issueIdentifier,
                title: "",
              },
            }
          : {}),
      },
      promptContext: payload.prompt,
    };
  }
  return {
    webhookId: identity.webhookId,
    webhookTimestamp: Date.parse(payload.occurredAt),
    action: "prompted",
    agentSession: { id: identity.linearSessionId },
    agentActivity: {
      id: identity.executionId,
      createdAt: payload.occurredAt,
      content: {
        type: "prompt",
        body: payload.prompt,
        ...(payload.signal !== undefined ? { signal: payload.signal } : {}),
      },
      ...(payload.signal !== undefined ? { signal: payload.signal } : {}),
    },
  };
}

function readRawBody(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => {
      chunks.push(chunk);
    });
    req.on("end", () => {
      resolve(Buffer.concat(chunks));
    });
    req.on("error", (err) => {
      reject(err);
    });
  });
}

/**
 * Narrows a parsed webhook body to an agent session event: top-level
 * `type: "AgentSessionEvent"` with `action: "created" | "prompted"`, per
 * linear.app/developers/agent-interaction. Other webhook categories
 * (data-change events, Issue SLA, etc.) return undefined and are ignored.
 */
function parseAgentSessionEvent(payload: unknown): LinearAgentSessionEvent | undefined {
  if (payload === null || typeof payload !== "object") {
    return undefined;
  }
  const obj = payload as Record<string, unknown>;

  if (obj.type !== "AgentSessionEvent") {
    return undefined;
  }
  if (obj.action !== "created" && obj.action !== "prompted") {
    return undefined;
  }
  if (!isBoundedIdentifier(obj.webhookId)) {
    return undefined;
  }

  const agentSession = obj.agentSession;
  if (agentSession === null || typeof agentSession !== "object") {
    return undefined;
  }
  if (!isBoundedIdentifier((agentSession as Record<string, unknown>).id)) {
    return undefined;
  }
  if (obj.action === "prompted") {
    const agentActivity = asRecord(obj.agentActivity);
    if (
      !isBoundedIdentifier(agentActivity?.id) ||
      (agentActivity.createdAt !== undefined &&
        (typeof agentActivity.createdAt !== "string" ||
          !Number.isFinite(Date.parse(agentActivity.createdAt))))
    ) {
      return undefined;
    }
  }

  return obj as unknown as LinearAgentSessionEvent;
}

function eventIdentity(event: LinearAgentSessionEvent): IngressEventIdentity {
  return {
    webhookId: event.webhookId,
    executionId:
      event.action === "created"
        ? `created:${event.agentSession.id}`
        : event.agentActivity.id,
    linearSessionId: event.agentSession.id,
    action: event.action,
  };
}

function recoveryPayload(
  event: LinearAgentSessionEvent,
): IngressRecoveryPayload {
  const occurredAt = new Date(
    event.action === "prompted" && event.agentActivity.createdAt !== undefined
      ? Date.parse(event.agentActivity.createdAt)
      : event.webhookTimestamp,
  ).toISOString();
  const issueIdentifier = event.agentSession.issue?.identifier;
  if (event.action === "created") {
    return {
      action: "created",
      occurredAt,
      prompt: event.promptContext ?? event.agentSession.issue?.title ?? "",
      ...(issueIdentifier !== undefined ? { issueIdentifier } : {}),
    };
  }
  const prompt =
    event.agentActivity.content?.body ?? event.agentActivity.body ?? "";
  const signal =
    event.agentActivity.content?.signal ?? event.agentActivity.signal;
  return {
    action: "prompted",
    occurredAt,
    prompt,
    stop: isStopPrompt(prompt, signal),
    ...(signal !== undefined ? { signal } : {}),
  };
}

async function processClaimedWebhook(
  event: LinearAgentSessionEvent,
  identity: IngressEventIdentity,
  recoveryOrder: RecoveryOrder,
  deps: InternalServerDeps,
): Promise<void> {
  const sessionId = event.agentSession.id;
  const issueIdentifier = event.agentSession.issue?.identifier;
  const prompt =
    event.action === "created"
      ? event.promptContext ?? event.agentSession.issue?.title ?? ""
      : event.agentActivity.content?.body ?? event.agentActivity.body ?? "";
  const isStop =
    event.action === "prompted" &&
    isStopPrompt(
      prompt,
      event.agentActivity.content?.signal ?? event.agentActivity.signal,
    );
  const controller = isStop
    ? undefined
    : registerSessionRun(deps, sessionId, recoveryOrder);
  let enqueued = false;
  try {
    try {
      const dispatch = await deps.bridgeState.markDispatchStarted(
        identity.webhookId,
      );
      if (dispatch === "superseded") {
        return;
      }
    } catch (error) {
      let released = false;
      let releaseFailed = false;
      try {
        released = await deps.bridgeState.releasePreDispatchClaim(
          identity.webhookId,
        );
      } catch (releaseError) {
        releaseFailed = true;
        console.error(
          `[linear-agent-bridge] pre-dispatch claim release failed: webhook=${identity.webhookId} execution=${identity.executionId} error=${boundedErrorClass(releaseError)}`,
        );
      }
      if (released || releaseFailed) {
        throw new PreDispatchClaimReleasedError();
      }
      throw error;
    }
    console.log(
      `[linear-agent-bridge] agent session event: action=${event.action} session=${event.agentSession.id} webhook=${identity.webhookId}`,
    );

    if (event.action === "created") {
      // 10s liveness rule: emit a thought before doing anything else.
      await emitActivity(
        deps,
        identity.executionId,
        "liveness",
        sessionId,
        {
          type: "thought",
          body: CREATED_THOUGHT_BODY,
        },
        { ephemeral: true, signal: controller!.signal },
      );
      if (controller!.signal.aborted) {
        if (!deps.closing) {
          await deps.bridgeState.completeEvent(identity.webhookId);
        }
        return;
      }
      enqueueSessionRun(
        deps,
        { linearSessionId: sessionId, prompt },
        issueIdentifier,
        controller!,
        identity,
      );
      enqueued = true;
      return;
    }

    if (isStop) {
      abortSessionRuns(deps, sessionId, recoveryOrder);
      await emitActivity(
        deps,
        identity.executionId,
        "stop-response",
        sessionId,
        { type: "response", body: STOPPED_RESPONSE_BODY },
        { signal: deps.shutdownController.signal },
      );
      if (!deps.closing) {
        await deps.bridgeState.completeEvent(identity.webhookId);
      }
      return;
    }

    if (controller!.signal.aborted) {
      if (!deps.closing) {
        await deps.bridgeState.completeEvent(identity.webhookId);
      }
      return;
    }
    if (prompt === "") {
      console.log(
        `[linear-agent-bridge] prompted with empty body: session=${sessionId} activity=${event.agentActivity.id}`,
      );
    }
    await emitActivity(
      deps,
      identity.executionId,
      "liveness",
      sessionId,
      {
        type: "thought",
        body: PROMPTED_THOUGHT_BODY,
      },
      { ephemeral: true, signal: controller!.signal },
    );
    if (controller!.signal.aborted) {
      if (!deps.closing) {
        await deps.bridgeState.completeEvent(identity.webhookId);
      }
      return;
    }
    enqueueSessionRun(
      deps,
      { linearSessionId: sessionId, prompt },
      issueIdentifier,
      controller!,
      identity,
      { loadStoredSessionAtExecution: true },
    );
    enqueued = true;
  } finally {
    if (controller !== undefined && !enqueued) {
      unregisterRun(deps, sessionId, controller);
    }
  }
}

function registerSessionRun(
  deps: InternalServerDeps,
  sessionId: string,
  recoveryOrder: RecoveryOrder,
): AbortController {
  const controller = new AbortController();
  let runs = deps.activeRuns.get(sessionId);
  if (runs === undefined) {
    runs = new Set<ActiveRun>();
    deps.activeRuns.set(sessionId, runs);
  }
  runs.add({ controller, recoveryOrder });
  return controller;
}

function enqueueSessionRun(
  deps: InternalServerDeps,
  request: Omit<SessionRequest, "abortController">,
  issueIdentifier: string | undefined,
  controller: AbortController,
  identity: IngressEventIdentity,
  options: { loadStoredSessionAtExecution?: boolean } = {},
): void {
  void deps.queue
    .enqueue(async () => {
      let effectiveRequest = request;
      let effectiveIssueIdentifier = issueIdentifier;
      let terminalReason: TurnTerminalReason = controller.signal.aborted
        ? "stopped"
        : "failed";
      try {
        if (options.loadStoredSessionAtExecution === true) {
          const storedSession = await deps.store.get(request.linearSessionId);
          effectiveRequest = {
            ...request,
            ...(storedSession?.runtimeSessionId !== undefined
              ? { resumeSessionId: storedSession.runtimeSessionId }
              : {}),
          };
          effectiveIssueIdentifier ??= storedSession?.issueIdentifier;
        }
        if (!controller.signal.aborted) {
          console.log(
            `[linear-agent-bridge] turn start: session=${request.linearSessionId} queue=${deps.queue.size}`,
          );
          terminalReason = await runSessionTask(
            deps,
            { ...effectiveRequest, abortController: controller },
            effectiveIssueIdentifier,
            identity.executionId,
          );
        }
      } finally {
        console.log(
          `[linear-agent-bridge] turn terminal: session=${request.linearSessionId} reason=${terminalReason} queue=${Math.max(0, deps.queue.size - 1)}`,
        );
        try {
          if (!deps.closing) {
            if (terminalReason === "failed" || terminalReason === "inactive") {
              await deps.bridgeState.failEvent(
                identity.webhookId,
                terminalReason === "inactive"
                  ? "RuntimeTimeout"
                  : "RuntimeExecutionError",
              );
            } else {
              await deps.bridgeState.completeEvent(identity.webhookId);
            }
          }
        } finally {
          unregisterRun(deps, request.linearSessionId, controller);
        }
      }
    })
    .catch((error: unknown) => {
      if (!deps.closing) {
        console.error(
          `[linear-agent-bridge] queued turn finalization failed: webhook=${identity.webhookId} execution=${identity.executionId} error=${boundedErrorClass(error)}`,
        );
      }
    });
}

function abortSessionRuns(
  deps: InternalServerDeps,
  sessionId: string,
  stopOrder: RecoveryOrder,
): void {
  for (const run of deps.activeRuns.get(sessionId) ?? []) {
    if (runIsAtOrBeforeStop(run.recoveryOrder, stopOrder)) {
      run.controller.abort(new Error("Stopped by user"));
    }
  }
}

function unregisterRun(
  deps: InternalServerDeps,
  sessionId: string,
  controller: AbortController,
): void {
  const runs = deps.activeRuns.get(sessionId);
  if (runs !== undefined) {
    for (const run of runs) {
      if (run.controller === controller) {
        runs.delete(run);
        break;
      }
    }
  }
  if (runs?.size === 0) {
    deps.activeRuns.delete(sessionId);
  }
}

function recoveryOrderFromReceipt(
  sequence: number | undefined,
  payload: IngressRecoveryPayload,
): RecoveryOrder {
  if (
    !Number.isSafeInteger(sequence) ||
    sequence === undefined ||
    sequence <= 0
  ) {
    throw new IngressRecoveryEnvelopeError();
  }
  return {
    action: payload.action,
    occurredAt: payload.occurredAt,
    sequence,
  };
}

function runIsAtOrBeforeStop(
  runOrder: RecoveryOrder,
  stopOrder: RecoveryOrder,
): boolean {
  if (runOrder.action === "created") {
    return true;
  }
  const byTime =
    Date.parse(runOrder.occurredAt) - Date.parse(stopOrder.occurredAt);
  return (
    byTime < 0 ||
    (byTime === 0 && runOrder.sequence <= stopOrder.sequence)
  );
}

function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.reject(signal.reason);
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(signal?.reason ?? new Error("Aborted"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * Runs one session turn: iterates the runtime, persisting the runtime
 * session id and forwarding activities as they arrive. Swallows nothing —
 * a failed iteration still emits a best-effort error activity and is
 * logged, but never rejects/crashes the caller (the queue, the process).
 */
async function runSessionTask(
  deps: InternalServerDeps,
  request: SessionRequest,
  issueIdentifier: string | undefined,
  executionId: string,
): Promise<TurnTerminalReason> {
  const controller = request.abortController;
  let activitySequence = 0;
  let acceptEvents = true;
  let inactivityTriggered = false;
  let inactivityTimer: ReturnType<typeof setTimeout>;
  let resolveWatchdog!: (outcome: "inactive" | "stopped") => void;
  const watchdog = new Promise<"inactive" | "stopped">((resolve) => {
    resolveWatchdog = resolve;
  });
  const forceCloseRuntime = (): void => {
    try {
      deps.runtime.forceCloseSession?.(request);
    } catch (error) {
      console.error(
        `[linear-agent-bridge] runtime force-close failed: session=${request.linearSessionId} error=${boundedErrorClass(error)}`,
      );
    }
  };
  const onControllerAbort = (): void => {
    acceptEvents = false;
    forceCloseRuntime();
    resolveWatchdog(inactivityTriggered ? "inactive" : "stopped");
  };
  controller?.signal.addEventListener("abort", onControllerAbort, {
    once: true,
  });
  if (controller?.signal.aborted === true) {
    onControllerAbort();
  }
  const armWatchdog = (): void => {
    clearTimeout(inactivityTimer);
    inactivityTimer = setTimeout(() => {
      acceptEvents = false;
      inactivityTriggered = true;
      if (controller !== undefined) {
        controller.abort(new Error("Session inactivity limit exceeded"));
      } else {
        forceCloseRuntime();
        resolveWatchdog("inactive");
      }
    }, deps.config.runInactivityTimeoutMs);
  };
  // Queue and webhook time do not count. The first watchdog window begins
  // only now, once this serial task is actually executing.
  armWatchdog();
  const consumeRuntime = async (): Promise<{ error?: unknown }> => {
    try {
      for await (const event of deps.runtime.runSession(request)) {
        if (!acceptEvents || controller?.signal.aborted === true) {
          break;
        }
        if (event.kind === "done") {
          return {};
        }
        // Reset before persistence or outbound Linear delivery. Those
        // operations and any retries they perform are not runtime progress.
        armWatchdog();
        await handleRuntimeEvent(
          deps,
          request,
          issueIdentifier,
          event,
          executionId,
          activitySequence,
        );
        if (event.kind === "activity") {
          activitySequence += 1;
        }
      }
      return {};
    } catch (error) {
      return { error };
    }
  };

  const outcome = await Promise.race([
    consumeRuntime().then((result) => ({ source: "runtime" as const, ...result })),
    watchdog.then((reason) => ({ source: "watchdog" as const, reason })),
  ]);
  clearTimeout(inactivityTimer!);
  controller?.signal.removeEventListener("abort", onControllerAbort);

  if (
    inactivityTriggered ||
    (outcome.source === "watchdog" && outcome.reason === "inactive")
  ) {
    void emitActivity(
      deps,
      executionId,
      "inactivity-error",
      request.linearSessionId,
      {
        type: "error",
        body: `This request was inactive for ${formatDuration(deps.config.runInactivityTimeoutMs)} and was stopped.`,
      },
    )
      .catch((activityErr: unknown) => {
        console.error(
          `[linear-agent-bridge] failed to emit inactivity activity: session=${request.linearSessionId} error=${boundedErrorClass(activityErr)}`,
        );
      });
    return "inactive";
  }

  if (outcome.source === "watchdog") {
    return "stopped";
  }
  if (outcome.error !== undefined) {
    if (controller?.signal.aborted === true) {
      return "stopped";
    }
    console.error(
      `[linear-agent-bridge] session run failed: session=${request.linearSessionId} error=RuntimeExecutionError`,
    );
    const body =
      outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
    try {
      await emitActivity(
        deps,
        executionId,
        "runtime-error",
        request.linearSessionId,
        { type: "error", body },
      );
    } catch (activityErr) {
      console.error(
        `[linear-agent-bridge] failed to emit error activity: session=${request.linearSessionId} error=${boundedErrorClass(activityErr)}`,
      );
    }
    return "failed";
  }
  return controller?.signal.aborted === true ? "stopped" : "completed";
}

async function handleRuntimeEvent(
  deps: InternalServerDeps,
  request: SessionRequest,
  issueIdentifier: string | undefined,
  event: RuntimeEvent,
  executionId: string,
  activitySequence: number,
): Promise<void> {
  if (event.kind === "session-started") {
    await deps.store.put({
      linearSessionId: request.linearSessionId,
      runtimeSessionId: event.runtimeSessionId,
      runtime: deps.runtime.name,
      issueIdentifier,
      updatedAt: new Date().toISOString(),
    });
    return;
  }

  if (event.kind === "activity") {
    const ephemeral =
      event.activity.type === "thought" ||
      (event.activity.type === "action" && event.activity.result === undefined);
    await emitActivity(
      deps,
      executionId,
      `runtime-${activitySequence}`,
      request.linearSessionId,
      event.activity,
      {
        ...(ephemeral ? { ephemeral: true } : {}),
        ...(request.abortController !== undefined
          ? { signal: request.abortController.signal }
          : {}),
      },
    );
  }
}

async function emitActivity(
  deps: InternalServerDeps,
  executionId: string,
  activityKey: string,
  agentSessionId: string,
  content: AgentActivityContent,
  options: { ephemeral?: boolean; signal?: AbortSignal } = {},
): Promise<void> {
  const activityId = await deps.bridgeState.getOrCreateActivityId(
    executionId,
    activityKey,
  );
  const signal =
    options.signal === undefined
      ? deps.shutdownController.signal
      : AbortSignal.any([options.signal, deps.shutdownController.signal]);
  await deps.linear.createActivity(agentSessionId, content, {
    activityId,
    ...options,
    signal,
  });
}

async function markIngressFailed(
  deps: InternalServerDeps,
  identity: IngressEventIdentity,
  errorClass: ReceiptErrorClass,
): Promise<void> {
  try {
    await deps.bridgeState.failEvent(identity.webhookId, errorClass);
  } catch (error) {
    console.error(
      `[linear-agent-bridge] ingress failure state could not be persisted: webhook=${identity.webhookId} execution=${identity.executionId} error=${boundedErrorClass(error)}`,
    );
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function isBoundedIdentifier(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256;
}

function boundedLogValue(value: unknown): string {
  if (typeof value !== "string") {
    return "unknown";
  }
  return value.slice(0, 64).replace(/[^A-Za-z0-9_.:-]/g, "_");
}

function boundedErrorClass(error: unknown): string {
  if (error instanceof BridgeStateLockTimeoutError) {
    return "BridgeStateLockTimeoutError";
  }
  if (error instanceof PreDispatchClaimReleasedError) {
    return "PreDispatchClaimReleasedError";
  }
  if (error instanceof ClaimOwnershipError) {
    return "ClaimOwnershipError";
  }
  if (error instanceof LegacyIngressRecoveryMismatchError) {
    return "LegacyIngressRecoveryMismatchError";
  }
  if (error instanceof LegacyIngressRecoveryUnavailableError) {
    return "LegacyIngressRecoveryUnavailableError";
  }
  if (error instanceof IngressRecoveryEnvelopeError) {
    return "IngressRecoveryEnvelopeError";
  }
  if (error instanceof ServerListenError) {
    return "ServerListenError";
  }
  if (error instanceof LinearActivityError) {
    return "LinearActivityError";
  }
  if (
    error instanceof Error &&
    typeof (error as NodeJS.ErrnoException).code === "string"
  ) {
    return "FilesystemError";
  }
  if (error instanceof Error && error.name === "AbortError") {
    return "AbortError";
  }
  return "UnknownError";
}

function formatDuration(milliseconds: number): string {
  if (milliseconds % 60_000 === 0) {
    const minutes = milliseconds / 60_000;
    return `${minutes} ${minutes === 1 ? "minute" : "minutes"}`;
  }
  if (milliseconds % 1000 === 0) {
    const seconds = milliseconds / 1000;
    return `${seconds} ${seconds === 1 ? "second" : "seconds"}`;
  }
  return `${milliseconds} ms`;
}

async function handleOAuthCallback(
  url: URL,
  res: ServerResponse,
  deps: ServerDeps,
  oauthStates: OAuthStateStore,
): Promise<void> {
  const state = url.searchParams.get("state");
  if (state === null || !oauthStates.consume(state)) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("invalid or expired state parameter");
    return;
  }

  const code = url.searchParams.get("code");
  if (code === null) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("missing code parameter");
    return;
  }

  const tokenFetch = deps.tokenFetch ?? globalThis.fetch;
  const body = new URLSearchParams({
    code,
    redirect_uri: OAUTH_REDIRECT_URI,
    client_id: deps.config.linearClientId,
    client_secret: deps.config.linearClientSecret,
    grant_type: "authorization_code",
  });

  try {
    const response = await tokenFetch(LINEAR_TOKEN_URL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
    });

    if (!response.ok) {
      await discardResponseBody(response);
      throw new Error(
        `Linear OAuth token exchange failed: ${response.status} ${response.statusText}`,
      );
    }

    const json = (await response.json()) as LinearOAuthTokenResponse;
    await deps.oauth.install(json);
    console.log("[linear-agent-bridge] OAuth token pair installed");

    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(
      "<html><body><p>Authorization complete. The agent will refresh its Linear access automatically.</p></body></html>",
    );
  } catch (err) {
    console.error(
      `[linear-agent-bridge] OAuth token exchange failed: error=${boundedErrorClass(err)}`,
    );
    res.writeHead(500, { "Content-Type": "text/plain" });
    res.end("OAuth token exchange failed");
  }
}
