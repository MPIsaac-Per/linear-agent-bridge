import { randomBytes } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type {
  AgentRuntime,
  LinearAgentSessionEvent,
  RuntimeEvent,
  SessionRequest,
} from "./types.js";
import type { Config } from "./config.js";
import { verifyWebhook } from "./linear/webhook-verify.js";
import type { LinearAgentClient, FetchFn } from "./linear/client.js";
import type {
  LinearOAuthTokenManager,
  LinearOAuthTokenResponse,
} from "./linear/oauth.js";
import type { JsonSessionStore } from "./sessions/store.js";
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

export interface ServerDeps {
  config: Config;
  runtime: AgentRuntime;
  linear: LinearAgentClient;
  oauth: LinearOAuthTokenManager;
  store: JsonSessionStore;
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

/**
 * HTTP server + session orchestration.
 *
 * POST /webhook
 *   1. Read raw body, verify signature (webhook-verify).
 *   2. Ack 200 immediately (Linear requires a response within 5s).
 *   3. For agent session events:
 *      - created: emit an immediate `thought` activity (10s liveness rule),
 *        then enqueue the session run.
 *      - prompted: look up the stored runtime session id and enqueue a
 *        resumed run with the follow-up prompt.
 *   4. Session run: iterate runtime.runSession(), forward each activity to
 *      Linear, persist the runtime session id on session-started, emit an
 *      `error` activity on failure so the session never hangs silently.
 *
 * GET /oauth/callback — verify one-time OAuth state, exchange the code for a
 * rotating actor=app token pair, and persist it for automatic refresh.
 * GET /healthz — liveness for launchd.
 */
export function startServer(deps: ServerDeps): { close(): Promise<void> } {
  const oauthStates = new OAuthStateStore();
  const server = createServer((req, res) => {
    handleRequest(req, res, deps, oauthStates).catch((err: unknown) => {
      console.error("[linear-atlas-agent] request handler failed:", err);
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "text/plain" });
      }
      res.end();
    });
  });

  server.listen(deps.config.port, () => {
    const address = server.address();
    if (deps.onListening !== undefined && address !== null && typeof address === "object") {
      deps.onListening(address.port);
    }
    void emitOAuthAuthorizationUrlIfNeeded(deps, oauthStates);
  });

  return {
    close(): Promise<void> {
      return new Promise((resolve, reject) => {
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
  deps: ServerDeps,
  oauthStates: OAuthStateStore,
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");

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
    `[linear-atlas-agent] OAuth authorization URL (valid for 10 minutes): ${authorizationUrl}`,
  );
  deps.onOAuthAuthorizationUrl?.(authorizationUrl);
}

async function handleWebhook(
  req: IncomingMessage,
  res: ServerResponse,
  deps: ServerDeps,
): Promise<void> {
  const rawBody = await readRawBody(req);

  const signatureHeaderRaw = req.headers["linear-signature"];
  const signatureHeader = Array.isArray(signatureHeaderRaw)
    ? signatureHeaderRaw[0]
    : signatureHeaderRaw;

  if (!verifyWebhook(rawBody, signatureHeader, deps.config.linearWebhookSecret)) {
    console.error("[linear-atlas-agent] webhook rejected: invalid signature or stale timestamp");
    res.writeHead(401, { "Content-Type": "text/plain" });
    res.end("invalid signature");
    return;
  }

  // Ack first — Linear requires a response within 5s — then work.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");

  processWebhookPayload(rawBody, deps).catch((err: unknown) => {
    console.error("[linear-atlas-agent] webhook processing failed:", err);
  });
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

  const agentSession = obj.agentSession;
  if (agentSession === null || typeof agentSession !== "object") {
    return undefined;
  }
  if (typeof (agentSession as Record<string, unknown>).id !== "string") {
    return undefined;
  }

  return obj as unknown as LinearAgentSessionEvent;
}

async function processWebhookPayload(rawBody: Buffer, deps: ServerDeps): Promise<void> {
  let payload: unknown;
  try {
    payload = JSON.parse(rawBody.toString("utf8"));
  } catch {
    return;
  }

  const event = parseAgentSessionEvent(payload);
  if (event === undefined) {
    // Other webhook categories: already acked 200, nothing to do — but say
    // what arrived so payload-shape mismatches are visible in the log.
    const p = payload as Record<string, unknown> | null;
    console.log(
      `[linear-atlas-agent] ignored webhook: type=${String(p?.type)} action=${String(p?.action)} keys=${p ? Object.keys(p).join(",") : "null"}`,
    );
    return;
  }
  console.log(
    `[linear-atlas-agent] agent session event: action=${event.action} session=${event.agentSession.id}`,
  );

  const sessionId = event.agentSession.id;
  const issueIdentifier = event.agentSession.issue?.identifier;

  if (event.action === "created") {
    // 10s liveness rule: emit a thought before doing anything else.
    await deps.linear.createActivity(sessionId, {
      type: "thought",
      body: CREATED_THOUGHT_BODY,
    });

    const prompt = event.promptContext ?? event.agentSession.issue?.title ?? "";
    void deps.queue.enqueue(() =>
      runSessionTask(deps, { linearSessionId: sessionId, prompt }, issueIdentifier),
    );
    return;
  }

  // action === "prompted" — the user text lives in the content union
  // (verified against live payloads 2026-08-12); bare body is a fallback.
  const record = await deps.store.get(sessionId);
  const prompt = event.agentActivity?.content?.body ?? event.agentActivity?.body ?? "";
  if (prompt === "") {
    console.log(
      `[linear-atlas-agent] prompted with empty body; agentActivity=${JSON.stringify((payload as Record<string, unknown>).agentActivity)?.slice(0, 600)}`,
    );
  }
  void deps.queue.enqueue(() =>
    runSessionTask(
      deps,
      { linearSessionId: sessionId, prompt, resumeSessionId: record?.runtimeSessionId },
      issueIdentifier ?? record?.issueIdentifier,
    ),
  );
}

/**
 * Runs one session turn: iterates the runtime, persisting the runtime
 * session id and forwarding activities as they arrive. Swallows nothing —
 * a failed iteration still emits a best-effort error activity and is
 * logged, but never rejects/crashes the caller (the queue, the process).
 */
async function runSessionTask(
  deps: ServerDeps,
  request: SessionRequest,
  issueIdentifier: string | undefined,
): Promise<void> {
  try {
    for await (const event of deps.runtime.runSession(request)) {
      await handleRuntimeEvent(deps, request, issueIdentifier, event);
    }
  } catch (err) {
    console.error(
      `[linear-atlas-agent] session run failed for ${request.linearSessionId}:`,
      err,
    );
    const body = err instanceof Error ? err.message : String(err);
    try {
      await deps.linear.createActivity(request.linearSessionId, { type: "error", body });
    } catch (activityErr) {
      console.error(
        `[linear-atlas-agent] failed to emit error activity for ${request.linearSessionId}:`,
        activityErr,
      );
    }
  }
}

async function handleRuntimeEvent(
  deps: ServerDeps,
  request: SessionRequest,
  issueIdentifier: string | undefined,
  event: RuntimeEvent,
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
    await deps.linear.createActivity(request.linearSessionId, event.activity);
  }
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
      const text = await response.text();
      throw new Error(
        `Linear OAuth token exchange failed: ${response.status} ${response.statusText} — ${text}`,
      );
    }

    const json = (await response.json()) as LinearOAuthTokenResponse;
    await deps.oauth.install(json);
    console.log("[linear-atlas-agent] OAuth token pair installed");

    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(
      "<html><body><p>Authorization complete. The agent will refresh its Linear access automatically.</p></body></html>",
    );
  } catch (err) {
    console.error("[linear-atlas-agent] OAuth token exchange failed:", err);
    res.writeHead(500, { "Content-Type": "text/plain" });
    res.end("OAuth token exchange failed");
  }
}
