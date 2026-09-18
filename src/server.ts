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
  LinearQueryError,
  type LinearAgentClient,
  type FetchFn,
  type LinearAgentSessionActivities,
  type ReconciledAgentActivity,
} from "./linear/client.js";
import type {
  LinearOAuthTokenManager,
  LinearOAuthTokenResponse,
} from "./linear/oauth.js";
import type { JsonSessionStore } from "./sessions/store.js";
import type {
  BridgeStateStore,
  AutonomousGoalState,
  IngressEventIdentity,
  RecoverableIngressEvent,
  ReconciliationCursor,
  ReceiptErrorClass,
} from "./state/store.js";
import {
  BridgeStateLockTimeoutError,
  ClaimOwnershipError,
  LegacyIngressRecoveryMismatchError,
  LegacyIngressRecoveryUnavailableError,
  compareCursors,
} from "./state/store.js";
import {
  isStopPrompt,
  IngressRecoveryEnvelopeError,
  type IngressRecoveryPayload,
} from "./state/recovery-envelope.js";
import type { SessionLanes } from "./queue.js";
import {
  autonomousGoalPrompt,
  parseAutonomousGoalDecision,
} from "./goals/protocol.js";

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
/** Acknowledge a follow-up when it starts after an earlier turn in its lane. */
const PROMPTED_THOUGHT_BODY = "Working on it…";
const STOPPED_RESPONSE_BODY = "Stopped.";
const RUNTIME_PROVIDER_MISMATCH_BODY =
  "This agent session was started with a different runtime and cannot be resumed safely. Start a new Linear agent session after changing RUNTIME.";
const STALLED_WARNING_INTERVAL_MS = 15 * 60 * 1000;
type TurnTerminalReason = "completed" | "inactive" | "stopped" | "failed";

const GOAL_PROTOCOL_ERROR_BODY =
  "I paused autonomous work because the runtime did not return a valid lifecycle result. Reply here to resume.";
const GOAL_STEP_LIMIT_BODY =
  "I reached the configured autonomous-step limit. Reply here to authorize another bounded set of steps.";
const GOAL_LABEL_REMOVED_BODY =
  "I paused because the configured autonomous-goal label is no longer on this issue. Add the label back and reply here to resume.";
const GOAL_INTERRUPTED_BODY =
  "The service restarted after a provider turn had begun, so I paused rather than risk repeating side effects. Review the activity above and reply here to resume.";
const GOAL_RUNTIME_FAILED_BODY =
  "The provider turn stopped before it produced a safe lifecycle result. Reply here after reviewing the error to resume.";
const GOAL_COMPLETION_STATE_BODY =
  "The work is verified, but this issue has no completed workflow state available. Configure a completed state and reply here to retry completion.";
const GOAL_COMPLETION_RETRY_BODY =
  "The work is verified, but I could not move the issue to completed. Reply here to retry the Linear update.";

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
  reconciliationTimer?: ReturnType<typeof setInterval> | undefined;
  reconciliationInFlight?: Promise<void> | undefined;
  reconciliationController?: AbortController | undefined;
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

interface RuntimeTurnOutcome {
  terminalReason: TurnTerminalReason;
  response?: string | undefined;
}

type ActivityScope =
  | { kind: "ingress"; executionId: string }
  | { kind: "goal"; linearSessionId: string; prefix: string };

export interface ServerDeps {
  config: Config;
  runtime: AgentRuntime;
  linear: LinearAgentClient;
  oauth: LinearOAuthTokenManager;
  store: JsonSessionStore;
  bridgeState: BridgeStateStore;
  queue: SessionLanes;
  /**
   * Fetch used for the OAuth token exchange in GET /oauth/callback.
   * Defaults to the global fetch; tests inject a fake so the real Linear
   * OAuth endpoint is never called.
   */
  tokenFetch?: FetchFn;
  /**
   * Test hook: called once with the actual bound port and address right after
   * the server starts listening. Lets tests set config.port = 0 (ephemeral)
   * and verify the listener boundary without reaching into the HTTP server.
   */
  onListening?: (port: number, address: string) => void;
  /** Test hook for the state-bearing URL printed during initial setup. */
  onOAuthAuthorizationUrl?: (url: string) => void;
  /** Test seam for work that begins only after the HTTP acknowledgement. */
  schedulePostResponseWork?: (work: () => void) => void;
  /** Time boundary used by reconciliation; tests may inject a fixed clock. */
  now?: () => number;
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
        let preflightedGoalSessionIds: ReadonlySet<string>;
        try {
          preflightedGoalSessionIds =
            await preflightAutonomousGoalRecovery(internalDeps);
        } catch (error) {
          if (!internalDeps.closing) {
            console.error(
              `[linear-agent-bridge] autonomous goal recovery preflight failed: error=${boundedErrorClass(error)}`,
            );
          }
          throw error;
        }
        if (internalDeps.closing) {
          return;
        }
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
        await scheduleAutonomousGoalRecovery(
          internalDeps,
          preflightedGoalSessionIds,
        );
        scheduleReconciliation(internalDeps);
        internalDeps.reconciliationTimer ??= setInterval(
          () => scheduleReconciliation(internalDeps),
          deps.config.reconcileIntervalMs,
        );
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

  server.listen(deps.config.port, "127.0.0.1", () => {
    resolveListenOutcome();
    const address = server.address();
    if (
      deps.onListening !== undefined &&
      address !== null &&
      typeof address === "object"
    ) {
      deps.onListening(address.port, address.address);
    }
    requestStartupRecovery();
  });

  return {
    ready,
    async close(): Promise<void> {
      internalDeps.closing = true;
      rejectReady(new Error("Server shutting down"));
      internalDeps.shutdownController.abort(new Error("Server shutting down"));
      if (internalDeps.reconciliationTimer !== undefined) {
        clearInterval(internalDeps.reconciliationTimer);
      }
      internalDeps.reconciliationController?.abort(
        new Error("Server shutting down"),
      );
      for (const runs of internalDeps.activeRuns.values()) {
        for (const run of runs) {
          run.controller.abort(new Error("Server shutting down"));
        }
      }
      await Promise.allSettled([...internalDeps.processingInFlight]);
      await internalDeps.recoveryInFlight?.catch(() => undefined);
      await startupAttempt?.catch(() => undefined);
      await internalDeps.reconciliationInFlight?.catch(() => undefined);
      await internalDeps.queue.drain();
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

function scheduleReconciliation(deps: InternalServerDeps): void {
  if (deps.closing || deps.reconciliationInFlight !== undefined) {
    return;
  }
  const controller = new AbortController();
  deps.reconciliationController = controller;
  const reconciliation = reconcileAgentSessions(deps)
    .catch((error: unknown) => {
      if (isExpectedReconciliationShutdown(deps, error)) {
        return;
      }
      console.error(
        `[linear-agent-bridge] reconciliation failed: scope=run error=${boundedErrorClass(error)}`,
      );
    })
    .finally(() => {
      if (deps.reconciliationInFlight === reconciliation) {
        deps.reconciliationInFlight = undefined;
        deps.reconciliationController = undefined;
      }
    });
  deps.reconciliationInFlight = reconciliation;
}

async function scheduleAutonomousGoalRecovery(
  deps: InternalServerDeps,
  preflightedSessionIds: ReadonlySet<string>,
): Promise<void> {
  if (deps.closing) {
    return;
  }
  const recovery = recoverAutonomousGoals(
    deps,
    preflightedSessionIds,
  ).catch((error: unknown) => {
    if (!deps.closing) {
      console.error(
        `[linear-agent-bridge] autonomous goal recovery failed: error=${boundedErrorClass(error)}`,
      );
    }
  });
  deps.processingInFlight.add(recovery);
  void recovery.finally(() => deps.processingInFlight.delete(recovery));
  await recovery;
}

async function preflightAutonomousGoalRecovery(
  deps: InternalServerDeps,
): Promise<ReadonlySet<string>> {
  const goals = await deps.bridgeState.listRecoverableAutonomousGoals();
  if (goals.length === 0) {
    return new Set();
  }
  const now = deps.now?.() ?? Date.now();
  const watchingSince = await deps.bridgeState.ensureWatchingSince();
  const preflighted = new Set<string>();
  for (const goal of goals) {
    if (deps.closing) {
      return preflighted;
    }
    // A provider turn left running across process death has unknown side
    // effects. Persist the blocked boundary before reconciliation can dispatch
    // downtime guidance. The pending notice stays encrypted until either a
    // stop removes it or recovery/guidance emits it.
    if (goal.status === "running") {
      await deps.bridgeState.blockAutonomousGoal(
        goal.linearSessionId,
        `goal-interrupted-${goal.step}`,
        GOAL_INTERRUPTED_BODY,
      );
    }
    // A stop or guidance prompt may have landed while the process was down.
    // Reconcile this known goal session before its recovery task enters the
    // FIFO lane. The goal's durable preparation initializes reconciliation,
    // so this never adopts post-goal activity as pre-existing history.
    try {
      await reconcileAgentSession(
        deps,
        goal.linearSessionId,
        now,
        watchingSince,
      );
    } catch (error) {
      if (isExpectedReconciliationShutdown(deps, error)) {
        return preflighted;
      }
      console.error(
        `[linear-agent-bridge] autonomous recovery reconciliation failed: session=${boundedLogValue(goal.linearSessionId)} error=${boundedErrorClass(error)}`,
      );
      // Fail the startup recovery barrier closed. Accepted ingress for this
      // session may be older than an unseen stop, so it cannot run safely
      // merely because explicit goal recovery was excluded.
      throw error;
    }
    preflighted.add(goal.linearSessionId);
  }
  return preflighted;
}

async function recoverAutonomousGoals(
  deps: InternalServerDeps,
  preflightedSessionIds: ReadonlySet<string>,
): Promise<void> {
  const goals = await deps.bridgeState.listRecoverableAutonomousGoals();
  for (const goal of goals) {
    if (deps.closing) {
      return;
    }
    if (preflightedSessionIds.has(goal.linearSessionId)) {
      enqueueAutonomousGoalRecovery(deps, goal);
    }
  }
}

function enqueueAutonomousGoalRecovery(
  deps: InternalServerDeps,
  recoveredGoal: AutonomousGoalState,
): void {
  const controller = registerSessionRun(deps, recoveredGoal.linearSessionId, {
    action: "created",
    occurredAt: recoveredGoal.updatedAt,
    sequence: Number.MAX_SAFE_INTEGER,
  });
  const queued = deps.queue.enqueue(recoveredGoal.linearSessionId, async () => {
    let terminalReason: TurnTerminalReason = controller.signal.aborted
      ? "stopped"
      : "failed";
    try {
      let goal = await deps.bridgeState.getAutonomousGoal(
        recoveredGoal.linearSessionId,
      );
      if (
        goal === undefined ||
        goal.status === "completed" ||
        goal.status === "stopped" ||
        goal.status === "declined"
      ) {
        terminalReason = goal?.status === "stopped" ? "stopped" : "completed";
        return;
      }
      if (goal.status === "blocked") {
        await recoverAutonomousGoalPendingNotice(
          deps,
          goal,
          controller.signal,
        );
        terminalReason = "completed";
        return;
      }
      if (deps.config.autonomousGoalLabelId === undefined) {
        await blockAutonomousGoalWithNotice(
          deps,
          goal,
          `goal-configuration-removed-${goal.step}`,
          GOAL_LABEL_REMOVED_BODY,
          controller.signal,
        );
        terminalReason = "completed";
        return;
      }
      if (goal.runtime !== deps.runtime.name) {
        await blockAutonomousGoalWithNotice(
          deps,
          goal,
          `goal-provider-mismatch-${goal.step}`,
          RUNTIME_PROVIDER_MISMATCH_BODY,
          controller.signal,
        );
        terminalReason = "completed";
        return;
      }
      if (goal.status === "running") {
        await blockAutonomousGoalWithNotice(
          deps,
          goal,
          `goal-interrupted-${goal.step}`,
          GOAL_INTERRUPTED_BODY,
          controller.signal,
        );
        terminalReason = "completed";
        return;
      }
      if (goal.status === "authorizing") {
        goal = await resolveAutonomousGoalAuthorization(
          deps,
          goal,
          controller.signal,
        );
        if (goal.status !== "active") {
          terminalReason = "completed";
          return;
        }
      }
      if (goal.status === "completing") {
        terminalReason = await finishAutonomousGoalCompletion(
          deps,
          goal,
          controller.signal,
        );
        return;
      }
      const stored = await deps.store.get(goal.linearSessionId);
      if (
        stored?.runtimeSessionId !== undefined &&
        stored.runtime !== deps.runtime.name
      ) {
        await blockAutonomousGoalWithNotice(
          deps,
          goal,
          `goal-provider-mismatch-${goal.step}`,
          RUNTIME_PROVIDER_MISMATCH_BODY,
          controller.signal,
        );
        terminalReason = "completed";
        return;
      }
      terminalReason = await runAutonomousGoalTask(
        deps,
        {
          linearSessionId: goal.linearSessionId,
          prompt: `Resume autonomous work on ${goal.issueIdentifier ?? "the attached Linear issue"} from the durable goal state.`,
          ...(stored?.runtimeSessionId !== undefined
            ? { resumeSessionId: stored.runtimeSessionId }
            : {}),
          abortController: controller,
        },
        goal.issueIdentifier,
      );
    } finally {
      console.log(
        `[linear-agent-bridge] autonomous recovery terminal: session=${recoveredGoal.linearSessionId} reason=${terminalReason}`,
      );
      unregisterRun(deps, recoveredGoal.linearSessionId, controller);
    }
  });
  void queued.catch((error: unknown) => {
    if (!deps.closing) {
      console.error(
        `[linear-agent-bridge] autonomous recovery queue failed: session=${recoveredGoal.linearSessionId} error=${boundedErrorClass(error)}`,
      );
    }
  });
}

async function reconcileAgentSessions(deps: InternalServerDeps): Promise<void> {
  const now = deps.now?.() ?? Date.now();
  // Stamped once, on the first run this state file ever sees. Sessions older
  // than it predate the bridge and are history; sessions newer than it were
  // created while the bridge was watching, so an undispatched one is missed
  // work rather than history.
  const watchingSince = await deps.bridgeState.ensureWatchingSince();
  const sessionIds = new Set([
    ...(await deps.bridgeState.listKnownSessionIds()),
    ...(await deps.store.listSessionIds()),
  ]);

  try {
    const recent = await deps.linear.listRecentAppOwnedSessions({
      updatedAfter: new Date(now - deps.config.reconcileLookbackMs).toISOString(),
      maxSessions: deps.config.reconcileMaxSessions,
      signal: deps.reconciliationController?.signal,
    });
    for (const session of recent) {
      sessionIds.add(session.id);
    }
  } catch (error) {
    if (isExpectedReconciliationShutdown(deps, error)) {
      return;
    }
    console.error(
      `[linear-agent-bridge] reconciliation failed: scope=discovery error=${boundedErrorClass(error)}`,
    );
  }

  for (const sessionId of [...sessionIds].sort()) {
    if (deps.closing) {
      return;
    }
    try {
      await reconcileAgentSession(deps, sessionId, now, watchingSince);
    } catch (error) {
      if (isExpectedReconciliationShutdown(deps, error)) {
        return;
      }
      console.error(
        `[linear-agent-bridge] reconciliation failed: scope=session session=${boundedLogValue(sessionId)} error=${boundedErrorClass(error)}`,
      );
    }
  }
}

async function reconcileAgentSession(
  deps: InternalServerDeps,
  sessionId: string,
  now: number,
  watchingSince: string,
): Promise<void> {
  const reconciliationState =
    await deps.bridgeState.getReconciliationState(sessionId);
  const snapshot = await deps.linear.listAgentSessionActivities(sessionId, {
    lookbackAfter: new Date(
      now - deps.config.reconcileLookbackMs,
    ).toISOString(),
    ...(reconciliationState.processedThrough !== undefined
      ? { processedThrough: reconciliationState.processedThrough }
      : {}),
    signal: deps.reconciliationController?.signal,
  });
  if (reconciliationCancelled(deps)) {
    return;
  }

  // Fix 1: cold start. A session reconciliation has never seen has no basis
  // for calling anything in the window "missed" — the bridge simply was not
  // watching. Adopt the newest observed activity as the watermark, dispatch
  // nothing, and pick up genuinely new prompts from the next pass onward.
  // Without this, the first reconciliation of every session replays its whole
  // history as fresh turns.
  if (reconciliationState.initializedAt === undefined) {
    const newestSeen = snapshot.activities.at(-1);
    const sessionCreatedAt = Date.parse(snapshot.createdAt);
    // Two-sided on purpose. Newer than the marker means the bridge was running
    // when Linear created this session, so its opening prompt should have
    // arrived. Inside the lookback bounds the blast radius of a long outage and
    // keeps recovery within the window where the created claim still exists.
    const createdAfterWatching =
      Number.isFinite(sessionCreatedAt) &&
      sessionCreatedAt > Date.parse(watchingSince) &&
      sessionCreatedAt >= now - deps.config.reconcileLookbackMs;
    // A created webhook that is merely slow has not been lost, and this session
    // is too young to judge. Leave it undecided rather than settling it as
    // history: initializing here writes initializedAt permanently, so the
    // deferred decision the grace exists to allow would never happen.
    if (
      createdAfterWatching &&
      sessionCreatedAt > now - deps.config.agentSessionAckGraceMs
    ) {
      return;
    }
    const createdWhileWatching = createdAfterWatching;
    // The created webhook path claims `created:<sessionId>`; reconciliation
    // claims activity ids. Different keys, so a claim here is the only evidence
    // that the opening prompt already ran. Without this check a session whose
    // webhook arrived normally would run its first prompt twice.
    const createdClaim = createdWhileWatching
      ? await deps.bridgeState.getClaim(`created:${sessionId}`)
      : undefined;
    if (!createdWhileWatching || createdClaim !== undefined) {
      await deps.bridgeState.initializeReconciliationSession(
        sessionId,
        newestSeen === undefined ? undefined : activityCursor(newestSeen),
      );
      console.log(
        `[linear-agent-bridge] reconciliation initialized: session=${sessionId} observed=${snapshot.activities.length} dispatched=0`,
      );
      return;
    }
    // Created while this bridge was watching and never claimed: the opening
    // webhook was lost. Record the sighting without a watermark and fall
    // through to the normal dispatch path, which owns the stop fence and the
    // activity-id deduplication.
    await deps.bridgeState.initializeReconciliationSession(sessionId);
    console.log(
      `[linear-agent-bridge] reconciliation recovering lost created: session=${sessionId} observed=${snapshot.activities.length}`,
    );
  }
  const humanPrompts = snapshot.activities.filter(
    (activity) =>
      activity.type === "prompt" && activity.userId !== snapshot.appUserId,
  );
  const stopActivities = humanPrompts.filter(isStopActivity);
  const newestStop = stopActivities.at(-1);
  let newestStopClaim:
    | Awaited<ReturnType<BridgeStateStore["claimStopEvent"]>>
    | undefined;

  // Establish the newest stop fence before dispatching anything else from
  // this fetched set. claimStopEvent writes the semantic claim and fence while
  // holding the same inter-process lock.
  if (newestStop !== undefined) {
    const newestStopEvent = reconciledPromptEvent(
      sessionId,
      newestStop,
      snapshot.issueIdentifier,
    );
    newestStopClaim = await deps.bridgeState.claimStopEvent(
      reconciliationIdentity(sessionId, newestStop.id),
      activityCursor(newestStop),
      recoveryPayload(newestStopEvent),
    );
  }

  if (reconciliationCancelled(deps)) {
    return;
  }

  await warnIfSessionStalled(deps, snapshot, now);

  for (const activity of snapshot.activities) {
    if (reconciliationCancelled(deps)) {
      return;
    }
    if (
      activity.type !== "prompt" ||
      activity.userId === snapshot.appUserId
    ) {
      await deps.bridgeState.markActivityProcessed(
        sessionId,
        activityCursor(activity),
      );
      continue;
    }

    const event = reconciledPromptEvent(
      sessionId,
      activity,
      snapshot.issueIdentifier,
    );
    const identity = reconciliationIdentity(sessionId, activity.id);
    const recoverablePayload = recoveryPayload(event);
    const claim =
      newestStop?.id === activity.id
        ? newestStopClaim!
        : isStopActivity(activity)
          ? await deps.bridgeState.claimStopEvent(
              identity,
              activityCursor(activity),
              recoverablePayload,
            )
          : await deps.bridgeState.claimEvent(identity, recoverablePayload);

    if (claim.disposition === "claimed") {
      if (reconciliationCancelled(deps)) {
        return;
      }
      const outcome = await processClaimedEvent(
        event,
        identity,
        recoveryOrderFromReceipt(
          claim.receipt.recoverySequence,
          recoverablePayload,
        ),
        deps,
        "reconciliation",
        deps.reconciliationController?.signal,
      );
      if (outcome === "retryable") {
        return;
      }
    }
    // A shutdown-aborted dispatch settles without finishing the turn, so the
    // checkpoint must not move past it.
    if (reconciliationCancelled(deps)) {
      return;
    }
    // The checkpoint advances only after the prompt has a durable semantic
    // claim (or durable duplicate/superseded disposition) and any claimed
    // dispatch setup has completed.
    await deps.bridgeState.markActivityProcessed(
      sessionId,
      activityCursor(activity),
    );
  }
}

function reconciliationCancelled(deps: InternalServerDeps): boolean {
  return deps.closing || deps.reconciliationController?.signal.aborted === true;
}

function isExpectedReconciliationShutdown(
  deps: InternalServerDeps,
  error: unknown,
): boolean {
  return (
    deps.closing &&
    (deps.reconciliationController?.signal.aborted === true ||
      (error instanceof Error && error.name === "AbortError"))
  );
}

async function warnIfSessionStalled(
  deps: InternalServerDeps,
  snapshot: LinearAgentSessionActivities,
  now: number,
): Promise<void> {
  const fence = (
    await deps.bridgeState.getReconciliationState(snapshot.id)
  ).stopFence;
  const graceCutoff = now - deps.config.agentSessionAckGraceMs;
  let stalled: ReconciledAgentActivity | undefined;
  for (const activity of [...snapshot.activities].reverse()) {
    if (
      activity.type !== "prompt" ||
      activity.userId === snapshot.appUserId ||
      isStopActivity(activity) ||
      Date.parse(activity.createdAt) > graceCutoff ||
      (fence !== undefined && compareCursors(activityCursor(activity), fence) <= 0)
    ) {
      continue;
    }
    if ((await deps.bridgeState.getClaim(activity.id)) === undefined) {
      stalled = activity;
      break;
    }
  }
  if (
    stalled !== undefined &&
    (await deps.bridgeState.claimStalledSessionWarning(
      snapshot.id,
      stalled.id,
      STALLED_WARNING_INTERVAL_MS,
    ))
  ) {
    const ageMs = Math.max(0, now - Date.parse(stalled.createdAt));
    console.warn(
      `[linear-agent-bridge] stalled_agent_session session=${boundedLogValue(snapshot.id)} activity=${boundedLogValue(stalled.id)} age_ms=${ageMs}`,
    );
  }
}

function reconciliationIdentity(
  sessionId: string,
  activityId: string,
): IngressEventIdentity {
  return {
    webhookId: `reconcile:${activityId}`,
    executionId: activityId,
    linearSessionId: sessionId,
    action: "prompted",
  };
}

function reconciledPromptEvent(
  sessionId: string,
  activity: ReconciledAgentActivity,
  issueIdentifier?: string,
): LinearAgentSessionEvent {
  return {
    webhookId: `reconcile:${activity.id}`,
    action: "prompted",
    agentSession: {
      id: sessionId,
      ...(issueIdentifier !== undefined
        ? {
            issue: {
              id: issueIdentifier,
              identifier: issueIdentifier,
              title: "",
            },
          }
        : {}),
    },
    webhookTimestamp: Date.parse(activity.createdAt),
    agentActivity: {
      id: activity.id,
      createdAt: activity.createdAt,
      content: {
        type: "prompt",
        body: activity.body ?? "",
        ...(activity.signal !== undefined ? { signal: activity.signal } : {}),
      },
      ...(activity.signal !== undefined ? { signal: activity.signal } : {}),
    },
  };
}

function activityCursor(activity: ReconciledAgentActivity): ReconciliationCursor {
  return { createdAt: activity.createdAt, id: activity.id };
}

function isStopActivity(activity: ReconciledAgentActivity): boolean {
  return (
    activity.signal === "stop" || /^stop[.!]?$/i.test((activity.body ?? "").trim())
  );
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
  // Linear's own words: webhookId is "ID uniquely identifying this webhook",
  // the configuration, constant across every delivery it sends. The per-payload
  // identity is the Linear-Delivery header, "a UUID (v4) that uniquely
  // identifies this payload". Keying durable receipts on webhookId meant the
  // first delivery took the slot and every later one collided with it.
  const deliveryHeaderRaw = req.headers["linear-delivery"];
  const deliveryHeader = Array.isArray(deliveryHeaderRaw)
    ? deliveryHeaderRaw[0]
    : deliveryHeaderRaw;

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

  const identity = eventIdentity(event, deliveryHeader);
  const recoverablePayload = recoveryPayload(event);
  const repairingLegacyReceipt =
    !deps.dispatchReady &&
    !deps.recoveryBlocked &&
    deps.recoveryAwaitingRedelivery;
  let claimResult;
  let recoveryOrder: RecoveryOrder | undefined;
  try {
    claimResult = repairingLegacyReceipt
      ? await deps.bridgeState.claimEvent(identity, recoverablePayload, {
          repairLegacyOnly: true,
        })
      : isStopEvent(event)
        ? await deps.bridgeState.claimStopEvent(
            identity,
            eventActivityCursor(event),
            recoverablePayload,
          )
        : await deps.bridgeState.claimEvent(identity, recoverablePayload);
    if (claimResult.disposition === "claimed") {
      recoveryOrder = recoveryOrderFromReceipt(
        claimResult.receipt.recoverySequence,
        recoverablePayload,
      );
      // The stop fence is already durable. Abort matching in-memory work
      // before acknowledging the delivery so no finalizer can cross an
      // avoidable post-stop side-effect window while post-response work waits.
      if (isStopEvent(event)) {
        abortSessionRuns(deps, event.agentSession.id, recoveryOrder);
      }
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
  scope: "webhook" | "recovery" | "reconciliation",
  dispatchSignal?: AbortSignal,
): Promise<"settled" | "retryable"> {
  try {
    await processClaimedWebhook(
      event,
      identity,
      recoveryOrder,
      deps,
      scope,
      dispatchSignal,
    );
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
  const issue = asRecord((agentSession as Record<string, unknown>).issue);
  if (
    ((agentSession as Record<string, unknown>).issue !== undefined &&
      issue === undefined) ||
    (issue !== undefined &&
      ((issue.id !== undefined && !isBoundedIdentifier(issue.id)) ||
        (issue.identifier !== undefined &&
          !isBoundedIdentifier(issue.identifier)) ||
        (issue.title !== undefined && typeof issue.title !== "string")))
  ) {
    return undefined;
  }
  if (obj.action === "prompted") {
    const agentActivity = asRecord(obj.agentActivity);
    if (
      !isBoundedIdentifier(agentActivity?.id) ||
      typeof agentActivity?.createdAt !== "string" ||
      !Number.isFinite(Date.parse(agentActivity.createdAt))
    ) {
      return undefined;
    }
    const content =
      agentActivity.content === undefined
        ? undefined
        : asRecord(agentActivity.content);
    if (
      (agentActivity.content !== undefined && content === undefined) ||
      (content?.type !== undefined && content.type !== "prompt") ||
      (content?.body !== undefined && typeof content.body !== "string") ||
      (content?.signal !== undefined &&
        content.signal !== null &&
        typeof content.signal !== "string") ||
      (agentActivity.body !== undefined &&
        typeof agentActivity.body !== "string") ||
      (agentActivity.signal !== undefined &&
        agentActivity.signal !== null &&
        typeof agentActivity.signal !== "string")
    ) {
      return undefined;
    }
  }

  return obj as unknown as LinearAgentSessionEvent;
}

/**
 * Build the durable ingress identity for a delivery.
 *
 * `deliveryId` is the Linear-Delivery header and is the only per-payload
 * value Linear provides. `event.webhookId` identifies the webhook
 * configuration and repeats on every delivery, so it cannot key a receipt.
 * When the header is absent the payload's webhookId is combined with the
 * execution identity, which keeps receipts distinct per unit of work rather
 * than collapsing every delivery onto one key.
 */
function eventIdentity(
  event: LinearAgentSessionEvent,
  deliveryId?: string,
): IngressEventIdentity {
  const executionId =
    event.action === "created"
      ? `created:${event.agentSession.id}`
      : event.agentActivity.id;
  return {
    webhookId: isBoundedIdentifier(deliveryId)
      ? deliveryId
      : `${event.webhookId}:${executionId}`,
    executionId,
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
  // Linear sends `null` for an absent signal; the persisted envelope is
  // null-free, so normalize at the boundary.
  const signal =
    (event.agentActivity.content?.signal ?? event.agentActivity.signal) ??
    undefined;
  return {
    action: "prompted",
    occurredAt,
    prompt,
    stop: isStopPrompt(prompt, signal),
    ...(signal !== undefined ? { signal } : {}),
  };
}

function isStopEvent(event: LinearAgentSessionEvent): boolean {
  if (event.action !== "prompted") {
    return false;
  }
  const prompt =
    event.agentActivity.content?.body ?? event.agentActivity.body ?? "";
  return (
    event.agentActivity.content?.signal === "stop" ||
    event.agentActivity.signal === "stop" ||
    /^stop[.!]?$/i.test(prompt.trim())
  );
}

function eventActivityCursor(
  event: LinearAgentSessionEvent,
): ReconciliationCursor {
  if (event.action !== "prompted") {
    throw new Error("Only prompted events have activity cursors");
  }
  return {
    createdAt:
      event.agentActivity.createdAt ??
      new Date(event.webhookTimestamp).toISOString(),
    id: event.agentActivity.id,
  };
}

async function processClaimedWebhook(
  event: LinearAgentSessionEvent,
  identity: IngressEventIdentity,
  recoveryOrder: RecoveryOrder,
  deps: InternalServerDeps,
  scope: "webhook" | "recovery" | "reconciliation",
  dispatchSignal?: AbortSignal,
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
      (event.agentActivity.content?.signal ?? event.agentActivity.signal) ??
        undefined,
    );
  // Register before crossing the durable dispatch boundary. A concurrent stop
  // therefore either wins the state lock and fences this prompt, or sees and
  // aborts its controller after dispatch has begun.
  const controller = isStop
    ? undefined
    : registerSessionRun(deps, sessionId, recoveryOrder);
  const queuedBehindExistingRun =
    controller !== undefined && (deps.activeRuns.get(sessionId)?.size ?? 0) > 1;
  const dispatchCursor =
    scope !== "webhook" && event.action === "prompted" && !isStop
      ? eventActivityCursor(event)
      : undefined;
  let enqueued = false;
  try {
    dispatchSignal?.throwIfAborted();
    try {
      const dispatch = await deps.bridgeState.beginEventDispatch(
        identity.webhookId,
        dispatchCursor,
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
      const dispatch = await deps.bridgeState.beginEventDispatch(
        identity.webhookId,
        dispatchCursor,
      );
      if (dispatch === "superseded") {
        return;
      }
    }
    dispatchSignal?.throwIfAborted();
    if (controller?.signal.aborted === true) {
      if (!deps.closing) {
        await deps.bridgeState.completeEvent(identity.webhookId);
      }
      return;
    }
    console.log(
      `[linear-agent-bridge] agent session event: action=${event.action} session=${sessionId} webhook=${identity.webhookId}`,
    );

    if (event.action === "created") {
      // 10s liveness rule: emit a thought before doing anything else.
      dispatchSignal?.throwIfAborted();
      // Start the outbound liveness write, then reserve this event's FIFO lane
      // position synchronously. The queued task waits for the write before it
      // performs any work, while a later prompt cannot overtake the opening
      // event during the outbound await.
      const liveness = emitActivity(
        deps,
        identity.executionId,
        "liveness",
        sessionId,
        {
          type: "thought",
          body: CREATED_THOUGHT_BODY,
        },
        {
          ephemeral: true,
          signal:
            dispatchSignal === undefined
              ? controller!.signal
              : AbortSignal.any([controller!.signal, dispatchSignal]),
        },
      ).then(() => dispatchSignal?.throwIfAborted());

      enqueueSessionRun(
        deps,
        { linearSessionId: sessionId, prompt },
        issueIdentifier,
        controller!,
        identity,
        deps.config.autonomousGoalLabelId !== undefined &&
          event.agentSession.issue?.id !== undefined
          ? {
              autonomousGoalMode: "start",
              autonomousGoalIssueId: event.agentSession.issue.id,
              autonomousGoalOpeningSequence: recoveryOrder.sequence,
              executionBarrier: liveness,
            }
          : { executionBarrier: liveness },
      );
      enqueued = true;
      try {
        await liveness;
      } catch (error) {
        if (controller!.signal.aborted) {
          if (!deps.closing) {
            await deps.bridgeState.completeEvent(identity.webhookId);
          }
          return;
        }
        throw error;
      }
      return;
    }

    if (isStop) {
      dispatchSignal?.throwIfAborted();
      abortSessionRuns(deps, sessionId, recoveryOrder);
      dispatchSignal?.throwIfAborted();
      await emitActivity(
        deps,
        identity.executionId,
        "stop-response",
        sessionId,
        { type: "response", body: STOPPED_RESPONSE_BODY },
        {
          signal:
            dispatchSignal === undefined
              ? deps.shutdownController.signal
              : AbortSignal.any([
                  deps.shutdownController.signal,
                  dispatchSignal,
                ]),
        },
      );
      if (!deps.closing) {
        await deps.bridgeState.completeEvent(identity.webhookId);
      }
      return;
    }

    dispatchSignal?.throwIfAborted();
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
    dispatchSignal?.throwIfAborted();
    const liveness =
      deps.queue.size(sessionId) === 0 && !queuedBehindExistingRun
        ? emitActivity(
            deps,
            identity.executionId,
            "liveness",
            sessionId,
            {
              type: "thought",
              body: PROMPTED_THOUGHT_BODY,
            },
            {
              ephemeral: true,
              signal:
                dispatchSignal === undefined
                  ? controller!.signal
                  : AbortSignal.any([controller!.signal, dispatchSignal]),
            },
          ).then(() => dispatchSignal?.throwIfAborted())
        : undefined;
    enqueueSessionRun(
      deps,
      { linearSessionId: sessionId, prompt },
      issueIdentifier,
      controller!,
      identity,
      {
        loadStoredSessionAtExecution: true,
        queuedBehindExistingRun,
        ...(liveness !== undefined ? { executionBarrier: liveness } : {}),
        // The lane loads goal state at execution time. This recognizes an
        // existing goal even after configuration is removed without putting
        // an await before enqueue that could invert per-session FIFO order.
        autonomousGoalMode: "guidance",
      },
    );
    enqueued = true;
    if (liveness !== undefined) {
      try {
        await liveness;
      } catch (error) {
        if (controller!.signal.aborted) {
          if (!deps.closing) {
            await deps.bridgeState.completeEvent(identity.webhookId);
          }
          return;
        }
        throw error;
      }
    }
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
  options: {
    loadStoredSessionAtExecution?: boolean;
    autonomousGoalMode?: "start" | "guidance";
    autonomousGoalIssueId?: string;
    autonomousGoalOpeningSequence?: number;
    queuedBehindExistingRun?: boolean;
    executionBarrier?: Promise<void>;
  } = {},
): void {
  const laneDepth = deps.queue.size(request.linearSessionId);
  const isQueuedFollowUp =
    options.loadStoredSessionAtExecution === true &&
    (laneDepth > 0 || options.queuedBehindExistingRun === true);
  const queuedNotice = isQueuedFollowUp
    ? emitActivity(
        deps,
        identity.executionId,
        "queued-notice",
        request.linearSessionId,
        {
          type: "thought",
          body: "Your follow-up is queued behind the current turn on this thread; I'll take it as soon as that turn finishes.",
        },
        { signal: controller.signal },
      ).catch((error: unknown) => {
        if (!controller.signal.aborted) {
          console.error(
            `[linear-agent-bridge] queued notice delivery failed: session=${request.linearSessionId} error=${boundedErrorClass(error)}`,
          );
        }
      })
    : Promise.resolve();
  // Observe a barrier rejection immediately even when this item is queued
  // behind another turn. Re-throw it only from the lane callback so shutdown
  // cannot leave an unhandled rejected promise waiting for its FIFO slot.
  const executionBarrier = options.executionBarrier?.then(
    () => ({ ok: true as const }),
    (error: unknown) => ({ ok: false as const, error }),
  );
  const queuedRun = deps.queue.enqueue(request.linearSessionId, async () => {
    let effectiveRequest = request;
    let effectiveIssueIdentifier = issueIdentifier;
    let terminalReason: TurnTerminalReason = controller.signal.aborted
      ? "stopped"
      : "failed";
    try {
      const barrierResult = await executionBarrier;
      if (barrierResult?.ok === false) {
        if (controller.signal.aborted) {
          terminalReason = "stopped";
          return;
        }
        throw barrierResult.error;
      }
      if (controller.signal.aborted) {
        terminalReason = "stopped";
        return;
      }
      await queuedNotice;
      let goal = await deps.bridgeState.getAutonomousGoal(
        request.linearSessionId,
      );
      if (options.loadStoredSessionAtExecution === true) {
        const storedSession = await deps.store.get(request.linearSessionId);
        if (
          storedSession?.runtimeSessionId !== undefined &&
          storedSession.runtime !== deps.runtime.name
        ) {
          if (
            goal !== undefined &&
            goal.status !== "completed" &&
            goal.status !== "stopped" &&
            goal.status !== "declined"
          ) {
            await blockAutonomousGoalWithNotice(
              deps,
              goal,
              `goal-provider-mismatch-${goal.step}`,
              RUNTIME_PROVIDER_MISMATCH_BODY,
              controller.signal,
              options.autonomousGoalMode === "guidance"
                ? identity.executionId
                : undefined,
            );
            terminalReason = "completed";
            return;
          }
          await emitActivity(
            deps,
            identity.executionId,
            "runtime-provider-mismatch",
            request.linearSessionId,
            { type: "error", body: RUNTIME_PROVIDER_MISMATCH_BODY },
            { signal: controller.signal },
          );
          return;
        }
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
          `[linear-agent-bridge] turn start: session=${request.linearSessionId} queue=${deps.queue.size(request.linearSessionId)}`,
        );
        if (isQueuedFollowUp) {
          try {
            await emitActivity(
              deps,
              identity.executionId,
              "queued-start",
              request.linearSessionId,
              {
                type: "thought",
                body: PROMPTED_THOUGHT_BODY,
              },
              { ephemeral: true, signal: controller.signal },
            );
          } catch (error) {
            if (controller.signal.aborted) {
              terminalReason = "stopped";
              return;
            }
            throw error;
          }
        }
        if (
          options.autonomousGoalMode === "start" &&
          goal === undefined &&
          options.autonomousGoalIssueId !== undefined &&
          options.autonomousGoalOpeningSequence !== undefined
        ) {
          goal = await deps.bridgeState.prepareAutonomousGoal({
            linearSessionId: request.linearSessionId,
            issueId: options.autonomousGoalIssueId,
            ...(effectiveIssueIdentifier !== undefined
              ? { issueIdentifier: effectiveIssueIdentifier }
              : {}),
            runtime: deps.runtime.name,
            openingRecoverySequence: options.autonomousGoalOpeningSequence,
            objective: request.prompt,
          });
        }
        if (
          options.autonomousGoalMode === "start" &&
          goal?.status === "stopped"
        ) {
          terminalReason = "stopped";
          return;
        }
        if (
          goal !== undefined &&
          goal.status !== "completed" &&
          goal.status !== "stopped" &&
          goal.status !== "declined" &&
          goal.runtime !== deps.runtime.name
        ) {
          await blockAutonomousGoalWithNotice(
            deps,
            goal,
            `goal-provider-mismatch-${goal.step}`,
            RUNTIME_PROVIDER_MISMATCH_BODY,
            controller.signal,
            options.autonomousGoalMode === "guidance"
              ? identity.executionId
              : undefined,
          );
          terminalReason = "completed";
          return;
        }
        if (goal !== undefined && options.autonomousGoalMode === "guidance") {
          if (goal.status === "blocked" && goal.pendingNotice !== undefined) {
            await recoverAutonomousGoalPendingNotice(
              deps,
              goal,
              controller.signal,
            );
            goal =
              (await deps.bridgeState.getAutonomousGoal(
                request.linearSessionId,
              )) ?? goal;
            if (goal.status === "stopped") {
              terminalReason = "stopped";
              return;
            }
          }
          if (
            goal.status !== "completed" &&
            goal.status !== "stopped" &&
            goal.status !== "declined" &&
            deps.config.autonomousGoalLabelId === undefined
          ) {
            await blockAutonomousGoalWithNotice(
              deps,
              goal,
              `goal-configuration-removed-${goal.step}`,
              GOAL_LABEL_REMOVED_BODY,
              controller.signal,
              identity.executionId,
            );
            terminalReason = "completed";
            return;
          }
          if (goal.status === "authorizing") {
            goal = await resolveAutonomousGoalAuthorization(
              deps,
              goal,
              controller.signal,
            );
          }
          if (
            goal.status === "blocked" ||
            goal.status === "active" ||
            goal.status === "completing"
          ) {
            const context = await currentGoalIssueContext(
              deps,
              goal,
              controller.signal,
            );
            if (!context.authorized) {
              await blockAutonomousGoalWithNotice(
                deps,
                goal,
                `goal-label-removed-${goal.step}`,
                GOAL_LABEL_REMOVED_BODY,
                controller.signal,
                identity.executionId,
              );
              terminalReason = "completed";
              return;
            }
            goal = await deps.bridgeState.resumeAutonomousGoal(
              request.linearSessionId,
              identity.executionId,
            );
          }
        }
        if (
          goal?.status === "authorizing" &&
          options.autonomousGoalMode === "start"
        ) {
          goal = await resolveAutonomousGoalAuthorization(
            deps,
            goal,
            controller.signal,
          );
        }
        if (goal?.status === "completing") {
          terminalReason = await finishAutonomousGoalCompletion(
            deps,
            goal,
            controller.signal,
          );
        } else if (goal?.status === "active") {
          terminalReason = await runAutonomousGoalTask(
            deps,
            { ...effectiveRequest, abortController: controller },
            effectiveIssueIdentifier,
            options.autonomousGoalMode === "guidance"
              ? { firstStepIsGuidance: true }
              : {},
          );
        } else if (
          goal !== undefined &&
          goal.status !== "completed" &&
          goal.status !== "stopped" &&
          goal.status !== "declined"
        ) {
          // Nonterminal autonomous state never falls through to an ordinary
          // runtime turn. Recovery or guidance must first cross its explicit
          // goal transition and preserve the lifecycle fences.
          terminalReason = "completed";
          return;
        } else {
          terminalReason = (
            await runSessionTask(
              deps,
              { ...effectiveRequest, abortController: controller },
              effectiveIssueIdentifier,
              { kind: "ingress", executionId: identity.executionId },
            )
          ).terminalReason;
        }
      }
    } finally {
      console.log(
        `[linear-agent-bridge] turn terminal: session=${request.linearSessionId} reason=${terminalReason} queue=${Math.max(0, deps.queue.size(request.linearSessionId) - 1)}`,
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
  });


  void queuedRun.catch((error: unknown) => {
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

async function currentGoalIssueContext(
  deps: InternalServerDeps,
  goal: AutonomousGoalState,
  signal?: AbortSignal,
) {
  const labelId = deps.config.autonomousGoalLabelId;
  if (labelId === undefined) {
    throw new Error("Autonomous goal label configuration is unavailable");
  }
  return await deps.linear.getAutonomousGoalIssueContext(
    goal.issueId,
    labelId,
    signal,
  );
}

async function resolveAutonomousGoalAuthorization(
  deps: InternalServerDeps,
  goal: AutonomousGoalState,
  signal?: AbortSignal,
): Promise<AutonomousGoalState> {
  const context = await currentGoalIssueContext(deps, goal, signal);
  if (!context.authorized || context.alreadyCompleted) {
    return await deps.bridgeState.declineAutonomousGoal(goal.linearSessionId);
  }
  return await deps.bridgeState.activateAutonomousGoal(goal.linearSessionId);
}

async function blockAutonomousGoalWithNotice(
  deps: InternalServerDeps,
  goal: AutonomousGoalState,
  activityKey: string,
  body: string,
  signal?: AbortSignal,
  guidanceExecutionId?: string,
): Promise<void> {
  signal?.throwIfAborted();
  const blocked = await deps.bridgeState.blockAutonomousGoal(
    goal.linearSessionId,
    activityKey,
    body,
    guidanceExecutionId,
  );
  await emitAutonomousGoalActivity(
    deps,
    blocked.linearSessionId,
    activityKey,
    { type: "elicitation", body },
    signal !== undefined ? { signal } : {},
  );
  await deps.bridgeState.clearAutonomousGoalPendingNotice(
    blocked.linearSessionId,
    activityKey,
  );
}

async function goalActivityExists(
  deps: InternalServerDeps,
  goal: AutonomousGoalState,
  activityKey: string,
  signal?: AbortSignal,
): Promise<boolean> {
  const activityId =
    await deps.bridgeState.getOrCreateAutonomousGoalActivityId(
      goal.linearSessionId,
      activityKey,
    );
  const snapshot = await deps.linear.listAgentSessionActivities(
    goal.linearSessionId,
    {
      lookbackAfter: goal.createdAt,
      ...(signal !== undefined ? { signal } : {}),
    },
  );
  return snapshot.activities.some((activity) => activity.id === activityId);
}

async function recoverAutonomousGoalPendingNotice(
  deps: InternalServerDeps,
  goal: AutonomousGoalState,
  signal?: AbortSignal,
): Promise<void> {
  const pending = goal.pendingNotice;
  if (pending === undefined) {
    return;
  }
  if (!(await goalActivityExists(deps, goal, pending.activityKey, signal))) {
    const body = await deps.bridgeState.getAutonomousGoalPendingNoticeBody(
      goal.linearSessionId,
      pending.activityKey,
    );
    if (body === undefined) {
      return;
    }
    await emitAutonomousGoalActivity(
      deps,
      goal.linearSessionId,
      pending.activityKey,
      pending.kind === "completion"
        ? { type: "response", body }
        : { type: "elicitation", body },
      signal !== undefined ? { signal } : {},
    );
  }
  const current = await deps.bridgeState.getAutonomousGoal(goal.linearSessionId);
  if (current?.status === "stopped") {
    return;
  }
  if (pending.kind === "completion") {
    await deps.bridgeState.completeAutonomousGoal(goal.linearSessionId);
  } else {
    await deps.bridgeState.clearAutonomousGoalPendingNotice(
      goal.linearSessionId,
      pending.activityKey,
    );
  }
}

async function finishAutonomousGoalCompletion(
  deps: InternalServerDeps,
  goal: AutonomousGoalState,
  signal?: AbortSignal,
): Promise<TurnTerminalReason> {
  signal?.throwIfAborted();
  const current = await deps.bridgeState.getAutonomousGoal(goal.linearSessionId);
  if (current?.status === "stopped") {
    return "stopped";
  }
  if (current?.status !== "completing" || current.completionStateId === undefined) {
    return "failed";
  }
  const context = await currentGoalIssueContext(deps, current, signal);
  const postQueryGoal = await deps.bridgeState.getAutonomousGoal(
    current.linearSessionId,
  );
  if (postQueryGoal?.status === "stopped") {
    return "stopped";
  }
  if ((postQueryGoal?.pendingGuidanceIds.length ?? 0) > 0) {
    await deps.bridgeState.resumeAutonomousGoal(current.linearSessionId);
    return "completed";
  }
  if (!context.authorized) {
    await blockAutonomousGoalWithNotice(
      deps,
      current,
      `goal-label-removed-${current.step}`,
      GOAL_LABEL_REMOVED_BODY,
      signal,
    );
    return "completed";
  }
  if (!context.alreadyCompleted) {
    const dispatched =
      await deps.bridgeState.beginAutonomousGoalCompletionDispatch(
        current.linearSessionId,
      );
    if (dispatched.status === "stopped") {
      return "stopped";
    }
    if (dispatched.status === "active") {
      return "completed";
    }
    signal?.throwIfAborted();
    try {
      await deps.linear.completeIssue(
        context.issueId,
        current.completionStateId,
        signal,
      );
    } catch (error) {
      if (signal?.aborted === true) {
        return "stopped";
      }
      await blockAutonomousGoalWithNotice(
        deps,
        current,
        `goal-completion-retry-${current.step}`,
        GOAL_COMPLETION_RETRY_BODY,
        signal,
      );
      return "completed";
    }
  }
  signal?.throwIfAborted();
  const latest = await deps.bridgeState.getAutonomousGoal(current.linearSessionId);
  if (latest?.status === "stopped") {
    return "stopped";
  }
  const pending = latest?.pendingNotice;
  if (latest?.status !== "completing" || pending?.kind !== "completion") {
    return "failed";
  }
  // The issue mutation and activity emission cannot share one transaction.
  // Reconcile by the stable Linear activity id before emitting so a crash
  // between those writes does not duplicate the completion response.
  await recoverAutonomousGoalPendingNotice(deps, latest, signal);
  return "completed";
}

async function runAutonomousGoalTask(
  deps: InternalServerDeps,
  initialRequest: SessionRequest,
  issueIdentifier: string | undefined,
  options: { firstStepIsGuidance?: boolean } = {},
): Promise<TurnTerminalReason> {
  let firstStep = true;
  const goalSignal =
    initialRequest.abortController?.signal ?? deps.shutdownController.signal;
  while (!deps.closing && !goalSignal.aborted) {
    const goal = await deps.bridgeState.getAutonomousGoal(
      initialRequest.linearSessionId,
    );
    if (goal?.status === "stopped") {
      return "stopped";
    }
    if (goal?.status !== "active") {
      return goal?.status === "completed" || goal?.status === "blocked"
        ? "completed"
        : "failed";
    }
    if (goal.stepsSinceGuidance >= deps.config.autonomousGoalMaxSteps) {
      await blockAutonomousGoalWithNotice(
        deps,
        goal,
        `goal-step-limit-${goal.step}`,
        GOAL_STEP_LIMIT_BODY,
        goalSignal,
      );
      return "completed";
    }
    const context = await currentGoalIssueContext(
      deps,
      goal,
      goalSignal,
    );
    if (!context.authorized) {
      await blockAutonomousGoalWithNotice(
        deps,
        goal,
        `goal-label-removed-${goal.step}`,
        GOAL_LABEL_REMOVED_BODY,
        goalSignal,
      );
      return "completed";
    }
    const started =
      firstStep && options.firstStepIsGuidance === true
        ? await deps.bridgeState.beginAutonomousGoalGuidanceStep(
            goal.linearSessionId,
          )
        : await deps.bridgeState.beginAutonomousGoalStep(goal.linearSessionId);
    if (started.disposition !== "started") {
      return started.goal?.status === "stopped" ? "stopped" : "completed";
    }
    const step = started.goal.step;
    const storedSession = await deps.store.get(goal.linearSessionId);
    if (
      storedSession?.runtimeSessionId !== undefined &&
      storedSession.runtime !== deps.runtime.name
    ) {
      await blockAutonomousGoalWithNotice(
        deps,
        started.goal,
        `goal-provider-mismatch-${step}`,
        RUNTIME_PROVIDER_MISMATCH_BODY,
        goalSignal,
      );
      return "completed";
    }
    const openingObjective =
      storedSession?.runtimeSessionId === undefined
        ? await deps.bridgeState.getAutonomousGoalObjective(
            goal.linearSessionId,
          )
        : undefined;
    const stepPrompt =
      openingObjective !== undefined &&
      openingObjective !== initialRequest.prompt
        ? `${openingObjective}\n\nCurrent guidance:\n${initialRequest.prompt}`
        : initialRequest.prompt;
    const request: SessionRequest = {
      linearSessionId: goal.linearSessionId,
      prompt: autonomousGoalPrompt(stepPrompt, {
        step,
        maxSteps: deps.config.autonomousGoalMaxSteps,
        continuation: !firstStep,
      }),
      ...(storedSession?.runtimeSessionId !== undefined
        ? { resumeSessionId: storedSession.runtimeSessionId }
        : initialRequest.resumeSessionId !== undefined
          ? { resumeSessionId: initialRequest.resumeSessionId }
          : {}),
      ...(initialRequest.abortController !== undefined
        ? { abortController: initialRequest.abortController }
        : {}),
    };
    const outcome = await runSessionTask(
      deps,
      request,
      issueIdentifier ?? goal.issueIdentifier,
      {
        kind: "goal",
        linearSessionId: goal.linearSessionId,
        prefix: `goal-step-${step}`,
      },
      { captureResponse: true },
    );
    if (outcome.terminalReason !== "completed") {
      // Inactivity has already aborted the provider-turn controller. Register
      // a fresh, session-scoped finalizer so the durable block can complete,
      // while a Linear stop or service shutdown can still abort its notice.
      const finalizationController =
        outcome.terminalReason === "inactive"
          ? registerSessionRun(deps, goal.linearSessionId, {
              action: "created",
              occurredAt: started.goal.updatedAt,
              sequence: Number.MAX_SAFE_INTEGER,
            })
          : undefined;
      const lifecycleSignal = finalizationController?.signal ?? goalSignal;
      try {
        const latest = await deps.bridgeState.getAutonomousGoal(
          goal.linearSessionId,
        );
        if (latest?.status === "stopped" || deps.closing) {
          return "stopped";
        }
        await blockAutonomousGoalWithNotice(
          deps,
          latest ?? started.goal,
          `goal-runtime-failed-${step}`,
          GOAL_RUNTIME_FAILED_BODY,
          lifecycleSignal,
        );
        const finalized = await deps.bridgeState.getAutonomousGoal(
          goal.linearSessionId,
        );
        if (finalized?.status === "stopped") {
          return "stopped";
        }
        return outcome.terminalReason;
      } catch (error) {
        if (lifecycleSignal.aborted) {
          const latest = await deps.bridgeState.getAutonomousGoal(
            goal.linearSessionId,
          );
          if (latest?.status === "stopped" || deps.closing) {
            return "stopped";
          }
        }
        throw error;
      } finally {
        if (finalizationController !== undefined) {
          unregisterRun(deps, goal.linearSessionId, finalizationController);
        }
      }
    }
    const decision =
      outcome.response === undefined
        ? undefined
        : parseAutonomousGoalDecision(outcome.response);
    if (decision === undefined) {
      await blockAutonomousGoalWithNotice(
        deps,
        started.goal,
        `goal-protocol-error-${step}`,
        GOAL_PROTOCOL_ERROR_BODY,
        goalSignal,
      );
      return "completed";
    }
    // Human guidance already waiting in this session's FIFO lane supersedes
    // every provider decision, including completion. Return the durable goal
    // to active and yield before interpreting the result so no issue mutation
    // can race ahead of requirements the user has already supplied.
    const decisionGoal = await deps.bridgeState.getAutonomousGoal(
      goal.linearSessionId,
    );
    if (decisionGoal?.status === "stopped") {
      return "stopped";
    }
    if (
      deps.queue.size(goal.linearSessionId) > 1 ||
      (decisionGoal?.pendingGuidanceIds.length ?? 0) > 0
    ) {
      await deps.bridgeState.continueAutonomousGoal(goal.linearSessionId);
      await emitAutonomousGoalActivity(
        deps,
        goal.linearSessionId,
        `goal-step-${step}-summary`,
        {
          type: "thought",
          body:
            decision.status === "completed"
              ? `${decision.message} I will apply the guidance already queued before completing the issue.`
              : decision.message,
        },
        { signal: goalSignal },
      );
      return "completed";
    }
    if (decision.status === "continue") {
      await deps.bridgeState.continueAutonomousGoal(goal.linearSessionId);
      await emitAutonomousGoalActivity(
        deps,
        goal.linearSessionId,
        `goal-step-${step}-summary`,
        { type: "thought", body: decision.message },
        { signal: goalSignal },
      );
      firstStep = false;
      continue;
    }
    if (decision.status === "blocked") {
      await blockAutonomousGoalWithNotice(
        deps,
        started.goal,
        `goal-step-${step}-blocked`,
        decision.message,
        goalSignal,
      );
      return "completed";
    }

    const completionContext = await currentGoalIssueContext(
      deps,
      started.goal,
      goalSignal,
    );
    const postCompletionQueryGoal = await deps.bridgeState.getAutonomousGoal(
      goal.linearSessionId,
    );
    if (postCompletionQueryGoal?.status === "stopped") {
      return "stopped";
    }
    if ((postCompletionQueryGoal?.pendingGuidanceIds.length ?? 0) > 0) {
      await deps.bridgeState.continueAutonomousGoal(goal.linearSessionId);
      await emitAutonomousGoalActivity(
        deps,
        goal.linearSessionId,
        `goal-step-${step}-summary`,
        {
          type: "thought",
          body: `${decision.message} I will apply the guidance already queued before completing the issue.`,
        },
        { signal: goalSignal },
      );
      return "completed";
    }
    if (!completionContext.authorized) {
      await blockAutonomousGoalWithNotice(
        deps,
        started.goal,
        `goal-label-removed-${step}`,
        GOAL_LABEL_REMOVED_BODY,
        goalSignal,
      );
      return "completed";
    }
    const completionStateId = completionContext.alreadyCompleted
      ? completionContext.currentStateId
      : completionContext.completedStateId;
    if (completionStateId === undefined) {
      await blockAutonomousGoalWithNotice(
        deps,
        started.goal,
        `goal-completion-state-${step}`,
        GOAL_COMPLETION_STATE_BODY,
        goalSignal,
      );
      return "completed";
    }
    const completionKey = `goal-step-${step}-completion`;
    const finalBody = `${decision.message}\n\nVerification: ${decision.verification}\n\n${completionContext.issueIdentifier} was moved to completed.`;
    const completing = await deps.bridgeState.beginAutonomousGoalCompletion(
      goal.linearSessionId,
      completionStateId,
      completionKey,
      finalBody,
    );
    return await finishAutonomousGoalCompletion(
      deps,
      completing,
      goalSignal,
    );
  }
  return "stopped";
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
  activityScope: ActivityScope,
  options: { captureResponse?: boolean } = {},
): Promise<RuntimeTurnOutcome> {
  const controller = request.abortController;
  const turnStartedAt = deps.now?.() ?? Date.now();
  let lastNoticeAt = turnStartedAt;
  let noticeSequence = 0;
  let lastAction: { action: string; parameter: string } | undefined;
  let activitySequence = 0;
  let capturedResponse: string | undefined;
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
        if (
          options.captureResponse === true &&
          event.kind === "activity" &&
          event.activity.type === "response"
        ) {
          capturedResponse = event.activity.body;
        } else {
          await handleRuntimeEvent(
            deps,
            request,
            issueIdentifier,
            event,
            activityScope,
            activitySequence,
          );
        }
        if (event.kind === "activity") {
          activitySequence += 1;
        }
        if (event.kind === "activity" && event.activity.type === "action") {
          lastAction = {
            action: event.activity.action,
            parameter: event.activity.parameter,
          };
        }
        const now = deps.now?.() ?? Date.now();
        if (now - lastNoticeAt >= deps.config.progressNoticeIntervalMs) {
          noticeSequence += 1;
          await emitScopedActivity(
            deps,
            activityScope,
            `progress-${noticeSequence}`,
            request.linearSessionId,
            {
              type: "thought",
              body:
                lastAction === undefined
                  ? `Still working (${formatDuration(now - turnStartedAt)}).`
                  : `Still working (${formatDuration(now - turnStartedAt)}). Last step: ${lastAction.action} — ${lastAction.parameter}`,
            },
            controller !== undefined ? { signal: controller.signal } : {},
          );
          lastNoticeAt = now;
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
    void emitScopedActivity(
      deps,
      activityScope,
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
    return { terminalReason: "inactive" };
  }

  if (outcome.source === "watchdog") {
    return { terminalReason: "stopped" };
  }
  if (outcome.error !== undefined) {
    if (controller?.signal.aborted === true) {
      return { terminalReason: "stopped" };
    }
    console.error(
      `[linear-agent-bridge] session run failed: session=${request.linearSessionId} error=RuntimeExecutionError`,
    );
    const body =
      outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
    try {
      await emitScopedActivity(
        deps,
        activityScope,
        "runtime-error",
        request.linearSessionId,
        { type: "error", body },
      );
    } catch (activityErr) {
      console.error(
        `[linear-agent-bridge] failed to emit error activity: session=${request.linearSessionId} error=${boundedErrorClass(activityErr)}`,
      );
    }
    return { terminalReason: "failed" };
  }
  return {
    terminalReason:
      controller?.signal.aborted === true ? "stopped" : "completed",
    ...(capturedResponse !== undefined ? { response: capturedResponse } : {}),
  };
}

async function handleRuntimeEvent(
  deps: InternalServerDeps,
  request: SessionRequest,
  issueIdentifier: string | undefined,
  event: RuntimeEvent,
  activityScope: ActivityScope,
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
    await emitScopedActivity(
      deps,
      activityScope,
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

async function emitScopedActivity(
  deps: InternalServerDeps,
  scope: ActivityScope,
  activityKey: string,
  agentSessionId: string,
  content: AgentActivityContent,
  options: { ephemeral?: boolean; signal?: AbortSignal } = {},
): Promise<void> {
  if (scope.kind === "ingress") {
    await emitActivity(
      deps,
      scope.executionId,
      activityKey,
      agentSessionId,
      content,
      options,
    );
    return;
  }
  await emitAutonomousGoalActivity(
    deps,
    scope.linearSessionId,
    `${scope.prefix}-${activityKey}`,
    content,
    options,
  );
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

async function emitAutonomousGoalActivity(
  deps: InternalServerDeps,
  linearSessionId: string,
  activityKey: string,
  content: AgentActivityContent,
  options: { ephemeral?: boolean; signal?: AbortSignal } = {},
): Promise<void> {
  options.signal?.throwIfAborted();
  const goal = await deps.bridgeState.getAutonomousGoal(linearSessionId);
  if (goal?.status === "stopped") {
    return;
  }
  options.signal?.throwIfAborted();
  const activityId =
    await deps.bridgeState.getOrCreateAutonomousGoalActivityId(
      linearSessionId,
      activityKey,
    );
  const latest = await deps.bridgeState.getAutonomousGoal(linearSessionId);
  if (latest?.status === "stopped") {
    return;
  }
  const signal =
    options.signal === undefined
      ? deps.shutdownController.signal
      : AbortSignal.any([options.signal, deps.shutdownController.signal]);
  signal.throwIfAborted();
  await deps.linear.createActivity(linearSessionId, content, {
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
  if (error instanceof LinearQueryError) {
    return "LinearQueryError";
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
