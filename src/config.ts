import { parseCanonicalRecoveryKey } from "./state/recovery-envelope.js";

export interface Config {
  linearClientId: string;
  linearClientSecret: string;
  linearWebhookSecret: string;
  linearAccessToken: string;
  port: number;
  runtime: "claude" | "codex";
  kbPath: string;
  sessionStorePath: string;
  bridgeStateStorePath: string;
  oauthTokenStorePath: string;
  runInactivityTimeoutMs: number;
  ingressRecoveryKey: string;
  ingressRecoveryPreviousKeys: string[];
  reconcileIntervalMs: number;
  reconcileLookbackMs: number;
  reconcileMaxSessions: number;
  agentSessionAckGraceMs: number;
}

const DEFAULT_PORT = "3979";
const DEFAULT_RUNTIME = "claude";
const DEFAULT_SESSION_STORE_PATH = "./data/sessions.json";
const DEFAULT_BRIDGE_STATE_STORE_PATH = "./data/bridge-state.json";
const DEFAULT_OAUTH_TOKEN_STORE_PATH = "./data/oauth-tokens.json";
const DEFAULT_RUN_INACTIVITY_TIMEOUT_MS = "300000";
const DEFAULT_RECONCILE_INTERVAL_MS = "60000";
const DEFAULT_RECONCILE_LOOKBACK_MS = "86400000";
const DEFAULT_RECONCILE_MAX_SESSIONS = "250";
const DEFAULT_AGENT_SESSION_ACK_GRACE_MS = "120000";
let warnedAboutDeprecatedRunTimeout = false;

function requireEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key];
  if (value === undefined || value === "") {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

function positiveInteger(value: string, key: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${key} "${value}": expected a positive integer`);
  }
  return parsed;
}

function parseRecoveryKeys(
  primary: string,
  previousRaw: string | undefined,
): string[] {
  if (parseCanonicalRecoveryKey(primary) === undefined) {
    throw new Error(
      "Invalid INGRESS_RECOVERY_KEY: expected canonical 32-byte base64url",
    );
  }
  const previous =
    previousRaw === undefined || previousRaw === ""
      ? []
      : previousRaw.split(",");
  if (
    previous.length > 4 ||
    previous.some((value) => parseCanonicalRecoveryKey(value) === undefined) ||
    new Set([primary, ...previous]).size !== previous.length + 1
  ) {
    throw new Error(
      "Invalid INGRESS_RECOVERY_PREVIOUS_KEYS: expected up to four unique canonical 32-byte base64url keys",
    );
  }
  return previous;
}

function integerInRange(
  value: string,
  key: string,
  minimum: number,
  maximum: number,
): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `Invalid ${key} "${value}": expected an integer from ${minimum} to ${maximum}`,
    );
  }
  return parsed;
}

/**
 * Load and validate config from process.env (see .env.example).
 * Missing required values fail fast, naming the first missing variable
 * in declared order: LINEAR_CLIENT_ID, LINEAR_CLIENT_SECRET,
 * LINEAR_WEBHOOK_SECRET, LINEAR_ACCESS_TOKEN, INGRESS_RECOVERY_KEY.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const linearClientId = requireEnv(env, "LINEAR_CLIENT_ID");
  const linearClientSecret = requireEnv(env, "LINEAR_CLIENT_SECRET");
  const linearWebhookSecret = requireEnv(env, "LINEAR_WEBHOOK_SECRET");
  const linearAccessToken = requireEnv(env, "LINEAR_ACCESS_TOKEN");
  const ingressRecoveryKey = requireEnv(env, "INGRESS_RECOVERY_KEY");
  const ingressRecoveryPreviousKeys = parseRecoveryKeys(
    ingressRecoveryKey,
    env.INGRESS_RECOVERY_PREVIOUS_KEYS,
  );

  const runtimeRaw = env.RUNTIME ?? DEFAULT_RUNTIME;
  if (runtimeRaw !== "claude" && runtimeRaw !== "codex") {
    throw new Error(`Invalid RUNTIME "${runtimeRaw}": expected "claude" or "codex"`);
  }

  const portRaw = env.PORT ?? DEFAULT_PORT;
  const port = positiveInteger(portRaw, "PORT");
  let inactivityTimeoutRaw = env.RUN_INACTIVITY_TIMEOUT_MS;
  let inactivityTimeoutKey = "RUN_INACTIVITY_TIMEOUT_MS";
  if (env.RUN_TIMEOUT_MS !== undefined && !warnedAboutDeprecatedRunTimeout) {
    warnedAboutDeprecatedRunTimeout = true;
    console.warn(
      "[linear-agent-bridge] RUN_TIMEOUT_MS is deprecated; use RUN_INACTIVITY_TIMEOUT_MS instead.",
    );
  }
  if (inactivityTimeoutRaw === undefined && env.RUN_TIMEOUT_MS !== undefined) {
    inactivityTimeoutRaw = env.RUN_TIMEOUT_MS;
    inactivityTimeoutKey = "RUN_TIMEOUT_MS";
  }
  const runInactivityTimeoutMs = positiveInteger(
    inactivityTimeoutRaw ?? DEFAULT_RUN_INACTIVITY_TIMEOUT_MS,
    inactivityTimeoutKey,
  );

  return {
    linearClientId,
    linearClientSecret,
    linearWebhookSecret,
    linearAccessToken,
    port,
    runtime: runtimeRaw,
    // The directory agent sessions run in — its CLAUDE.md stack and any
    // project-scope MCP config load automatically. Defaults to the
    // service's own working directory; point it at your knowledge base.
    kbPath: env.KB_PATH ?? process.cwd(),
    sessionStorePath: env.SESSION_STORE_PATH ?? DEFAULT_SESSION_STORE_PATH,
    bridgeStateStorePath:
      env.BRIDGE_STATE_STORE_PATH ?? DEFAULT_BRIDGE_STATE_STORE_PATH,
    oauthTokenStorePath:
      env.OAUTH_TOKEN_STORE_PATH ?? DEFAULT_OAUTH_TOKEN_STORE_PATH,
    runInactivityTimeoutMs,
    ingressRecoveryKey,
    ingressRecoveryPreviousKeys,
    reconcileIntervalMs: positiveInteger(
      env.RECONCILE_INTERVAL_MS ?? DEFAULT_RECONCILE_INTERVAL_MS,
      "RECONCILE_INTERVAL_MS",
    ),
    reconcileLookbackMs: positiveInteger(
      env.RECONCILE_LOOKBACK_MS ?? DEFAULT_RECONCILE_LOOKBACK_MS,
      "RECONCILE_LOOKBACK_MS",
    ),
    reconcileMaxSessions: integerInRange(
      env.RECONCILE_MAX_SESSIONS ?? DEFAULT_RECONCILE_MAX_SESSIONS,
      "RECONCILE_MAX_SESSIONS",
      1,
      250,
    ),
    agentSessionAckGraceMs: positiveInteger(
      env.AGENT_SESSION_ACK_GRACE_MS ?? DEFAULT_AGENT_SESSION_ACK_GRACE_MS,
      "AGENT_SESSION_ACK_GRACE_MS",
    ),
  };
}
