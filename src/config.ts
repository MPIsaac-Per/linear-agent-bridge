export interface Config {
  linearClientId: string;
  linearClientSecret: string;
  linearWebhookSecret: string;
  linearAccessToken: string;
  port: number;
  runtime: "claude" | "codex";
  kbPath: string;
  sessionStorePath: string;
  oauthTokenStorePath: string;
}

const DEFAULT_PORT = "3979";
const DEFAULT_RUNTIME = "claude";
const DEFAULT_SESSION_STORE_PATH = "./data/sessions.json";
const DEFAULT_OAUTH_TOKEN_STORE_PATH = "./data/oauth-tokens.json";

function requireEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key];
  if (value === undefined || value === "") {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

/**
 * Load and validate config from process.env (see .env.example).
 * Missing required values fail fast, naming the first missing variable
 * in declared order: LINEAR_CLIENT_ID, LINEAR_CLIENT_SECRET,
 * LINEAR_WEBHOOK_SECRET, LINEAR_ACCESS_TOKEN.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const linearClientId = requireEnv(env, "LINEAR_CLIENT_ID");
  const linearClientSecret = requireEnv(env, "LINEAR_CLIENT_SECRET");
  const linearWebhookSecret = requireEnv(env, "LINEAR_WEBHOOK_SECRET");
  const linearAccessToken = requireEnv(env, "LINEAR_ACCESS_TOKEN");

  const runtimeRaw = env.RUNTIME ?? DEFAULT_RUNTIME;
  if (runtimeRaw !== "claude" && runtimeRaw !== "codex") {
    throw new Error(`Invalid RUNTIME "${runtimeRaw}": expected "claude" or "codex"`);
  }

  const portRaw = env.PORT ?? DEFAULT_PORT;
  const port = Number(portRaw);
  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`Invalid PORT "${portRaw}": expected a positive integer`);
  }

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
    oauthTokenStorePath:
      env.OAUTH_TOKEN_STORE_PATH ?? DEFAULT_OAUTH_TOKEN_STORE_PATH,
  };
}
