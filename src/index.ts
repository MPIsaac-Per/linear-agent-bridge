import { loadConfig } from "./config.js";
import { startServer } from "./server.js";
import { LinearAgentClient } from "./linear/client.js";
import { LinearOAuthTokenManager } from "./linear/oauth.js";
import { JsonSessionStore } from "./sessions/store.js";
import { JsonBridgeStateStore } from "./state/store.js";
import { createIngressRecoveryKeyring } from "./state/recovery-envelope.js";
import { SerialQueue } from "./queue.js";
import { ClaudeRuntime } from "./runtime/claude.js";
import { CodexRuntime } from "./runtime/codex.js";
import {
  awaitReadyOrShutdown,
  installGracefulShutdown,
} from "./shutdown.js";
import type { AgentRuntime } from "./types.js";

function buildRuntime(runtime: "claude" | "codex", kbPath: string): AgentRuntime {
  return runtime === "claude" ? new ClaudeRuntime(kbPath) : new CodexRuntime();
}

const config = loadConfig();
const oauth = new LinearOAuthTokenManager({
  clientId: config.linearClientId,
  clientSecret: config.linearClientSecret,
  initialAccessToken: config.linearAccessToken,
  storePath: config.oauthTokenStorePath,
});
await oauth.load();
const server = startServer({
  config,
  runtime: buildRuntime(config.runtime, config.kbPath),
  linear: new LinearAgentClient(oauth),
  oauth,
  store: new JsonSessionStore(config.sessionStorePath),
  bridgeState: new JsonBridgeStateStore(config.bridgeStateStorePath, {
    recoveryKeyring: createIngressRecoveryKeyring(
      config.ingressRecoveryKey,
      config.ingressRecoveryPreviousKeys,
    ),
  }),
  queue: new SerialQueue(),
});
const shutdown = installGracefulShutdown(server);
if (await awaitReadyOrShutdown(server.ready, shutdown)) {
  console.log(
    `linear-agent-bridge listening on :${config.port} (runtime: ${config.runtime})`,
  );
}
