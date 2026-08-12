import { loadConfig } from "./config.js";
import { startServer } from "./server.js";
import { LinearAgentClient } from "./linear/client.js";
import { JsonSessionStore } from "./sessions/store.js";
import { SerialQueue } from "./queue.js";
import { ClaudeRuntime } from "./runtime/claude.js";
import { CodexRuntime } from "./runtime/codex.js";
import type { AgentRuntime } from "./types.js";

function buildRuntime(runtime: "claude" | "codex", kbPath: string): AgentRuntime {
  return runtime === "claude" ? new ClaudeRuntime(kbPath) : new CodexRuntime();
}

const config = loadConfig();
startServer({
  config,
  runtime: buildRuntime(config.runtime, config.kbPath),
  linear: new LinearAgentClient(config.linearAccessToken),
  store: new JsonSessionStore(config.sessionStorePath),
  queue: new SerialQueue(),
});
console.log(`linear-atlas-agent listening on :${config.port} (runtime: ${config.runtime})`);
