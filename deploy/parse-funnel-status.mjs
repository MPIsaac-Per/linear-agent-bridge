#!/usr/bin/env node
import { readFileSync } from "node:fs";

const [mode, target, statusPath] = process.argv.slice(2);

function publicRoutes(config) {
  const allowedHosts = Object.entries(config.AllowFunnel ?? {})
    .filter(([_hostPort, allowed]) => allowed === true)
    .map(([hostPort]) => hostPort);
  if (allowedHosts.length === 0) return [];

  const routes = [];
  for (const hostPort of allowedHosts) {
    const authority = new URL(`https://${hostPort}`);
    if (
      authority.protocol !== "https:" ||
      authority.username !== "" ||
      authority.password !== "" ||
      authority.pathname !== "/" ||
      authority.search !== "" ||
      authority.hash !== ""
    ) {
      throw new Error("authority");
    }

    const server = config.Web?.[hostPort];
    const handlers = Object.entries(server?.Handlers ?? {});
    if (handlers.length === 0) throw new Error("unresolved public route");
    for (const [mount, handler] of handlers) {
      if (
        !mount.startsWith("/") ||
        /[\u0000-\u0020\u007f?#]/.test(mount)
      ) {
        throw new Error("mount");
      }
      const prefix = mount === "/" ? "" : mount.replace(/\/$/, "");
      routes.push({
        proxy: handler?.Proxy,
        webhookUrl: `${authority.origin}${prefix}/webhook`,
      });
    }
  }
  return routes;
}

try {
  if (
    (mode !== "preflight" && mode !== "verify") ||
    target === undefined ||
    statusPath === undefined
  ) {
    throw new Error("arguments");
  }
  const config = JSON.parse(readFileSync(statusPath, "utf8"));
  const routes = publicRoutes(config);

  if (mode === "preflight" && routes.length === 0) {
    process.stdout.write("empty");
  } else if (routes.length === 1 && routes[0].proxy === target) {
    process.stdout.write(
      mode === "preflight"
        ? `existing ${routes[0].webhookUrl}`
        : routes[0].webhookUrl,
    );
  } else {
    throw new Error("unsafe public route state");
  }
} catch {
  process.stderr.write(
    "Funnel status is not empty or a unique public route to the requested target\n",
  );
  process.exit(1);
}
