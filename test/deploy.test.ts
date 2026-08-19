import { createHmac } from "node:crypto";
import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawn } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";

const repoRoot = path.resolve(import.meta.dirname, "..");
const verifierPath = path.join(repoRoot, "deploy", "verify-ingress.sh");
const funnelParserPath = path.join(repoRoot, "deploy", "parse-funnel-status.mjs");
const secret = "verifier-secret-must-stay-redacted";
const temporaryDirectories: string[] = [];

interface RecordedCurlCall {
  args: string[];
  body: string;
  headers: string[];
  env: Record<string, string>;
}

interface RecordedNodeCall {
  args: string[];
  env: Record<string, string>;
}

interface FakeResolution {
  stdout: string;
  exit: number;
}

const DEFAULT_RESOLUTION: FakeResolution = {
  stdout: "public 203.0.113.10\nsystem 203.0.113.10\n",
  exit: 0,
};

async function makeFakeChildren(options: {
  healthExit?: string;
  authenticationControlExit?: string;
  webhookExit?: string;
  healthStatus?: string;
  authenticationControlStatus?: string;
  webhookStatus?: string;
  resolution?: FakeResolution;
  failingAddresses?: string[];
}): Promise<{
  curlPath: string;
  curlLogPath: string;
  nodePath: string;
  nodeLogPath: string;
}> {
  const binDir = await fs.mkdtemp(path.join(os.tmpdir(), "ingress-curl-"));
  temporaryDirectories.push(binDir);
  const curlLogPath = path.join(binDir, "curl-calls.jsonl");
  const nodeLogPath = path.join(binDir, "node-calls.jsonl");
  const fakeCurl = `#!${process.execPath}
const fs = require("node:fs");
const args = process.argv.slice(2);
const configIndex = args.indexOf("--config");
const headers = configIndex === -1 ? [] : fs.readFileSync(args[configIndex + 1], "utf8")
  .trim().split("\\n").filter(Boolean)
  .map(line => line.match(/^header = "(.*)"$/)[1]);
const dataIndex = args.indexOf("--data-binary");
const body = dataIndex === -1 ? "" : fs.readFileSync(args[dataIndex + 1].slice(1), "utf8");
fs.appendFileSync(${JSON.stringify(curlLogPath)}, JSON.stringify({ args, body, headers, env: process.env }) + "\\n");
const url = args[args.length - 1];
const resolveIndex = args.indexOf("--resolve");
const resolvedAddress = resolveIndex === -1
  ? ""
  : args[resolveIndex + 1].split(":").slice(2).join(":").split("[").join("").split("]").join("");
const failingAddresses = ${JSON.stringify(options.failingAddresses ?? [])};
if (failingAddresses.includes(resolvedAddress)) {
  // Models a family that is unreachable from the public internet: the
  // connection never completes, so curl exits nonzero with no status.
  process.stdout.write("000 0.000");
  process.exit(7);
}
const isHealth = new URL(url).pathname.endsWith("/healthz");
const isSigned = headers.some(header => header.toLowerCase().startsWith("linear-signature:"));
const exitCode = Number(isHealth
  ? ${JSON.stringify(options.healthExit ?? "0")}
  : isSigned
    ? ${JSON.stringify(options.webhookExit ?? "0")}
    : ${JSON.stringify(options.authenticationControlExit ?? "0")});
const status = isHealth
  ? ${JSON.stringify(options.healthStatus ?? "200")}
  : isSigned
    ? ${JSON.stringify(options.webhookStatus ?? "200")}
    : ${JSON.stringify(options.authenticationControlStatus ?? "401")};
process.stdout.write(status + " " + (isHealth ? "0.041" : isSigned ? "0.052" : "0.047"));
process.exit(exitCode);
`;
  const curlPath = path.join(binDir, "curl");
  await fs.writeFile(curlPath, fakeCurl, { mode: 0o755 });
  const nodePath = path.join(binDir, "node");
  const resolutionPath = path.join(binDir, "resolution.json");
  await fs.writeFile(
    resolutionPath,
    JSON.stringify(options.resolution ?? DEFAULT_RESOLUTION),
  );
  const fakeNode = `#!${process.execPath}
const fs = require("node:fs");
const { spawnSync } = require("node:child_process");
const args = process.argv.slice(2);
fs.appendFileSync(${JSON.stringify(nodeLogPath)}, JSON.stringify({ args, env: process.env }) + "\\n");
if (args[0] && args[0].endsWith("resolve-public-addresses.mjs")) {
  const canned = JSON.parse(fs.readFileSync(${JSON.stringify(resolutionPath)}, "utf8"));
  process.stdout.write(canned.stdout);
  process.exit(canned.exit);
}
const result = spawnSync(${JSON.stringify(process.execPath)}, args, { env: process.env, stdio: "inherit" });
process.exit(result.status ?? 1);
`;
  await fs.writeFile(nodePath, fakeNode, { mode: 0o755 });
  return { curlPath, curlLogPath, nodePath, nodeLogPath };
}

async function runVerifier(
  envOverrides: Record<string, string | undefined> = {},
  fakes: { resolution?: FakeResolution; failingAddresses?: string[] } = {},
): Promise<{
  code: number | null;
  stdout: string;
  stderr: string;
  calls: RecordedCurlCall[];
  nodeCalls: RecordedNodeCall[];
}> {
  const children = await makeFakeChildren({
    ...(envOverrides.FAKE_HEALTH_EXIT !== undefined
      ? { healthExit: envOverrides.FAKE_HEALTH_EXIT }
      : {}),
    ...(envOverrides.FAKE_WEBHOOK_EXIT !== undefined
      ? { webhookExit: envOverrides.FAKE_WEBHOOK_EXIT }
      : {}),
    ...(envOverrides.FAKE_HEALTH_STATUS !== undefined
      ? { healthStatus: envOverrides.FAKE_HEALTH_STATUS }
      : {}),
    ...(envOverrides.FAKE_AUTH_CONTROL_EXIT !== undefined
      ? { authenticationControlExit: envOverrides.FAKE_AUTH_CONTROL_EXIT }
      : {}),
    ...(envOverrides.FAKE_AUTH_CONTROL_STATUS !== undefined
      ? { authenticationControlStatus: envOverrides.FAKE_AUTH_CONTROL_STATUS }
      : {}),
    ...(envOverrides.FAKE_WEBHOOK_STATUS !== undefined
      ? { webhookStatus: envOverrides.FAKE_WEBHOOK_STATUS }
      : {}),
    ...(fakes.resolution !== undefined ? { resolution: fakes.resolution } : {}),
    ...(fakes.failingAddresses !== undefined
      ? { failingAddresses: fakes.failingAddresses }
      : {}),
  });
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    WEBHOOK_URL: "https://edge.example/linear/webhook",
    LINEAR_WEBHOOK_SECRET: secret,
    LINEAR_CLIENT_SECRET: "client-secret-must-not-reach-child",
    LINEAR_ACCESS_TOKEN: "access-token-must-not-reach-child",
    OAUTH_ACCESS_TOKEN: "oauth-token-must-not-reach-child",
    UNRELATED_CREDENTIAL: "unrelated-must-not-reach-child",
    VERIFY_INGRESS_CURL_BIN: children.curlPath,
    VERIFY_INGRESS_NODE_BIN: children.nodePath,
    ...envOverrides,
  };
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) delete env[key];
  }

  const startedAt = Date.now();
  const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve) => {
      const child = spawn("/bin/bash", [verifierPath], { cwd: repoRoot, env });
      let stdout = "";
      let stderr = "";
      child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
      child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
      child.on("close", (code) => resolve({ code, stdout, stderr }));
    },
  );
  const finishedAt = Date.now();
  const calls = await fs
    .readFile(children.curlLogPath, "utf8")
    .then((text) => text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)))
    .catch(() => [] as RecordedCurlCall[]);
  const nodeCalls = await fs
    .readFile(children.nodeLogPath, "utf8")
    .then((text) => text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)))
    .catch(() => [] as RecordedNodeCall[]);

  const firstWebhookCall = calls.find(
    (call) => optionValue(call.args, "--request") === "POST",
  );
  if (firstWebhookCall !== undefined) {
    const payload = JSON.parse(firstWebhookCall.body) as { webhookTimestamp: number };
    expect(payload.webhookTimestamp).toBeGreaterThanOrEqual(startedAt);
    expect(payload.webhookTimestamp).toBeLessThanOrEqual(finishedAt);
  }
  return { ...result, calls, nodeCalls };
}

function optionValue(args: string[], option: string): string | undefined {
  const index = args.indexOf(option);
  return index === -1 ? undefined : args[index + 1];
}

function verifierControlledEnv(env: Record<string, string>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(env).filter(
      ([name]) => !name.startsWith("__CF") && name !== "SHLVL",
    ),
  );
}

async function runInstaller(
  envOverrides: Record<string, string | undefined> = {},
): Promise<{
  code: number | null;
  stdout: string;
  stderr: string;
  calls: string;
  tempFiles: string[];
}> {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "bridge-install-"));
  temporaryDirectories.push(directory);
  const copiedRepo = path.join(directory, "repo");
  const deployDirectory = path.join(copiedRepo, "deploy");
  const binDirectory = path.join(directory, "bin");
  const homeDirectory = path.join(directory, "home");
  const tempDirectory = path.join(directory, "tmp");
  const callsPath = path.join(directory, "calls.log");
  const statusCountPath = path.join(directory, "status-count");
  await Promise.all([
    fs.mkdir(deployDirectory, { recursive: true }),
    fs.mkdir(binDirectory),
    fs.mkdir(homeDirectory),
    fs.mkdir(tempDirectory),
  ]);
  await Promise.all([
    fs.copyFile(path.join(repoRoot, "deploy", "install.sh"), path.join(deployDirectory, "install.sh")),
    fs.copyFile(path.join(repoRoot, "deploy", "launchd.plist.template"), path.join(deployDirectory, "launchd.plist.template")),
    fs.copyFile(funnelParserPath, path.join(deployDirectory, "parse-funnel-status.mjs")),
    fs.writeFile(path.join(copiedRepo, ".env"), "PORT=4123\n"),
  ]);

  const fakes: Record<string, string> = {
    npm: "#!/bin/bash\nprintf 'npm %s\\n' \"$*\" >> \"$FAKE_INSTALL_LOG\"\n",
    launchctl: "#!/bin/bash\nprintf 'launchctl %s\\n' \"$*\" >> \"$FAKE_INSTALL_LOG\"\n",
    sleep: "#!/bin/bash\nexit 0\n",
    node: `#!/bin/bash\nif [[ "$*" == *"dist/config.js"* ]]; then\n\tprintf '%s' "\${PORT:-4123}"\n\texit "\${FAKE_CONFIG_EXIT:-0}"\nfi\nexec ${JSON.stringify(process.execPath)} "$@"\n`,
    curl: "#!/bin/bash\nprintf 'curl %s\\n' \"$*\" >> \"$FAKE_INSTALL_LOG\"\nprintf '%s' \"${FAKE_HEALTH_BODY:-ok}\"\nexit \"${FAKE_HEALTH_EXIT:-0}\"\n",
    tailscale: "#!/bin/bash\nprintf 'tailscale %s\\n' \"$*\" >> \"$FAKE_INSTALL_LOG\"\nif [ \"$1 $2 $3\" = 'funnel status --json' ]; then count=0; if [ -f \"$FAKE_TAILSCALE_STATUS_COUNT_FILE\" ]; then count=$(sed -n '1p' \"$FAKE_TAILSCALE_STATUS_COUNT_FILE\"); fi; next=$((count + 1)); printf '%s' \"$next\" > \"$FAKE_TAILSCALE_STATUS_COUNT_FILE\"; if [ \"$count\" -eq 0 ]; then printf '%s' \"$FAKE_TAILSCALE_PREFLIGHT_STATUS\"; exit \"${FAKE_TAILSCALE_PREFLIGHT_STATUS_EXIT:-0}\"; fi; printf '%s' \"$FAKE_TAILSCALE_POST_STATUS\"; exit \"${FAKE_TAILSCALE_POST_STATUS_EXIT:-0}\"; fi\nif [ \"${3:-}\" = 'off' ]; then exit \"${FAKE_TAILSCALE_CLEANUP_EXIT:-0}\"; fi\nif [ \"${2:-}\" = '--bg' ] && [ \"${FAKE_TAILSCALE_INTERRUPT:-0}\" = '1' ]; then kill -TERM \"$PPID\"; exit 143; fi\nexit \"${FAKE_TAILSCALE_EXIT:-0}\"\n",
  };
  await Promise.all(
    Object.entries(fakes).map(([name, contents]) =>
      fs.writeFile(path.join(binDirectory, name), contents, { mode: 0o755 }),
    ),
  );

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: homeDirectory,
    TMPDIR: tempDirectory,
    PATH: `${binDirectory}:${process.env.PATH ?? ""}`,
    TAILSCALE_BIN: path.join(binDirectory, "tailscale"),
    FAKE_INSTALL_LOG: callsPath,
    FAKE_TAILSCALE_STATUS_COUNT_FILE: statusCountPath,
    FAKE_TAILSCALE_PREFLIGHT_STATUS: "{}",
    FAKE_TAILSCALE_POST_STATUS: JSON.stringify({
      Web: {
        "bridge.example.ts.net:443": {
          Handlers: { "/": { Proxy: "http://127.0.0.1:4123" } },
        },
      },
      AllowFunnel: { "bridge.example.ts.net:443": true },
    }),
    ...envOverrides,
  };
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) delete env[key];
  }
  const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve) => {
      const child = spawn("/bin/bash", [path.join(deployDirectory, "install.sh")], {
        cwd: copiedRepo,
        env,
      });
      let stdout = "";
      let stderr = "";
      child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
      child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
      child.on("close", (code) => resolve({ code, stdout, stderr }));
    },
  );
  return {
    ...result,
    calls: await fs.readFile(callsPath, "utf8").catch(() => ""),
    tempFiles: await fs.readdir(tempDirectory),
  };
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      fs.rm(directory, { recursive: true, force: true }),
    ),
  );
});

describe("deploy/verify-ingress.sh", () => {
  it("requires an unsigned 401 control before accepting the exact signed harmless webhook", async () => {
    const result = await runVerifier();

    expect(result.code).toBe(0);
    expect(result.calls).toHaveLength(3);
    const [health, authenticationControl, webhook] = result.calls;
    expect(optionValue(health.args, "--request")).toBe("GET");
    expect(health.args.at(-1)).toBe("https://edge.example/linear/healthz");
    expect(optionValue(authenticationControl.args, "--request")).toBe("POST");
    expect(authenticationControl.args.at(-1)).toBe(
      "https://edge.example/linear/webhook",
    );
    expect(authenticationControl.headers).toEqual(["content-type: application/json"]);
    expect(optionValue(webhook.args, "--request")).toBe("POST");
    expect(webhook.args.at(-1)).toBe("https://edge.example/linear/webhook");
    expect(authenticationControl.body).toBe(webhook.body);
    expect(JSON.parse(webhook.body)).toEqual({
      type: "IngressVerificationEvent",
      action: "verify",
      webhookTimestamp: expect.any(Number),
    });

    const signatureHeader = webhook.headers[0];
    expect(signatureHeader).toBe(
      `linear-signature: ${createHmac("sha256", secret).update(webhook.body).digest("hex")}`,
    );
    for (const call of result.calls) {
      expect(optionValue(call.args, "--connect-timeout")).toBe("5");
      expect(optionValue(call.args, "--max-time")).toBe("15");
      expect(optionValue(call.args, "--output")).toBe("/dev/null");
      expect(call.args).not.toContain("--location");
      expect(call.args[0]).toBe("-q");
      expect(verifierControlledEnv(call.env)).toEqual({});
    }
    expect(result.nodeCalls).toHaveLength(4);
    expect(verifierControlledEnv(result.nodeCalls[0].env)).toEqual({
      WEBHOOK_URL: "https://edge.example/linear/webhook",
    });
    // The resolver child receives the host and the resolver list, and nothing
    // else: no secret, no URL, no inherited environment.
    expect(verifierControlledEnv(result.nodeCalls[1].env)).toEqual({
      VERIFY_HOST: "edge.example",
      VERIFY_RESOLVERS: "1.1.1.1,8.8.8.8",
    });
    expect(verifierControlledEnv(result.nodeCalls[2].env)).toEqual({});
    expect(verifierControlledEnv(result.nodeCalls[3].env)).toEqual({});
    expect(result.stdout).toContain(
      "healthz url=https://edge.example/linear/healthz address=203.0.113.10 http_status=200 elapsed_seconds=0.041",
    );
    expect(result.stdout).toContain(
      "authentication_control url=https://edge.example/linear/webhook address=203.0.113.10 http_status=401 elapsed_seconds=0.047",
    );
    expect(result.stdout).toContain(
      "webhook url=https://edge.example/linear/webhook address=203.0.113.10 http_status=200 elapsed_seconds=0.052",
    );
    expect(result.stdout + result.stderr).not.toContain(secret);
    expect(result.stdout + result.stderr).not.toContain(webhook.body);
    expect(result.stdout + result.stderr).not.toContain(signatureHeader ?? "missing-signature");
    for (const call of [authenticationControl, webhook]) {
      expect(call.args.join(" ")).not.toContain(secret);
      expect(call.args.join(" ")).not.toContain(call.body);
      expect(call.args.join(" ")).not.toContain(signatureHeader ?? "missing-signature");
    }
    for (const call of result.nodeCalls) {
      expect(call.args.join(" ")).not.toContain(secret);
      expect(call.args.join(" ")).not.toContain(webhook.body);
      expect(call.args.join(" ")).not.toContain(signatureHeader ?? "missing-signature");
      expect(Object.values(call.env).join(" ")).not.toContain(secret);
      expect(Object.values(call.env).join(" ")).not.toContain(webhook.body);
      expect(Object.values(call.env).join(" ")).not.toContain(
        signatureHeader ?? "missing-signature",
      );
    }
  });

  it("rejects an ingress that accepts the unsigned authentication control", async () => {
    const result = await runVerifier({ FAKE_AUTH_CONTROL_STATUS: "200" });

    expect(result.code).not.toBe(0);
    expect(result.calls).toHaveLength(2);
    expect(result.calls[1].headers).toEqual(["content-type: application/json"]);
    expect(result.stdout).toContain(
      "authentication_control url=https://edge.example/linear/webhook address=203.0.113.10 http_status=200 elapsed_seconds=0.047",
    );
    expect(result.stdout + result.stderr).not.toContain(secret);
    expect(result.stdout + result.stderr).not.toContain(result.calls[1].body);
  });

  it.each([
    "http://edge.example/webhook",
    "https://user:password@edge.example/webhook",
    "https://edge.example/webhook?token=secret",
    "https://edge.example/webhook?",
    "https://edge.example/webhook#fragment",
    "https://edge.example/webhook#",
    "https://edge.example/not-webhook",
    "https://edge.example/webhook/",
    "https://edge.example/webhook-extra",
    "https://edge.example/foo//webhook",
    " https://edge.example/webhook",
    "https://edge.example/webhook ",
    "https://edge.example/webhook\n",
  ])("rejects an unsafe or non-canonical webhook URL: %s", async (webhookUrl) => {
    const result = await runVerifier({ WEBHOOK_URL: webhookUrl });
    expect(result.code).not.toBe(0);
    expect(result.calls).toHaveLength(0);
    expect(result.stdout + result.stderr).not.toContain("password");
    expect(result.stdout + result.stderr).not.toContain("token=secret");
  });

  it("requires both environment variables", async () => {
    expect((await runVerifier({ WEBHOOK_URL: undefined })).code).not.toBe(0);
    expect((await runVerifier({ LINEAR_WEBHOOK_SECRET: undefined })).code).not.toBe(0);
  });

  it.each([
    ["DNS", { FAKE_HEALTH_EXIT: "6" }],
    ["TLS", { FAKE_HEALTH_EXIT: "35" }],
    ["connection", { FAKE_HEALTH_EXIT: "7" }],
    ["authentication control timeout", { FAKE_AUTH_CONTROL_EXIT: "28" }],
    ["authentication control HTTP", { FAKE_AUTH_CONTROL_STATUS: "403" }],
    ["timeout", { FAKE_WEBHOOK_EXIT: "28" }],
    ["HTTP", { FAKE_WEBHOOK_STATUS: "401" }],
  ])("returns nonzero for %s failures without leaking request material", async (_kind, env) => {
    const result = await runVerifier(env);
    expect(result.code).not.toBe(0);
    expect(result.stdout + result.stderr).not.toContain(secret);
    for (const call of result.calls) {
      if (call.body !== "") {
        expect(result.stdout + result.stderr).not.toContain(call.body);
      }
      const signature = call.headers[0];
      if (signature !== undefined) {
        expect(result.stdout + result.stderr).not.toContain(signature);
      }
    }
  });

  it("reports the split between system and public resolution as the headline", async () => {
    const result = await runVerifier({}, {
      resolution: {
        // What a tailnet-joined host sees: the overlay address locally, the
        // real ingress address from outside.
        stdout: "public 203.0.113.10\nsystem 100.64.0.1\n",
        exit: 0,
      },
    });

    expect(result.code).toBe(0);
    expect(result.stdout).toContain("split_horizon_dns host=edge.example");
    expect(result.stdout).toContain("system_resolver=100.64.0.1");
    expect(result.stdout).toContain("public_resolver=203.0.113.10");
    for (const call of result.calls) {
      expect(optionValue(call.args, "--resolve")).toBe(
        "edge.example:443:203.0.113.10",
      );
    }
  });

  it("fails from inside the overlay network when only the public path is broken", async () => {
    const result = await runVerifier({}, {
      resolution: {
        stdout: "public 203.0.113.10\nsystem 100.64.0.1\n",
        exit: 0,
      },
      // The private path would answer every probe. Only the public one is down,
      // which is the case the verifier previously reported as healthy.
      failingAddresses: ["203.0.113.10"],
    });

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain("the public path failed at: 203.0.113.10");
    expect(result.stdout).toContain("split_horizon_dns host=edge.example");
    expect(result.stdout + result.stderr).not.toContain(secret);
  });

  it("fails when one address family is unreachable and the other is not", async () => {
    const result = await runVerifier({}, {
      resolution: {
        stdout:
          "public 203.0.113.10\npublic 2001:db8::1\nsystem 203.0.113.10\nsystem 2001:db8::1\n",
        exit: 0,
      },
      failingAddresses: ["2001:db8::1"],
    });

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain("the public path failed at: 2001:db8::1");
    // The working family still reports, so a reader sees the whole picture.
    expect(result.stdout).toContain("address=203.0.113.10 http_status=200");
    expect(result.stdout).toContain("address=2001:db8::1 http_status=000");
    // curl needs the IPv6 literal bracketed in --resolve.
    const sixCall = result.calls.find((call) =>
      (optionValue(call.args, "--resolve") ?? "").includes("2001:db8::1"),
    );
    expect(optionValue(sixCall?.args ?? [], "--resolve")).toBe(
      "edge.example:443:[2001:db8::1]",
    );
  });

  it("refuses to report success when no configured resolver can be reached", async () => {
    const result = await runVerifier({}, {
      resolution: {
        stdout: "resolver_failed 1.1.1.1 ETIMEOUT\nresolver_failed 8.8.8.8 ECONNREFUSED\n",
        exit: 3,
      },
    });

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain("the public path was NOT tested");
    expect(result.stdout).toContain("resolver_unreachable 1.1.1.1 ETIMEOUT");
    expect(result.stdout).toContain("resolver_unreachable 8.8.8.8 ECONNREFUSED");
    // Falling back to the system resolver here would prove nothing, so nothing
    // is probed at all.
    expect(result.calls).toHaveLength(0);
  });

  it("honours VERIFY_INGRESS_RESOLVERS and keeps the secret away from the resolver", async () => {
    const result = await runVerifier({
      VERIFY_INGRESS_RESOLVERS: "9.9.9.9,149.112.112.112",
    });

    expect(result.code).toBe(0);
    const resolverCall = result.nodeCalls[1];
    expect(verifierControlledEnv(resolverCall.env)).toEqual({
      VERIFY_HOST: "edge.example",
      VERIFY_RESOLVERS: "9.9.9.9,149.112.112.112",
    });
    expect(Object.values(resolverCall.env).join(" ")).not.toContain(secret);
  });
});

describe("deploy/parse-funnel-status.mjs", () => {
  async function parseStatus(
    status: unknown,
    target = "http://127.0.0.1:3979",
    mode = "verify",
  ) {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), "funnel-status-"));
    temporaryDirectories.push(directory);
    const statusPath = path.join(directory, "status.json");
    await fs.writeFile(statusPath, JSON.stringify(status));
    return await new Promise<{ code: number | null; stdout: string; stderr: string }>(
      (resolve) => {
        const child = spawn(process.execPath, [funnelParserPath, mode, target, statusPath]);
        let stdout = "";
        let stderr = "";
        child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
        child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
        child.on("close", (code) => resolve({ code, stdout, stderr }));
      },
    );
  }

  it("returns the sole public route whose proxy exactly matches the loopback target", async () => {
    const result = await parseStatus({
      Web: {
        "bridge.example.ts.net:443": {
          Handlers: { "/": { Proxy: "http://127.0.0.1:3979" } },
        },
      },
      AllowFunnel: {
        "bridge.example.ts.net:443": true,
      },
    });
    expect(result).toEqual({
      code: 0,
      stdout: "https://bridge.example.ts.net/webhook",
      stderr: "",
    });
  });

  it("classifies empty and exact-existing public Funnel state before mutation", async () => {
    expect(await parseStatus({}, undefined, "preflight")).toEqual({
      code: 0,
      stdout: "empty",
      stderr: "",
    });
    const existing = await parseStatus(
      {
        Web: {
          "bridge.example.ts.net:443": {
            Handlers: { "/": { Proxy: "http://127.0.0.1:3979" } },
          },
        },
        AllowFunnel: { "bridge.example.ts.net:443": true },
      },
      undefined,
      "preflight",
    );
    expect(existing).toEqual({
      code: 0,
      stdout: "existing https://bridge.example.ts.net/webhook",
      stderr: "",
    });
  });

  it("rejects unrelated or ambiguous public routes during preflight", async () => {
    const unrelated = {
      Web: {
        "other.example.ts.net:443": {
          Handlers: { "/": { Proxy: "http://127.0.0.1:9999" } },
        },
      },
      AllowFunnel: { "other.example.ts.net:443": true },
    };
    expect((await parseStatus(unrelated, undefined, "preflight")).code).not.toBe(0);
    expect(
      (
        await parseStatus(
          { AllowFunnel: { "unknown.example.ts.net:443": true } },
          undefined,
          "preflight",
        )
      ).code,
    ).not.toBe(0);
  });

  it.each([
    ["not public", { Web: { "bridge.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:3979" } } } } }],
    ["wrong target", { Web: { "bridge.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:9999" } } } }, AllowFunnel: { "bridge.example.ts.net:443": true } }],
    ["ambiguous", { Web: { "one.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:3979" } } }, "two.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:3979" } } } }, AllowFunnel: { "one.example.ts.net:443": true, "two.example.ts.net:443": true } }],
  ])("fails closed when the matching route is %s", async (_label, status) => {
    expect((await parseStatus(status)).code).not.toBe(0);
  });

});

describe("deploy/install.sh", () => {
  const exactStatus = {
    Web: {
      "bridge.example.ts.net:443": {
        Handlers: { "/": { Proxy: "http://127.0.0.1:4123" } },
      },
    },
    AllowFunnel: { "bridge.example.ts.net:443": true },
  };

  it("fails when the explicit loopback health check fails", async () => {
    const result = await runInstaller({ FAKE_HEALTH_EXIT: "7" });
    expect(result.code).not.toBe(0);
    expect(result.calls).toContain(
      "curl -q -fsS --connect-timeout 1 --max-time 1 http://127.0.0.1:4123/healthz",
    );
    expect(result.calls).not.toContain("tailscale funnel");
  });

  it("completes an explicit local-only install without invoking Tailscale", async () => {
    const result = await runInstaller({ SKIP_FUNNEL: "1" });

    expect(result.code).toBe(0);
    expect(result.calls).toContain(
      "curl -q -fsS --connect-timeout 1 --max-time 1 http://127.0.0.1:4123/healthz",
    );
    expect(result.calls).not.toContain("tailscale");
    expect(result.stdout).toContain(
      "SKIP_FUNNEL=1: service installed without public ingress",
    );
  });

  it("enables direct Funnel only from empty state and verifies the exact route", async () => {
    const result = await runInstaller();
    expect(result.code).toBe(0);
    expect(result.calls).toContain(
      "tailscale funnel --bg http://127.0.0.1:4123",
    );
    expect(result.calls).toContain("tailscale funnel status --json");
    expect(result.calls).not.toContain("tailscale funnel --https=443 off");
    expect(result.stdout).toContain(
      "Webhook URL: https://bridge.example.ts.net/webhook",
    );
    expect(result.tempFiles).toEqual([]);
  });

  it("treats an exact existing target route as idempotent without mutation", async () => {
    const result = await runInstaller({
      FAKE_TAILSCALE_PREFLIGHT_STATUS: JSON.stringify(exactStatus),
    });
    expect(result.code).toBe(0);
    expect(result.calls).not.toContain("tailscale funnel --bg");
    expect(result.calls).not.toContain("tailscale funnel --https=443 off");
    expect(result.stdout).toContain(
      "Webhook URL: https://bridge.example.ts.net/webhook",
    );
  });

  it("refuses unrelated or ambiguous preexisting public routes without mutation", async () => {
    const unrelated = {
      Web: {
        "other.example.ts.net:443": {
          Handlers: { "/": { Proxy: "http://127.0.0.1:9999" } },
        },
      },
      AllowFunnel: { "other.example.ts.net:443": true },
    };
    const ambiguous = {
      Web: {
        "one.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:4123" } } },
        "two.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:4123" } } },
      },
      AllowFunnel: { "one.example.ts.net:443": true, "two.example.ts.net:443": true },
    };
    for (const status of [unrelated, ambiguous]) {
      const result = await runInstaller({
        FAKE_TAILSCALE_PREFLIGHT_STATUS: JSON.stringify(status),
      });
      expect(result.code).not.toBe(0);
      expect(result.calls).not.toContain("tailscale funnel --bg");
      expect(result.calls).not.toContain("tailscale funnel --https=443 off");
      expect(result.tempFiles).toEqual([]);
    }
  });

  it("cleans up installer-created Funnel after post-enable verification failures", async () => {
    const ambiguousPostStatus = {
      Web: {
        "one.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:4123" } } },
        "two.example.ts.net:443": { Handlers: { "/": { Proxy: "http://127.0.0.1:4123" } } },
      },
      AllowFunnel: { "one.example.ts.net:443": true, "two.example.ts.net:443": true },
    };
    const parseFailure = await runInstaller({
      FAKE_TAILSCALE_POST_STATUS: JSON.stringify(ambiguousPostStatus),
    });
    expect(parseFailure.code).not.toBe(0);
    expect(parseFailure.calls).toContain("tailscale funnel --bg");
    expect(parseFailure.calls).toContain("tailscale funnel --https=443 off");
    expect(parseFailure.tempFiles).toEqual([]);

    const statusFailure = await runInstaller({
      FAKE_TAILSCALE_POST_STATUS_EXIT: "1",
    });
    expect(statusFailure.code).not.toBe(0);
    expect(statusFailure.calls).toContain("tailscale funnel --bg");
    expect(statusFailure.calls).toContain("tailscale funnel --https=443 off");
    expect(statusFailure.tempFiles).toEqual([]);

    const cleanupFailure = await runInstaller({
      FAKE_TAILSCALE_POST_STATUS: JSON.stringify(ambiguousPostStatus),
      FAKE_TAILSCALE_CLEANUP_EXIT: "1",
    });
    expect(cleanupFailure.code).not.toBe(0);
    expect(cleanupFailure.stderr).toContain("Funnel cleanup failed; run:");
    expect(cleanupFailure.stderr).toContain("funnel --https=443 off");
  });

  it("has cleanup armed when enable is interrupted", async () => {
    const result = await runInstaller({ FAKE_TAILSCALE_INTERRUPT: "1" });
    expect(result.code).not.toBe(0);
    expect(result.calls).toContain(
      "tailscale funnel --bg http://127.0.0.1:4123",
    );
    expect(result.calls).toContain("tailscale funnel --https=443 off");
  });

  it("fails before mutation for unavailable Tailscale or preflight status failure", async () => {
    expect(
      (await runInstaller({ TAILSCALE_BIN: "/nonexistent/tailscale" })).code,
    ).not.toBe(0);
    const preflightFailure = await runInstaller({
      FAKE_TAILSCALE_PREFLIGHT_STATUS_EXIT: "1",
    });
    expect(preflightFailure.code).not.toBe(0);
    expect(preflightFailure.calls).not.toContain("tailscale funnel --bg");
    expect(preflightFailure.calls).not.toContain("tailscale funnel --https=443 off");
  });
});
