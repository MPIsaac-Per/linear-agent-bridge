import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";

const validEnv = {
  LINEAR_CLIENT_ID: "client-id",
  LINEAR_CLIENT_SECRET: "client-secret",
  LINEAR_WEBHOOK_SECRET: "webhook-secret",
  LINEAR_ACCESS_TOKEN: "access-token",
  INGRESS_RECOVERY_KEY: "A".repeat(43),
};
const recoveryKeys = Array.from({ length: 6 }, (_, index) =>
  Buffer.alloc(32, index + 1).toString("base64url"),
);

describe("loadConfig", () => {
  it("loads all required fields plus defaults when only required vars are set", () => {
    const config = loadConfig({ ...validEnv });

    expect(config).toEqual({
      linearClientId: "client-id",
      linearClientSecret: "client-secret",
      linearWebhookSecret: "webhook-secret",
      linearAccessToken: "access-token",
      port: 3979,
      runtime: "claude",
      kbPath: process.cwd(),
      sessionStorePath: "./data/sessions.json",
      bridgeStateStorePath: "./data/bridge-state.json",
      oauthTokenStorePath: "./data/oauth-tokens.json",
      runInactivityTimeoutMs: 300000,
      ingressRecoveryKey: "A".repeat(43),
      ingressRecoveryPreviousKeys: [],
      reconcileIntervalMs: 60000,
      reconcileLookbackMs: 86400000,
      reconcileMaxSessions: 250,
      agentSessionAckGraceMs: 120000,
    });
  });

  it("honors path, port, and runtime overrides", () => {
    const config = loadConfig({
      ...validEnv,
      PORT: "8080",
      RUNTIME: "codex",
      KB_PATH: "/tmp/kb",
      SESSION_STORE_PATH: "/tmp/sessions.json",
      BRIDGE_STATE_STORE_PATH: "/tmp/bridge-state.json",
      OAUTH_TOKEN_STORE_PATH: "/tmp/oauth-tokens.json",
      RUN_INACTIVITY_TIMEOUT_MS: "45000",
      RECONCILE_INTERVAL_MS: "31000",
      RECONCILE_LOOKBACK_MS: "7200000",
      RECONCILE_MAX_SESSIONS: "125",
      AGENT_SESSION_ACK_GRACE_MS: "90000",
    });

    expect(config.port).toBe(8080);
    expect(config.runtime).toBe("codex");
    expect(config.kbPath).toBe("/tmp/kb");
    expect(config.sessionStorePath).toBe("/tmp/sessions.json");
    expect(config.bridgeStateStorePath).toBe("/tmp/bridge-state.json");
    expect(config.oauthTokenStorePath).toBe("/tmp/oauth-tokens.json");
    expect(config.runInactivityTimeoutMs).toBe(45000);
    expect(config.reconcileIntervalMs).toBe(31000);
    expect(config.reconcileLookbackMs).toBe(7200000);
    expect(config.reconcileMaxSessions).toBe(125);
    expect(config.agentSessionAckGraceMs).toBe(90000);
  });

  it.each([
    ["LINEAR_CLIENT_ID"],
    ["LINEAR_CLIENT_SECRET"],
    ["LINEAR_WEBHOOK_SECRET"],
    ["LINEAR_ACCESS_TOKEN"],
    ["INGRESS_RECOVERY_KEY"],
  ])("throws naming %s when missing", (missingKey) => {
    const env = { ...validEnv };
    delete (env as Record<string, string | undefined>)[missingKey];

    expect(() => loadConfig(env)).toThrow(missingKey);
  });

  it("names the first missing var when multiple are missing, in declared order", () => {
    expect(() => loadConfig({})).toThrow("LINEAR_CLIENT_ID");
  });

  it("treats an empty string as missing, not present", () => {
    expect(() => loadConfig({ ...validEnv, LINEAR_CLIENT_ID: "" })).toThrow(
      "LINEAR_CLIENT_ID",
    );
  });

  it("throws for an invalid RUNTIME value", () => {
    expect(() => loadConfig({ ...validEnv, RUNTIME: "gpt" })).toThrow(/RUNTIME/);
  });

  it.each(["not-a-number", "0", "-1", "3.5"])(
    "throws for an invalid PORT value %s",
    (badPort) => {
      expect(() => loadConfig({ ...validEnv, PORT: badPort })).toThrow(/PORT/);
    },
  );

  it.each([
    "RECONCILE_INTERVAL_MS",
    "RECONCILE_LOOKBACK_MS",
    "RECONCILE_MAX_SESSIONS",
    "AGENT_SESSION_ACK_GRACE_MS",
  ])("throws for an invalid %s value", (key) => {
    expect(() => loadConfig({ ...validEnv, [key]: "0" })).toThrow(key);
  });

  it("rejects a reconciliation session cap above Linear's hard scan limit", () => {
    expect(() =>
      loadConfig({ ...validEnv, RECONCILE_MAX_SESSIONS: "251" }),
    ).toThrow(/RECONCILE_MAX_SESSIONS/);
    expect(
      loadConfig({ ...validEnv, RECONCILE_MAX_SESSIONS: "250" })
        .reconcileMaxSessions,
    ).toBe(250);
  });

  it.each(["not-a-number", "0", "-1", "3.5"])(
    "throws for an invalid RUN_INACTIVITY_TIMEOUT_MS value %s",
    (badTimeout) => {
      expect(() =>
        loadConfig({ ...validEnv, RUN_INACTIVITY_TIMEOUT_MS: badTimeout }),
      ).toThrow(/RUN_INACTIVITY_TIMEOUT_MS/);
    },
  );

  it("uses the deprecated fallback, gives the new variable precedence, and warns once whenever the old variable is present", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      expect(
        loadConfig({
          ...validEnv,
          RUN_INACTIVITY_TIMEOUT_MS: "43000",
          RUN_TIMEOUT_MS: "not-a-number",
        }).runInactivityTimeoutMs,
      ).toBe(43000);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(
        loadConfig({ ...validEnv, RUN_TIMEOUT_MS: "41000" })
          .runInactivityTimeoutMs,
      ).toBe(41000);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledWith(
        "[linear-agent-bridge] RUN_TIMEOUT_MS is deprecated; use RUN_INACTIVITY_TIMEOUT_MS instead.",
      );
    } finally {
      warn.mockRestore();
    }
  });

  it("parses a valid PORT string to a number", () => {
    const config = loadConfig({ ...validEnv, PORT: "5000" });
    expect(config.port).toBe(5000);
    expect(typeof config.port).toBe("number");
  });

  it("parses retained recovery keys in order", () => {
    const config = loadConfig({
      ...validEnv,
      INGRESS_RECOVERY_PREVIOUS_KEYS: recoveryKeys.slice(0, 2).join(","),
    });

    expect(config.ingressRecoveryPreviousKeys).toEqual(
      recoveryKeys.slice(0, 2),
    );
  });

  it.each(["short", `${"A".repeat(43)}=`, "!".repeat(43)])(
    "rejects a noncanonical primary recovery key without echoing it",
    (invalidKey) => {
      expect(() =>
        loadConfig({ ...validEnv, INGRESS_RECOVERY_KEY: invalidKey }),
      ).toThrow(
        "Invalid INGRESS_RECOVERY_KEY: expected canonical 32-byte base64url",
      );
    },
  );

  it.each([
    `${recoveryKeys[0]},`,
    `,${recoveryKeys[0]}`,
    `${recoveryKeys[0]},not-canonical`,
  ])("rejects malformed or empty retained recovery key members", (value) => {
    expect(() =>
      loadConfig({ ...validEnv, INGRESS_RECOVERY_PREVIOUS_KEYS: value }),
    ).toThrow("Invalid INGRESS_RECOVERY_PREVIOUS_KEYS");
  });

  it("rejects duplicate current and retained recovery keys", () => {
    expect(() =>
      loadConfig({
        ...validEnv,
        INGRESS_RECOVERY_PREVIOUS_KEYS: validEnv.INGRESS_RECOVERY_KEY,
      }),
    ).toThrow("Invalid INGRESS_RECOVERY_PREVIOUS_KEYS");
  });

  it("rejects more than four retained recovery keys", () => {
    expect(() =>
      loadConfig({
        ...validEnv,
        INGRESS_RECOVERY_PREVIOUS_KEYS: recoveryKeys.slice(0, 5).join(","),
      }),
    ).toThrow("Invalid INGRESS_RECOVERY_PREVIOUS_KEYS");
  });
});
