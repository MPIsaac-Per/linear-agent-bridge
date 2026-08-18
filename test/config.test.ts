import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";

const validEnv = {
  LINEAR_CLIENT_ID: "client-id",
  LINEAR_CLIENT_SECRET: "client-secret",
  LINEAR_WEBHOOK_SECRET: "webhook-secret",
  LINEAR_ACCESS_TOKEN: "access-token",
};

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
      oauthTokenStorePath: "./data/oauth-tokens.json",
      runInactivityTimeoutMs: 300000,
    });
  });

  it("honors path, port, and runtime overrides", () => {
    const config = loadConfig({
      ...validEnv,
      PORT: "8080",
      RUNTIME: "codex",
      KB_PATH: "/tmp/kb",
      SESSION_STORE_PATH: "/tmp/sessions.json",
      OAUTH_TOKEN_STORE_PATH: "/tmp/oauth-tokens.json",
      RUN_INACTIVITY_TIMEOUT_MS: "45000",
    });

    expect(config.port).toBe(8080);
    expect(config.runtime).toBe("codex");
    expect(config.kbPath).toBe("/tmp/kb");
    expect(config.sessionStorePath).toBe("/tmp/sessions.json");
    expect(config.oauthTokenStorePath).toBe("/tmp/oauth-tokens.json");
    expect(config.runInactivityTimeoutMs).toBe(45000);
  });

  it.each([
    ["LINEAR_CLIENT_ID"],
    ["LINEAR_CLIENT_SECRET"],
    ["LINEAR_WEBHOOK_SECRET"],
    ["LINEAR_ACCESS_TOKEN"],
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
});
