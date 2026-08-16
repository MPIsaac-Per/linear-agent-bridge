import { createHmac } from "node:crypto";
import { promises as fsPromises } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { LinearAgentClient } from "../src/linear/client.js";
import { LinearOAuthTokenManager } from "../src/linear/oauth.js";
import { verifyWebhook } from "../src/linear/webhook-verify.js";
import type { AgentActivityContent } from "../src/types.js";

const GRAPHQL_URL = "https://api.linear.app/graphql";

function jsonResponse(
  body: unknown,
  init: { ok?: boolean; status?: number; statusText?: string } = {},
): Response {
  const ok = init.ok ?? true;
  const status = init.status ?? (ok ? 200 : 500);
  return {
    ok,
    status,
    statusText: init.statusText ?? (ok ? "OK" : "Internal Server Error"),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response;
}

describe("LinearAgentClient.createActivity", () => {
  it("retains a newly rotated pair in memory when durable persistence fails", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
    });

    try {
      await oauth.install({
        access_token: "initial-access",
        refresh_token: "initial-refresh",
        expires_in: 86399,
      });
      await fsPromises.rm(tokenStorePath);
      await fsPromises.mkdir(tokenStorePath);

      await expect(
        oauth.install({
          access_token: "rotated-access",
          refresh_token: "rotated-refresh",
          expires_in: 86399,
        }),
      ).rejects.toThrow();
      expect(await oauth.getAccessToken()).toBe("rotated-access");
      expect(await oauth.hasRefreshToken()).toBe(true);
    } finally {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("refreshes an expired OAuth token, persists the rotated pair, and retries once", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(
          { errors: [{ message: "Authentication required" }] },
          { ok: false, status: 401, statusText: "Unauthorized" },
        ),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          access_token: "fresh-access",
          refresh_token: "fresh-refresh",
          expires_in: 86399,
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ data: { agentActivityCreate: { success: true } } }),
      );
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "expired-access",
      storePath: tokenStorePath,
      fetchFn,
    });
    await oauth.install({
      access_token: "expired-access",
      refresh_token: "initial-refresh",
      expires_in: 0,
    });
    const client = new LinearAgentClient(oauth, fetchFn);

    try {
      await client.createActivity("session-refresh", { type: "thought", body: "hello" });

      expect(fetchFn).toHaveBeenCalledTimes(3);
      expect(fetchFn.mock.calls[0]?.[0]).toBe(GRAPHQL_URL);
      expect(fetchFn.mock.calls[1]?.[0]).toBe("https://api.linear.app/oauth/token");
      const refreshParams = new URLSearchParams(
        fetchFn.mock.calls[1]?.[1]?.body as string,
      );
      expect(refreshParams.get("grant_type")).toBe("refresh_token");
      expect(refreshParams.get("refresh_token")).toBe("initial-refresh");
      expect(fetchFn.mock.calls[2]?.[1]?.headers).toMatchObject({
        Authorization: "Bearer fresh-access",
      });
      expect(JSON.parse(await fsPromises.readFile(tokenStorePath, "utf8"))).toMatchObject({
        accessToken: "fresh-access",
        refreshToken: "fresh-refresh",
      });
    } finally {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("POSTs the agentActivityCreate mutation with a thought activity", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(jsonResponse({ data: { agentActivityCreate: { success: true } } }));
    const client = new LinearAgentClient("test-token", fetchFn);
    const content: AgentActivityContent = { type: "thought", body: "thinking..." };

    await client.createActivity("session-1", content, { ephemeral: true });

    expect(fetchFn).toHaveBeenCalledTimes(1);
    const [url, init] = fetchFn.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(GRAPHQL_URL);
    expect(init.method).toBe("POST");
    expect(init.headers).toMatchObject({
      Authorization: "Bearer test-token",
      "Content-Type": "application/json",
    });

    const parsedBody = JSON.parse(init.body as string) as {
      query: string;
      variables: {
        input: {
          agentSessionId: string;
          content: AgentActivityContent;
          ephemeral?: boolean;
        };
      };
    };
    expect(parsedBody.query).toContain("agentActivityCreate");
    expect(parsedBody.query).toContain("AgentActivityCreateInput");
    expect(parsedBody.variables).toEqual({
      input: { agentSessionId: "session-1", content, ephemeral: true },
    });
  });

  it("POSTs a response activity with the correct content shape", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(jsonResponse({ data: { agentActivityCreate: { success: true } } }));
    const client = new LinearAgentClient("test-token", fetchFn);
    const content: AgentActivityContent = { type: "response", body: "all done" };

    await client.createActivity("session-2", content);

    const [, init] = fetchFn.mock.calls[0] as [string, RequestInit];
    const parsedBody = JSON.parse(init.body as string) as {
      variables: { input: { agentSessionId: string; content: AgentActivityContent } };
    };
    expect(parsedBody.variables.input).toEqual({ agentSessionId: "session-2", content });
  });

  it("sends the Authorization bearer header derived from the constructor token", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(jsonResponse({ data: { agentActivityCreate: { success: true } } }));
    const client = new LinearAgentClient("secret-abc-123", fetchFn);

    await client.createActivity("session-x", { type: "thought", body: "x" });

    const [, init] = fetchFn.mock.calls[0] as [string, RequestInit];
    const headers = init.headers as Record<string, string>;
    expect(headers.Authorization).toBe("Bearer secret-abc-123");
  });

  it("throws on a non-2xx HTTP response", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      jsonResponse(
        { error: "server exploded" },
        { ok: false, status: 500, statusText: "Internal Server Error" },
      ),
    );
    const client = new LinearAgentClient("test-token", fetchFn);

    await expect(
      client.createActivity("session-3", { type: "thought", body: "x" }),
    ).rejects.toThrow(/500/);
  });

  it("throws with the GraphQL error text when the response carries errors", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(jsonResponse({ errors: [{ message: "agentSessionId not found" }] }));
    const client = new LinearAgentClient("test-token", fetchFn);

    await expect(
      client.createActivity("session-4", { type: "thought", body: "x" }),
    ).rejects.toThrow(/agentSessionId not found/);
  });

  it("throws when the mutation reports success: false", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(jsonResponse({ data: { agentActivityCreate: { success: false } } }));
    const client = new LinearAgentClient("test-token", fetchFn);

    await expect(
      client.createActivity("session-5", { type: "thought", body: "x" }),
    ).rejects.toThrow();
  });
});

const WEBHOOK_SECRET = "whsec_test_secret";

function makeBody(webhookTimestamp: number): Buffer {
  return Buffer.from(
    JSON.stringify({
      action: "create",
      type: "AgentSessionEvent",
      webhookTimestamp,
      data: {},
    }),
    "utf8",
  );
}

function sign(body: Buffer, secret: string): string {
  return createHmac("sha256", secret).update(body).digest("hex");
}

describe("verifyWebhook", () => {
  it("accepts a valid signature with a fresh timestamp", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now - 1000);
    const signature = sign(body, WEBHOOK_SECRET);

    expect(verifyWebhook(body, signature, WEBHOOK_SECRET, now)).toBe(true);
  });

  it("accepts a timestamp exactly at the 60s staleness boundary", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now - 60_000);
    const signature = sign(body, WEBHOOK_SECRET);

    expect(verifyWebhook(body, signature, WEBHOOK_SECRET, now)).toBe(true);
  });

  it("defaults the staleness reference to Date.now() when omitted", () => {
    const body = makeBody(Date.now());
    const signature = sign(body, WEBHOOK_SECRET);

    expect(verifyWebhook(body, signature, WEBHOOK_SECRET)).toBe(true);
  });

  it("rejects when signed with the wrong secret", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now - 1000);
    const signature = sign(body, "wrong-secret");

    expect(verifyWebhook(body, signature, WEBHOOK_SECRET, now)).toBe(false);
  });

  it("rejects a tampered body (signature no longer matches)", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now - 1000);
    const signature = sign(body, WEBHOOK_SECRET);
    const tampered = Buffer.from(
      body.toString("utf8").replace('"create"', '"delete"'),
      "utf8",
    );

    expect(verifyWebhook(tampered, signature, WEBHOOK_SECRET, now)).toBe(false);
  });

  it("rejects a missing signature header", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now - 1000);

    expect(verifyWebhook(body, undefined, WEBHOOK_SECRET, now)).toBe(false);
  });

  it("rejects a stale timestamp older than 60s", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now - 61_000);
    const signature = sign(body, WEBHOOK_SECRET);

    expect(verifyWebhook(body, signature, WEBHOOK_SECRET, now)).toBe(false);
  });

  it("rejects a timestamp more than 60s in the future (clock skew)", () => {
    const now = 1_700_000_000_000;
    const body = makeBody(now + 61_000);
    const signature = sign(body, WEBHOOK_SECRET);

    expect(verifyWebhook(body, signature, WEBHOOK_SECRET, now)).toBe(false);
  });
});
