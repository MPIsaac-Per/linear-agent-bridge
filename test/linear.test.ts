import { createHmac } from "node:crypto";
import { promises as fsPromises } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  LinearActivityError,
  LinearAgentClient,
} from "../src/linear/client.js";
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

function observableFailureResponse(
  status: number,
  statusText: string,
  secretBody: string,
  onCancel: () => void,
): Response {
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(secretBody));
      },
      cancel() {
        onCancel();
      },
    }),
    { status, statusText },
  );
}

function oauthDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe("LinearAgentClient.createActivity", () => {
  it("safely detaches an already-aborted caller from a later rejecting token lookup", async () => {
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "unused",
      storePath: path.join(os.tmpdir(), "unused-aborted-token-store.json"),
    });
    vi.spyOn(oauth, "getAccessToken").mockRejectedValue(
      new Error("late private token lookup failure"),
    );
    const fetchFn = vi.fn();
    const client = new LinearAgentClient(oauth, fetchFn);
    const controller = new AbortController();
    controller.abort(new Error("caller closed"));

    await expect(
      client.createActivity(
        "session-aborted-token-lookup",
        { type: "thought", body: "never sent" },
        { signal: controller.signal },
      ),
    ).rejects.toThrow("caller closed");
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("autonomously retries a pre-rename failure before adopting the rotated pair", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const fetchFn = vi.fn().mockResolvedValue(
      jsonResponse({
        access_token: "rotated-access",
        refresh_token: "rotated-refresh",
        expires_in: 86399,
      }),
    );
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
      fetchFn,
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
        oauth.refreshAfterUnauthorized("initial-access"),
      ).rejects.toThrow();
      expect(await oauth.getAccessToken()).toBe("initial-access");

      await fsPromises.rm(tokenStorePath, { recursive: true });
      await vi.waitFor(
        async () => expect(await oauth.getAccessToken()).toBe("rotated-access"),
        { timeout: 1_000, interval: 20 },
      );
      expect(fetchFn).toHaveBeenCalledTimes(1);
      expect(JSON.parse(await fsPromises.readFile(tokenStorePath, "utf8"))).toMatchObject({
        accessToken: "rotated-access",
        refreshToken: "rotated-refresh",
      });
      expect((await fsPromises.stat(tokenStorePath)).mode & 0o777).toBe(0o600);

      const reloaded = new LinearOAuthTokenManager({
        clientId: "client-id",
        clientSecret: "client-secret",
        initialAccessToken: "unused",
        storePath: tokenStorePath,
      });
      await expect(reloaded.getAccessToken()).resolves.toBe("rotated-access");
    } finally {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("autonomously retries a post-rename sync failure before adoption", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const fetchFn = vi.fn().mockResolvedValue(
      jsonResponse({
        access_token: "rotated-access",
        refresh_token: "rotated-refresh",
        expires_in: 86399,
      }),
    );
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
      fetchFn,
    });

    try {
      await oauth.install({
        access_token: "initial-access",
        refresh_token: "initial-refresh",
        expires_in: 86399,
      });
      await fsPromises.chmod(tmpDir, 0o300);

      await expect(
        oauth.refreshAfterUnauthorized("initial-access"),
      ).rejects.toThrow();
      expect(await oauth.getAccessToken()).toBe("initial-access");

      await fsPromises.chmod(tmpDir, 0o700);
      expect(JSON.parse(await fsPromises.readFile(tokenStorePath, "utf8"))).toMatchObject({
        accessToken: "rotated-access",
        refreshToken: "rotated-refresh",
      });
      await vi.waitFor(
        async () => expect(await oauth.getAccessToken()).toBe("rotated-access"),
        { timeout: 1_000, interval: 20 },
      );
      expect(fetchFn).toHaveBeenCalledTimes(1);
    } finally {
      await fsPromises.chmod(tmpDir, 0o700).catch(() => undefined);
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("retries an incomplete directory-chain sync and repeats it in a fresh manager", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "first", "second", "tokens.json");
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
      fetchFn: vi.fn(),
    });

    try {
      await fsPromises.chmod(tmpDir, 0o300);
      await expect(
        oauth.install({
          access_token: "first-access",
          refresh_token: "first-refresh",
          expires_in: 86399,
        }),
      ).rejects.toThrow();
      await expect(
        oauth.refreshAfterUnauthorized("initial-access"),
      ).rejects.toThrow();
      expect(await oauth.getAccessToken()).toBe("initial-access");

      await fsPromises.chmod(tmpDir, 0o700);
      await vi.waitFor(
        async () => expect(await oauth.getAccessToken()).toBe("first-access"),
        { timeout: 1_000, interval: 20 },
      );

      const freshManager = new LinearOAuthTokenManager({
        clientId: "client-id",
        clientSecret: "client-secret",
        initialAccessToken: "unused",
        storePath: tokenStorePath,
      });
      await freshManager.load();
      await fsPromises.chmod(tmpDir, 0o300);
      await expect(
        freshManager.install({
          access_token: "second-access",
          refresh_token: "second-refresh",
          expires_in: 86399,
        }),
      ).rejects.toThrow();
      expect(await freshManager.getAccessToken()).toBe("first-access");

      await fsPromises.chmod(tmpDir, 0o700);
      await vi.waitFor(
        async () => expect(await freshManager.getAccessToken()).toBe("second-access"),
        { timeout: 1_000, interval: 20 },
      );
    } finally {
      await fsPromises.chmod(tmpDir, 0o700).catch(() => undefined);
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("preserves the persistence error when temporary-file cleanup also fails", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const fetchFn = vi.fn().mockResolvedValue(
      jsonResponse({
        access_token: "rotated-access",
        refresh_token: "rotated-refresh",
        expires_in: 86399,
      }),
    );
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
      fetchFn,
    });
    const primaryError = new Error("primary token-store failure");
    const cleanupError = new Error("secondary temporary-file cleanup failure");

    try {
      await oauth.install({
        access_token: "initial-access",
        refresh_token: "initial-refresh",
        expires_in: 86399,
      });
      vi.spyOn(fsPromises, "rename").mockRejectedValueOnce(primaryError);
      vi.spyOn(fsPromises, "rm").mockRejectedValueOnce(cleanupError);

      const caught = await oauth
        .refreshAfterUnauthorized("initial-access")
        .catch((err: unknown) => err);
      expect(caught).toBe(primaryError);
      expect(String(caught)).not.toContain(cleanupError.message);
      vi.restoreAllMocks();
      await vi.waitFor(
        async () => expect(await oauth.getAccessToken()).toBe("rotated-access"),
        { timeout: 1_000, interval: 20 },
      );
    } finally {
      vi.restoreAllMocks();
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("serializes concurrent token installs so the newer pair wins durably", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
    });
    const firstRenameStarted = oauthDeferred<void>();
    const releaseFirstRename = oauthDeferred<void>();

    try {
      await oauth.install({
        access_token: "initial-access",
        refresh_token: "initial-refresh",
        expires_in: 86399,
      });
      const actualRename = fsPromises.rename.bind(fsPromises);
      const renameSpy = vi
        .spyOn(fsPromises, "rename")
        .mockImplementation(async (from, to) => {
          if (renameSpy.mock.calls.length === 1) {
            firstRenameStarted.resolve();
            await releaseFirstRename.promise;
          }
          await actualRename(from, to);
        });

      const firstInstall = oauth.install({
        access_token: "first-access",
        refresh_token: "first-refresh",
        expires_in: 86399,
      });
      await firstRenameStarted.promise;
      const secondInstall = oauth.install({
        access_token: "second-access",
        refresh_token: "second-refresh",
        expires_in: 86399,
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      const renameCallsBeforeRelease = renameSpy.mock.calls.length;
      releaseFirstRename.resolve();
      const installResults = await Promise.allSettled([firstInstall, secondInstall]);
      expect(renameCallsBeforeRelease).toBe(1);
      expect(installResults.map((result) => result.status)).toEqual([
        "fulfilled",
        "fulfilled",
      ]);
      expect(await oauth.getAccessToken()).toBe("second-access");
      expect(JSON.parse(await fsPromises.readFile(tokenStorePath, "utf8"))).toMatchObject({
        accessToken: "second-access",
        refreshToken: "second-refresh",
      });
    } finally {
      releaseFirstRename.resolve();
      vi.restoreAllMocks();
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("discards a stale refresh response when a newer install advances the generation", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const refreshStarted = oauthDeferred<void>();
    const refreshResponse = oauthDeferred<Response>();
    const fetchFn = vi.fn(async () => {
      refreshStarted.resolve();
      return refreshResponse.promise;
    });
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
      fetchFn,
    });

    try {
      await oauth.install({
        access_token: "initial-access",
        refresh_token: "initial-refresh",
        expires_in: 86399,
      });
      const refresh = oauth.refreshAfterUnauthorized("initial-access");
      await refreshStarted.promise;
      await oauth.install({
        access_token: "newer-access",
        refresh_token: "newer-refresh",
        expires_in: 86399,
      });
      refreshResponse.resolve(
        jsonResponse({
          access_token: "stale-access",
          refresh_token: "stale-refresh",
          expires_in: 86399,
        }),
      );

      await expect(refresh).resolves.toBe("newer-access");
      expect(fetchFn).toHaveBeenCalledTimes(1);
      expect(JSON.parse(await fsPromises.readFile(tokenStorePath, "utf8"))).toMatchObject({
        accessToken: "newer-access",
        refreshToken: "newer-refresh",
      });
    } finally {
      refreshResponse.resolve(jsonResponse({}));
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("autonomous retry persists the newest pending pair without another refresh", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    const fetchFn = vi.fn().mockResolvedValue(
      jsonResponse({
        access_token: "refresh-access",
        refresh_token: "refresh-token",
        expires_in: 86399,
      }),
    );
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "initial-access",
      storePath: tokenStorePath,
      fetchFn,
    });

    try {
      await oauth.install({
        access_token: "initial-access",
        refresh_token: "initial-refresh",
        expires_in: 86399,
      });
      const persistenceFailure = new Error("private persistence failure");
      const actualRename = fsPromises.rename.bind(fsPromises);
      vi.spyOn(fsPromises, "rename")
        .mockRejectedValueOnce(persistenceFailure)
        .mockRejectedValueOnce(persistenceFailure)
        .mockImplementation(actualRename);

      await expect(
        oauth.refreshAfterUnauthorized("initial-access"),
      ).rejects.toBe(persistenceFailure);
      await expect(
        oauth.install({
          access_token: "newest-access",
          refresh_token: "newest-refresh",
          expires_in: 86399,
        }),
      ).rejects.toBe(persistenceFailure);

      await vi.waitFor(
        async () => expect(await oauth.getAccessToken()).toBe("newest-access"),
        { timeout: 1_000, interval: 20 },
      );
      expect(fetchFn).toHaveBeenCalledTimes(1);
      expect(JSON.parse(await fsPromises.readFile(tokenStorePath, "utf8"))).toMatchObject({
        accessToken: "newest-access",
        refreshToken: "newest-refresh",
      });
    } finally {
      vi.restoreAllMocks();
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("refreshes an expired OAuth token, persists the rotated pair, and retries once", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const tokenStorePath = path.join(tmpDir, "tokens.json");
    let unauthorizedCanceled = false;
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        observableFailureResponse(
          401,
          "Unauthorized",
          "raw-expired-auth-secret",
          () => {
            unauthorizedCanceled = true;
          },
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
      expect(unauthorizedCanceled).toBe(true);
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

  it("cancels every failed OAuth refresh body during repeated rate limits and outages", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-oauth-"));
    const canceled: number[] = [];
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        observableFailureResponse(
          429,
          "Too Many Requests",
          "raw-refresh-rate-limit-secret",
          () => canceled.push(429),
        ),
      )
      .mockResolvedValueOnce(
        observableFailureResponse(
          502,
          "Bad Gateway",
          "raw-refresh-outage-secret",
          () => canceled.push(502),
        ),
      );
    const oauth = new LinearOAuthTokenManager({
      clientId: "client-id",
      clientSecret: "client-secret",
      initialAccessToken: "expired-access",
      storePath: path.join(tmpDir, "tokens.json"),
      fetchFn,
    });

    try {
      await oauth.install({
        access_token: "expired-access",
        refresh_token: "initial-refresh",
        expires_in: 0,
      });
      for (const status of [429, 502]) {
        const error = await oauth
          .refreshAfterUnauthorized("expired-access")
          .catch((caught: unknown) => caught);
        expect(error).toBeInstanceOf(Error);
        expect(String(error)).toContain(
          `Linear OAuth token refresh failed: ${status}`,
        );
        expect(String(error)).not.toContain("raw-refresh-rate-limit-secret");
        expect(String(error)).not.toContain("raw-refresh-outage-secret");
      }
      expect(canceled).toEqual([429, 502]);
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

    await client.createActivity("session-1", content, {
      activityId: "f15c2bc6-9aac-42f7-862c-6fe926b13527",
      ephemeral: true,
    });

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
          id: string;
          agentSessionId: string;
          content: AgentActivityContent;
          ephemeral?: boolean;
        };
      };
    };
    expect(parsedBody.query).toContain("agentActivityCreate");
    expect(parsedBody.query).toContain("AgentActivityCreateInput");
    expect(parsedBody.variables).toEqual({
      input: {
        id: "f15c2bc6-9aac-42f7-862c-6fe926b13527",
        agentSessionId: "session-1",
        content,
        ephemeral: true,
      },
    });
  });

  it("reuses the caller UUID when OAuth refresh retries an activity", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-id-"));
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
      storePath: path.join(tmpDir, "tokens.json"),
      fetchFn,
    });
    await oauth.install({
      access_token: "expired-access",
      refresh_token: "initial-refresh",
      expires_in: 0,
    });
    const client = new LinearAgentClient(oauth, fetchFn);

    try {
      await client.createActivity(
        "session-idempotent",
        { type: "response", body: "done" },
        { activityId: "e9523ef5-53cc-477f-8c5b-cf8b33ce16b9" },
      );

      const firstInput = JSON.parse(fetchFn.mock.calls[0]?.[1]?.body as string)
        .variables.input;
      const retriedInput = JSON.parse(fetchFn.mock.calls[2]?.[1]?.body as string)
        .variables.input;
      expect(firstInput.id).toBe("e9523ef5-53cc-477f-8c5b-cf8b33ce16b9");
      expect(retriedInput.id).toBe(firstInput.id);
    } finally {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
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
        { error: "raw-linear-response-body" },
        { ok: false, status: 500, statusText: "Internal Server Error" },
      ),
    );
    const client = new LinearAgentClient("test-token", fetchFn);

    const error = await client
      .createActivity("session-3", { type: "thought", body: "x" })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toContain("500");
    expect(String(error)).not.toContain("raw-linear-response-body");
  });

  it("cancels every failed HTTP body during repeated rate limits and outages", async () => {
    const canceled: number[] = [];
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        observableFailureResponse(
          429,
          "Too Many Requests",
          "raw-rate-limit-secret",
          () => canceled.push(429),
        ),
      )
      .mockResolvedValueOnce(
        observableFailureResponse(
          503,
          "Service Unavailable",
          "raw-outage-secret",
          () => canceled.push(503),
        ),
      );
    const client = new LinearAgentClient("test-token", fetchFn);

    for (const status of [429, 503]) {
      const error = await client
        .createActivity(`session-${status}`, { type: "thought", body: "x" })
        .catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(LinearActivityError);
      expect(String(error)).toContain(String(status));
      expect(String(error)).not.toContain("raw-rate-limit-secret");
      expect(String(error)).not.toContain("raw-outage-secret");
    }
    expect(canceled).toEqual([429, 503]);
  });

  it("does not echo GraphQL response error text", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(jsonResponse({ errors: [{ message: "agentSessionId not found" }] }));
    const client = new LinearAgentClient("test-token", fetchFn);

    const error = await client
      .createActivity("session-4", { type: "thought", body: "x" })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toContain("GraphQL error");
    expect(String(error)).not.toContain("agentSessionId not found");
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

describe("LinearAgentClient.activityExists", () => {
  it("confirms only the exact caller activity id in the expected session", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      jsonResponse({
        data: {
          agentActivities: {
            nodes: [
              {
                id: "activity-id",
                agentSession: { id: "session-id" },
              },
            ],
          },
        },
      }),
    );
    const client = new LinearAgentClient("token", fetchFn);

    await expect(
      client.activityExists("session-id", "activity-id"),
    ).resolves.toBe(true);
    const request = JSON.parse(fetchFn.mock.calls[0]![1]!.body as string) as {
      query: string;
      variables: { id: string };
    };
    expect(request.query).toContain("agentActivities(first: 2");
    expect(request.variables).toEqual({ id: "activity-id" });
  });

  it("returns false only when the exact activity is absent", async () => {
    const client = new LinearAgentClient(
      "token",
      vi.fn().mockResolvedValue(
        jsonResponse({ data: { agentActivities: { nodes: [] } } }),
      ),
    );
    await expect(
      client.activityExists("session-id", "missing-activity"),
    ).resolves.toBe(false);
  });

  it("fails closed when the query identity is inconsistent", async () => {
    const client = new LinearAgentClient(
      "token",
      vi.fn().mockResolvedValue(
        jsonResponse({
          data: {
            agentActivities: {
              nodes: [
                {
                  id: "activity-id",
                  agentSession: { id: "different-session" },
                },
              ],
            },
          },
        }),
      ),
    );
    await expect(
      client.activityExists("session-id", "activity-id"),
    ).rejects.toBeInstanceOf(LinearActivityError);
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
