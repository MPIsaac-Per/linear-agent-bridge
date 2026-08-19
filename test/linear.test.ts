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

describe("LinearAgentClient reconciliation reads", () => {
  it("paginates recent sessions, filters by the authenticated app user client-side, and caps results", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          data: {
            viewer: { id: "app-user-1" },
            agentSessions: {
              nodes: [
                {
                  id: "foreign-session",
                  updatedAt: "2026-08-18T11:59:00.000Z",
                  appUser: { id: "another-app" },
                },
                {
                  id: "owned-session-2",
                  updatedAt: "2026-08-18T11:58:00.000Z",
                  appUser: { id: "app-user-1" },
                  issue: { identifier: "MPI-2" },
                },
              ],
              pageInfo: { hasNextPage: true, endCursor: "sessions-page-2" },
            },
          },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          data: {
            viewer: { id: "app-user-1" },
            agentSessions: {
              nodes: [
                {
                  id: "owned-session-1",
                  updatedAt: "2026-08-18T11:57:00.000Z",
                  appUser: { id: "app-user-1" },
                },
              ],
              pageInfo: { hasNextPage: false, endCursor: null },
            },
          },
        }),
      );
    const client = new LinearAgentClient("test-token", fetchFn);

    await expect(
      client.listRecentAppOwnedSessions({
        updatedAfter: "2026-08-17T12:00:00.000Z",
        maxSessions: 2,
      }),
    ).resolves.toEqual([
      {
        id: "owned-session-2",
        updatedAt: "2026-08-18T11:58:00.000Z",
        appUserId: "app-user-1",
        issueIdentifier: "MPI-2",
      },
      {
        id: "owned-session-1",
        updatedAt: "2026-08-18T11:57:00.000Z",
        appUserId: "app-user-1",
      },
    ]);

    const first = JSON.parse(fetchFn.mock.calls[0]?.[1]?.body as string);
    const second = JSON.parse(fetchFn.mock.calls[1]?.[1]?.body as string);
    expect(first.query).toContain("agentSessions(");
    expect(first.query).not.toContain("agentSessions(filter:");
    expect(first.variables).toEqual({ first: 50, after: null });
    expect(second.variables).toEqual({ first: 50, after: "sessions-page-2" });
  });

  it("paginates one session's activities to its watermark and returns deterministic Linear order", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          data: {
            agentSession: {
              id: "session-1",
              appUser: { id: "app-user-1" },
              issue: { identifier: "MPI-1" },
              activities: {
                nodes: [
                  {
                    id: "prompt-c",
                    createdAt: "2026-08-18T12:03:00.000Z",
                    signal: null,
                    user: { id: "human-1" },
                    content: {
                      __typename: "AgentActivityPromptContent",
                      body: "third",
                    },
                  },
                  {
                    id: "prompt-b",
                    createdAt: "2026-08-18T12:02:00.000Z",
                    signal: "stop",
                    user: { id: "human-1" },
                    content: {
                      __typename: "AgentActivityPromptContent",
                      body: "stop",
                    },
                  },
                ],
                pageInfo: { hasNextPage: true, endCursor: "activities-page-2" },
              },
            },
          },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          data: {
            agentSession: {
              id: "session-1",
              appUser: { id: "app-user-1" },
              issue: { identifier: "MPI-1" },
              activities: {
                nodes: [
                  {
                    id: "prompt-a",
                    createdAt: "2026-08-18T12:01:00.000Z",
                    signal: null,
                    user: { id: "human-1" },
                    content: {
                      __typename: "AgentActivityPromptContent",
                      body: "first",
                    },
                  },
                  {
                    id: "bridge-thought-without-user",
                    createdAt: "2026-08-18T12:01:30.000Z",
                    signal: null,
                    content: {
                      __typename: "AgentActivityThoughtContent",
                    },
                  },
                ],
                pageInfo: { hasNextPage: true, endCursor: "activities-page-3" },
              },
            },
          },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          data: {
            agentSession: {
              id: "session-1",
              appUser: { id: "app-user-1" },
              issue: { identifier: "MPI-1" },
              activities: {
                nodes: [
                  {
                    id: "prompt-z",
                    createdAt: "2026-08-18T12:01:00.000Z",
                    signal: null,
                    user: { id: "human-1" },
                    content: {
                      __typename: "AgentActivityPromptContent",
                      body: "same-time after watermark",
                    },
                  },
                ],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          },
        }),
      );
    const client = new LinearAgentClient("test-token", fetchFn);

    const result = await client.listAgentSessionActivities("session-1", {
      lookbackAfter: "2026-08-11T12:00:00.000Z",
      processedThrough: {
        createdAt: "2026-08-18T12:01:00.000Z",
        id: "prompt-a",
      },
    });

    expect(result).toEqual({
      id: "session-1",
      appUserId: "app-user-1",
      issueIdentifier: "MPI-1",
      activities: [
        {
          id: "prompt-z",
          createdAt: "2026-08-18T12:01:00.000Z",
          userId: "human-1",
          type: "prompt",
          body: "same-time after watermark",
        },
        {
          id: "bridge-thought-without-user",
          createdAt: "2026-08-18T12:01:30.000Z",
          type: "thought",
        },
        {
          id: "prompt-b",
          createdAt: "2026-08-18T12:02:00.000Z",
          userId: "human-1",
          signal: "stop",
          type: "prompt",
          body: "stop",
        },
        {
          id: "prompt-c",
          createdAt: "2026-08-18T12:03:00.000Z",
          userId: "human-1",
          type: "prompt",
          body: "third",
        },
      ],
    });
    expect(fetchFn).toHaveBeenCalledTimes(3);
    const second = JSON.parse(fetchFn.mock.calls[1]?.[1]?.body as string);
    expect(second.variables).toEqual({
      sessionId: "session-1",
      first: 50,
      after: "activities-page-2",
      lookbackAfter: "2026-08-11T12:00:00.000Z",
    });
    const third = JSON.parse(fetchFn.mock.calls[2]?.[1]?.body as string);
    expect(third.variables.after).toBe("activities-page-3");
  });

  it("retains a same-millisecond activity whose id sorts below the watermark", async () => {
    const fetchFn = vi.fn().mockResolvedValueOnce(
      jsonResponse({
        data: {
          agentSession: {
            id: "session-tiebreak",
            appUser: { id: "app-user-1" },
            activities: {
              nodes: [
                {
                  id: "aaa-sorts-before-watermark",
                  createdAt: "2026-08-18T12:01:00.000Z",
                  signal: null,
                  user: { id: "human-1" },
                  content: {
                    __typename: "AgentActivityPromptContent",
                    body: "must not be skipped forever",
                  },
                },
                {
                  id: "zzz-watermark",
                  createdAt: "2026-08-18T12:01:00.000Z",
                  signal: null,
                  user: { id: "human-1" },
                  content: {
                    __typename: "AgentActivityPromptContent",
                    body: "already processed",
                  },
                },
              ],
              pageInfo: { hasNextPage: false, endCursor: null },
            },
          },
        },
      }),
    );
    const client = new LinearAgentClient("test-token", fetchFn);

    const result = await client.listAgentSessionActivities("session-tiebreak", {
      lookbackAfter: "2026-08-11T12:00:00.000Z",
      processedThrough: {
        createdAt: "2026-08-18T12:01:00.000Z",
        id: "zzz-watermark",
      },
    });

    // The watermark activity itself is the only one known to be processed at
    // that millisecond; an id tiebreaker would drop its sibling permanently.
    expect(result.activities.map((activity) => activity.id)).toEqual([
      "aaa-sorts-before-watermark",
    ]);
  });

  it("uses the existing OAuth refresh path for reconciliation queries", async () => {
    const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "linear-query-oauth-"));
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({}, { ok: false, status: 401, statusText: "Unauthorized" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          access_token: "fresh-access",
          refresh_token: "fresh-refresh",
          expires_in: 86399,
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          data: {
            viewer: { id: "app-user-1" },
            agentSessions: {
              nodes: [],
              pageInfo: { hasNextPage: false, endCursor: null },
            },
          },
        }),
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
      await expect(
        client.listRecentAppOwnedSessions({
          updatedAfter: "2026-08-17T12:00:00.000Z",
          maxSessions: 250,
        }),
      ).resolves.toEqual([]);
      expect(fetchFn.mock.calls[2]?.[1]?.headers).toMatchObject({
        Authorization: "Bearer fresh-access",
      });
    } finally {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("rejects a recent-session scan cap above 250 at the Linear boundary", async () => {
    const fetchFn = vi.fn();
    const client = new LinearAgentClient("test-token", fetchFn);

    await expect(
      client.listRecentAppOwnedSessions({
        updatedAfter: "2026-08-17T12:00:00.000Z",
        maxSessions: 251,
      }),
    ).rejects.toThrow(/maxSessions/);
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("rejects a looping pagination cursor instead of issuing unbounded reads", async () => {
    const page = {
      data: {
        viewer: { id: "app-user-1" },
        agentSessions: {
          nodes: [
            {
              id: "foreign-session",
              updatedAt: "2026-08-18T11:59:00.000Z",
              appUser: { id: "another-app" },
            },
          ],
          pageInfo: { hasNextPage: true, endCursor: "same-cursor" },
        },
      },
    };
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(page))
      .mockResolvedValueOnce(jsonResponse(page));
    const client = new LinearAgentClient("test-token", fetchFn);

    await expect(
      client.listRecentAppOwnedSessions({
        updatedAfter: "2026-08-17T12:00:00.000Z",
        maxSessions: 250,
      }),
    ).rejects.toThrow(/shape/);
    expect(fetchFn).toHaveBeenCalledTimes(2);
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
