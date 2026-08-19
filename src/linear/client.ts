import type { AgentActivityContent } from "../types.js";
import type { ReconciliationCursor } from "../state/store.js";
import type { LinearOAuthTokenManager } from "./oauth.js";

const LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql";

const AGENT_ACTIVITY_CREATE_MUTATION = `
  mutation AgentActivityCreate($input: AgentActivityCreateInput!) {
    agentActivityCreate(input: $input) {
      success
    }
  }
`;

const RECENT_AGENT_SESSIONS_QUERY = `
  query ReconciliationAgentSessions($first: Int!, $after: String) {
    viewer { id }
    agentSessions(first: $first, after: $after, orderBy: updatedAt) {
      nodes {
        id
        updatedAt
        appUser { id }
        issue { identifier }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

const AGENT_SESSION_ACTIVITIES_QUERY = `
  query ReconciliationAgentSessionActivities(
    $sessionId: String!
    $first: Int!
    $after: String
    # Linear's createdAt comparator takes DateTimeOrDuration, not DateTime.
    # Declaring DateTime! makes the server reject the whole query with a 400.
    $lookbackAfter: DateTimeOrDuration!
  ) {
    agentSession(id: $sessionId) {
      id
      appUser { id }
      issue { identifier }
      activities(
        first: $first
        after: $after
        orderBy: createdAt
        filter: { createdAt: { gte: $lookbackAfter } }
      ) {
        nodes {
          id
          createdAt
          signal
          user { id }
          content {
            __typename
            ... on AgentActivityPromptContent { body }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
`;

interface GraphQLError {
  message: string;
}

interface AgentActivityCreateResponse {
  data?: {
    agentActivityCreate?: {
      success: boolean;
    };
  };
  errors?: GraphQLError[];
}

interface PageInfo {
  hasNextPage: boolean;
  endCursor: string | null;
}

export interface LinearAgentSessionSummary {
  id: string;
  updatedAt: string;
  appUserId: string;
  issueIdentifier?: string | undefined;
}

export type ReconciledAgentActivityType =
  | "action"
  | "elicitation"
  | "error"
  | "prompt"
  | "response"
  | "thought";

export interface ReconciledAgentActivity extends ReconciliationCursor {
  /** Absent on app-generated output, which carries no Linear user. */
  userId?: string | undefined;
  type: ReconciledAgentActivityType;
  signal?: string | undefined;
  body?: string | undefined;
}

export interface LinearAgentSessionActivities {
  id: string;
  appUserId: string;
  issueIdentifier?: string | undefined;
  activities: ReconciledAgentActivity[];
}

/** Static, body-free failure surfaced to ingress orchestration and logs. */
export class LinearActivityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LinearActivityError";
  }
}

export class LinearQueryError extends Error {
  constructor(operation: string, failure: "http" | "graphql" | "shape") {
    super(`Linear ${operation} query failed: ${failure}`);
    this.name = "LinearQueryError";
  }
}

/** Signature-compatible subset of the global `fetch` used for injection. */
export type FetchFn = typeof fetch;

/**
 * Reject with the caller's abort reason the moment the signal fires, and
 * detach from `promise` so a later rejection cannot overwrite that reason.
 */
function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) {
    return promise;
  }
  if (signal.aborted) {
    void promise.catch(() => undefined);
    return Promise.reject(signal.reason);
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

/** Release an unused HTTP response without decoding or exposing its body. */
export async function discardResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Preserve the caller's bounded HTTP error when cleanup itself fails.
  }
}

/**
 * Thin Linear GraphQL client for the Agent Interaction API.
 * Emits agent activities via the `agentActivityCreate` mutation:
 *
 *   mutation AgentActivityCreate($input: AgentActivityCreateInput!) {
 *     agentActivityCreate(input: $input) { success }
 *   }
 *
 * Input carries `agentSessionId` and `content` (shaped by activity type).
 * Timing rules from Linear's docs: respond to a webhook within 5s and, on
 * `created`, emit an activity within 10s or the session is marked
 * unresponsive — the server acks first, then works.
 */
export class LinearAgentClient {
  constructor(
    private readonly tokenSource: string | LinearOAuthTokenManager,
    private readonly fetchFn: FetchFn = globalThis.fetch,
  ) {}

  async createActivity(
    agentSessionId: string,
    content: AgentActivityContent,
    options: {
      activityId?: string;
      ephemeral?: boolean;
      signal?: AbortSignal;
    } = {},
  ): Promise<void> {
    const accessToken = await abortable(
      this.getAccessToken(options.signal),
      options.signal,
    );
    let response = await this.postActivity(
      accessToken,
      agentSessionId,
      content,
      options,
    );

    if (response.status === 401 && typeof this.tokenSource !== "string") {
      await discardResponseBody(response);
      const refreshedAccessToken = await abortable(
        this.tokenSource.refreshAfterUnauthorized(accessToken, options.signal),
        options.signal,
      );
      response = await this.postActivity(
        refreshedAccessToken,
        agentSessionId,
        content,
        options,
      );
    }

    if (!response.ok) {
      await discardResponseBody(response);
      throw new LinearActivityError(
        `Linear agentActivityCreate failed: ${response.status} ${response.statusText}`,
      );
    }

    const json = (await response.json()) as AgentActivityCreateResponse;

    if (json.errors && json.errors.length > 0) {
      throw new LinearActivityError("Linear agentActivityCreate GraphQL error");
    }

    if (json.data?.agentActivityCreate?.success !== true) {
      throw new LinearActivityError(
        "Linear agentActivityCreate returned success: false with no GraphQL errors",
      );
    }
  }

  async listRecentAppOwnedSessions(options: {
    updatedAfter: string;
    maxSessions: number;
    signal?: AbortSignal | undefined;
  }): Promise<LinearAgentSessionSummary[]> {
    validateTimestamp(options.updatedAfter, "updatedAfter");
    if (
      !Number.isInteger(options.maxSessions) ||
      options.maxSessions <= 0 ||
      options.maxSessions > 250
    ) {
      throw new Error("maxSessions must be an integer from 1 to 250");
    }

    const sessions: LinearAgentSessionSummary[] = [];
    const seenCursors = new Set<string>();
    let after: string | null = null;
    while (sessions.length < options.maxSessions) {
      const json = await this.queryGraphQL(
        RECENT_AGENT_SESSIONS_QUERY,
        { first: 50, after },
        "agentSessions",
        options.signal,
      );
      const data = asRecord(asRecord(json)?.data);
      const viewer = asRecord(data?.viewer);
      const connection = asRecord(data?.agentSessions);
      const viewerId = viewer?.id;
      const nodes = connection?.nodes;
      const pageInfo = parsePageInfo(connection?.pageInfo, "agentSessions");
      if (typeof viewerId !== "string" || !Array.isArray(nodes)) {
        throw new LinearQueryError("agentSessions", "shape");
      }

      let pageReachedLookback = false;
      for (const rawNode of nodes) {
        const node = asRecord(rawNode);
        const appUser = asRecord(node?.appUser);
        if (
          typeof node?.id !== "string" ||
          typeof node.updatedAt !== "string" ||
          !Number.isFinite(Date.parse(node.updatedAt)) ||
          typeof appUser?.id !== "string"
        ) {
          throw new LinearQueryError("agentSessions", "shape");
        }
        if (Date.parse(node.updatedAt) < Date.parse(options.updatedAfter)) {
          pageReachedLookback = true;
          continue;
        }
        if (appUser.id !== viewerId) {
          continue;
        }
        const issueIdentifier = asRecord(node.issue)?.identifier;
        sessions.push({
          id: node.id,
          updatedAt: node.updatedAt,
          appUserId: appUser.id,
          ...(typeof issueIdentifier === "string" ? { issueIdentifier } : {}),
        });
        if (sessions.length >= options.maxSessions) {
          break;
        }
      }

      if (!pageInfo.hasNextPage || pageReachedLookback) {
        break;
      }
      after = nextPageCursor(pageInfo, seenCursors, nodes.length, "agentSessions");
    }
    return sessions;
  }

  async listAgentSessionActivities(
    sessionId: string,
    options: {
      lookbackAfter: string;
      processedThrough?: ReconciliationCursor | undefined;
      signal?: AbortSignal | undefined;
    },
  ): Promise<LinearAgentSessionActivities> {
    if (sessionId.length === 0) {
      throw new Error("sessionId must not be empty");
    }
    validateTimestamp(options.lookbackAfter, "lookbackAfter");

    const activities: ReconciledAgentActivity[] = [];
    const seenCursors = new Set<string>();
    let after: string | null = null;
    let session: Omit<LinearAgentSessionActivities, "activities"> | undefined;
    while (true) {
      const json = await this.queryGraphQL(
        AGENT_SESSION_ACTIVITIES_QUERY,
        {
          sessionId,
          first: 50,
          after,
          lookbackAfter: options.lookbackAfter,
        },
        "agentSessionActivities",
        options.signal,
      );
      const rawSession = asRecord(asRecord(asRecord(json)?.data)?.agentSession);
      const appUser = asRecord(rawSession?.appUser);
      const connection = asRecord(rawSession?.activities);
      const nodes = connection?.nodes;
      const pageInfo = parsePageInfo(
        connection?.pageInfo,
        "agentSessionActivities",
      );
      if (
        typeof rawSession?.id !== "string" ||
        typeof appUser?.id !== "string" ||
        !Array.isArray(nodes)
      ) {
        throw new LinearQueryError("agentSessionActivities", "shape");
      }
      const issueIdentifier = asRecord(rawSession.issue)?.identifier;
      session = {
        id: rawSession.id,
        appUserId: appUser.id,
        ...(typeof issueIdentifier === "string" ? { issueIdentifier } : {}),
      };

      let reachedWatermark = false;
      for (const rawNode of nodes) {
        const activity = parseAgentActivity(rawNode);
        if (options.processedThrough !== undefined) {
          const activityTime = Date.parse(activity.createdAt);
          const watermarkTime = Date.parse(options.processedThrough.createdAt);
          if (activityTime < watermarkTime) {
            reachedWatermark = true;
            continue;
          }
          // At the watermark timestamp only the watermark activity itself is
          // known-processed. Ids do not order the connection and Date.parse
          // collapses finer precision, so a same-millisecond sibling is
          // re-offered rather than skipped: the durable semantic claim
          // deduplicates a repeat, while skipping drops the prompt forever.
          if (
            activityTime === watermarkTime &&
            activity.id === options.processedThrough.id
          ) {
            continue;
          }
        }
        if (
          Date.parse(activity.createdAt) < Date.parse(options.lookbackAfter)
        ) {
          continue;
        }
        activities.push(activity);
      }
      if (!pageInfo.hasNextPage || reachedWatermark) {
        break;
      }
      after = nextPageCursor(
        pageInfo,
        seenCursors,
        nodes.length,
        "agentSessionActivities",
      );
    }

    activities.sort(compareActivityCursor);
    return { ...session!, activities };
  }

  private async getAccessToken(signal?: AbortSignal): Promise<string> {
    if (typeof this.tokenSource === "string") {
      signal?.throwIfAborted();
      return this.tokenSource;
    }
    return await this.tokenSource.getAccessToken(signal);
  }

  private async queryGraphQL(
    query: string,
    variables: Record<string, unknown>,
    operation: string,
    signal?: AbortSignal,
  ): Promise<unknown> {
    const accessToken = await this.getAccessToken(signal);
    let response = await this.postQuery(accessToken, query, variables, signal);
    if (response.status === 401 && typeof this.tokenSource !== "string") {
      await discardResponseBody(response);
      const refreshedAccessToken =
        await this.tokenSource.refreshAfterUnauthorized(accessToken, signal);
      response = await this.postQuery(
        refreshedAccessToken,
        query,
        variables,
        signal,
      );
    }
    if (!response.ok) {
      await discardResponseBody(response);
      throw new LinearQueryError(operation, "http");
    }
    const json = (await response.json()) as unknown;
    const errors = asRecord(json)?.errors;
    if (Array.isArray(errors) && errors.length > 0) {
      throw new LinearQueryError(operation, "graphql");
    }
    return json;
  }

  private postQuery(
    accessToken: string,
    query: string,
    variables: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<Response> {
    return this.fetchFn(LINEAR_GRAPHQL_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({ query, variables }),
      ...(signal !== undefined ? { signal } : {}),
    });
  }

  private postActivity(
    accessToken: string,
    agentSessionId: string,
    content: AgentActivityContent,
    options: {
      activityId?: string;
      ephemeral?: boolean;
      signal?: AbortSignal;
    },
  ): Promise<Response> {
    return this.fetchFn(LINEAR_GRAPHQL_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({
        query: AGENT_ACTIVITY_CREATE_MUTATION,
        variables: {
          input: {
            ...(options.activityId !== undefined ? { id: options.activityId } : {}),
            agentSessionId,
            content,
            ...(options.ephemeral !== undefined
              ? { ephemeral: options.ephemeral }
              : {}),
          },
        },
      }),
      ...(options.signal !== undefined ? { signal: options.signal } : {}),
    });
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function parsePageInfo(value: unknown, operation: string): PageInfo {
  const pageInfo = asRecord(value);
  if (
    typeof pageInfo?.hasNextPage !== "boolean" ||
    (pageInfo.endCursor !== null && typeof pageInfo.endCursor !== "string")
  ) {
    throw new LinearQueryError(operation, "shape");
  }
  return {
    hasNextPage: pageInfo.hasNextPage,
    endCursor: pageInfo.endCursor as string | null,
  };
}

function nextPageCursor(
  pageInfo: PageInfo,
  seen: Set<string>,
  nodeCount: number,
  operation: string,
): string {
  const cursor = pageInfo.endCursor;
  if (
    !pageInfo.hasNextPage ||
    cursor === null ||
    cursor === "" ||
    nodeCount === 0 ||
    seen.has(cursor)
  ) {
    throw new LinearQueryError(operation, "shape");
  }
  seen.add(cursor);
  return cursor;
}

const ACTIVITY_TYPENAMES: Record<string, ReconciledAgentActivityType> = {
  AgentActivityActionContent: "action",
  AgentActivityElicitationContent: "elicitation",
  AgentActivityErrorContent: "error",
  AgentActivityPromptContent: "prompt",
  AgentActivityResponseContent: "response",
  AgentActivityThoughtContent: "thought",
};

function parseAgentActivity(value: unknown): ReconciledAgentActivity {
  const activity = asRecord(value);
  const user = asRecord(activity?.user);
  const content = asRecord(activity?.content);
  const type =
    typeof content?.__typename === "string"
      ? ACTIVITY_TYPENAMES[content.__typename]
      : undefined;
  if (
    typeof activity?.id !== "string" ||
    typeof activity.createdAt !== "string" ||
    !Number.isFinite(Date.parse(activity.createdAt)) ||
    type === undefined
  ) {
    throw new LinearQueryError("agentSessionActivities", "shape");
  }
  if (
    type === "prompt" &&
    (typeof user?.id !== "string" || typeof content?.body !== "string")
  ) {
    throw new LinearQueryError("agentSessionActivities", "shape");
  }
  return {
    id: activity.id,
    createdAt: activity.createdAt,
    type,
    ...(typeof user?.id === "string" ? { userId: user.id } : {}),
    ...(typeof activity.signal === "string" ? { signal: activity.signal } : {}),
    ...(type === "prompt" ? { body: content!.body as string } : {}),
  };
}

function compareActivityCursor(
  left: ReconciliationCursor,
  right: ReconciliationCursor,
): number {
  const byCreatedAt = Date.parse(left.createdAt) - Date.parse(right.createdAt);
  return byCreatedAt === 0 ? left.id.localeCompare(right.id) : byCreatedAt;
}

function validateTimestamp(value: string, name: string): void {
  if (!Number.isFinite(Date.parse(value))) {
    throw new Error(`${name} must be an ISO-8601 timestamp`);
  }
}
