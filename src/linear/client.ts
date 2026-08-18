import type { AgentActivityContent } from "../types.js";
import type { LinearOAuthTokenManager } from "./oauth.js";

const LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql";

const AGENT_ACTIVITY_CREATE_MUTATION = `
  mutation AgentActivityCreate($input: AgentActivityCreateInput!) {
    agentActivityCreate(input: $input) {
      success
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

/** Static, body-free failure surfaced to ingress orchestration and logs. */
export class LinearActivityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LinearActivityError";
  }
}

/** Signature-compatible subset of the global `fetch` used for injection. */
export type FetchFn = typeof fetch;

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
    const accessToken = await this.getAccessToken();
    let response = await this.postActivity(
      accessToken,
      agentSessionId,
      content,
      options,
    );

    if (response.status === 401 && typeof this.tokenSource !== "string") {
      await discardResponseBody(response);
      const refreshedAccessToken =
        await this.tokenSource.refreshAfterUnauthorized(accessToken);
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

  private async getAccessToken(): Promise<string> {
    return typeof this.tokenSource === "string"
      ? this.tokenSource
      : this.tokenSource.getAccessToken();
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
