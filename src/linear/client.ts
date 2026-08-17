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

/** Signature-compatible subset of the global `fetch` used for injection. */
export type FetchFn = typeof fetch;

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
    options: { ephemeral?: boolean; signal?: AbortSignal } = {},
  ): Promise<void> {
    const accessToken = await this.getAccessToken();
    let response = await this.postActivity(
      accessToken,
      agentSessionId,
      content,
      options,
    );

    if (response.status === 401 && typeof this.tokenSource !== "string") {
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
      const bodyText = await response.text();
      throw new Error(
        `Linear agentActivityCreate failed: ${response.status} ${response.statusText} — ${bodyText}`,
      );
    }

    const json = (await response.json()) as AgentActivityCreateResponse;

    if (json.errors && json.errors.length > 0) {
      throw new Error(
        `Linear agentActivityCreate GraphQL error: ${json.errors
          .map((e) => e.message)
          .join("; ")}`,
      );
    }

    if (json.data?.agentActivityCreate?.success !== true) {
      throw new Error(
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
    options: { ephemeral?: boolean; signal?: AbortSignal },
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
