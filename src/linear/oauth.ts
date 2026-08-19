import { randomUUID } from "node:crypto";
import { promises as fsPromises } from "node:fs";
import * as path from "node:path";
import { discardResponseBody, type FetchFn } from "./client.js";

const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";

export interface LinearOAuthTokenResponse {
  access_token?: string;
  refresh_token?: string;
  expires_in?: number;
}

interface StoredOAuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresAt: string;
}

export interface LinearOAuthTokenManagerOptions {
  clientId: string;
  clientSecret: string;
  initialAccessToken: string;
  storePath: string;
  fetchFn?: FetchFn;
}

/**
 * Owns Linear's rotating OAuth token pair.
 *
 * Linear access tokens expire after 24 hours. Each refresh consumes the
 * current refresh token and returns a replacement pair, so the pair is
 * persisted atomically after both authorization-code exchange and refresh.
 */
export class LinearOAuthTokenManager {
  private accessToken: string;
  private refreshToken: string | undefined;
  private expiresAt: string | undefined;
  private loaded = false;
  private refreshPromise: Promise<string> | undefined;
  private readonly fetchFn: FetchFn;

  constructor(private readonly options: LinearOAuthTokenManagerOptions) {
    this.accessToken = options.initialAccessToken;
    this.fetchFn = options.fetchFn ?? globalThis.fetch;
  }

  async load(): Promise<void> {
    if (this.loaded) {
      return;
    }

    try {
      const stored = JSON.parse(
        await fsPromises.readFile(this.options.storePath, "utf8"),
      ) as Partial<StoredOAuthTokens>;
      if (
        typeof stored.accessToken !== "string" ||
        typeof stored.refreshToken !== "string" ||
        typeof stored.expiresAt !== "string"
      ) {
        throw new Error("Linear OAuth token store has an invalid shape");
      }
      this.accessToken = stored.accessToken;
      this.refreshToken = stored.refreshToken;
      this.expiresAt = stored.expiresAt;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
        throw err;
      }
    }
    this.loaded = true;
  }

  async getAccessToken(signal?: AbortSignal): Promise<string> {
    await waitForPromise(this.load(), signal);
    return this.accessToken;
  }

  async hasRefreshToken(): Promise<boolean> {
    await this.load();
    return this.refreshToken !== undefined;
  }

  async install(response: LinearOAuthTokenResponse): Promise<void> {
    await this.load();
    const tokens = parseTokenResponse(response);
    this.accessToken = tokens.accessToken;
    this.refreshToken = tokens.refreshToken;
    this.expiresAt = tokens.expiresAt;
    await this.persist(tokens);
  }

  async refreshAfterUnauthorized(
    failedAccessToken: string,
    signal?: AbortSignal,
  ): Promise<string> {
    await waitForPromise(this.load(), signal);

    // Another caller may already have rotated the pair while this request was
    // in flight. Reuse that access token instead of consuming the new refresh
    // token a second time.
    if (failedAccessToken !== this.accessToken) {
      return this.accessToken;
    }

    if (this.refreshPromise === undefined) {
      // The shared refresh is intentionally not cancelled with an individual
      // caller. Linear may already have consumed the rotating refresh token,
      // so the replacement pair must still be installed and persisted.
      this.refreshPromise = this.refresh().finally(() => {
        this.refreshPromise = undefined;
      });
    }
    return await waitForPromise(this.refreshPromise, signal);
  }

  private async refresh(): Promise<string> {
    if (this.refreshToken === undefined) {
      throw new Error(
        "Linear OAuth access expired and no refresh token is stored; authorize the app once more",
      );
    }

    const body = new URLSearchParams({
      refresh_token: this.refreshToken,
      grant_type: "refresh_token",
      client_id: this.options.clientId,
      client_secret: this.options.clientSecret,
    });
    const response = await this.fetchFn(LINEAR_TOKEN_URL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
    });
    if (!response.ok) {
      await discardResponseBody(response);
      throw new Error(
        `Linear OAuth token refresh failed: ${response.status} ${response.statusText}`,
      );
    }

    await this.install((await response.json()) as LinearOAuthTokenResponse);
    return this.accessToken;
  }

  private async persist(tokens: StoredOAuthTokens): Promise<void> {
    const directory = path.dirname(this.options.storePath);
    await fsPromises.mkdir(directory, { recursive: true });
    const tempPath = `${this.options.storePath}.${randomUUID()}.tmp`;
    try {
      await fsPromises.writeFile(tempPath, `${JSON.stringify(tokens, null, 2)}\n`, {
        mode: 0o600,
      });
      await fsPromises.rename(tempPath, this.options.storePath);
    } finally {
      await fsPromises.rm(tempPath, { force: true });
    }
  }
}

function waitForPromise<T>(
  promise: Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  if (signal === undefined) {
    return promise;
  }

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (complete: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      signal.removeEventListener("abort", onAbort);
      complete();
    };
    const onAbort = (): void => {
      finish(() => reject(abortReason(signal)));
    };

    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => finish(() => resolve(value)),
      (error: unknown) => finish(() => reject(error)),
    );
    if (signal.aborted) {
      onAbort();
    }
  });
}

function abortReason(signal: AbortSignal): unknown {
  if (signal.reason !== undefined) {
    return signal.reason;
  }
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

function parseTokenResponse(response: LinearOAuthTokenResponse): StoredOAuthTokens {
  if (typeof response.access_token !== "string" || response.access_token === "") {
    throw new Error("Linear OAuth token response missing access_token");
  }
  if (typeof response.refresh_token !== "string" || response.refresh_token === "") {
    throw new Error("Linear OAuth token response missing refresh_token");
  }
  if (
    typeof response.expires_in !== "number" ||
    !Number.isFinite(response.expires_in) ||
    response.expires_in < 0
  ) {
    throw new Error("Linear OAuth token response missing valid expires_in");
  }

  return {
    accessToken: response.access_token,
    refreshToken: response.refresh_token,
    expiresAt: new Date(Date.now() + response.expires_in * 1000).toISOString(),
  };
}
