import { randomUUID } from "node:crypto";
import { promises as fsPromises } from "node:fs";
import * as path from "node:path";
import { discardResponseBody, type FetchFn } from "./client.js";

const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";
const INITIAL_PERSISTENCE_RETRY_DELAY_MS = 100;
const MAX_PERSISTENCE_RETRY_DELAY_MS = 30_000;
const DEFAULT_SHUTDOWN_FLUSH_TIMEOUT_MS = 1_000;

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

interface PendingOAuthTokens {
  generation: number;
  tokens: StoredOAuthTokens;
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
  private pendingTokens: PendingOAuthTokens | undefined;
  private mutationGeneration = 0;
  private mutationTail: Promise<void> = Promise.resolve();
  private directoryChainSynced = false;
  private loaded = false;
  private loadPromise: Promise<void> | undefined;
  private refreshPromise: Promise<string> | undefined;
  private persistenceRetryTimer: ReturnType<typeof setTimeout> | undefined;
  private persistenceRetryDelayMs = INITIAL_PERSISTENCE_RETRY_DELAY_MS;
  private readonly fetchFn: FetchFn;

  constructor(private readonly options: LinearOAuthTokenManagerOptions) {
    this.accessToken = options.initialAccessToken;
    this.fetchFn = options.fetchFn ?? globalThis.fetch;
  }

  async load(): Promise<void> {
    if (this.loaded) {
      return;
    }

    if (this.loadPromise === undefined) {
      this.loadPromise = this.loadFromStore().finally(() => {
        if (!this.loaded) {
          this.loadPromise = undefined;
        }
      });
    }
    await this.loadPromise;
  }

  private async loadFromStore(): Promise<void> {
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

  async getAccessToken(): Promise<string> {
    await this.load();
    return this.accessToken;
  }

  async hasRefreshToken(): Promise<boolean> {
    await this.load();
    return this.refreshToken !== undefined;
  }

  async flushPendingPersistence(
    timeoutMs = DEFAULT_SHUTDOWN_FLUSH_TIMEOUT_MS,
  ): Promise<void> {
    const flush = (async () => {
      await this.refreshPromise?.catch(() => undefined);
      this.clearPendingRetry();
      await this.serializeMutation(async () => {
        const pending = this.pendingTokens;
        if (pending !== undefined) {
          await this.persistAndAdopt(pending);
        }
      });
    })();
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        flush,
        new Promise<never>((_resolve, reject) => {
          timeout = setTimeout(
            () => reject(new Error("Linear OAuth shutdown flush timed out")),
            timeoutMs,
          );
          timeout.unref?.();
        }),
      ]);
    } finally {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }
    }
  }

  async install(response: LinearOAuthTokenResponse): Promise<void> {
    await this.load();
    const tokens = parseTokenResponse(response);
    await this.serializeMutation(async () => {
      const pending = this.stagePending(tokens);
      await this.persistAndAdopt(pending);
    });
  }

  async refreshAfterUnauthorized(failedAccessToken: string): Promise<string> {
    await this.load();

    if (this.refreshPromise === undefined) {
      const refreshPromise = this.refresh(failedAccessToken);
      const sharedRefresh = refreshPromise.finally(() => {
        if (this.refreshPromise === sharedRefresh) {
          this.refreshPromise = undefined;
        }
      });
      this.refreshPromise = sharedRefresh;
    }
    return this.refreshPromise;
  }

  private async refresh(failedAccessToken: string): Promise<string> {
    const decision = await this.serializeMutation(async () => {
      if (this.pendingTokens !== undefined) {
        await this.persistAndAdopt(this.pendingTokens);
        return { kind: "return" as const, accessToken: this.accessToken };
      }

      // Another caller may already have rotated the pair while this request
      // was in flight. Reuse that durable access token instead of consuming
      // its refresh token a second time.
      if (failedAccessToken !== this.accessToken) {
        return { kind: "return" as const, accessToken: this.accessToken };
      }

      if (this.refreshToken === undefined) {
        throw new Error(
          "Linear OAuth access expired and no refresh token is stored; authorize the app once more",
        );
      }
      return {
        kind: "refresh" as const,
        generation: this.mutationGeneration,
        refreshToken: this.refreshToken,
      };
    });

    if (decision.kind === "return") {
      return decision.accessToken;
    }

    const body = new URLSearchParams({
      refresh_token: decision.refreshToken,
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

    const tokens = parseTokenResponse(
      (await response.json()) as LinearOAuthTokenResponse,
    );
    return this.serializeMutation(async () => {
      if (
        this.mutationGeneration !== decision.generation ||
        this.refreshToken !== decision.refreshToken ||
        this.pendingTokens !== undefined
      ) {
        return this.accessToken;
      }

      const pending = this.stagePending(tokens);
      await this.persistAndAdopt(pending);
      return this.accessToken;
    });
  }

  private serializeMutation<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutationTail.then(operation);
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private stagePending(tokens: StoredOAuthTokens): PendingOAuthTokens {
    const pending = {
      generation: this.mutationGeneration + 1,
      tokens,
    };
    this.mutationGeneration = pending.generation;
    this.pendingTokens = pending;
    return pending;
  }

  private async persistAndAdopt(pending: PendingOAuthTokens): Promise<boolean> {
    if (!this.isCurrentPending(pending)) {
      this.schedulePendingRetry();
      return false;
    }

    try {
      await this.persist(pending.tokens);
    } catch (err) {
      this.schedulePendingRetry();
      throw err;
    }

    if (!this.isCurrentPending(pending)) {
      this.schedulePendingRetry();
      return false;
    }

    this.accessToken = pending.tokens.accessToken;
    this.refreshToken = pending.tokens.refreshToken;
    this.expiresAt = pending.tokens.expiresAt;
    this.pendingTokens = undefined;
    this.clearPendingRetry();
    return true;
  }

  private isCurrentPending(pending: PendingOAuthTokens): boolean {
    return (
      this.pendingTokens === pending &&
      this.mutationGeneration === pending.generation
    );
  }

  private schedulePendingRetry(): void {
    if (
      this.pendingTokens === undefined ||
      this.persistenceRetryTimer !== undefined
    ) {
      return;
    }

    const delayMs = this.persistenceRetryDelayMs;
    const timer = setTimeout(() => {
      if (this.persistenceRetryTimer === timer) {
        this.persistenceRetryTimer = undefined;
      }
      void this.serializeMutation(async () => {
        const pending = this.pendingTokens;
        if (pending === undefined) {
          return;
        }
        try {
          await this.persistAndAdopt(pending);
        } catch {
          // persistAndAdopt schedules the next bounded retry.
        }
      }).catch(() => undefined);
    }, delayMs);
    timer.unref?.();
    this.persistenceRetryTimer = timer;
    this.persistenceRetryDelayMs = Math.min(
      delayMs * 2,
      MAX_PERSISTENCE_RETRY_DELAY_MS,
    );
  }

  private clearPendingRetry(): void {
    if (this.persistenceRetryTimer !== undefined) {
      clearTimeout(this.persistenceRetryTimer);
      this.persistenceRetryTimer = undefined;
    }
    this.persistenceRetryDelayMs = INITIAL_PERSISTENCE_RETRY_DELAY_MS;
  }

  private async persist(tokens: StoredOAuthTokens): Promise<void> {
    const directory = path.resolve(path.dirname(this.options.storePath));
    if (!this.directoryChainSynced) {
      await ensureDirectoryDurable(directory);
      this.directoryChainSynced = true;
    }
    const tempPath = `${this.options.storePath}.${randomUUID()}.tmp`;
    let tempFile: Awaited<ReturnType<typeof fsPromises.open>> | undefined;
    let persistenceFailed = false;
    let persistenceError: unknown;
    try {
      tempFile = await fsPromises.open(tempPath, "wx", 0o600);
      await tempFile.writeFile(`${JSON.stringify(tokens, null, 2)}\n`, "utf8");
      await tempFile.chmod(0o600);
      await tempFile.sync();
      await tempFile.close();
      tempFile = undefined;
      await fsPromises.rename(tempPath, this.options.storePath);
      await syncDirectory(directory);
    } catch (err) {
      persistenceFailed = true;
      persistenceError = err;
    }

    let cleanupFailed = false;
    let cleanupError: unknown;
    if (tempFile !== undefined) {
      try {
        await tempFile.close();
      } catch (err) {
        cleanupFailed = true;
        cleanupError = err;
      }
    }
    try {
      await fsPromises.rm(tempPath, { force: true });
    } catch (err) {
      if (!cleanupFailed) {
        cleanupFailed = true;
        cleanupError = err;
      }
    }

    if (persistenceFailed) {
      throw persistenceError;
    }
    if (cleanupFailed) {
      throw cleanupError;
    }
  }
}

async function ensureDirectoryDurable(directory: string): Promise<void> {
  const missingDirectories: string[] = [];
  let cursor = directory;

  while (true) {
    try {
      const existing = await fsPromises.stat(cursor);
      if (!existing.isDirectory()) {
        throw new Error(`Linear OAuth token store parent is not a directory: ${cursor}`);
      }
      break;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
        throw err;
      }
      missingDirectories.push(cursor);
      const parent = path.dirname(cursor);
      if (parent === cursor) {
        throw err;
      }
      cursor = parent;
    }
  }

  for (const missing of missingDirectories.reverse()) {
    try {
      await fsPromises.mkdir(missing, { mode: 0o700 });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") {
        throw err;
      }
      const existing = await fsPromises.stat(missing);
      if (!existing.isDirectory()) {
        throw err;
      }
    }
  }

  const directoryChain: string[] = [];
  cursor = directory;
  while (true) {
    directoryChain.unshift(cursor);
    const parent = path.dirname(cursor);
    if (parent === cursor) {
      break;
    }
    cursor = parent;
  }
  for (const ancestor of directoryChain) {
    await syncDirectory(ancestor);
  }
}

async function syncDirectory(directory: string): Promise<void> {
  const handle = await fsPromises.open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
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
