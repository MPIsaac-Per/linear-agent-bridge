import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import * as path from "node:path";

export const DEFAULT_RECEIPT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const DEFAULT_RECEIPT_MAX_ENTRIES = 10_000;

const MAX_IDENTIFIER_LENGTH = 256;
const MAX_EXECUTION_ID_LENGTH = 512;
const MAX_ACTIVITY_KEY_LENGTH = 128;
const MAX_ACTIVITY_IDS_PER_CLAIM = 10_000;
const LOCK_RETRY_MS = 10;
const LOCK_TIMEOUT_MS = 5_000;
const STALE_LOCK_MS = 30_000;

export type IngressAction = "created" | "prompted";
export type IngressStatus =
  | "received"
  | "claimed"
  | "completed"
  | "failed"
  | "superseded";

export type ReceiptDisposition =
  | "received"
  | "claimed"
  | "duplicate"
  | "superseded"
  | "ambiguous";

export type ReceiptErrorClass =
  | "AmbiguousDispatch"
  | "IngressPersistenceError"
  | "RuntimeExecutionError"
  | "RuntimeTimeout"
  | "WebhookProcessingError";

export interface ReceiptOutcome {
  httpStatus: 200 | 503;
  result:
    | "retry"
    | "accepted"
    | "dispatch_started"
    | "not_dispatched"
    | "completed"
    | "processing_failed";
  disposition: ReceiptDisposition;
  errorClass?: ReceiptErrorClass | undefined;
}

export interface IngressEventIdentity {
  webhookId: string;
  executionId: string;
  linearSessionId: string;
  action: IngressAction;
}

export interface IngressReceipt extends IngressEventIdentity {
  status: IngressStatus;
  ownerId?: string | undefined;
  receivedAt: string;
  updatedAt: string;
  claimedAt?: string | undefined;
  completedAt?: string | undefined;
  failedAt?: string | undefined;
  supersededAt?: string | undefined;
  supersededByWebhookId?: string | undefined;
  dispatchStartedAt?: string | undefined;
  outcome: ReceiptOutcome;
}

export interface IngressClaim {
  executionId: string;
  webhookId: string;
  linearSessionId: string;
  action: IngressAction;
  status: "claimed" | "completed" | "failed";
  ownerId: string;
  claimedAt: string;
  updatedAt: string;
  dispatchStartedAt?: string | undefined;
  activityIds: Record<string, string>;
}

export type ClaimEventResult =
  | { disposition: "claimed"; receipt: IngressReceipt }
  | { disposition: "duplicate"; receipt: IngressReceipt }
  | { disposition: "superseded"; receipt: IngressReceipt }
  | { disposition: "ambiguous"; receipt: IngressReceipt };

export interface BridgeStateStore {
  claimEvent(identity: IngressEventIdentity): Promise<ClaimEventResult>;
  markDispatchStarted(webhookId: string): Promise<void>;
  completeEvent(webhookId: string): Promise<void>;
  failEvent(webhookId: string, errorClass?: ReceiptErrorClass): Promise<void>;
  getReceipt(webhookId: string): Promise<IngressReceipt | undefined>;
  getClaim(executionId: string): Promise<IngressClaim | undefined>;
  getOrCreateActivityId(executionId: string, activityKey: string): Promise<string>;
}

interface PersistedBridgeState {
  version: 1;
  receipts: Record<string, IngressReceipt>;
  claims: Record<string, IngressClaim>;
}

export interface JsonBridgeStateStoreOptions {
  maxEntries?: number;
  retentionMs?: number;
  now?: () => number;
  ownerId?: string;
}

export class ClaimOwnershipError extends Error {
  constructor(webhookId: string) {
    super(`Claim ownership was lost for webhookId "${webhookId}"`);
    this.name = "ClaimOwnershipError";
  }
}

/**
 * Durable ingress receipt, semantic claim, and outbound idempotency state.
 *
 * Mutations take an inter-process file lock and replace the JSON target with a
 * same-directory rename. An event is written once as `received` and again as
 * `claimed` before claimEvent resolves, so the HTTP layer can safely return
 * 200 only after both durable lifecycle steps have completed.
 */
export class JsonBridgeStateStore implements BridgeStateStore {
  private readonly maxEntries: number;
  private readonly retentionMs: number;
  private readonly now: () => number;
  private readonly ownerId: string;
  private mutationTail: Promise<void> = Promise.resolve();

  constructor(
    private readonly statePath: string,
    options: JsonBridgeStateStoreOptions = {},
  ) {
    this.maxEntries = options.maxEntries ?? DEFAULT_RECEIPT_MAX_ENTRIES;
    this.retentionMs = options.retentionMs ?? DEFAULT_RECEIPT_RETENTION_MS;
    this.now = options.now ?? Date.now;
    this.ownerId = options.ownerId ?? randomUUID();

    if (!Number.isInteger(this.maxEntries) || this.maxEntries <= 0) {
      throw new Error("maxEntries must be a positive integer");
    }
    if (!Number.isInteger(this.retentionMs) || this.retentionMs <= 0) {
      throw new Error("retentionMs must be a positive integer");
    }
    validateIdentifier(this.ownerId, "ownerId");
  }

  async claimEvent(identity: IngressEventIdentity): Promise<ClaimEventResult> {
    validateIdentity(identity);

    return await this.mutate(async () => {
      const state = await this.readState();
      this.prune(state);
      const existingReceipt = state.receipts[identity.webhookId];
      if (existingReceipt !== undefined) {
        assertSameIdentity(existingReceipt, identity);
        if (existingReceipt.status !== "received") {
          return await this.resolveExistingReceipt(state, existingReceipt);
        }
      } else {
        const timestamp = this.timestamp();
        state.receipts[identity.webhookId] = {
          ...identity,
          status: "received",
          receivedAt: timestamp,
          updatedAt: timestamp,
          outcome: {
            httpStatus: 503,
            result: "retry",
            disposition: "received",
            errorClass: "IngressPersistenceError",
          },
        };
        await this.writeState(state);
      }

      const receipt = state.receipts[identity.webhookId]!;
      const existingClaim = state.claims[identity.executionId];
      const timestamp = this.timestamp();
      if (existingClaim !== undefined) {
        if (
          existingClaim.status === "claimed" &&
          existingClaim.ownerId !== this.ownerId &&
          existingClaim.dispatchStartedAt === undefined
        ) {
          const priorReceipt = state.receipts[existingClaim.webhookId];
          if (
            priorReceipt !== undefined &&
            priorReceipt.webhookId !== receipt.webhookId
          ) {
            priorReceipt.status = "superseded";
            priorReceipt.supersededAt = timestamp;
            priorReceipt.supersededByWebhookId = receipt.webhookId;
            priorReceipt.updatedAt = timestamp;
            priorReceipt.outcome = {
              httpStatus: 200,
              result: "not_dispatched",
              disposition: "superseded",
            };
          }

          receipt.status = "claimed";
          receipt.ownerId = this.ownerId;
          receipt.claimedAt = timestamp;
          receipt.updatedAt = timestamp;
          receipt.outcome = acceptedOutcome();
          existingClaim.webhookId = receipt.webhookId;
          existingClaim.ownerId = this.ownerId;
          existingClaim.claimedAt = timestamp;
          existingClaim.updatedAt = timestamp;
          delete existingClaim.dispatchStartedAt;
          await this.writeState(state);
          return { disposition: "claimed", receipt };
        }

        if (
          existingClaim.status === "claimed" &&
          existingClaim.ownerId !== this.ownerId &&
          existingClaim.dispatchStartedAt !== undefined
        ) {
          receipt.status =
            existingClaim.webhookId === identity.webhookId
              ? "claimed"
              : "superseded";
          if (receipt.status === "superseded") {
            receipt.supersededAt = timestamp;
            receipt.supersededByWebhookId = existingClaim.webhookId;
          }
          receipt.updatedAt = timestamp;
          receipt.outcome = ambiguousOutcome();
          await this.writeState(state);
          return { disposition: "ambiguous", receipt };
        }

        receipt.status = "superseded";
        receipt.supersededAt = timestamp;
        receipt.supersededByWebhookId = existingClaim.webhookId;
        receipt.updatedAt = timestamp;
        receipt.outcome = {
          httpStatus: 200,
          result: "not_dispatched",
          disposition: "superseded",
        };
        this.prune(state);
        await this.writeState(state);
        return { disposition: "superseded", receipt };
      }

      receipt.status = "claimed";
      receipt.ownerId = this.ownerId;
      receipt.claimedAt = timestamp;
      receipt.updatedAt = timestamp;
      receipt.outcome = acceptedOutcome();
      state.claims[identity.executionId] = {
        executionId: identity.executionId,
        webhookId: identity.webhookId,
        linearSessionId: identity.linearSessionId,
        action: identity.action,
        status: "claimed",
        ownerId: this.ownerId,
        claimedAt: timestamp,
        updatedAt: timestamp,
        activityIds: {},
      };
      this.prune(state);
      await this.writeState(state);
      return { disposition: "claimed", receipt };
    });
  }

  markDispatchStarted(webhookId: string): Promise<void> {
    validateIdentifier(webhookId, "webhookId");

    return this.mutate(async () => {
      const state = await this.readState();
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (claim.dispatchStartedAt !== undefined) {
        return;
      }

      const timestamp = this.timestamp();
      claim.dispatchStartedAt = timestamp;
      claim.updatedAt = timestamp;
      receipt.dispatchStartedAt = timestamp;
      receipt.updatedAt = timestamp;
      receipt.outcome = {
        httpStatus: 200,
        result: "dispatch_started",
        disposition: "claimed",
      };
      await this.writeState(state);
    });
  }

  completeEvent(webhookId: string): Promise<void> {
    return this.terminalize(webhookId, "completed");
  }

  failEvent(
    webhookId: string,
    errorClass: ReceiptErrorClass = "WebhookProcessingError",
  ): Promise<void> {
    return this.terminalize(webhookId, "failed", errorClass);
  }

  async getReceipt(webhookId: string): Promise<IngressReceipt | undefined> {
    validateIdentifier(webhookId, "webhookId");
    return (await this.readState()).receipts[webhookId];
  }

  async getClaim(executionId: string): Promise<IngressClaim | undefined> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    return (await this.readState()).claims[executionId];
  }

  getOrCreateActivityId(
    executionId: string,
    activityKey: string,
  ): Promise<string> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    validateIdentifier(activityKey, "activityKey", MAX_ACTIVITY_KEY_LENGTH);

    return this.mutate(async () => {
      const state = await this.readState();
      const claim = state.claims[executionId];
      if (claim === undefined) {
        throw new Error(`No ingress claim for executionId "${executionId}"`);
      }
      if (claim.ownerId !== this.ownerId) {
        throw new ClaimOwnershipError(claim.webhookId);
      }
      if (claim.dispatchStartedAt === undefined) {
        throw new Error(`Dispatch has not started for executionId "${executionId}"`);
      }
      const existing = claim.activityIds[activityKey];
      if (existing !== undefined) {
        return existing;
      }
      if (Object.keys(claim.activityIds).length >= MAX_ACTIVITY_IDS_PER_CLAIM) {
        throw new Error(
          `Too many outbound activity ids for executionId "${executionId}"`,
        );
      }

      const activityId = randomUUID();
      claim.activityIds[activityKey] = activityId;
      claim.updatedAt = this.timestamp();
      await this.writeState(state);
      return activityId;
    });
  }

  private terminalize(
    webhookId: string,
    status: "completed" | "failed",
    errorClass?: ReceiptErrorClass,
  ): Promise<void> {
    validateIdentifier(webhookId, "webhookId");

    return this.mutate(async () => {
      const state = await this.readState();
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (claim.dispatchStartedAt === undefined) {
        throw new Error(`Dispatch has not started for webhookId "${webhookId}"`);
      }

      const timestamp = this.timestamp();
      receipt.status = status;
      receipt.updatedAt = timestamp;
      claim.status = status;
      claim.updatedAt = timestamp;
      if (status === "completed") {
        receipt.completedAt = timestamp;
        delete receipt.failedAt;
        receipt.outcome = {
          httpStatus: 200,
          result: "completed",
          disposition: "claimed",
        };
      } else {
        receipt.failedAt = timestamp;
        delete receipt.completedAt;
        receipt.outcome = {
          httpStatus: 200,
          result: "processing_failed",
          disposition: "claimed",
          errorClass: errorClass ?? "WebhookProcessingError",
        };
      }
      this.prune(state);
      await this.writeState(state);
    });
  }

  private async resolveExistingReceipt(
    state: PersistedBridgeState,
    receipt: IngressReceipt,
  ): Promise<ClaimEventResult> {
    const claim = state.claims[receipt.executionId];
    const timestamp = this.timestamp();
    if (
      receipt.status === "claimed" &&
      claim?.status === "claimed" &&
      claim.webhookId === receipt.webhookId
    ) {
      if (claim.ownerId !== this.ownerId) {
        if (claim.dispatchStartedAt === undefined) {
          claim.ownerId = this.ownerId;
          claim.claimedAt = timestamp;
          claim.updatedAt = timestamp;
          receipt.ownerId = this.ownerId;
          receipt.claimedAt = timestamp;
          receipt.updatedAt = timestamp;
          receipt.outcome = acceptedOutcome();
          await this.writeState(state);
          return { disposition: "claimed", receipt };
        }

        receipt.updatedAt = timestamp;
        receipt.outcome = ambiguousOutcome();
        await this.writeState(state);
        return { disposition: "ambiguous", receipt };
      }

      receipt.updatedAt = timestamp;
      receipt.outcome = {
        httpStatus: 200,
        result: "not_dispatched",
        disposition: "duplicate",
      };
      await this.writeState(state);
      return { disposition: "duplicate", receipt };
    }

    if (receipt.outcome.disposition === "ambiguous") {
      return { disposition: "ambiguous", receipt };
    }
    receipt.updatedAt = timestamp;
    receipt.outcome = {
      httpStatus: 200,
      result: "not_dispatched",
      disposition: receipt.status === "superseded" ? "superseded" : "duplicate",
    };
    await this.writeState(state);
    return {
      disposition: receipt.status === "superseded" ? "superseded" : "duplicate",
      receipt,
    };
  }

  private ownedActiveClaim(
    state: PersistedBridgeState,
    webhookId: string,
  ): { claim: IngressClaim; receipt: IngressReceipt } {
    const receipt = state.receipts[webhookId];
    if (receipt === undefined || receipt.status !== "claimed") {
      throw new ClaimOwnershipError(webhookId);
    }
    const claim = state.claims[receipt.executionId];
    if (
      claim === undefined ||
      claim.webhookId !== webhookId ||
      claim.status !== "claimed" ||
      claim.ownerId !== this.ownerId
    ) {
      throw new ClaimOwnershipError(webhookId);
    }
    return { claim, receipt };
  }

  private mutate<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutationTail.then(() => this.withFileLock(operation));
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async withFileLock<T>(operation: () => Promise<T>): Promise<T> {
    const directory = path.dirname(this.statePath);
    const lockPath = `${this.statePath}.lock`;
    await fs.mkdir(directory, { recursive: true });
    const startedAt = Date.now();
    let handle: Awaited<ReturnType<typeof fs.open>> | undefined;

    while (handle === undefined) {
      try {
        handle = await fs.open(lockPath, "wx", 0o600);
      } catch (error) {
        if (!isNodeError(error, "EEXIST")) {
          throw error;
        }
        await this.removeStaleLock(lockPath);
        if (Date.now() - startedAt >= LOCK_TIMEOUT_MS) {
          throw new Error(`Timed out acquiring bridge state lock: ${lockPath}`);
        }
        await delay(LOCK_RETRY_MS);
      }
    }

    try {
      return await operation();
    } finally {
      await handle.close();
      await fs.unlink(lockPath).catch((error: unknown) => {
        if (!isNodeError(error, "ENOENT")) {
          throw error;
        }
      });
    }
  }

  private async removeStaleLock(lockPath: string): Promise<void> {
    try {
      const stat = await fs.stat(lockPath);
      if (Date.now() - stat.mtimeMs > STALE_LOCK_MS) {
        await fs.unlink(lockPath);
      }
    } catch (error) {
      if (!isNodeError(error, "ENOENT")) {
        throw error;
      }
    }
  }

  private async readState(): Promise<PersistedBridgeState> {
    let raw: string;
    try {
      raw = await fs.readFile(this.statePath, "utf8");
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return emptyState();
      }
      throw error;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (error) {
      throw new Error(`Invalid bridge state JSON at ${this.statePath}`, {
        cause: error,
      });
    }
    if (!isPersistedBridgeState(parsed)) {
      throw new Error(`Invalid bridge state structure at ${this.statePath}`);
    }
    return parsed;
  }

  private async writeState(state: PersistedBridgeState): Promise<void> {
    const directory = path.dirname(this.statePath);
    await fs.mkdir(directory, { recursive: true });
    const tmpPath = path.join(
      directory,
      `.${path.basename(this.statePath)}.${randomUUID()}.tmp`,
    );

    try {
      await fs.writeFile(tmpPath, JSON.stringify(state, null, 2), {
        encoding: "utf8",
        mode: 0o600,
      });
      await fs.rename(tmpPath, this.statePath);
      await fs.chmod(this.statePath, 0o600);
    } finally {
      await fs.unlink(tmpPath).catch((error: unknown) => {
        if (!isNodeError(error, "ENOENT")) {
          throw error;
        }
      });
    }
  }

  private prune(state: PersistedBridgeState): void {
    const cutoff = this.now() - this.retentionMs;
    const terminal = Object.values(state.receipts)
      .filter((receipt) => isTerminal(receipt.status))
      .sort((left, right) => Date.parse(left.updatedAt) - Date.parse(right.updatedAt));

    for (const receipt of terminal) {
      if (Date.parse(receipt.updatedAt) < cutoff) {
        removeReceipt(state, receipt);
      }
    }

    const remainingTerminal = Object.values(state.receipts)
      .filter((receipt) => isTerminal(receipt.status))
      .sort((left, right) => Date.parse(left.updatedAt) - Date.parse(right.updatedAt));
    while (
      Object.keys(state.receipts).length > this.maxEntries &&
      remainingTerminal.length > 0
    ) {
      removeReceipt(state, remainingTerminal.shift()!);
    }
  }

  private timestamp(): string {
    return new Date(this.now()).toISOString();
  }
}

function emptyState(): PersistedBridgeState {
  return { version: 1, receipts: {}, claims: {} };
}

function acceptedOutcome(): ReceiptOutcome {
  return {
    httpStatus: 200,
    result: "accepted",
    disposition: "claimed",
  };
}

function ambiguousOutcome(): ReceiptOutcome {
  return {
    httpStatus: 200,
    result: "not_dispatched",
    disposition: "ambiguous",
    errorClass: "AmbiguousDispatch",
  };
}

function validateIdentity(identity: IngressEventIdentity): void {
  validateIdentifier(identity.webhookId, "webhookId");
  validateIdentifier(
    identity.executionId,
    "executionId",
    MAX_EXECUTION_ID_LENGTH,
  );
  validateIdentifier(identity.linearSessionId, "linearSessionId");
  if (identity.action !== "created" && identity.action !== "prompted") {
    throw new Error(`Invalid action "${String(identity.action)}"`);
  }
}

function validateIdentifier(
  value: string,
  name: string,
  maximum = MAX_IDENTIFIER_LENGTH,
): void {
  if (typeof value !== "string" || value.length === 0 || value.length > maximum) {
    throw new Error(`${name} must contain 1-${maximum} characters`);
  }
}

function assertSameIdentity(
  receipt: IngressReceipt,
  identity: IngressEventIdentity,
): void {
  if (
    receipt.executionId !== identity.executionId ||
    receipt.linearSessionId !== identity.linearSessionId ||
    receipt.action !== identity.action
  ) {
    throw new Error(`webhookId "${identity.webhookId}" was reused for another event`);
  }
}

function removeReceipt(state: PersistedBridgeState, receipt: IngressReceipt): void {
  delete state.receipts[receipt.webhookId];
  const claim = state.claims[receipt.executionId];
  if (
    claim?.webhookId === receipt.webhookId &&
    (claim.status === "completed" || claim.status === "failed")
  ) {
    delete state.claims[receipt.executionId];
  }
}

function isTerminal(status: IngressStatus): boolean {
  return status === "completed" || status === "failed" || status === "superseded";
}

function isPersistedBridgeState(value: unknown): value is PersistedBridgeState {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    record.version === 1 &&
    record.receipts !== null &&
    typeof record.receipts === "object" &&
    !Array.isArray(record.receipts) &&
    record.claims !== null &&
    typeof record.claims === "object" &&
    !Array.isArray(record.claims)
  );
}

function isNodeError(error: unknown, code: string): error is NodeJS.ErrnoException {
  return error instanceof Error && (error as NodeJS.ErrnoException).code === code;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
