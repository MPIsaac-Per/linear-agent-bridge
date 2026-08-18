import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { constants as fsConstants, promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

export const DEFAULT_RECEIPT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const DEFAULT_RECEIPT_MAX_ENTRIES = 10_000;

const MAX_IDENTIFIER_LENGTH = 256;
const MAX_EXECUTION_ID_LENGTH = 512;
const MAX_ACTIVITY_KEY_LENGTH = 128;
const MAX_ACTIVITY_IDS_PER_CLAIM = 10_000;
const LOCK_RETRY_MS = 10;
const LOCK_TIMEOUT_MS = 1_000;
const MAX_PROCESS_IDENTITY_LENGTH = 512;
const execFileAsync = promisify(execFile);
const DARWIN_PROCESS_IDENTITY_HELPER = fileURLToPath(
  new URL("../../dist/native/process_identity", import.meta.url),
);

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
  releasePreDispatchClaim(webhookId: string): Promise<boolean>;
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
  lockRetryMs?: number;
  lockTimeoutMs?: number;
  lockProcessIdentity?: (
    pid: number,
    deadline: number,
  ) => Promise<string | undefined>;
  lockBootIdentity?: (deadline: number) => Promise<string | undefined>;
  lockProcessUid?: (
    pid: number,
    deadline: number,
  ) => Promise<number | undefined>;
}

interface LegacyLockOwnerRecord {
  token: string;
  pid: number;
  hostname: string;
}

interface BootScopedLockOwnerRecord extends LegacyLockOwnerRecord {
  processIdentity: string;
}

interface LockOwnerRecord extends BootScopedLockOwnerRecord {
  uid: number;
}

export class ClaimOwnershipError extends Error {
  constructor(webhookId: string) {
    super(`Claim ownership was lost for webhookId "${webhookId}"`);
    this.name = "ClaimOwnershipError";
  }
}

export class BridgeStateLockTimeoutError extends Error {
  constructor() {
    super("Timed out acquiring bridge state lock");
    this.name = "BridgeStateLockTimeoutError";
  }
}

class DarwinProcessIdentityHelperUnavailableError extends Error {
  constructor() {
    super(
      "macOS process identity helper is unavailable; run npm run native:build (Xcode Command Line Tools are required)",
    );
    this.name = "DarwinProcessIdentityHelperUnavailableError";
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
  private readonly lockRetryMs: number;
  private readonly lockTimeoutMs: number;
  private readonly lockProcessIdentity: (
    pid: number,
    deadline: number,
  ) => Promise<string | undefined>;
  private readonly lockBootIdentity: (
    deadline: number,
  ) => Promise<string | undefined>;
  private readonly lockProcessUid: (
    pid: number,
    deadline: number,
  ) => Promise<number | undefined>;
  private currentProcessIdentityPromise: Promise<string | undefined> | undefined;
  private currentBootIdentityPromise: Promise<string | undefined> | undefined;
  // This process may reclaim a visible pre-dispatch claim only when the
  // mutation that created it never completed through lock release.
  private readonly locallyAcceptedPreDispatchClaims = new Set<string>();
  private mutationTail: Promise<void> = Promise.resolve();

  constructor(
    private readonly statePath: string,
    options: JsonBridgeStateStoreOptions = {},
  ) {
    this.maxEntries = options.maxEntries ?? DEFAULT_RECEIPT_MAX_ENTRIES;
    this.retentionMs = options.retentionMs ?? DEFAULT_RECEIPT_RETENTION_MS;
    this.now = options.now ?? Date.now;
    this.ownerId = options.ownerId ?? randomUUID();
    this.lockRetryMs = options.lockRetryMs ?? LOCK_RETRY_MS;
    this.lockTimeoutMs = options.lockTimeoutMs ?? LOCK_TIMEOUT_MS;
    this.lockProcessIdentity =
      options.lockProcessIdentity ?? defaultLockProcessIdentity;
    this.lockBootIdentity = options.lockBootIdentity ?? defaultLockBootIdentity;
    this.lockProcessUid = options.lockProcessUid ?? defaultLockProcessUid;

    if (!Number.isInteger(this.maxEntries) || this.maxEntries <= 0) {
      throw new Error("maxEntries must be a positive integer");
    }
    if (!Number.isInteger(this.retentionMs) || this.retentionMs <= 0) {
      throw new Error("retentionMs must be a positive integer");
    }
    if (!Number.isInteger(this.lockRetryMs) || this.lockRetryMs <= 0) {
      throw new Error("lockRetryMs must be a positive integer");
    }
    if (!Number.isInteger(this.lockTimeoutMs) || this.lockTimeoutMs <= 0) {
      throw new Error("lockTimeoutMs must be a positive integer");
    }
    validateIdentifier(this.ownerId, "ownerId");
  }

  async claimEvent(identity: IngressEventIdentity): Promise<ClaimEventResult> {
    validateIdentity(identity);

    return await this.mutateClaim(async () => {
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
        this.locallyAcceptedPreDispatchClaims.delete(webhookId);
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
      this.locallyAcceptedPreDispatchClaims.delete(webhookId);
    });
  }

  releasePreDispatchClaim(webhookId: string): Promise<boolean> {
    validateIdentifier(webhookId, "webhookId");
    this.locallyAcceptedPreDispatchClaims.delete(webhookId);

    return this.mutate(async () => {
      const state = await this.readState();
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (claim.dispatchStartedAt !== undefined) {
        return false;
      }

      const timestamp = this.timestamp();
      delete state.claims[claim.executionId];
      receipt.status = "received";
      receipt.updatedAt = timestamp;
      delete receipt.ownerId;
      delete receipt.claimedAt;
      delete receipt.dispatchStartedAt;
      receipt.outcome = {
        httpStatus: 503,
        result: "retry",
        disposition: "received",
        errorClass: "IngressPersistenceError",
      };
      await this.writeState(state);
      return true;
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

      if (
        claim.dispatchStartedAt === undefined &&
        !this.locallyAcceptedPreDispatchClaims.has(receipt.webhookId)
      ) {
        claim.claimedAt = timestamp;
        claim.updatedAt = timestamp;
        receipt.claimedAt = timestamp;
        receipt.updatedAt = timestamp;
        receipt.outcome = acceptedOutcome();
        await this.writeState(state);
        return { disposition: "claimed", receipt };
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

  private mutateClaim(
    operation: () => Promise<ClaimEventResult>,
  ): Promise<ClaimEventResult> {
    return this.mutate(operation, (result) => {
      if (result.disposition === "claimed") {
        this.locallyAcceptedPreDispatchClaims.add(result.receipt.webhookId);
      }
    });
  }

  private mutate<T>(
    operation: () => Promise<T>,
    onSuccess?: (result: T) => void,
  ): Promise<T> {
    const deadline = Date.now() + this.lockTimeoutMs;
    let started = false;
    let timeout: NodeJS.Timeout | undefined;
    const scheduled = this.mutationTail.then(() => {
      if (Date.now() >= deadline) {
        throw new BridgeStateLockTimeoutError();
      }
      started = true;
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }
      return this.withFileLock(operation, deadline).then((result) => {
        onSuccess?.(result);
        return result;
      });
    });
    this.mutationTail = scheduled.then(
      () => undefined,
      () => undefined,
    );
    return new Promise<T>((resolve, reject) => {
      timeout = setTimeout(() => {
        if (!started) {
          reject(new BridgeStateLockTimeoutError());
        }
      }, this.lockTimeoutMs);
      scheduled.then(resolve, reject).finally(() => {
        if (timeout !== undefined) {
          clearTimeout(timeout);
        }
      });
    });
  }

  private async withFileLock<T>(
    operation: () => Promise<T>,
    deadline: number,
  ): Promise<T> {
    const directory = path.dirname(this.statePath);
    const lockPath = `${this.statePath}.lock`;
    await fs.mkdir(directory, { recursive: true });
    const lockToken = randomUUID();
    const candidatePath = `${lockPath}.${lockToken}.candidate`;
    const candidateOwnerPath = path.join(candidatePath, `${lockToken}.json`);
    await fs.mkdir(candidatePath, { mode: 0o700 });
    let acquired = false;
    try {
      await this.writeLockOwner(candidateOwnerPath, lockToken, deadline);
      while (!acquired) {
        if (Date.now() >= deadline) {
          throw new BridgeStateLockTimeoutError();
        }
        try {
          await fs.rename(candidatePath, lockPath);
          acquired = true;
          if (Date.now() >= deadline) {
            throw new BridgeStateLockTimeoutError();
          }
        } catch (error) {
          if (
            !isNodeError(error, "EEXIST") &&
            !isNodeError(error, "ENOTEMPTY")
          ) {
            throw error;
          }
          await this.removeAbandonedLock(lockPath, deadline);
          if (Date.now() >= deadline) {
            throw new BridgeStateLockTimeoutError();
          }
          await delay(this.lockRetryMs);
        }
      }

      return await operation();
    } finally {
      if (acquired) {
        await this.releaseOwnedLock(lockPath, lockToken);
      }
      await fs.rm(candidatePath, { recursive: true, force: true });
    }
  }

  private async writeLockOwner(
    ownerPath: string,
    token: string,
    deadline: number,
  ): Promise<void> {
    const handle = await fs.open(ownerPath, "wx", 0o600);
    try {
      const processIdentity = await this.resolveLockProcessIdentity(
        process.pid,
        deadline,
      );
      if (
        !isValidProcessIdentity(processIdentity) ||
        parseLockProcessIdentityBoot(processIdentity) === undefined
      ) {
        throw new Error("Could not determine current process identity for state lock");
      }
      const uid = process.getuid?.();
      if (!isValidUid(uid)) {
        throw new Error("Could not determine current user identity for state lock");
      }
      assertBeforeLockDeadline(deadline);
      const owner: LockOwnerRecord = {
        token,
        pid: process.pid,
        hostname: os.hostname(),
        processIdentity,
        uid,
      };
      await handle.writeFile(`${JSON.stringify(owner)}\n`, "utf8");
      await handle.chmod(0o600);
      await handle.sync();
    } finally {
      await handle.close();
    }
  }

  private async removeAbandonedLock(
    lockPath: string,
    deadline: number,
  ): Promise<void> {
    let entries: string[];
    try {
      entries = await fs.readdir(lockPath);
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return;
      }
      throw error;
    }
    if (entries.length !== 1 || !entries[0]!.endsWith(".json")) {
      return;
    }

    const ownerPath = path.join(lockPath, entries[0]!);
    let parsed: unknown;
    try {
      parsed = JSON.parse(await fs.readFile(ownerPath, "utf8"));
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return;
      }
      return;
    }
    if (!isLegacyLockOwnerRecord(parsed)) {
      return;
    }
    const owner = parsed;
    const expectedName = `${owner.token}.json`;
    if (
      entries[0] !== expectedName ||
      owner.hostname !== os.hostname() ||
      !Number.isSafeInteger(owner.pid) ||
      owner.pid <= 0
    ) {
      return;
    }

    const bootScopedOwner = isBootScopedLockOwnerRecord(owner) ? owner : undefined;
    const recordedBootIdentity =
      bootScopedOwner === undefined
        ? undefined
        : parseLockProcessIdentityBoot(bootScopedOwner.processIdentity);
    let currentBootIdentity: string | undefined;
    if (recordedBootIdentity !== undefined) {
      try {
        currentBootIdentity = await this.resolveCurrentBootIdentity(deadline);
      } catch (error) {
        if (error instanceof BridgeStateLockTimeoutError) {
          throw error;
        }
      }
      if (
        currentBootIdentity !== undefined &&
        currentBootIdentity !== recordedBootIdentity
      ) {
        assertBeforeLockDeadline(deadline);
        await this.removeLockDirectoryOwnedBy(lockPath, owner.token);
        return;
      }
    }

    if (!isProcessAlive(owner.pid)) {
      assertBeforeLockDeadline(deadline);
      await this.removeLockDirectoryOwnedBy(lockPath, owner.token);
      return;
    }
    if (
      recordedBootIdentity === undefined ||
      currentBootIdentity === undefined
    ) {
      return;
    }
    if (!isLockOwnerRecord(owner)) {
      return;
    }

    let currentProcessUid: number | undefined;
    try {
      currentProcessUid = await this.resolveLockProcessUid(owner.pid, deadline);
    } catch (error) {
      if (error instanceof BridgeStateLockTimeoutError) {
        throw error;
      }
    }
    if (currentProcessUid !== undefined && currentProcessUid !== owner.uid) {
      assertBeforeLockDeadline(deadline);
      await this.removeLockDirectoryOwnedBy(lockPath, owner.token);
      return;
    }

    let currentProcessIdentity: string | undefined;
    try {
      currentProcessIdentity = await this.resolveLockProcessIdentity(
        owner.pid,
        deadline,
      );
    } catch (error) {
      if (error instanceof BridgeStateLockTimeoutError) {
        throw error;
      }
      return;
    }
    if (
      currentProcessIdentity === undefined ||
      currentProcessIdentity === owner.processIdentity
    ) {
      return;
    }

    assertBeforeLockDeadline(deadline);
    await this.removeLockDirectoryOwnedBy(lockPath, owner.token);
  }

  private async resolveLockProcessIdentity(
    pid: number,
    deadline: number,
  ): Promise<string | undefined> {
    assertBeforeLockDeadline(deadline);
    const isCurrentProcess = pid === process.pid;
    let lookup = isCurrentProcess
      ? this.currentProcessIdentityPromise
      : undefined;
    if (lookup === undefined) {
      lookup = Promise.resolve().then(() =>
        this.lockProcessIdentity(pid, deadline),
      );
      if (isCurrentProcess) {
        this.currentProcessIdentityPromise = lookup;
      }
    }

    try {
      const identity = await resolveBeforeLockDeadline(lookup, deadline);
      const validIdentity =
        isValidProcessIdentity(identity) &&
        parseLockProcessIdentityBoot(identity) !== undefined
          ? identity
          : undefined;
      if (
        isCurrentProcess &&
        validIdentity === undefined &&
        this.currentProcessIdentityPromise === lookup
      ) {
        this.currentProcessIdentityPromise = undefined;
      }
      return validIdentity;
    } catch (error) {
      if (
        isCurrentProcess &&
        this.currentProcessIdentityPromise === lookup
      ) {
        this.currentProcessIdentityPromise = undefined;
      }
      throw error;
    }
  }

  private async resolveCurrentBootIdentity(
    deadline: number,
  ): Promise<string | undefined> {
    assertBeforeLockDeadline(deadline);
    let lookup = this.currentBootIdentityPromise;
    if (lookup === undefined) {
      lookup = Promise.resolve().then(() => this.lockBootIdentity(deadline));
      this.currentBootIdentityPromise = lookup;
    }

    try {
      const identity = await resolveBeforeLockDeadline(lookup, deadline);
      const normalizedIdentity =
        identity === undefined ? undefined : parseBootSessionUuid(identity);
      if (
        normalizedIdentity === undefined &&
        this.currentBootIdentityPromise === lookup
      ) {
        this.currentBootIdentityPromise = undefined;
      }
      return normalizedIdentity;
    } catch (error) {
      if (this.currentBootIdentityPromise === lookup) {
        this.currentBootIdentityPromise = undefined;
      }
      throw error;
    }
  }

  private async resolveLockProcessUid(
    pid: number,
    deadline: number,
  ): Promise<number | undefined> {
    assertBeforeLockDeadline(deadline);
    const uid = await resolveBeforeLockDeadline(
      Promise.resolve().then(() => this.lockProcessUid(pid, deadline)),
      deadline,
    );
    return isValidUid(uid) ? uid : undefined;
  }

  private async releaseOwnedLock(lockPath: string, token: string): Promise<void> {
    await this.removeLockDirectoryOwnedBy(lockPath, token);
  }

  private async removeLockDirectoryOwnedBy(
    lockPath: string,
    token: string,
  ): Promise<void> {
    const ownerPath = path.join(lockPath, `${token}.json`);
    try {
      await fs.unlink(ownerPath);
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return;
      }
      throw error;
    }

    try {
      await fs.rmdir(lockPath);
    } catch (error) {
      if (!isNodeError(error, "ENOENT") && !isNodeError(error, "ENOTEMPTY")) {
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
    let tmpHandle: Awaited<ReturnType<typeof fs.open>> | undefined;

    try {
      tmpHandle = await fs.open(tmpPath, "wx", 0o600);
      await tmpHandle.writeFile(JSON.stringify(state, null, 2), "utf8");
      await tmpHandle.chmod(0o600);
      await tmpHandle.sync();
      await tmpHandle.close();
      tmpHandle = undefined;

      await fs.rename(tmpPath, this.statePath);
      const directoryHandle = await fs.open(directory, "r");
      try {
        await directoryHandle.sync();
      } finally {
        await directoryHandle.close();
      }
    } finally {
      await tmpHandle?.close().catch(() => undefined);
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

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !isNodeError(error, "ESRCH");
  }
}

function isLegacyLockOwnerRecord(
  value: unknown,
): value is LegacyLockOwnerRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.token === "string" &&
    record.token.length > 0 &&
    record.token.length <= 256 &&
    typeof record.pid === "number" &&
    Number.isSafeInteger(record.pid) &&
    typeof record.hostname === "string" &&
    record.hostname.length > 0 &&
    record.hostname.length <= 256
  );
}

function isLockOwnerRecord(value: unknown): value is LockOwnerRecord {
  if (!isBootScopedLockOwnerRecord(value)) {
    return false;
  }
  const record = value as unknown as Record<string, unknown>;
  return isValidUid(record.uid);
}

function isBootScopedLockOwnerRecord(
  value: unknown,
): value is BootScopedLockOwnerRecord {
  if (!isLegacyLockOwnerRecord(value)) {
    return false;
  }
  const record = value as unknown as Record<string, unknown>;
  return isValidProcessIdentity(record.processIdentity);
}

function isValidProcessIdentity(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_PROCESS_IDENTITY_LENGTH
  );
}

function isValidUid(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0 &&
    value <= 0xffff_ffff
  );
}

function defaultLockBootIdentity(
  deadline: number,
): Promise<string | undefined> {
  return readBootIdentity(deadline);
}

async function readBootIdentity(deadline: number): Promise<string | undefined> {
  assertBeforeLockDeadline(deadline);
  let output: string;
  try {
    if (process.platform === "linux") {
      output = await fs.readFile(
        "/proc/sys/kernel/random/boot_id",
        "utf8",
      );
    } else if (process.platform === "darwin") {
      output = await execFileBeforeLockDeadline(
        "/usr/sbin/sysctl",
        ["-n", "kern.bootsessionuuid"],
        deadline,
        4 * 1024,
      );
    } else {
      return undefined;
    }
  } catch (error) {
    if (error instanceof BridgeStateLockTimeoutError) {
      throw error;
    }
    return undefined;
  }
  assertBeforeLockDeadline(deadline);
  return parseBootSessionUuid(output);
}

function defaultLockProcessUid(
  pid: number,
  deadline: number,
): Promise<number | undefined> {
  return readProcessUid(pid, deadline);
}

async function readProcessUid(
  pid: number,
  deadline: number,
): Promise<number | undefined> {
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    return undefined;
  }
  assertBeforeLockDeadline(deadline);
  let output: string;
  try {
    if (process.platform === "linux") {
      output = await fs.readFile(`/proc/${pid}/status`, "utf8");
      assertBeforeLockDeadline(deadline);
      return parseLinuxProcessRealUid(output);
    }
    if (process.platform === "darwin") {
      output = await execFileBeforeLockDeadline(
        "/bin/ps",
        darwinProcessRealUidArgs(pid),
        deadline,
        128,
      );
      return parseDarwinProcessRealUid(output);
    }
    return undefined;
  } catch (error) {
    if (error instanceof BridgeStateLockTimeoutError) {
      throw error;
    }
    return undefined;
  }
}

function defaultLockProcessIdentity(
  pid: number,
  deadline: number,
): Promise<string | undefined> {
  return readProcessIdentity(pid, deadline);
}

async function readProcessIdentity(
  pid: number,
  deadline: number,
): Promise<string | undefined> {
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    return undefined;
  }
  if (process.platform === "linux") {
    let stat: string;
    let bootId: string;
    try {
      [stat, bootId] = await Promise.all([
        fs.readFile(`/proc/${pid}/stat`, "utf8"),
        fs.readFile("/proc/sys/kernel/random/boot_id", "utf8"),
      ]);
    } catch {
      return undefined;
    }
    assertBeforeLockDeadline(deadline);
    return buildLinuxLockProcessIdentity(bootId, stat);
  }
  if (process.platform === "darwin") {
    assertBeforeLockDeadline(deadline);
    try {
      await fs.access(DARWIN_PROCESS_IDENTITY_HELPER, fsConstants.X_OK);
    } catch {
      throw new DarwinProcessIdentityHelperUnavailableError();
    }
    assertBeforeLockDeadline(deadline);

    let processStartOutput: string;
    let bootSessionOutput: string;
    try {
      [processStartOutput, bootSessionOutput] = await Promise.all([
        execFileBeforeLockDeadline(
          DARWIN_PROCESS_IDENTITY_HELPER,
          [String(pid)],
          deadline,
          128,
        ),
        execFileBeforeLockDeadline(
          "/usr/sbin/sysctl",
          ["-n", "kern.bootsessionuuid"],
          deadline,
          4 * 1024,
        ),
      ]);
    } catch {
      return undefined;
    }
    assertBeforeLockDeadline(deadline);
    return buildDarwinLockProcessIdentity(
      bootSessionOutput,
      processStartOutput,
    );
  }
  return undefined;
}

async function execFileBeforeLockDeadline(
  executable: string,
  args: string[],
  deadline: number,
  maxBuffer: number,
): Promise<string> {
  assertBeforeLockDeadline(deadline);
  const result = await execFileAsync(executable, args, {
    encoding: "utf8",
    timeout: Math.max(1, deadline - Date.now()),
    maxBuffer,
    env: { LC_ALL: "C" },
  });
  assertBeforeLockDeadline(deadline);
  return result.stdout;
}

export function parseBootSessionUuid(output: string): string | undefined {
  const value = output.trim();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    value,
  )
    ? value.toLowerCase()
    : undefined;
}

export function parseLockProcessIdentityBoot(
  identity: string,
): string | undefined {
  const linux = /^linux-boot:([^:]+):proc-start:(\d+)$/.exec(identity);
  if (linux !== null) {
    return parseBootSessionUuid(linux[1]!);
  }
  const darwin = /^darwin-boot:([^:]+):proc-start:(.+)$/.exec(identity);
  if (
    darwin !== null &&
    parseDarwinProcessStartTime(darwin[2]!) !== undefined
  ) {
    return parseBootSessionUuid(darwin[1]!);
  }
  return undefined;
}

export function parseLinuxProcessRealUid(
  status: string,
): number | undefined {
  const matches = status
    .split(/\r?\n/)
    .map((line) => /^Uid:\s+(\d+)\s+\d+\s+\d+\s+\d+\s*$/.exec(line))
    .filter((match): match is RegExpExecArray => match !== null);
  return matches.length === 1 ? parseNumericUid(matches[0]![1]!) : undefined;
}

/** @internal Pure argv constructor kept exported for cross-platform lock tests. */
export function darwinProcessRealUidArgs(pid: number): string[] {
  return ["-o", "ruid=", "-p", String(pid)];
}

export function parseDarwinProcessRealUid(output: string): number | undefined {
  return parseNumericUid(output.trim());
}

function parseNumericUid(value: string): number | undefined {
  if (!/^(0|[1-9]\d{0,9})$/.test(value)) {
    return undefined;
  }
  const uid = Number(value);
  return isValidUid(uid) ? uid : undefined;
}

export function parseDarwinProcessStartTime(
  output: string,
): string | undefined {
  const value = output.trim();
  const match = /^([1-9]\d{0,19}):(0|[1-9]\d{0,5})$/.exec(value);
  if (match === null || Number(match[2]) >= 1_000_000) {
    return undefined;
  }
  return `${match[1]}:${match[2]}`;
}

export function parseLinuxProcessStartTicks(
  stat: string,
): string | undefined {
  const commandEnd = stat.lastIndexOf(")");
  if (commandEnd < 0) {
    return undefined;
  }
  const fieldsAfterCommand = stat
    .slice(commandEnd + 1)
    .trim()
    .split(/\s+/);
  const startTicks = fieldsAfterCommand[19];
  return startTicks !== undefined && /^\d+$/.test(startTicks)
    ? startTicks
    : undefined;
}

/** @internal Pure constructor kept exported for cross-platform lock tests. */
export function buildLinuxLockProcessIdentity(
  bootIdOutput: string,
  processStat: string,
): string | undefined {
  const bootSessionUuid = parseBootSessionUuid(bootIdOutput);
  const processStartTicks = parseLinuxProcessStartTicks(processStat);
  return bootSessionUuid === undefined || processStartTicks === undefined
    ? undefined
    : `linux-boot:${bootSessionUuid}:proc-start:${processStartTicks}`;
}

/** @internal Pure constructor kept exported for cross-platform lock tests. */
export function buildDarwinLockProcessIdentity(
  bootSessionOutput: string,
  processStartOutput: string,
): string | undefined {
  const bootSessionUuid = parseBootSessionUuid(bootSessionOutput);
  const processStartTime = parseDarwinProcessStartTime(processStartOutput);
  return bootSessionUuid === undefined || processStartTime === undefined
    ? undefined
    : `darwin-boot:${bootSessionUuid}:proc-start:${processStartTime}`;
}

function assertBeforeLockDeadline(deadline: number): void {
  if (Date.now() >= deadline) {
    throw new BridgeStateLockTimeoutError();
  }
}

async function resolveBeforeLockDeadline<T>(
  promise: Promise<T>,
  deadline: number,
): Promise<T> {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) {
    void promise.catch(() => undefined);
    throw new BridgeStateLockTimeoutError();
  }

  let timeout: NodeJS.Timeout | undefined;
  const deadlineReached = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      reject(new BridgeStateLockTimeoutError());
    }, remainingMs);
  });
  try {
    const result = await Promise.race([promise, deadlineReached]);
    assertBeforeLockDeadline(deadline);
    return result;
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
