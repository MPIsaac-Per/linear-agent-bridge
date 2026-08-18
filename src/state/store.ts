import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { constants as fsConstants, promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  IngressRecoveryEnvelopeError,
  isCanonicalRecoveryTimestamp,
  openIngressRecoveryPayload,
  sealIngressRecoveryPayload,
  type IngressRecoveryKeyring,
  type IngressRecoveryPayload,
  type SealedIngressRecoveryEnvelope,
} from "./recovery-envelope.js";

export const DEFAULT_RECEIPT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const DEFAULT_RECEIPT_MAX_ENTRIES = 10_000;
export const MAX_RECOVERABLE_INGRESS_EVENTS = 128;
export const RECOVERABLE_INGRESS_BATCH_SIZE = 16;

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
  recoverySequence?: number | undefined;
  recoveryEnvelope?: SealedIngressRecoveryEnvelope | undefined;
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
  activityOutbox?: Record<string, ActivityOutboxEntry> | undefined;
}

export interface ActivityOutboxEntry {
  activityId: string;
  agentSessionId: string;
  contentDigest: string;
  attempts: number;
  status: "pending" | "delivered";
  updatedAt: string;
}

export type ClaimEventResult =
  | { disposition: "claimed"; receipt: IngressReceipt }
  | { disposition: "duplicate"; receipt: IngressReceipt }
  | { disposition: "superseded"; receipt: IngressReceipt }
  | { disposition: "ambiguous"; receipt: IngressReceipt };

export type RecoverableIngressEvent =
  | {
      identity: IngressEventIdentity;
      sequence: number;
      payload: IngressRecoveryPayload;
      available: true;
    }
  | {
      identity: IngressEventIdentity;
      sequence?: number | undefined;
      available: false;
      reason: "missing" | "invalid";
    };

export type DispatchStartDisposition = "dispatch_started" | "superseded";
export type DispatchEligibility = "eligible" | "superseded";

export interface BridgeStateStore {
  claimEvent(
    identity: IngressEventIdentity,
    recoveryPayload?: IngressRecoveryPayload,
    options?: { repairLegacyOnly?: boolean },
  ): Promise<ClaimEventResult>;
  markDispatchStarted(webhookId: string): Promise<DispatchStartDisposition>;
  rollbackRuntimeStartIntent(
    webhookId: string,
    recoveryPayload: IngressRecoveryPayload,
  ): Promise<void>;
  checkDispatchEligibility(webhookId: string): Promise<DispatchEligibility>;
  releasePreDispatchClaim(webhookId: string): Promise<boolean>;
  completeEvent(webhookId: string): Promise<void>;
  completeEventWithoutRuntime(webhookId: string): Promise<void>;
  failEvent(webhookId: string, errorClass?: ReceiptErrorClass): Promise<void>;
  getReceipt(webhookId: string): Promise<IngressReceipt | undefined>;
  getClaim(executionId: string): Promise<IngressClaim | undefined>;
  assertRecoverableEventsAvailable(): Promise<void>;
  listRecoverableEvents(
    afterSequence?: number,
  ): Promise<RecoverableIngressEvent[]>;
  getOrCreateActivityId(
    executionId: string,
    activityKey: string,
    signal?: AbortSignal,
  ): Promise<string>;
  prepareActivity(
    executionId: string,
    activityKey: string,
    agentSessionId: string,
    contentDigest: string,
  ): Promise<ActivityOutboxEntry>;
  markActivityAttempted(
    executionId: string,
    activityKey: string,
  ): Promise<ActivityOutboxEntry>;
  markActivityDelivered(executionId: string, activityKey: string): Promise<void>;
}

interface PersistedBridgeState {
  version: 1;
  receipts: Record<string, IngressReceipt>;
  claims: Record<string, IngressClaim>;
  nextRecoverySequence?: number | undefined;
  recoveryStopFences?: Record<string, RecoveryStopFence> | undefined;
}

interface RecoveryStopFence {
  occurredAt: string;
  sequence: number;
  webhookId: string;
  executionId: string;
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
  recoveryKeyring?: IngressRecoveryKeyring;
  /** May only lower the hard admission cap; primarily useful for tests. */
  maxRecoverableEvents?: number;
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

export class BridgeStateIntegrityError extends Error {
  constructor() {
    super("Bridge state integrity validation failed");
    this.name = "BridgeStateIntegrityError";
  }
}

export class DispatchMarkerDurabilityError extends Error {
  constructor() {
    super("Dispatch marker durability was not confirmed");
    this.name = "DispatchMarkerDurabilityError";
  }
}

export class TerminalStateDurabilityError extends Error {
  constructor() {
    super("Terminal state durability was not confirmed");
    this.name = "TerminalStateDurabilityError";
  }
}

class DeferredBridgeStateLockTimeoutError extends BridgeStateLockTimeoutError {
  constructor(readonly settlement: Promise<void>) {
    super();
    this.name = "BridgeStateLockTimeoutError";
  }
}

class StateTargetDurabilityError extends Error {
  constructor(cause: unknown) {
    super("Bridge state target durability was not confirmed", { cause });
    this.name = "StateTargetDurabilityError";
  }
}

export class LegacyIngressRecoveryUnavailableError extends Error {
  constructor() {
    super("Accepted legacy ingress requires a signed matching redelivery");
    this.name = "LegacyIngressRecoveryUnavailableError";
  }
}

export class LegacyIngressRecoveryMismatchError extends Error {
  constructor() {
    super("Webhook does not match a repairable legacy ingress receipt");
    this.name = "LegacyIngressRecoveryMismatchError";
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
  private readonly recoveryKeyring: IngressRecoveryKeyring | undefined;
  private readonly maxRecoverableEvents: number;
  // This process may reclaim a visible pre-dispatch claim only when the
  // mutation that created it never completed through lock release.
  private readonly locallyAcceptedPreDispatchClaims = new Set<string>();
  // A failed release may leave this process's exact owner token visible. Only
  // that token is eligible for the next mutation to reclaim without process
  // liveness heuristics; replacement locks remain protected by their token.
  private readonly strandedOwnedLockTokens = new Map<string, Set<string>>();
  private readonly locallyUnconfirmedDispatchMarkers = new Set<string>();
  private readonly locallyUnconfirmedTerminalWrites = new Set<string>();
  private readonly locallyUnconfirmedRuntimeRollbacks = new Set<string>();
  private stateDirectoryReady: Promise<void> | undefined;
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
    this.recoveryKeyring = options.recoveryKeyring;
    this.maxRecoverableEvents =
      options.maxRecoverableEvents ?? MAX_RECOVERABLE_INGRESS_EVENTS;

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
    if (
      !Number.isInteger(this.maxRecoverableEvents) ||
      this.maxRecoverableEvents <= 0 ||
      this.maxRecoverableEvents > MAX_RECOVERABLE_INGRESS_EVENTS
    ) {
      throw new Error(
        `maxRecoverableEvents must be an integer between 1 and ${MAX_RECOVERABLE_INGRESS_EVENTS}`,
      );
    }
    validateIdentifier(this.ownerId, "ownerId");
  }

  async claimEvent(
    identity: IngressEventIdentity,
    recoveryPayload?: IngressRecoveryPayload,
    options: { repairLegacyOnly?: boolean } = {},
  ): Promise<ClaimEventResult> {
    validateIdentity(identity);
    if (options.repairLegacyOnly === true && recoveryPayload === undefined) {
      throw new LegacyIngressRecoveryMismatchError();
    }
    if (recoveryPayload !== undefined && this.recoveryKeyring === undefined) {
      throw new IngressRecoveryEnvelopeError();
    }

    return await this.mutateClaim(async (deadline) => {
      const state = await this.readState(deadline);
      this.prune(state);
      const existingReceipt = state.receipts[identity.webhookId];
      if (
        options.repairLegacyOnly === true &&
        (existingReceipt === undefined ||
          existingReceipt.recoveryEnvelope !== undefined ||
          existingReceipt.recoverySequence !== undefined ||
          existingReceipt.dispatchStartedAt !== undefined ||
          (existingReceipt.status !== "received" &&
            existingReceipt.status !== "claimed"))
      ) {
        throw new LegacyIngressRecoveryMismatchError();
      }
      if (existingReceipt !== undefined) {
        assertSameIdentity(existingReceipt, identity);
      } else {
        const timestamp = this.timestamp();
        let recovery: {
          recoverySequence: number;
          recoveryEnvelope: SealedIngressRecoveryEnvelope;
        } | undefined;
        if (recoveryPayload !== undefined) {
          recovery = this.createRecoveryEnvelope(
            state,
            identity,
            recoveryPayload,
            true,
          );
          this.recordRecoveryStopFence(
            state,
            identity,
            recoveryPayload,
            recovery.recoverySequence,
          );
        }
        state.receipts[identity.webhookId] = {
          ...identity,
          ...(recovery ?? {}),
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
        await this.writeState(state, deadline);
      }

      const receipt = state.receipts[identity.webhookId]!;
      if (
        recoveryPayload !== undefined &&
        receipt.recoveryEnvelope === undefined &&
        (receipt.status === "received" ||
          (receipt.status === "claimed" &&
            receipt.dispatchStartedAt === undefined))
      ) {
        const recovery = this.createRecoveryEnvelope(
          state,
          identity,
          recoveryPayload,
          false,
        );
        Object.assign(receipt, recovery);
        this.recordRecoveryStopFence(
          state,
          identity,
          recoveryPayload,
          recovery.recoverySequence,
        );
      }
      if (existingReceipt !== undefined && receipt.status !== "received") {
        return await this.resolveExistingReceipt(state, receipt, deadline);
      }
      const existingClaim = state.claims[identity.executionId];
      const timestamp = this.timestamp();
      if (existingClaim !== undefined) {
        if (
          existingClaim.status === "claimed" &&
          existingClaim.dispatchStartedAt === undefined &&
          receipt.status === "received" &&
          !this.locallyAcceptedPreDispatchClaims.has(existingClaim.webhookId)
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
            clearRecoveryEnvelope(priorReceipt);
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
          await this.writeState(state, deadline);
          return { disposition: "claimed", receipt };
        }
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
            clearRecoveryEnvelope(priorReceipt);
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
          await this.writeState(state, deadline);
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
          clearRecoveryEnvelope(receipt);
          receipt.outcome = ambiguousOutcome();
          await this.writeState(state, deadline);
          return { disposition: "ambiguous", receipt };
        }

        receipt.status = "superseded";
        receipt.supersededAt = timestamp;
        receipt.supersededByWebhookId = existingClaim.webhookId;
        receipt.updatedAt = timestamp;
        clearRecoveryEnvelope(receipt);
        receipt.outcome = {
          httpStatus: 200,
          result: "not_dispatched",
          disposition: "superseded",
        };
        this.prune(state);
        await this.writeState(state, deadline);
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
      await this.writeState(state, deadline);
      return { disposition: "claimed", receipt };
    });
  }

  markDispatchStarted(
    webhookId: string,
  ): Promise<DispatchStartDisposition> {
    validateIdentifier(webhookId, "webhookId");

    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (claim.dispatchStartedAt !== undefined) {
        if (this.locallyUnconfirmedDispatchMarkers.has(webhookId)) {
          await this.writeState(state, deadline);
          this.locallyUnconfirmedDispatchMarkers.delete(webhookId);
        }
        this.locallyAcceptedPreDispatchClaims.delete(webhookId);
        return "dispatch_started";
      }
      const fence = state.recoveryStopFences?.[receipt.linearSessionId];
      if (fence !== undefined && !isValidRecoveryStopFence(fence)) {
        throw new IngressRecoveryEnvelopeError();
      }
      if (
        fence !== undefined &&
        fence.executionId !== receipt.executionId &&
        this.receiptIsAtOrBeforeFence(receipt, fence)
      ) {
        this.supersedeOwnedActiveClaimInState(
          state,
          webhookId,
          fence.webhookId,
        );
        await this.writeState(state, deadline);
        this.locallyAcceptedPreDispatchClaims.delete(webhookId);
        return "superseded";
      }

      const timestamp = this.timestamp();
      claim.dispatchStartedAt = timestamp;
      claim.updatedAt = timestamp;
      receipt.dispatchStartedAt = timestamp;
      receipt.updatedAt = timestamp;
      delete receipt.recoverySequence;
      delete receipt.recoveryEnvelope;
      receipt.outcome = {
        httpStatus: 200,
        result: "dispatch_started",
        disposition: "claimed",
      };
      try {
        await this.writeState(state, deadline);
      } catch (error) {
        if (error instanceof StateTargetDurabilityError) {
          this.locallyUnconfirmedDispatchMarkers.add(webhookId);
          throw new DispatchMarkerDurabilityError();
        }
        try {
          const visible = await this.readState(deadline);
          const visibleReceipt = visible.receipts[webhookId];
          const visibleClaim = visible.claims[receipt.executionId];
          if (
            visibleReceipt?.dispatchStartedAt === timestamp &&
            visibleClaim?.dispatchStartedAt === timestamp &&
            visibleClaim.webhookId === webhookId
          ) {
            this.locallyUnconfirmedDispatchMarkers.add(webhookId);
            throw new DispatchMarkerDurabilityError();
          }
        } catch (verificationError) {
          if (verificationError instanceof DispatchMarkerDurabilityError) {
            throw verificationError;
          }
        }
        throw error;
      }
      this.locallyAcceptedPreDispatchClaims.delete(webhookId);
      return "dispatch_started";
    });
  }

  async checkDispatchEligibility(
    webhookId: string,
  ): Promise<DispatchEligibility> {
    validateIdentifier(webhookId, "webhookId");
    const state = await this.readState(Date.now() + this.lockTimeoutMs);
    const { receipt } = this.ownedActiveClaim(state, webhookId);
    const fence = state.recoveryStopFences?.[receipt.linearSessionId];
    if (fence === undefined) {
      return "eligible";
    }
    if (!isValidRecoveryStopFence(fence)) {
      throw new IngressRecoveryEnvelopeError();
    }
    return fence.executionId !== receipt.executionId &&
      this.receiptIsAtOrBeforeFence(receipt, fence)
      ? "superseded"
      : "eligible";
  }

  rollbackRuntimeStartIntent(
    webhookId: string,
    recoveryPayload: IngressRecoveryPayload,
  ): Promise<void> {
    validateIdentifier(webhookId, "webhookId");
    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (claim.dispatchStartedAt === undefined) {
        if (this.locallyUnconfirmedRuntimeRollbacks.has(webhookId)) {
          await this.writeState(state, deadline);
          this.locallyUnconfirmedRuntimeRollbacks.delete(webhookId);
        }
        return;
      }
      const recovery = this.createRecoveryEnvelope(
        state,
        receiptIdentity(receipt),
        recoveryPayload,
        false,
      );
      delete claim.dispatchStartedAt;
      delete receipt.dispatchStartedAt;
      Object.assign(receipt, recovery);
      const timestamp = this.timestamp();
      claim.updatedAt = timestamp;
      receipt.updatedAt = timestamp;
      receipt.outcome = acceptedOutcome();
      try {
        await this.writeState(state, deadline);
      } catch (error) {
        if (error instanceof StateTargetDurabilityError) {
          this.locallyUnconfirmedRuntimeRollbacks.add(webhookId);
        }
        throw error;
      }
      this.locallyUnconfirmedDispatchMarkers.delete(webhookId);
    });
  }

  releasePreDispatchClaim(webhookId: string): Promise<boolean> {
    validateIdentifier(webhookId, "webhookId");
    this.locallyAcceptedPreDispatchClaims.delete(webhookId);

    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (claim.dispatchStartedAt !== undefined) {
        return false;
      }

      const timestamp = this.timestamp();
      receipt.status = "received";
      receipt.updatedAt = timestamp;
      delete receipt.ownerId;
      delete receipt.claimedAt;
      delete receipt.dispatchStartedAt;
      claim.updatedAt = timestamp;
      receipt.outcome = {
        httpStatus: 503,
        result: "retry",
        disposition: "received",
        errorClass: "IngressPersistenceError",
      };
      await this.writeState(state, deadline);
      return true;
    });
  }

  completeEvent(webhookId: string): Promise<void> {
    return this.terminalize(webhookId, "completed", undefined, true);
  }

  completeEventWithoutRuntime(webhookId: string): Promise<void> {
    return this.terminalize(webhookId, "completed", undefined, false);
  }

  failEvent(
    webhookId: string,
    errorClass: ReceiptErrorClass = "WebhookProcessingError",
  ): Promise<void> {
    return this.terminalize(webhookId, "failed", errorClass, true);
  }

  async getReceipt(webhookId: string): Promise<IngressReceipt | undefined> {
    validateIdentifier(webhookId, "webhookId");
    return (await this.readState(Date.now() + this.lockTimeoutMs)).receipts[
      webhookId
    ];
  }

  async getClaim(executionId: string): Promise<IngressClaim | undefined> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    return (await this.readState(Date.now() + this.lockTimeoutMs)).claims[
      executionId
    ];
  }

  async assertRecoverableEventsAvailable(): Promise<void> {
    const state = await this.readState(Date.now() + this.lockTimeoutMs);
    this.assertRecoveryStateIsBounded(state);
    let missingLegacyEnvelope = false;
    const recoverySequences = new Set<number>();
    for (const receipt of activeRecoverableReceipts(state)) {
      const candidate = this.decodeRecoverableReceipt(receipt);
      if (!candidate.available && candidate.reason === "invalid") {
        throw new IngressRecoveryEnvelopeError();
      }
      if (candidate.available && recoverySequences.has(candidate.sequence)) {
        throw new IngressRecoveryEnvelopeError();
      }
      if (candidate.available) {
        recoverySequences.add(candidate.sequence);
      }
      missingLegacyEnvelope ||= !candidate.available;
    }
    if (missingLegacyEnvelope) {
      throw new LegacyIngressRecoveryUnavailableError();
    }
  }

  async listRecoverableEvents(
    afterSequence = 0,
  ): Promise<RecoverableIngressEvent[]> {
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) {
      throw new IngressRecoveryEnvelopeError();
    }
    const state = await this.readState(Date.now() + this.lockTimeoutMs);
    this.assertRecoveryStateIsBounded(state);
    return activeRecoverableReceipts(state)
      .filter(
        (receipt) =>
          receipt.recoverySequence !== undefined &&
          receipt.recoverySequence > afterSequence,
      )
      .sort((left, right) => {
        const leftSequence = left.recoverySequence ?? Number.MAX_SAFE_INTEGER;
        const rightSequence = right.recoverySequence ?? Number.MAX_SAFE_INTEGER;
        return (
          leftSequence - rightSequence ||
          left.webhookId.localeCompare(right.webhookId)
        );
      })
      .slice(0, RECOVERABLE_INGRESS_BATCH_SIZE)
      .map((receipt) => this.decodeRecoverableReceipt(receipt));
  }

  private decodeRecoverableReceipt(
    receipt: IngressReceipt,
  ): RecoverableIngressEvent {
    const identity = receiptIdentity(receipt);
    const missingLegacyEnvelope =
      receipt.recoverySequence === undefined &&
      receipt.recoveryEnvelope === undefined;
    if (
      this.recoveryKeyring === undefined ||
      !Number.isSafeInteger(receipt.recoverySequence) ||
      receipt.recoverySequence === undefined ||
      receipt.recoverySequence <= 0
    ) {
      return {
        identity,
        available: false,
        reason: missingLegacyEnvelope ? "missing" : "invalid",
      };
    }
    if (receipt.recoveryEnvelope === undefined) {
      return {
        identity,
        sequence: receipt.recoverySequence,
        available: false,
        reason: "invalid",
      };
    }
    try {
      return {
        identity,
        sequence: receipt.recoverySequence,
        payload: openIngressRecoveryPayload(
          this.recoveryKeyring,
          identity,
          receipt.recoverySequence,
          receipt.recoveryEnvelope,
        ),
        available: true,
      };
    } catch {
      return {
        identity,
        sequence: receipt.recoverySequence,
        available: false,
        reason: "invalid",
      };
    }
  }

  private assertRecoveryStateIsBounded(state: PersistedBridgeState): void {
    for (const fence of Object.values(state.recoveryStopFences ?? {})) {
      if (!isValidRecoveryStopFence(fence)) {
        throw new IngressRecoveryEnvelopeError();
      }
    }
    if (
      activeRecoverableReceipts(state).length > this.maxRecoverableEvents
    ) {
      throw new IngressRecoveryEnvelopeError();
    }
  }

  private createRecoveryEnvelope(
    state: PersistedBridgeState,
    identity: IngressEventIdentity,
    payload: IngressRecoveryPayload,
    admittingNewEvent: boolean,
  ): {
    recoverySequence: number;
    recoveryEnvelope: SealedIngressRecoveryEnvelope;
  } {
    if (this.recoveryKeyring === undefined) {
      throw new IngressRecoveryEnvelopeError();
    }
    let activeRecoveryCount = activeRecoverableReceipts(state).length;
    if (
      admittingNewEvent &&
      activeRecoveryCount >= this.maxRecoverableEvents &&
      payload.action === "prompted" &&
      payload.stop
    ) {
      const reclaimable = activeRecoverableReceipts(state)
        .filter(
          (receipt) =>
            receipt.linearSessionId === identity.linearSessionId &&
            receipt.executionId !== identity.executionId &&
            receipt.dispatchStartedAt === undefined,
        )
        .sort(
          (left, right) =>
            (left.recoverySequence ?? Number.MAX_SAFE_INTEGER) -
            (right.recoverySequence ?? Number.MAX_SAFE_INTEGER),
        );
      while (
        activeRecoveryCount >= this.maxRecoverableEvents &&
        reclaimable.length > 0
      ) {
        const receipt = reclaimable.shift()!;
        const timestamp = this.timestamp();
        receipt.status = "superseded";
        receipt.supersededAt = timestamp;
        receipt.supersededByWebhookId = identity.webhookId;
        receipt.updatedAt = timestamp;
        clearRecoveryEnvelope(receipt);
        receipt.outcome = {
          httpStatus: 200,
          result: "not_dispatched",
          disposition: "superseded",
        };
        const claim = state.claims[receipt.executionId];
        if (claim?.webhookId === receipt.webhookId) {
          claim.status = "completed";
          claim.updatedAt = timestamp;
        }
        activeRecoveryCount -= 1;
      }
    }
    if (
      (admittingNewEvent && activeRecoveryCount >= this.maxRecoverableEvents) ||
      (!admittingNewEvent && activeRecoveryCount > this.maxRecoverableEvents)
    ) {
      throw new IngressRecoveryEnvelopeError();
    }
    let highestSequence = 0;
    for (const receipt of Object.values(state.receipts)) {
      if (
        receipt.recoverySequence !== undefined &&
        Number.isSafeInteger(receipt.recoverySequence) &&
        receipt.recoverySequence > highestSequence
      ) {
        highestSequence = receipt.recoverySequence;
      }
    }
    for (const fence of Object.values(state.recoveryStopFences ?? {})) {
      if (!isValidRecoveryStopFence(fence)) {
        throw new IngressRecoveryEnvelopeError();
      }
      highestSequence = Math.max(highestSequence, fence.sequence);
    }
    if (
      state.nextRecoverySequence !== undefined &&
      (!Number.isSafeInteger(state.nextRecoverySequence) ||
        state.nextRecoverySequence <= 0)
    ) {
      throw new IngressRecoveryEnvelopeError();
    }
    const sequence = Math.max(
      state.nextRecoverySequence ?? 1,
      highestSequence + 1,
    );
    if (!Number.isSafeInteger(sequence) || sequence <= 0) {
      throw new IngressRecoveryEnvelopeError();
    }
    const nextSequence = sequence + 1;
    if (!Number.isSafeInteger(nextSequence)) {
      throw new IngressRecoveryEnvelopeError();
    }
    const recoveryEnvelope = sealIngressRecoveryPayload(
      this.recoveryKeyring,
      identity,
      sequence,
      payload,
    );
    state.nextRecoverySequence = nextSequence;
    return {
      recoverySequence: sequence,
      recoveryEnvelope,
    };
  }

  private recordRecoveryStopFence(
    state: PersistedBridgeState,
    identity: IngressEventIdentity,
    payload: IngressRecoveryPayload,
    sequence: number,
  ): void {
    if (payload.action !== "prompted" || !payload.stop) {
      return;
    }
    const semanticDeliveryExists = Object.values(state.receipts).some(
      (receipt) =>
        receipt.webhookId !== identity.webhookId &&
        receipt.executionId === identity.executionId,
    );
    if (semanticDeliveryExists) {
      return;
    }
    const fences = (state.recoveryStopFences ??= {});
    const existing = fences[identity.linearSessionId];
    if (
      existing === undefined ||
      compareRecoveryOrder(payload.occurredAt, sequence, existing) > 0
    ) {
      fences[identity.linearSessionId] = {
        occurredAt: payload.occurredAt,
        sequence,
        webhookId: identity.webhookId,
        executionId: identity.executionId,
      };
    }
  }

  private receiptIsAtOrBeforeFence(
    receipt: IngressReceipt,
    fence: RecoveryStopFence,
  ): boolean {
    if (
      this.recoveryKeyring === undefined ||
      receipt.recoveryEnvelope === undefined ||
      receipt.recoverySequence === undefined
    ) {
      throw new IngressRecoveryEnvelopeError();
    }
    const payload = openIngressRecoveryPayload(
      this.recoveryKeyring,
      receiptIdentity(receipt),
      receipt.recoverySequence,
      receipt.recoveryEnvelope,
    );
    if (!isValidRecoveryStopFence(fence)) {
      throw new IngressRecoveryEnvelopeError();
    }
    if (receipt.action === "created") {
      return true;
    }
    const byTime = Date.parse(payload.occurredAt) - Date.parse(fence.occurredAt);
    return (
      byTime < 0 ||
      (byTime === 0 && receipt.recoverySequence <= fence.sequence)
    );
  }



  getOrCreateActivityId(
    executionId: string,
    activityKey: string,
    signal?: AbortSignal,
  ): Promise<string> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    validateIdentifier(activityKey, "activityKey", MAX_ACTIVITY_KEY_LENGTH);

    try {
      throwIfAborted(signal);
    } catch (error) {
      return Promise.reject(error);
    }
    return this.mutate(async (deadline) => {
      throwIfAborted(signal);
      const state = await this.readState(deadline);
      throwIfAborted(signal);
      const claim = state.claims[executionId];
      if (claim === undefined) {
        throw new Error(`No ingress claim for executionId "${executionId}"`);
      }
      if (claim.ownerId !== this.ownerId) {
        throw new ClaimOwnershipError(claim.webhookId);
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
      throwIfAborted(signal);
      await this.writeState(state, deadline);
      return activityId;
    });
  }

  prepareActivity(
    executionId: string,
    activityKey: string,
    agentSessionId: string,
    contentDigest: string,
  ): Promise<ActivityOutboxEntry> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    validateIdentifier(activityKey, "activityKey", MAX_ACTIVITY_KEY_LENGTH);
    validateIdentifier(agentSessionId, "agentSessionId");
    if (!/^[a-f0-9]{64}$/.test(contentDigest)) {
      throw new Error("contentDigest must be a lowercase SHA-256 digest");
    }

    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const claim = state.claims[executionId];
      if (claim === undefined || claim.status !== "claimed") {
        throw new Error(`No active ingress claim for executionId "${executionId}"`);
      }
      if (claim.ownerId !== this.ownerId) {
        throw new ClaimOwnershipError(claim.webhookId);
      }
      const outbox = (claim.activityOutbox ??= {});
      const existing = outbox[activityKey];
      if (existing !== undefined) {
        if (
          existing.agentSessionId !== agentSessionId ||
          existing.contentDigest !== contentDigest
        ) {
          throw new Error(
            `Activity binding changed for executionId "${executionId}"`,
          );
        }
        return { ...existing };
      }
      if (Object.keys(claim.activityIds).length >= MAX_ACTIVITY_IDS_PER_CLAIM) {
        throw new Error(
          `Too many outbound activity ids for executionId "${executionId}"`,
        );
      }
      const activityId = claim.activityIds[activityKey] ?? randomUUID();
      claim.activityIds[activityKey] = activityId;
      const entry: ActivityOutboxEntry = {
        activityId,
        agentSessionId,
        contentDigest,
        attempts: 0,
        status: "pending",
        updatedAt: this.timestamp(),
      };
      outbox[activityKey] = entry;
      claim.updatedAt = entry.updatedAt;
      await this.writeState(state, deadline);
      return { ...entry };
    });
  }

  markActivityDelivered(
    executionId: string,
    activityKey: string,
  ): Promise<void> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    validateIdentifier(activityKey, "activityKey", MAX_ACTIVITY_KEY_LENGTH);
    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const claim = state.claims[executionId];
      if (
        claim === undefined ||
        claim.status !== "claimed" ||
        claim.ownerId !== this.ownerId
      ) {
        throw new ClaimOwnershipError(claim?.webhookId ?? executionId);
      }
      const entry = claim.activityOutbox?.[activityKey];
      if (entry === undefined) {
        throw new Error(
          `No pending activity for executionId "${executionId}"`,
        );
      }
      if (entry.status === "delivered") {
        return;
      }
      entry.status = "delivered";
      entry.updatedAt = this.timestamp();
      claim.updatedAt = entry.updatedAt;
      await this.writeState(state, deadline);
    });
  }

  markActivityAttempted(
    executionId: string,
    activityKey: string,
  ): Promise<ActivityOutboxEntry> {
    validateIdentifier(executionId, "executionId", MAX_EXECUTION_ID_LENGTH);
    validateIdentifier(activityKey, "activityKey", MAX_ACTIVITY_KEY_LENGTH);
    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const claim = state.claims[executionId];
      if (
        claim === undefined ||
        claim.status !== "claimed" ||
        claim.ownerId !== this.ownerId
      ) {
        throw new ClaimOwnershipError(claim?.webhookId ?? executionId);
      }
      const entry = claim.activityOutbox?.[activityKey];
      if (entry === undefined) {
        throw new Error(
          `No pending activity for executionId "${executionId}"`,
        );
      }
      if (entry.status === "delivered") {
        return { ...entry };
      }
      if (!Number.isSafeInteger(entry.attempts) || entry.attempts < 0) {
        throw new Error("Invalid activity outbox attempt count");
      }
      entry.attempts += 1;
      entry.updatedAt = this.timestamp();
      claim.updatedAt = entry.updatedAt;
      await this.writeState(state, deadline);
      return { ...entry };
    });
  }

  private terminalize(
    webhookId: string,
    status: "completed" | "failed",
    errorClass?: ReceiptErrorClass,
    requireRuntimeIntent = true,
  ): Promise<void> {
    validateIdentifier(webhookId, "webhookId");

    return this.mutate(async (deadline) => {
      const state = await this.readState(deadline);
      const currentReceipt = state.receipts[webhookId];
      if (
        currentReceipt?.status === status &&
        state.claims[currentReceipt.executionId]?.status === status
      ) {
        if (this.locallyUnconfirmedTerminalWrites.has(webhookId)) {
          await this.writeState(state, deadline);
          this.locallyUnconfirmedTerminalWrites.delete(webhookId);
        }
        return;
      }
      const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
      if (requireRuntimeIntent && claim.dispatchStartedAt === undefined) {
        throw new Error(`Dispatch has not started for webhookId "${webhookId}"`);
      }
      if (!requireRuntimeIntent && claim.dispatchStartedAt !== undefined) {
        throw new Error(`Dispatch already started for webhookId "${webhookId}"`);
      }

      const timestamp = this.timestamp();
      receipt.status = status;
      receipt.updatedAt = timestamp;
      claim.status = status;
      claim.updatedAt = timestamp;
      clearRecoveryEnvelope(receipt);
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
      try {
        await this.writeState(state, deadline);
      } catch (error) {
        if (error instanceof StateTargetDurabilityError) {
          this.locallyUnconfirmedTerminalWrites.add(webhookId);
          throw new TerminalStateDurabilityError();
        }
        try {
          const visible = await this.readState(deadline);
          const visibleReceipt = visible.receipts[webhookId];
          const visibleClaim = visible.claims[receipt.executionId];
          const terminalTimestamp =
            status === "completed"
              ? visibleReceipt?.completedAt
              : visibleReceipt?.failedAt;
          if (
            visibleReceipt?.status === status &&
            visibleClaim?.status === status &&
            terminalTimestamp === timestamp
          ) {
            this.locallyUnconfirmedTerminalWrites.add(webhookId);
            throw new TerminalStateDurabilityError();
          }
        } catch (verificationError) {
          if (verificationError instanceof TerminalStateDurabilityError) {
            throw verificationError;
          }
        }
        throw error;
      }
    });
  }

  private supersedeOwnedActiveClaimInState(
    state: PersistedBridgeState,
    webhookId: string,
    supersededByWebhookId: string,
  ): void {
    const { claim, receipt } = this.ownedActiveClaim(state, webhookId);
    if (claim.dispatchStartedAt !== undefined) {
      throw new Error(`Dispatch already started for webhookId "${webhookId}"`);
    }
    const timestamp = this.timestamp();
    receipt.status = "superseded";
    receipt.supersededAt = timestamp;
    receipt.supersededByWebhookId = supersededByWebhookId;
    receipt.updatedAt = timestamp;
    clearRecoveryEnvelope(receipt);
    receipt.outcome = {
      httpStatus: 200,
      result: "not_dispatched",
      disposition: "superseded",
    };
    claim.status = "completed";
    claim.updatedAt = timestamp;
  }

  private async resolveExistingReceipt(
    state: PersistedBridgeState,
    receipt: IngressReceipt,
    deadline: number,
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
          await this.writeState(state, deadline);
          return { disposition: "claimed", receipt };
        }

        receipt.updatedAt = timestamp;
        receipt.outcome = ambiguousOutcome();
        await this.writeState(state, deadline);
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
        await this.writeState(state, deadline);
        return { disposition: "claimed", receipt };
      }

      receipt.updatedAt = timestamp;
      receipt.outcome = {
        httpStatus: 200,
        result: "not_dispatched",
        disposition: "duplicate",
      };
      await this.writeState(state, deadline);
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
    await this.writeState(state, deadline);
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
    operation: (deadline: number) => Promise<ClaimEventResult>,
  ): Promise<ClaimEventResult> {
    return this.mutate(operation, (result) => {
      if (result.disposition === "claimed") {
        this.locallyAcceptedPreDispatchClaims.add(result.receipt.webhookId);
      }
    });
  }

  private mutate<T>(
    operation: (deadline: number) => Promise<T>,
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
      return this.withFileLock(() => operation(deadline), deadline).then((result) => {
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
    await this.ensureDurableStateDirectory(directory, deadline);
    const lockToken = randomUUID();
    const candidatePath = `${lockPath}.${lockToken}.candidate`;
    const candidateOwnerPath = path.join(candidatePath, `${lockToken}.json`);
    let acquired = false;
    let deferredRelease: Promise<void> | undefined;
    try {
      const candidateMkdir = fs.mkdir(candidatePath, { mode: 0o700 });
      try {
        await resolveBeforeLockDeadline(candidateMkdir, deadline);
      } catch (error) {
        if (error instanceof BridgeStateLockTimeoutError) {
          void candidateMkdir
            .then(() => fs.rm(candidatePath, { recursive: true, force: true }))
            .catch(() => undefined);
        }
        throw error;
      }
      await this.writeLockOwner(candidateOwnerPath, lockToken, deadline);
      while (!acquired) {
        if (Date.now() >= deadline) {
          throw new BridgeStateLockTimeoutError();
        }
        try {
          const rename = fs.rename(candidatePath, lockPath);
          try {
            await resolveBeforeLockDeadline(rename, deadline);
          } catch (error) {
            if (error instanceof BridgeStateLockTimeoutError) {
              void rename
                .then(() =>
                  this.releaseOwnedLock(
                    lockPath,
                    lockToken,
                    Date.now() + this.lockTimeoutMs,
                  ),
                )
                .catch(() => undefined);
            }
            throw error;
          }
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
          await resolveBeforeLockDeadline(
            delay(Math.min(this.lockRetryMs, Math.max(1, deadline - Date.now()))),
            deadline,
          );
        }
      }

      try {
        return await resolveBeforeLockDeadline(operation(), deadline);
      } catch (error) {
        if (error instanceof DeferredBridgeStateLockTimeoutError) {
          deferredRelease = error.settlement;
        }
        throw error;
      }
    } finally {
      if (acquired) {
        if (deferredRelease !== undefined) {
          this.rememberStrandedOwnedLock(lockPath, lockToken);
          void deferredRelease
            .then(() =>
              this.releaseOwnedLock(
                lockPath,
                lockToken,
                Date.now() + this.lockTimeoutMs,
              ),
            )
            .catch(() => undefined);
        } else {
          const release = this.releaseOwnedLock(
            lockPath,
            lockToken,
            Date.now() + this.lockTimeoutMs,
          );
          try {
            await resolveBeforeLockDeadline(release, deadline);
          } catch (error) {
            this.rememberStrandedOwnedLock(lockPath, lockToken);
            void release
              .then(() => this.forgetStrandedOwnedLock(lockPath, lockToken))
              .catch(() => undefined);
            throw error;
          }
        }
      }
      const cleanup = fs.rm(candidatePath, { recursive: true, force: true });
      try {
        await resolveBeforeLockDeadline(cleanup, deadline);
      } catch (error) {
        void cleanup.catch(() => undefined);
        if (!(error instanceof BridgeStateLockTimeoutError)) {
          throw error;
        }
      }
    }
  }

  private async ensureDurableStateDirectory(
    directory: string,
    deadline: number,
  ): Promise<void> {
    const resolvedDirectory = path.resolve(directory);
    try {
      assertBeforeLockDeadline(deadline);
      const firstCreated = await resolveBeforeLockDeadline(
        fs.mkdir(resolvedDirectory, { recursive: true }),
        deadline,
      );
      if (
        firstCreated === undefined &&
        this.stateDirectoryReady !== undefined
      ) {
        return;
      }

      this.stateDirectoryReady = undefined;
      for (const prefix of absoluteDirectoryPrefixes(resolvedDirectory)) {
        await syncDirectoryBeforeLockDeadline(prefix, deadline);
      }
      this.stateDirectoryReady = Promise.resolve();
    } catch (error) {
      this.stateDirectoryReady = undefined;
      throw error;
    }
  }

  private async writeLockOwner(
    ownerPath: string,
    token: string,
    deadline: number,
  ): Promise<void> {
    const open = fs.open(ownerPath, "wx", 0o600);
    let handle: Awaited<ReturnType<typeof fs.open>>;
    try {
      handle = await resolveBeforeLockDeadline(open, deadline);
    } catch (error) {
      void open.then((lateHandle) => lateHandle.close()).catch(() => undefined);
      throw error;
    }
    let closeDeferred = false;
    const run = async (operation: Promise<unknown>): Promise<void> => {
      try {
        await resolveBeforeLockDeadline(operation, deadline);
      } catch (error) {
        if (error instanceof BridgeStateLockTimeoutError) {
          closeDeferred = true;
          void operation
            .then(
              () => handle.close(),
              () => handle.close(),
            )
            .catch(() => undefined);
        }
        throw error;
      }
    };
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
      await run(handle.writeFile(`${JSON.stringify(owner)}\n`, "utf8"));
      await run(handle.chmod(0o600));
      await run(handle.sync());
    } finally {
      if (!closeDeferred) {
        await resolveBeforeLockDeadline(handle.close(), deadline);
      }
    }
  }

  private async removeAbandonedLock(
    lockPath: string,
    deadline: number,
  ): Promise<void> {
    let entries: string[];
    try {
      entries = await resolveBeforeLockDeadline(fs.readdir(lockPath), deadline);
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return;
      }
      throw error;
    }
    if (entries.length === 0) {
      assertBeforeLockDeadline(deadline);
      try {
        await resolveBeforeLockDeadline(fs.rmdir(lockPath), deadline);
      } catch (error) {
        if (
          !isNodeError(error, "ENOENT") &&
          !isNodeError(error, "ENOTEMPTY")
        ) {
          throw error;
        }
      }
      return;
    }
    if (entries.length !== 1 || !entries[0]!.endsWith(".json")) {
      return;
    }

    const ownerPath = path.join(lockPath, entries[0]!);
    let parsed: unknown;
    try {
      parsed = JSON.parse(
        await resolveBeforeLockDeadline(fs.readFile(ownerPath, "utf8"), deadline),
      );
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

    if (this.strandedOwnedLockTokens.get(lockPath)?.has(owner.token) === true) {
      assertBeforeLockDeadline(deadline);
      await resolveBeforeLockDeadline(
        this.removeLockDirectoryOwnedBy(lockPath, owner.token, deadline),
        deadline,
      );
      this.forgetStrandedOwnedLock(lockPath, owner.token);
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
        await this.removeLockDirectoryOwnedBy(lockPath, owner.token, deadline);
        return;
      }
    }

    if (!isProcessAlive(owner.pid)) {
      assertBeforeLockDeadline(deadline);
      await this.removeLockDirectoryOwnedBy(lockPath, owner.token, deadline);
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
      await this.removeLockDirectoryOwnedBy(lockPath, owner.token, deadline);
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
    await this.removeLockDirectoryOwnedBy(lockPath, owner.token, deadline);
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

  private async releaseOwnedLock(
    lockPath: string,
    token: string,
    deadline: number,
  ): Promise<void> {
    try {
      await this.removeLockDirectoryOwnedBy(lockPath, token, deadline);
      this.forgetStrandedOwnedLock(lockPath, token);
    } catch (error) {
      this.rememberStrandedOwnedLock(lockPath, token);
      throw error;
    }
  }

  private rememberStrandedOwnedLock(lockPath: string, token: string): void {
    let tokens = this.strandedOwnedLockTokens.get(lockPath);
    if (tokens === undefined) {
      tokens = new Set<string>();
      this.strandedOwnedLockTokens.set(lockPath, tokens);
    }
    tokens.add(token);
  }

  private forgetStrandedOwnedLock(lockPath: string, token: string): void {
    const tokens = this.strandedOwnedLockTokens.get(lockPath);
    tokens?.delete(token);
    if (tokens?.size === 0) {
      this.strandedOwnedLockTokens.delete(lockPath);
    }
  }

  private async removeLockDirectoryOwnedBy(
    lockPath: string,
    token: string,
    deadline: number,
  ): Promise<void> {
    const ownerPath = path.join(lockPath, `${token}.json`);
    try {
      await resolveBeforeLockDeadline(fs.unlink(ownerPath), deadline);
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return;
      }
      throw error;
    }

    try {
      await resolveBeforeLockDeadline(fs.rmdir(lockPath), deadline);
    } catch (error) {
      if (!isNodeError(error, "ENOENT") && !isNodeError(error, "ENOTEMPTY")) {
        throw error;
      }
    }
  }

  private async readState(deadline?: number): Promise<PersistedBridgeState> {
    let raw: string;
    try {
      const read = fs.readFile(this.statePath, "utf8");
      raw =
        deadline === undefined
          ? await read
          : await resolveBeforeLockDeadline(read, deadline);
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
      throw new BridgeStateIntegrityError();
    }
    if (!isPersistedBridgeState(parsed)) {
      throw new BridgeStateIntegrityError();
    }
    return parsed;
  }

  private async writeState(
    state: PersistedBridgeState,
    deadline: number,
  ): Promise<void> {
    const directory = path.dirname(this.statePath);
    const tmpPath = path.join(
      directory,
      `.${path.basename(this.statePath)}.${randomUUID()}.tmp`,
    );
    let tmpHandle: Awaited<ReturnType<typeof fs.open>> | undefined;
    let closeDeferred = false;
    let targetRenamed = false;
    const runHandleOperation = async (
      operation: Promise<unknown>,
    ): Promise<void> => {
      try {
        await resolveBeforeLockDeadline(operation, deadline);
      } catch (error) {
        if (
          error instanceof BridgeStateLockTimeoutError &&
          tmpHandle !== undefined
        ) {
          const handle = tmpHandle;
          closeDeferred = true;
          void operation
            .then(
              () => handle.close(),
              () => handle.close(),
            )
            .catch(() => undefined);
        }
        throw error;
      }
    };

    try {
      const open = fs.open(tmpPath, "wx", 0o600);
      try {
        tmpHandle = await resolveBeforeLockDeadline(open, deadline);
      } catch (error) {
        void open.then((lateHandle) => lateHandle.close()).catch(() => undefined);
        throw error;
      }
      await runHandleOperation(
        tmpHandle.writeFile(JSON.stringify(state, null, 2), "utf8"),
      );
      await runHandleOperation(tmpHandle.chmod(0o600));
      await runHandleOperation(tmpHandle.sync());
      await resolveBeforeLockDeadline(tmpHandle.close(), deadline);
      tmpHandle = undefined;

      const rename = fs.rename(tmpPath, this.statePath);
      try {
        await resolveBeforeLockDeadline(rename, deadline);
        targetRenamed = true;
      } catch (error) {
        if (error instanceof BridgeStateLockTimeoutError) {
          const settlement = rename.then(
            () => undefined,
            () => undefined,
          );
          throw new DeferredBridgeStateLockTimeoutError(settlement);
        }
        throw error;
      }
      const directoryOpen = fs.open(directory, "r");
      let directoryHandle: Awaited<ReturnType<typeof fs.open>>;
      try {
        directoryHandle = await resolveBeforeLockDeadline(
          directoryOpen,
          deadline,
        );
      } catch (error) {
        void directoryOpen
          .then((lateHandle) => lateHandle.close())
          .catch(() => undefined);
        throw error;
      }
      let directoryCloseDeferred = false;
      try {
        const sync = directoryHandle.sync();
        try {
          await resolveBeforeLockDeadline(sync, deadline);
        } catch (error) {
          if (error instanceof BridgeStateLockTimeoutError) {
            directoryCloseDeferred = true;
            void sync
              .then(
                () => directoryHandle.close(),
                () => directoryHandle.close(),
              )
              .catch(() => undefined);
          }
          throw error;
        }
      } finally {
        if (!directoryCloseDeferred) {
          await resolveBeforeLockDeadline(directoryHandle.close(), deadline);
        }
      }
    } catch (error) {
      if (
        targetRenamed &&
        !(error instanceof StateTargetDurabilityError)
      ) {
        throw new StateTargetDurabilityError(error);
      }
      throw error;
    } finally {
      if (tmpHandle !== undefined && !closeDeferred) {
        await resolveBeforeLockDeadline(tmpHandle.close(), deadline).catch(
          () => undefined,
        );
      }
      const cleanup = fs.unlink(tmpPath).catch((error: unknown) => {
        if (!isNodeError(error, "ENOENT")) {
          throw error;
        }
      });
      try {
        await resolveBeforeLockDeadline(cleanup, deadline);
      } catch (error) {
        void cleanup.catch(() => undefined);
        if (!(error instanceof BridgeStateLockTimeoutError)) {
          throw error;
        }
      }
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
    for (const [sessionId, fence] of Object.entries(
      state.recoveryStopFences ?? {},
    )) {
      if (
        isValidRecoveryStopFence(fence) &&
        state.receipts[fence.webhookId] === undefined
      ) {
        delete state.recoveryStopFences![sessionId];
      }
    }
    if (Object.keys(state.recoveryStopFences ?? {}).length === 0) {
      delete state.recoveryStopFences;
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

function receiptIdentity(receipt: IngressReceipt): IngressEventIdentity {
  return {
    webhookId: receipt.webhookId,
    executionId: receipt.executionId,
    linearSessionId: receipt.linearSessionId,
    action: receipt.action,
  };
}

function clearRecoveryEnvelope(receipt: IngressReceipt): void {
  delete receipt.recoverySequence;
  delete receipt.recoveryEnvelope;
}

function activeRecoverableReceipts(
  state: PersistedBridgeState,
): IngressReceipt[] {
  return Object.values(state.receipts).filter(
    (receipt) =>
      (receipt.status === "received" || receipt.status === "claimed") &&
      receipt.dispatchStartedAt === undefined,
  );
}

function compareRecoveryOrder(
  occurredAt: string,
  sequence: number,
  other: { occurredAt: string; sequence: number },
): number {
  const byTime = Date.parse(occurredAt) - Date.parse(other.occurredAt);
  return byTime === 0 ? sequence - other.sequence : byTime;
}

function isValidRecoveryStopFence(value: unknown): value is RecoveryStopFence {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    Object.keys(record).length === 4 &&
    isCanonicalRecoveryTimestamp(record.occurredAt) &&
    typeof record.sequence === "number" &&
    Number.isSafeInteger(record.sequence) &&
    record.sequence > 0 &&
    typeof record.webhookId === "string" &&
    record.webhookId.length > 0 &&
    record.webhookId.length <= MAX_IDENTIFIER_LENGTH &&
    typeof record.executionId === "string" &&
    record.executionId.length > 0 &&
    record.executionId.length <= MAX_EXECUTION_ID_LENGTH
  );
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

function absoluteDirectoryPrefixes(directory: string): string[] {
  const { root } = path.parse(directory);
  const prefixes = [root];
  let prefix = root;
  const relative = path.relative(root, directory);
  for (const segment of relative.split(path.sep)) {
    if (segment.length === 0) {
      continue;
    }
    prefix = path.join(prefix, segment);
    prefixes.push(prefix);
  }
  return prefixes;
}

async function syncDirectoryBeforeLockDeadline(
  directory: string,
  deadline: number,
): Promise<void> {
  assertBeforeLockDeadline(deadline);
  const openPromise = fs.open(directory, "r");
  let handle: Awaited<ReturnType<typeof fs.open>>;
  try {
    handle = await resolveBeforeLockDeadline(openPromise, deadline);
  } catch (error) {
    void openPromise
      .then((lateHandle) => lateHandle.close())
      .catch(() => undefined);
    throw error;
  }

  let syncPromise: Promise<void> | undefined;
  try {
    assertBeforeLockDeadline(deadline);
    syncPromise = handle.sync();
    await resolveBeforeLockDeadline(syncPromise, deadline);
  } catch (error) {
    if (
      error instanceof BridgeStateLockTimeoutError &&
      syncPromise !== undefined
    ) {
      void syncPromise
        .then(
          () => handle.close(),
          () => handle.close(),
        )
        .catch(() => undefined);
    } else {
      await handle.close().catch(() => undefined);
    }
    throw error;
  }

  await resolveBeforeLockDeadline(handle.close(), deadline);
}

function assertBeforeLockDeadline(deadline: number): void {
  if (Date.now() >= deadline) {
    throw new BridgeStateLockTimeoutError();
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted === true) {
    throw signal.reason;
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
