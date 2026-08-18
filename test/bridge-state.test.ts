import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildDarwinLockProcessIdentity,
  buildLinuxLockProcessIdentity,
  darwinProcessRealUidArgs,
  DispatchMarkerDurabilityError,
  JsonBridgeStateStore,
  LegacyIngressRecoveryMismatchError,
  LegacyIngressRecoveryUnavailableError,
  parseBootSessionUuid,
  parseDarwinProcessRealUid,
  parseDarwinProcessStartTime,
  parseLinuxProcessRealUid,
  parseLinuxProcessStartTicks,
  parseLockProcessIdentityBoot,
  type IngressEventIdentity,
  type JsonBridgeStateStoreOptions,
} from "../src/state/store.js";
import {
  createIngressRecoveryKeyring,
  IngressRecoveryEnvelopeError,
  openIngressRecoveryPayload,
  parseCanonicalRecoveryKey,
  sealIngressRecoveryPayload,
} from "../src/state/recovery-envelope.js";

let tmpDir: string;

const BOOT_A = "24f0c7a0-3dd9-4b33-869c-8f07d374ebd8";
const BOOT_B = "79326562-1a4c-42a2-ac6d-00478b65895d";
const TEST_UID = process.getuid?.() ?? 501;
const CURRENT_PROCESS_IDENTITY = `linux-boot:${BOOT_A}:proc-start:100`;
const RECOVERY_KEY_A = "A".repeat(43);
const RECOVERY_KEY_B = Buffer.alloc(32, 1).toString("base64url");
const TEST_LOCK_OPTIONS = {
  lockProcessIdentity: async () => CURRENT_PROCESS_IDENTITY,
  lockBootIdentity: async () => BOOT_A,
  lockProcessUid: async () => TEST_UID,
} satisfies JsonBridgeStateStoreOptions;

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "bridge-state-test-"));
});

afterEach(async () => {
  vi.restoreAllMocks();
  await fs.rm(tmpDir, { recursive: true, force: true });
});

function event(overrides: Partial<IngressEventIdentity> = {}): IngressEventIdentity {
  return {
    webhookId: "webhook-1",
    executionId: "created:session-1",
    linearSessionId: "session-1",
    action: "created",
    ...overrides,
  };
}

function deferred<T = void>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs = 500,
): Promise<void> {
  const startedAt = Date.now();
  while (!(await predicate())) {
    if (Date.now() - startedAt >= timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

async function writeLockOwner(
  lockPath: string,
  token: string,
  owner: Record<string, unknown>,
): Promise<void> {
  await fs.mkdir(lockPath, { mode: 0o700 });
  await fs.writeFile(
    path.join(lockPath, `${token}.json`),
    `${JSON.stringify({ token, ...owner })}\n`,
    { mode: 0o600 },
  );
}

type LockProcessIdentity = NonNullable<
  JsonBridgeStateStoreOptions["lockProcessIdentity"]
>;
type LockBootIdentity = NonNullable<
  JsonBridgeStateStoreOptions["lockBootIdentity"]
>;
type LockProcessUid = NonNullable<
  JsonBridgeStateStoreOptions["lockProcessUid"]
>;

describe("ingress recovery envelopes", () => {
  const identity = event();
  const payload = {
    action: "created" as const,
    prompt: "private exact recovery prompt",
    occurredAt: "2026-08-18T12:00:00.000Z",
    issueIdentifier: "MPI-1448",
  };

  it("accepts only canonical 32-byte base64url keys", () => {
    expect(parseCanonicalRecoveryKey(RECOVERY_KEY_A)).toHaveLength(32);
    expect(parseCanonicalRecoveryKey(`${RECOVERY_KEY_A}=`)).toBeUndefined();
    expect(parseCanonicalRecoveryKey(RECOVERY_KEY_A.slice(1))).toBeUndefined();
    expect(parseCanonicalRecoveryKey("_".repeat(44))).toBeUndefined();
  });

  it("decrypts retained-key envelopes after rotation and encrypts new work with the primary key", () => {
    const oldKeyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    const oldEnvelope = sealIngressRecoveryPayload(
      oldKeyring,
      identity,
      7,
      payload,
    );
    const rotatedKeyring = createIngressRecoveryKeyring(RECOVERY_KEY_B, [
      RECOVERY_KEY_A,
    ]);

    expect(
      openIngressRecoveryPayload(rotatedKeyring, identity, 7, oldEnvelope),
    ).toEqual(payload);
    const newEnvelope = sealIngressRecoveryPayload(
      rotatedKeyring,
      identity,
      8,
      payload,
    );
    expect(newEnvelope.keyId).toBe(rotatedKeyring.primary.id);
    expect(() =>
      openIngressRecoveryPayload(oldKeyring, identity, 8, newEnvelope),
    ).toThrow(IngressRecoveryEnvelopeError);
  });

  it("rejects tamper and identity, action, and sequence AAD swaps", () => {
    const keyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    const envelope = sealIngressRecoveryPayload(keyring, identity, 3, payload);
    const first = envelope.ciphertext[0] === "A" ? "B" : "A";

    expect(() =>
      openIngressRecoveryPayload(keyring, identity, 3, {
        ...envelope,
        ciphertext: `${first}${envelope.ciphertext.slice(1)}`,
      }),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      openIngressRecoveryPayload(
        keyring,
        { ...identity, webhookId: "webhook-swapped" },
        3,
        envelope,
      ),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      openIngressRecoveryPayload(
        keyring,
        { ...identity, executionId: "created:session-swapped" },
        3,
        envelope,
      ),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      openIngressRecoveryPayload(
        keyring,
        { ...identity, action: "prompted" },
        3,
        envelope,
      ),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      openIngressRecoveryPayload(keyring, identity, 4, envelope),
    ).toThrow(IngressRecoveryEnvelopeError);
  });

  it("enforces prompt and encoded field boundaries before decoding", () => {
    const keyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    const maximumPrompt = "x".repeat(256 * 1024);
    expect(() =>
      sealIngressRecoveryPayload(keyring, identity, 1, {
        ...payload,
        prompt: maximumPrompt,
      }),
    ).not.toThrow();
    expect(() =>
      sealIngressRecoveryPayload(keyring, identity, 1, {
        ...payload,
        prompt: `${maximumPrompt}x`,
      }),
    ).toThrow(IngressRecoveryEnvelopeError);

    const envelope = sealIngressRecoveryPayload(keyring, identity, 2, payload);
    expect(() =>
      openIngressRecoveryPayload(keyring, identity, 2, {
        ...envelope,
        nonce: "A".repeat(17),
      }),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      openIngressRecoveryPayload(keyring, identity, 2, {
        ...envelope,
        tag: "A".repeat(23),
      }),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      openIngressRecoveryPayload(keyring, identity, 2, {
        ...envelope,
        ciphertext: "A".repeat(Math.ceil(((258 * 1024 + 1) * 4) / 3)),
      }),
    ).toThrow(IngressRecoveryEnvelopeError);
  });

  it("rejects noncanonical timestamps and contradictory stop metadata", () => {
    const keyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    expect(() =>
      sealIngressRecoveryPayload(keyring, identity, 1, {
        ...payload,
        occurredAt: "2026-08-18T12:00:00Z",
      }),
    ).toThrow(IngressRecoveryEnvelopeError);
    expect(() =>
      sealIngressRecoveryPayload(
        keyring,
        {
          ...identity,
          action: "prompted",
          executionId: "activity-stop",
        },
        1,
        {
          action: "prompted",
          prompt: "stop",
          signal: "stop",
          stop: false,
          occurredAt: payload.occurredAt,
        },
      ),
    ).toThrow(IngressRecoveryEnvelopeError);
  });
});

describe("JsonBridgeStateStore", () => {
  it("reclaims an empty lock directory left after owner unlink", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    await fs.mkdir(lockPath, { mode: 0o700 });
    const originalRename = fs.rename.bind(fs);
    const renameSpy = vi
      .spyOn(fs, "rename")
      .mockImplementation(async (from, to) => {
        if (String(to) === lockPath) {
          try {
            await fs.stat(lockPath);
          } catch (error) {
            if (
              error instanceof Error &&
              "code" in error &&
              error.code === "ENOENT"
            ) {
              return originalRename(from, to);
            }
            throw error;
          }
          throw Object.assign(new Error("lock directory exists"), {
            code: "EEXIST",
          });
        }
        return originalRename(from, to);
      });
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-release-crash",
      lockTimeoutMs: 500,
      lockRetryMs: 1,
      ...TEST_LOCK_OPTIONS,
    });

    try {
      await expect(store.claimEvent(event())).resolves.toMatchObject({
        disposition: "claimed",
      });
      await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
        status: "claimed",
      });
      await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      renameSpy.mockRestore();
    }
  });

  it("retries current-process identity lookup after a transient unavailable result", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    let currentLookups = 0;
    const lockProcessIdentity: LockProcessIdentity = async () => {
      currentLookups += 1;
      return currentLookups === 1 ? undefined : CURRENT_PROCESS_IDENTITY;
    };
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-a",
      lockTimeoutMs: 100,
      lockProcessIdentity,
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Could not determine current process identity/,
    );
    await waitFor(async () => (await fs.readdir(tmpDir)).length === 0);
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(currentLookups).toBe(2);
  });

  it("rejects a stalled current-process identity lookup on the absolute deadline and retries cleanly", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const stalledIdentity = deferred<string | undefined>();
    let currentLookups = 0;
    const lockProcessIdentity: LockProcessIdentity = async (pid) => {
      expect(pid).toBe(process.pid);
      currentLookups += 1;
      return currentLookups === 1
        ? stalledIdentity.promise
        : CURRENT_PROCESS_IDENTITY;
    };
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-a",
      lockTimeoutMs: 250,
      lockProcessIdentity,
    });
    const startedAt = Date.now();

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(Date.now() - startedAt).toBeLessThan(500);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
    await waitFor(async () => (await fs.readdir(tmpDir)).length === 0);

    stalledIdentity.resolve(CURRENT_PROCESS_IDENTITY);
    await new Promise<void>((resolve) => setImmediate(resolve));
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(currentLookups).toBe(2);
  });

  it("bounds a stalled state temp sync and never publishes the timed-out mutation later", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const stalledSync = deferred();
    const originalOpen = fs.open.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      const openedPath = String(args[0]);
      if (
        openedPath.includes(".bridge-state.json.") &&
        openedPath.endsWith(".tmp")
      ) {
        vi.spyOn(handle, "sync").mockImplementation(() => stalledSync.promise);
      }
      return handle;
    });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 40,
    });
    const claim = store.claimEvent(event());

    try {
      await expect(
        Promise.race([
          claim,
          new Promise<never>((_resolve, reject) =>
            setTimeout(
              () => reject(new Error("state temp sync exceeded deadline")),
              250,
            ),
          ),
        ]),
      ).rejects.toThrow(/Timed out acquiring bridge state lock/);
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });

      stalledSync.resolve();
      await claim.catch(() => undefined);
      await new Promise<void>((resolve) => setImmediate(resolve));
      await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
    } finally {
      stalledSync.resolve();
      await claim.catch(() => undefined);
      openSpy.mockRestore();
    }
  });

  it("retains the lock until a timed-out state rename settles", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const releaseRename = deferred();
    const originalRename = fs.rename.bind(fs);
    let stalledStateRename = false;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (String(to) === storePath && !stalledStateRename) {
        stalledStateRename = true;
        await releaseRename.promise;
      }
      await originalRename(from, to);
    });
    const first = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 250,
      lockRetryMs: 1,
    });
    const second = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-b",
      lockTimeoutMs: 500,
      lockRetryMs: 1,
    });

    try {
      await expect(first.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      await expect(fs.stat(lockPath)).resolves.toBeDefined();
      let secondSettled = false;
      const secondClaim = second
        .claimEvent(
          event({
            webhookId: "webhook-after-late-state-rename",
            executionId: "created:session-after-late-state-rename",
            linearSessionId: "session-after-late-state-rename",
          }),
        )
        .finally(() => {
          secondSettled = true;
        });
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      expect(secondSettled).toBe(false);

      releaseRename.resolve();
      await expect(secondClaim).resolves.toMatchObject({
        disposition: "claimed",
      });
      await expect(
        second.getReceipt("webhook-after-late-state-rename"),
      ).resolves.toMatchObject({ status: "claimed" });
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
      await expect(
        second.getReceipt("webhook-after-late-state-rename"),
      ).resolves.toMatchObject({ status: "claimed", ownerId: "runtime-b" });
    } finally {
      releaseRename.resolve();
      renameSpy.mockRestore();
    }
  });

  it("bounds a stalled lock-directory read without reclaiming or mutating later", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "live-owner-stalled-directory-read";
    await writeLockOwner(lockPath, token, {
      pid: process.pid,
      hostname: os.hostname(),
      processIdentity: CURRENT_PROCESS_IDENTITY,
      uid: TEST_UID,
    });
    const releaseRead = deferred();
    const originalReaddir = fs.readdir.bind(fs);
    const readdirSpy = vi.spyOn(fs, "readdir").mockImplementation(async (...args) => {
      if (String(args[0]) === lockPath) {
        await releaseRead.promise;
      }
      return originalReaddir(...args);
    });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-b",
      lockTimeoutMs: 40,
      lockRetryMs: 1,
    });

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      await expect(fs.stat(path.join(lockPath, `${token}.json`))).resolves.toBeDefined();
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });

      releaseRead.resolve();
      await new Promise<void>((resolve) => setImmediate(resolve));
      await expect(fs.stat(path.join(lockPath, `${token}.json`))).resolves.toBeDefined();
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      releaseRead.resolve();
      readdirSpy.mockRestore();
    }
  });

  it("removes a candidate directory that is created after its deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const releaseMkdir = deferred();
    const originalMkdir = fs.mkdir.bind(fs);
    let stalledCandidatePath: string | undefined;
    const mkdirSpy = vi.spyOn(fs, "mkdir").mockImplementation(async (...args) => {
      const target = String(args[0]);
      if (target.endsWith(".candidate") && stalledCandidatePath === undefined) {
        stalledCandidatePath = target;
        await releaseMkdir.promise;
      }
      return originalMkdir(...args);
    });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 250,
    });

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      expect(stalledCandidatePath).toBeDefined();
      releaseMkdir.resolve();
      await waitFor(async () => {
        try {
          await fs.stat(stalledCandidatePath!);
          return false;
        } catch (error) {
          return (
            error instanceof Error &&
            "code" in error &&
            error.code === "ENOENT"
          );
        }
      });
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      releaseMkdir.resolve();
      mkdirSpy.mockRestore();
    }
  });

  it("bounds stalled recovery reads and retries without late side effects", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 250,
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await store.claimEvent(event(), {
      action: "created",
      prompt: "bounded recovery read",
      occurredAt: "2026-08-18T12:00:00.000Z",
    });
    const releaseRead = deferred();
    const originalReadFile = fs.readFile.bind(fs);
    let stalled = true;
    const readSpy = vi.spyOn(fs, "readFile").mockImplementation(async (...args) => {
      if (stalled && String(args[0]) === storePath) {
        await releaseRead.promise;
      }
      return originalReadFile(...args);
    });

    try {
      await expect(store.assertRecoverableEventsAvailable()).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      stalled = false;
      releaseRead.resolve();
      await expect(store.assertRecoverableEventsAvailable()).resolves.toBeUndefined();
      await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
        status: "claimed",
      });
    } finally {
      stalled = false;
      releaseRead.resolve();
      readSpy.mockRestore();
    }
  });

  it("bounds stalled lock release and lets the mutation tail serve later work", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const releaseRmdir = deferred();
    const originalRmdir = fs.rmdir.bind(fs);
    let stalledFirstRelease = false;
    const rmdirSpy = vi.spyOn(fs, "rmdir").mockImplementation(async (...args) => {
      if (String(args[0]) === lockPath && !stalledFirstRelease) {
        stalledFirstRelease = true;
        await releaseRmdir.promise;
      }
      return originalRmdir(...args);
    });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 250,
      lockRetryMs: 1,
    });
    const firstClaim = store.claimEvent(event());

    try {
      await expect(
        Promise.race([
          firstClaim,
          new Promise<never>((_resolve, reject) =>
            setTimeout(
              () => reject(new Error("lock release exceeded deadline")),
              500,
            ),
          ),
        ]),
      ).rejects.toThrow(/Timed out acquiring bridge state lock/);
      await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
        status: "claimed",
      });

      await expect(
        store.claimEvent(
          event({
            webhookId: "webhook-after-stalled-release",
            executionId: "created:session-after-stalled-release",
            linearSessionId: "session-after-stalled-release",
          }),
        ),
      ).resolves.toMatchObject({ disposition: "claimed" });
    } finally {
      releaseRmdir.resolve();
      await firstClaim.catch(() => undefined);
      rmdirSpy.mockRestore();
    }
  });

  it("reclaims its own lock after a one-shot owner unlink failure", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const originalUnlink = fs.unlink.bind(fs);
    let failedOwnerUnlink = false;
    const unlinkSpy = vi.spyOn(fs, "unlink").mockImplementation(async (...args) => {
      const target = String(args[0]);
      if (
        !failedOwnerUnlink &&
        path.dirname(target) === lockPath &&
        target.endsWith(".json")
      ) {
        failedOwnerUnlink = true;
        throw Object.assign(new Error("synthetic owner unlink failure"), {
          code: "EIO",
        });
      }
      return originalUnlink(...args);
    });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 500,
      lockRetryMs: 1,
    });

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        "synthetic owner unlink failure",
      );
      expect(await fs.readdir(lockPath)).toHaveLength(1);
      await expect(
        store.claimEvent(
          event({
            webhookId: "webhook-after-owner-unlink-failure",
            executionId: "created:session-after-owner-unlink-failure",
            linearSessionId: "session-after-owner-unlink-failure",
          }),
        ),
      ).resolves.toMatchObject({ disposition: "claimed" });
      await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      unlinkSpy.mockRestore();
      await fs.rm(lockPath, { recursive: true, force: true });
    }
  });

  it("times out a stalled live-owner identity lookup without later reclaiming its lock", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "live-owner-stalled-identity";
    const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
    await writeLockOwner(lockPath, token, {
      pid: liveOwnerPid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
      uid: TEST_UID,
    });
    const stalledIdentity = deferred<string | undefined>();
    const lockProcessIdentity: LockProcessIdentity = async (pid) =>
      pid === process.pid ? CURRENT_PROCESS_IDENTITY : stalledIdentity.promise;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-b",
      lockTimeoutMs: 40,
      lockProcessIdentity,
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async () => TEST_UID,
    });
    const startedAt = Date.now();

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(Date.now() - startedAt).toBeLessThan(200);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
    await waitFor(async () =>
      (await fs.readdir(tmpDir)).every(
        (entry) => entry === "bridge-state.json.lock",
      ),
    );
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);

    stalledIdentity.resolve(`linux-boot:${BOOT_A}:proc-start:201`);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
  });

  it("bounds a stalled boot lookup, ignores its late result, and retries it on the next mutation", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "previous-boot-stalled-lookup";
    const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
    await writeLockOwner(lockPath, token, {
      pid: liveOwnerPid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
    });
    const stalledBoot = deferred<string | undefined>();
    let bootLookups = 0;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-reboot",
      lockTimeoutMs: 250,
      lockProcessIdentity: async (pid) => {
        if (pid !== process.pid) {
          throw new Error("stalled boot lookup inspected process birth");
        }
        return `linux-boot:${BOOT_B}:proc-start:300`;
      },
      lockBootIdentity: async () => {
        bootLookups += 1;
        return bootLookups === 1 ? stalledBoot.promise : BOOT_B;
      },
      lockProcessUid: async () => {
        throw new Error("stalled boot lookup inspected process uid");
      },
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);

    stalledBoot.resolve(BOOT_B);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(bootLookups).toBe(2);
  });

  it("retries a malformed current boot identity instead of caching it", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
    await writeLockOwner(lockPath, "previous-boot-invalid-current", {
      pid: liveOwnerPid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
    });
    let bootLookups = 0;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-reboot",
      lockTimeoutMs: 250,
      lockProcessIdentity: async () =>
        `linux-boot:${BOOT_B}:proc-start:300`,
      lockBootIdentity: async () => {
        bootLookups += 1;
        return bootLookups === 1 ? "not-a-boot-uuid" : BOOT_B;
      },
      lockProcessUid: async () => {
        throw new Error("cross-boot retry inspected process uid");
      },
    });

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(bootLookups).toBe(2);
  });

  it("bounds a stalled live-owner uid lookup without later reclaiming its lock", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "same-boot-stalled-uid";
    const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
    await writeLockOwner(lockPath, token, {
      pid: liveOwnerPid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
      uid: 12_345,
    });
    const stalledUid = deferred<number | undefined>();
    const inspectedBirthPids: number[] = [];
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-same-boot",
      lockTimeoutMs: 250,
      lockProcessIdentity: async (pid) => {
        inspectedBirthPids.push(pid);
        return pid === process.pid ? CURRENT_PROCESS_IDENTITY : undefined;
      },
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async () => stalledUid.promise,
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(inspectedBirthPids).toEqual([process.pid]);
    await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);

    stalledUid.resolve(12_346);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
  });

  it("parses only strict kernel boot and process-birth identities", () => {
    expect(
      parseBootSessionUuid("24F0C7A0-3DD9-4B33-869C-8F07D374EBD8\n"),
    ).toBe("24f0c7a0-3dd9-4b33-869c-8f07d374ebd8");
    expect(parseBootSessionUuid("secret\nnot-a-uuid\n")).toBeUndefined();
    expect(parseDarwinProcessStartTime("1724000000:862743\n")).toBe(
      "1724000000:862743",
    );
    expect(parseDarwinProcessStartTime("1724000000:1000000\n")).toBeUndefined();
    expect(parseDarwinProcessStartTime("1724000000:12secret\n")).toBeUndefined();
    expect(
      parseLinuxProcessStartTicks(
        "123 (node worker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 424242 20",
      ),
    ).toBe("424242");
    expect(parseLinuxProcessStartTicks("123 (node) S too-short")).toBeUndefined();
    expect(
      parseLockProcessIdentityBoot(
        `linux-boot:${BOOT_A}:proc-start:424242`,
      ),
    ).toBe(BOOT_A);
    expect(
      parseLockProcessIdentityBoot(
        `darwin-boot:${BOOT_B}:proc-start:1724000000:862743`,
      ),
    ).toBe(BOOT_B);
    expect(
      parseLockProcessIdentityBoot(
        `darwin-boot:${BOOT_B}:proc-start:1724000000:1000000`,
      ),
    ).toBeUndefined();
    expect(
      parseLockProcessIdentityBoot(`linux-boot:${BOOT_A}:proc-start:12:secret`),
    ).toBeUndefined();
    expect(
      parseLinuxProcessRealUid(
        "Name:\tnode\nUid:\t501\t502\t503\t504\nGid:\t20\t20\t20\t20\n",
      ),
    ).toBe(501);
    expect(parseLinuxProcessRealUid("Uid:\t501\t502\tsecret\t504\n")).toBeUndefined();
    expect(darwinProcessRealUidArgs(123)).toEqual([
      "-o",
      "ruid=",
      "-p",
      "123",
    ]);
    expect(parseDarwinProcessRealUid("  501\n")).toBe(501);
    expect(
      parseDarwinProcessRealUid("501\nprivate-data\n"),
    ).toBeUndefined();
    expect(parseDarwinProcessRealUid("4294967296\n")).toBeUndefined();
  });

  it("scopes Linux start ticks and Darwin microsecond start times to a boot", () => {
    const bootA = "24F0C7A0-3DD9-4B33-869C-8F07D374EBD8";
    const bootB = "79326562-1A4C-42A2-AC6D-00478B65895D";
    const stat =
      "123 (node worker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 424242 20";
    const linuxBeforeReboot = buildLinuxLockProcessIdentity(bootA, stat);
    const linuxAfterReboot = buildLinuxLockProcessIdentity(bootB, stat);
    expect(linuxBeforeReboot).toBe(
      "linux-boot:24f0c7a0-3dd9-4b33-869c-8f07d374ebd8:proc-start:424242",
    );
    expect(linuxAfterReboot).not.toBe(linuxBeforeReboot);

    const darwinFirst = buildDarwinLockProcessIdentity(
      bootA,
      "1724000000:862743\n",
    );
    const darwinRecycled = buildDarwinLockProcessIdentity(
      bootA,
      "1724000000:862744\n",
    );
    expect(darwinFirst).toBe(
      "darwin-boot:24f0c7a0-3dd9-4b33-869c-8f07d374ebd8:proc-start:1724000000:862743",
    );
    expect(darwinRecycled).not.toBe(darwinFirst);
  });

  it("writes a default boot-scoped process-birth identity", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const originalRename = fs.rename.bind(fs);
    let persistedIdentity: string | undefined;
    let persistedUid: number | undefined;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (String(to) === lockPath) {
        const [ownerName] = await fs.readdir(String(from));
        const owner = JSON.parse(
          await fs.readFile(path.join(String(from), ownerName!), "utf8"),
        ) as { processIdentity?: string; uid?: number };
        persistedIdentity = owner.processIdentity;
        persistedUid = owner.uid;
      }
      await originalRename(from, to);
    });

    try {
      const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
      await store.claimEvent(event());
    } finally {
      renameSpy.mockRestore();
    }
    if (process.platform === "darwin") {
      expect(persistedIdentity).toMatch(
        /^darwin-boot:[0-9a-f-]{36}:proc-start:[1-9]\d*:\d+$/,
      );
    } else if (process.platform === "linux") {
      expect(persistedIdentity).toMatch(
        /^linux-boot:[0-9a-f-]{36}:proc-start:\d+$/,
      );
    }
    expect(persistedUid).toBe(TEST_UID);

    if (process.platform === "darwin" || process.platform === "linux") {
      await writeLockOwner(lockPath, "same-process-different-uid", {
        pid: process.pid,
        hostname: os.hostname(),
        processIdentity: persistedIdentity,
        uid: TEST_UID === 0xffff_ffff ? TEST_UID - 1 : TEST_UID + 1,
      });
      const contender = new JsonBridgeStateStore(storePath, {
        ownerId: "runtime-default-uid-provider",
      });
      await expect(
        contender.claimEvent(
          event({ webhookId: "webhook-2", executionId: "created:session-2" }),
        ),
      ).resolves.toMatchObject({ disposition: "claimed" });
      await waitFor(async () =>
        fs.stat(lockPath).then(
          () => false,
          (error: unknown) =>
            error instanceof Error &&
            (error as NodeJS.ErrnoException).code === "ENOENT",
        ),
      );
    }
  });

  it("rejects queued work that reaches the mutation tail after its absolute deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-a",
      lockTimeoutMs: 25,
    });
    const gate = deferred();
    (
      store as unknown as {
        mutationTail: Promise<void>;
      }
    ).mutationTail = gate.promise;

    const claim = store.claimEvent(event());
    const blockedUntil = Date.now() + 50;
    while (Date.now() < blockedUntil) {
      // Keep the timer callback pending so resolving the tail queues admission first.
    }
    gate.resolve();

    await expect(claim).rejects.toThrow(/Timed out acquiring bridge state lock/);
    await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("does not mutate after a retry delay crosses the absolute lock deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const lockToken = "live-owner";
    await fs.mkdir(lockPath, { mode: 0o700 });
    await fs.writeFile(
      path.join(lockPath, `${lockToken}.json`),
      `${JSON.stringify({
        token: lockToken,
        pid: process.pid,
        hostname: os.hostname(),
      })}\n`,
      { mode: 0o600 },
    );
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-a",
      lockTimeoutMs: 50,
      lockRetryMs: 100,
    });
    const removeLock = setTimeout(() => {
      void fs.rm(lockPath, { recursive: true, force: true });
    }, 60);

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      clearTimeout(removeLock);
      await fs.rm(lockPath, { recursive: true, force: true });
    }
  });

  it("releases an acquired lock without mutating when rename completes after the deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-a",
      lockTimeoutMs: 25,
    });
    const originalRename = fs.rename.bind(fs);
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (String(to) === lockPath) {
        const blockedUntil = Date.now() + 40;
        while (Date.now() < blockedUntil) {
          // Model a filesystem rename that begins in time and completes too late.
        }
      }
      await originalRename(from, to);
    });

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
      await waitFor(async () =>
        fs.stat(lockPath).then(
          () => false,
          (error: unknown) =>
            error instanceof Error &&
            (error as NodeJS.ErrnoException).code === "ENOENT",
        ),
      );
    } finally {
      renameSpy.mockRestore();
    }
  });

  it("bounds lock contention well inside Linear's five-second delivery deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const writer = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    const contender = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-b",
      lockTimeoutMs: 100,
    });
    const renameEntered = deferred();
    const releaseRename = deferred();
    const originalRename = fs.rename.bind(fs);
    let held = false;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (!held && String(to) === storePath) {
        held = true;
        renameEntered.resolve();
        await releaseRename.promise;
      }
      await originalRename(from, to);
    });

    const activeWrite = writer.claimEvent(event());
    await renameEntered.promise;
    const startedAt = Date.now();
    try {
      await expect(
        contender.claimEvent(
          event({ webhookId: "webhook-2", executionId: "created:session-2" }),
        ),
      ).rejects.toThrow(/Timed out acquiring bridge state lock/);
      expect(Date.now() - startedAt).toBeLessThan(1_000);
    } finally {
      releaseRename.resolve();
      await activeWrite;
      renameSpy.mockRestore();
    }
  });

  it("does not break an old lock while its owning process is alive", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const writer = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    const contender = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-b",
      lockTimeoutMs: 75,
    });
    const renameEntered = deferred();
    const releaseRename = deferred();
    const originalRename = fs.rename.bind(fs);
    let held = false;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (!held && String(to) === storePath) {
        held = true;
        renameEntered.resolve();
        await releaseRename.promise;
      }
      await originalRename(from, to);
    });

    const activeWrite = writer.claimEvent(event());
    await renameEntered.promise;
    const ancient = new Date("2000-01-01T00:00:00.000Z");
    const [ownerName] = await fs.readdir(lockPath);
    await fs.utimes(path.join(lockPath, ownerName!), ancient, ancient);
    await fs.utimes(lockPath, ancient, ancient);
    try {
      await expect(
        contender.claimEvent(
          event({ webhookId: "webhook-2", executionId: "created:session-2" }),
        ),
      ).rejects.toThrow(/Timed out acquiring bridge state lock/);
      expect(await fs.readdir(lockPath)).toEqual([ownerName]);
    } finally {
      releaseRename.resolve();
      await activeWrite;
      renameSpy.mockRestore();
    }
  });

  it("preserves a same-host lock when the live PID has the recorded process identity", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "matching-live-owner";
    await writeLockOwner(lockPath, token, {
      pid: process.pid,
      hostname: os.hostname(),
      processIdentity: CURRENT_PROCESS_IDENTITY,
      uid: TEST_UID,
    });
    const lockProcessIdentity: LockProcessIdentity = async () =>
      CURRENT_PROCESS_IDENTITY;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-b",
      lockTimeoutMs: 50,
      lockProcessIdentity,
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async () => TEST_UID,
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
  });

  it("compares Darwin real uid to the recorded real uid when effective uid differs", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "matching-real-uid-owner";
    const realUid = 501;
    const effectiveUid = 0;
    const uidByPsColumn: Record<string, number> = {
      "ruid=": realUid,
      "uid=": effectiveUid,
    };
    await writeLockOwner(lockPath, token, {
      pid: process.pid,
      hostname: os.hostname(),
      processIdentity: CURRENT_PROCESS_IDENTITY,
      uid: realUid,
    });
    const inspectedUidPids: number[] = [];
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-different-effective-uid",
      lockTimeoutMs: 250,
      lockProcessIdentity: async () => CURRENT_PROCESS_IDENTITY,
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async (pid) => {
        inspectedUidPids.push(pid);
        return uidByPsColumn[darwinProcessRealUidArgs(pid)[1]!];
      },
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(inspectedUidPids.length).toBeGreaterThan(0);
    expect(inspectedUidPids.every((pid) => pid === process.pid)).toBe(true);
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
  });

  it("reclaims an intermediate-format lock from a different boot without inspecting the live PID", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
    await writeLockOwner(lockPath, "previous-boot-owner", {
      pid: liveOwnerPid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
    });
    const inspectedPids: number[] = [];
    const lockProcessIdentity: LockProcessIdentity = async (pid) => {
      inspectedPids.push(pid);
      if (pid !== process.pid) {
        throw new Error("cross-boot reclaim inspected process birth");
      }
      return `linux-boot:${BOOT_B}:proc-start:300`;
    };
    const lockBootIdentity: LockBootIdentity = async () => BOOT_B;
    const lockProcessUid: LockProcessUid = async () => {
      throw new Error("cross-boot reclaim inspected process uid");
    };
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-reboot",
      lockTimeoutMs: 250,
      lockProcessIdentity,
      lockBootIdentity,
      lockProcessUid,
    });

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(inspectedPids).toEqual([process.pid]);
    await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("reclaims a same-boot lock owned by a different uid without inspecting process birth", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
    await writeLockOwner(lockPath, "different-user-owner", {
      pid: liveOwnerPid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
      uid: 12_345,
    });
    const inspectedBirthPids: number[] = [];
    const inspectedUidPids: number[] = [];
    const lockProcessIdentity: LockProcessIdentity = async (pid) => {
      inspectedBirthPids.push(pid);
      if (pid !== process.pid) {
        throw new Error("different-uid reclaim inspected process birth");
      }
      return CURRENT_PROCESS_IDENTITY;
    };
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-different-user",
      lockTimeoutMs: 250,
      lockProcessIdentity,
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async (pid) => {
        inspectedUidPids.push(pid);
        return 12_346;
      },
    });

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(inspectedUidPids).toEqual([liveOwnerPid]);
    expect(inspectedBirthPids).toEqual([process.pid]);
    await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it.each([
    ["matching", 12_345],
    ["unavailable", undefined],
  ])(
    "preserves a same-boot lock when the live uid is %s and process birth is unavailable",
    async (_kind, liveUid) => {
      const storePath = path.join(tmpDir, "bridge-state.json");
      const lockPath = `${storePath}.lock`;
      const token = `unqueryable-birth-${String(_kind)}`;
      const liveOwnerPid = process.pid === 1 ? process.ppid : 1;
      await writeLockOwner(lockPath, token, {
        pid: liveOwnerPid,
        hostname: os.hostname(),
        processIdentity: `linux-boot:${BOOT_A}:proc-start:200`,
        uid: 12_345,
      });
      const inspectedBirthPids: number[] = [];
      const inspectedUidPids: number[] = [];
      const store = new JsonBridgeStateStore(storePath, {
        ownerId: "runtime-unqueryable-owner",
        lockTimeoutMs: 250,
        lockProcessIdentity: async (pid) => {
          inspectedBirthPids.push(pid);
          return pid === process.pid ? CURRENT_PROCESS_IDENTITY : undefined;
        },
        lockBootIdentity: async () => BOOT_A,
        lockProcessUid: async (pid) => {
          inspectedUidPids.push(pid);
          return liveUid;
        },
      });

      await expect(store.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      expect(inspectedUidPids.length).toBeGreaterThan(0);
      expect(inspectedUidPids.every((pid) => pid === liveOwnerPid)).toBe(true);
      expect(inspectedBirthPids).toContain(process.pid);
      const inspectedOwnerBirthPids = inspectedBirthPids.filter(
        (pid) => pid !== process.pid,
      );
      expect(inspectedOwnerBirthPids.length).toBeGreaterThan(0);
      expect(
        inspectedOwnerBirthPids.every((pid) => pid === liveOwnerPid),
      ).toBe(true);
      expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
      await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
    },
  );

  it("reclaims a same-host lock when the live PID belongs to a recycled process", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    await writeLockOwner(lockPath, "recycled-owner", {
      pid: process.pid,
      hostname: os.hostname(),
      processIdentity: `linux-boot:${BOOT_A}:proc-start:99`,
      uid: TEST_UID,
    });
    const lockProcessIdentity: LockProcessIdentity = async () =>
      CURRENT_PROCESS_IDENTITY;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-recycle",
      lockTimeoutMs: 250,
      lockProcessIdentity,
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async () => TEST_UID,
    });

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("reclaims a same-host lock after its recorded process exits", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const deadPid = 2_147_483_647;
    await writeLockOwner(lockPath, "dead-owner", {
      pid: deadPid,
      hostname: os.hostname(),
      processIdentity: "process-start-dead",
    });
    const inspectedPids: number[] = [];
    const lockProcessIdentity: LockProcessIdentity = async (pid) => {
      inspectedPids.push(pid);
      return CURRENT_PROCESS_IDENTITY;
    };
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-exit",
      lockTimeoutMs: 250,
      lockProcessIdentity,
    });

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(inspectedPids).toEqual([process.pid]);
    await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it.each([
    ["legacy", {}],
    ["malformed", { processIdentity: 42 }],
    ["boot-scoped without uid", { processIdentity: CURRENT_PROCESS_IDENTITY }],
  ])("preserves a %s same-host owner record for safe migration", async (_kind, extra) => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const token = "unverifiable-owner";
    await writeLockOwner(lockPath, token, {
      pid: process.pid,
      hostname: os.hostname(),
      ...extra,
    });
    const lockProcessIdentity: LockProcessIdentity = async () =>
      CURRENT_PROCESS_IDENTITY;
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-migration",
      lockTimeoutMs: 50,
      lockProcessIdentity,
      lockBootIdentity: async () => BOOT_A,
      lockProcessUid: async () => {
        throw new Error("legacy owner unexpectedly inspected process uid");
      },
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
  });

  it("does not unlink a replacement lock when an earlier owner finishes", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    const writer = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    const renameEntered = deferred();
    const releaseRename = deferred();
    const originalRename = fs.rename.bind(fs);
    let held = false;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (!held && String(to) === storePath) {
        held = true;
        renameEntered.resolve();
        await releaseRename.promise;
      }
      await originalRename(from, to);
    });

    const activeWrite = writer.claimEvent(event());
    await renameEntered.promise;
    await fs.rm(lockPath, { recursive: true });
    await fs.mkdir(lockPath, { mode: 0o700 });
    const replacementToken = "replacement-owner";
    await fs.writeFile(
      path.join(lockPath, `${replacementToken}.json`),
      `${JSON.stringify({
        token: replacementToken,
        pid: process.pid,
        hostname: os.hostname(),
      })}\n`,
      { mode: 0o600 },
    );

    releaseRename.resolve();
    try {
      await activeWrite;
      expect(await fs.readdir(lockPath)).toEqual([`${replacementToken}.json`]);
    } finally {
      renameSpy.mockRestore();
      await fs.rm(lockPath, { recursive: true, force: true });
    }
  });

  it.each([
    ["legacy", {}],
    ["malformed", { processIdentity: 42 }],
  ])("reclaims a dead %s owner record during migration", async (kind, extra) => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    await writeLockOwner(lockPath, `abandoned-${kind}-owner`, {
      pid: 2_147_483_647,
      hostname: os.hostname(),
      ...extra,
    });

    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-crash",
      lockTimeoutMs: 250,
    });
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("syncs each newly created state-directory parent before any state write", async () => {
    const firstCreatedDirectory = path.join(tmpDir, "state");
    const stateDirectory = path.join(firstCreatedDirectory, "nested");
    const storePath = path.join(stateDirectory, "bridge-state.json");
    const operations: string[] = [];
    let failCreatedParentSync = true;
    const originalMkdir = fs.mkdir.bind(fs);
    const originalOpen = fs.open.bind(fs);
    const mkdirSpy = vi.spyOn(fs, "mkdir").mockImplementation(async (...args) => {
      const result = await originalMkdir(...args);
      if (String(args[0]) === stateDirectory) {
        operations.push("mkdir-state-directory");
      }
      return result;
    });
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      const openedPath = String(args[0]);
      if (openedPath === tmpDir || openedPath === firstCreatedDirectory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          operations.push(`sync-parent:${path.basename(openedPath)}`);
          if (openedPath === firstCreatedDirectory && failCreatedParentSync) {
            throw new Error("synthetic created-parent sync failure");
          }
          await originalSync();
        });
      }
      return handle;
    });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
    });

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        "synthetic created-parent sync failure",
      );
      expect(operations).toEqual([
        "mkdir-state-directory",
        `sync-parent:${path.basename(tmpDir)}`,
        "sync-parent:state",
      ]);
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });

      operations.length = 0;
      failCreatedParentSync = false;
      await expect(store.claimEvent(event())).resolves.toMatchObject({
        disposition: "claimed",
      });
      expect(operations).toEqual([
        "mkdir-state-directory",
        `sync-parent:${path.basename(tmpDir)}`,
        "sync-parent:state",
      ]);

      operations.length = 0;
      const restartedStore = new JsonBridgeStateStore(storePath, {
        ...TEST_LOCK_OPTIONS,
        ownerId: "runtime-a",
      });
      await expect(restartedStore.claimEvent(event())).resolves.toMatchObject({
        disposition: "claimed",
      });
      expect(operations).toEqual([
        "mkdir-state-directory",
        `sync-parent:${path.basename(tmpDir)}`,
        "sync-parent:state",
      ]);
    } finally {
      mkdirSpy.mockRestore();
      openSpy.mockRestore();
    }
  });

  it("times out a stalled ancestor sync before lock creation and closes after settlement", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const stalledSync = deferred();
    const originalOpen = fs.open.bind(fs);
    const originalMkdir = fs.mkdir.bind(fs);
    let candidateCreated = false;
    let directoryClosed = false;
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === tmpDir) {
        const originalClose = handle.close.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(() => stalledSync.promise);
        vi.spyOn(handle, "close").mockImplementation(async () => {
          directoryClosed = true;
          await originalClose();
        });
      }
      return handle;
    });
    const mkdirSpy = vi
      .spyOn(fs, "mkdir")
      .mockImplementation(async (...args) => {
        if (String(args[0]).endsWith(".candidate")) {
          candidateCreated = true;
        }
        return originalMkdir(...args);
      });
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 40,
    });

    try {
      await expect(store.claimEvent(event())).rejects.toThrow(
        /Timed out acquiring bridge state lock/,
      );
      expect(candidateCreated).toBe(false);
      expect(directoryClosed).toBe(false);
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });

      stalledSync.resolve();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(directoryClosed).toBe(true);
      await expect(fs.stat(storePath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      stalledSync.resolve();
      await new Promise<void>((resolve) => setImmediate(resolve));
      openSpy.mockRestore();
      mkdirSpy.mockRestore();
    }
  });

  it("syncs and secures each temporary state file before rename, then syncs the directory", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const directory = path.dirname(storePath);
    const operations: string[] = [];
    const originalOpen = fs.open.bind(fs);
    const originalRename = fs.rename.bind(fs);
    const originalChmod = fs.chmod.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      const openedPath = String(args[0]);
      if (openedPath.includes(".bridge-state.json.") && openedPath.endsWith(".tmp")) {
        const originalHandleChmod = handle.chmod.bind(handle);
        const originalHandleSync = handle.sync.bind(handle);
        vi.spyOn(handle, "chmod").mockImplementation(async (mode) => {
          operations.push(`temp-chmod:${mode.toString(8)}`);
          await originalHandleChmod(mode);
        });
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          operations.push("temp-sync");
          await originalHandleSync();
        });
      } else if (openedPath === directory) {
        const originalHandleSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          operations.push("directory-sync");
          await originalHandleSync();
        });
      }
      return handle;
    });
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (String(to) === storePath) {
        operations.push("rename");
      }
      await originalRename(from, to);
    });
    const chmodSpy = vi.spyOn(fs, "chmod").mockImplementation(async (target, mode) => {
      if (String(target) === storePath) {
        operations.push(`target-chmod:${mode.toString(8)}`);
      }
      await originalChmod(target, mode);
    });

    try {
      const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
      await store.claimEvent(event());
      expect(operations).toEqual([
        "directory-sync",
        "temp-chmod:600",
        "temp-sync",
        "rename",
        "directory-sync",
        "temp-chmod:600",
        "temp-sync",
        "rename",
        "directory-sync",
      ]);
      expect((await fs.stat(storePath)).mode & 0o777).toBe(0o600);
    } finally {
      openSpy.mockRestore();
      renameSpy.mockRestore();
      chmodSpy.mockRestore();
    }
  });

  it("durably receives and claims a valid event", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const now = Date.parse("2026-08-18T12:00:00.000Z");
    const store = new JsonBridgeStateStore(storePath, {
      now: () => now,
      ownerId: "runtime-a",
    });

    await expect(store.claimEvent(event())).resolves.toEqual({
      disposition: "claimed",
      receipt: expect.objectContaining({
        webhookId: "webhook-1",
        executionId: "created:session-1",
        status: "claimed",
        receivedAt: "2026-08-18T12:00:00.000Z",
        claimedAt: "2026-08-18T12:00:00.000Z",
        outcome: {
          httpStatus: 200,
          result: "accepted",
          disposition: "claimed",
        },
      }),
    });

    const reloaded = new JsonBridgeStateStore(storePath, { ownerId: "runtime-b" });
    await expect(reloaded.getReceipt("webhook-1")).resolves.toMatchObject({
      status: "claimed",
      ownerId: "runtime-a",
    });
    await expect(reloaded.getClaim("created:session-1")).resolves.toMatchObject({
      webhookId: "webhook-1",
      status: "claimed",
      ownerId: "runtime-a",
    });
  });

  it("persists only encrypted recovery content and removes the envelope at terminalization", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const keyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: keyring,
    });
    const privatePrompt = "private prompt body that must stay encrypted";
    const privateIssue = "PRIVATE-ISSUE-TITLE";
    await store.claimEvent(event(), {
      action: "created",
      prompt: privatePrompt,
      issueIdentifier: privateIssue,
      occurredAt: "2026-08-18T12:00:00.000Z",
    });

    const rawAccepted = await fs.readFile(storePath, "utf8");
    expect(rawAccepted).not.toContain(privatePrompt);
    expect(rawAccepted).not.toContain(privateIssue);
    await expect(store.assertRecoverableEventsAvailable()).resolves.toBeUndefined();
    await expect(store.listRecoverableEvents()).resolves.toEqual([
      expect.objectContaining({
        available: true,
        sequence: 1,
        payload: expect.objectContaining({ prompt: privatePrompt }),
      }),
    ]);

    await expect(store.markDispatchStarted("webhook-1")).resolves.toBe(
      "dispatch_started",
    );
    const persisted = JSON.parse(
      await fs.readFile(storePath, "utf8"),
    ) as { receipts: Record<string, Record<string, unknown>> };
    expect(persisted.receipts["webhook-1"]).toMatchObject({
      recoveryEnvelope: expect.any(Object),
      recoverySequence: 1,
    });
    await expect(store.listRecoverableEvents()).resolves.toEqual([]);
    await store.completeEvent("webhook-1");
    const terminal = JSON.parse(await fs.readFile(storePath, "utf8")) as {
      receipts: Record<string, Record<string, unknown>>;
    };
    expect(terminal.receipts["webhook-1"]).not.toHaveProperty(
      "recoveryEnvelope",
    );
    expect(terminal.receipts["webhook-1"]).not.toHaveProperty(
      "recoverySequence",
    );

    const privateSignal = "private-follow-up-signal";
    await store.claimEvent(
      event({
        webhookId: "webhook-private-prompted",
        executionId: "activity-private-prompted",
        action: "prompted",
      }),
      {
        action: "prompted",
        prompt: privatePrompt,
        signal: privateSignal,
        stop: false,
        occurredAt: "2026-08-18T12:00:01.000Z",
      },
    );
    const rawPrompted = await fs.readFile(storePath, "utf8");
    expect(rawPrompted).not.toContain(privatePrompt);
    expect(rawPrompted).not.toContain(privateSignal);
    await expect(
      store.claimEvent(
        event({
          webhookId: "webhook-private-prompted-duplicate",
          executionId: "activity-private-prompted",
          action: "prompted",
        }),
        {
          action: "prompted",
          prompt: privatePrompt,
          signal: privateSignal,
          stop: false,
          occurredAt: "2026-08-18T12:00:01.000Z",
        },
      ),
    ).resolves.toMatchObject({ disposition: "superseded" });
    const afterSupersede = JSON.parse(
      await fs.readFile(storePath, "utf8"),
    ) as { receipts: Record<string, Record<string, unknown>> };
    expect(
      afterSupersede.receipts["webhook-private-prompted-duplicate"],
    ).not.toHaveProperty("recoveryEnvelope");
  });

  it("repairs only true legacy marker-free receipts and rejects asymmetric state", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const legacy = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-before-upgrade",
    });
    await legacy.claimEvent(event());
    const upgraded = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-after-upgrade",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await expect(upgraded.assertRecoverableEventsAvailable()).rejects.toBeInstanceOf(
      LegacyIngressRecoveryUnavailableError,
    );
    await expect(
      upgraded.claimEvent(
        event(),
        {
          action: "created",
          prompt: "signed redelivery body",
          occurredAt: "2026-08-18T12:00:00.000Z",
        },
        { repairLegacyOnly: true },
      ),
    ).resolves.toMatchObject({ disposition: "claimed" });
    await expect(upgraded.assertRecoverableEventsAvailable()).resolves.toBeUndefined();

    const raw = JSON.parse(await fs.readFile(storePath, "utf8")) as {
      receipts: Record<string, Record<string, unknown>>;
    };
    delete raw.receipts["webhook-1"]!.recoveryEnvelope;
    await fs.writeFile(storePath, JSON.stringify(raw), { mode: 0o600 });
    const asymmetric = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-after-tamper",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await expect(asymmetric.assertRecoverableEventsAvailable()).rejects.toBeInstanceOf(
      IngressRecoveryEnvelopeError,
    );
    await expect(
      asymmetric.claimEvent(
        event(),
        {
          action: "created",
          prompt: "must not repair asymmetric state",
          occurredAt: "2026-08-18T12:00:00.000Z",
        },
        { repairLegacyOnly: true },
      ),
    ).rejects.toBeInstanceOf(LegacyIngressRecoveryMismatchError);
  });

  it("fails closed when a pending envelope key is unavailable", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const writer = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await writer.claimEvent(event(), {
      action: "created",
      prompt: "encrypted with retired key",
      occurredAt: "2026-08-18T12:00:00.000Z",
    });
    const reader = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-b",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_B),
    });

    await expect(reader.assertRecoverableEventsAvailable()).rejects.toBeInstanceOf(
      IngressRecoveryEnvelopeError,
    );
  });

  it("orders same-millisecond stop fences by the durable recovery sequence", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    const occurredAt = "2026-08-18T12:00:00.000Z";
    const olderPrompt = event({
      webhookId: "webhook-older-prompt",
      executionId: "activity-older-prompt",
      action: "prompted",
    });
    const stop = event({
      webhookId: "webhook-stop",
      executionId: "activity-stop",
      action: "prompted",
    });
    await store.claimEvent(olderPrompt, {
      action: "prompted",
      prompt: "older work",
      occurredAt,
      stop: false,
    });
    await store.claimEvent(stop, {
      action: "prompted",
      prompt: "stop",
      signal: "stop",
      occurredAt,
      stop: true,
    });
    await expect(
      store.markDispatchStarted(olderPrompt.webhookId),
    ).resolves.toBe("superseded");

    const newerPrompt = event({
      webhookId: "webhook-newer-prompt",
      executionId: "activity-newer-prompt",
      action: "prompted",
    });
    await store.claimEvent(newerPrompt, {
      action: "prompted",
      prompt: "newer work",
      occurredAt,
      stop: false,
    });
    await expect(
      store.markDispatchStarted(newerPrompt.webhookId),
    ).resolves.toBe("dispatch_started");
  });

  it("does not advance a stop fence for the same execution under a new webhook id", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    const occurredAt = "2026-08-18T12:00:00.000Z";
    const originalStop = event({
      webhookId: "webhook-stop-original",
      executionId: "activity-stop",
      action: "prompted",
    });
    const promptAcceptedAfterStop = event({
      webhookId: "webhook-prompt-after-stop",
      executionId: "activity-prompt-after-stop",
      action: "prompted",
    });
    const laterDistinctStop = event({
      webhookId: "webhook-distinct-stop",
      executionId: "activity-distinct-stop",
      action: "prompted",
    });
    const duplicateStop = event({
      webhookId: "webhook-stop-redelivery",
      executionId: originalStop.executionId,
      action: "prompted",
    });
    const stopPayload = {
      action: "prompted" as const,
      prompt: "stop",
      signal: "stop",
      occurredAt,
      stop: true,
    };

    await expect(
      store.claimEvent(originalStop, stopPayload),
    ).resolves.toMatchObject({ disposition: "claimed" });
    await expect(
      store.claimEvent(laterDistinctStop, stopPayload),
    ).resolves.toMatchObject({ disposition: "claimed" });
    await expect(
      store.claimEvent(promptAcceptedAfterStop, {
        action: "prompted",
        prompt: "legitimate same-time follow-up",
        occurredAt,
        stop: false,
      }),
    ).resolves.toMatchObject({ disposition: "claimed" });
    await expect(
      store.claimEvent(duplicateStop, stopPayload),
    ).resolves.toMatchObject({ disposition: "superseded" });
    const persisted = JSON.parse(await fs.readFile(storePath, "utf8")) as {
      recoveryStopFences: Record<
        string,
        {
          occurredAt: string;
          sequence: number;
          webhookId: string;
          executionId: string;
        }
      >;
    };
    expect(persisted.recoveryStopFences[originalStop.linearSessionId]).toEqual({
      occurredAt,
      sequence: 2,
      webhookId: laterDistinctStop.webhookId,
      executionId: laterDistinctStop.executionId,
    });

    const restartedStore = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-b",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await expect(
      restartedStore.claimEvent(promptAcceptedAfterStop, {
        action: "prompted",
        prompt: "legitimate same-time follow-up",
        occurredAt,
        stop: false,
      }),
    ).resolves.toMatchObject({ disposition: "claimed" });
    await expect(
      restartedStore.markDispatchStarted(promptAcceptedAfterStop.webhookId),
    ).resolves.toBe("dispatch_started");
  });

  it("fails closed on a malformed persisted stop fence", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await store.claimEvent(
      event({
        webhookId: "webhook-malformed-fence-stop",
        executionId: "activity-malformed-fence-stop",
        action: "prompted",
      }),
      {
        action: "prompted",
        prompt: "stop",
        signal: "stop",
        stop: true,
        occurredAt: "2026-08-18T12:00:00.000Z",
      },
    );
    const raw = JSON.parse(await fs.readFile(storePath, "utf8")) as {
      recoveryStopFences: Record<string, { occurredAt: string }>;
    };
    raw.recoveryStopFences["session-1"]!.occurredAt = "not-canonical";
    await fs.writeFile(storePath, JSON.stringify(raw), { mode: 0o600 });

    await expect(store.assertRecoverableEventsAvailable()).rejects.toBeInstanceOf(
      IngressRecoveryEnvelopeError,
    );
  });

  it("bounds active recovery admission and returns fixed-size recovery batches", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const keyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    const capped = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: keyring,
      maxRecoverableEvents: 3,
    });
    for (let index = 0; index < 2; index += 1) {
      await capped.claimEvent(
        event({
          webhookId: `webhook-cap-${index}`,
          executionId: `created:session-cap-${index}`,
          linearSessionId: `session-cap-${index}`,
        }),
        {
          action: "created",
          prompt: `prompt ${index}`,
          occurredAt: "2026-08-18T12:00:00.000Z",
        },
      );
    }
    await expect(
      capped.claimEvent(
        event({
          webhookId: "webhook-cap-rejected",
          executionId: "created:session-cap-rejected",
          linearSessionId: "session-cap-rejected",
        }),
        {
          action: "created",
          prompt: "over capacity",
          occurredAt: "2026-08-18T12:00:00.000Z",
        },
      ),
    ).rejects.toBeInstanceOf(IngressRecoveryEnvelopeError);

    const batchPath = path.join(tmpDir, "bridge-state-batches.json");
    const batched = new JsonBridgeStateStore(batchPath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: keyring,
      maxRecoverableEvents: 20,
    });
    for (let index = 0; index < 17; index += 1) {
      await batched.claimEvent(
        event({
          webhookId: `webhook-batch-${index}`,
          executionId: `created:session-batch-${index}`,
          linearSessionId: `session-batch-${index}`,
        }),
        {
          action: "created",
          prompt: `batch ${index}`,
          occurredAt: "2026-08-18T12:00:00.000Z",
        },
      );
    }
    const firstBatch = await batched.listRecoverableEvents();
    expect(firstBatch).toHaveLength(16);
    expect(firstBatch.every((candidate) => candidate.available)).toBe(true);
    const lastSequence = firstBatch.at(-1)?.sequence;
    expect(lastSequence).toBe(16);
    const raw = JSON.parse(await fs.readFile(batchPath, "utf8")) as {
      nextRecoverySequence?: number;
    };
    raw.nextRecoverySequence = 1;
    await fs.writeFile(batchPath, JSON.stringify(raw), { mode: 0o600 });
    await expect(
      batched.claimEvent(
        event({
          webhookId: "webhook-batch-after-stale-cursor",
          executionId: "created:session-batch-after-stale-cursor",
          linearSessionId: "session-batch-after-stale-cursor",
        }),
        {
          action: "created",
          prompt: "after stale cursor",
          occurredAt: "2026-08-18T12:00:00.000Z",
        },
      ),
    ).resolves.toMatchObject({ receipt: { recoverySequence: 18 } });
    await expect(batched.listRecoverableEvents(lastSequence)).resolves.toHaveLength(
      2,
    );
  });

  it("admits a same-session stop at capacity by superseding older pre-intent work", async () => {
    const storePath = path.join(tmpDir, "bridge-state-stop-capacity.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
      maxRecoverableEvents: 1,
    });
    await store.claimEvent(
      event({
        webhookId: "webhook-capacity-created",
        executionId: "created:session-capacity-stop",
        linearSessionId: "session-capacity-stop",
      }),
      {
        action: "created",
        prompt: "older work",
        occurredAt: "2026-08-18T12:00:00.000Z",
      },
    );

    await expect(
      store.claimEvent(
        event({
          webhookId: "webhook-capacity-stop",
          executionId: "activity-capacity-stop",
          linearSessionId: "session-capacity-stop",
          action: "prompted",
        }),
        {
          action: "prompted",
          prompt: "stop",
          stop: true,
          signal: "stop",
          occurredAt: "2026-08-18T12:00:01.000Z",
        },
      ),
    ).resolves.toMatchObject({ disposition: "claimed" });
    await expect(
      store.getReceipt("webhook-capacity-created"),
    ).resolves.toMatchObject({
      status: "superseded",
      supersededByWebhookId: "webhook-capacity-stop",
    });
    await expect(store.listRecoverableEvents()).resolves.toHaveLength(1);
  });

  it("rejects an older same-session stop at capacity without superseding newer work", async () => {
    const storePath = path.join(tmpDir, "bridge-state-older-stop-capacity.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
      maxRecoverableEvents: 1,
    });
    const prompt = event({
      webhookId: "webhook-capacity-newer-prompt",
      executionId: "activity-capacity-newer-prompt",
      linearSessionId: "session-capacity-older-stop",
      action: "prompted",
    });
    await store.claimEvent(prompt, {
      action: "prompted",
      prompt: "newer accepted work",
      stop: false,
      occurredAt: "2026-08-18T12:00:02.000Z",
    });

    await expect(
      store.claimEvent(
        event({
          webhookId: "webhook-capacity-older-stop",
          executionId: "activity-capacity-older-stop",
          linearSessionId: prompt.linearSessionId,
          action: "prompted",
        }),
        {
          action: "prompted",
          prompt: "stop",
          stop: true,
          signal: "stop",
          occurredAt: "2026-08-18T12:00:01.000Z",
        },
      ),
    ).rejects.toBeInstanceOf(IngressRecoveryEnvelopeError);
    await expect(store.getReceipt(prompt.webhookId)).resolves.toMatchObject({
      status: "claimed",
      recoverySequence: 1,
    });
    await expect(
      store.getReceipt("webhook-capacity-older-stop"),
    ).resolves.toBeUndefined();
    await expect(store.listRecoverableEvents()).resolves.toEqual([
      expect.objectContaining({
        identity: expect.objectContaining({ webhookId: prompt.webhookId }),
        sequence: 1,
        available: true,
      }),
    ]);
  });

  it("bounds process-local acceptance bookkeeping across repeated capacity stops", async () => {
    const storePath = path.join(tmpDir, "bridge-state-capacity-bookkeeping.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
      maxRecoverableEvents: 1,
    });
    const sessionId = "session-capacity-bookkeeping";
    await store.claimEvent(
      event({
        webhookId: "webhook-capacity-bookkeeping-created",
        executionId: `created:${sessionId}`,
        linearSessionId: sessionId,
      }),
      {
        action: "created",
        prompt: "initial work",
        occurredAt: "2026-08-18T12:00:00.000Z",
      },
    );

    for (let index = 1; index <= 12; index += 1) {
      await store.claimEvent(
        event({
          webhookId: `webhook-capacity-bookkeeping-stop-${index}`,
          executionId: `activity-capacity-bookkeeping-stop-${index}`,
          linearSessionId: sessionId,
          action: "prompted",
        }),
        {
          action: "prompted",
          prompt: "stop",
          stop: true,
          signal: "stop",
          occurredAt: "2026-08-18T12:00:01.000Z",
        },
      );
    }

    const acceptedWebhookIds = (
      store as unknown as {
        locallyAcceptedPreDispatchClaims: Set<string>;
      }
    ).locallyAcceptedPreDispatchClaims;
    expect(acceptedWebhookIds).toEqual(
      new Set(["webhook-capacity-bookkeeping-stop-12"]),
    );
    await expect(store.listRecoverableEvents()).resolves.toEqual([
      expect.objectContaining({
        identity: expect.objectContaining({
          webhookId: "webhook-capacity-bookkeeping-stop-12",
        }),
      }),
    ]);
  });

  it("reclaims a same-process pre-dispatch claim when its durability acknowledgement fails", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const directory = path.dirname(storePath);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
    });
    const originalOpen = fs.open.bind(fs);
    let directorySyncs = 0;
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          directorySyncs += 1;
          if (directorySyncs === 3) {
            throw new Error("synthetic final claim directory sync failure");
          }
          await originalSync();
        });
      }
      return handle;
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      "synthetic final claim directory sync failure",
    );
    const visibleReceipt = await store.getReceipt("webhook-1");
    expect(visibleReceipt).toMatchObject({
      status: "claimed",
      ownerId: "runtime-a",
    });
    expect(visibleReceipt?.dispatchStartedAt).toBeUndefined();
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
      receipt: { status: "claimed", ownerId: "runtime-a" },
    });
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "duplicate",
    });

    openSpy.mockRestore();
  });

  it("deduplicates a concurrent same-process retry after a durable claim succeeds", async () => {
    const store = new JsonBridgeStateStore(path.join(tmpDir, "bridge-state.json"), {
      ownerId: "runtime-a",
    });

    const [first, retry] = await Promise.all([
      store.claimEvent(event()),
      store.claimEvent(event()),
    ]);

    expect(first.disposition).toBe("claimed");
    expect(retry.disposition).toBe("duplicate");
  });

  it("does not reclaim when a failed marker call left the dispatch marker visible", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const directory = path.dirname(storePath);
    const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    const originalOpen = fs.open.bind(fs);
    let directorySyncs = 0;
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          directorySyncs += 1;
          if (directorySyncs === 4) {
            throw new Error("synthetic dispatch marker directory sync failure");
          }
          await originalSync();
        });
      }
      return handle;
    });

    await store.claimEvent(event());
    await expect(store.markDispatchStarted("webhook-1")).rejects.toThrow(
      "Dispatch marker durability was not confirmed",
    );

    const originalReadFile = fs.readFile.bind(fs);
    let releaseReadFailed = false;
    const readSpy = vi.spyOn(fs, "readFile").mockImplementation(async (...args) => {
      if (!releaseReadFailed && String(args[0]) === storePath) {
        releaseReadFailed = true;
        throw new Error("synthetic release read failure");
      }
      return await originalReadFile(...args);
    });
    await expect(store.releasePreDispatchClaim("webhook-1")).rejects.toThrow(
      "synthetic release read failure",
    );
    readSpy.mockRestore();

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "duplicate",
      receipt: { dispatchStartedAt: expect.any(String) },
    });

    openSpy.mockRestore();
  });

  it("retries a locally unconfirmed visible dispatch marker without exposing it after restart", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const directory = path.dirname(storePath);
    const keyring = createIngressRecoveryKeyring(RECOVERY_KEY_A);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: keyring,
    });
    const prompt = "private marker confirmation prompt";
    await store.claimEvent(event(), {
      action: "created",
      prompt,
      occurredAt: "2026-08-18T12:00:00.000Z",
    });

    const originalOpen = fs.open.bind(fs);
    const originalRename = fs.rename.bind(fs);
    let markerRenameVisible = false;
    let failedMarkerSync = false;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      await originalRename(from, to);
      if (String(to) === storePath) {
        markerRenameVisible = true;
      }
    });
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          if (markerRenameVisible && !failedMarkerSync) {
            markerRenameVisible = false;
            failedMarkerSync = true;
            throw new Error("synthetic visible marker sync failure");
          }
          await originalSync();
        });
      }
      return handle;
    });

    try {
      await expect(
        store.markDispatchStarted("webhook-1"),
      ).rejects.toThrow("Dispatch marker durability was not confirmed");
    } finally {
      renameSpy.mockRestore();
      openSpy.mockRestore();
    }

    await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
      status: "claimed",
      dispatchStartedAt: expect.any(String),
    });
    const visible = await store.getReceipt("webhook-1");
    expect(visible).toMatchObject({
      recoverySequence: 1,
      recoveryEnvelope: expect.any(Object),
    });
    await expect(store.releasePreDispatchClaim("webhook-1")).resolves.toBe(false);
    await expect(store.listRecoverableEvents()).resolves.toEqual([]);

    const restartedStore = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-after-restart",
      recoveryKeyring: keyring,
    });
    await expect(restartedStore.listRecoverableEvents()).resolves.toEqual([]);

    await expect(store.markDispatchStarted("webhook-1")).resolves.toBe(
      "dispatch_started",
    );
    const confirmed = await store.getReceipt("webhook-1");
    expect(confirmed).toMatchObject({
      recoverySequence: 1,
      recoveryEnvelope: expect.any(Object),
    });
    await store.completeEvent("webhook-1");
    const terminal = await store.getReceipt("webhook-1");
    expect(terminal).not.toHaveProperty("recoverySequence");
    expect(terminal).not.toHaveProperty("recoveryEnvelope");
    expect(await fs.readFile(storePath, "utf8")).not.toContain(prompt);
  });

  it("keeps repeated visible dispatch-marker confirmation failures retryable", async () => {
    const storePath = path.join(tmpDir, "bridge-state-repeated-marker-confirmation.json");
    const directory = path.dirname(storePath);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
    });
    await store.claimEvent(event());
    const originalOpen = fs.open.bind(fs);
    const originalRename = fs.rename.bind(fs);
    let markerRenameVisible = false;
    let failedMarkerSyncs = 0;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      await originalRename(from, to);
      if (String(to) === storePath) {
        markerRenameVisible = true;
      }
    });
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          if (markerRenameVisible && failedMarkerSyncs < 2) {
            markerRenameVisible = false;
            failedMarkerSyncs += 1;
            throw new Error(`synthetic marker confirmation failure ${failedMarkerSyncs}`);
          }
          await originalSync();
        });
      }
      return handle;
    });

    try {
      await expect(store.markDispatchStarted("webhook-1")).rejects.toBeInstanceOf(
        DispatchMarkerDurabilityError,
      );
      await expect(store.markDispatchStarted("webhook-1")).rejects.toBeInstanceOf(
        DispatchMarkerDurabilityError,
      );
      await expect(store.markDispatchStarted("webhook-1")).resolves.toBe(
        "dispatch_started",
      );
      expect(failedMarkerSyncs).toBe(2);
    } finally {
      renameSpy.mockRestore();
      openSpy.mockRestore();
    }
  });

  it("confirms a dispatch marker after its post-rename directory sync exceeds the deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state-stalled-marker-sync.json");
    const directory = path.dirname(storePath);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 500,
    });
    await store.claimEvent(event());
    (
      store as unknown as {
        lockTimeoutMs: number;
      }
    ).lockTimeoutMs = 250;
    const stalledSync = deferred();
    const originalOpen = fs.open.bind(fs);
    let stallNextDirectorySync = true;
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          if (stallNextDirectorySync) {
            stallNextDirectorySync = false;
            await stalledSync.promise;
            return;
          }
          await originalSync();
        });
      }
      return handle;
    });

    try {
      await expect(store.markDispatchStarted("webhook-1")).rejects.toThrow(
        "Dispatch marker durability was not confirmed",
      );
      stalledSync.resolve();
      await expect(store.markDispatchStarted("webhook-1")).resolves.toBe(
        "dispatch_started",
      );
    } finally {
      stalledSync.resolve();
      openSpy.mockRestore();
    }
  });

  it("retains the lock and confirms a dispatch marker whose target rename settles late", async () => {
    const storePath = path.join(tmpDir, "bridge-state-late-marker-rename.json");
    const lockPath = `${storePath}.lock`;
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 500,
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await store.claimEvent(event(), {
      action: "created",
      prompt: "late marker rename prompt",
      occurredAt: "2026-08-18T12:00:00.000Z",
    });
    (
      store as unknown as {
        lockTimeoutMs: number;
      }
    ).lockTimeoutMs = 50;
    const markerRenameStarted = deferred();
    const releaseRename = deferred();
    const originalRename = fs.rename.bind(fs);
    const originalUnlink = fs.unlink.bind(fs);
    let stalledMarkerRename = false;
    let stalledMarkerTempPath: string | undefined;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      if (String(to) === storePath && !stalledMarkerRename) {
        stalledMarkerRename = true;
        stalledMarkerTempPath = String(from);
        markerRenameStarted.resolve();
        await releaseRename.promise;
        await originalRename(from, to);
        return;
      }
      await originalRename(from, to);
    });
    const unlinkSpy = vi.spyOn(fs, "unlink").mockImplementation(async (...args) => {
      if (String(args[0]) === stalledMarkerTempPath) {
        await releaseRename.promise;
      }
      return originalUnlink(...args);
    });

    try {
      const firstMarker = store.markDispatchStarted("webhook-1");
      await markerRenameStarted.promise;
      await expect(firstMarker).rejects.toThrow(
        "Dispatch marker durability was not confirmed",
      );
      await expect(fs.stat(lockPath)).resolves.toBeDefined();
      (
        store as unknown as {
          lockTimeoutMs: number;
        }
      ).lockTimeoutMs = 500;
      let laterMutationSettled = false;
      const laterMutation = store
        .claimEvent(
          event({
            webhookId: "webhook-after-late-marker-rename",
            executionId: "created:session-after-late-marker-rename",
            linearSessionId: "session-after-late-marker-rename",
          }),
          {
            action: "created",
            prompt: "later mutation must survive",
            occurredAt: "2026-08-18T12:00:01.000Z",
          },
        )
        .finally(() => {
          laterMutationSettled = true;
        });
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      expect(laterMutationSettled).toBe(false);

      releaseRename.resolve();
      await expect(laterMutation).resolves.toMatchObject({
        disposition: "claimed",
      });
      await expect(store.markDispatchStarted("webhook-1")).resolves.toBe(
        "dispatch_started",
      );
      await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
        status: "claimed",
        dispatchStartedAt: expect.any(String),
      });
      await expect(
        store.getReceipt("webhook-after-late-marker-rename"),
      ).resolves.toMatchObject({ status: "claimed" });
    } finally {
      releaseRename.resolve();
      unlinkSpy.mockRestore();
      renameSpy.mockRestore();
    }
  });

  it("restores the original recovery order when runtime intent is rolled back behind a same-time stop", async () => {
    const storePath = path.join(tmpDir, "bridge-state-rollback-order.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    const occurredAt = "2026-08-18T12:00:00.000Z";
    const promptIdentity = event({
      webhookId: "webhook-prompt-before-stop",
      executionId: "activity-prompt-before-stop",
      linearSessionId: "session-rollback-order",
      action: "prompted",
    });
    const promptPayload = {
      action: "prompted" as const,
      prompt: "continue before stop",
      stop: false,
      occurredAt,
    };
    await store.claimEvent(promptIdentity, promptPayload);
    await store.markDispatchStarted(promptIdentity.webhookId);
    await store.claimEvent(
      event({
        webhookId: "webhook-stop-after-prompt",
        executionId: "activity-stop-after-prompt",
        linearSessionId: "session-rollback-order",
        action: "prompted",
      }),
      {
        action: "prompted",
        prompt: "stop",
        signal: "stop",
        stop: true,
        occurredAt,
      },
    );

    await store.rollbackRuntimeStartIntent(
      promptIdentity.webhookId,
      promptPayload,
    );
    await expect(store.getReceipt(promptIdentity.webhookId)).resolves.toMatchObject({
      recoverySequence: 1,
    });
    await expect(
      store.checkDispatchEligibility(promptIdentity.webhookId),
    ).resolves.toBe("superseded");
    await expect(
      store.markDispatchStarted(promptIdentity.webhookId),
    ).resolves.toBe("superseded");
  });

  it("keeps runtime-intent recovery capacity reserved until terminalization", async () => {
    const storePath = path.join(tmpDir, "bridge-state-rollback-cap.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
      maxRecoverableEvents: 1,
    });
    const payload = {
      action: "created" as const,
      prompt: "reserved recovery payload",
      occurredAt: "2026-08-18T12:00:00.000Z",
    };
    await store.claimEvent(event(), payload);
    await store.markDispatchStarted("webhook-1");

    await expect(
      store.claimEvent(
        event({
          webhookId: "webhook-capacity-cannot-fill",
          executionId: "created:session-capacity-cannot-fill",
          linearSessionId: "session-capacity-cannot-fill",
        }),
        {
          action: "created",
          prompt: "must not consume reserved capacity",
          occurredAt: "2026-08-18T12:00:01.000Z",
        },
      ),
    ).rejects.toBeInstanceOf(IngressRecoveryEnvelopeError);

    await store.rollbackRuntimeStartIntent("webhook-1", payload);
    await expect(store.listRecoverableEvents()).resolves.toEqual([
      expect.objectContaining({ sequence: 1, available: true }),
    ]);
  });

  it("reclaims when marker and release fail before either state write", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    await store.claimEvent(event());

    const originalOpen = fs.open.bind(fs);
    let markerOpenFailed = false;
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const openedPath = String(args[0]);
      if (
        !markerOpenFailed &&
        openedPath.includes(".bridge-state.json.") &&
        openedPath.endsWith(".tmp")
      ) {
        markerOpenFailed = true;
        throw new Error("synthetic marker open failure");
      }
      return await originalOpen(...args);
    });
    await expect(store.markDispatchStarted("webhook-1")).rejects.toThrow(
      "synthetic marker open failure",
    );
    openSpy.mockRestore();

    const originalReadFile = fs.readFile.bind(fs);
    let releaseReadFailed = false;
    const readSpy = vi.spyOn(fs, "readFile").mockImplementation(async (...args) => {
      if (!releaseReadFailed && String(args[0]) === storePath) {
        releaseReadFailed = true;
        throw new Error("synthetic release read failure");
      }
      return await originalReadFile(...args);
    });
    await expect(store.releasePreDispatchClaim("webhook-1")).rejects.toThrow(
      "synthetic release read failure",
    );
    readSpy.mockRestore();

    const visibleReceipt = await store.getReceipt("webhook-1");
    expect(visibleReceipt?.dispatchStartedAt).toBeUndefined();
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "duplicate",
    });
  });

  it("deduplicates in-process retries and makes a post-dispatch cross-runtime retry ambiguous", async () => {
    const store = new JsonBridgeStateStore(path.join(tmpDir, "bridge-state.json"), {
      ownerId: "runtime-a",
    });

    expect((await store.claimEvent(event())).disposition).toBe("claimed");
    expect((await store.claimEvent(event())).disposition).toBe("duplicate");
    await store.markDispatchStarted("webhook-1");
    expect((await store.claimEvent(event())).disposition).toBe("duplicate");

    const crossedRuntime = new JsonBridgeStateStore(
      path.join(tmpDir, "bridge-state.json"),
      { ownerId: "runtime-b" },
    );
    const second = await crossedRuntime.claimEvent(
      event({ webhookId: "webhook-2" }),
    );
    expect(second).toMatchObject({
      disposition: "ambiguous",
      receipt: {
        webhookId: "webhook-2",
        executionId: "created:session-1",
        status: "superseded",
        supersededByWebhookId: "webhook-1",
        outcome: {
          httpStatus: 200,
          result: "not_dispatched",
          disposition: "ambiguous",
          errorClass: "AmbiguousDispatch",
        },
      },
    });
    await expect(crossedRuntime.getClaim("created:session-1")).resolves.toMatchObject({
      webhookId: "webhook-1",
      status: "claimed",
      ownerId: "runtime-a",
      dispatchStartedAt: expect.any(String),
    });
  });

  it("reclaims a prior runtime claim when dispatch never started", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const runtimeA = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    const runtimeB = new JsonBridgeStateStore(storePath, { ownerId: "runtime-b" });

    expect((await runtimeA.claimEvent(event())).disposition).toBe("claimed");
    const reclaimed = await runtimeB.claimEvent(event());

    expect(reclaimed).toMatchObject({
      disposition: "claimed",
      receipt: {
        webhookId: "webhook-1",
        status: "claimed",
        ownerId: "runtime-b",
        outcome: {
          httpStatus: 200,
          result: "accepted",
          disposition: "claimed",
        },
      },
    });
    await expect(runtimeA.markDispatchStarted("webhook-1")).rejects.toThrow(
      /ownership/i,
    );
    await expect(runtimeB.markDispatchStarted("webhook-1")).resolves.toBe(
      "dispatch_started",
    );
  });

  it("releases only a pre-dispatch claim and lets the same delivery reclaim it", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });

    await store.claimEvent(event());
    await expect(store.releasePreDispatchClaim("webhook-1")).resolves.toBe(true);
    await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
      status: "received",
      outcome: {
        httpStatus: 503,
        result: "retry",
        disposition: "received",
        errorClass: "IngressPersistenceError",
      },
    });
    await expect(store.getClaim("created:session-1")).resolves.toMatchObject({
      webhookId: "webhook-1",
      activityIds: {},
    });

    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    await store.markDispatchStarted("webhook-1");
    await expect(store.releasePreDispatchClaim("webhook-1")).resolves.toBe(false);
    await expect(store.getClaim("created:session-1")).resolves.toMatchObject({
      dispatchStartedAt: expect.any(String),
    });
  });

  it("allows only the winning owner to cross the dispatch boundary during simultaneous claims", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const runtimeA = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    const runtimeB = new JsonBridgeStateStore(storePath, { ownerId: "runtime-b" });

    const results = await Promise.all([
      runtimeA.claimEvent(event()),
      runtimeB.claimEvent(event()),
    ]);
    expect(results.map((result) => result.disposition)).toEqual([
      "claimed",
      "claimed",
    ]);

    const dispatchMarks = await Promise.allSettled([
      runtimeA.markDispatchStarted("webhook-1"),
      runtimeB.markDispatchStarted("webhook-1"),
    ]);
    expect(dispatchMarks.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(dispatchMarks.filter((result) => result.status === "rejected")).toHaveLength(1);
  });

  it("persists completed and failed terminal lifecycle states", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    await store.claimEvent(event());
    await store.markDispatchStarted("webhook-1");
    await store.completeEvent("webhook-1");

    await store.claimEvent(
      event({
        webhookId: "webhook-2",
        executionId: "activity-2",
        action: "prompted",
      }),
    );
    await store.markDispatchStarted("webhook-2");
    await store.failEvent("webhook-2", "RuntimeExecutionError");

    const reloaded = new JsonBridgeStateStore(storePath, { ownerId: "runtime-b" });
    await expect(reloaded.getReceipt("webhook-1")).resolves.toMatchObject({
      status: "completed",
      completedAt: expect.any(String),
      outcome: {
        httpStatus: 200,
        result: "completed",
        disposition: "claimed",
      },
    });
    await expect(reloaded.getClaim("created:session-1")).resolves.toMatchObject({
      status: "completed",
    });
    await expect(reloaded.getReceipt("webhook-2")).resolves.toMatchObject({
      status: "failed",
      failedAt: expect.any(String),
      outcome: {
        httpStatus: 200,
        result: "processing_failed",
        disposition: "claimed",
        errorClass: "RuntimeExecutionError",
      },
    });
    await expect(reloaded.getClaim("activity-2")).resolves.toMatchObject({
      status: "failed",
    });
  });

  it("rewrites a locally visible terminal state to confirm its directory durability", async () => {
    const storePath = path.join(tmpDir, "bridge-state-terminal-confirmation.json");
    const directory = path.dirname(storePath);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
    });
    await store.claimEvent(event());
    await store.markDispatchStarted("webhook-1");

    const originalOpen = fs.open.bind(fs);
    const originalRename = fs.rename.bind(fs);
    let terminalRenameVisible = false;
    let failedTerminalSync = false;
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
      await originalRename(from, to);
      if (String(to) === storePath) {
        const visible = JSON.parse(await fs.readFile(storePath, "utf8")) as {
          receipts?: Record<string, { status?: string }>;
        };
        terminalRenameVisible =
          visible.receipts?.["webhook-1"]?.status === "completed";
      }
    });
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          if (terminalRenameVisible && !failedTerminalSync) {
            failedTerminalSync = true;
            terminalRenameVisible = false;
            throw new Error("synthetic terminal directory sync failure");
          }
          await originalSync();
        });
      }
      return handle;
    });

    try {
      await expect(store.completeEvent("webhook-1")).rejects.toThrow(
        "Terminal state durability was not confirmed",
      );
      await expect(store.getReceipt("webhook-1")).resolves.toMatchObject({
        status: "completed",
      });
      await expect(store.completeEvent("webhook-1")).resolves.toBeUndefined();
      expect(failedTerminalSync).toBe(true);
    } finally {
      renameSpy.mockRestore();
      openSpy.mockRestore();
    }
  });

  it("confirms terminal state after its post-rename directory sync exceeds the deadline", async () => {
    const storePath = path.join(tmpDir, "bridge-state-stalled-terminal-sync.json");
    const directory = path.dirname(storePath);
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
      lockTimeoutMs: 500,
    });
    await store.claimEvent(event());
    await store.markDispatchStarted("webhook-1");
    (
      store as unknown as {
        lockTimeoutMs: number;
      }
    ).lockTimeoutMs = 50;
    const stalledSync = deferred();
    const originalOpen = fs.open.bind(fs);
    let stallNextDirectorySync = true;
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === directory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          if (stallNextDirectorySync) {
            stallNextDirectorySync = false;
            await stalledSync.promise;
            return;
          }
          await originalSync();
        });
      }
      return handle;
    });

    try {
      await expect(store.completeEvent("webhook-1")).rejects.toThrow(
        "Terminal state durability was not confirmed",
      );
      stalledSync.resolve();
      await expect(store.completeEvent("webhook-1")).resolves.toBeUndefined();
    } finally {
      stalledSync.resolve();
      openSpy.mockRestore();
    }
  });

  it("reuses a persisted caller UUID for the same outbound activity", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const writer = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });
    await writer.claimEvent(event());
    await writer.markDispatchStarted("webhook-1");

    const first = await writer.getOrCreateActivityId(
      "created:session-1",
      "liveness",
    );
    expect(first).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );

    const reloaded = new JsonBridgeStateStore(storePath, { ownerId: "runtime-b" });
    await expect(reloaded.getClaim("created:session-1")).resolves.toMatchObject({
      activityIds: { liveness: first },
    });
  });

  it("persists a content-bound activity outbox before dispatch and preserves it across release", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      ownerId: "runtime-a",
    });
    await store.claimEvent(event());
    const digest = "a".repeat(64);

    const prepared = await store.prepareActivity(
      "created:session-1",
      "liveness",
      "session-1",
      digest,
    );
    expect(prepared).toMatchObject({ status: "pending", attempts: 0 });
    const attempted = await store.markActivityAttempted(
      "created:session-1",
      "liveness",
    );
    expect(attempted).toMatchObject({
      activityId: prepared.activityId,
      status: "pending",
      attempts: 1,
    });
    await expect(store.releasePreDispatchClaim("webhook-1")).resolves.toBe(true);
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    await expect(
      store.prepareActivity(
        "created:session-1",
        "liveness",
        "session-1",
        digest,
      ),
    ).resolves.toEqual(attempted);
    await expect(
      store.prepareActivity(
        "created:session-1",
        "liveness",
        "session-1",
        "b".repeat(64),
      ),
    ).rejects.toThrow("Activity binding changed");
    await store.markActivityDelivered("created:session-1", "liveness");
    await expect(
      store.prepareActivity(
        "created:session-1",
        "liveness",
        "session-1",
        digest,
      ),
    ).resolves.toMatchObject({
      activityId: prepared.activityId,
      status: "delivered",
      attempts: 1,
    });
  });

  it("prunes terminal receipts after seven days while preserving active claims", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    let now = Date.parse("2026-08-01T00:00:00.000Z");
    const store = new JsonBridgeStateStore(storePath, {
      now: () => now,
      ownerId: "runtime-a",
    });
    await store.claimEvent(event({ webhookId: "old-terminal" }));
    await store.markDispatchStarted("old-terminal");
    await store.completeEvent("old-terminal");
    await store.claimEvent(
      event({ webhookId: "old-active", executionId: "created:session-active" }),
    );

    now += 8 * 24 * 60 * 60 * 1000;
    await store.claimEvent(
      event({ webhookId: "new-active", executionId: "created:session-new" }),
    );

    await expect(store.getReceipt("old-terminal")).resolves.toBeUndefined();
    await expect(store.getReceipt("old-active")).resolves.toMatchObject({
      status: "claimed",
    });
  });

  it("prunes a stop fence with its retained terminal receipt", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    let now = Date.parse("2026-08-01T00:00:00.000Z");
    const store = new JsonBridgeStateStore(storePath, {
      ...TEST_LOCK_OPTIONS,
      now: () => now,
      ownerId: "runtime-a",
      recoveryKeyring: createIngressRecoveryKeyring(RECOVERY_KEY_A),
    });
    await store.claimEvent(
      event({
        webhookId: "old-stop",
        executionId: "activity-old-stop",
        action: "prompted",
      }),
      {
        action: "prompted",
        prompt: "stop",
        signal: "stop",
        stop: true,
        occurredAt: new Date(now).toISOString(),
      },
    );
    await store.markDispatchStarted("old-stop");
    await store.completeEvent("old-stop");

    now += 8 * 24 * 60 * 60 * 1000;
    await store.claimEvent(
      event({
        webhookId: "new-after-stop-retention",
        executionId: "created:new-after-stop-retention",
        linearSessionId: "new-after-stop-retention",
      }),
      {
        action: "created",
        prompt: "new work",
        occurredAt: new Date(now).toISOString(),
      },
    );
    const raw = JSON.parse(await fs.readFile(storePath, "utf8")) as {
      recoveryStopFences?: Record<string, unknown>;
    };
    expect(raw.recoveryStopFences?.["session-1"]).toBeUndefined();
  });

  it("caps retained receipts by evicting the oldest terminal entry first", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    let now = Date.parse("2026-08-18T00:00:00.000Z");
    const store = new JsonBridgeStateStore(storePath, {
      maxEntries: 2,
      now: () => now,
      ownerId: "runtime-a",
    });

    await store.claimEvent(event({ webhookId: "terminal-1" }));
    await store.markDispatchStarted("terminal-1");
    await store.completeEvent("terminal-1");
    now += 1;
    await store.claimEvent(
      event({ webhookId: "terminal-2", executionId: "created:session-2" }),
    );
    await store.markDispatchStarted("terminal-2");
    await store.completeEvent("terminal-2");
    now += 1;
    await store.claimEvent(
      event({ webhookId: "active-3", executionId: "created:session-3" }),
    );

    await expect(store.getReceipt("terminal-1")).resolves.toBeUndefined();
    await expect(store.getReceipt("terminal-2")).resolves.toBeDefined();
    await expect(store.getReceipt("active-3")).resolves.toMatchObject({
      status: "claimed",
    });
  });

  it("rejects oversized identifiers and leaves only the atomic target file", async () => {
    const storePath = path.join(tmpDir, "nested", "bridge-state.json");
    const store = new JsonBridgeStateStore(storePath, { ownerId: "runtime-a" });

    await expect(
      store.claimEvent(event({ webhookId: "x".repeat(257) })),
    ).rejects.toThrow(/webhookId/);
    await store.claimEvent(event());

    expect(await fs.readdir(path.dirname(storePath))).toEqual([
      "bridge-state.json",
    ]);
    const persisted = await fs.readFile(storePath, "utf8");
    expect(persisted).not.toContain("prompt");
    expect(persisted).not.toContain("body");
  });
});
