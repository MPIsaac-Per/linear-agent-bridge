import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildDarwinLockProcessIdentity,
  buildLinuxLockProcessIdentity,
  darwinProcessRealUidArgs,
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
      lockTimeoutMs: 100,
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
    expect(await fs.readdir(tmpDir)).toEqual([]);
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
      lockTimeoutMs: 40,
      lockProcessIdentity,
    });
    const startedAt = Date.now();

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(Date.now() - startedAt).toBeLessThan(200);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
    expect(await fs.readdir(tmpDir)).toEqual([]);

    stalledIdentity.reject(new Error("late identity failure with private data"));
    await new Promise<void>((resolve) => setImmediate(resolve));
    await expect(store.claimEvent(event())).resolves.toMatchObject({
      disposition: "claimed",
    });
    expect(currentLookups).toBe(2);
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
    expect(await fs.readdir(tmpDir)).toEqual(["bridge-state.json.lock"]);
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
      lockTimeoutMs: 40,
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
      lockTimeoutMs: 40,
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
      await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
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
      await expect(fs.stat(lockPath)).rejects.toMatchObject({ code: "ENOENT" });
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
      lockTimeoutMs: 50,
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
        lockTimeoutMs: 50,
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

  it("persists only encrypted recovery content and removes the envelope at dispatch", async () => {
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
    expect(persisted.receipts["webhook-1"]).not.toHaveProperty(
      "recoveryEnvelope",
    );
    expect(persisted.receipts["webhook-1"]).not.toHaveProperty(
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
      maxRecoverableEvents: 2,
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

  it("reclaims a same-process pre-dispatch claim when its durability acknowledgement fails", async () => {
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
          if (directorySyncs === 2) {
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
          if (directorySyncs === 3) {
            throw new Error("synthetic dispatch marker directory sync failure");
          }
          await originalSync();
        });
      }
      return handle;
    });

    await store.claimEvent(event());
    await expect(store.markDispatchStarted("webhook-1")).rejects.toThrow(
      "synthetic dispatch marker directory sync failure",
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
    await expect(store.getClaim("created:session-1")).resolves.toBeUndefined();

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
