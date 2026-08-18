import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  JsonBridgeStateStore,
  type IngressEventIdentity,
  type JsonBridgeStateStoreOptions,
} from "../src/state/store.js";

let tmpDir: string;

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

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
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

describe("JsonBridgeStateStore", () => {
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
      processIdentity: "process-start-a",
    });
    const lockProcessIdentity: LockProcessIdentity = async () => "process-start-a";
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-b",
      lockTimeoutMs: 50,
      lockProcessIdentity,
    });

    await expect(store.claimEvent(event())).rejects.toThrow(
      /Timed out acquiring bridge state lock/,
    );
    expect(await fs.readdir(lockPath)).toEqual([`${token}.json`]);
    await expect(store.getReceipt("webhook-1")).resolves.toBeUndefined();
  });

  it("reclaims a same-host lock when the live PID belongs to a recycled process", async () => {
    const storePath = path.join(tmpDir, "bridge-state.json");
    const lockPath = `${storePath}.lock`;
    await writeLockOwner(lockPath, "recycled-owner", {
      pid: process.pid,
      hostname: os.hostname(),
      processIdentity: "process-start-before-recycle",
    });
    const lockProcessIdentity: LockProcessIdentity = async () =>
      "process-start-after-recycle";
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-after-recycle",
      lockTimeoutMs: 250,
      lockProcessIdentity,
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
      return "process-start-current";
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
      "process-start-current";
    const store = new JsonBridgeStateStore(storePath, {
      ownerId: "runtime-migration",
      lockTimeoutMs: 50,
      lockProcessIdentity,
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
    await expect(runtimeB.markDispatchStarted("webhook-1")).resolves.toBeUndefined();
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
