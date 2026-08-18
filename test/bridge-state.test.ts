import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  JsonBridgeStateStore,
  type IngressEventIdentity,
} from "../src/state/store.js";

let tmpDir: string;

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "bridge-state-test-"));
});

afterEach(async () => {
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

describe("JsonBridgeStateStore", () => {
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
