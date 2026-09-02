import { describe, expect, it } from "vitest";
import { SessionLanes } from "../src/queue.js";

function createDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Flush pending microtasks by yielding to a macrotask boundary. */
function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("SessionLanes", () => {
  it("resolves with the task's own return value", async () => {
    const lanes = new SessionLanes();
    await expect(lanes.enqueue("session-a", async () => 42)).resolves.toBe(42);
  });

  it("rejects with the task's own error", async () => {
    const lanes = new SessionLanes();
    await expect(
      lanes.enqueue("session-a", async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
  });

  it("never overlaps tasks in the same session", async () => {
    const lanes = new SessionLanes();
    const firstRelease = createDeferred<void>();
    const secondStarted = createDeferred<void>();

    const first = lanes.enqueue("session-a", () => firstRelease.promise);
    const second = lanes.enqueue("session-a", async () => {
      secondStarted.resolve();
    });

    await tick();
    expect(lanes.size("session-a")).toBe(2);
    let secondRunning = false;
    void secondStarted.promise.then(() => {
      secondRunning = true;
    });
    await tick();
    expect(secondRunning).toBe(false);

    firstRelease.resolve();
    await Promise.all([first, second]);
    expect(secondRunning).toBe(true);
  });

  it("executes one session's tasks in strict FIFO order", async () => {
    const lanes = new SessionLanes();
    const order: number[] = [];

    const results = await Promise.all([
      lanes.enqueue("session-a", async () => {
        order.push(1);
        return "a";
      }),
      lanes.enqueue("session-a", async () => {
        order.push(2);
        return "b";
      }),
      lanes.enqueue("session-a", async () => {
        order.push(3);
        return "c";
      }),
    ]);

    expect(order).toEqual([1, 2, 3]);
    expect(results).toEqual(["a", "b", "c"]);
  });

  it("does not stall a session lane when a task rejects", async () => {
    const lanes = new SessionLanes();
    const order: string[] = [];

    const first = lanes.enqueue("session-a", async () => {
      order.push("first");
      throw new Error("fail");
    });
    const second = lanes.enqueue("session-a", async () => {
      order.push("second");
      return "ok";
    });

    await expect(first).rejects.toThrow("fail");
    await expect(second).resolves.toBe("ok");
    expect(order).toEqual(["first", "second"]);
  });

  it("runs different session lanes concurrently", async () => {
    const lanes = new SessionLanes();
    const release = createDeferred<void>();
    const running = new Set<string>();
    const bothRunning = createDeferred<void>();

    const run = (sessionId: string): Promise<void> =>
      lanes.enqueue(sessionId, async () => {
        running.add(sessionId);
        if (running.size === 2) {
          bothRunning.resolve();
        }
        await release.promise;
      });

    const first = run("session-a");
    const second = run("session-b");
    await bothRunning.promise;
    expect(running).toEqual(new Set(["session-a", "session-b"]));

    release.resolve();
    await Promise.all([first, second]);
  });

  it("reports waiting and running depth for one session only", async () => {
    const lanes = new SessionLanes();
    const firstRelease = createDeferred<void>();
    const secondRelease = createDeferred<void>();

    const first = lanes.enqueue("session-a", () => firstRelease.promise);
    const second = lanes.enqueue("session-a", () => secondRelease.promise);
    const other = lanes.enqueue("session-b", async () => undefined);
    await tick();

    expect(lanes.size("session-a")).toBe(2);
    expect(lanes.size("session-b")).toBe(0);

    firstRelease.resolve();
    await first;
    await tick();
    expect(lanes.size("session-a")).toBe(1);

    secondRelease.resolve();
    await Promise.all([second, other]);
    expect(lanes.size("session-a")).toBe(0);
  });

  it("drain waits for every current lane to settle", async () => {
    const lanes = new SessionLanes();
    const firstRelease = createDeferred<void>();
    const secondRelease = createDeferred<void>();
    const first = lanes.enqueue("session-a", () => firstRelease.promise);
    const second = lanes.enqueue("session-b", () => secondRelease.promise);
    const drained = lanes.drain();
    let drainResolved = false;
    void drained.then(() => {
      drainResolved = true;
    });

    firstRelease.resolve();
    await first;
    await tick();
    expect(drainResolved).toBe(false);

    secondRelease.resolve();
    await Promise.all([second, drained]);
    expect(drainResolved).toBe(true);
  });

  it("drops an idle lane after its last task settles", async () => {
    const lanes = new SessionLanes();
    await lanes.enqueue("session-a", async () => undefined);
    expect(lanes.size("session-a")).toBe(0);

    await expect(
      lanes.enqueue("session-a", async () => "reused"),
    ).resolves.toBe("reused");
    expect(lanes.size("session-a")).toBe(0);
  });
});
