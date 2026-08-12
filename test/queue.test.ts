import { describe, expect, it } from "vitest";
import { SerialQueue } from "../src/queue.js";

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

describe("SerialQueue", () => {
  it("resolves with the task's own return value", async () => {
    const queue = new SerialQueue();
    await expect(queue.enqueue(async () => 42)).resolves.toBe(42);
  });

  it("rejects with the task's own error", async () => {
    const queue = new SerialQueue();
    await expect(
      queue.enqueue(async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
  });

  it("never overlaps two tasks in time", async () => {
    const queue = new SerialQueue();
    const events: string[] = [];

    const p1 = queue.enqueue(async () => {
      events.push("1-start");
      await delay(20);
      events.push("1-end");
    });
    const p2 = queue.enqueue(async () => {
      events.push("2-start");
      await delay(5);
      events.push("2-end");
    });

    await Promise.all([p1, p2]);

    // Task 2 has the shorter delay; if the queue ran tasks concurrently,
    // "2-end" (or "2-start") would land before "1-end".
    expect(events).toEqual(["1-start", "1-end", "2-start", "2-end"]);
  });

  it("executes tasks in strict FIFO order across 3+ tasks", async () => {
    const queue = new SerialQueue();
    const order: number[] = [];

    const results = await Promise.all([
      queue.enqueue(async () => {
        order.push(1);
        return "a";
      }),
      queue.enqueue(async () => {
        order.push(2);
        return "b";
      }),
      queue.enqueue(async () => {
        order.push(3);
        return "c";
      }),
      queue.enqueue(async () => {
        order.push(4);
        return "d";
      }),
    ]);

    expect(order).toEqual([1, 2, 3, 4]);
    expect(results).toEqual(["a", "b", "c", "d"]);
  });

  it("does not stall the queue when a task rejects", async () => {
    const queue = new SerialQueue();
    const order: string[] = [];

    const p1 = queue.enqueue(async () => {
      order.push("1");
      throw new Error("fail");
    });
    const p2 = queue.enqueue(async () => {
      order.push("2");
      return "ok";
    });
    const p3 = queue.enqueue(async () => {
      order.push("3");
      return "ok2";
    });

    await expect(p1).rejects.toThrow("fail");
    await expect(p2).resolves.toBe("ok");
    await expect(p3).resolves.toBe("ok2");
    expect(order).toEqual(["1", "2", "3"]);
  });

  it("size is 0 when idle", () => {
    const queue = new SerialQueue();
    expect(queue.size).toBe(0);
  });

  it("size reflects waiting + running tasks through a task's lifecycle", async () => {
    const queue = new SerialQueue();
    const d1 = createDeferred<void>();
    const d2 = createDeferred<void>();

    const p1 = queue.enqueue(() => d1.promise);
    await tick();
    expect(queue.size).toBe(1); // task 1 running, nothing waiting

    const p2 = queue.enqueue(() => d2.promise);
    await tick();
    expect(queue.size).toBe(2); // task 1 running, task 2 waiting

    d1.resolve();
    await p1;
    await tick();
    expect(queue.size).toBe(1); // task 2 now running

    d2.resolve();
    await p2;
    expect(queue.size).toBe(0);
  });
});
