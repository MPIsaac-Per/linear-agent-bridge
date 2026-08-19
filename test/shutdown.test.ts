import { afterEach, describe, expect, it, vi } from "vitest";
import {
  awaitReadyOrShutdown,
  installGracefulShutdown,
  type ShutdownSignalSource,
} from "../src/shutdown.js";

/** A signal source a test controls, so no test signals the test runner. */
function fakeSignals(): ShutdownSignalSource & {
  raise: (signal: "SIGINT" | "SIGTERM") => void;
  listenerCount: () => number;
} {
  const listeners = new Map<string, Set<() => void>>();
  return {
    on(signal, listener) {
      const existing = listeners.get(signal) ?? new Set();
      existing.add(listener);
      listeners.set(signal, existing);
      return this;
    },
    removeListener(signal, listener) {
      listeners.get(signal)?.delete(listener);
      return this;
    },
    raise(signal) {
      for (const listener of [...(listeners.get(signal) ?? [])]) {
        listener();
      }
    },
    listenerCount() {
      let total = 0;
      for (const set of listeners.values()) {
        total += set.size;
      }
      return total;
    },
  };
}

const TIMEOUT_MS = 10_000;

afterEach(() => {
  vi.useRealTimers();
});

describe("installGracefulShutdown", () => {
  it("closes once and exits 0 when the close completes", async () => {
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    let closeCalls = 0;
    const shutdown = installGracefulShutdown(
      {
        close: async () => {
          closeCalls += 1;
        },
      },
      {
        timeoutMs: TIMEOUT_MS,
        signalSource: signals,
        exit: (code) => exitCodes.push(code),
      },
    );

    signals.raise("SIGTERM");
    await shutdown.shutdown();

    expect(closeCalls).toBe(1);
    expect(exitCodes).toEqual([0]);
    // Nothing left holding the event loop open.
    expect(signals.listenerCount()).toBe(0);
  });

  it("produces exactly one close for two signals in quick succession", async () => {
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    let closeCalls = 0;
    const shutdown = installGracefulShutdown(
      {
        close: async () => {
          closeCalls += 1;
        },
      },
      {
        timeoutMs: TIMEOUT_MS,
        signalSource: signals,
        exit: (code) => exitCodes.push(code),
      },
    );

    signals.raise("SIGTERM");
    signals.raise("SIGINT");
    await shutdown.shutdown();

    expect(closeCalls).toBe(1);
    expect(exitCodes).toEqual([0]);
  });

  it("exits 1 with a bounded diagnostic when close rejects", async () => {
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    const errors: unknown[] = [];
    const shutdown = installGracefulShutdown(
      {
        close: async () => {
          throw new TypeError("secret-bearing detail that must not be logged");
        },
      },
      {
        timeoutMs: TIMEOUT_MS,
        signalSource: signals,
        exit: (code) => exitCodes.push(code),
        onError: (error) => errors.push(error),
      },
    );

    signals.raise("SIGTERM");
    await expect(shutdown.shutdown()).rejects.toThrow(TypeError);

    expect(exitCodes).toEqual([1]);
    expect(errors).toHaveLength(1);
  });

  it("exits 1 once the timeout elapses rather than hanging on a close that never settles", async () => {
    vi.useFakeTimers();
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    const logged: string[] = [];
    const errorSpy = vi
      .spyOn(console, "error")
      .mockImplementation((message: unknown) => {
        logged.push(String(message));
      });

    try {
      installGracefulShutdown(
        {
          // Never settles. A real filesystem or socket can do this.
          close: () => new Promise<void>(() => undefined),
        },
        {
          timeoutMs: TIMEOUT_MS,
          signalSource: signals,
          exit: (code) => exitCodes.push(code),
        },
      );

      signals.raise("SIGTERM");
      expect(exitCodes).toEqual([]);
      await vi.advanceTimersByTimeAsync(TIMEOUT_MS);

      expect(exitCodes).toEqual([1]);
      expect(logged.join(" ")).toContain("shutdown timed out");
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("does not shorten the deadline when a second signal arrives during shutdown", async () => {
    vi.useFakeTimers();
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    installGracefulShutdown(
      { close: () => new Promise<void>(() => undefined) },
      {
        timeoutMs: TIMEOUT_MS,
        signalSource: signals,
        exit: (code) => exitCodes.push(code),
      },
    );

    signals.raise("SIGTERM");
    await vi.advanceTimersByTimeAsync(TIMEOUT_MS / 2);
    signals.raise("SIGINT");
    await vi.advanceTimersByTimeAsync(TIMEOUT_MS / 2 - 1);

    // The second signal must not have restarted or shortened the window.
    expect(exitCodes).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(exitCodes).toEqual([1]);
  });
});

describe("awaitReadyOrShutdown", () => {
  it("shuts down cleanly with no unhandled rejection when a signal beats ready", async () => {
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    let closeCalls = 0;
    const shutdown = installGracefulShutdown(
      {
        close: async () => {
          closeCalls += 1;
        },
      },
      {
        timeoutMs: TIMEOUT_MS,
        signalSource: signals,
        exit: (code) => exitCodes.push(code),
      },
    );
    // Startup recovery can block on OAuth indefinitely, so a signal arriving
    // while ready is still pending is reachable, not a corner case.
    const ready = Promise.reject(new Error("startup aborted"));

    signals.raise("SIGTERM");
    const started = await awaitReadyOrShutdown(ready, shutdown);

    expect(started).toBe(false);
    expect(closeCalls).toBe(1);
    expect(exitCodes).toEqual([0]);
  });

  it("reports a genuine startup failure rather than swallowing it", async () => {
    const signals = fakeSignals();
    const shutdown = installGracefulShutdown(
      { close: async () => undefined },
      { timeoutMs: TIMEOUT_MS, signalSource: signals, exit: () => undefined },
    );
    const ready = Promise.reject(new Error("port in use"));

    await expect(awaitReadyOrShutdown(ready, shutdown)).rejects.toThrow(
      "port in use",
    );
    expect(signals.listenerCount()).toBe(0);
  });
});

describe("installGracefulShutdown exit discipline", () => {
  it("does not exit 0 after the deadline already exited 1", async () => {
    vi.useFakeTimers();
    const signals = fakeSignals();
    const exitCodes: number[] = [];
    let releaseClose: (() => void) | undefined;
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      installGracefulShutdown(
        {
          close: () =>
            new Promise<void>((resolve) => {
              releaseClose = resolve;
            }),
        },
        {
          timeoutMs: TIMEOUT_MS,
          signalSource: signals,
          exit: (code) => exitCodes.push(code),
        },
      );

      signals.raise("SIGTERM");
      await vi.advanceTimersByTimeAsync(TIMEOUT_MS);
      expect(exitCodes).toEqual([1]);

      // The close finally settles, long after the deadline gave up on it.
      releaseClose?.();
      await vi.advanceTimersByTimeAsync(1);

      expect(exitCodes).toEqual([1]);
    } finally {
      errorSpy.mockRestore();
    }
  });
});
