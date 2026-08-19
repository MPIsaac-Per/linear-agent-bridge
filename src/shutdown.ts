/**
 * Graceful shutdown for the bridge process.
 *
 * launchd sends SIGTERM on `launchctl bootout` and `launchctl kickstart -k`,
 * and systemd sends it on `systemctl stop` and `systemctl restart`. The
 * installer runs one of those on every install, so this path is routine rather
 * than exceptional.
 *
 * It matters because the durability work assumes `server.close()` runs: close
 * sets `closing`, aborts the shutdown controller, clears the reconciliation
 * timer, aborts the reconciliation controller, waits for the queue boundary,
 * and lets in-flight dispatch markers settle. Killed abruptly instead, a turn
 * dies with no terminal Linear activity and a claim can strand between its
 * dispatch marker and completion, where recovery can only call it ambiguous.
 */

export interface GracefulServer {
  close(): Promise<void>;
}

export interface ShutdownSignalSource {
  on(signal: "SIGINT" | "SIGTERM", listener: () => void): unknown;
  removeListener(signal: "SIGINT" | "SIGTERM", listener: () => void): unknown;
}

export interface GracefulShutdown {
  shutdown(): Promise<void>;
  isShuttingDown(): boolean;
  dispose(): void;
}

export interface GracefulShutdownOptions {
  /** Bound on the close. An unbounded wait is a defect, not a safe default. */
  timeoutMs: number;
  signalSource?: ShutdownSignalSource;
  onError?: (error: unknown) => void;
  exit?: (code: number) => void;
}

/**
 * Bounded classifier. The error name only: a raw error can carry a path, a
 * prompt, or a credential, and none of those belong in this log line.
 */
function shutdownErrorClass(error: unknown): string {
  return error instanceof Error ? error.name : "UnknownError";
}

/**
 * Await startup, unless a signal already began shutting the process down.
 *
 * Startup recovery can block on OAuth indefinitely, so a signal arriving while
 * `ready` is still pending is reachable. Returns false when shutdown won, so
 * the caller skips its "listening" log rather than announcing a server that is
 * on its way out.
 */
export async function awaitReadyOrShutdown(
  ready: Promise<void>,
  shutdown: GracefulShutdown,
): Promise<boolean> {
  try {
    await ready;
    return true;
  } catch (error) {
    if (!shutdown.isShuttingDown()) {
      shutdown.dispose();
      throw error;
    }
    await shutdown.shutdown().catch(() => undefined);
    return false;
  }
}

export function installGracefulShutdown(
  server: GracefulServer,
  options: GracefulShutdownOptions,
): GracefulShutdown {
  const signalSource = options.signalSource ?? process;
  const exit = options.exit ?? process.exit;
  const onError =
    options.onError ??
    ((error: unknown): void => {
      console.error(
        `[linear-agent-bridge] graceful shutdown failed: error=${shutdownErrorClass(error)}`,
      );
      process.exitCode = 1;
    });
  let shutdownPromise: Promise<void> | undefined;
  let signalObserved = false;
  let settled = false;
  let exited = false;
  let deadline: NodeJS.Timeout | undefined;

  // The process exits once. A close that settles after the deadline has
  // already given up must not follow exit(1) with exit(0).
  const exitOnce = (code: number): void => {
    if (exited) {
      return;
    }
    exited = true;
    exit(code);
  };

  const dispose = (): void => {
    signalSource.removeListener("SIGINT", onSignal);
    signalSource.removeListener("SIGTERM", onSignal);
    if (deadline !== undefined) {
      clearTimeout(deadline);
      deadline = undefined;
    }
  };

  const shutdown = (): Promise<void> => {
    if (shutdownPromise === undefined) {
      shutdownPromise = Promise.resolve()
        .then(() => server.close())
        .catch((error: unknown) => {
          onError(error);
          throw error;
        })
        .finally(() => {
          settled = true;
          dispose();
        });
    }
    return shutdownPromise;
  };

  const onSignal = (): void => {
    // A second signal must not start a second close, and must not restart or
    // shorten the window the first one opened.
    if (signalObserved) {
      return;
    }
    signalObserved = true;
    deadline = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      console.error(
        `[linear-agent-bridge] shutdown timed out after ${options.timeoutMs}ms`,
      );
      dispose();
      exitOnce(1);
    }, options.timeoutMs);
    // Do not hold the process open on the deadline timer alone.
    deadline.unref?.();
    void shutdown().then(
      () => exitOnce(0),
      () => exitOnce(1),
    );
  };

  signalSource.on("SIGINT", onSignal);
  signalSource.on("SIGTERM", onSignal);
  return {
    shutdown,
    isShuttingDown: () => shutdownPromise !== undefined,
    dispose,
  };
}
