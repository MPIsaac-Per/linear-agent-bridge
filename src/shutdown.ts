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
  options: {
    signalSource?: ShutdownSignalSource;
    onError?: (error: unknown) => void;
    exit?: (code: number) => void;
  } = {},
): GracefulShutdown {
  const signalSource = options.signalSource ?? process;
  const exit = options.exit ?? process.exit;
  const onError =
    options.onError ??
    ((error: unknown): void => {
      const errorClass = error instanceof Error ? error.name : "UnknownError";
      console.error(
        `[linear-agent-bridge] graceful shutdown failed: error=${errorClass}`,
      );
      process.exitCode = 1;
    });
  let shutdownPromise: Promise<void> | undefined;
  let signalObserved = false;

  const dispose = (): void => {
    signalSource.removeListener("SIGINT", onSignal);
    signalSource.removeListener("SIGTERM", onSignal);
  };
  const shutdown = (): Promise<void> => {
    if (shutdownPromise === undefined) {
      shutdownPromise = Promise.resolve()
        .then(() => server.close())
        .catch((error: unknown) => {
          onError(error);
          throw error;
        })
        .finally(dispose);
    }
    return shutdownPromise;
  };
  const onSignal = (): void => {
    if (signalObserved) {
      return;
    }
    signalObserved = true;
    void shutdown().then(
      () => exit(0),
      () => exit(1),
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
