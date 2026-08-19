import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import * as path from "node:path";

export interface SessionRecord {
  linearSessionId: string;
  runtimeSessionId: string;
  runtime: string;
  issueIdentifier?: string | undefined;
  updatedAt: string;
}

export class SessionStoreReadPendingError extends Error {
  constructor(readonly settlement: Promise<void>) {
    super("Session store read is still pending");
    this.name = "SessionStoreReadPendingError";
  }
}

type SessionMap = Record<string, SessionRecord>;

interface PendingSessionRead {
  promise: Promise<SessionMap>;
  settlement: Promise<void>;
  detachedSessionIds: Set<string>;
  generation: number;
}

/**
 * Maps Linear agent-session ids to runtime session ids so follow-up
 * prompts resume the same agent conversation. JSON file on disk;
 * writes are durable and atomic on supported POSIX platforms.
 */
export class JsonSessionStore {
  private readonly resolvedPath: string;
  private directoryReady: Promise<void> | undefined;
  private mutationTail: Promise<void> = Promise.resolve();
  private readonly recoveredRecords = new Map<string, SessionRecord | null>();
  private pendingRead: PendingSessionRead | undefined;
  private snapshotGeneration = 0;

  constructor(pathname: string) {
    this.resolvedPath = path.resolve(pathname);
  }

  async get(
    linearSessionId: string,
    signal?: AbortSignal,
  ): Promise<SessionRecord | undefined> {
    await beforeAbort(this.mutationTail, signal);
    if (this.recoveredRecords.has(linearSessionId)) {
      const recovered = this.recoveredRecords.get(linearSessionId);
      this.recoveredRecords.delete(linearSessionId);
      return recovered ?? undefined;
    }
    const sessions = await this.readSnapshotBeforeAbort(linearSessionId, signal);
    return sessions[linearSessionId];
  }

  put(record: SessionRecord, signal?: AbortSignal): Promise<void> {
    if (!isSessionRecord(record.linearSessionId, record)) {
      return Promise.reject(new Error("Invalid session record"));
    }
    this.recoveredRecords.clear();
    this.snapshotGeneration += 1;
    const write = this.mutationTail.then(() => this.putSerialized(record, signal));
    this.mutationTail = write.then(
      () => undefined,
      () => undefined,
    );
    return write;
  }

  private async putSerialized(
    record: SessionRecord,
    signal?: AbortSignal,
  ): Promise<void> {
    throwIfAborted(signal);
    const sessions = await this.readAll();
    throwIfAborted(signal);
    sessions[record.linearSessionId] = record;
    await this.writeAll(sessions, signal);
  }

  private async readSnapshotBeforeAbort(
    linearSessionId: string,
    signal?: AbortSignal,
  ): Promise<SessionMap> {
    const read = this.pendingRead ?? this.startSnapshotRead();
    if (signalIsAborted(signal)) {
      read.detachedSessionIds.add(linearSessionId);
      void read.promise.catch(() => undefined);
      throw new SessionStoreReadPendingError(read.settlement);
    }
    try {
      return await beforeAbort(read.promise, signal);
    } catch (error) {
      if (signalIsAborted(signal)) {
        read.detachedSessionIds.add(linearSessionId);
        throw new SessionStoreReadPendingError(read.settlement);
      }
      throw error;
    }
  }

  private startSnapshotRead(): PendingSessionRead {
    const generation = this.snapshotGeneration;
    const pending = {
      promise: this.readAll(),
      settlement: Promise.resolve(),
      detachedSessionIds: new Set<string>(),
      generation,
    } satisfies PendingSessionRead;
    pending.settlement = pending.promise.then(
      (sessions) => {
        if (
          this.pendingRead === pending &&
          this.snapshotGeneration === pending.generation
        ) {
          for (const linearSessionId of pending.detachedSessionIds) {
            this.recoveredRecords.set(
              linearSessionId,
              sessions[linearSessionId] ?? null,
            );
          }
        }
        if (this.pendingRead === pending) {
          this.pendingRead = undefined;
        }
      },
      () => {
        if (this.pendingRead === pending) {
          this.pendingRead = undefined;
        }
      },
    );
    this.pendingRead = pending;
    return pending;
  }

  /** Only a missing file reads as an empty store. */
  private async readAll(): Promise<SessionMap> {
    let raw: string;
    try {
      raw = await fs.readFile(this.resolvedPath, "utf8");
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return emptySessionMap();
      }
      throw error;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (error) {
      throw new Error(`Invalid session store JSON at ${this.resolvedPath}`, {
        cause: error,
      });
    }
    if (!isSessionMap(parsed)) {
      throw new Error(
        `Invalid session store structure at ${this.resolvedPath}`,
      );
    }
    return toSessionMap(parsed);
  }

  private async ensureDurableDirectory(directory: string): Promise<void> {
    const attempt =
      this.directoryReady ?? this.prepareDurableDirectory(directory);
    this.directoryReady = attempt;
    try {
      await attempt;
    } catch (error) {
      if (this.directoryReady === attempt) {
        this.directoryReady = undefined;
      }
      throw error;
    }
  }

  private async prepareDurableDirectory(directory: string): Promise<void> {
    await fs.mkdir(directory, { recursive: true });
    if (!SUPPORTS_DIRECTORY_FSYNC) {
      return;
    }
    for (const prefix of absoluteDirectoryPrefixes(directory)) {
      await syncDirectory(prefix);
    }
  }

  /** Write and sync a secured temp file before atomically replacing the target. */
  private async writeAll(
    sessions: SessionMap,
    signal?: AbortSignal,
  ): Promise<void> {
    const directory = path.dirname(this.resolvedPath);
    throwIfAborted(signal);
    await this.ensureDurableDirectory(directory);
    throwIfAborted(signal);

    const tmpPath = path.join(
      directory,
      `.${path.basename(this.resolvedPath)}.${randomUUID()}.tmp`,
    );
    let tmpHandle: Awaited<ReturnType<typeof fs.open>> | undefined;
    let ownsTempPath = false;
    let operationFailed = false;
    let operationError: unknown;

    try {
      throwIfAborted(signal);
      tmpHandle = await fs.open(tmpPath, "wx", 0o600);
      ownsTempPath = true;
      throwIfAborted(signal);
      await tmpHandle.writeFile(JSON.stringify(sessions, null, 2), "utf8");
      throwIfAborted(signal);
      await tmpHandle.chmod(0o600);
      throwIfAborted(signal);
      await tmpHandle.sync();
      throwIfAborted(signal);
      await tmpHandle.close();
      tmpHandle = undefined;

      // Rename is the point after which cancellation cannot safely undo the
      // durable mapping. Once entered, finish its directory sync and let the
      // caller track settlement through shutdown.
      throwIfAborted(signal);
      await fs.rename(tmpPath, this.resolvedPath);
      ownsTempPath = false;
      await syncDirectory(directory);
    } catch (error) {
      operationFailed = true;
      operationError = error;
    }

    const closeError = await closeHandle(tmpHandle);
    const unlinkError = ownsTempPath ? await unlinkTemp(tmpPath) : undefined;
    if (operationFailed) {
      throw operationError;
    }
    if (closeError !== undefined) {
      throw closeError;
    }
    if (unlinkError !== undefined) {
      throw unlinkError;
    }
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal !== undefined && signalIsAborted(signal)) {
    throw signal.reason ?? new Error("Session store write aborted");
  }
}

function signalIsAborted(signal?: AbortSignal): boolean {
  return signal?.aborted === true;
}

function beforeAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) {
    return promise;
  }
  if (signal.aborted) {
    void promise.catch(() => undefined);
    return Promise.reject(signal.reason);
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

const SUPPORTS_DIRECTORY_FSYNC =
  process.platform === "darwin" || process.platform === "linux";
const REQUIRED_SESSION_FIELDS = [
  "linearSessionId",
  "runtimeSessionId",
  "runtime",
  "updatedAt",
] as const;
const SESSION_FIELDS = new Set<string>([
  ...REQUIRED_SESSION_FIELDS,
  "issueIdentifier",
]);

function emptySessionMap(): SessionMap {
  return Object.create(null) as SessionMap;
}

function toSessionMap(value: SessionMap): SessionMap {
  const sessions = emptySessionMap();
  for (const [key, entry] of Object.entries(value)) {
    sessions[key] = entry;
  }
  return sessions;
}

function isSessionMap(value: unknown): value is SessionMap {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  return Object.entries(value).every(([key, entry]) =>
    isSessionRecord(key, entry),
  );
}

function isSessionRecord(key: string, value: unknown): value is SessionRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record);
  return (
    keys.every((field) => SESSION_FIELDS.has(field)) &&
    REQUIRED_SESSION_FIELDS.every((field) => Object.hasOwn(record, field)) &&
    isNonEmptyString(key) &&
    record.linearSessionId === key &&
    isNonEmptyString(record.runtimeSessionId) &&
    isNonEmptyString(record.runtime) &&
    (record.issueIdentifier === undefined ||
      isNonEmptyString(record.issueIdentifier)) &&
    isCanonicalTimestamp(record.updatedAt)
  );
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isCanonicalTimestamp(value: unknown): value is string {
  if (typeof value !== "string") {
    return false;
  }
  const timestamp = Date.parse(value);
  return (
    Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value
  );
}

function absoluteDirectoryPrefixes(directory: string): string[] {
  const resolvedDirectory = path.resolve(directory);
  const { root } = path.parse(resolvedDirectory);
  const prefixes = [root];
  let prefix = root;
  for (const segment of path.relative(root, resolvedDirectory).split(path.sep)) {
    if (segment.length > 0) {
      prefix = path.join(prefix, segment);
      prefixes.push(prefix);
    }
  }
  return prefixes;
}

async function syncDirectory(directory: string): Promise<void> {
  if (!SUPPORTS_DIRECTORY_FSYNC) {
    return;
  }
  const handle = await fs.open(directory, "r");
  let syncFailed = false;
  let syncError: unknown;
  try {
    await handle.sync();
  } catch (error) {
    syncFailed = true;
    syncError = error;
  }
  const closeError = await closeHandle(handle);
  if (syncFailed) {
    throw syncError;
  }
  if (closeError !== undefined) {
    throw closeError;
  }
}

async function closeHandle(
  handle: Awaited<ReturnType<typeof fs.open>> | undefined,
): Promise<unknown | undefined> {
  if (handle === undefined) {
    return undefined;
  }
  try {
    await handle.close();
    return undefined;
  } catch (error) {
    return error;
  }
}

async function unlinkTemp(tmpPath: string): Promise<unknown | undefined> {
  try {
    await fs.unlink(tmpPath);
    return undefined;
  } catch (error) {
    return isNodeError(error, "ENOENT") ? undefined : error;
  }
}

function isNodeError(
  error: unknown,
  code: string,
): error is NodeJS.ErrnoException {
  return error instanceof Error && (error as NodeJS.ErrnoException).code === code;
}
